//! bench7 — axum implementation. Same endpoints, SQL, cache, batcher and JWT
//! rules as the other apps (see README "App contract").
//!
//! * ids: UUIDv7 generated here; created_at = uuid_extract_timestamp(id).
//! * cache: per-key TTL from deterministic hash bucketing (no thundering herd).
//! * writes: per-table batcher with BATCH_LANES lanes; a request goes to the lane of its key
//!   (user / conversation / post), so order per key is kept while up to BATCH_LANES flushes per
//!   table are in flight. Each lane appends requests into column vectors, then writes one upsert
//!   transaction per batch (unnest of the column arrays); the reply is sent after commit.
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts},
    response::Response,
    routing::{get, post},
    serve::ListenerExt,
};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use moka::{Expiry, future::Cache};
use serde::{Deserialize, Serialize};
use sqlx::{
    PgPool, Postgres,
    postgres::{PgArguments, PgConnectOptions, PgPoolOptions},
    query::Query as SqlQuery,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const PAGE: usize = 5;
/// write attempts per batch (backoff 50, 100, 200 ms between them)
const WRITE_ATTEMPTS: u32 = 4;

// ---------- SQL (identical in every app) ----------

macro_rules! post_select {
    () => {
        "SELECT p.id, p.user_id, u.username, u.display_name, u.avatar_url, u.is_verified, p.reply_to_id, \
         p.title, left(p.body, 210) AS preview, p.lang, p.media_url, p.like_count, p.comment_count, \
         p.share_count, (extract(epoch FROM p.created_at)*1000)::int8 AS created_ms \
         FROM posts p JOIN users u ON u.id = p.user_id "
    };
}
const SQL_PUBLIC: &str = concat!(post_select!(), "WHERE p.visibility = 0 AND p.deleted_at IS NULL AND p.id < $1 ORDER BY p.id DESC LIMIT 5");
const SQL_PRIVATE: &str = concat!(
    post_select!(),
    "WHERE p.user_id = $1 AND p.visibility = 1 AND p.deleted_at IS NULL AND p.id < $2 ORDER BY p.id DESC LIMIT 5"
);
const SQL_MESSAGES: &str = "SELECT m.id, m.conversation_id, m.sender_id, u.username AS sender_username, \
    u.display_name AS sender_display_name, u.avatar_url AS sender_avatar_url, m.content_type, \
    left(m.body, 240) AS preview, m.attachment_url, (extract(epoch FROM m.created_at)*1000)::int8 AS created_ms, \
    (extract(epoch FROM m.read_at)*1000)::int8 AS read_ms \
    FROM messages m JOIN users u ON u.id = m.sender_id \
    WHERE m.recipient_id = $1 AND m.deleted_at IS NULL AND m.id < $2 ORDER BY m.id DESC LIMIT 5";
const SQL_USER_EXISTS: &str = "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)";

// batch sources: unnest of the column arrays
const POSTS_UNNEST: &str = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int2[], $5::text[], $6::text[], $7::text[], $8::text[]) \
    AS t(id, user_id, reply_to_id, visibility, title, body, lang, media_url)";
const MESSAGES_UNNEST: &str = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::int2[], $6::text[], $7::text[]) \
    AS t(id, new_conv_id, sender_id, recipient_id, content_type, body, attachment_url)";
const LIKES_UNNEST: &str = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS t(id, user_id, post_id)";

fn sql_posts(src: &str) -> String {
    format!(
        "WITH raw AS ({src}), \
         ins AS ( \
           INSERT INTO posts (id, user_id, reply_to_id, visibility, title, body, lang, media_url, like_count, comment_count, \
                              share_count, view_count, is_edited, created_at, updated_at, deleted_at) \
           SELECT r.id, r.user_id, r.reply_to_id, r.visibility, r.title, r.body, r.lang, r.media_url, 0, 0, 0, 0, false, \
                  uuid_extract_timestamp(r.id), uuid_extract_timestamp(r.id), NULL \
           FROM raw r \
           WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) \
             AND (r.reply_to_id IS NULL OR EXISTS (SELECT 1 FROM posts p WHERE p.id = r.reply_to_id)) \
           ON CONFLICT (id) DO NOTHING \
           RETURNING user_id, reply_to_id), \
         by_user AS ( \
           UPDATE users SET posts_count = users.posts_count + c.n \
           FROM (SELECT user_id, count(*) AS n FROM ins GROUP BY user_id ORDER BY user_id) c \
           WHERE users.id = c.user_id) \
         UPDATE posts SET comment_count = posts.comment_count + c.n \
         FROM (SELECT reply_to_id, count(*) AS n FROM ins WHERE reply_to_id IS NOT NULL GROUP BY reply_to_id ORDER BY reply_to_id) c \
         WHERE posts.id = c.reply_to_id"
    )
}

fn sql_messages(src: &str) -> String {
    format!(
        "WITH raw AS ({src}), \
         v AS ( \
           SELECT r.*, least(r.sender_id, r.recipient_id) AS ua, greatest(r.sender_id, r.recipient_id) AS ub \
           FROM raw r \
           WHERE r.sender_id <> r.recipient_id \
             AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.recipient_id) \
             AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.sender_id)), \
         last AS ( \
           SELECT DISTINCT ON (ua, ub) ua, ub, new_conv_id, id, left(body, 100) AS preview, \
                  count(*) OVER (PARTITION BY ua, ub) AS n \
           FROM v ORDER BY ua, ub, id DESC), \
         conv AS ( \
           INSERT INTO conversations AS c (id, user_a_id, user_b_id, last_message_id, last_message_preview, \
                                           last_message_at, message_count, created_at, updated_at) \
           SELECT new_conv_id, ua, ub, id, preview, uuid_extract_timestamp(id), n, \
                  uuid_extract_timestamp(new_conv_id), uuid_extract_timestamp(id) \
           FROM last \
           ON CONFLICT (user_a_id, user_b_id) DO UPDATE SET \
             last_message_id = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_id ELSE c.last_message_id END, \
             last_message_preview = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_preview ELSE c.last_message_preview END, \
             last_message_at = greatest(EXCLUDED.last_message_at, c.last_message_at), \
             message_count = c.message_count + EXCLUDED.message_count, \
             updated_at = greatest(EXCLUDED.updated_at, c.updated_at) \
           RETURNING c.id, c.user_a_id, c.user_b_id) \
         INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content_type, body, attachment_url, \
                               created_at, read_at, edited_at, deleted_at) \
         SELECT v.id, conv.id, v.sender_id, v.recipient_id, v.content_type, v.body, v.attachment_url, \
                uuid_extract_timestamp(v.id), NULL, NULL, NULL \
         FROM v JOIN conv ON conv.user_a_id = v.ua AND conv.user_b_id = v.ub \
         ON CONFLICT (id) DO NOTHING"
    )
}

fn sql_likes(src: &str) -> String {
    format!(
        "WITH raw AS ({src}), \
         ins AS ( \
           INSERT INTO likes (id, user_id, post_id, created_at) \
           SELECT r.id, r.user_id, r.post_id, uuid_extract_timestamp(r.id) \
           FROM raw r \
           WHERE EXISTS (SELECT 1 FROM posts p WHERE p.id = r.post_id) \
             AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) \
           ON CONFLICT DO NOTHING \
           RETURNING post_id) \
         UPDATE posts SET like_count = posts.like_count + c.n \
         FROM (SELECT post_id, count(*) AS n FROM ins GROUP BY post_id ORDER BY post_id) c \
         WHERE posts.id = c.post_id"
    )
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

// ---------- column batches: each request is appended to per-column vectors ----------

trait Batch: Default + Send + Sync + 'static {
    type Row: Send + 'static;
    const NAME: &'static str;
    fn push(&mut self, r: Self::Row);
    fn len(&self) -> usize;
    fn bind(&self, q: SqlQuery<'static, Postgres, PgArguments>) -> SqlQuery<'static, Postgres, PgArguments>;
}

#[derive(Default)]
struct PostBatch {
    id: Vec<Uuid>,
    user_id: Vec<Uuid>,
    reply_to: Vec<Option<Uuid>>,
    visibility: Vec<i16>,
    title: Vec<String>,
    body: Vec<String>,
    lang: Vec<String>,
    media_url: Vec<Option<String>>,
}
struct NewPost {
    user_id: Uuid,
    reply_to: Option<Uuid>,
    visibility: i16,
    title: String,
    body: String,
    lang: String,
    media_url: Option<String>,
}
impl Batch for PostBatch {
    type Row = NewPost;
    const NAME: &'static str = "posts";
    fn push(&mut self, r: NewPost) {
        self.id.push(Uuid::now_v7());
        self.user_id.push(r.user_id);
        self.reply_to.push(r.reply_to);
        self.visibility.push(r.visibility);
        self.title.push(r.title);
        self.body.push(r.body);
        self.lang.push(r.lang);
        self.media_url.push(r.media_url);
    }
    fn len(&self) -> usize {
        self.id.len()
    }
    fn bind(&self, q: SqlQuery<'static, Postgres, PgArguments>) -> SqlQuery<'static, Postgres, PgArguments> {
        q.bind(&self.id)
            .bind(&self.user_id)
            .bind(&self.reply_to)
            .bind(&self.visibility)
            .bind(&self.title)
            .bind(&self.body)
            .bind(&self.lang)
            .bind(&self.media_url)
    }
}

#[derive(Default)]
struct MessageBatch {
    id: Vec<Uuid>,
    new_conv_id: Vec<Uuid>,
    sender: Vec<Uuid>,
    recipient: Vec<Uuid>,
    content_type: Vec<i16>,
    body: Vec<String>,
    attachment_url: Vec<Option<String>>,
}
struct NewMessage {
    sender: Uuid,
    recipient: Uuid,
    content_type: i16,
    body: String,
    attachment_url: Option<String>,
}
impl Batch for MessageBatch {
    type Row = NewMessage;
    const NAME: &'static str = "messages";
    fn push(&mut self, r: NewMessage) {
        self.id.push(Uuid::now_v7());
        self.new_conv_id.push(Uuid::now_v7());
        self.sender.push(r.sender);
        self.recipient.push(r.recipient);
        self.content_type.push(r.content_type);
        self.body.push(r.body);
        self.attachment_url.push(r.attachment_url);
    }
    fn len(&self) -> usize {
        self.id.len()
    }
    fn bind(&self, q: SqlQuery<'static, Postgres, PgArguments>) -> SqlQuery<'static, Postgres, PgArguments> {
        q.bind(&self.id)
            .bind(&self.new_conv_id)
            .bind(&self.sender)
            .bind(&self.recipient)
            .bind(&self.content_type)
            .bind(&self.body)
            .bind(&self.attachment_url)
    }
}

#[derive(Default)]
struct LikeBatch {
    id: Vec<Uuid>,
    user_id: Vec<Uuid>,
    post_id: Vec<Uuid>,
}
struct NewLike {
    user_id: Uuid,
    post_id: Uuid,
}
impl Batch for LikeBatch {
    type Row = NewLike;
    const NAME: &'static str = "likes";
    fn push(&mut self, r: NewLike) {
        self.id.push(Uuid::now_v7());
        self.user_id.push(r.user_id);
        self.post_id.push(r.post_id);
    }
    fn len(&self) -> usize {
        self.id.len()
    }
    fn bind(&self, q: SqlQuery<'static, Postgres, PgArguments>) -> SqlQuery<'static, Postgres, PgArguments> {
        q.bind(&self.id).bind(&self.user_id).bind(&self.post_id)
    }
}

// ---------- batcher: BATCH_LANES queues + writer loops per table ----------

/// Transient errors worth another attempt: deadlock, serialization, statement timeout, too many
/// connections, server shutdown, connection failures, and I/O / pool errors without a SQLSTATE.
fn retryable(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(d) => d.code().is_some_and(|c| {
            matches!(c.as_ref(), "40P01" | "40001" | "57014" | "53300" | "57P01" | "57P02" | "57P03") || c.starts_with("08")
        }),
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::Protocol(_) => true,
        _ => false,
    }
}

/// Lane key: last 4 bytes of a UUID (random part of a v7 id).
fn key32(u: &Uuid) -> u32 {
    let b = u.as_bytes();
    u32::from_be_bytes([b[12], b[13], b[14], b[15]])
}

#[derive(Default)]
struct Stats {
    flushes: AtomicU64,
    rows: AtomicU64,
    flush_us: AtomicU64,
    max_us: AtomicU64,
    retries: AtomicU64,
    fails: AtomicU64,
}

struct Writer {
    pool: PgPool,
    sql: &'static str,
}

impl Writer {
    /// One upsert transaction per batch. Ids were generated when the rows were collected, so a
    /// retry after an ambiguous commit is a no-op (ON CONFLICT (id) DO NOTHING). Transient errors
    /// are retried with backoff; after the last attempt the batch fails loudly (500 to every waiter).
    async fn write<B: Batch>(&self, b: &B, st: &Stats) -> bool {
        for attempt in 1..=WRITE_ATTEMPTS {
            match b.bind(sqlx::query(self.sql)).execute(&self.pool).await.map(|_| ()) {
                Ok(()) => return true,
                Err(e) if attempt < WRITE_ATTEMPTS && retryable(&e) => {
                    st.retries.fetch_add(1, Relaxed);
                    eprintln!("batch {} attempt {attempt} failed, retrying: {e}", B::NAME);
                    tokio::time::sleep(Duration::from_millis(50 << (attempt - 1))).await;
                }
                Err(e) => {
                    st.fails.fetch_add(1, Relaxed);
                    eprintln!("BATCH FAILED {} rows={} after {attempt} attempt(s): {e}", B::NAME, b.len());
                    return false;
                }
            }
        }
        false
    }
}

/// row, reply channel, arrival time (the batch window runs from the arrival of its first row)
type Job<R> = (R, oneshot::Sender<bool>, tokio::time::Instant);

#[derive(Clone, Copy)]
struct BatchCfg {
    max_rows: usize,
    window: Duration,
    lanes: usize,
    queue_max: usize,
    stats: bool,
}

struct Batcher<B: Batch> {
    lanes: Vec<mpsc::Sender<Job<B::Row>>>,
}

impl<B: Batch> Batcher<B> {
    fn new(writer: Writer, cfg: BatchCfg) -> Self {
        let writer = Arc::new(writer);
        let stats = Arc::new(Stats::default());
        let per_lane = (cfg.queue_max / cfg.lanes).max(1);
        let mut lanes = Vec::with_capacity(cfg.lanes);
        for _ in 0..cfg.lanes {
            let (tx, mut rx) = mpsc::channel::<Job<B::Row>>(per_lane);
            lanes.push(tx);
            let (writer, stats) = (writer.clone(), stats.clone());
            tokio::spawn(async move {
                let mut waiters = Vec::with_capacity(cfg.max_rows);
                while let Some((r, w, at)) = rx.recv().await {
                    let mut batch = B::default();
                    batch.push(r);
                    waiters.push(w);
                    let deadline = at + cfg.window;
                    while batch.len() < cfg.max_rows {
                        match tokio::time::timeout_at(deadline, rx.recv()).await {
                            Ok(Some((r, w, _))) => {
                                batch.push(r);
                                waiters.push(w);
                            }
                            _ => break,
                        }
                    }
                    let t0 = Instant::now();
                    let ok = writer.write(&batch, &stats).await;
                    let us = t0.elapsed().as_micros() as u64;
                    stats.flushes.fetch_add(1, Relaxed);
                    stats.rows.fetch_add(batch.len() as u64, Relaxed);
                    stats.flush_us.fetch_add(us, Relaxed);
                    stats.max_us.fetch_max(us, Relaxed);
                    // ack only after the transaction committed (or failed for good)
                    for w in waiters.drain(..) {
                        let _ = w.send(ok);
                    }
                }
            });
        }
        if cfg.stats {
            let txs = lanes.clone();
            tokio::spawn(async move {
                let mut iv = tokio::time::interval(Duration::from_secs(10));
                iv.tick().await;
                loop {
                    iv.tick().await;
                    let f = stats.flushes.swap(0, Relaxed);
                    let rows = stats.rows.swap(0, Relaxed);
                    let us = stats.flush_us.swap(0, Relaxed);
                    let max = stats.max_us.swap(0, Relaxed);
                    let (re, fa) = (stats.retries.swap(0, Relaxed), stats.fails.swap(0, Relaxed));
                    let queued: usize = txs.iter().map(|t| t.max_capacity() - t.capacity()).sum();
                    if f > 0 || queued > 0 {
                        eprintln!(
                            "batch-stats {} flushes={f} rows={rows} avg_rows={:.0} avg_flush_ms={:.1} max_flush_ms={:.1} queued={queued} retries={re} fails={fa}",
                            B::NAME,
                            rows as f64 / f.max(1) as f64,
                            us as f64 / f.max(1) as f64 / 1000.0,
                            max as f64 / 1000.0
                        );
                    }
                }
            });
        }
        Self { lanes }
    }

    /// Queues the row on the lane of `key` and replies after its batch committed.
    /// Full lane queue (WRITE_QUEUE_MAX / BATCH_LANES rows) -> 503 busy.
    async fn submit(&self, row: B::Row, key: u32) -> Response {
        let (tx, rx) = oneshot::channel();
        if self.lanes[key as usize % self.lanes.len()].try_send((row, tx, tokio::time::Instant::now())).is_err() {
            return err(StatusCode::SERVICE_UNAVAILABLE, "busy");
        }
        match rx.await {
            Ok(true) => json_static(r#"{"ok":true}"#),
            _ => err(StatusCode::INTERNAL_SERVER_ERROR, "db"),
        }
    }
}

// ---------- cache TTL: deterministic hash bucketing ----------
// ttl_ms = MIN + (h % BINS) * W + ((h >> 32) % W),  W = (MAX - MIN) / BINS,
// h = FNV-1a 64 of "public:<cursor uuid>". Same key -> same TTL on every node,
// different keys spread evenly over [MIN, MAX) so entries never expire together.

fn fnv1a64(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in s {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

struct HashTtl {
    min_ms: u64,
    width_ms: u64,
    bins: u64,
}
impl HashTtl {
    fn ttl(&self, key: &Uuid) -> Duration {
        let mut s = [0u8; 43];
        s[..7].copy_from_slice(b"public:");
        key.hyphenated().encode_lower(&mut s[7..]);
        let h = fnv1a64(&s);
        Duration::from_millis(self.min_ms + (h % self.bins) * self.width_ms + ((h >> 32) % self.width_ms.max(1)))
    }
}
impl Expiry<Uuid, Bytes> for HashTtl {
    fn expire_after_create(&self, key: &Uuid, _v: &Bytes, _at: Instant) -> Option<Duration> {
        Some(self.ttl(key))
    }
}

// ---------- state ----------

#[derive(Clone)]
struct App(Arc<Inner>);

struct Inner {
    pool: PgPool,
    cache: Cache<Uuid, Bytes>,
    enc: EncodingKey,
    dec: DecodingKey,
    val: Validation,
    iss: String,
    aud: String,
    ttl: u64,
    issuer_key: Vec<u8>,
    posts: Batcher<PostBatch>,
    messages: Batcher<MessageBatch>,
    likes: Batcher<LikeBatch>,
    n_posts: &'static str,
    n_messages: &'static str,
    n_likes: &'static str,
}

fn env_or(k: &str, d: &str) -> String {
    env::var(k).unwrap_or_else(|_| d.to_string())
}
fn env_num<T: std::str::FromStr>(k: &str, d: T) -> T {
    env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
fn now_s() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

// ---------- responses ----------

fn json_static(s: &'static str) -> Response {
    json_bytes(Bytes::from_static(s.as_bytes()), StatusCode::OK, None)
}
fn json_bytes(b: Bytes, code: StatusCode, cache: Option<&'static str>) -> Response {
    let mut r = Response::new(Body::from(b));
    *r.status_mut() = code;
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(c) = cache {
        h.insert("x-cache", HeaderValue::from_static(c));
    }
    r
}
fn err(code: StatusCode, msg: &'static str) -> Response {
    let body = match msg {
        "unauthorized" => r#"{"error":"unauthorized"}"#,
        "bad_request" => r#"{"error":"bad_request"}"#,
        "not_found" => r#"{"error":"not_found"}"#,
        "busy" => r#"{"error":"busy"}"#,
        _ => r#"{"error":"db"}"#,
    };
    json_bytes(Bytes::from_static(body.as_bytes()), code, None)
}
fn unauthorized() -> Response {
    err(StatusCode::UNAUTHORIZED, "unauthorized")
}
fn bad_request() -> Response {
    err(StatusCode::BAD_REQUEST, "bad_request")
}

/// canonical lowercase/uppercase hyphenated UUID only (36 chars)
fn parse_uuid(s: &str) -> Option<Uuid> {
    if s.len() != 36 {
        return None;
    }
    Uuid::try_parse(s).ok()
}

// ---------- JWT ----------

#[derive(Deserialize)]
struct Claims {
    sub: String,
    iat: u64,
}

#[derive(Serialize)]
struct TokenClaims<'a> {
    sub: String,
    iat: u64,
    exp: u64,
    iss: &'a str,
    aud: &'a str,
}

struct Auth(Uuid);

impl FromRequestParts<App> for Auth {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or_else(unauthorized)?;
        let data = jsonwebtoken::decode::<Claims>(token, &app.0.dec, &app.0.val).map_err(|_| unauthorized())?;
        if data.claims.iat > now_s() {
            return Err(unauthorized());
        }
        let sub = parse_uuid(&data.claims.sub).ok_or_else(unauthorized)?;
        Ok(Auth(sub))
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[derive(Deserialize)]
struct TokenReq {
    user_id: String,
}

async fn auth_token(State(app): State<App>, headers: HeaderMap, body: Bytes) -> Response {
    let key = headers.get("x-issuer-key").map(|v| v.as_bytes()).unwrap_or(b"");
    if !ct_eq(key, &app.0.issuer_key) {
        return unauthorized();
    }
    let Ok(req) = serde_json::from_slice::<TokenReq>(&body) else { return bad_request() };
    let Some(uid) = parse_uuid(&req.user_id) else { return bad_request() };
    let exists: bool = match sqlx::query_scalar(SQL_USER_EXISTS).bind(uid).fetch_one(&app.0.pool).await {
        Ok(v) => v,
        Err(_) => return err(StatusCode::INTERNAL_SERVER_ERROR, "db"),
    };
    if !exists {
        return err(StatusCode::NOT_FOUND, "not_found");
    }
    let iat = now_s();
    let claims = TokenClaims { sub: uid.to_string(), iat, exp: iat + app.0.ttl, iss: &app.0.iss, aud: &app.0.aud };
    let Ok(token) = jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &app.0.enc) else {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "db");
    };
    let body = serde_json::to_vec(&serde_json::json!({ "token": token, "expires_in": app.0.ttl })).unwrap();
    json_bytes(Bytes::from(body), StatusCode::OK, None)
}

// ---------- reads ----------

#[derive(Deserialize)]
struct Cursor {
    before: Option<String>,
}
impl Cursor {
    fn get(&self) -> Option<Uuid> {
        match &self.before {
            None => Some(Uuid::max()),
            Some(s) => parse_uuid(s),
        }
    }
}

#[derive(Serialize, sqlx::FromRow)]
struct PostItem {
    id: Uuid,
    user_id: Uuid,
    username: String,
    display_name: String,
    avatar_url: Option<String>,
    is_verified: bool,
    reply_to_id: Option<Uuid>,
    title: String,
    preview: String,
    lang: String,
    media_url: Option<String>,
    like_count: i64,
    comment_count: i64,
    share_count: i64,
    created_ms: i64,
}
#[derive(Serialize, sqlx::FromRow)]
struct MessageItem {
    id: Uuid,
    conversation_id: Uuid,
    sender_id: Uuid,
    sender_username: String,
    sender_display_name: String,
    sender_avatar_url: Option<String>,
    content_type: i16,
    preview: String,
    attachment_url: Option<String>,
    created_ms: i64,
    read_ms: Option<i64>,
}
#[derive(Serialize)]
struct Page<T> {
    items: Vec<T>,
    next: Option<Uuid>,
}

fn page<T: Serialize>(items: Vec<T>, id: impl Fn(&T) -> Uuid) -> Bytes {
    let next = if items.len() == PAGE { items.last().map(&id) } else { None };
    Bytes::from(serde_json::to_vec(&Page { items, next }).unwrap())
}

async fn public_posts(State(app): State<App>, Query(c): Query<Cursor>) -> Response {
    let Some(before) = c.get() else { return bad_request() };
    let pool = app.0.pool.clone();
    let entry = app
        .0
        .cache
        .entry(before)
        .or_try_insert_with(async move {
            let rows: Vec<PostItem> = sqlx::query_as(SQL_PUBLIC).bind(before).fetch_all(&pool).await?;
            Ok::<_, sqlx::Error>(page(rows, |p| p.id))
        })
        .await;
    match entry {
        Ok(e) => {
            let tag = if e.is_fresh() { "miss" } else { "hit" };
            json_bytes(e.into_value(), StatusCode::OK, Some(tag))
        }
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "db"),
    }
}

async fn private_posts(State(app): State<App>, Auth(uid): Auth, Query(c): Query<Cursor>) -> Response {
    let Some(before) = c.get() else { return bad_request() };
    match sqlx::query_as::<_, PostItem>(SQL_PRIVATE).bind(uid).bind(before).fetch_all(&app.0.pool).await {
        Ok(rows) => json_bytes(page(rows, |p| p.id), StatusCode::OK, None),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "db"),
    }
}

async fn list_messages(State(app): State<App>, Auth(uid): Auth, Query(c): Query<Cursor>) -> Response {
    let Some(before) = c.get() else { return bad_request() };
    match sqlx::query_as::<_, MessageItem>(SQL_MESSAGES).bind(uid).bind(before).fetch_all(&app.0.pool).await {
        Ok(rows) => json_bytes(page(rows, |m| m.id), StatusCode::OK, None),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "db"),
    }
}

// ---------- writes ----------

#[derive(Deserialize)]
struct PostReq {
    title: String,
    body: String,
    visibility: i16,
    lang: Option<String>,
    reply_to: Option<String>,
    media_url: Option<String>,
}
#[derive(Deserialize)]
struct MessageReq {
    to: String,
    body: String,
    content_type: Option<i16>,
    attachment_url: Option<String>,
}

fn len_ok(s: &str, min: usize, max: usize) -> bool {
    s.len() >= min && s.len() <= max * 4 && (min..=max).contains(&s.chars().count())
}

fn parse_post(uid: Uuid, body: &[u8]) -> Option<NewPost> {
    let r = serde_json::from_slice::<PostReq>(body).ok()?;
    let lang = r.lang.unwrap_or_else(|| "en".to_string());
    if !len_ok(&r.title, 1, 200)
        || !len_ok(&r.body, 1, 8000)
        || !(r.visibility == 0 || r.visibility == 1)
        || !len_ok(&lang, 2, 8)
        || r.media_url.as_deref().is_some_and(|m| !len_ok(m, 1, 500))
    {
        return None;
    }
    let reply_to = match r.reply_to.as_deref() {
        None => None,
        Some(s) => Some(parse_uuid(s)?),
    };
    Some(NewPost { user_id: uid, reply_to, visibility: r.visibility, title: r.title, body: r.body, lang, media_url: r.media_url })
}

fn parse_message(uid: Uuid, body: &[u8]) -> Option<NewMessage> {
    let r = serde_json::from_slice::<MessageReq>(body).ok()?;
    let to = parse_uuid(&r.to)?;
    let content_type = r.content_type.unwrap_or(0);
    if to == uid
        || !len_ok(&r.body, 1, 8000)
        || !(0..=3).contains(&content_type)
        || r.attachment_url.as_deref().is_some_and(|m| !len_ok(m, 1, 500))
    {
        return None;
    }
    Some(NewMessage { sender: uid, recipient: to, content_type, body: r.body, attachment_url: r.attachment_url })
}

async fn create_post(State(app): State<App>, Auth(uid): Auth, body: Bytes) -> Response {
    let Some(p) = parse_post(uid, &body) else { return bad_request() };
    let key = key32(&p.user_id);
    app.0.posts.submit(p, key).await
}

async fn send_message(State(app): State<App>, Auth(uid): Auth, body: Bytes) -> Response {
    let Some(m) = parse_message(uid, &body) else { return bad_request() };
    // symmetric: both directions of a conversation share one lane
    let key = key32(&m.sender) ^ key32(&m.recipient);
    app.0.messages.submit(m, key).await
}

async fn like_post(State(app): State<App>, Auth(uid): Auth, Path(id): Path<String>) -> Response {
    let Some(post_id) = parse_uuid(&id) else { return bad_request() };
    app.0.likes.submit(NewLike { user_id: uid, post_id }, key32(&post_id)).await
}

// ---------- group 2 ("normal", prefix /n): no batching, no cache ----------
// Same SQL as the batch writers, fed one row from scalar parameters, one statement per request.

const POSTS_ONE: &str = "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS reply_to_id, $4::int2 AS visibility, \
    $5::text AS title, $6::text AS body, $7::text AS lang, $8::text AS media_url";
const MESSAGES_ONE: &str = "SELECT $1::uuid AS id, $2::uuid AS new_conv_id, $3::uuid AS sender_id, $4::uuid AS recipient_id, \
    $5::int2 AS content_type, $6::text AS body, $7::text AS attachment_url";
const LIKES_ONE: &str = "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS post_id";

/// Runs one write statement; retried on deadlock / serialization failure like the batch writer.
async fn exec_one<'q, F>(pool: &PgPool, name: &str, mk: F) -> Response
where
    F: Fn() -> SqlQuery<'q, Postgres, PgArguments>,
{
    for attempt in 0..3 {
        match mk().execute(pool).await {
            Ok(_) => return json_static(r#"{"ok":true}"#),
            Err(e) => {
                let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.into_owned());
                if attempt < 2 && matches!(code.as_deref(), Some("40P01") | Some("40001")) {
                    continue;
                }
                eprintln!("write {name} failed: {e}");
                break;
            }
        }
    }
    err(StatusCode::INTERNAL_SERVER_ERROR, "db")
}

async fn n_public_posts(State(app): State<App>, Query(c): Query<Cursor>) -> Response {
    let Some(before) = c.get() else { return bad_request() };
    match sqlx::query_as::<_, PostItem>(SQL_PUBLIC).bind(before).fetch_all(&app.0.pool).await {
        Ok(rows) => json_bytes(page(rows, |p| p.id), StatusCode::OK, None),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "db"),
    }
}

async fn n_create_post(State(app): State<App>, Auth(uid): Auth, body: Bytes) -> Response {
    let Some(p) = parse_post(uid, &body) else { return bad_request() };
    let id = Uuid::now_v7();
    exec_one(&app.0.pool, "posts", || {
        sqlx::query(app.0.n_posts)
            .bind(id)
            .bind(p.user_id)
            .bind(p.reply_to)
            .bind(p.visibility)
            .bind(&p.title)
            .bind(&p.body)
            .bind(&p.lang)
            .bind(&p.media_url)
    })
    .await
}

async fn n_send_message(State(app): State<App>, Auth(uid): Auth, body: Bytes) -> Response {
    let Some(m) = parse_message(uid, &body) else { return bad_request() };
    let (id, conv) = (Uuid::now_v7(), Uuid::now_v7());
    exec_one(&app.0.pool, "messages", || {
        sqlx::query(app.0.n_messages)
            .bind(id)
            .bind(conv)
            .bind(m.sender)
            .bind(m.recipient)
            .bind(m.content_type)
            .bind(&m.body)
            .bind(&m.attachment_url)
    })
    .await
}

async fn n_like_post(State(app): State<App>, Auth(uid): Auth, Path(id): Path<String>) -> Response {
    let Some(post_id) = parse_uuid(&id) else { return bad_request() };
    let lid = Uuid::now_v7();
    exec_one(&app.0.pool, "likes", || sqlx::query(app.0.n_likes).bind(lid).bind(uid).bind(post_id)).await
}

async fn health() -> Response {
    json_static(r#"{"ok":true,"framework":"axum"}"#)
}

// ---------- main ----------

#[tokio::main]
async fn main() {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool_size: u32 = env_num("DB_POOL_TOTAL", 12);
    let bcfg = BatchCfg {
        max_rows: env_num::<usize>("BATCH_MAX_ROWS", 1000).max(1),
        window: Duration::from_millis(env_num("BATCH_WINDOW_MS", 200)),
        lanes: env_num::<usize>("BATCH_LANES", 4).max(1),
        queue_max: env_num("WRITE_QUEUE_MAX", 40_000),
        stats: env_or("BATCH_STATS", "0") == "1",
    };
    let stmt_timeout: u64 = env_num("STATEMENT_TIMEOUT_MS", 5000);
    let secret = env::var("JWT_SECRET").expect("JWT_SECRET");
    let iss = env_or("JWT_ISS", "bench7");
    let aud = env_or("JWT_AUD", "bench7-api");

    let opts = PgPoolOptions::new().max_connections(pool_size).min_connections(pool_size);
    let mut conn: PgConnectOptions = url.parse().expect("DATABASE_URL");
    if stmt_timeout > 0 {
        conn = conn.options([("statement_timeout", stmt_timeout.to_string())]);
    }
    let pool = opts.connect_with(conn).await.expect("db connect");

    let ttl_min: u64 = env_num("CACHE_TTL_MIN_MS", 45_000);
    let ttl_max: u64 = env_num("CACHE_TTL_MAX_MS", 60_000);
    let bins: u64 = env_num::<u64>("CACHE_TTL_BINS", 15).max(1);
    let cache = Cache::builder()
        .max_capacity(env_num::<u64>("CACHE_MAX_MB", 64) * 1024 * 1024)
        .weigher(|_k: &Uuid, v: &Bytes| (v.len() + 16).min(u32::MAX as usize) as u32)
        .expire_after(HashTtl { min_ms: ttl_min, width_ms: (ttl_max.saturating_sub(ttl_min) / bins).max(1), bins })
        .build();

    let writer = |sql: String| Writer { pool: pool.clone(), sql: leak(sql) };
    let posts = Batcher::<PostBatch>::new(writer(sql_posts(POSTS_UNNEST)), bcfg);
    let messages = Batcher::<MessageBatch>::new(writer(sql_messages(MESSAGES_UNNEST)), bcfg);
    let likes = Batcher::<LikeBatch>::new(writer(sql_likes(LIKES_UNNEST)), bcfg);

    let mut val = Validation::new(Algorithm::HS256);
    val.leeway = 0;
    val.validate_exp = true;
    val.set_issuer(&[iss.as_str()]);
    val.set_audience(&[aud.as_str()]);
    val.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);

    let app = App(Arc::new(Inner {
        pool,
        cache,
        enc: EncodingKey::from_secret(secret.as_bytes()),
        dec: DecodingKey::from_secret(secret.as_bytes()),
        val,
        iss,
        aud,
        ttl: env_num("JWT_TTL_S", 3600),
        issuer_key: env::var("TOKEN_ISSUER_KEY").expect("TOKEN_ISSUER_KEY").into_bytes(),
        posts,
        messages,
        likes,
        n_posts: leak(sql_posts(POSTS_ONE)),
        n_messages: leak(sql_messages(MESSAGES_ONE)),
        n_likes: leak(sql_likes(LIKES_ONE)),
    }));

    let router = Router::new()
        .route("/health", get(health))
        .route("/auth/token", post(auth_token))
        .route("/posts/public", get(public_posts))
        .route("/posts/private", get(private_posts))
        .route("/posts", post(create_post))
        .route("/posts/{id}/like", post(like_post))
        .route("/messages", get(list_messages).post(send_message))
        .route("/n/health", get(health))
        .route("/n/posts/public", get(n_public_posts))
        .route("/n/posts/private", get(private_posts))
        .route("/n/posts", post(n_create_post))
        .route("/n/posts/{id}/like", post(n_like_post))
        .route("/n/messages", get(list_messages).post(n_send_message))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(app);

    if let Ok(path) = env::var("LISTEN_UNIX") {
        let _ = std::fs::remove_file(&path);
        let ul = tokio::net::UnixListener::bind(&path).expect("bind unix");
        let r = router.clone();
        tokio::spawn(async move { axum::serve(ul, r).await.unwrap() });
        eprintln!("axum listening on unix:{path}");
    }
    let addr = format!("{}:{}", env_or("HOST", "0.0.0.0"), env_or("PORT", "8080"));
    let tl = tokio::net::TcpListener::bind(&addr).await.expect("bind tcp").tap_io(|s| {
        let _ = s.set_nodelay(true);
    });
    eprintln!(
        "axum listening on {addr} pool={pool_size} batch_rows={} batch_window_ms={} lanes={} queue_max={} stmt_timeout_ms={stmt_timeout}",
        bcfg.max_rows,
        bcfg.window.as_millis(),
        bcfg.lanes,
        bcfg.queue_max
    );
    axum::serve(tl, router)
        .with_graceful_shutdown(async {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            tokio::select! { _ = term.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
        })
        .await
        .unwrap();
}
