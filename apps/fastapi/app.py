# bench7 — FastAPI implementation (uvicorn workers + uvloop/httptools + asyncpg + cachetools + PyJWT + orjson).
# Same endpoints, SQL, cache, batcher and JWT rules as the other apps.
#   - ids: UUIDv7 generated here; created_at = uuid_extract_timestamp(id)
#   - cache TTL: deterministic hash bucketing (spreads expiries, no thundering herd)
#   - writes: per-table batcher, lanes keyed by user / conversation / post -> column lists ->
#     one upsert transaction per batch (unnest); the reply is sent after commit
import asyncio
import hmac
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from importlib.metadata import version as pkg_version

import asyncpg
import jwt
import orjson
from cachetools import TLRUCache
from fastapi import FastAPI, Request
from fastapi.responses import Response

env = os.environ
WORKERS = int(env.get("WORKERS") or os.cpu_count() or 1)
# total connections stay = DB_POOL_TOTAL across all worker processes (minus rounding)
POOL = max(1, int(env.get("DB_POOL_TOTAL", "12")) // WORKERS)
BATCH_MAX = max(1, int(env.get("BATCH_MAX_ROWS", "1000")))
WINDOW = int(env.get("BATCH_WINDOW_MS", "200")) / 1000
# BATCH_LANES and WRITE_QUEUE_MAX are totals per table across all worker processes
LANES = max(1, round(int(env.get("BATCH_LANES", "4")) / WORKERS))
LANE_CAP = max(1, int(env.get("WRITE_QUEUE_MAX", "40000")) // WORKERS // LANES)
WRITE_ATTEMPTS = 4
STATS = env.get("BATCH_STATS") == "1"
STMT_TIMEOUT_MS = int(env.get("STATEMENT_TIMEOUT_MS", "5000"))
RETRY_STATES = {"40P01", "40001", "57014", "53300", "57P01", "57P02", "57P03"}
BODY_LIMIT = 16 * 1024
SECRET = env["JWT_SECRET"]
ISSUER_KEY = env["TOKEN_ISSUER_KEY"].encode()
ISS = env.get("JWT_ISS", "bench7")
AUD = env.get("JWT_AUD", "bench7-api")
JWT_TTL = int(env.get("JWT_TTL_S", "3600"))
TTL_MIN = int(env.get("CACHE_TTL_MIN_MS", "45000"))
TTL_MAX = int(env.get("CACHE_TTL_MAX_MS", "60000"))
TTL_BINS = max(1, int(env.get("CACHE_TTL_BINS", "15")))
TTL_W = max(1, (TTL_MAX - TTL_MIN) // TTL_BINS)
CACHE_BYTES = int(env.get("CACHE_MAX_MB", "64")) * 1024 * 1024 // WORKERS
UUID_MAX = "ffffffff-ffff-ffff-ffff-ffffffffffff"
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

# ---------- SQL (identical in every app) ----------

POST_SELECT = (
    "SELECT p.id, p.user_id, u.username, u.display_name, u.avatar_url, u.is_verified, p.reply_to_id, "
    "p.title, left(p.body, 210) AS preview, p.lang, p.media_url, p.like_count, p.comment_count, "
    "p.share_count, (extract(epoch FROM p.created_at)*1000)::int8 AS created_ms "
    "FROM posts p JOIN users u ON u.id = p.user_id "
)
SQL_PUBLIC = POST_SELECT + "WHERE p.visibility = 0 AND p.deleted_at IS NULL AND p.id < $1 ORDER BY p.id DESC LIMIT 5"
SQL_PRIVATE = POST_SELECT + "WHERE p.user_id = $1 AND p.visibility = 1 AND p.deleted_at IS NULL AND p.id < $2 ORDER BY p.id DESC LIMIT 5"
SQL_MESSAGES = (
    "SELECT m.id, m.conversation_id, m.sender_id, u.username AS sender_username, "
    "u.display_name AS sender_display_name, u.avatar_url AS sender_avatar_url, m.content_type, "
    "left(m.body, 240) AS preview, m.attachment_url, (extract(epoch FROM m.created_at)*1000)::int8 AS created_ms, "
    "(extract(epoch FROM m.read_at)*1000)::int8 AS read_ms "
    "FROM messages m JOIN users u ON u.id = m.sender_id "
    "WHERE m.recipient_id = $1 AND m.deleted_at IS NULL AND m.id < $2 ORDER BY m.id DESC LIMIT 5"
)
SQL_USER_EXISTS = "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)"

POSTS_UNNEST = (
    "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int2[], $5::text[], $6::text[], $7::text[], $8::text[]) "
    "AS t(id, user_id, reply_to_id, visibility, title, body, lang, media_url)"
)
MESSAGES_UNNEST = (
    "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::int2[], $6::text[], $7::text[]) "
    "AS t(id, new_conv_id, sender_id, recipient_id, content_type, body, attachment_url)"
)
LIKES_UNNEST = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS t(id, user_id, post_id)"

# group 2 (/n): same write SQL fed one row from scalar parameters
POSTS_ONE = (
    "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS reply_to_id, $4::int2 AS visibility, "
    "$5::text AS title, $6::text AS body, $7::text AS lang, $8::text AS media_url"
)
MESSAGES_ONE = (
    "SELECT $1::uuid AS id, $2::uuid AS new_conv_id, $3::uuid AS sender_id, $4::uuid AS recipient_id, "
    "$5::int2 AS content_type, $6::text AS body, $7::text AS attachment_url"
)
LIKES_ONE = "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS post_id"


def sql_posts(src):
    return (
        "WITH raw AS (" + src + "), "
        "ins AS ( "
        "INSERT INTO posts (id, user_id, reply_to_id, visibility, title, body, lang, media_url, like_count, comment_count, "
        "share_count, view_count, is_edited, created_at, updated_at, deleted_at) "
        "SELECT r.id, r.user_id, r.reply_to_id, r.visibility, r.title, r.body, r.lang, r.media_url, 0, 0, 0, 0, false, "
        "uuid_extract_timestamp(r.id), uuid_extract_timestamp(r.id), NULL "
        "FROM raw r "
        "WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) "
        "AND (r.reply_to_id IS NULL OR EXISTS (SELECT 1 FROM posts p WHERE p.id = r.reply_to_id)) "
        "ON CONFLICT (id) DO NOTHING "
        "RETURNING user_id, reply_to_id), "
        "by_user AS ( "
        "UPDATE users SET posts_count = users.posts_count + c.n "
        "FROM (SELECT user_id, count(*) AS n FROM ins GROUP BY user_id ORDER BY user_id) c "
        "WHERE users.id = c.user_id) "
        "UPDATE posts SET comment_count = posts.comment_count + c.n "
        "FROM (SELECT reply_to_id, count(*) AS n FROM ins WHERE reply_to_id IS NOT NULL GROUP BY reply_to_id ORDER BY reply_to_id) c "
        "WHERE posts.id = c.reply_to_id"
    )


def sql_messages(src):
    return (
        "WITH raw AS (" + src + "), "
        "v AS ( "
        "SELECT r.*, least(r.sender_id, r.recipient_id) AS ua, greatest(r.sender_id, r.recipient_id) AS ub "
        "FROM raw r "
        "WHERE r.sender_id <> r.recipient_id "
        "AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.recipient_id) "
        "AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.sender_id)), "
        "last AS ( "
        "SELECT DISTINCT ON (ua, ub) ua, ub, new_conv_id, id, left(body, 100) AS preview, "
        "count(*) OVER (PARTITION BY ua, ub) AS n "
        "FROM v ORDER BY ua, ub, id DESC), "
        "conv AS ( "
        "INSERT INTO conversations AS c (id, user_a_id, user_b_id, last_message_id, last_message_preview, "
        "last_message_at, message_count, created_at, updated_at) "
        "SELECT new_conv_id, ua, ub, id, preview, uuid_extract_timestamp(id), n, "
        "uuid_extract_timestamp(new_conv_id), uuid_extract_timestamp(id) "
        "FROM last "
        "ON CONFLICT (user_a_id, user_b_id) DO UPDATE SET "
        "last_message_id = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_id ELSE c.last_message_id END, "
        "last_message_preview = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_preview ELSE c.last_message_preview END, "
        "last_message_at = greatest(EXCLUDED.last_message_at, c.last_message_at), "
        "message_count = c.message_count + EXCLUDED.message_count, "
        "updated_at = greatest(EXCLUDED.updated_at, c.updated_at) "
        "RETURNING c.id, c.user_a_id, c.user_b_id) "
        "INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content_type, body, attachment_url, "
        "created_at, read_at, edited_at, deleted_at) "
        "SELECT v.id, conv.id, v.sender_id, v.recipient_id, v.content_type, v.body, v.attachment_url, "
        "uuid_extract_timestamp(v.id), NULL, NULL, NULL "
        "FROM v JOIN conv ON conv.user_a_id = v.ua AND conv.user_b_id = v.ub "
        "ON CONFLICT (id) DO NOTHING"
    )


def sql_likes(src):
    return (
        "WITH raw AS (" + src + "), "
        "ins AS ( "
        "INSERT INTO likes (id, user_id, post_id, created_at) "
        "SELECT r.id, r.user_id, r.post_id, uuid_extract_timestamp(r.id) "
        "FROM raw r "
        "WHERE EXISTS (SELECT 1 FROM posts p WHERE p.id = r.post_id) "
        "AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) "
        "ON CONFLICT DO NOTHING "
        "RETURNING post_id) "
        "UPDATE posts SET like_count = posts.like_count + c.n "
        "FROM (SELECT post_id, count(*) AS n FROM ins GROUP BY post_id ORDER BY post_id) c "
        "WHERE posts.id = c.post_id"
    )


N_POSTS = sql_posts(POSTS_ONE)
N_MESSAGES = sql_messages(MESSAGES_ONE)
N_LIKES = sql_likes(LIKES_ONE)


# ---------- helpers ----------


def fmt_uuid(h):
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def uuid_decode(b):
    return fmt_uuid(b.hex())


def uuid_encode(s):
    return bytes.fromhex(s.replace("-", ""))


_rand = b""
_rand_i = 0
_last_ms = 0
_seq = 0


def _random8():
    global _rand, _rand_i
    if _rand_i >= len(_rand):
        _rand, _rand_i = os.urandom(8 * 512), 0
    i = _rand_i
    _rand_i += 8
    return int.from_bytes(_rand[i : i + 8])


def uuid7():
    """RFC 9562 UUIDv7: 48-bit ms | ver 7 | 12-bit monotonic seq | var 10 | 62 random bits."""
    global _last_ms, _seq
    ms = time.time_ns() // 1_000_000
    if ms > _last_ms:
        _last_ms, _seq = ms, _random8() & 0x7FF
    else:
        _seq += 1
        if _seq > 0xFFF:
            _last_ms, _seq = _last_ms + 1, 0
    v = (_last_ms << 80) | (0x7 << 76) | (_seq << 64) | 0x8000000000000000 | (_random8() & 0x3FFFFFFFFFFFFFFF)
    return fmt_uuid(f"{v:032x}")


def parse_uuid(s):
    return s.lower() if type(s) is str and len(s) == 36 and UUID_RE.match(s) else None


def key32(u):
    """Lane key: last 4 bytes of a UUID (random part of a v7 id)."""
    return int(u[-8:], 16)


def len_ok(s, lo, hi):
    return type(s) is str and lo <= len(s) <= hi


def opt_len_ok(s, lo, hi):
    return s is None or len_ok(s, lo, hi)


def ttl_seconds(key):
    """ttl_ms = MIN + (h % BINS) * W + ((h >> 32) % W), h = FNV-1a 64("public:" + uuid)."""
    h = 0xCBF29CE484222325
    for b in ("public:" + key).encode():
        h = ((h ^ b) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return (TTL_MIN + (h % TTL_BINS) * TTL_W + ((h >> 32) % TTL_W)) / 1000.0


def page(rows):
    items = [dict(r) for r in rows]
    return orjson.dumps({"items": items, "next": items[4]["id"] if len(items) == 5 else None})


JSON = "application/json"
OK = b'{"ok":true}'
ERR = {
    400: b'{"error":"bad_request"}',
    401: b'{"error":"unauthorized"}',
    404: b'{"error":"not_found"}',
    413: b'{"error":"too_large"}',
    500: b'{"error":"db"}',
    503: b'{"error":"busy"}',
}


def fail(code):
    return Response(ERR[code], code, media_type=JSON)


def done(code):
    return Response(OK, 200, media_type=JSON) if code == 200 else fail(code)


def auth(request):
    h = request.headers.get("authorization")
    if not h or not h.startswith("Bearer "):
        return None
    try:
        c = jwt.decode(
            h[7:],
            SECRET,
            algorithms=["HS256"],
            audience=AUD,
            issuer=ISS,
            leeway=0,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except jwt.PyJWTError:
        return None
    return parse_uuid(c.get("sub"))


def cursor(request):
    v = request.query_params.get("before")
    return UUID_MAX if v is None else parse_uuid(v)


async def read_json(request):
    """Returns (dict, 0) or (None, http_code). Body limit 16 KB."""
    cl = request.headers.get("content-length")
    if cl is not None and cl.isdigit() and int(cl) > BODY_LIMIT:
        return None, 413
    body = await request.body()
    if len(body) > BODY_LIMIT:
        return None, 413
    try:
        v = orjson.loads(body)
    except orjson.JSONDecodeError:
        return None, 400
    return (v, 0) if type(v) is dict else (None, 400)


def parse_post(uid, b):
    """Shared /posts + /n/posts validation: (uid, reply_to, visibility, title, body, lang, media_url) or None."""
    title, text, vis = b.get("title"), b.get("body"), b.get("visibility")
    lang = b.get("lang")
    if lang is None:
        lang = "en"
    media, reply = b.get("media_url"), b.get("reply_to")
    if not (len_ok(title, 1, 200) and len_ok(text, 1, 8000) and type(vis) is int and vis in (0, 1)
            and len_ok(lang, 2, 8) and opt_len_ok(media, 1, 500)):
        return None
    if reply is not None:
        reply = parse_uuid(reply)
        if reply is None:
            return None
    return (uid, reply, vis, title, text, lang, media)


def parse_message(uid, b):
    """Shared /messages + /n/messages validation: (uid, to, content_type, body, attachment_url) or None."""
    to, text, ct, att = parse_uuid(b.get("to")), b.get("body"), b.get("content_type"), b.get("attachment_url")
    if ct is None:
        ct = 0
    if to is None or to == uid or not len_ok(text, 1, 8000) or type(ct) is not int or not 0 <= ct <= 3 \
            or not opt_len_ok(att, 1, 500):
        return None
    return (uid, to, ct, text, att)


# ---------- batcher: LANES queues per table -> column lists -> one transaction per batch ----------


def batch_retryable(e):
    """Deadlock, serialization, statement timeout, too many connections, server shutdown, connection
    failures (class 08) and errors without a SQLSTATE (socket / pool / driver connection errors)."""
    s = getattr(e, "sqlstate", None)
    return s is None or s in RETRY_STATES or s.startswith("08")


class Lane:
    """One queue + one writer; the window runs from the arrival of the batch's first row."""

    def __init__(self, b):
        self.b = b
        self.q = []
        self.busy = False
        self.timer = None

    def add(self, job):
        self.q.append(job)
        if not self.busy:
            if len(self.q) >= BATCH_MAX:
                if self.timer:
                    self.timer.cancel()
                self._spawn()
            elif self.timer is None:
                self._arm()

    def _arm(self):
        delay = max(0.0, self.q[0][2] + WINDOW - time.monotonic())
        self.timer = asyncio.get_running_loop().call_later(delay, self._spawn)

    def _spawn(self):
        self.timer = None
        self.busy = True
        t = asyncio.create_task(self.run())
        self.b.tasks.add(t)
        t.add_done_callback(self.b.tasks.discard)

    async def run(self):
        batch = self.q[:BATCH_MAX]
        del self.q[:BATCH_MAX]
        # ids are generated here, once: a retry re-sends the same ids (ON CONFLICT (id) DO NOTHING)
        rows = [self.b.to_row(r) for r, _, _ in batch]
        t0 = time.perf_counter()
        ok = await self.b.write_retry(rows)
        self.b.stat(len(rows), (time.perf_counter() - t0) * 1000)
        # ack only after the transaction committed (or failed for good)
        code = 200 if ok else 500
        for _, fut, _ in batch:
            if not fut.done():
                fut.set_result(code)
        self.busy = False
        if len(self.q) >= BATCH_MAX:
            self._spawn()
        elif self.q:
            self._arm()


class Batcher:
    def __init__(self, name, unnest_src, sql_fn, to_row):
        self.name = name
        self.sql = sql_fn(unnest_src)
        self.to_row = to_row  # request row -> full tuple (with fresh uuidv7 ids)
        self.lanes = [Lane(self) for _ in range(LANES)]
        self.tasks = set()
        self.st = [0, 0, 0.0, 0.0, 0, 0]  # flushes, rows, ms, max ms, retries, fails

    def submit(self, row, key):
        """Queues the row on the lane of `key`; full lane queue -> 503 busy."""
        fut = asyncio.get_running_loop().create_future()
        lane = self.lanes[key % LANES]
        if len(lane.q) >= LANE_CAP:
            fut.set_result(503)
            return fut
        lane.add((row, fut, time.monotonic()))
        return fut

    def stat(self, rows, ms):
        s = self.st
        s[0] += 1
        s[1] += rows
        s[2] += ms
        s[3] = max(s[3], ms)

    async def report(self):
        while True:
            await asyncio.sleep(10)
            f, rows, ms, mx, re_, fa = self.st
            self.st = [0, 0, 0.0, 0.0, 0, 0]
            queued = sum(len(lane.q) for lane in self.lanes)
            if f or queued:
                print(f"batch-stats {self.name} pid={os.getpid()} flushes={f} rows={rows} avg_rows={rows / max(1, f):.0f} "
                      f"avg_flush_ms={ms / max(1, f):.1f} max_flush_ms={mx:.1f} queued={queued} retries={re_} fails={fa}",
                      flush=True)

    async def write(self, rows):
        await pool.execute(self.sql, *(list(col) for col in zip(*rows)))

    async def write_retry(self, rows):
        """One upsert transaction per batch; transient errors retried with backoff, then fail loudly."""
        for attempt in range(1, WRITE_ATTEMPTS + 1):
            try:
                await self.write(rows)
                return True
            except Exception as e:  # noqa: BLE001 — every failure maps to 500 for the waiting requests
                if attempt < WRITE_ATTEMPTS and batch_retryable(e):
                    self.st[4] += 1
                    print(f"batch {self.name} attempt {attempt} failed, retrying: {e!r}", flush=True)
                    await asyncio.sleep(0.05 * (1 << (attempt - 1)))
                    continue
                self.st[5] += 1
                print(f"BATCH FAILED {self.name} rows={len(rows)} after {attempt} attempt(s): {e!r}", flush=True)
                return False
        return False


posts = Batcher("posts", POSTS_UNNEST, sql_posts, lambda r: (uuid7(), *r))
messages = Batcher("messages", MESSAGES_UNNEST, sql_messages, lambda r: (uuid7(), uuid7(), *r))
likes = Batcher("likes", LIKES_UNNEST, sql_likes, lambda r: (uuid7(), *r))

# ---------- cache ----------

cache = TLRUCache(
    maxsize=CACHE_BYTES,
    ttu=lambda _k, _v, now: now + ttl_seconds(_k),
    timer=time.monotonic,
    getsizeof=len,
)
inflight = {}

pool = None


async def init_conn(conn):
    await conn.set_type_codec("uuid", schema="pg_catalog", encoder=uuid_encode, decoder=uuid_decode, format="binary")


async def _no_reset(conn):
    # skip asyncpg's per-release Connection.reset() queries (no session state is changed by this app)
    return None


def _versions():
    def v(name):
        try:
            return pkg_version(name)
        except Exception:  # noqa: BLE001
            return "?"
    pkgs = " ".join(f"{n}={v(n)}" for n in ("asyncpg", "uvloop", "orjson", "fastapi", "uvicorn"))
    return f"python={sys.version.split()[0]} {pkgs}"


@asynccontextmanager
async def lifespan(_app):
    global pool
    pool = await asyncpg.create_pool(
        env["DATABASE_URL"], min_size=POOL, max_size=POOL, init=init_conn, reset=_no_reset,
        server_settings={"statement_timeout": str(STMT_TIMEOUT_MS)} if STMT_TIMEOUT_MS > 0 else None,
    )
    reporters = [asyncio.create_task(b.report()) for b in (posts, messages, likes)] if STATS else []
    print(f"fastapi worker {os.getpid()} pool={POOL} batch_rows={BATCH_MAX} "
          f"batch_window_ms={int(WINDOW * 1000)} lanes={LANES} lane_queue={LANE_CAP} stmt_timeout_ms={STMT_TIMEOUT_MS} "
          f"{_versions()}", flush=True)
    yield
    for t in reporters:
        t.cancel()
    await pool.close()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/health")
@app.get("/n/health")
async def health():
    return Response(b'{"ok":true,"framework":"fastapi"}', media_type=JSON)


@app.post("/auth/token")
async def token(request: Request):
    if not hmac.compare_digest(request.headers.get("x-issuer-key", "").encode(), ISSUER_KEY):
        return fail(401)
    body, code = await read_json(request)
    if code:
        return fail(code)
    uid = parse_uuid(body.get("user_id"))
    if uid is None:
        return fail(400)
    try:
        exists = await pool.fetchval(SQL_USER_EXISTS, uid)
    except Exception:  # noqa: BLE001
        return fail(500)
    if not exists:
        return fail(404)
    now = int(time.time())
    tok = jwt.encode({"sub": uid, "iss": ISS, "aud": AUD, "iat": now, "exp": now + JWT_TTL}, SECRET, algorithm="HS256")
    return Response(orjson.dumps({"token": tok, "expires_in": JWT_TTL}), media_type=JSON)


@app.get("/posts/public")
async def public(request: Request):
    key = cursor(request)
    if key is None:
        return fail(400)
    b = cache.get(key)
    if b is not None:
        return Response(b, media_type=JSON, headers={"x-cache": "hit"})
    fut = inflight.get(key)
    if fut is not None:  # another request is already loading this page
        try:
            b = await asyncio.shield(fut)
        except Exception:  # noqa: BLE001
            return fail(500)
    else:
        fut = inflight[key] = asyncio.get_running_loop().create_future()
        try:
            b = page(await pool.fetch(SQL_PUBLIC, key))
            cache[key] = b
            fut.set_result(b)
        except Exception as e:  # noqa: BLE001
            fut.set_exception(e)
            fut.exception()  # mark retrieved when nobody else is waiting
            return fail(500)
        finally:
            del inflight[key]
            if not fut.done():  # leader cancelled (client went away): release waiters
                fut.set_exception(RuntimeError("leader cancelled"))
                fut.exception()
    return Response(b, media_type=JSON, headers={"x-cache": "miss"})


@app.get("/posts/private")
@app.get("/n/posts/private")
async def private(request: Request):
    uid = auth(request)
    if uid is None:
        return fail(401)
    before = cursor(request)
    if before is None:
        return fail(400)
    try:
        return Response(page(await pool.fetch(SQL_PRIVATE, uid, before)), media_type=JSON)
    except Exception:  # noqa: BLE001
        return fail(500)


@app.get("/messages")
@app.get("/n/messages")
async def list_messages(request: Request):
    uid = auth(request)
    if uid is None:
        return fail(401)
    before = cursor(request)
    if before is None:
        return fail(400)
    try:
        return Response(page(await pool.fetch(SQL_MESSAGES, uid, before)), media_type=JSON)
    except Exception:  # noqa: BLE001
        return fail(500)


@app.post("/posts")
async def create_post(request: Request):
    uid = auth(request)
    if uid is None:
        return fail(401)
    b, code = await read_json(request)
    if code:
        return fail(code)
    row = parse_post(uid, b)
    if row is None:
        return fail(400)
    return done(await posts.submit(row, key32(uid)))


@app.post("/messages")
async def send_message(request: Request):
    uid = auth(request)
    if uid is None:
        return fail(401)
    b, code = await read_json(request)
    if code:
        return fail(code)
    row = parse_message(uid, b)
    if row is None:
        return fail(400)
    # symmetric key: both directions of a conversation share one lane
    return done(await messages.submit(row, key32(uid) ^ key32(row[1])))


@app.post("/posts/{post_id}/like")
async def like(request: Request, post_id: str):
    uid = auth(request)
    if uid is None:
        return fail(401)
    pid = parse_uuid(post_id)
    if pid is None:
        return fail(400)
    return done(await likes.submit((uid, pid), key32(pid)))


# ---------- group 2 ("normal", prefix /n): no batching, no cache ----------


async def exec_one(name, sql, *args):
    """One write statement on the pool; retried on deadlock / serialization failure like the batcher."""
    for attempt in range(3):
        try:
            await pool.execute(sql, *args)
            return done(200)
        except Exception as e:  # noqa: BLE001
            if attempt < 2 and getattr(e, "sqlstate", None) in ("40P01", "40001"):
                continue
            print(f"write {name} failed: {e}", flush=True)
            break
    return fail(500)


@app.get("/n/posts/public")
async def n_public(request: Request):
    key = cursor(request)
    if key is None:
        return fail(400)
    try:
        return Response(page(await pool.fetch(SQL_PUBLIC, key)), media_type=JSON)
    except Exception:  # noqa: BLE001
        return fail(500)


@app.post("/n/posts")
async def n_create_post(request: Request):
    uid = auth(request)
    if uid is None:
        return fail(401)
    b, code = await read_json(request)
    if code:
        return fail(code)
    row = parse_post(uid, b)
    if row is None:
        return fail(400)
    return await exec_one("posts", N_POSTS, uuid7(), *row)


@app.post("/n/messages")
async def n_send_message(request: Request):
    uid = auth(request)
    if uid is None:
        return fail(401)
    b, code = await read_json(request)
    if code:
        return fail(code)
    row = parse_message(uid, b)
    if row is None:
        return fail(400)
    return await exec_one("messages", N_MESSAGES, uuid7(), uuid7(), *row)


@app.post("/n/posts/{post_id}/like")
async def n_like(request: Request, post_id: str):
    uid = auth(request)
    if uid is None:
        return fail(401)
    pid = parse_uuid(post_id)
    if pid is None:
        return fail(400)
    return await exec_one("likes", N_LIKES, uuid7(), uid, pid)
