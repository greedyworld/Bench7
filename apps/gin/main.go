// bench7 — Gin implementation (gin + pgx/v5 + ristretto/v2 + golang-jwt/v5).
// Same endpoints, SQL, cache, batcher and JWT rules as the other apps.
//   - ids: UUIDv7 generated here; created_at = uuid_extract_timestamp(id)
//   - cache TTL: deterministic hash bucketing (spreads expiries, no thundering herd)
//   - writes: per-table batcher, lanes keyed by user / conversation / post -> column slices ->
//     one upsert transaction per batch (unnest); the reply is sent after commit
package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/gin-gonic/gin"
	json "github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

const (
	pageSize  = 5
	bodyLimit = 16 * 1024
	// write attempts per batch (backoff 50, 100, 200 ms between them)
	writeAttempts = 4
)

// ---------- SQL (identical in every app) ----------

const postSelect = "SELECT p.id, p.user_id, u.username, u.display_name, u.avatar_url, u.is_verified, p.reply_to_id, " +
	"p.title, left(p.body, 210) AS preview, p.lang, p.media_url, p.like_count, p.comment_count, " +
	"p.share_count, (extract(epoch FROM p.created_at)*1000)::int8 AS created_ms " +
	"FROM posts p JOIN users u ON u.id = p.user_id "
const sqlPublic = postSelect + "WHERE p.visibility = 0 AND p.deleted_at IS NULL AND p.id < $1 ORDER BY p.id DESC LIMIT 5"
const sqlPrivate = postSelect + "WHERE p.user_id = $1 AND p.visibility = 1 AND p.deleted_at IS NULL AND p.id < $2 ORDER BY p.id DESC LIMIT 5"
const sqlMessages = "SELECT m.id, m.conversation_id, m.sender_id, u.username AS sender_username, " +
	"u.display_name AS sender_display_name, u.avatar_url AS sender_avatar_url, m.content_type, " +
	"left(m.body, 240) AS preview, m.attachment_url, (extract(epoch FROM m.created_at)*1000)::int8 AS created_ms, " +
	"(extract(epoch FROM m.read_at)*1000)::int8 AS read_ms " +
	"FROM messages m JOIN users u ON u.id = m.sender_id " +
	"WHERE m.recipient_id = $1 AND m.deleted_at IS NULL AND m.id < $2 ORDER BY m.id DESC LIMIT 5"
const sqlUserExists = "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)"

const postsUnnest = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int2[], $5::text[], $6::text[], $7::text[], $8::text[]) " +
	"AS t(id, user_id, reply_to_id, visibility, title, body, lang, media_url)"
const messagesUnnest = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::int2[], $6::text[], $7::text[]) " +
	"AS t(id, new_conv_id, sender_id, recipient_id, content_type, body, attachment_url)"
const likesUnnest = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS t(id, user_id, post_id)"

// group 2 ("normal", prefix /n): same SQL as the batch writers, fed one row from scalar parameters.
const postsOne = "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS reply_to_id, $4::int2 AS visibility, " +
	"$5::text AS title, $6::text AS body, $7::text AS lang, $8::text AS media_url"
const messagesOne = "SELECT $1::uuid AS id, $2::uuid AS new_conv_id, $3::uuid AS sender_id, $4::uuid AS recipient_id, " +
	"$5::int2 AS content_type, $6::text AS body, $7::text AS attachment_url"
const likesOne = "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS post_id"

func sqlPosts(src string) string {
	return "WITH raw AS (" + src + "), " +
		"ins AS ( " +
		"INSERT INTO posts (id, user_id, reply_to_id, visibility, title, body, lang, media_url, like_count, comment_count, " +
		"share_count, view_count, is_edited, created_at, updated_at, deleted_at) " +
		"SELECT r.id, r.user_id, r.reply_to_id, r.visibility, r.title, r.body, r.lang, r.media_url, 0, 0, 0, 0, false, " +
		"uuid_extract_timestamp(r.id), uuid_extract_timestamp(r.id), NULL " +
		"FROM raw r " +
		"WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) " +
		"AND (r.reply_to_id IS NULL OR EXISTS (SELECT 1 FROM posts p WHERE p.id = r.reply_to_id)) " +
		"ON CONFLICT (id) DO NOTHING " +
		"RETURNING user_id, reply_to_id), " +
		"by_user AS ( " +
		"UPDATE users SET posts_count = users.posts_count + c.n " +
		"FROM (SELECT user_id, count(*) AS n FROM ins GROUP BY user_id ORDER BY user_id) c " +
		"WHERE users.id = c.user_id) " +
		"UPDATE posts SET comment_count = posts.comment_count + c.n " +
		"FROM (SELECT reply_to_id, count(*) AS n FROM ins WHERE reply_to_id IS NOT NULL GROUP BY reply_to_id ORDER BY reply_to_id) c " +
		"WHERE posts.id = c.reply_to_id"
}

func sqlMessagesBatch(src string) string {
	return "WITH raw AS (" + src + "), " +
		"v AS ( " +
		"SELECT r.*, least(r.sender_id, r.recipient_id) AS ua, greatest(r.sender_id, r.recipient_id) AS ub " +
		"FROM raw r " +
		"WHERE r.sender_id <> r.recipient_id " +
		"AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.recipient_id) " +
		"AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.sender_id)), " +
		"last AS ( " +
		"SELECT DISTINCT ON (ua, ub) ua, ub, new_conv_id, id, left(body, 100) AS preview, " +
		"count(*) OVER (PARTITION BY ua, ub) AS n " +
		"FROM v ORDER BY ua, ub, id DESC), " +
		"conv AS ( " +
		"INSERT INTO conversations AS c (id, user_a_id, user_b_id, last_message_id, last_message_preview, " +
		"last_message_at, message_count, created_at, updated_at) " +
		"SELECT new_conv_id, ua, ub, id, preview, uuid_extract_timestamp(id), n, " +
		"uuid_extract_timestamp(new_conv_id), uuid_extract_timestamp(id) " +
		"FROM last " +
		"ON CONFLICT (user_a_id, user_b_id) DO UPDATE SET " +
		"last_message_id = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_id ELSE c.last_message_id END, " +
		"last_message_preview = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_preview ELSE c.last_message_preview END, " +
		"last_message_at = greatest(EXCLUDED.last_message_at, c.last_message_at), " +
		"message_count = c.message_count + EXCLUDED.message_count, " +
		"updated_at = greatest(EXCLUDED.updated_at, c.updated_at) " +
		"RETURNING c.id, c.user_a_id, c.user_b_id) " +
		"INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content_type, body, attachment_url, " +
		"created_at, read_at, edited_at, deleted_at) " +
		"SELECT v.id, conv.id, v.sender_id, v.recipient_id, v.content_type, v.body, v.attachment_url, " +
		"uuid_extract_timestamp(v.id), NULL, NULL, NULL " +
		"FROM v JOIN conv ON conv.user_a_id = v.ua AND conv.user_b_id = v.ub " +
		"ON CONFLICT (id) DO NOTHING"
}

func sqlLikes(src string) string {
	return "WITH raw AS (" + src + "), " +
		"ins AS ( " +
		"INSERT INTO likes (id, user_id, post_id, created_at) " +
		"SELECT r.id, r.user_id, r.post_id, uuid_extract_timestamp(r.id) " +
		"FROM raw r " +
		"WHERE EXISTS (SELECT 1 FROM posts p WHERE p.id = r.post_id) " +
		"AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) " +
		"ON CONFLICT DO NOTHING " +
		"RETURNING post_id) " +
		"UPDATE posts SET like_count = posts.like_count + c.n " +
		"FROM (SELECT post_id, count(*) AS n FROM ins GROUP BY post_id ORDER BY post_id) c " +
		"WHERE posts.id = c.post_id"
}

// ---------- helpers ----------

func envOr(k, d string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func pgUUID(u uuid.UUID) pgtype.UUID { return pgtype.UUID{Bytes: u, Valid: true} }

// canonical hyphenated UUID only (36 chars)
func parseUUID(s string) (uuid.UUID, bool) {
	if len(s) != 36 {
		return uuid.Nil, false
	}
	u, err := uuid.Parse(s)
	return u, err == nil
}

func lenOK(s string, min, max int) bool {
	if len(s) < min || len(s) > max*4 {
		return false
	}
	n := utf8.RuneCountInString(s)
	return n >= min && n <= max
}

func optLenOK(s *string, min, max int) bool { return s == nil || lenOK(*s, min, max) }

var uuidMax = uuid.UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// ---------- cache TTL: deterministic hash bucketing ----------
// ttl_ms = MIN + (h % BINS) * W + ((h >> 32) % W), W = (MAX - MIN) / BINS, h = FNV-1a 64("public:" + uuid)

type hashTTL struct{ min, width, bins uint64 }

func (t hashTTL) ttl(key string) time.Duration {
	h := uint64(0xcbf29ce484222325)
	for _, b := range []byte("public:" + key) {
		h ^= uint64(b)
		h *= 0x100000001b3
	}
	return time.Duration(t.min+(h%t.bins)*t.width+((h>>32)%t.width)) * time.Millisecond
}

// ---------- column batches ----------

type newPost struct {
	uid        uuid.UUID
	replyTo    pgtype.UUID
	visibility int16
	title      string
	body       string
	lang       string
	mediaURL   *string
}

type newMessage struct {
	uid, to     uuid.UUID
	contentType int16
	body        string
	attachment  *string
}

type newLike struct{ uid, postID uuid.UUID }

// table describes how a batch of rows becomes column slices for unnest.
type table[R any] struct {
	name   string
	unnest func(rows []R) []any
}

var postsTable = table[newPost]{
	name: "posts",
	unnest: func(rows []newPost) []any {
		n := len(rows)
		id, uid, reply := make([]pgtype.UUID, n), make([]pgtype.UUID, n), make([]pgtype.UUID, n)
		vis := make([]int16, n)
		title, body, lang, media := make([]string, n), make([]string, n), make([]string, n), make([]*string, n)
		for i, r := range rows {
			id[i], uid[i], reply[i], vis[i] = pgUUID(uuid.Must(uuid.NewV7())), pgUUID(r.uid), r.replyTo, r.visibility
			title[i], body[i], lang[i], media[i] = r.title, r.body, r.lang, r.mediaURL
		}
		return []any{id, uid, reply, vis, title, body, lang, media}
	},
}

var messagesTable = table[newMessage]{
	name: "messages",
	unnest: func(rows []newMessage) []any {
		n := len(rows)
		id, conv, snd, rcp := make([]pgtype.UUID, n), make([]pgtype.UUID, n), make([]pgtype.UUID, n), make([]pgtype.UUID, n)
		ct := make([]int16, n)
		body, att := make([]string, n), make([]*string, n)
		for i, r := range rows {
			id[i], conv[i], snd[i], rcp[i] = pgUUID(uuid.Must(uuid.NewV7())), pgUUID(uuid.Must(uuid.NewV7())), pgUUID(r.uid), pgUUID(r.to)
			ct[i], body[i], att[i] = r.contentType, r.body, r.attachment
		}
		return []any{id, conv, snd, rcp, ct, body, att}
	},
}

var likesTable = table[newLike]{
	name: "likes",
	unnest: func(rows []newLike) []any {
		n := len(rows)
		id, uid, pid := make([]pgtype.UUID, n), make([]pgtype.UUID, n), make([]pgtype.UUID, n)
		for i, r := range rows {
			id[i], uid[i], pid[i] = pgUUID(uuid.Must(uuid.NewV7())), pgUUID(r.uid), pgUUID(r.postID)
		}
		return []any{id, uid, pid}
	},
}

// ---------- batcher: BATCH_LANES queues + writer goroutines per table ----------

type job[R any] struct {
	row  R
	done chan int
	at   time.Time // the batch window runs from the arrival of its first row
}

type batchCfg struct {
	maxRows, lanes, queueMax int
	window                   time.Duration
	stats                    bool
}

type batchStats struct{ flushes, rows, flushUs, maxUs, retries, fails atomic.Int64 }

type batcher[R any] struct{ lanes []chan job[R] }

// laneKey: last 4 bytes of a UUID (random part of a v7 id).
func laneKey(u uuid.UUID) uint32 {
	return uint32(u[12])<<24 | uint32(u[13])<<16 | uint32(u[14])<<8 | uint32(u[15])
}

// batchRetryable: deadlock, serialization, statement timeout, too many connections, server shutdown,
// connection failures (class 08) and errors without a SQLSTATE (I/O, pool, connect errors).
func batchRetryable(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return true
	}
	switch pe.Code {
	case "40P01", "40001", "57014", "53300", "57P01", "57P02", "57P03":
		return true
	}
	return strings.HasPrefix(pe.Code, "08")
}

func newBatcher[R any](pool *pgxpool.Pool, t table[R], sql string, cfg batchCfg) *batcher[R] {
	b := &batcher[R]{lanes: make([]chan job[R], cfg.lanes)}
	st := &batchStats{}
	// transient errors are retried with backoff; after the last attempt the batch fails loudly
	writeRetry := func(rows []R) bool {
		// ids are generated here, once: a retry re-sends the same ids (ON CONFLICT (id) DO NOTHING)
		args := t.unnest(rows)
		for attempt := 1; ; attempt++ {
			_, err := pool.Exec(context.Background(), sql, args...)
			if err == nil {
				return true
			}
			if attempt < writeAttempts && batchRetryable(err) {
				st.retries.Add(1)
				log.Printf("batch %s attempt %d failed, retrying: %v", t.name, attempt, err)
				time.Sleep(time.Duration(50<<(attempt-1)) * time.Millisecond)
				continue
			}
			st.fails.Add(1)
			log.Printf("BATCH FAILED %s rows=%d after %d attempt(s): %v", t.name, len(rows), attempt, err)
			return false
		}
	}
	perLane := max(1, cfg.queueMax/cfg.lanes)
	for i := range b.lanes {
		ch := make(chan job[R], perLane)
		b.lanes[i] = ch
		go func() {
			rows := make([]R, 0, cfg.maxRows)
			waiters := make([]chan int, 0, cfg.maxRows)
			timer := time.NewTimer(time.Hour)
			timer.Stop()
			for j := range ch {
				rows, waiters = append(rows[:0], j.row), append(waiters[:0], j.done)
				timer.Reset(time.Until(j.at.Add(cfg.window)))
			collect:
				for len(rows) < cfg.maxRows {
					select { // queued rows first, even when the window already ran out
					case j := <-ch:
						rows, waiters = append(rows, j.row), append(waiters, j.done)
						continue
					default:
					}
					select {
					case j := <-ch:
						rows, waiters = append(rows, j.row), append(waiters, j.done)
					case <-timer.C:
						break collect
					}
				}
				timer.Stop()
				t0 := time.Now()
				code := http.StatusInternalServerError
				if writeRetry(rows) {
					code = http.StatusOK
				}
				us := time.Since(t0).Microseconds()
				st.flushes.Add(1)
				st.rows.Add(int64(len(rows)))
				st.flushUs.Add(us)
				for m := st.maxUs.Load(); us > m && !st.maxUs.CompareAndSwap(m, us); m = st.maxUs.Load() {
				}
				// ack only after the transaction committed (or failed for good)
				for _, w := range waiters {
					w <- code
				}
			}
		}()
	}
	if cfg.stats {
		go func() {
			for range time.Tick(10 * time.Second) {
				f, rows, us, mx := st.flushes.Swap(0), st.rows.Swap(0), st.flushUs.Swap(0), st.maxUs.Swap(0)
				re, fa := st.retries.Swap(0), st.fails.Swap(0)
				queued := 0
				for _, ch := range b.lanes {
					queued += len(ch)
				}
				if f > 0 || queued > 0 {
					fm := float64(max(1, f))
					log.Printf("batch-stats %s flushes=%d rows=%d avg_rows=%.0f avg_flush_ms=%.1f max_flush_ms=%.1f queued=%d retries=%d fails=%d",
						t.name, f, rows, float64(rows)/fm, float64(us)/fm/1000, float64(mx)/1000, queued, re, fa)
				}
			}
		}()
	}
	return b
}

// submit queues the row on the lane of key and returns after its batch committed.
// Full lane queue (WRITE_QUEUE_MAX / BATCH_LANES rows) -> 503 busy.
func (b *batcher[R]) submit(row R, key uint32) int {
	done := make(chan int, 1)
	select {
	case b.lanes[key%uint32(len(b.lanes))] <- job[R]{row, done, time.Now()}:
		return <-done
	default:
		return http.StatusServiceUnavailable
	}
}

// execOne runs one write statement directly on the pool (group 2: no batcher);
// retried on deadlock / serialization failure like the batch writer.
func execOne(ctx context.Context, pool *pgxpool.Pool, name, sql string, args ...any) int {
	for attempt := 0; attempt < 3; attempt++ {
		_, err := pool.Exec(ctx, sql, args...)
		if err == nil {
			return http.StatusOK
		}
		var pe *pgconn.PgError
		if attempt < 2 && errors.As(err, &pe) && (pe.Code == "40P01" || pe.Code == "40001") {
			continue
		}
		log.Printf("write %s failed: %v", name, err)
		break
	}
	return http.StatusInternalServerError
}

// ---------- read models ----------

type postItem struct {
	ID           pgtype.UUID `json:"id"`
	UserID       pgtype.UUID `json:"user_id"`
	Username     string      `json:"username"`
	DisplayName  string      `json:"display_name"`
	AvatarURL    *string     `json:"avatar_url"`
	IsVerified   bool        `json:"is_verified"`
	ReplyToID    pgtype.UUID `json:"reply_to_id"`
	Title        string      `json:"title"`
	Preview      string      `json:"preview"`
	Lang         string      `json:"lang"`
	MediaURL     *string     `json:"media_url"`
	LikeCount    int64       `json:"like_count"`
	CommentCount int64       `json:"comment_count"`
	ShareCount   int64       `json:"share_count"`
	CreatedMs    int64       `json:"created_ms"`
}

type messageItem struct {
	ID                pgtype.UUID `json:"id"`
	ConversationID    pgtype.UUID `json:"conversation_id"`
	SenderID          pgtype.UUID `json:"sender_id"`
	SenderUsername    string      `json:"sender_username"`
	SenderDisplayName string      `json:"sender_display_name"`
	SenderAvatarURL   *string     `json:"sender_avatar_url"`
	ContentType       int16       `json:"content_type"`
	Preview           string      `json:"preview"`
	AttachmentURL     *string     `json:"attachment_url"`
	CreatedMs         int64       `json:"created_ms"`
	ReadMs            *int64      `json:"read_ms"`
}

type page[T any] struct {
	Items []T         `json:"items"`
	Next  pgtype.UUID `json:"next"`
}

func queryPage[T any](ctx context.Context, pool *pgxpool.Pool, scan func(pgx.Rows, *T) error, id func(*T) pgtype.UUID, q string, args ...any) ([]byte, error) {
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]T, 0, pageSize)
	for rows.Next() {
		var it T
		if err := scan(rows, &it); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	p := page[T]{Items: items}
	if len(items) == pageSize {
		p.Next = id(&items[pageSize-1])
	}
	return json.Marshal(p)
}

func scanPost(r pgx.Rows, p *postItem) error {
	return r.Scan(&p.ID, &p.UserID, &p.Username, &p.DisplayName, &p.AvatarURL, &p.IsVerified, &p.ReplyToID, &p.Title,
		&p.Preview, &p.Lang, &p.MediaURL, &p.LikeCount, &p.CommentCount, &p.ShareCount, &p.CreatedMs)
}

func scanMessage(r pgx.Rows, m *messageItem) error {
	return r.Scan(&m.ID, &m.ConversationID, &m.SenderID, &m.SenderUsername, &m.SenderDisplayName, &m.SenderAvatarURL,
		&m.ContentType, &m.Preview, &m.AttachmentURL, &m.CreatedMs, &m.ReadMs)
}

func postID(p *postItem) pgtype.UUID       { return p.ID }
func messageID(m *messageItem) pgtype.UUID { return m.ID }

// ---------- responses ----------

var (
	okBody = []byte(`{"ok":true}`)
	errs   = map[int][]byte{
		400: []byte(`{"error":"bad_request"}`),
		401: []byte(`{"error":"unauthorized"}`),
		404: []byte(`{"error":"not_found"}`),
		413: []byte(`{"error":"too_large"}`),
		500: []byte(`{"error":"db"}`),
		503: []byte(`{"error":"busy"}`),
	}
)

const ctJSON = "application/json"

func fail(c *gin.Context, code int) { c.Data(code, ctJSON, errs[code]) }

func done(c *gin.Context, code int) {
	if code == http.StatusOK {
		c.Data(code, ctJSON, okBody)
		return
	}
	fail(c, code)
}

// readBody reads at most 16 KB; returns 0 on success or the HTTP error code.
func readBody(c *gin.Context, v any) int {
	b, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, bodyLimit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return http.StatusRequestEntityTooLarge
		}
		return http.StatusBadRequest
	}
	if json.Unmarshal(b, v) != nil {
		return http.StatusBadRequest
	}
	return 0
}

// ---------- main ----------

type tokenReq struct {
	UserID *string `json:"user_id"`
}

type postReq struct {
	Title      *string `json:"title"`
	Body       *string `json:"body"`
	Visibility *int16  `json:"visibility"`
	Lang       *string `json:"lang"`
	ReplyTo    *string `json:"reply_to"`
	MediaURL   *string `json:"media_url"`
}

type messageReq struct {
	To            *string `json:"to"`
	Body          *string `json:"body"`
	ContentType   *int16  `json:"content_type"`
	AttachmentURL *string `json:"attachment_url"`
}

// decodePost reads + validates a post body; returns 0 or the HTTP error code.
func decodePost(c *gin.Context, uid uuid.UUID) (newPost, int) {
	var req postReq
	if code := readBody(c, &req); code != 0 {
		return newPost{}, code
	}
	lang := "en"
	if req.Lang != nil {
		lang = *req.Lang
	}
	if req.Title == nil || req.Body == nil || req.Visibility == nil || !lenOK(*req.Title, 1, 200) || !lenOK(*req.Body, 1, 8000) ||
		(*req.Visibility != 0 && *req.Visibility != 1) || !lenOK(lang, 2, 8) || !optLenOK(req.MediaURL, 1, 500) {
		return newPost{}, http.StatusBadRequest
	}
	var replyTo pgtype.UUID
	if req.ReplyTo != nil {
		u, ok := parseUUID(*req.ReplyTo)
		if !ok {
			return newPost{}, http.StatusBadRequest
		}
		replyTo = pgUUID(u)
	}
	return newPost{uid: uid, replyTo: replyTo, visibility: *req.Visibility, title: *req.Title, body: *req.Body, lang: lang, mediaURL: req.MediaURL}, 0
}

// decodeMessage reads + validates a message body; returns 0 or the HTTP error code.
func decodeMessage(c *gin.Context, uid uuid.UUID) (newMessage, int) {
	var req messageReq
	if code := readBody(c, &req); code != 0 {
		return newMessage{}, code
	}
	if req.To == nil || req.Body == nil {
		return newMessage{}, http.StatusBadRequest
	}
	to, ok := parseUUID(*req.To)
	var ct int16
	if req.ContentType != nil {
		ct = *req.ContentType
	}
	if !ok || to == uid || !lenOK(*req.Body, 1, 8000) || ct < 0 || ct > 3 || !optLenOK(req.AttachmentURL, 1, 500) {
		return newMessage{}, http.StatusBadRequest
	}
	return newMessage{uid: uid, to: to, contentType: ct, body: *req.Body, attachment: req.AttachmentURL}, 0
}

func main() {
	ctx := context.Background()
	poolSize := envInt("DB_POOL_TOTAL", 12)
	bcfg := batchCfg{
		maxRows:  max(1, envInt("BATCH_MAX_ROWS", 1000)),
		window:   time.Duration(envInt("BATCH_WINDOW_MS", 200)) * time.Millisecond,
		lanes:    max(1, envInt("BATCH_LANES", 4)),
		queueMax: envInt("WRITE_QUEUE_MAX", 40000),
		stats:    os.Getenv("BATCH_STATS") == "1",
	}
	stmtTimeout := envInt("STATEMENT_TIMEOUT_MS", 5000)
	secret := []byte(os.Getenv("JWT_SECRET"))
	issuerKey := []byte(os.Getenv("TOKEN_ISSUER_KEY"))
	iss, aud := envOr("JWT_ISS", "bench7"), envOr("JWT_AUD", "bench7-api")
	jwtTTL := envInt("JWT_TTL_S", 3600)
	if len(secret) == 0 || len(issuerKey) == 0 {
		log.Fatal("JWT_SECRET and TOKEN_ISSUER_KEY are required")
	}
	uuid.EnableRandPool()

	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxConns, cfg.MinConns = int32(poolSize), int32(poolSize)
	if stmtTimeout > 0 {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.Itoa(stmtTimeout)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	ttlMin, ttlMax := uint64(envInt("CACHE_TTL_MIN_MS", 45000)), uint64(envInt("CACHE_TTL_MAX_MS", 60000))
	bins := uint64(max(1, envInt("CACHE_TTL_BINS", 15)))
	ttl := hashTTL{min: ttlMin, width: max(1, (ttlMax-ttlMin)/bins), bins: bins}
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters:        200_000,
		MaxCost:            int64(envInt("CACHE_MAX_MB", 64)) << 20,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	var flights singleflight.Group

	posts := newBatcher(pool, postsTable, sqlPosts(postsUnnest), bcfg)
	messages := newBatcher(pool, messagesTable, sqlMessagesBatch(messagesUnnest), bcfg)
	likes := newBatcher(pool, likesTable, sqlLikes(likesUnnest), bcfg)
	// group 2 uses the scalar source
	nPosts, nMessages, nLikes := sqlPosts(postsOne), sqlMessagesBatch(messagesOne), sqlLikes(likesOne)

	jwt.MarshalSingleStringAsArray = false
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(iss),
		jwt.WithAudience(aud),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(0),
	)
	keyFn := func(*jwt.Token) (any, error) { return secret, nil }
	auth := func(c *gin.Context) (uuid.UUID, bool) {
		h := c.GetHeader("Authorization")
		if len(h) < 8 || h[:7] != "Bearer " {
			return uuid.Nil, false
		}
		var cl jwt.RegisteredClaims
		if _, err := parser.ParseWithClaims(h[7:], &cl, keyFn); err != nil {
			return uuid.Nil, false
		}
		return parseUUID(cl.Subject)
	}
	cursor := func(c *gin.Context) (uuid.UUID, bool) {
		v, ok := c.GetQuery("before")
		if !ok {
			return uuidMax, true
		}
		return parseUUID(v)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	healthBody := []byte(`{"ok":true,"framework":"gin"}`)
	health := func(c *gin.Context) { c.Data(200, ctJSON, healthBody) }
	r.GET("/health", health)

	r.POST("/auth/token", func(c *gin.Context) {
		if subtle.ConstantTimeCompare([]byte(c.GetHeader("x-issuer-key")), issuerKey) != 1 {
			fail(c, 401)
			return
		}
		var req tokenReq
		if code := readBody(c, &req); code != 0 {
			fail(c, code)
			return
		}
		if req.UserID == nil {
			fail(c, 400)
			return
		}
		uid, ok := parseUUID(*req.UserID)
		if !ok {
			fail(c, 400)
			return
		}
		var exists bool
		if err := pool.QueryRow(c, sqlUserExists, pgUUID(uid)).Scan(&exists); err != nil {
			fail(c, 500)
			return
		}
		if !exists {
			fail(c, 404)
			return
		}
		now := time.Now()
		tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
			Subject:   uid.String(),
			Issuer:    iss,
			Audience:  jwt.ClaimStrings{aud},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(jwtTTL) * time.Second)),
		}).SignedString(secret)
		if err != nil {
			fail(c, 500)
			return
		}
		c.Data(200, ctJSON, []byte(fmt.Sprintf(`{"token":%q,"expires_in":%d}`, tok, jwtTTL)))
	})

	r.GET("/posts/public", func(c *gin.Context) {
		before, ok := cursor(c)
		if !ok {
			fail(c, 400)
			return
		}
		key := before.String()
		if b, hit := cache.Get(key); hit {
			c.Header("x-cache", "hit")
			c.Data(200, ctJSON, b)
			return
		}
		v, err, _ := flights.Do(key, func() (any, error) {
			if b, hit := cache.Get(key); hit {
				return b, nil
			}
			b, err := queryPage(context.Background(), pool, scanPost, postID, sqlPublic, pgUUID(before))
			if err != nil {
				return nil, err
			}
			cache.SetWithTTL(key, b, int64(len(b)+len(key)), ttl.ttl(key))
			return b, nil
		})
		if err != nil {
			fail(c, 500)
			return
		}
		c.Header("x-cache", "miss")
		c.Data(200, ctJSON, v.([]byte))
	})

	privatePosts := func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		before, ok := cursor(c)
		if !ok {
			fail(c, 400)
			return
		}
		b, err := queryPage(c, pool, scanPost, postID, sqlPrivate, pgUUID(uid), pgUUID(before))
		if err != nil {
			fail(c, 500)
			return
		}
		c.Data(200, ctJSON, b)
	}
	r.GET("/posts/private", privatePosts)

	listMessages := func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		before, ok := cursor(c)
		if !ok {
			fail(c, 400)
			return
		}
		b, err := queryPage(c, pool, scanMessage, messageID, sqlMessages, pgUUID(uid), pgUUID(before))
		if err != nil {
			fail(c, 500)
			return
		}
		c.Data(200, ctJSON, b)
	}
	r.GET("/messages", listMessages)

	r.POST("/posts", func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		p, code := decodePost(c, uid)
		if code != 0 {
			fail(c, code)
			return
		}
		done(c, posts.submit(p, laneKey(p.uid)))
	})

	r.POST("/messages", func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		m, code := decodeMessage(c, uid)
		if code != 0 {
			fail(c, code)
			return
		}
		// symmetric key: both directions of a conversation share one lane
		done(c, messages.submit(m, laneKey(m.uid)^laneKey(m.to)))
	})

	r.POST("/posts/:id/like", func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		pid, ok := parseUUID(c.Param("id"))
		if !ok {
			fail(c, 400)
			return
		}
		done(c, likes.submit(newLike{uid: uid, postID: pid}, laneKey(pid)))
	})

	// ---------- group 2 ("normal", prefix /n): no batching, no cache ----------
	r.GET("/n/health", health)

	r.GET("/n/posts/public", func(c *gin.Context) {
		before, ok := cursor(c)
		if !ok {
			fail(c, 400)
			return
		}
		b, err := queryPage(c, pool, scanPost, postID, sqlPublic, pgUUID(before))
		if err != nil {
			fail(c, 500)
			return
		}
		c.Data(200, ctJSON, b)
	})

	r.GET("/n/posts/private", privatePosts)
	r.GET("/n/messages", listMessages)

	r.POST("/n/posts", func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		p, code := decodePost(c, uid)
		if code != 0 {
			fail(c, code)
			return
		}
		id := pgUUID(uuid.Must(uuid.NewV7()))
		done(c, execOne(c, pool, "posts", nPosts, id, pgUUID(p.uid), p.replyTo, p.visibility, p.title, p.body, p.lang, p.mediaURL))
	})

	r.POST("/n/messages", func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		m, code := decodeMessage(c, uid)
		if code != 0 {
			fail(c, code)
			return
		}
		id, conv := pgUUID(uuid.Must(uuid.NewV7())), pgUUID(uuid.Must(uuid.NewV7()))
		done(c, execOne(c, pool, "messages", nMessages, id, conv, pgUUID(m.uid), pgUUID(m.to), m.contentType, m.body, m.attachment))
	})

	r.POST("/n/posts/:id/like", func(c *gin.Context) {
		uid, ok := auth(c)
		if !ok {
			fail(c, 401)
			return
		}
		pid, ok := parseUUID(c.Param("id"))
		if !ok {
			fail(c, 400)
			return
		}
		done(c, execOne(c, pool, "likes", nLikes, pgUUID(uuid.Must(uuid.NewV7())), pgUUID(uid), pgUUID(pid)))
	})

	addr := envOr("HOST", "0.0.0.0") + ":" + envOr("PORT", "8080")
	srv := &http.Server{Addr: addr, Handler: r, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 75 * time.Second}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Printf("go runtime=%s GOMAXPROCS=%d GOGC=%s GOMEMLIMIT=%s", runtime.Version(), runtime.GOMAXPROCS(0),
		envOr("GOGC", "unset"), envOr("GOMEMLIMIT", "unset"))
	log.Printf("gin listening on %s pool=%d batch_rows=%d batch_window_ms=%d lanes=%d queue_max=%d stmt_timeout_ms=%d",
		addr, poolSize, bcfg.maxRows, bcfg.window.Milliseconds(), bcfg.lanes, bcfg.queueMax, stmtTimeout)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	pool.Close()
}
