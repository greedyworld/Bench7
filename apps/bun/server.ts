// bench7 — Bun implementation (Bun.serve + Bun.sql, one process per CPU with
// SO_REUSEPORT). Same endpoints, SQL, cache, batcher and JWT rules as the other apps.
//  * ids: Bun.randomUUIDv7(); created_at = uuid_extract_timestamp(id)
//  * cache TTL: deterministic hash bucketing (spreads expiries, no thundering herd)
//  * writes: per-table batcher, lanes keyed by user / conversation / post -> column arrays ->
//    one upsert statement per batch (unnest); the reply is sent after commit
//
// Bun.sql does not bind plain JS arrays as Postgres array parameters, so the
// column arrays are sent as Postgres array-literal text parameters ($1::uuid[] etc.),
// which is what postgres.js does internally.
import { SQL } from 'bun'
import { timingSafeEqual } from 'node:crypto'
import { availableParallelism } from 'node:os'
import { LRUCache } from 'lru-cache'
import { createSigner, createVerifier } from 'fast-jwt'

const env = process.env
const WORKERS = Number(env.WORKERS || availableParallelism())
// BATCH_LANES and WRITE_QUEUE_MAX are totals per table across all worker processes
const LANES = Math.max(1, Math.round(Number(env.BATCH_LANES || 4) / WORKERS))
const LANE_CAP = Math.max(1, Math.floor(Number(env.WRITE_QUEUE_MAX || 40000) / WORKERS / LANES))
const WRITE_ATTEMPTS = 4
const STMT_TIMEOUT_MS = Number(env.STATEMENT_TIMEOUT_MS ?? 5000)
// total connections stay = DB_POOL_TOTAL across all worker processes (minus rounding)
const POOL_MAX = Math.max(1, Math.floor(Number(env.DB_POOL_TOTAL || 12) / WORKERS))

// ---------- SQL (identical in every app) ----------
const POST_SELECT = 'SELECT p.id, p.user_id, u.username, u.display_name, u.avatar_url, u.is_verified, p.reply_to_id, ' +
  'p.title, left(p.body, 210) AS preview, p.lang, p.media_url, p.like_count, p.comment_count, ' +
  'p.share_count, (extract(epoch FROM p.created_at)*1000)::int8 AS created_ms ' +
  'FROM posts p JOIN users u ON u.id = p.user_id '
const SQL_PUBLIC = POST_SELECT + 'WHERE p.visibility = 0 AND p.deleted_at IS NULL AND p.id < $1 ORDER BY p.id DESC LIMIT 5'
const SQL_PRIVATE = POST_SELECT + 'WHERE p.user_id = $1 AND p.visibility = 1 AND p.deleted_at IS NULL AND p.id < $2 ORDER BY p.id DESC LIMIT 5'
const SQL_MESSAGES = 'SELECT m.id, m.conversation_id, m.sender_id, u.username AS sender_username, ' +
  'u.display_name AS sender_display_name, u.avatar_url AS sender_avatar_url, m.content_type, ' +
  'left(m.body, 240) AS preview, m.attachment_url, (extract(epoch FROM m.created_at)*1000)::int8 AS created_ms, ' +
  '(extract(epoch FROM m.read_at)*1000)::int8 AS read_ms ' +
  'FROM messages m JOIN users u ON u.id = m.sender_id ' +
  'WHERE m.recipient_id = $1 AND m.deleted_at IS NULL AND m.id < $2 ORDER BY m.id DESC LIMIT 5'
const SQL_USER_EXISTS = 'SELECT EXISTS(SELECT 1 FROM users WHERE id = $1) AS exists'

const POSTS_UNNEST = 'SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int2[], $5::text[], $6::text[], $7::text[], $8::text[]) ' +
  'AS t(id, user_id, reply_to_id, visibility, title, body, lang, media_url)'
const MESSAGES_UNNEST = 'SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::int2[], $6::text[], $7::text[]) ' +
  'AS t(id, new_conv_id, sender_id, recipient_id, content_type, body, attachment_url)'
const LIKES_UNNEST = 'SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS t(id, user_id, post_id)'
// group 2 (/n): same write SQL fed one row from scalar parameters
const POSTS_ONE = 'SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS reply_to_id, $4::int2 AS visibility, ' +
  '$5::text AS title, $6::text AS body, $7::text AS lang, $8::text AS media_url'
const MESSAGES_ONE = 'SELECT $1::uuid AS id, $2::uuid AS new_conv_id, $3::uuid AS sender_id, $4::uuid AS recipient_id, ' +
  '$5::int2 AS content_type, $6::text AS body, $7::text AS attachment_url'
const LIKES_ONE = 'SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS post_id'

const sqlPosts = (src: string) => `WITH raw AS (${src}), ` +
  'ins AS ( ' +
  'INSERT INTO posts (id, user_id, reply_to_id, visibility, title, body, lang, media_url, like_count, comment_count, ' +
  'share_count, view_count, is_edited, created_at, updated_at, deleted_at) ' +
  'SELECT r.id, r.user_id, r.reply_to_id, r.visibility, r.title, r.body, r.lang, r.media_url, 0, 0, 0, 0, false, ' +
  'uuid_extract_timestamp(r.id), uuid_extract_timestamp(r.id), NULL ' +
  'FROM raw r ' +
  'WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) ' +
  'AND (r.reply_to_id IS NULL OR EXISTS (SELECT 1 FROM posts p WHERE p.id = r.reply_to_id)) ' +
  'ON CONFLICT (id) DO NOTHING ' +
  'RETURNING user_id, reply_to_id), ' +
  'by_user AS ( ' +
  'UPDATE users SET posts_count = users.posts_count + c.n ' +
  'FROM (SELECT user_id, count(*) AS n FROM ins GROUP BY user_id ORDER BY user_id) c ' +
  'WHERE users.id = c.user_id) ' +
  'UPDATE posts SET comment_count = posts.comment_count + c.n ' +
  'FROM (SELECT reply_to_id, count(*) AS n FROM ins WHERE reply_to_id IS NOT NULL GROUP BY reply_to_id ORDER BY reply_to_id) c ' +
  'WHERE posts.id = c.reply_to_id'

const sqlMessages = (src: string) => `WITH raw AS (${src}), ` +
  'v AS ( ' +
  'SELECT r.*, least(r.sender_id, r.recipient_id) AS ua, greatest(r.sender_id, r.recipient_id) AS ub ' +
  'FROM raw r ' +
  'WHERE r.sender_id <> r.recipient_id ' +
  'AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.recipient_id) ' +
  'AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.sender_id)), ' +
  'last AS ( ' +
  'SELECT DISTINCT ON (ua, ub) ua, ub, new_conv_id, id, left(body, 100) AS preview, ' +
  'count(*) OVER (PARTITION BY ua, ub) AS n ' +
  'FROM v ORDER BY ua, ub, id DESC), ' +
  'conv AS ( ' +
  'INSERT INTO conversations AS c (id, user_a_id, user_b_id, last_message_id, last_message_preview, ' +
  'last_message_at, message_count, created_at, updated_at) ' +
  'SELECT new_conv_id, ua, ub, id, preview, uuid_extract_timestamp(id), n, ' +
  'uuid_extract_timestamp(new_conv_id), uuid_extract_timestamp(id) ' +
  'FROM last ' +
  'ON CONFLICT (user_a_id, user_b_id) DO UPDATE SET ' +
  'last_message_id = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_id ELSE c.last_message_id END, ' +
  'last_message_preview = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_preview ELSE c.last_message_preview END, ' +
  'last_message_at = greatest(EXCLUDED.last_message_at, c.last_message_at), ' +
  'message_count = c.message_count + EXCLUDED.message_count, ' +
  'updated_at = greatest(EXCLUDED.updated_at, c.updated_at) ' +
  'RETURNING c.id, c.user_a_id, c.user_b_id) ' +
  'INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content_type, body, attachment_url, ' +
  'created_at, read_at, edited_at, deleted_at) ' +
  'SELECT v.id, conv.id, v.sender_id, v.recipient_id, v.content_type, v.body, v.attachment_url, ' +
  'uuid_extract_timestamp(v.id), NULL, NULL, NULL ' +
  'FROM v JOIN conv ON conv.user_a_id = v.ua AND conv.user_b_id = v.ub ' +
  'ON CONFLICT (id) DO NOTHING'

const sqlLikes = (src: string) => `WITH raw AS (${src}), ` +
  'ins AS ( ' +
  'INSERT INTO likes (id, user_id, post_id, created_at) ' +
  'SELECT r.id, r.user_id, r.post_id, uuid_extract_timestamp(r.id) ' +
  'FROM raw r ' +
  'WHERE EXISTS (SELECT 1 FROM posts p WHERE p.id = r.post_id) ' +
  'AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) ' +
  'ON CONFLICT DO NOTHING ' +
  'RETURNING post_id) ' +
  'UPDATE posts SET like_count = posts.like_count + c.n ' +
  'FROM (SELECT post_id, count(*) AS n FROM ins GROUP BY post_id ORDER BY post_id) c ' +
  'WHERE posts.id = c.post_id'

const SQL_POSTS = sqlPosts(POSTS_UNNEST)
const SQL_MESSAGES_BATCH = sqlMessages(MESSAGES_UNNEST)
const SQL_LIKES = sqlLikes(LIKES_UNNEST)
const N_POSTS = sqlPosts(POSTS_ONE)
const N_MESSAGES = sqlMessages(MESSAGES_ONE)
const N_LIKES = sqlLikes(LIKES_ONE)

const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
const UUID_MAX = 'ffffffff-ffff-ffff-ffff-ffffffffffff'
// lane key: last 4 bytes of a UUID (random part of a v7 id)
const key32 = (u) => parseInt(u.slice(-8), 16) >>> 0

// ---------- cache TTL: deterministic hash bucketing ----------
// ttl_ms = MIN + (h % BINS) * W + ((h >> 32) % W), W = (MAX - MIN) / BINS, h = FNV-1a 64("public:" + uuid)
const M64 = (1n << 64n) - 1n
function fnv1a64 (s) {
  let h = 0xcbf29ce484222325n
  for (let i = 0; i < s.length; i++) h = ((h ^ BigInt(s.charCodeAt(i))) * 0x100000001b3n) & M64
  return h
}

// ---------- Postgres array literals (text params); NULL is the unquoted keyword ----------
const uuidArr = (a) => '{' + a.map((x) => x === null ? 'NULL' : x).join(',') + '}'
const intArr = (a) => '{' + a.join(',') + '}'
const txtArr = (a) => '{' + a.map((s) => s === null ? 'NULL' : '"' + s.replace(/[\\"]/g, '\\$&') + '"').join(',') + '}'

if (!env.BENCH7_CHILD) {
  console.error(`bun primary pid=${process.pid} Bun.version=${Bun.version} workers=${WORKERS} pool_per_worker=${POOL_MAX} ` +
    (WORKERS > 1 ? `-> spawning ${WORKERS} child processes` : '-> single process'))
}
if (!env.BENCH7_CHILD && WORKERS > 1) {
  const kids = []
  for (let i = 0; i < WORKERS; i++) {
    kids.push(Bun.spawn([process.execPath, import.meta.path], {
      env: { ...env, BENCH7_CHILD: '1' }, stdout: 'inherit', stderr: 'inherit',
      onExit: (p, code) => { console.error(`worker ${p.pid} exited ${code}`); process.exit(1) }
    }))
  }
  const stop = () => { for (const k of kids) k.kill('SIGTERM'); setTimeout(() => process.exit(0), 3000) }
  process.on('SIGTERM', stop); process.on('SIGINT', stop)
} else {
  await start()
}

async function start () {
  const poolMax = POOL_MAX
  const sql = new SQL(env.DATABASE_URL, {
    max: poolMax,
    prepare: true,
    bigint: false,
    idleTimeout: 0,
    connection: STMT_TIMEOUT_MS > 0 ? { statement_timeout: String(STMT_TIMEOUT_MS) } : {}
  })

  const ISS = env.JWT_ISS || 'bench7'
  const AUD = env.JWT_AUD || 'bench7-api'
  const TTL = Number(env.JWT_TTL_S || 3600)
  const ISSUER_KEY = Buffer.from(env.TOKEN_ISSUER_KEY)
  const sign = createSigner({ key: env.JWT_SECRET, algorithm: 'HS256' })
  const verify = createVerifier({
    key: env.JWT_SECRET,
    algorithms: ['HS256'],
    allowedIss: ISS,
    allowedAud: AUD,
    requiredClaims: ['exp', 'iss', 'aud', 'sub'],
    clockTolerance: 0,
    cache: false
  })

  // int8 comes back as text with bigint:false; counters and ms fit in 2^53
  const postRows = (rows) => {
    for (const r of rows) {
      r.like_count = Number(r.like_count); r.comment_count = Number(r.comment_count)
      r.share_count = Number(r.share_count); r.created_ms = Number(r.created_ms)
    }
    return rows
  }
  const msgRows = (rows) => {
    for (const r of rows) { r.created_ms = Number(r.created_ms); r.read_ms = r.read_ms === null ? null : Number(r.read_ms) }
    return rows
  }
  const page = (items) => JSON.stringify({ items: [...items], next: items.length === 5 ? items[4].id : null })

  const TTL_MIN = Number(env.CACHE_TTL_MIN_MS || 45000)
  const TTL_MAX = Number(env.CACHE_TTL_MAX_MS || 60000)
  const BINS = BigInt(Math.max(1, Number(env.CACHE_TTL_BINS || 15)))
  const WIDTH = BigInt(Math.max(1, Math.floor((TTL_MAX - TTL_MIN) / Number(BINS))))
  const ttlFor = (key) => {
    const h = fnv1a64('public:' + key)
    return TTL_MIN + Number(h % BINS) * Number(WIDTH) + Number((h >> 32n) % WIDTH)
  }
  const cache = new LRUCache({
    maxSize: Math.floor(Number(env.CACHE_MAX_MB || 64) * 1024 * 1024 / WORKERS),
    sizeCalculation: (v) => v.length + 8,
    ttl: TTL_MAX,
    fetchMethod: async (before, _stale, { options }) => {
      options.ttl = ttlFor(before)
      return page(postRows(await sql.unsafe(SQL_PUBLIC, [before])))
    }
  })

  // ---- write batcher: LANES queues per table, one in-flight statement per lane ----
  const BATCH_MAX = Math.max(1, Number(env.BATCH_MAX_ROWS || 1000))
  const WINDOW = Number(env.BATCH_WINDOW_MS || 200)
  const STATS = env.BATCH_STATS === '1'
  const retryable = (e) => e && ['40P01', '40001'].includes(e.errno ?? e.code)
  // batch writes also retry statement timeout, too many connections, server shutdown, connection
  // failures and driver errors that carry no SQLSTATE (connection closed, socket errors)
  const RETRY = new Set(['40P01', '40001', '57014', '53300', '57P01', '57P02', '57P03'])
  const batchRetryable = (e) => {
    if (!e) return false
    const s = e.errno
    if (typeof s === 'string' && /^[0-9A-Z]{5}$/.test(s)) return RETRY.has(s) || s.startsWith('08')
    return e.code !== 'ERR_POSTGRES_SERVER_ERROR'
  }
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

  // one lane = one queue + one writer; the window runs from the arrival of the batch's first row
  class Lane {
    constructor (b) { this.b = b; this.q = []; this.busy = false; this.timer = null }
    add (job) {
      this.q.push(job)
      if (this.busy) return
      if (this.q.length >= BATCH_MAX) { clearTimeout(this.timer); this.timer = null; this.run() } else if (!this.timer) this.arm()
    }
    arm () {
      this.timer = setTimeout(() => { this.timer = null; this.run() }, Math.max(0, this.q[0].t + WINDOW - Date.now()))
    }
    async run () {
      this.busy = true
      const batch = this.q.splice(0, BATCH_MAX)
      // ids are generated here, once: a retry re-sends the same ids (ON CONFLICT (id) DO NOTHING)
      const cols = Array.from({ length: this.b.cols }, () => [])
      for (const j of batch) this.b.push(cols, j.row)
      const t0 = performance.now()
      const ok = await this.b.write(this.b.params(cols), batch.length)
      this.b.stat(batch.length, performance.now() - t0)
      // ack only after the statement committed (or failed for good)
      for (const j of batch) j.resolve(ok ? 200 : 500)
      this.busy = false
      if (this.q.length >= BATCH_MAX) this.run()
      else if (this.q.length) this.arm()
    }
  }

  class Batcher {
    constructor (name, cols, push, query, params) {
      this.name = name; this.cols = cols; this.push = push; this.query = query; this.params = params
      this.lanes = Array.from({ length: LANES }, () => new Lane(this))
      this.st = { f: 0, rows: 0, ms: 0, max: 0, re: 0, fa: 0 }
      if (STATS) {
        setInterval(() => {
          const s = this.st
          const queued = this.lanes.reduce((n, l) => n + l.q.length, 0)
          if (s.f || queued) {
            console.error(`batch-stats ${name} pid=${process.pid} flushes=${s.f} rows=${s.rows} avg_rows=${(s.rows / Math.max(1, s.f)).toFixed(0)} ` +
              `avg_flush_ms=${(s.ms / Math.max(1, s.f)).toFixed(1)} max_flush_ms=${s.max.toFixed(1)} queued=${queued} retries=${s.re} fails=${s.fa}`)
          }
          this.st = { f: 0, rows: 0, ms: 0, max: 0, re: 0, fa: 0 }
        }, 10000).unref()
      }
    }

    // full lane queue -> 503 busy
    submit (row, key) {
      const lane = this.lanes[key % LANES]
      if (lane.q.length >= LANE_CAP) return Promise.resolve(503)
      return new Promise((resolve) => lane.add({ row, resolve, t: Date.now() }))
    }

    stat (rows, ms) {
      const s = this.st
      s.f++; s.rows += rows; s.ms += ms; if (ms > s.max) s.max = ms
    }

    // one upsert statement per batch; transient errors retried with backoff, then fail loudly
    async write (params, n) {
      for (let attempt = 1; ; attempt++) {
        try {
          await sql.unsafe(this.query, params)
          return true
        } catch (e) {
          if (attempt < WRITE_ATTEMPTS && batchRetryable(e)) {
            this.st.re++
            console.error(`batch ${this.name} attempt ${attempt} failed, retrying: ${e.message}`)
            await sleep(50 << (attempt - 1))
            continue
          }
          this.st.fa++
          console.error(`BATCH FAILED ${this.name} rows=${n} after ${attempt} attempt(s): ${e.message}`)
          return false
        }
      }
    }
  }
  const v7 = () => Bun.randomUUIDv7()
  const posts = new Batcher('posts', 8,
    (c, r) => { c[0].push(v7()); c[1].push(r.uid); c[2].push(r.reply_to); c[3].push(r.visibility); c[4].push(r.title); c[5].push(r.body); c[6].push(r.lang); c[7].push(r.media_url) },
    SQL_POSTS,
    (c) => [uuidArr(c[0]), uuidArr(c[1]), uuidArr(c[2]), intArr(c[3]), txtArr(c[4]), txtArr(c[5]), txtArr(c[6]), txtArr(c[7])])
  const messages = new Batcher('messages', 7,
    (c, r) => { c[0].push(v7()); c[1].push(v7()); c[2].push(r.uid); c[3].push(r.to); c[4].push(r.content_type); c[5].push(r.body); c[6].push(r.attachment_url) },
    SQL_MESSAGES_BATCH,
    (c) => [uuidArr(c[0]), uuidArr(c[1]), uuidArr(c[2]), uuidArr(c[3]), intArr(c[4]), txtArr(c[5]), txtArr(c[6])])
  const likes = new Batcher('likes', 3,
    (c, r) => { c[0].push(v7()); c[1].push(r.uid); c[2].push(r.post_id) },
    SQL_LIKES,
    (c) => [uuidArr(c[0]), uuidArr(c[1]), uuidArr(c[2])])

  // group 2: one statement per request on the pool, retried on deadlock / serialization failure
  async function execOne (name, text, params) {
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await sql.unsafe(text, params)
        return 200
      } catch (e) {
        if (attempt < 2 && retryable(e)) continue
        console.error(`write ${name} failed: ${e.message}`)
        break
      }
    }
    return 500
  }

  // ---- responses ----
  const JSON_H = { 'content-type': 'application/json' }
  const HIT_H = { 'content-type': 'application/json', 'x-cache': 'hit' }
  const MISS_H = { 'content-type': 'application/json', 'x-cache': 'miss' }
  const ERR = {
    400: '{"error":"bad_request"}', 401: '{"error":"unauthorized"}', 404: '{"error":"not_found"}',
    413: '{"error":"too_large"}', 500: '{"error":"db"}', 503: '{"error":"busy"}'
  }
  const fail = (code) => new Response(ERR[code], { status: code, headers: JSON_H })
  const json = (body) => new Response(body, { headers: JSON_H })
  const OK = '{"ok":true}'
  const done = (code) => code === 200 ? json(OK) : fail(code)

  function auth (req) {
    const h = req.headers.get('authorization')
    if (!h || !h.startsWith('Bearer ')) return null
    let c
    try { c = verify(h.slice(7)) } catch { return null }
    if (typeof c.sub !== 'string' || !UUID_RE.test(c.sub)) return null
    if (typeof c.iat === 'number' && c.iat > Math.floor(Date.now() / 1000)) return null
    return c.sub.toLowerCase()
  }
  const isUuid = (v) => typeof v === 'string' && UUID_RE.test(v)
  // returns the lowercase cursor, or null when invalid
  function cursor (url) {
    const q = url.indexOf('?')
    if (q < 0) return UUID_MAX
    const v = new URLSearchParams(url.slice(q + 1)).get('before')
    if (v === null) return UUID_MAX
    return UUID_RE.test(v) ? v.toLowerCase() : null
  }
  const strOk = (s, min, max) => {
    if (typeof s !== 'string' || s.length < min || s.length > max * 2) return false
    const n = [...s].length
    return n >= min && n <= max
  }
  const optStr = (s, min, max) => s === undefined || s === null || strOk(s, min, max)
  async function body (req) {
    const len = Number(req.headers.get('content-length') || 0)
    if (len > 16 * 1024) return 413
    try {
      const t = await req.text()
      if (t.length > 16 * 1024) return 413
      const b = JSON.parse(t)
      return b && typeof b === 'object' && !Array.isArray(b) ? b : 400
    } catch { return 400 }
  }

  // shared validation for /posts and /n/posts; returns the row or null (400)
  function parsePost (uid, b) {
    const lang = b.lang ?? 'en'
    if (!strOk(b.title, 1, 200) || !strOk(b.body, 1, 8000) || (b.visibility !== 0 && b.visibility !== 1) ||
        !strOk(lang, 2, 8) || !optStr(b.media_url, 1, 500) ||
        !(b.reply_to === undefined || b.reply_to === null || isUuid(b.reply_to))) return null
    return {
      uid,
      reply_to: b.reply_to ? b.reply_to.toLowerCase() : null,
      visibility: b.visibility,
      title: b.title,
      body: b.body,
      lang,
      media_url: b.media_url ?? null
    }
  }
  // shared validation for /messages and /n/messages; returns the row or null (400)
  function parseMessage (uid, b) {
    if (!isUuid(b.to)) return null
    const to = b.to.toLowerCase()
    const ct = b.content_type ?? 0
    if (to === uid || !strOk(b.body, 1, 8000) || !Number.isInteger(ct) || ct < 0 || ct > 3 ||
        !optStr(b.attachment_url, 1, 500)) return null
    return { uid, to, content_type: ct, body: b.body, attachment_url: b.attachment_url ?? null }
  }

  const health = () => json('{"ok":true,"framework":"bun"}')
  async function privatePosts (req) {
    const uid = auth(req)
    if (!uid) return fail(401)
    const before = cursor(req.url)
    if (before === null) return fail(400)
    try { return json(page(postRows(await sql.unsafe(SQL_PRIVATE, [uid, before])))) } catch { return fail(500) }
  }
  async function listMessages (req) {
    const uid = auth(req)
    if (!uid) return fail(401)
    const before = cursor(req.url)
    if (before === null) return fail(400)
    try { return json(page(msgRows(await sql.unsafe(SQL_MESSAGES, [uid, before])))) } catch { return fail(500) }
  }

  const server = Bun.serve({
    hostname: env.HOST || '0.0.0.0',
    port: Number(env.PORT || 8080),
    reusePort: true,
    maxRequestBodySize: 16 * 1024,
    idleTimeout: 75,
    routes: {
      '/health': health,
      '/n/health': health,

      '/auth/token': {
        POST: async (req) => {
          const key = Buffer.from(req.headers.get('x-issuer-key') || '')
          if (key.length !== ISSUER_KEY.length || !timingSafeEqual(key, ISSUER_KEY)) return fail(401)
          const b = await body(req)
          if (typeof b === 'number') return fail(b)
          if (!isUuid(b.user_id)) return fail(400)
          const uid = b.user_id.toLowerCase()
          let rows
          try { rows = await sql.unsafe(SQL_USER_EXISTS, [uid]) } catch { return fail(500) }
          if (!rows[0].exists) return fail(404)
          const iat = Math.floor(Date.now() / 1000)
          const token = sign({ sub: uid, iat, exp: iat + TTL, iss: ISS, aud: AUD })
          return json(JSON.stringify({ token, expires_in: TTL }))
        }
      },

      '/posts/public': {
        GET: async (req) => {
          const before = cursor(req.url)
          if (before === null) return fail(400)
          const status = {}
          let b
          try { b = await cache.fetch(before, { status }) } catch { return fail(500) }
          return new Response(b, { headers: status.fetch === 'hit' ? HIT_H : MISS_H })
        }
      },

      '/posts/private': { GET: privatePosts },

      '/messages': {
        GET: listMessages,
        POST: async (req) => {
          const uid = auth(req)
          if (!uid) return fail(401)
          const b = await body(req)
          if (typeof b === 'number') return fail(b)
          const m = parseMessage(uid, b)
          if (!m) return fail(400)
          // symmetric key: both directions of a conversation share one lane
          return done(await messages.submit(m, (key32(m.uid) ^ key32(m.to)) >>> 0))
        }
      },

      '/posts': {
        POST: async (req) => {
          const uid = auth(req)
          if (!uid) return fail(401)
          const b = await body(req)
          if (typeof b === 'number') return fail(b)
          const p = parsePost(uid, b)
          if (!p) return fail(400)
          return done(await posts.submit(p, key32(uid)))
        }
      },

      '/posts/:id/like': {
        POST: async (req) => {
          const uid = auth(req)
          if (!uid) return fail(401)
          const id = req.params.id
          if (!UUID_RE.test(id)) return fail(400)
          const pid = id.toLowerCase()
          return done(await likes.submit({ uid, post_id: pid }, key32(pid)))
        }
      },

      // ---- group 2: no cache / x-cache / in-flight de-dup, no batching ----
      '/n/posts/public': {
        GET: async (req) => {
          const before = cursor(req.url)
          if (before === null) return fail(400)
          try { return json(page(postRows(await sql.unsafe(SQL_PUBLIC, [before])))) } catch { return fail(500) }
        }
      },

      '/n/posts/private': { GET: privatePosts },

      '/n/messages': {
        GET: listMessages,
        POST: async (req) => {
          const uid = auth(req)
          if (!uid) return fail(401)
          const b = await body(req)
          if (typeof b === 'number') return fail(b)
          const m = parseMessage(uid, b)
          if (!m) return fail(400)
          return done(await execOne('messages', N_MESSAGES,
            [v7(), v7(), m.uid, m.to, m.content_type, m.body, m.attachment_url]))
        }
      },

      '/n/posts': {
        POST: async (req) => {
          const uid = auth(req)
          if (!uid) return fail(401)
          const b = await body(req)
          if (typeof b === 'number') return fail(b)
          const p = parsePost(uid, b)
          if (!p) return fail(400)
          return done(await execOne('posts', N_POSTS,
            [v7(), p.uid, p.reply_to, p.visibility, p.title, p.body, p.lang, p.media_url]))
        }
      },

      '/n/posts/:id/like': {
        POST: async (req) => {
          const uid = auth(req)
          if (!uid) return fail(401)
          const id = req.params.id
          if (!UUID_RE.test(id)) return fail(400)
          return done(await execOne('likes', N_LIKES, [v7(), uid, id.toLowerCase()]))
        }
      }
    },
    fetch: () => fail(404),
    error: () => fail(500)
  })
  console.error(`bun worker ${process.pid} on ${server.port} pool=${poolMax} batch_rows=${BATCH_MAX} batch_window_ms=${WINDOW} lanes=${LANES} lane_queue=${LANE_CAP} stmt_timeout_ms=${STMT_TIMEOUT_MS}`)
  process.on('SIGTERM', async () => { server.stop(); await sql.close(); process.exit(0) })
}
