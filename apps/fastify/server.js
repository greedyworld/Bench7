// bench7 — Fastify implementation (node:cluster, one process per CPU).
// Runs on Node (`fastify`) and on the Bun runtime (`fastify-bun`), same code.
// Same endpoints, SQL, cache, batcher and JWT rules as the other apps.
//  * ids: UUIDv7 generated here; created_at = uuid_extract_timestamp(id)
//  * cache TTL: deterministic hash bucketing (spreads expiries, no thundering herd)
//  * writes: per-table batcher, lanes keyed by user / conversation / post -> column arrays ->
//    one upsert transaction per batch (unnest); the reply is sent after commit
import cluster from 'node:cluster'
import { availableParallelism } from 'node:os'
import { timingSafeEqual, randomFillSync } from 'node:crypto'
import Fastify from 'fastify'
import postgres from 'postgres'
import { LRUCache } from 'lru-cache'
import { createSigner, createVerifier } from 'fast-jwt'

const env = process.env
const WORKERS = Number(env.WORKERS || availableParallelism())
// BATCH_LANES and WRITE_QUEUE_MAX are totals per table across all worker processes
const LANES = Math.max(1, Math.round(Number(env.BATCH_LANES || 4) / WORKERS))
const LANE_CAP = Math.max(1, Math.floor(Number(env.WRITE_QUEUE_MAX || 40000) / WORKERS / LANES))
const WRITE_ATTEMPTS = 4
const FRAMEWORK = typeof Bun !== 'undefined' ? 'fastify-bun' : 'fastify'
const RUNTIME = process.versions.bun ? 'bun ' + process.versions.bun : 'node ' + process.version
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

const sqlPosts = (src) => `WITH raw AS (${src}), ` +
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

const sqlMessages = (src) => `WITH raw AS (${src}), ` +
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

const sqlLikes = (src) => `WITH raw AS (${src}), ` +
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

// ---------- UUIDv7 (48-bit unix ms | ver 7 | 12-bit monotonic counter | variant | 62 random bits) ----------
const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'))
const RND = Buffer.alloc(8192)
let rndPos = RND.length
let lastMs = 0
let seq = 0
function uuidv7 () {
  if (rndPos + 10 > RND.length) { randomFillSync(RND); rndPos = 0 }
  let ms = Date.now()
  if (ms > lastMs) { lastMs = ms; seq = RND[rndPos] & 0x7f } else if (++seq > 0xfff) { lastMs++; seq = 0 }
  ms = lastMs
  const r = RND; const p = rndPos; rndPos += 8
  const hi = Math.floor(ms / 0x10000); const lo = ms % 0x10000
  return HEX[(hi >>> 24) & 255] + HEX[(hi >>> 16) & 255] + HEX[(hi >>> 8) & 255] + HEX[hi & 255] + '-' +
    HEX[lo >>> 8] + HEX[lo & 255] + '-' +
    HEX[0x70 | (seq >>> 8)] + HEX[seq & 255] + '-' +
    HEX[0x80 | (r[p] & 0x3f)] + HEX[r[p + 1]] + '-' +
    HEX[r[p + 2]] + HEX[r[p + 3]] + HEX[r[p + 4]] + HEX[r[p + 5]] + HEX[r[p + 6]] + HEX[r[p + 7]]
}

const UUID_PATTERN = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
const UUID_RE = new RegExp(UUID_PATTERN)
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

// ---------- column batches: each request is appended to per-column arrays ----------
const BATCHES = {
  posts: {
    cols: 8,
    push: (c, r) => { c[0].push(uuidv7()); c[1].push(r.uid); c[2].push(r.reply_to); c[3].push(r.visibility); c[4].push(r.title); c[5].push(r.body); c[6].push(r.lang); c[7].push(r.media_url) },
    unnest: sqlPosts(POSTS_UNNEST)
  },
  messages: {
    cols: 7,
    push: (c, r) => { c[0].push(uuidv7()); c[1].push(uuidv7()); c[2].push(r.uid); c[3].push(r.to); c[4].push(r.content_type); c[5].push(r.body); c[6].push(r.attachment_url) },
    unnest: sqlMessages(MESSAGES_UNNEST)
  },
  likes: {
    cols: 3,
    push: (c, r) => { c[0].push(uuidv7()); c[1].push(r.uid); c[2].push(r.post_id) },
    unnest: sqlLikes(LIKES_UNNEST)
  }
}
const N_POSTS = sqlPosts(POSTS_ONE)
const N_MESSAGES = sqlMessages(MESSAGES_ONE)
const N_LIKES = sqlLikes(LIKES_ONE)

function start () {
  const poolMax = POOL_MAX
  const sql = postgres(env.DATABASE_URL, {
    max: poolMax,
    prepare: true,
    idle_timeout: 0,
    onnotice: () => {},
    connection: Number(env.STATEMENT_TIMEOUT_MS ?? 5000) > 0 ? { statement_timeout: Number(env.STATEMENT_TIMEOUT_MS ?? 5000) } : {},
    types: { int8: { to: 20, from: [20], parse: Number, serialize: String } }
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

  // ---- cache: bytes-bounded LRU, per-key hash-bucketed TTL, in-flight de-duplication ----
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
      return page(await sql.unsafe(SQL_PUBLIC, [before], { prepare: true }))
    }
  })

  // ---- write batcher: LANES queues per table, one in-flight transaction per lane ----
  const BATCH_MAX = Math.max(1, Number(env.BATCH_MAX_ROWS || 1000))
  const WINDOW = Number(env.BATCH_WINDOW_MS || 200)
  const STATS = env.BATCH_STATS === '1'
  const retryable = (e) => e && (e.code === '40P01' || e.code === '40001')
  // batch writes also retry statement timeout, too many connections, server shutdown, connection
  // failures and errors without a SQLSTATE (socket / driver connection errors)
  const RETRY = new Set(['40P01', '40001', '57014', '53300', '57P01', '57P02', '57P03'])
  const batchRetryable = (e) => {
    const c = e && e.code
    return !c || RETRY.has(c) || String(c).startsWith('08') || !/^[0-9A-Z]{5}$/.test(c)
  }
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

  // one lane = one queue + one writer; the window runs from the arrival of the batch's first row
  class Lane {
    constructor (b) { this.b = b; this.q = []; this.busy = false; this.timer = null }
    push (job) {
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
      const cols = Array.from({ length: this.b.spec.cols }, () => [])
      for (const j of batch) this.b.spec.push(cols, j.row)
      const t0 = performance.now()
      const ok = await this.b.write(cols)
      this.b.stat(batch.length, performance.now() - t0)
      // ack only after the transaction committed (or failed for good)
      for (const j of batch) j.resolve(ok ? 200 : 500)
      this.busy = false
      if (this.q.length >= BATCH_MAX) this.run()
      else if (this.q.length) this.arm()
    }
  }

  class Batcher {
    constructor (name) {
      this.name = name
      this.spec = BATCHES[name]
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
      return new Promise((resolve) => lane.push({ row, resolve, t: Date.now() }))
    }

    stat (rows, ms) {
      const s = this.st
      s.f++; s.rows += rows; s.ms += ms; if (ms > s.max) s.max = ms
    }

    // one upsert transaction per batch; transient errors retried with backoff, then fail loudly
    async write (cols) {
      for (let attempt = 1; ; attempt++) {
        try {
          await sql.unsafe(this.spec.unnest, cols, { prepare: true })
          return true
        } catch (e) {
          if (attempt < WRITE_ATTEMPTS && batchRetryable(e)) {
            this.st.re++
            console.error(`batch ${this.name} attempt ${attempt} failed, retrying: ${e.message}`)
            await sleep(50 << (attempt - 1))
            continue
          }
          this.st.fa++
          console.error(`BATCH FAILED ${this.name} rows=${cols[0].length} after ${attempt} attempt(s): ${e.message}`)
          return false
        }
      }
    }
  }
  const posts = new Batcher('posts')
  const messages = new Batcher('messages')
  const likes = new Batcher('likes')

  // group 2: one statement per request on the pool, retried on deadlock / serialization failure
  async function execOne (reply, name, text, params) {
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await sql.unsafe(text, params, { prepare: true })
        return reply.type('application/json').send(OK)
      } catch (e) {
        if (attempt < 2 && retryable(e)) continue
        console.error(`write ${name} failed: ${e.message}`)
        break
      }
    }
    return fail(reply, 500)
  }

  function page (rows) {
    return Buffer.from(JSON.stringify({ items: rows, next: rows.length === 5 ? rows[4].id : null }))
  }

  const OK = Buffer.from('{"ok":true}')
  const ERR = {
    400: Buffer.from('{"error":"bad_request"}'),
    401: Buffer.from('{"error":"unauthorized"}'),
    404: Buffer.from('{"error":"not_found"}'),
    413: Buffer.from('{"error":"too_large"}'),
    500: Buffer.from('{"error":"db"}'),
    503: Buffer.from('{"error":"busy"}')
  }
  const fail = (reply, code) => reply.code(code).type('application/json').send(ERR[code])

  async function auth (req, reply) {
    const h = req.headers.authorization
    if (!h || !h.startsWith('Bearer ')) return fail(reply, 401)
    let c
    try { c = verify(h.slice(7)) } catch { return fail(reply, 401) }
    if (typeof c.sub !== 'string' || !UUID_RE.test(c.sub)) return fail(reply, 401)
    if (typeof c.iat === 'number' && c.iat > Math.floor(Date.now() / 1000)) return fail(reply, 401)
    req.uid = c.sub.toLowerCase()
  }

  const app = Fastify({
    logger: false,
    bodyLimit: 16 * 1024,
    keepAliveTimeout: 75000,
    return503OnClosing: false,
    ajv: { customOptions: { coerceTypes: false, removeAdditional: false } }
  })
  app.setErrorHandler((err, req, reply) => fail(reply, err.statusCode === 413 ? 413 : (err.statusCode >= 400 && err.statusCode < 500 ? 400 : 500)))

  const uuid = { type: 'string', pattern: UUID_PATTERN }
  const cursor = { type: 'object', properties: { before: uuid } }
  const before = (q) => (q.before ?? UUID_MAX).toLowerCase()

  const health = (req, reply) => reply.type('application/json').send(`{"ok":true,"framework":"${FRAMEWORK}"}`)
  app.get('/health', health)
  app.get('/n/health', health)

  app.post('/auth/token', {
    // issuer key is checked before the body is parsed or validated
    onRequest: async (req, reply) => {
      const key = Buffer.from(req.headers['x-issuer-key'] || '')
      if (key.length !== ISSUER_KEY.length || !timingSafeEqual(key, ISSUER_KEY)) return fail(reply, 401)
    },
    schema: { body: { type: 'object', required: ['user_id'], properties: { user_id: uuid } } }
  }, async (req, reply) => {
    const uid = req.body.user_id.toLowerCase()
    let exists
    try { [{ exists }] = await sql.unsafe(SQL_USER_EXISTS, [uid], { prepare: true }) } catch { return fail(reply, 500) }
    if (!exists) return fail(reply, 404)
    const iat = Math.floor(Date.now() / 1000)
    const token = sign({ sub: uid, iat, exp: iat + TTL, iss: ISS, aud: AUD })
    return reply.type('application/json').send(JSON.stringify({ token, expires_in: TTL }))
  })

  app.get('/posts/public', { schema: { querystring: cursor } }, async (req, reply) => {
    const status = {}
    let body
    try { body = await cache.fetch(before(req.query), { status }) } catch { return fail(reply, 500) }
    return reply.type('application/json').header('x-cache', status.fetch === 'hit' ? 'hit' : 'miss').send(body)
  })

  // group 2: no cache, no x-cache header, no in-flight de-duplication
  app.get('/n/posts/public', { schema: { querystring: cursor } }, async (req, reply) => {
    try {
      return reply.type('application/json').send(page(await sql.unsafe(SQL_PUBLIC, [before(req.query)], { prepare: true })))
    } catch { return fail(reply, 500) }
  })

  const readOpts = { schema: { querystring: cursor }, onRequest: auth }
  const privatePosts = async (req, reply) => {
    try {
      return reply.type('application/json').send(page(await sql.unsafe(SQL_PRIVATE, [req.uid, before(req.query)], { prepare: true })))
    } catch { return fail(reply, 500) }
  }
  const listMessages = async (req, reply) => {
    try {
      return reply.type('application/json').send(page(await sql.unsafe(SQL_MESSAGES, [req.uid, before(req.query)], { prepare: true })))
    } catch { return fail(reply, 500) }
  }
  app.get('/posts/private', readOpts, privatePosts)
  app.get('/n/posts/private', readOpts, privatePosts)
  app.get('/messages', readOpts, listMessages)
  app.get('/n/messages', readOpts, listMessages)

  const done = (reply, code) => code === 200 ? reply.type('application/json').send(OK) : fail(reply, code)
  const optStr = (min, max) => ({ type: ['string', 'null'], minLength: min, maxLength: max })

  // shared request validation (schema) + row extraction for /posts and /n/posts
  const postOpts = {
    onRequest: auth,
    schema: {
      body: {
        type: 'object',
        required: ['title', 'body', 'visibility'],
        properties: {
          title: { type: 'string', minLength: 1, maxLength: 200 },
          body: { type: 'string', minLength: 1, maxLength: 8000 },
          visibility: { type: 'integer', enum: [0, 1] },
          lang: optStr(2, 8),
          reply_to: { type: ['string', 'null'], pattern: UUID_PATTERN },
          media_url: optStr(1, 500)
        }
      }
    }
  }
  const parsePost = (req) => {
    const b = req.body
    return {
      uid: req.uid,
      reply_to: b.reply_to ? b.reply_to.toLowerCase() : null,
      visibility: b.visibility,
      title: b.title,
      body: b.body,
      lang: b.lang ?? 'en',
      media_url: b.media_url ?? null
    }
  }
  app.post('/posts', postOpts, async (req, reply) => done(reply, await posts.submit(parsePost(req), key32(req.uid))))
  app.post('/n/posts', postOpts, (req, reply) => {
    const p = parsePost(req)
    return execOne(reply, 'posts', N_POSTS, [uuidv7(), p.uid, p.reply_to, p.visibility, p.title, p.body, p.lang, p.media_url])
  })

  const messageOpts = {
    onRequest: auth,
    schema: {
      body: {
        type: 'object',
        required: ['to', 'body'],
        properties: {
          to: uuid,
          body: { type: 'string', minLength: 1, maxLength: 8000 },
          content_type: { type: ['integer', 'null'], minimum: 0, maximum: 3 },
          attachment_url: optStr(1, 500)
        }
      }
    }
  }
  // returns null when the message is invalid beyond the schema (sending to self)
  const parseMessage = (req) => {
    const b = req.body
    const to = b.to.toLowerCase()
    if (to === req.uid) return null
    return { uid: req.uid, to, content_type: b.content_type ?? 0, body: b.body, attachment_url: b.attachment_url ?? null }
  }
  app.post('/messages', messageOpts, async (req, reply) => {
    const m = parseMessage(req)
    if (!m) return fail(reply, 400)
    // symmetric key: both directions of a conversation share one lane
    return done(reply, await messages.submit(m, (key32(m.uid) ^ key32(m.to)) >>> 0))
  })
  app.post('/n/messages', messageOpts, (req, reply) => {
    const m = parseMessage(req)
    if (!m) return fail(reply, 400)
    return execOne(reply, 'messages', N_MESSAGES, [uuidv7(), uuidv7(), m.uid, m.to, m.content_type, m.body, m.attachment_url])
  })

  const likeOpts = {
    onRequest: auth,
    schema: { params: { type: 'object', properties: { id: uuid } } }
  }
  app.post('/posts/:id/like', likeOpts, async (req, reply) => {
    const pid = req.params.id.toLowerCase()
    return done(reply, await likes.submit({ uid: req.uid, post_id: pid }, key32(pid)))
  })
  app.post('/n/posts/:id/like', likeOpts, (req, reply) =>
    execOne(reply, 'likes', N_LIKES, [uuidv7(), req.uid, req.params.id.toLowerCase()]))

  app.listen({ host: env.HOST || '0.0.0.0', port: Number(env.PORT || 8080), backlog: 65535 })
    .then(() => console.error(`${FRAMEWORK} worker pid=${process.pid} cluster.isWorker=${cluster.isWorker} runtime=${RUNTIME} pool=${poolMax} batch_rows=${BATCH_MAX} batch_window_ms=${WINDOW} lanes=${LANES} lane_queue=${LANE_CAP}`))
    .catch((e) => { console.error(e); process.exit(1) })
  process.on('SIGTERM', () => app.close().then(() => sql.end()).then(() => process.exit(0)))
}

if (cluster.isPrimary) {
  console.error(`${FRAMEWORK} primary pid=${process.pid} runtime=${RUNTIME} workers=${WORKERS} pool_per_worker=${POOL_MAX} ` +
    (WORKERS > 1 ? `-> forking ${WORKERS} workers via node:cluster` : '-> single process, no fork'))
}
if (cluster.isPrimary && WORKERS > 1) {
  for (let i = 0; i < WORKERS; i++) cluster.fork()
  console.error(`${FRAMEWORK} primary: cluster.fork() returned ${Object.keys(cluster.workers || {}).length}/${WORKERS} workers`)
  cluster.on('online', (w) => console.error(`${FRAMEWORK} primary: worker pid=${w.process.pid} online`))
  cluster.on('exit', (w, code) => { console.error(`worker ${w.process.pid} exited ${code}`); process.exit(1) })
  const stop = () => { for (const w of Object.values(cluster.workers)) w.kill('SIGTERM'); setTimeout(() => process.exit(0), 3000) }
  process.on('SIGTERM', stop); process.on('SIGINT', stop)
} else {
  start()
}
