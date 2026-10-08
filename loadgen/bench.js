// bench7 k6 load script: open-loop (ramping-arrival-rate). Started by run_k6.sh (one k6 per generator).
//
// Env:
//   BASE          app base url            (default http://127.0.0.1:8080)
//   PREFIX        route group: "" = design "batching" (batched writes + cached public reads),
//                 "/n" = design "normal" (one INSERT per request, no cache)          (default "")
//   WARMUP_S / WARMUP_RPS / WARMUP_VUS   constant-rate warm-up before the ramp (default 0 = none)
//   TIMEOUT       per-request timeout (default 10s)
//   BUNDLE        bundle json from the app host's /bundle: {users, public_cursors, like_posts, tokens:[{uid,tok}]}
//   STAGES        "dur_s:target,..." piecewise-linear ramp from START_RPS (default 10:1000,20:5000,90:21000)
//   START_RPS     (default 0)
//   PRE_VUS / MAX_VUS        (default 2000 / 4000)
//   SUMMARY       json summary output path (default summary.json)
//   MIX           "public:56.25,private:6.25,list_messages:12.5,send:15,create_post:10"
//                 = 75% reads (75% of them cache-served public pages, 25% direct Postgres reads)
//                 + 25% batched ~2 KB writes
//   PUBLIC_FIRST_PCT  % of public reads hitting the first page (default 20)
//   PUBLIC_HOT    public reads use only the first N seed cursors so they stay cache hits (default 256)
//   BODY_CHARS    message/post body length (default 1990 -> ~2 KB JSON request)
//
// Per-endpoint status-class counters and log-bucket latency counters are exported so the
// collector can compute per-second rates and windowed percentiles from the REST API.
import http from 'k6/http';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

const E = __ENV;
const BASE = (E.BASE || 'http://127.0.0.1:8080') + (E.PREFIX || '');
const num = (k, d) => (E[k] !== undefined && E[k] !== '' ? Number(E[k]) : d);
const TIMEOUT = E.TIMEOUT || '10s';

// One SharedArray per list so every element is shared across VUs (indexing one returns a small copy).
// The bundle is written on the app host by run_<fw>.sh (tokens minted there; k6 never sees the issuer key).
const seedArr = (name, key) => new SharedArray(name, () => JSON.parse(open(E.BUNDLE || './bundle.json'))[key]);
const USERS = seedArr('users', 'users');
const CURSORS = seedArr('cursors', 'public_cursors');
const LIKES = seedArr('likes', 'like_posts');
const AUTH = seedArr('auth', 'tokens');
const BODY_CHARS = num('BODY_CHARS', 1990);
const PUBLIC_FIRST = num('PUBLIC_FIRST_PCT', 20) / 100;
const PUBLIC_HOT = Math.min(num('PUBLIC_HOT', 256), CURSORS.length);

// ---- weighted mix ----
const EPS = ['public', 'private', 'list_messages', 'send', 'create_post', 'like'];
const MIX = Object.fromEntries((E.MIX || 'public:56.25,private:6.25,list_messages:12.5,send:15,create_post:10')
  .split(',').map((kv) => kv.split(':')).map(([k, v]) => [k.trim(), Number(v)]));
for (const k of Object.keys(MIX)) if (!EPS.includes(k)) throw new Error('unknown endpoint in MIX: ' + k);
const CUM = [];
{
  let acc = 0;
  const tot = EPS.reduce((s, k) => s + (MIX[k] || 0), 0);
  for (const k of EPS) { acc += (MIX[k] || 0) / tot; CUM.push([acc, k]); }
}

// ---- metrics ----
// latency buckets: upper bound of bucket i = B0 * F^i ms (i = 0..NB-1); last bucket is +inf
const B0 = 0.2, F = 1.2, NB = 70;
const LNF = Math.log(F);
const lat = {}, st = {};
const CLASSES = ['s0', 's2xx', 's4xx', 's503', 's5xx'];
for (const ep of EPS) {
  lat[ep] = [];
  for (let i = 0; i < NB; i++) lat[ep].push(new Counter(`lat_${ep}_${i}`));
  st[ep] = {};
  for (const c of CLASSES) st[ep][c] = new Counter(`st_${ep}_${c}`);
}
const cacheHit = new Counter('cache_hit');
const cacheMiss = new Counter('cache_miss');

function record(ep, res) {
  const ms = res.timings.duration;
  let b = ms <= B0 ? 0 : Math.ceil(Math.log(ms / B0) / LNF);
  if (b >= NB) b = NB - 1;
  lat[ep][b].add(1);
  const s = res.status;
  st[ep][s === 0 ? 's0' : s < 300 ? 's2xx' : s === 503 ? 's503' : s < 500 ? 's4xx' : 's5xx'].add(1);
}

// ---- payloads ----
const ALPHA = 'abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789 .,;!?';
function randText(n) {
  const a = new Array(n);
  for (let i = 0; i < n; i++) a[i] = ALPHA.charAt((Math.random() * ALPHA.length) | 0);
  return a.join('');
}
// Each VU keeps 8 bodies, copied at init from a pool built once and shared by all VUs. (Building
// them in every VU took most of the ~45 s k6 needed to initialise 6,250 VUs; reading the
// SharedArray per request would cost ~12 us, a plain array <1 us.) A random 12-char prefix
// makes each row unique.
const POOL = new SharedArray('bodies', () => Array.from({ length: 4096 }, () => randText(BODY_CHARS - 12)));
const pick = (a) => a[(Math.random() * a.length) | 0];
const BODIES = Array.from({ length: 8 }, () => pick(POOL));
const uniq = () => Math.random().toString(36).slice(2, 8) + Date.now().toString(36).slice(-6);
const body = () => uniq().padEnd(12, 'x') + pick(BODIES);

// ---- options ----
const STAGES = (E.STAGES || '10:1000,20:5000,90:21000')
  .split(',').map((s) => s.split(':').map(Number)).map(([d, t]) => ({ duration: `${d}s`, target: Math.round(t) }));
const WARMUP_S = num('WARMUP_S', 0);
const WARMUP_RPS = num('WARMUP_RPS', 0);
const scenarios = {
  ramp: {
    executor: 'ramping-arrival-rate', exec: 'run',
    startRate: num('START_RPS', 0), timeUnit: '1s',
    preAllocatedVUs: num('PRE_VUS', 2000), maxVUs: num('MAX_VUS', 4000),
    stages: STAGES,
    startTime: `${WARMUP_S > 0 && WARMUP_RPS > 0 ? WARMUP_S : 0}s`,
    tags: { phase: 'ramp' },
  },
};
if (WARMUP_S > 0 && WARMUP_RPS > 0) {
  // own small VU pool: warm-up VUs are separate from the ramp's (k6 does not share VUs between scenarios)
  const wv = num('WARMUP_VUS', Math.max(100, Math.ceil(WARMUP_RPS / 2)));
  scenarios.warmup = {
    executor: 'constant-arrival-rate', exec: 'run',
    rate: Math.round(WARMUP_RPS), timeUnit: '1s', duration: `${WARMUP_S}s`,
    preAllocatedVUs: wv, maxVUs: wv * 2,
    tags: { phase: 'warmup' },
  };
}
export const options = {
  discardResponseBodies: true,
  systemTags: ['status', 'method', 'scenario'],
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  insecureSkipTLSVerify: true,
  scenarios,
};

export function run() {
  const r = Math.random();
  let ep = CUM[CUM.length - 1][1];
  for (const [c, k] of CUM) if (r < c) { ep = k; break; }
  const a = pick(AUTH);
  const me = { uid: a.uid, h: { authorization: a.tok }, hj: { authorization: a.tok, 'content-type': 'application/json' } };
  const p = { tags: { ep }, timeout: TIMEOUT };
  let res;
  switch (ep) {
    case 'public': {
      const url = Math.random() < PUBLIC_FIRST ? `${BASE}/posts/public`
        : `${BASE}/posts/public?before=${CURSORS[(Math.random() * PUBLIC_HOT) | 0]}`;
      res = http.get(url, p);
      const xc = res.headers['X-Cache'];
      if (xc === 'hit') cacheHit.add(1); else if (xc === 'miss') cacheMiss.add(1);
      break;
    }
    case 'private':
      p.headers = me.h;
      res = http.get(`${BASE}/posts/private`, p);
      break;
    case 'list_messages':
      p.headers = me.h;
      res = http.get(`${BASE}/messages`, p);
      break;
    case 'send': {
      let i = (Math.random() * USERS.length) | 0, to = USERS[i];
      if (to === me.uid) to = USERS[(i + 1) % USERS.length];
      p.headers = me.hj;
      res = http.post(`${BASE}/messages`, JSON.stringify({ to, body: body() }), p);
      break;
    }
    case 'create_post': {
      p.headers = me.hj;
      const post = { title: 'post ' + uniq(), body: body(), visibility: Math.random() < 0.9 ? 0 : 1 };
      if (Math.random() < 0.2) post.reply_to = pick(LIKES);
      res = http.post(`${BASE}/posts`, JSON.stringify(post), p);
      break;
    }
    case 'like':
      p.headers = me.h;
      res = http.post(`${BASE}/posts/${pick(LIKES)}/like`, null, p);
      break;
  }
  record(ep, res);
}

// Compact end-of-test summary on the terminal (the default one would list all ~450 bucket counters).
export function handleSummary(d) {
  const m = d.metrics, v = (k, f) => (m[k] && m[k].values[f] !== undefined ? m[k].values[f] : 0);
  const f1 = (x) => Number(x).toFixed(1), n0 = (x) => String(Math.round(x)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  const secs = d.state.testRunDurationMs / 1000;
  const lines = [
    '',
    `bench7 k6 summary (${E.INSTANCE || 'local'})  duration ${f1(secs)} s`,
    `  requests     ${n0(v('http_reqs', 'count'))}  (avg ${n0(v('http_reqs', 'rate'))}/s over the whole test)`,
    `  failed       ${(100 * v('http_req_failed', 'rate')).toFixed(3)} %`,
    `  latency ms   avg ${f1(v('http_req_duration', 'avg'))}  p50 ${f1(v('http_req_duration', 'med'))}  p90 ${f1(v('http_req_duration', 'p(90)'))}` +
      `  p95 ${f1(v('http_req_duration', 'p(95)'))}  p99 ${f1(v('http_req_duration', 'p(99)'))}  max ${f1(v('http_req_duration', 'max'))}`,
    `  dropped      ${n0(v('dropped_iterations', 'count'))} iterations (k6 had no free VU)`,
    `  VUs          max ${n0(v('vus_max', 'max'))}`,
    `  data         recv ${f1(v('data_received', 'count') / 2 ** 20)} MiB  sent ${f1(v('data_sent', 'count') / 2 ** 20)} MiB`,
    '  per endpoint (requests / non-2xx):',
  ];
  for (const ep of EPS) {
    const tot = CLASSES.reduce((s, c) => s + v(`st_${ep}_${c}`, 'count'), 0);
    if (tot) lines.push(`    ${ep.padEnd(14)} ${n0(tot).padStart(12)} / ${n0(tot - v(`st_${ep}_s2xx`, 'count'))}`);
  }
  const txt = lines.join('\n') + '\n';
  return { stdout: txt, [E.SUMMARY || 'summary.json']: JSON.stringify(d), [E.SUMMARY_TXT || 'summary.txt']: txt };
}
