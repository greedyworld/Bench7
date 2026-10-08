#!/usr/bin/env python3
"""bench7 load test coordinator (load generator). Started by ../run_k6.sh; stdlib only.

  1. GET http://<ip>:41901/bundle  (seed ids + JWTs, written by run_<fw>.sh on the app host)
  2. start one k6 PAUSED here (progress bar on this terminal) and one on every --agents machine
     (over ssh; their REST API is tunnelled to 127.0.0.1:6566, 6567, ...)
  3. resume all k6 at the same moment; every machine sends 1/N of the target rate
  4. every second: merge the k6 counters of all machines + host metrics (app host :41901/latest)
     + generator CPU/memory -> timeline.ndjson; check the stop rules over a trailing window
  5. first rule that fires stops every k6; then report.py writes report.md

Rate profile (--rps R1,R2,R3 --times T1,T2,T3): linear ramp START -> R1 over [0,T1], R1 -> R2 over [T1,T2],
R2 -> R3 over [T2,T3]. Default times 10,30,120 s. With --warmup S (default 60) the ramp is preceded by S seconds
at a constant --warmup-rps; those seconds go to warmup.ndjson, are never scored, and el = 0 is the ramp start.
--design batching (default) uses the Group 1 routes (batched writes, cached public reads); --design normal
uses the Group 2 routes under /n (one INSERT per request, no cache).
"""
import argparse
import atexit
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
B0, F, NB = 0.2, 1.2, 70  # latency buckets: must match bench.js
EPS = ["public", "private", "list_messages", "send", "create_post", "like"]
CLASSES = ["s0", "s2xx", "s4xx", "s503", "s5xx"]
MIX = "public:56.25,private:6.25,list_messages:12.5,send:15,create_post:10"


def floats(s):
    return [float(x) for x in re.split(r"[,\s]+", s.strip()) if x]


ap = argparse.ArgumentParser(prog="run_k6.sh", description="bench7 load test (k6, open loop, optional extra generators)")
ap.add_argument("--ip", required=True, help="app host IP (private IP from run_<fw>.sh)")
ap.add_argument("--port", type=int, default=8080, help="app port (default 8080)")
ap.add_argument("--rps", required=True, nargs="+",
                help="target rps at the end of each range, e.g. 1000,5000,21000 (or 1000 5000 21000)")
ap.add_argument("--times", default="10,30,120", help="end of each range in seconds (default 10,30,120)")
ap.add_argument("--p95", type=float, default=700, help="stop when p95 latency > this many ms (default 700)")
ap.add_argument("--p99", type=float, default=None, help="stop when p99 latency > this many ms (default off)")
ap.add_argument("--err", type=float, default=1.0, help="stop when errors > this %% of requests (default 1)")
ap.add_argument("--drop", type=float, default=1.0,
                help="stop when k6 drops > this %% of target iterations, i.e. ran out of VUs (default 1)")
ap.add_argument("--cpu", type=float, default=None, help="stop when app host CPU >= this %% (default off)")
ap.add_argument("--util", type=float, default=None, help="stop when app host disk util > this %% (default off)")
ap.add_argument("--window", type=int, default=5, help="stop rules use the trailing N seconds (default 5)")
ap.add_argument("--no-stop", action="store_true", help="never stop early (run the full profile)")
ap.add_argument("--agents", default="", help="extra generators, comma separated user@host (ssh -A or --ssh-key)")
ap.add_argument("--ssh-key", default=None, help="private key for the agents (default: ssh agent / ~/.ssh)")
ap.add_argument("--name", default=None, help="label for the results dir (default: framework from the bundle)")
ap.add_argument("--vus", type=int, default=6000, help="pre-allocated VUs per generator (default 6000)")
ap.add_argument("--max-vus", type=int, default=6000, help="max VUs per generator (default 6000)")
ap.add_argument("--metrics-port", type=int, default=41901, help="app host sampler port (default 41901)")
ap.add_argument("--design", choices=["batching", "normal"], default="batching",
                help="batching = Group 1 routes (batched writes + read cache); normal = Group 2 routes under /n "
                     "(one INSERT per request, no cache). Default batching")
ap.add_argument("--warmup", type=float, default=60, help="warm-up seconds at --warmup-rps before the ramp, not scored (default 60, 0 = off)")
ap.add_argument("--warmup-rps", type=float, default=1000, help="total warm-up rate (default 1000)")
ap.add_argument("--start-rps", type=float, default=None, help="ramp start rate (default: --warmup-rps if warm-up is on, else 0)")
ap.add_argument("--mix", default=MIX, help=f"endpoint weights (default {MIX})")
ap.add_argument("--public-hot", type=int, default=None, help="public reads use the first N seed cursors (k6 default 256)")
ap.add_argument("--public-first-pct", type=float, default=None, help="%% of public reads for the first page (k6 default 20)")
ap.add_argument("--payload-kb", type=float, default=5,
                help="post/message request size in KB (default 5 -> body of 4990 chars); ignored with --body-chars")
ap.add_argument("--body-chars", type=int, default=None, help="post/message body length (overrides --payload-kb)")
ap.add_argument("--timeout", default="10s", help="k6 per-request timeout (default 10s)")
ap.add_argument("--db-ip", default=None, help="Postgres host running its own sampler (2-machine setup); adds DB host metrics")
ap.add_argument("--db-metrics-port", type=int, default=41902, help="DB host sampler port (default 41902)")
ap.add_argument("--meta", action="append", default=[], metavar="KEY=VALUE",
                help="extra labels stored in meta.json (repeatable), e.g. --meta pool=24 --meta knob=GOGC=200")
ap.add_argument("--out", default=os.path.join(HERE, "..", "results"), help="results dir (default ../results)")
a = ap.parse_args()
if a.body_chars is None:
    a.body_chars = int(round(a.payload_kb * 1000)) - 10  # 2 -> 1990, 4 -> 3990, 5 -> 4990 (apps accept up to 8000)

RPS = [x for s in a.rps for x in floats(s)]
TIMES = floats(a.times)
if len(RPS) != len(TIMES) or any(t2 <= t1 for t1, t2 in zip([0] + TIMES, TIMES)):
    sys.exit(f"--rps has {len(RPS)} values and --times {len(TIMES)}; they must match and times must increase")
AGENTS = [x.strip() for x in a.agents.split(",") if x.strip()]
N = 1 + len(AGENTS)
APP = f"http://{a.ip}:{a.port}"
MET = f"http://{a.ip}:{a.metrics_port}"
DBMET = f"http://{a.db_ip}:{a.db_metrics_port}" if a.db_ip else None
PREFIX = "/n" if a.design == "normal" else ""
WARMUP = a.warmup if a.warmup > 0 and a.warmup_rps > 0 else 0
START_RPS = a.start_rps if a.start_rps is not None else (a.warmup_rps if WARMUP else 0)
DURS = [t2 - t1 for t1, t2 in zip([0] + TIMES, TIMES)]
STAGES_TOTAL = ",".join(f"{d:g}:{r:g}" for d, r in zip(DURS, RPS))
STAGES_EACH = ",".join(f"{d:g}:{round(r / N)}" for d, r in zip(DURS, RPS))  # every generator sends 1/N


def log(*m):
    print(*m, flush=True)


def get(url, timeout=3.0, raw=False):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        b = r.read()
    return b if raw else json.loads(b)


def patch_status(api, attrs):
    body = json.dumps({"data": {"type": "status", "id": "default", "attributes": attrs}}).encode()
    req = urllib.request.Request(api + "/v1/status", data=body, method="PATCH", headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.read()


# ---------------- 1. bundle + app check ----------------
try:
    bundle_bytes = get(MET + "/bundle", timeout=30, raw=True)
    bundle_meta = json.loads(bundle_bytes)
except Exception as e:  # noqa: BLE001
    sys.exit(f"cannot get the bundle from {MET}/bundle ({e}).\n"
             f"Start the app on the app host first: ./run_<fw>.sh  (and check the firewall allows :{a.metrics_port})")
try:
    get(APP + PREFIX + "/health", timeout=5, raw=True)
except Exception as e:  # noqa: BLE001
    sys.exit(f"app {APP}{PREFIX}/health not reachable: {e}")
if DBMET:
    try:
        get(DBMET + "/latest", timeout=5)
    except Exception as e:  # noqa: BLE001
        sys.exit(f"DB host sampler {DBMET}/latest not reachable ({e}): ./tune_postgres2.sh on the DB host")
FWNAME = bundle_meta.get("framework", "app")
NAME = a.name or f"{FWNAME}-{a.design}"
RUN_ID = f"{NAME}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
OUT = os.path.abspath(os.path.join(a.out, RUN_ID))
os.makedirs(OUT, exist_ok=True)
BUNDLE = os.path.join(OUT, "bundle.json")
fd = os.open(BUNDLE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "wb") as f:
    f.write(bundle_bytes)
del bundle_bytes, bundle_meta["users"], bundle_meta["public_cursors"], bundle_meta["like_posts"], bundle_meta["tokens"]

def on_exit():  # also runs after a crash: never leave k6 running or JWTs on disk
    k = globals().get("k6")
    if k is not None and k.poll() is None:
        k.kill()
    for ag in globals().get("agents", []):
        if ag.proc is not None and ag.proc.poll() is None:
            ag.proc.kill()  # remote side: stdin EOF -> SIGINT to its k6, trap removes the bundle
        elif ag.proc is None:  # copied but never started
            try:
                ag.ssh(f"rm -rf {RDIR}", timeout=20)
            except subprocess.TimeoutExpired:
                pass
    if os.path.exists(BUNDLE):
        os.remove(BUNDLE)


atexit.register(on_exit)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
signal.signal(signal.SIGHUP, lambda *_: sys.exit(129))

if not shutil.which("k6"):
    sys.exit("k6 is not installed here: ./connect_agent.sh")
k6_version = subprocess.run(["k6", "version"], capture_output=True, text=True).stdout.strip().splitlines()[0]
meta = {"run_id": RUN_ID, "framework": FWNAME, "name": NAME, "app": APP, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rps": RPS, "times": TIMES, "stages": STAGES_TOTAL, "stages_per_generator": STAGES_EACH,
        "stop": {"p95_ms": a.p95, "p99_ms": a.p99, "err_pct": a.err, "drop_pct": a.drop, "cpu_pct": a.cpu,
                 "util_pct": a.util, "window_s": a.window, "disabled": a.no_stop},
        "generators": ["local"] + [f"gen-{i + 2}" for i in range(len(AGENTS))], "vus": a.vus, "max_vus": a.max_vus, "mix": a.mix, "k6": k6_version,
        "bundle_created": bundle_meta.get("created"), "gen_nproc": os.cpu_count(),
        "design": a.design, "prefix": PREFIX, "warmup_s": WARMUP, "warmup_rps": a.warmup_rps if WARMUP else 0,
        "start_rps": START_RPS, "timeout": a.timeout, "db_host": bool(DBMET),
        "k6_knobs": {"public_hot": a.public_hot, "public_first_pct": a.public_first_pct, "body_chars": a.body_chars},
        "app_info": bundle_meta.get("info"), "extra": dict(kv.split("=", 1) if "=" in kv else (kv, True) for kv in a.meta)}
json.dump(meta, open(os.path.join(OUT, "meta.json"), "w"), indent=1)


def k6_env(instance, summary, summary_txt, bundle):
    env = {"BASE": APP, "PREFIX": PREFIX, "BUNDLE": bundle, "STAGES": STAGES_EACH, "START_RPS": f"{START_RPS / N:g}",
           "PRE_VUS": str(a.vus), "MAX_VUS": str(a.max_vus), "SUMMARY": summary, "SUMMARY_TXT": summary_txt,
           "INSTANCE": instance, "MIX": a.mix, "TIMEOUT": a.timeout,
           "WARMUP_S": f"{WARMUP:g}", "WARMUP_RPS": f"{a.warmup_rps / N:g}" if WARMUP else "0"}
    for k, v in (("PUBLIC_HOT", a.public_hot), ("PUBLIC_FIRST_PCT", a.public_first_pct), ("BODY_CHARS", a.body_chars)):
        if v is not None:
            env[k] = str(v)
    return [x for k, v in env.items() for x in ("-e", f"{k}={v}")]


# ---------------- 2. agents (remote k6 over ssh) ----------------
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ServerAliveInterval=10",
       "-o", "ConnectTimeout=15"] + (["-i", a.ssh_key] if a.ssh_key else [])
RDIR = f"/tmp/bench7-{RUN_ID}"


class Agent:
    def __init__(self, host, idx):
        self.host, self.port = host, 6566 + idx
        self.name = f"gen-{idx + 2}"  # label in result files (no user@ip)
        self.api = f"http://127.0.0.1:{self.port}"
        self.proc, self.cpu, self.mem_mb, self.exit, self.prev, self.lines = None, None, None, None, None, []

    def ssh(self, cmd, stdin=None, timeout=60):
        return subprocess.run(SSH + [self.host, cmd], input=stdin, capture_output=True, timeout=timeout)

    def prepare(self):
        r = self.ssh("command -v k6 >/dev/null && k6 version | head -1 && "
                     "! (ss -ltn 2>/dev/null | grep -q '127.0.0.1:6565 ') && echo port-free")
        out = r.stdout.decode()
        if r.returncode != 0 or "port-free" not in out:
            sys.exit(f"agent {self.host}: k6 missing or a k6 is already running there (port 6565 busy).\n"
                     f"{out}{r.stderr.decode()}\nRun ./connect_agent.sh {self.host} here; check ssh with: ssh {self.host} true")
        for name, data in (("bench.js", open(os.path.join(HERE, "bench.js"), "rb").read()), ("bundle.json", open(BUNDLE, "rb").read())):
            r = self.ssh(f"umask 077 && mkdir -p {RDIR} && cat > {RDIR}/{name}", stdin=data, timeout=120)
            if r.returncode:
                sys.exit(f"agent {self.host}: copy {name} failed: {r.stderr.decode()}")

    def start(self):
        envs = " ".join(shlex.quote(x) for x in k6_env(self.name, f"{RDIR}/summary.json", f"{RDIR}/summary.txt", f"{RDIR}/bundle.json"))
        script = f"""cd {RDIR} || exit 3
trap 'rm -f {RDIR}/bundle.json' EXIT   # bench JWTs never outlive the run
trap 'exit 129' HUP TERM
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
exec 3<&0   # ssh stdin: EOF = the coordinator is gone -> stop k6 (async jobs would get /dev/null as stdin)
k6 run --paused --address 127.0.0.1:6565 --no-usage-report --no-color {envs} bench.js > k6.log 2>&1 3<&- &
K=$!
( cat <&3 >/dev/null; kill -INT $K 2>/dev/null ) &
exec 3<&-
W=$!
while kill -0 $K 2>/dev/null; do echo "G $(head -1 /proc/stat) $(awk '/MemAvailable/{{print $2}}' /proc/meminfo)"; sleep 1; done
wait $K; RC=$?
kill $W 2>/dev/null
echo "EXIT $RC"
"""
        # own session: Ctrl-C on this terminal must not kill the ssh before we stop the remote k6 cleanly
        self.proc = subprocess.Popen(SSH + ["-o", "ExitOnForwardFailure=yes", "-L", f"{self.port}:127.0.0.1:6565",
                                            self.host, "bash -c " + shlex.quote(script)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     start_new_session=True)
        threading.Thread(target=self.reader, daemon=True).start()

    def reader(self):
        for raw in self.proc.stdout:
            line = raw.decode(errors="replace").strip()
            if line.startswith("G cpu "):
                f = line.split()
                cur, mem = list(map(int, f[2:10])), int(f[-1])
                if self.prev:
                    d = [y - x for x, y in zip(self.prev, cur)]
                    tot = sum(d)
                    if tot:
                        self.cpu = round(100 * (tot - d[3] - d[4]) / tot, 1)
                self.prev, self.mem_mb = cur, mem // 1024
            elif line.startswith("EXIT "):
                self.exit = int(line.split()[1])
            elif line:
                self.lines.append(line)

    def fetch_results(self):
        for name in ("k6.log", "summary.json", "summary.txt"):
            try:
                r = self.ssh(f"cat {RDIR}/{name}", timeout=120)
                if r.returncode == 0:
                    open(os.path.join(OUT, f"agent-{self.name}-{name}"), "wb").write(r.stdout)
            except subprocess.TimeoutExpired:
                pass
        try:
            self.ssh(f"rm -rf {RDIR}", timeout=30)  # the bundle holds bench JWTs
        except subprocess.TimeoutExpired:
            pass


agents = [Agent(h, i) for i, h in enumerate(AGENTS)]
for ag in agents:
    log(f"agent {ag.host}: copy bench.js + bundle")
    ag.prepare()

# ---------------- 3. start every k6 paused, then resume together ----------------
log(f"\n{NAME}: {APP}{PREFIX}  design: {a.design}  generators: {N}  warm-up: {WARMUP:g}s @ {a.warmup_rps if WARMUP else 0:g} rps  "
    f"profile: {STAGES_TOTAL} (s:rps from {START_RPS:g})  stop: p95>{a.p95:g}ms"
    + (f" p99>{a.p99:g}ms" if a.p99 else "") + f" err>{a.err:g}% drop>{a.drop:g}%" + (" [no-stop]" if a.no_stop else ""))
log(f"results: {OUT}\n")
LOCAL_API = "http://127.0.0.1:6565"
try:
    get(LOCAL_API + "/v1/status", timeout=1)
    sys.exit("a k6 is already running on this machine (127.0.0.1:6565): wait for it or kill it")
except SystemExit:
    raise
except Exception:  # noqa: BLE001
    pass
try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
except (ValueError, OSError):
    pass
for ag in agents:
    ag.start()
# local k6 shares this terminal: its progress bar is the live view
k6 = subprocess.Popen(["k6", "run", "--paused", "--address", "127.0.0.1:6565", "--no-usage-report"]
                      + k6_env("local", os.path.join(OUT, "summary.json"), os.path.join(OUT, "summary.txt"), BUNDLE)
                      + [os.path.join(HERE, "bench.js")])
APIS = [("local", LOCAL_API)] + [(ag.name, ag.api) for ag in agents]
pool = ThreadPoolExecutor(max(4, 2 * N))


def stop_all(reason=None):
    for _, api in APIS:
        pool.submit(lambda u=api: patch_status(u, {"stopped": True}))
    time.sleep(1)


def drop_bundles():
    if os.path.exists(BUNDLE):
        os.remove(BUNDLE)
    for ag in agents:
        try:
            ag.ssh(f"rm -f {RDIR}/bundle.json", timeout=20)
        except subprocess.TimeoutExpired:
            pass



deadline = time.time() + 180
ready = set()
while len(ready) < N and time.time() < deadline:
    if k6.poll() is not None or any(ag.proc.poll() is not None for ag in agents):
        break
    for name, api in APIS:
        try:
            ks = get(api + "/v1/status", timeout=1)["data"]["attributes"]
            # status 4 = paused before run, i.e. all pre-allocated VUs are initialized (1 = still initializing)
            if ks.get("paused") and ks.get("status", 0) >= 4:
                ready.add(name)
        except Exception:  # noqa: BLE001
            pass
    time.sleep(0.5)
if len(ready) < N:
    for ag in agents:
        if ag.proc.poll() is not None:
            log(f"agent {ag.host} exited: " + " | ".join(ag.lines[-5:]))
    stop_all()
    k6.send_signal(signal.SIGINT)
    sys.exit(f"not every k6 became ready (ready: {sorted(ready)})")

list(pool.map(lambda x: patch_status(x[1], {"paused": False}), APIS))
T0 = time.time() + WARMUP  # el = 0 at the start of the ramp (after the warm-up)
threading.Thread(target=drop_bundles, daemon=True).start()  # every k6 has loaded it during init

# ---------------- 4. collect + stop rules ----------------


def pct(h, q):
    tot = sum(h)
    if not tot:
        return None
    rank, c = q * tot, 0
    for i, n in enumerate(h):
        if n and c + n >= rank:
            hi = B0 * F ** i
            frac = (rank - c) / n
            return round(hi * frac if i == 0 else (hi / F) * F ** frac, 3)
        c += n
    return round(B0 * F ** (NB - 1), 3)


def lat_summary(h):
    return {"p50": pct(h, .5), "p90": pct(h, .9), "p95": pct(h, .95), "p99": pct(h, .99), "p999": pct(h, .999)}


def target_at(el):
    if el < 0:
        return a.warmup_rps
    x, lo = el, START_RPS
    for d, hi in zip(DURS, RPS):
        if x <= d:
            return round(lo + (hi - lo) * x / d, 1)
        x, lo = x - d, hi
    return RPS[-1]


def k6_metrics(api):
    out = {}
    for m in get(api + "/v1/metrics", timeout=10)["data"]:
        s = m["attributes"].get("sample") or {}
        out[m["id"]] = s.get("count", s.get("value"))
    return out


def proc_cpu():
    return list(map(int, open("/proc/stat").readline().split()[1:9]))


def mem_avail_mb():
    for line in open("/proc/meminfo"):
        if line.startswith("MemAvailable"):
            return int(line.split()[1]) // 1024
    return None


def fetch_host():
    try:
        return get(MET + "/latest", timeout=1)
    except Exception:  # noqa: BLE001
        return None


def fetch_db():
    try:
        return get(DBMET + "/latest", timeout=1)
    except Exception:  # noqa: BLE001
        return None


out = open(os.path.join(OUT, "timeline.ndjson"), "w", buffering=1)
wout = open(os.path.join(OUT, "warmup.ndjson"), "w", buffering=1) if WARMUP else None
live = open(os.path.join(OUT, "live.log"), "w", buffering=1)
prev = {name: None for name, _ in APIS}
prev_cpu, last_host_t, last_db_t, win, stopped = proc_cpu(), None, None, [], None
in_warmup = bool(WARMUP)
prev_t, next_tick = time.time(), time.time() + 1
interrupted = False


def on_sigint(*_):
    global interrupted
    interrupted = True


signal.signal(signal.SIGINT, on_sigint)  # k6 gets the same Ctrl-C and stops itself; we stop the agents

while k6.poll() is None:
    time.sleep(max(0.0, next_tick - time.time()))
    next_tick += 1
    if time.time() > next_tick:
        next_tick = time.time() + 1
    if interrupted and not stopped:
        stopped = {"el": round(time.time() - T0, 1), "reason": "interrupted (Ctrl-C)"}
        stop_all()
    futs = {name: pool.submit(k6_metrics, api) for name, api in APIS}
    hfut = pool.submit(fetch_host)
    dfut = pool.submit(fetch_db) if DBMET else None
    cur = {}
    for name, fu in futs.items():
        try:
            cur[name] = fu.result()
        except Exception:  # noqa: BLE001
            cur[name] = None  # late: its counts roll into the next tick
    host = hfut.result()
    if host and host.get("t") == last_host_t:
        host = None
    elif host:
        last_host_t = host.get("t")
    dbh = dfut.result() if dfut else None
    if dbh and dbh.get("t") == last_db_t:
        dbh = None
    elif dbh:
        last_db_t = dbh.get("t")
    now = time.time()
    c = proc_cpu()
    d = [y - x for x, y in zip(prev_cpu, c)]
    prev_cpu = c
    gens = [{"name": "local", "cpu": round(100 * (sum(d) - d[3] - d[4]) / sum(d), 1) if sum(d) else None, "mem_mb": mem_avail_mb()}]
    gens += [{"name": ag.name, "cpu": ag.cpu, "mem_mb": ag.mem_mb} for ag in agents]

    tot = {}  # merged per-tick deltas over every generator
    for name in cur:
        if cur[name] is None:
            continue
        if prev[name] is not None:
            for k, v in cur[name].items():
                if isinstance(v, (int, float)) and k not in ("vus", "vus_max"):
                    tot[k] = tot.get(k, 0) + v - (prev[name].get(k) or 0)
        prev[name] = cur[name]
    vus = sum((cur[n] or prev[n] or {}).get("vus") or 0 for n in cur)
    dt = now - prev_t
    prev_t = now
    if not tot:
        continue
    sc = 1 / dt if dt > 0 else 1
    el = round(now - T0, 1)
    st = {k: 0 for k in CLASSES}
    hist_all, ep_out, hist_sparse = [0] * NB, {}, {}
    for ep in EPS:
        es = {k: tot.get(f"st_{ep}_{k}", 0) for k in CLASSES}
        h = [tot.get(f"lat_{ep}_{i}", 0) for i in range(NB)]
        n = sum(es.values())
        for k in CLASSES:
            st[k] += es[k]
        for i, v in enumerate(h):
            hist_all[i] += v
        if n:
            hist_sparse[ep] = {i: v for i, v in enumerate(h) if v}
            ep_out[ep] = {"rps": round(n * sc), "err": n - es["s2xx"], **lat_summary(h)}
    total = sum(st.values())
    rec = {"t": round(now, 3), "el": el, "dt": round(dt, 3), "target_rps": target_at(el), "rps": round(total * sc),
           "ok_rps": round(st["s2xx"] * sc), "err_rps": round((total - st["s2xx"]) * sc), "status": st,
           "lat": lat_summary(hist_all), "ep": ep_out, "hist": hist_sparse,
           "h": {i: v for i, v in enumerate(hist_all) if v}, "vus": vus,
           "dropped_s": round(tot.get("dropped_iterations", 0) * sc),
           "rx_mbs": round(tot.get("data_received", 0) * sc / 2**20, 2), "tx_mbs": round(tot.get("data_sent", 0) * sc / 2**20, 2),
           "cache": {"hit_s": round(tot.get("cache_hit", 0) * sc), "miss_s": round(tot.get("cache_miss", 0) * sc)},
           "gens": gens, "host": host}
    if DBMET:
        rec["db"] = dbh
    if in_warmup and el >= 0:
        in_warmup = False
        win = []  # stop rules only see ramp seconds
        live.write("------ warm-up done, ramp starts ------\n")
    if in_warmup:
        rec["phase"] = "warmup"
        wout.write(json.dumps(rec, separators=(",", ":")) + "\n")
        hc = (host or {}).get("cpu") or {}
        live.write(f"{el:6.1f}s warm-up {rec['target_rps']:6.0f}  rps {rec['rps']:6d}  err {rec['err_rps']:4d}  "
                   f"p95 {rec['lat']['p95'] or 0:7.1f}ms  host cpu {hc.get('busy', 0):5.1f}%\n")
        reason = None
        if host and host.get("disk_free_gb") is not None and host["disk_free_gb"] < 3:
            reason = f"app host disk free {host['disk_free_gb']} GB < 3 GB"
        elif any(g["mem_mb"] is not None and g["mem_mb"] < 1500 for g in gens):
            reason = "a generator has < 1.5 GB free memory (too many VUs)"
        if reason and not stopped:
            stopped = {"el": el, "reason": reason + " (during warm-up)", "target_rps": rec["target_rps"]}
            stop_all()
        continue

    # ---- stop rules over the trailing window ----
    win.append({"h": hist_all, "n": total, "err": total - st["s2xx"], "drop": tot.get("dropped_iterations", 0),
                "target": rec["target_rps"] * dt, "cpu": ((host or {}).get("cpu") or {}).get("busy"),
                "gen_cpu": max((g["cpu"] or 0) for g in gens),
                "util": ((host or {}).get("disk") or {}).get("util")})
    win = win[-a.window:]
    wh = [sum(w["h"][i] for w in win) for i in range(NB)]
    wn = sum(w["n"] for w in win)
    avg = lambda k: (lambda v: sum(v) / len(v) if len(v) >= a.window * 0.6 else None)([w[k] for w in win if w[k] is not None])
    W = {"p95": pct(wh, .95), "p99": pct(wh, .99), "err_pct": round(100 * sum(w["err"] for w in win) / wn, 3) if wn else 0,
         "drop_pct": round(100 * sum(w["drop"] for w in win) / max(1, sum(w["target"] for w in win)), 3),
         "cpu": avg("cpu"), "gen_cpu": avg("gen_cpu"), "util": avg("util")}
    rec["win"] = {k: (round(v, 2) if isinstance(v, float) else v) for k, v in W.items()}
    reason = None
    if host and host.get("disk_free_gb") is not None and host["disk_free_gb"] < 3:
        reason = f"app host disk free {host['disk_free_gb']} GB < 3 GB"
    elif any(g["mem_mb"] is not None and g["mem_mb"] < 1500 for g in gens):
        reason = "a generator has < 1.5 GB free memory (too many VUs)"
    elif len(win) == a.window and el >= a.window:
        if W["p95"] is not None and W["p95"] > a.p95:
            reason = f"p95 {W['p95']:.1f} ms > {a.p95:g} ms"
        elif a.p99 and W["p99"] is not None and W["p99"] > a.p99:
            reason = f"p99 {W['p99']:.1f} ms > {a.p99:g} ms"
        elif W["err_pct"] > a.err:
            reason = f"errors {W['err_pct']:.2f}% > {a.err:g}%"
        elif W["drop_pct"] > a.drop:
            reason = f"k6 dropped {W['drop_pct']:.2f}% of target iterations > {a.drop:g}% (all VUs busy)"
        elif a.cpu and W["cpu"] is not None and W["cpu"] >= a.cpu:
            reason = f"app host CPU {W['cpu']:.1f}% >= {a.cpu:g}%"
        elif W["gen_cpu"] is not None and W["gen_cpu"] >= 90:
            reason = f"generator CPU {W['gen_cpu']:.1f}% >= 90% (the generator is the bottleneck: add --agents)"
        elif a.util and W["util"] is not None and W["util"] > a.util:
            reason = f"app host disk util {W['util']:.1f}% > {a.util:g}%"
        if reason:
            reason += f" over the last {a.window} s"
    if reason and not stopped and not a.no_stop:
        stopped = {"el": el, "reason": reason, "target_rps": rec["target_rps"], "window": rec["win"]}
        rec["stop"] = reason
        stop_all()
    out.write(json.dumps(rec, separators=(",", ":")) + "\n")
    hc = (host or {}).get("cpu") or {}
    dc = (dbh or {}).get("cpu") or {}
    live.write(f"{el:6.1f}s target {rec['target_rps']:8.0f}  rps {rec['rps']:6d}  err {rec['err_rps']:4d}  "
               f"p95 {rec['lat']['p95'] or 0:7.1f}ms  p99 {rec['lat']['p99'] or 0:7.1f}ms  vus {vus:5d}  drop {rec['dropped_s']:4d}  "
               f"host cpu {hc.get('busy', 0):5.1f}%" + (f"  db cpu {dc.get('busy', 0):5.1f}%" if DBMET else "")
               + f"  gen cpu {' '.join(str(g['cpu']) for g in gens)}"
               + (f"  STOP: {reason}" if rec.get("stop") else "") + "\n")

k6_rc = k6.wait()
signal.signal(signal.SIGINT, signal.SIG_DFL)
stop_all()
for ag in agents:
    try:
        ag.proc.stdin.close()  # remote: EOF -> SIGINT to its k6 if it is still running
    except OSError:
        pass
for ag in agents:
    try:
        ag.proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        ag.proc.kill()
    ag.fetch_results()
pool.shutdown(wait=False)
if not stopped:
    stopped = {"el": round(time.time() - T0, 1), "reason": "completed (no stop rule fired)"}
stopped["k6_exit"] = k6_rc
json.dump(stopped, open(os.path.join(OUT, "stop.json"), "w"), indent=1)
log(f"\nSTOP at {stopped['el']} s: {stopped['reason']}")
subprocess.run([sys.executable, os.path.join(HERE, "report.py"), OUT])
