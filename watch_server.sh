#!/usr/bin/env bash
# App host, while a framework runs: live view of the host, the app and Postgres. Reads the metrics sampler
# started by ./run_<framework>.sh (:41901/latest), so it adds no extra load.
#   ./watch_server.sh               dashboard redrawn in place every second
#   ./watch_server.sh --log         one line per second (scrolls; good for tee / copy-paste)
#   ./watch_server.sh --port 41902  another sampler, e.g. the DB host's (./tune_postgres2.sh starts it)
# Ctrl-C to quit. --log columns: host CPU (busy, user, sys, irq, iowait) | app and Postgres CPU (% of one core)
# and RAM | free RAM | disk IOPS, MB/s, write await, util | network Mbit/s and packets/s |
# TCP established, retransmits/s | Postgres TPS, cache hit %, backends, lock waits
set -euo pipefail
PORT=${METRICS_PORT:-41901} MODE=live
while [ $# -gt 0 ]; do
  case "$1" in
    --log) MODE=log ;;
    --port) PORT=${2:?--port N}; shift ;;
    -h | --help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown option $1 (--log, --port N)" >&2; exit 2 ;;
  esac
  shift
done
if [ "$MODE" = log ]; then
exec python3 - "$PORT" <<'EOF'
import json, signal, sys, time, urllib.request

signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
url = f"http://127.0.0.1:{sys.argv[1]}/latest"
H = (f"{'time':>8} | {'cpu%':>5} {'usr':>4} {'sys':>4} {'irq':>4} {'iow':>4} | {'app%':>5} {'appMB':>6} | "
     f"{'pg%':>5} {'pgMB':>6} | {'freeMB':>6} | {'rIOPS':>6} {'wIOPS':>6} {'rMB/s':>6} {'wMB/s':>6} {'wawt':>5} {'util':>5} | "
     f"{'rxMbit':>7} {'txMbit':>7} {'rxpps':>7} {'txpps':>7} | {'estab':>6} {'retx':>5} | {'pgTPS':>6} {'hit%':>6} {'bknd':>4} {'lockw':>5}")
n, last = 0, None
while True:
    try:
        d = json.load(urllib.request.urlopen(url, timeout=2))
    except Exception:
        print(f"no sampler on {url}: start a framework with ./run_<fw>.sh", flush=True)
        time.sleep(2)
        continue
    if not d.get("cpu") or d.get("t") == last:
        time.sleep(0.2)
        continue
    last = d["t"]
    if n % 20 == 0:
        print(H, flush=True)
    n += 1
    c, a, p, k, nt, t, pg, m = (d.get(x) or {} for x in ("cpu", "app", "pgproc", "disk", "net", "tcp", "pg", "mem"))
    f = lambda v, w, fmt="{:.0f}": (fmt.format(v) if isinstance(v, (int, float)) else "-").rjust(w)
    print(f"{time.strftime('%H:%M:%S', time.localtime(d['t']))} | {f(c.get('busy'), 5, '{:.1f}')} {f(c.get('user'), 4)} "
          f"{f(c.get('sys'), 4)} {f(c.get('irq'), 4)} {f(c.get('iowait'), 4)} | {f(a.get('cpu_pct'), 5)} {f(a.get('mem_mb'), 6)} | "
          f"{f(p.get('cpu_pct'), 5)} {f(p.get('mem_mb'), 6)} | {f(m.get('avail_mb'), 6)} | {f(k.get('r_iops'), 6)} {f(k.get('w_iops'), 6)} "
          f"{f(k.get('r_mbs'), 6, '{:.1f}')} {f(k.get('w_mbs'), 6, '{:.1f}')} {f(k.get('w_await_ms'), 5, '{:.1f}')} {f(k.get('util'), 5)} | "
          f"{f(nt.get('rx_mbit'), 7)} {f(nt.get('tx_mbit'), 7)} {f(nt.get('rx_pps'), 7)} {f(nt.get('tx_pps'), 7)} | "
          f"{f(t.get('estab'), 6)} {f(t.get('retrans_s'), 5)} | {f(pg.get('tps'), 6)} {f(pg.get('hit_pct'), 6, '{:.1f}')} "
          f"{f(pg.get('backends'), 4)} {f(pg.get('lock_wait'), 5)}", flush=True)
EOF
fi
exec python3 - "$PORT" <<'EOF'
import json, signal, sys, time, urllib.request

url = f"http://127.0.0.1:{sys.argv[1]}/latest"
out = sys.stdout


def bye(*_):
    out.write("\033[?25h\n")  # show the cursor again
    sys.exit(0)


signal.signal(signal.SIGINT, bye)
signal.signal(signal.SIGTERM, bye)


def f(v, fmt="{:,.0f}"):
    return fmt.format(v) if isinstance(v, (int, float)) else "-"


def bar(pct, width=30, full=100):
    n = int(round(width * min(max(pct or 0, 0), full) / full))
    return "[" + "#" * n + "." * (width - n) + "]"


peak = {}


def pk(key, v):
    if isinstance(v, (int, float)):
        peak[key] = max(peak.get(key, v), v)
    return f(peak.get(key))


out.write("\033[2J\033[?25l")  # clear once, hide the cursor
last = None
while True:
    try:
        d = json.load(urllib.request.urlopen(url, timeout=2))
    except Exception:
        out.write(f"\033[H\033[Jno sampler on {url}: start a framework with ./run_<fw>.sh  (retrying)\n")
        out.flush()
        time.sleep(2)
        continue
    if not d.get("cpu") or d.get("t") == last:
        time.sleep(0.2)
        continue
    last = d["t"]
    c, a, p, k, nt, t, pg, m = (d.get(x) or {} for x in ("cpu", "app", "pgproc", "disk", "net", "tcp", "pg", "mem"))
    ncpu = d.get("ncpu") or 1
    cores = d.get("cores") or {}
    core_line = "  ".join(f"{name} {f(v, '{:5.1f}')} %" for name, v in sorted(cores.items()))
    act = pg.get("act")
    active = sum(v for s, v in act.items() if s.startswith("active")) if isinstance(act, dict) else act
    lines = [
        f"bench7 app host   {time.strftime('%H:%M:%S', time.localtime(d['t']))}   ({ncpu} vCPU)   Ctrl-C to quit",
        "",
        f"                     now                                      peak",
        f"HOST CPU   {bar(c.get('busy'))} {f(c.get('busy'), '{:5.1f}')} %          {pk('cpu', c.get('busy'))} %",
        f"           user {f(c.get('user'))} %  sys {f(c.get('sys'))} %  irq {f(c.get('irq'))} %  iowait {f(c.get('iowait'))} %"
        f"  steal {f(c.get('steal'))} %  load {f(d.get('load1'), '{:.1f}')}",
        f"           {core_line}",
        "",
        f"APP        {bar(a.get('cpu_pct'), full=100 * ncpu)} {f(a.get('cpu_pct'), '{:5.0f}')} % of a core   {pk('app', a.get('cpu_pct'))} %",
        f"           RAM {f(a.get('mem_mb'))} MB   processes/threads {f(a.get('pids'))}",
        f"POSTGRES   {bar(p.get('cpu_pct'), full=100 * ncpu)} {f(p.get('cpu_pct'), '{:5.0f}')} % of a core   {pk('pg', p.get('cpu_pct'))} %",
        f"           RAM {f(p.get('mem_mb'))} MB   TPS {f(pg.get('tps'))}   cache hit {f(pg.get('hit_pct'), '{:.2f}')} %"
        f"   backends {f(pg.get('backends'))}   active {f(active)}   lock waits {f(pg.get('lock_wait'))}",
        f"           rows/s  inserted {f(pg.get('tup_inserted_s'))}  updated {f(pg.get('tup_updated_s'))}"
        f"  fetched {f(pg.get('tup_fetched_s'))}   WAL {f(pg.get('wal_mbs'), '{:.1f}')} MB/s",
        "",
        f"MEMORY     free {f(m.get('avail_mb'))} MB of {f(m.get('total_mb'))} MB   dirty {f(m.get('dirty_mb'))} MB"
        f"   major faults/s {f(m.get('majfault_s'))}   swap/s {f(m.get('swap_s'))}",
        f"DISK       read  {f(k.get('r_iops')):>7} IOPS {f(k.get('r_mbs'), '{:7.1f}')} MB/s   await {f(k.get('r_await_ms'), '{:.2f}')} ms",
        f"           write {f(k.get('w_iops')):>7} IOPS {f(k.get('w_mbs'), '{:7.1f}')} MB/s   await {f(k.get('w_await_ms'), '{:.2f}')} ms"
        f"   util {f(k.get('util'))} %   free {f(d.get('disk_free_gb'), '{:.1f}')} GB",
        f"NETWORK    rx {f(nt.get('rx_mbit')):>6} Mbit/s {f(nt.get('rx_pps')):>8} pkt/s      peak rx {pk('rx', nt.get('rx_mbit'))} Mbit/s",
        f"           tx {f(nt.get('tx_mbit')):>6} Mbit/s {f(nt.get('tx_pps')):>8} pkt/s      peak tx {pk('tx', nt.get('tx_mbit'))} Mbit/s",
        f"TCP        established {f(t.get('estab'))}   new/s {f(t.get('passive_open_s'))}   retransmits/s {f(t.get('retrans_s'))}"
        f"   resets/s {f(t.get('rst_s'))}   listen drops {f(t.get('listen_drops'))}",
    ]
    out.write("\033[H" + "".join(line + "\033[K\n" for line in lines) + "\033[J")
    out.flush()
EOF
