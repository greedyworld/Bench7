#!/usr/bin/env python3
"""bench7 server sampler (app + Postgres host). 1 Hz, stdlib only, run as root.

Writes one JSON line per second to --out and serves the latest sample at
GET http://<host>:<port>/latest (used by the load generator's stop-rule watcher).

Per second: machine CPU (total + per core), load, memory, disk I/O on the /data
device, network, TCP, free disk; cgroup CPU/memory/IO/pids of the app unit and of
Postgres; Postgres activity (TPS, tuples, cache hits, WAL, checkpoints, pg_stat_io,
backend states and wait events, lock waits, autovacuum). Every 10 s: DB size and
dead tuples.
"""
import argparse
import glob
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--port", type=int, default=41901)
ap.add_argument("--data", default="/data", help="mount point whose device is sampled")
ap.add_argument("--app-unit", default="bench7-app.service")
ap.add_argument("--db", default="bench")
ap.add_argument("--no-pg", action="store_true", help="no local Postgres (2-machine setup: the DB host runs its own sampler)")
ap.add_argument("--bundle", help="loadgen bundle json served at GET /bundle (seed ids + bench JWTs for k6)")
args = ap.parse_args()

CLK = os.sysconf("SC_CLK_TCK")
NCPU = os.cpu_count() or 1


def read(p):
    with open(p) as f:
        return f.read()


def read_bytes(p):
    with open(p, "rb") as f:
        return f.read()


def data_device():
    src = subprocess.run(["findmnt", "-no", "SOURCE", args.data], capture_output=True, text=True).stdout.strip()
    return os.path.basename(src) or "nvme0n1p1"


DEV = data_device()


def net_iface():
    for line in read("/proc/net/route").splitlines()[1:]:
        f = line.split()
        if f[1] == "00000000":
            return f[0]
    return "eth0"


IFACE = net_iface()


def cg_dir(name_glob):
    hits = glob.glob(f"/sys/fs/cgroup/system.slice/**/{name_glob}", recursive=True)
    return hits[0] if hits else None


# ---------------- raw readers (cumulative counters) ----------------
def r_stat():
    cpus, out = {}, {}
    for line in read("/proc/stat").splitlines():
        f = line.split()
        if f[0].startswith("cpu"):
            cpus[f[0]] = list(map(int, f[1:9]))  # user nice system idle iowait irq softirq steal
        elif f[0] in ("ctxt", "intr", "procs_running", "procs_blocked"):
            out[f[0]] = int(f[1])
    out["cpus"] = cpus
    return out


def r_diskstats():
    for line in read("/proc/diskstats").splitlines():
        f = line.split()
        if f[2] == DEV:
            v = list(map(int, f[3:14]))
            return {"rd": v[0], "rsec": v[2], "rms": v[3], "wr": v[4], "wsec": v[6], "wms": v[7],
                    "inflight": v[8], "ioms": v[9], "wtms": v[10]}
    return None


def r_netdev():
    for line in read("/proc/net/dev").splitlines()[2:]:
        name, rest = line.split(":", 1)
        if name.strip() == IFACE:
            v = list(map(int, rest.split()))
            return {"rxb": v[0], "rxp": v[1], "rxdrop": v[3], "txb": v[8], "txp": v[9], "txdrop": v[11]}
    return None


def r_snmp():
    out = {}
    for path in ("/proc/net/snmp", "/proc/net/netstat"):
        lines = read(path).splitlines()
        for hdr, val in zip(lines[::2], lines[1::2]):
            k, names = hdr.split(":", 1)
            _, vals = val.split(":", 1)
            if k in ("Tcp", "TcpExt"):
                for n, x in zip(names.split(), vals.split()):
                    out[n] = int(x)
    keep = ("ActiveOpens", "PassiveOpens", "RetransSegs", "InSegs", "OutSegs", "CurrEstab", "InErrs", "OutRsts",
            "ListenOverflows", "ListenDrops", "TCPTimeouts", "TCPBacklogDrop")
    return {k: out.get(k, 0) for k in keep}


def r_sockstat():
    out = {}
    for line in read("/proc/net/sockstat").splitlines():
        if line.startswith("TCP:"):
            f = line.split()[1:]
            out = {f[i]: int(f[i + 1]) for i in range(0, len(f), 2)}
    return out


def r_meminfo():
    m = {}
    for line in read("/proc/meminfo").splitlines():
        k, v = line.split(":", 1)
        m[k] = int(v.split()[0])  # kB
    return m


def r_vmstat():
    want = ("pgmajfault", "pswpin", "pswpout", "pgpgin", "pgpgout")
    out = {}
    for line in read("/proc/vmstat").splitlines():
        k, v = line.split()
        if k in want:
            out[k] = int(v)
    return out


def r_cgroup(d):
    if not d or not os.path.isdir(d):
        return None
    out = {}
    try:
        for line in read(f"{d}/cpu.stat").splitlines():
            k, v = line.split()
            out[k] = int(v)
        out["mem"] = int(read(f"{d}/memory.current"))
        ms = dict(l.split() for l in read(f"{d}/memory.stat").splitlines())
        out["anon"] = int(ms.get("anon", 0))
        out["file"] = int(ms.get("file", 0))
        out["pids"] = int(read(f"{d}/pids.current"))
        rb = wb = rio = wio = 0
        for line in read(f"{d}/io.stat").splitlines():
            kv = dict(x.split("=") for x in line.split()[1:])
            rb += int(kv.get("rbytes", 0)); wb += int(kv.get("wbytes", 0))
            rio += int(kv.get("rios", 0)); wio += int(kv.get("wios", 0))
        out.update(io_rb=rb, io_wb=wb, io_r=rio, io_w=wio)
    except (FileNotFoundError, ProcessLookupError, ValueError):
        return None
    return out


# ---------------- Postgres (one persistent psql) ----------------
PG_FAST = """SELECT json_build_object(
 'db', (SELECT row_to_json(d) FROM (SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned,
        tup_fetched, tup_inserted, tup_updated, tup_deleted, temp_files, temp_bytes, deadlocks, conflicts
        FROM pg_stat_database WHERE datname = current_database()) d),
 'act', (SELECT coalesce(json_object_agg(k, n), '{}') FROM (SELECT coalesce(state, 'none') || ':' ||
        coalesce(wait_event_type || '.' || wait_event, 'cpu') AS k, count(*) AS n FROM pg_stat_activity
        WHERE backend_type = 'client backend' AND datname = current_database() AND pid <> pg_backend_pid() GROUP BY 1) a),
 'autovac', (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker'),
 'lock_wait', (SELECT count(*) FROM pg_locks WHERE NOT granted),
 'wal', (SELECT row_to_json(w) FROM (SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM pg_stat_wal) w),
 'ckpt', (SELECT row_to_json(c) FROM (SELECT num_timed, num_requested, buffers_written, write_time, sync_time
        FROM pg_stat_checkpointer) c),
 'bgw', (SELECT row_to_json(b) FROM (SELECT buffers_clean, maxwritten_clean FROM pg_stat_bgwriter) b),
 'io', (SELECT row_to_json(i) FROM (SELECT sum(reads)::int8 AS reads, sum(read_bytes)::int8 AS read_bytes,
        sum(writes)::int8 AS writes, sum(write_bytes)::int8 AS write_bytes, sum(extends)::int8 AS extends,
        sum(fsyncs)::int8 AS fsyncs, sum(hits)::int8 AS hits, sum(evictions)::int8 AS evictions,
        sum(read_time)::float8 AS read_time, sum(write_time)::float8 AS write_time, sum(fsync_time)::float8 AS fsync_time
        FROM pg_stat_io) i));"""
PG_SLOW = """SELECT json_build_object(
 'db_bytes', pg_database_size(current_database()),
 'tables', (SELECT json_object_agg(relname, json_build_object('live', n_live_tup, 'dead', n_dead_tup,
        'bytes', pg_total_relation_size(relid))) FROM pg_stat_user_tables
        WHERE relname IN ('users', 'posts', 'messages', 'likes', 'conversations')));"""


class Pg:
    def __init__(self):
        self.p = None
        self.err = open(args.out + ".psql.err", "a")
        self.start()

    def start(self):
        self.p = subprocess.Popen(["runuser", "-u", "postgres", "--", "psql", "-X", "-q", "-A", "-t", "-d", args.db],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.err, text=True, bufsize=1)

    def query(self, sql):
        try:
            self.p.stdin.write(sql.replace("\n", " ") + "\n\\echo __END__\n")
            self.p.stdin.flush()
            res = None
            while True:
                line = self.p.stdout.readline()
                if not line:
                    raise BrokenPipeError
                line = line.rstrip("\n")
                if line == "__END__":
                    return res
                if line.startswith("{"):
                    res = json.loads(line)
        except (BrokenPipeError, OSError, ValueError):
            try:
                self.p.kill()
            except OSError:
                pass
            self.start()
            return None


# ---------------- derive per-second rates ----------------
def delta(a, b, k):
    return (b.get(k, 0) - a.get(k, 0)) if a and b else 0


def cpu_pct(a, b):
    d = [y - x for x, y in zip(a, b)]
    tot = sum(d) or 1
    user, nice, system, idle, iowait, irq, softirq, steal = d
    return {"busy": round(100 * (tot - idle - iowait) / tot, 1), "user": round(100 * (user + nice) / tot, 1),
            "sys": round(100 * system / tot, 1), "irq": round(100 * (irq + softirq) / tot, 1),
            "iowait": round(100 * iowait / tot, 1), "steal": round(100 * steal / tot, 1),
            "idle": round(100 * idle / tot, 1)}


def cg_rate(a, b, dt):
    if not a or not b:
        return None
    return {"cpu_pct": round(delta(a, b, "usage_usec") / 1e4 / dt, 1),  # 100 = one full core
            "user_pct": round(delta(a, b, "user_usec") / 1e4 / dt, 1),
            "sys_pct": round(delta(a, b, "system_usec") / 1e4 / dt, 1),
            "throttled": delta(a, b, "nr_throttled"),
            "mem_mb": round(b["mem"] / 2**20, 1), "anon_mb": round(b["anon"] / 2**20, 1),
            "file_mb": round(b["file"] / 2**20, 1), "pids": b["pids"],
            "io_r_mbs": round(delta(a, b, "io_rb") / 2**20 / dt, 2), "io_w_mbs": round(delta(a, b, "io_wb") / 2**20 / dt, 2),
            "io_r_iops": round(delta(a, b, "io_r") / dt), "io_w_iops": round(delta(a, b, "io_w") / dt)}


def pg_rate(a, b, dt):
    if not a or not b or not a.get("db") or not b.get("db"):
        return None
    da, db_ = a["db"], b["db"]
    r = lambda k: round((db_[k] - da[k]) / dt, 1)
    hits, reads = db_["blks_hit"] - da["blks_hit"], db_["blks_read"] - da["blks_read"]
    out = {"backends": db_["numbackends"], "tps": r("xact_commit"), "rollback_s": r("xact_rollback"),
           "blks_read_s": r("blks_read"), "blks_hit_s": r("blks_hit"),
           "hit_pct": round(100 * hits / (hits + reads), 2) if hits + reads else None,
           "tup_returned_s": r("tup_returned"), "tup_fetched_s": r("tup_fetched"), "tup_inserted_s": r("tup_inserted"),
           "tup_updated_s": r("tup_updated"), "tup_deleted_s": r("tup_deleted"), "temp_bytes_s": r("temp_bytes"),
           "deadlocks": db_["deadlocks"] - da["deadlocks"], "act": b.get("act"), "autovac": b.get("autovac"),
           "lock_wait": b.get("lock_wait")}
    wa, wb = a.get("wal") or {}, b.get("wal") or {}
    out.update(wal_mbs=round((float(wb.get("wal_bytes", 0)) - float(wa.get("wal_bytes", 0))) / 2**20 / dt, 2),
               wal_rec_s=round(delta(wa, wb, "wal_records") / dt), wal_fpi_s=round(delta(wa, wb, "wal_fpi") / dt),
               wal_buffers_full=delta(wa, wb, "wal_buffers_full"))
    ca, cb = a.get("ckpt") or {}, b.get("ckpt") or {}
    out.update(ckpt_timed=delta(ca, cb, "num_timed"), ckpt_req=delta(ca, cb, "num_requested"),
               ckpt_buf_s=round(delta(ca, cb, "buffers_written") / dt), ckpt_total=cb.get("num_timed", 0) + cb.get("num_requested", 0))
    ba, bb = a.get("bgw") or {}, b.get("bgw") or {}
    out.update(bgw_clean_s=round(delta(ba, bb, "buffers_clean") / dt))
    ia, ib = a.get("io") or {}, b.get("io") or {}
    io = lambda k: (ib.get(k) or 0) - (ia.get(k) or 0)
    out.update(io_read_mbs=round(io("read_bytes") / 2**20 / dt, 2), io_write_mbs=round(io("write_bytes") / 2**20 / dt, 2),
               io_reads_s=round(io("reads") / dt), io_writes_s=round(io("writes") / dt), io_extends_s=round(io("extends") / dt),
               io_fsyncs_s=round(io("fsyncs") / dt), io_evictions_s=round(io("evictions") / dt))
    return out


# ---------------- HTTP /latest ----------------
LATEST = {"b": b"{}"}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        code, body = 404, b'{"error":"not_found"}'
        if self.path.startswith("/latest"):
            code, body = 200, LATEST["b"]
        elif self.path.startswith("/bundle") and args.bundle:
            try:
                code, body = 200, read_bytes(args.bundle)
            except OSError:
                pass
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
threading.Thread(target=srv.serve_forever, daemon=True).start()

# ---------------- main loop ----------------
pg = None if args.no_pg else Pg()
app_cg = cg_dir(args.app_unit)
pg_cg = cg_dir("postgresql@*-main.service") or cg_dir("postgresql.service")
os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
out = open(args.out, "a", buffering=1)


def snap():
    return {"t": time.time(), "stat": r_stat(), "disk": r_diskstats(), "net": r_netdev(), "tcp": r_snmp(),
            "app": r_cgroup(app_cg), "pg_cg": r_cgroup(pg_cg), "pg": pg.query(PG_FAST) if pg else None, "vm": r_vmstat()}


prev = snap()
slow, n = None, 0
while True:
    time.sleep(max(0.0, 1.0 - (time.time() % 1.0)))
    if app_cg is None or not os.path.isdir(app_cg or ""):
        app_cg = cg_dir(args.app_unit)
    cur = snap()
    dt = cur["t"] - prev["t"] or 1.0
    if pg and n % 10 == 0:
        slow = pg.query(PG_SLOW)
    n += 1
    s1, s0 = cur["stat"], prev["stat"]
    cores = {k: cpu_pct(s0["cpus"][k], v)["busy"] for k, v in s1["cpus"].items() if k != "cpu" and k in s0["cpus"]}
    d0, d1 = prev["disk"], cur["disk"]
    disk = None
    if d0 and d1:
        rd, wr = delta(d0, d1, "rd"), delta(d0, d1, "wr")
        disk = {"dev": DEV, "r_iops": round(rd / dt), "w_iops": round(wr / dt),
                "r_mbs": round(delta(d0, d1, "rsec") * 512 / 2**20 / dt, 2), "w_mbs": round(delta(d0, d1, "wsec") * 512 / 2**20 / dt, 2),
                "r_await_ms": round(delta(d0, d1, "rms") / rd, 2) if rd else 0.0,
                "w_await_ms": round(delta(d0, d1, "wms") / wr, 2) if wr else 0.0,
                "util": round(min(100.0, delta(d0, d1, "ioms") / (dt * 10)), 1),
                "aqu": round(delta(d0, d1, "wtms") / (dt * 1000), 2), "inflight": d1["inflight"]}
    n0, n1 = prev["net"], cur["net"]
    net = None
    if n0 and n1:
        net = {"iface": IFACE, "rx_mbit": round(delta(n0, n1, "rxb") * 8 / 1e6 / dt, 2), "tx_mbit": round(delta(n0, n1, "txb") * 8 / 1e6 / dt, 2),
               "rx_pps": round(delta(n0, n1, "rxp") / dt), "tx_pps": round(delta(n0, n1, "txp") / dt),
               "drops": delta(n0, n1, "rxdrop") + delta(n0, n1, "txdrop")}
    t0, t1 = prev["tcp"], cur["tcp"]
    sock = r_sockstat()
    tcp = {"estab": t1["CurrEstab"], "inuse": sock.get("inuse"), "tw": sock.get("tw"), "orphan": sock.get("orphan"),
           "active_open_s": round(delta(t0, t1, "ActiveOpens") / dt), "passive_open_s": round(delta(t0, t1, "PassiveOpens") / dt),
           "retrans_s": round(delta(t0, t1, "RetransSegs") / dt), "rst_s": round(delta(t0, t1, "OutRsts") / dt),
           "listen_overflows": delta(t0, t1, "ListenOverflows"), "listen_drops": delta(t0, t1, "ListenDrops"),
           "backlog_drops": delta(t0, t1, "TCPBacklogDrop"), "timeouts": delta(t0, t1, "TCPTimeouts")}
    mi = r_meminfo()
    vfs = os.statvfs(args.data)
    v0, v1 = prev["vm"], cur["vm"]
    la = read("/proc/loadavg").split()
    sample = {
        "t": round(cur["t"], 3),
        "cpu": cpu_pct(s0["cpus"]["cpu"], s1["cpus"]["cpu"]), "cores": cores, "ncpu": NCPU,
        "load1": float(la[0]), "running": s1["procs_running"], "blocked": s1["procs_blocked"],
        "ctxt_s": round(delta(s0, s1, "ctxt") / dt), "intr_s": round(delta(s0, s1, "intr") / dt),
        "mem": {"total_mb": mi["MemTotal"] // 1024, "avail_mb": mi["MemAvailable"] // 1024, "cached_mb": mi["Cached"] // 1024,
                "dirty_mb": mi["Dirty"] // 1024, "writeback_mb": mi["Writeback"] // 1024,
                "majfault_s": round(delta(v0, v1, "pgmajfault") / dt), "swap_s": delta(v0, v1, "pswpin") + delta(v0, v1, "pswpout")},
        "disk": disk, "disk_free_gb": round(vfs.f_bavail * vfs.f_frsize / 2**30, 2), "net": net, "tcp": tcp,
        "app": cg_rate(prev["app"], cur["app"], dt), "pgproc": cg_rate(prev["pg_cg"], cur["pg_cg"], dt),
        "pg": pg_rate(prev["pg"], cur["pg"], dt),
    }
    if slow:
        sample["pg_size"] = slow
        slow = None
    line = json.dumps(sample, separators=(",", ":"))
    out.write(line + "\n")
    LATEST["b"] = line.encode()
    prev = cur
