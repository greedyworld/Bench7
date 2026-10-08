#!/usr/bin/env python3
"""bench7 report: compact, climb-only. stdlib only.

  python3 report.py <run-dir>                    -> <run-dir>/report.md + report.json (run_k6.sh runs this)
  python3 report.py --compare <run-dir> ... [-o compare.md]

The 1 Hz timeline is grouped in 5 s steps; a step is listed only while the achieved RPS is still climbing
(every row is a new high), so the report shows how throughput, latency and every server resource grew
with load, without the noise of the ramp-down / stop.
"""
import argparse
import json
import os
import sys

B0, F, NB = 0.2, 1.2, 70
STEP = 5


def pct(h, q):
    tot = sum(h)
    if not tot:
        return None
    rank, c = q * tot, 0
    for i, n in enumerate(h):
        if n and c + n >= rank:
            hi = B0 * F ** i
            frac = (rank - c) / n
            return hi * frac if i == 0 else (hi / F) * F ** frac
        c += n
    return B0 * F ** (NB - 1)


def hist(sparse):
    h = [0] * NB
    for i, v in (sparse or {}).items():
        h[int(i)] += v
    return h


def dig(d, *ks):
    for k in ks:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def mean(vals):
    v = [x for x in vals if isinstance(x, (int, float))]
    return sum(v) / len(v) if v else None


HOST_FIELDS = {  # name -> path in the sampler's /latest sample
    "cpu": ("cpu", "busy"), "user": ("cpu", "user"), "sys": ("cpu", "sys"), "irq": ("cpu", "irq"),
    "iowait": ("cpu", "iowait"), "steal": ("cpu", "steal"), "load1": ("load1",), "ctxt_s": ("ctxt_s",),
    "app_cpu": ("app", "cpu_pct"), "app_mem": ("app", "mem_mb"), "app_pids": ("app", "pids"),
    "pg_cpu": ("pgproc", "cpu_pct"), "pg_mem": ("pgproc", "mem_mb"),
    "tps": ("pg", "tps"), "hit": ("pg", "hit_pct"), "backends": ("pg", "backends"), "tup_ins": ("pg", "tup_inserted_s"),
    "wal": ("pg", "wal_mbs"), "lock_wait": ("pg", "lock_wait"),
    "util": ("disk", "util"), "r_iops": ("disk", "r_iops"), "w_iops": ("disk", "w_iops"),
    "r_await": ("disk", "r_await_ms"), "w_await": ("disk", "w_await_ms"), "w_mbs": ("disk", "w_mbs"),
    "mem_avail": ("mem", "avail_mb"), "rx_mbit": ("net", "rx_mbit"), "tx_mbit": ("net", "tx_mbit"),
    "estab": ("tcp", "estab"), "retrans": ("tcp", "retrans_s"),
}
PG_KEYS = ("pg_cpu", "pg_mem", "tps", "hit", "backends", "tup_ins", "wal", "lock_wait")
DB_FIELDS = {  # DB host (2-machine setup), same sampler
    "db_cpu": ("cpu", "busy"), "db_iowait": ("cpu", "iowait"), "db_util": ("disk", "util"),
    "db_r_iops": ("disk", "r_iops"), "db_w_iops": ("disk", "w_iops"), "db_w_await": ("disk", "w_await_ms"),
    "db_mem_avail": ("mem", "avail_mb"), "db_rx_mbit": ("net", "rx_mbit"), "db_tx_mbit": ("net", "tx_mbit"),
}


def steps(rows, stop_el):
    out = {}
    for r in rows:
        if r.get("el") is None or (stop_el is not None and r["el"] > stop_el + 0.5):
            continue
        out.setdefault(int(max(0, r["el"] - 0.001) // STEP), []).append(r)
    res = []
    for b in sorted(out):
        rs = out[b]
        if len(rs) < 3:  # a step the stop cut short
            continue
        dt = sum(r["dt"] for r in rs) or 1
        n = sum(sum(r["status"].values()) for r in rs)
        ok = sum(r["status"].get("s2xx", 0) for r in rs)
        h = [sum(x) for x in zip(*(hist(r.get("h")) for r in rs))]
        eps = {}
        for ep in {e for r in rs for e in r.get("hist", {})}:
            eh = [sum(x) for x in zip(*(hist(dig(r, "hist", ep)) for r in rs))]
            en = sum(dig(r, "ep", ep, "rps") or 0 for r in rs)  # per-second values
            eerr = sum(dig(r, "ep", ep, "err") or 0 for r in rs)
            eps[ep] = {"rps": en / len(rs), "err": eerr, "p50": pct(eh, .5), "p95": pct(eh, .95), "p99": pct(eh, .99)}
        s = {"t": (b + 1) * STEP, "target": rs[-1]["target_rps"], "rps": n / dt, "ok": ok / dt,
             "err_pct": 100 * (n - ok) / n if n else 0.0, "p50": pct(h, .5), "p95": pct(h, .95), "p99": pct(h, .99),
             "vus": max(r.get("vus") or 0 for r in rs), "drop": sum(r.get("dropped_s") or 0 for r in rs) / len(rs),
             "gen_cpu": mean([max((g.get("cpu") or 0) for g in r.get("gens", [])) for r in rs]),
             "rx_mbs": mean([r.get("rx_mbs") for r in rs]),
             "hit_s": sum(dig(r, "cache", "hit_s") or 0 for r in rs), "miss_s": sum(dig(r, "cache", "miss_s") or 0 for r in rs),
             "eps": eps}
        for k, path in HOST_FIELDS.items():
            # Postgres on its own machine: its process + activity numbers come from the DB host sampler
            src = "db" if k in PG_KEYS and any(r.get("db") for r in rs) else "host"
            s[k] = mean([dig(r.get(src) or {}, *path) for r in rs])
        for k, path in DB_FIELDS.items():
            s[k] = mean([dig(r.get("db") or {}, *path) for r in rs])
        res.append(s)
    return res


def f(x, nd=0, unit=""):
    if x is None:
        return "-"
    return f"{x:,.{nd}f}{unit}"


def analyze(run):
    meta = json.load(open(os.path.join(run, "meta.json")))
    stop = json.load(open(os.path.join(run, "stop.json"))) if os.path.exists(os.path.join(run, "stop.json")) else {}
    rows = [json.loads(l) for l in open(os.path.join(run, "timeline.ndjson")) if l.strip()]
    lim = meta["stop"]
    st = steps(rows, stop.get("el"))
    climb, best = [], -1
    for s in st:
        if s["rps"] > best:
            climb.append(s)
            best = s["rps"]
    ok_slo = [s for s in st if s["p95"] is not None and s["p95"] <= lim["p95_ms"] and s["err_pct"] <= lim["err_pct"]
              and (not lim.get("p99_ms") or (s["p99"] or 0) <= lim["p99_ms"])]
    slo = max(ok_slo, key=lambda s: s["ok"]) if ok_slo else None
    peak = max(st, key=lambda s: s["rps"]) if st else None
    summ = {"name": meta["name"], "framework": meta["framework"], "run_id": meta["run_id"], "started": meta["started"],
            "design": meta.get("design", "batching"), "db_host": meta.get("db_host", False), "warmup_s": meta.get("warmup_s", 0),
            "extra": meta.get("extra") or {},
            "generators": len(meta["generators"]), "stages": meta["stages"], "stop_reason": stop.get("reason"),
            "stop_el": stop.get("el"), "slo_rps": slo and round(slo["ok"]), "peak_rps": peak and round(peak["rps"])}
    if slo:
        summ.update(slo_t=slo["t"], slo_p50=slo["p50"], slo_p95=slo["p95"], slo_p99=slo["p99"], slo_cpu=slo["cpu"],
                    slo_app_cpu=slo["app_cpu"], slo_pg_cpu=slo["pg_cpu"], slo_util=slo["util"], slo_gen_cpu=slo["gen_cpu"],
                    slo_db_cpu=slo["db_cpu"], slo_db_util=slo["db_util"], slo_tps=slo["tps"],
                    app_cpu_per_krps=slo["app_cpu"] / (slo["ok"] / 1000) if slo["app_cpu"] and slo["ok"] else None,
                    app_mem=slo["app_mem"])
    return meta, stop, st, climb, slo, peak, summ


def bottleneck(s, gens):
    if s is None:
        return "-"
    if (s["gen_cpu"] or 0) >= 85:
        return f"load generator CPU ({s['gen_cpu']:.0f}%) - add --agents"
    if (s.get("db_cpu") or 0) >= 93:
        return f"DB host CPU {s['db_cpu']:.0f}% (Postgres {s['pg_cpu'] or 0:.0f}% of one core)"
    if (s.get("db_util") or 0) >= 85:
        return f"DB host disk (util {s['db_util']:.0f}%)"
    if (s["cpu"] or 0) >= 93:
        if s.get("db_cpu") is not None:
            return f"app host CPU {s['cpu']:.0f}% (app {s['app_cpu'] or 0:.0f}% of one core; Postgres is on the DB host at {s['db_cpu']:.0f}%)"
        who = "app" if (s["app_cpu"] or 0) >= (s["pg_cpu"] or 0) else "Postgres"
        return (f"app host CPU {s['cpu']:.0f}% (app {s['app_cpu']:.0f}% + Postgres {s['pg_cpu']:.0f}% of one core; "
                f"biggest consumer: {who})")
    if (s["util"] or 0) >= 85 or (s["iowait"] or 0) >= 15:
        return f"disk (util {s['util']:.0f}%, iowait {s['iowait']:.0f}%)"
    return "none saturated at this step (latency / queueing limit)"


def write_report(run):
    meta, stop, st, climb, slo, peak, summ = analyze(run)
    L = []
    w = L.append
    w(f"# {meta['name']} - bench7 load test\n")
    w(f"run `{meta['run_id']}` started {meta['started']} | app {meta['app']} | generators: {', '.join(meta['generators'])}\n")
    w("## Result\n")
    w("| | |\n|---|---|")
    w(f"| **max RPS within SLO** | **{f(summ['slo_rps'])}** (5 s step ending t={slo['t'] if slo else '-'} s; p95 {f(slo and slo['p95'], 1)} ms, "
      f"p99 {f(slo and slo['p99'], 1)} ms, errors {f(slo and slo['err_pct'], 2)} %) |")
    w(f"| peak achieved RPS | {f(summ['peak_rps'])} |")
    w(f"| stopped | t={f(stop.get('el'), 1)} s: {stop.get('reason', '-')} |")
    w(f"| bottleneck at max SLO RPS | {bottleneck(slo, meta['generators'])} |")
    if summ.get("app_cpu_per_krps"):
        w(f"| app CPU per 1k RPS | {summ['app_cpu_per_krps']:.1f} % of one core |")
    w("")
    w("## Test setup\n")
    lim = meta["stop"]
    design = meta.get("design", "batching")
    w(f"- design **{design}**: " + ("Group 1 routes (writes batched per table, public pages served from an in-process cache)"
                                   if design == "batching" else
                                   "Group 2 routes under `/n` (one INSERT statement per write request, every read hits Postgres, no cache)"))
    w("- Postgres: " + ("on its own machine (DB host metrics below)" if meta.get("db_host") else "on the app host"))
    if meta.get("warmup_s"):
        w(f"- warm-up: {meta['warmup_s']:g} s at {meta['warmup_rps']:g} rps before the ramp (not scored; t=0 is the ramp start)")
    if meta.get("extra"):
        w("- labels: " + ", ".join(f"`{k}={v}`" for k, v in meta["extra"].items()))
    w(f"- load profile (s:rps, linear ramps from {meta.get('start_rps', 0):g}): `{meta['stages']}`, open loop (k6 ramping-arrival-rate), "
      f"{len(meta['generators'])} generator(s), {meta['vus']}..{meta['max_vus']} VUs each")
    w(f"- stop rules (trailing {lim['window_s']} s): p95 > {lim['p95_ms']:g} ms" + (f", p99 > {lim['p99_ms']:g} ms" if lim.get("p99_ms") else "")
      + f", errors > {lim['err_pct']:g} %, dropped iterations > {lim['drop_pct']:g} %, generator CPU >= 90 %"
      + (f", disk util > {lim['util_pct']:g} %" if lim.get("util_pct") else "")
      + (f", app host CPU >= {lim['cpu_pct']:g} %" if lim.get("cpu_pct") else "") + (" (DISABLED)" if lim.get("disabled") else ""))
    bc = dig(meta, "k6_knobs", "body_chars") or 1990
    w(f"- mix `{meta['mix']}` (75 % reads, 25 % writes of ~{(bc + 10) / 1000:g} KB, body {bc} chars)")
    w(f"- {meta['k6']}")
    w("")
    w(f"## Climb ({STEP} s steps, only while RPS keeps rising)\n")
    w("| t s | target | RPS | ok RPS | err % | p50 ms | p95 ms | p99 ms | VUs | dropped/s | gen CPU % | cache hit % |")
    w("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for s in climb:
        hm = s["hit_s"] + s["miss_s"]
        w(f"| {s['t']} | {f(s['target'])} | {f(s['rps'])} | {f(s['ok'])} | {f(s['err_pct'], 2)} | {f(s['p50'], 1)} | {f(s['p95'], 1)} | "
          f"{f(s['p99'], 1)} | {f(s['vus'])} | {f(s['drop'])} | {f(s['gen_cpu'])} | {f(100 * s['hit_s'] / hm if hm else None, 1)} |")
    w("")
    w("## Server (app host) at the same steps\n")
    w("CPU columns are % of the whole machine; app / Postgres CPU are % of ONE core (200 = both vCPUs).\n")
    w("| t s | RPS | CPU % | user | sys | irq | iowait | app CPU | app MB | PG CPU | PG TPS | PG hit % | disk util % | r/w IOPS | WAL MB/s | net tx Mbit | TCP estab | retrans/s | mem free MB |")
    w("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for s in climb:
        w(f"| {s['t']} | {f(s['rps'])} | {f(s['cpu'], 1)} | {f(s['user'])} | {f(s['sys'])} | {f(s['irq'])} | {f(s['iowait'], 1)} | "
          f"{f(s['app_cpu'])} | {f(s['app_mem'])} | {f(s['pg_cpu'])} | {f(s['tps'])} | {f(s['hit'], 1)} | {f(s['util'])} | "
          f"{f(s['r_iops'])}/{f(s['w_iops'])} | {f(s['wal'], 1)} | {f(s['tx_mbit'])} | {f(s['estab'])} | {f(s['retrans'], 1)} | {f(s['mem_avail'])} |")
    w("")
    if meta.get("db_host"):
        w("## DB host at the same steps\n")
        w("| t s | RPS | CPU % | iowait | PG CPU | PG TPS | PG hit % | backends | disk util % | r/w IOPS | w await ms | WAL MB/s | net rx/tx Mbit | mem free MB |")
        w("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for s in climb:
            w(f"| {s['t']} | {f(s['rps'])} | {f(s['db_cpu'], 1)} | {f(s['db_iowait'], 1)} | {f(s['pg_cpu'])} | {f(s['tps'])} | {f(s['hit'], 1)} | "
              f"{f(s['backends'])} | {f(s['db_util'])} | {f(s['db_r_iops'])}/{f(s['db_w_iops'])} | {f(s['db_w_await'], 2)} | {f(s['wal'], 1)} | "
              f"{f(s['db_rx_mbit'])}/{f(s['db_tx_mbit'])} | {f(s['db_mem_avail'])} |")
        w("")
    if slo:
        w(f"## Endpoints at max SLO RPS (step ending t={slo['t']} s)\n")
        w("| endpoint | RPS | share % | errors | p50 ms | p95 ms | p99 ms |")
        w("|---|---:|---:|---:|---:|---:|---:|")
        tot = sum(e["rps"] for e in slo["eps"].values()) or 1
        for ep, e in sorted(slo["eps"].items(), key=lambda x: -x[1]["rps"]):
            w(f"| {ep} | {f(e['rps'])} | {f(100 * e['rps'] / tot, 1)} | {e['err']} | {f(e['p50'], 1)} | {f(e['p95'], 1)} | {f(e['p99'], 1)} |")
        w("")
    w("## Files\n")
    w("`timeline.ndjson` (1 Hz: merged k6 counters, latency histograms per endpoint, app host + generator metrics), "
      "`live.log` (1 line/s), `summary.json`/`summary.txt` (k6), `agent-*` (other generators), `meta.json`, `stop.json`.")
    open(os.path.join(run, "report.md"), "w").write("\n".join(L) + "\n")
    json.dump(summ, open(os.path.join(run, "report.json"), "w"), indent=1)
    return summ


def compare(runs, out):
    rows = []
    for r in runs:
        try:
            rows.append(write_report(r))
        except (OSError, ValueError, KeyError) as e:
            print(f"skip {r}: {e}", file=sys.stderr)
    rows.sort(key=lambda s: -(s.get("slo_rps") or 0))
    L = ["# bench7 comparison\n",
         "Max RPS within SLO = best 5 s step with p95 within the limit and errors within the limit.\n",
         "| # | name | design | DB | max SLO RPS | peak RPS | p95 ms @SLO | host CPU % @SLO | app CPU/1k RPS | PG CPU @SLO | DB host CPU % | PG TPS | disk util % | gen CPU % | stop |",
         "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for i, s in enumerate(rows, 1):
        L.append(f"| {i} | {s['name']} | {s.get('design', 'batching')} | {'own host' if s.get('db_host') else 'same host'} | "
                 f"**{f(s.get('slo_rps'))}** | {f(s.get('peak_rps'))} | {f(s.get('slo_p95'), 1)} | {f(s.get('slo_cpu'))} | "
                 f"{f(s.get('app_cpu_per_krps'), 1)} | {f(s.get('slo_pg_cpu'))} | {f(s.get('slo_db_cpu'))} | {f(s.get('slo_tps'))} | "
                 f"{f(s.get('slo_util'))} | {f(s.get('slo_gen_cpu'))} | "
                 f"t={f(s.get('stop_el'))} s {s.get('stop_reason') or ''} |")
    txt = "\n".join(L) + "\n"
    if out:
        open(out, "w").write(txt)
    print(txt)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("-o", "--out")
    a = ap.parse_args()
    if a.compare:
        compare(a.runs, a.out)
    else:
        for r in a.runs:
            s = write_report(r)
            print(f"\n{s['name']}: max RPS within SLO {f(s.get('slo_rps'))}, peak {f(s.get('peak_rps'))}, "
                  f"stopped t={f(s.get('stop_el'), 1)} s: {s.get('stop_reason')}")
            print(f"report: {os.path.join(r, 'report.md')}")
