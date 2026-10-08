# bench7 test report (2026-10-05)

Machines: app host and DB host are 2 vCPU / 4 GB (local NVMe for /data); load generators are
8 vCPU. Addresses are placeholders: `<app-ip>`, `<db-ip>`, `<gen-N-ip>`. Commands: [howto.readme](howto.readme).

Every run: seed `./seed_db.sh --size-gb 3` (3 GB, 2-2.5 min), warm-up 30 s at 1000 RPS, then
`--rps 1000,5000,21000,50000 --times 10,30,120,285`. Score = highest 5 s step with p95 <= 700 ms,
p99 <= 1000 ms, errors and dropped iterations <= 1 %. Errors were 0 in every run.

## Script check

The full sequence (tune_linux, setup_postgres, tune_postgres1/2, seed_db, setup_toolchain,
run_/kill_, connect_agent, run_k6, reset) was run on clean machines (reset in between).
Issue found and fixed: the Spring app no longer compiled after the COPY path was removed
(two imports had gone with it).

## Test 1: 1-VM (app + Postgres on the app host), 2 generators

Profile 1vm: pool 12, 1 lane, 200 rows, 20 ms window.

| framework / run | write size | max RPS within SLO | stop reason | app host CPU at max |
|---|---|---:|---|---|
| axum batching | 5 KB | 14,889 | p95 | 99 % (app 84 % + PG 106 % of 1 core) |
| axum batching | 2 KB | **16,612** | none (end of ramp) | 99 % |
| axum normal (Group 2) | 5 KB | 4,323 | p95 | 93 % |
| springboot batching | 5 KB | 7,772 | p95 | 78 % |
| springboot batching | 2 KB | 9,659 | p95 | 86 % |
| springboot, 5 ms window | 2 KB | 9,825 | p95 | - |
| springboot, DB_CONCURRENCY=10 | 2 KB | 9,712 | p95 | - |

- 1-VM is CPU bound: app and Postgres share 2 vCPUs, so the 1vm profile keeps one lane and a small
  pool. axum 16.6k vs 17.6k in older runs (those used the 29 GB seed and a 5 ms window).
- Spring stops on latency below 90 % CPU. The batching window (5 vs 20 ms) and capping reader
  connections (DB_CONCURRENCY=10) change nothing, so it is not pool starvation. Virtual threads were
  on in the old runs too. It is a Spring-side queueing limit; left as is.

## Test 2: 2-VM (Postgres on the DB host)

Profile 2vm: pool 24, 4 lanes, 1000 rows, 20 ms window. axum, batching.

| generators | write size | max RPS within SLO | stop reason | app CPU | DB CPU | DB iowait | DB disk util | WAL MB/s |
|---:|---|---:|---|---:|---:|---:|---:|---:|
| 2 | 5 KB | 24,497 | p95 | 84 % | 82 % | 16 % | 76 % | 37 |
| 2 | 2 KB | 24,368 | **generator CPU 91 %** | 79 % | 78 % | 10 % | 38 % | 18 |
| 4 | 2 KB | **30,112** | p95 | 89 % | 86 % | 2 % | 24 % | 22 |
| 4 | 3 KB | 28,471 | p95 | 89 % | 86 % | 5 % | 47 % | 27 |
| 4 | 4 KB | 28,015 | p95 | 87 % | 88 % | 7 % | 47 % | 36 |
| 4 | 5 KB | 24,231 | p95 | ~80 % | 81 % | 18 % | 82 % | 38 |
| 4 | 2 KB, faster bgwriter | 30,553 | p95 | 88 % | 86 % | 4 % | 26 % | 22 |
| 4 | 2 KB, p95 limit 3 s | 30,978 | k6 dropped > 1 % | 92 % | 83-91 % | 3 % | 26 % | 22 |

- With 2 generators the 2 KB run is limited by the generators, not the servers. With 4 it reaches
  30.1k (older best 31.9k, same shape, 29 GB seed), so 2-VM needs 4 generators.
- Ceiling: with the latency rule relaxed, throughput flattens at ~31k while p95 jumps to 1.4 s.
  So 30.1-30.5k within SLO is ~97 % of what two 2-vCPU hosts deliver. Both hosts stop at ~86-89 %
  CPU because on 2 vCPUs queueing latency rises steeply past that; the last few % of CPU only add
  latency. Reads served from Postgres wait ~170 ms and writes ~300 ms at that point.
- Not the limit at 2-4 KB: DB disk (24-47 % util, write await 5-7 ms), network (~0.8 Gbit/s),
  batching window, Postgres background writer (backend page writes fell 85 %, RPS +1.5 % = noise).

## Payload size

- Write size: the apps used to cap bodies at 4000 chars; the cap is now 8000, so `--payload-kb 5`
  works with no other change. Cost vs 2 KB: 3 KB -5 %, 4 KB -7 %, 5 KB -20 % on 2-VM (at 5 KB the
  WAL rate doubles, the 2-vCPU DB host's disk reaches ~80 % util and commit latency doubles);
  -10 % on 1-VM (CPU bound there).
- Read size is fixed by the apps (5 items per page, 210/240-char previews, ~2.5 KB per response),
  the same in every run. Making it bigger means changing all 8 apps; it would cost app CPU
  (network softirq is already ~28 % of the app host at 30k RPS), not bandwidth.

## Batching window

20 ms in both profiles. 1-VM Spring: 5 ms vs 20 ms within noise. Older 2-VM runs: 5-200 ms
windows within ~7 %. 20 ms keeps write latency low without hurting throughput.

## To go further

Bigger VMs (4 vCPU) for app and DB: the knee moves right and latency stays flat longer. On 2 vCPU
the remaining headroom is ~3 %.
