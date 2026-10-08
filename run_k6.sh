#!/usr/bin/env bash
# Step 6, on the main load generator: one load test against the app started with ./run_<framework>.sh.
#
#   ./run_k6.sh --ip <app-private-ip> --port 8080 --rps 1000,5000,21000,50000 --times 10,30,120,285 --warmup 30
#       open loop: ramp to 1000 rps by t=10 s, 5000 by 30 s, 21000 by 120 s, 50000 by 285 s; stops at the first limit
#   --db-ip <db-private-ip>   2-VM setup: also record the DB host metrics (sampler of ./tune_postgres2.sh)
#   --payload-kb 5            size of each post/message write (default 5 KB; 2 = the older ~2 KB runs)
#   --design batching|normal  Group 1 (batched writes + cache, default) or Group 2 (/n routes, plain)
#   stop rules: --p95 700 --p99 1000 --err 1 --drop 1 --cpu 95 --util 95 --window 5 | --no-stop
#   --agents user@<gen-2-ip>[,user@host...]  more generators (default: the ones saved by ./connect_agent.sh;
#                                            --agents '' = this machine only)
#   --name label --vus 6000 --max-vus 6000 --meta k=v ...    all options: ./run_k6.sh --help
# Results: results/<name>-<time>/report.md (+ timeline.ndjson, live.log, k6 summaries)
set -euo pipefail
ulimit -n 1048576 2>/dev/null || ulimit -n "$(ulimit -Hn)"
HERE=$(cd "$(dirname "$0")" && pwd)
args=("$@")
if [ -f "$HERE/k6-agents.env" ] && [[ " $* " != *" --agents"* ]] && [[ " $* " != *" -h "* && " $* " != *" --help "* ]]; then
  # shellcheck disable=SC1091
  . "$HERE/k6-agents.env"
  args+=(--agents "$AGENTS" --ssh-key "$SSH_KEY")
fi
exec python3 "$HERE/loadgen/loadtest.py" "${args[@]}"
