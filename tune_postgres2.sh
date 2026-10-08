#!/usr/bin/env bash
# Step 2.2, 2-VM setup (Postgres alone on this machine, the app host connects over the private network).
#   ./tune_postgres2.sh [--allow CIDR] [--set name=value]...
#
#   --allow CIDR        network allowed to connect as "bench" (scram-sha-256; default 10.0.0.0/8)
#   --set name=value    add/override a setting (loaded last), e.g. --set commit_delay=0
#
# Applies db/postgresql.conf + db/postgresql-dbhost.conf (Postgres gets the whole machine: bigger
# shared_buffers / WAL, listens on all interfaces), restarts Postgres and starts the DB host metrics
# sampler on :41902 (run_k6.sh --db-ip reads it; samples in results/dbhost/<time>/).
# The command is remembered: ./setup_postgres.sh applies it again after a VM restart.
# The app side of the 2-VM profile (24 connections, 4 batching lanes, 1000 rows / 20 ms) is set by
# ./run_<framework>.sh --db-host <this-ip>.
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$PWD
ALLOW=10.0.0.0/8 EXTRA="" ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --allow) ALLOW=${2:?--allow CIDR}; ARGS+=(--allow "$2"); shift ;;
    --set) [[ ${2:-} =~ ^[a-z_]+=.+$ ]] || { echo "--set name=value" >&2; exit 2; }; EXTRA+="$2"$'\n'; ARGS+=(--set "$2"); shift ;;
    -h | --help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option $1 (--allow CIDR, --set name=value)" >&2; exit 2 ;;
  esac
  shift
done
pgrep -f '[d]b/(re)?seed\.sh' >/dev/null && { echo "a seed is running: tune after it finishes" >&2; exit 1; }
[ -x /usr/lib/postgresql/18/bin/postgres ] || { echo "Postgres is not installed: ./setup_postgres.sh first" >&2; exit 1; }

sudo PG_ROLE=dbhost PG_ALLOW_CIDR="$ALLOW" PG_EXTRA="$EXTRA" ./scripts/install-pg.sh
sudo install -d /etc/bench7
printf '%s\n' tune_postgres2.sh "${ARGS[@]}" | sudo tee /etc/bench7/pg-tune >/dev/null

# DB host metrics sampler (only the one on :41902; an app sampler on this machine is left alone)
MPORT=41902
sudo pkill -f "[s]cripts/sampler\.py --port $MPORT" 2>/dev/null || true
OUT=$ROOT/results/dbhost/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"
sudo -u postgres psql -X -q -tA -d bench -c "select name || ' = ' || setting || coalesce(' ' || unit, '') || '  (' || source || ')'
  from pg_settings where source not in ('default', 'override') order by name" >"$OUT/settings.txt"
sudo bash -c "setsid nohup python3 '$ROOT/scripts/sampler.py' --port $MPORT --app-unit none.service \
  --out '$OUT/host.ndjson' >'$OUT/sampler.log' 2>&1 </dev/null &"
for _ in $(seq 1 20); do curl -fsS -m 1 "http://127.0.0.1:$MPORT/latest" 2>/dev/null | grep -q '"cpu"' && break; sleep 1; done
curl -fsS -m 1 "http://127.0.0.1:$MPORT/latest" | grep -q '"cpu"' || { echo "sampler did not start ($OUT/sampler.log)" >&2; exit 1; }
echo "DB host sampler on :$MPORT -> $OUT"

ip=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "src") print $(i + 1)}')
seeded=$(sudo -u postgres psql -X -d bench -tAc "select 1 from bench_meta where k = 'seeded_at'" 2>/dev/null || true)
cat <<EOF

DB host ready on $ip:5432 (metrics on :$MPORT).
$([ "$seeded" = 1 ] && echo "Database already seeded." || echo "Next here:  ./seed_db.sh --size-gb 3")
Then on the app host:  ./run_<framework>.sh --db-host $ip
It asks once for the database password; show it here with:  grep PG_PASSWORD bench7.env
EOF
