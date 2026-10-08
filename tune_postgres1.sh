#!/usr/bin/env bash
# Step 2.1, 1-VM setup (the app runs on this same machine as Postgres): Postgres settings for a shared box.
#   ./tune_postgres1.sh [--set name=value]...
#
# Applies db/postgresql.conf (synchronous_commit=on, fsync=on, full_page_writes=on, data checksums, lz4 WAL,
# max_wal_size 4GB, commit_delay=200 commit_siblings=5, max_connections=100, explicit huge pages) and restarts
# Postgres. --set adds/overrides settings (loaded last), e.g. --set commit_delay=0 --set shared_buffers=2GB.
# The command is remembered: ./setup_postgres.sh applies it again after a VM restart.
# The app side of the 1-VM profile (12 connections, 1 batching lane, 200 rows / 20 ms) is set by
# ./run_<framework>.sh because the database is local.
set -euo pipefail
cd "$(dirname "$0")"
EXTRA="" ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --set) [[ ${2:-} =~ ^[a-z_]+=.+$ ]] || { echo "--set name=value" >&2; exit 2; }; EXTRA+="$2"$'\n'; ARGS+=(--set "$2"); shift ;;
    -h | --help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown option $1 (--set name=value)" >&2; exit 2 ;;
  esac
  shift
done
pgrep -f '[d]b/(re)?seed\.sh' >/dev/null && { echo "a seed is running: tune after it finishes" >&2; exit 1; }
[ -x /usr/lib/postgresql/18/bin/postgres ] || { echo "Postgres is not installed: ./setup_postgres.sh first" >&2; exit 1; }

sudo PG_ROLE=local PG_EXTRA="$EXTRA" ./scripts/install-pg.sh
sudo pkill -f '[s]cripts/sampler\.py --port 41902' 2>/dev/null || true   # DB host sampler of tune_postgres2.sh
sudo install -d /etc/bench7
printf '%s\n' tune_postgres1.sh "${ARGS[@]}" | sudo tee /etc/bench7/pg-tune >/dev/null

if sudo -u postgres psql -X -d bench -tAc "select 1 from bench_meta where k = 'seeded_at'" 2>/dev/null | grep -q 1; then
  echo; echo "Postgres tuned for 1 VM (database already seeded). Next:  ./run_<framework>.sh   (e.g. ./run_axum.sh)"
else
  echo; echo "Postgres tuned for 1 VM. Next:  ./seed_db.sh --size-gb 3   then  ./run_<framework>.sh"
fi
