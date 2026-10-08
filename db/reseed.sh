#!/usr/bin/env bash
# Full re-seed: drop + recreate the bench database, then db/seed.sh as the repo owner.
# Every framework run starts from a freshly seeded DB (same size, no bloat, disk stays bounded).
# Seed ids are time-based UUIDv7, so results/seed-export.json changes on every re-seed.
# Usage: sudo ./db/reseed.sh
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
HERE=$(cd "$(dirname "$0")" && pwd)
OWNER=$(stat -c %U "$HERE")
pgs() { runuser -u postgres -- psql -X -q -v ON_ERROR_STOP=1 "$@"; }
# A bulk load writes as much WAL as data: with the DB host's max_wal_size = 16GB, ~29 GB of seed
# + 16 GB of pg_wal overflowed the 40 GB /data. Cap WAL during the load, restore the config after.
restore_wal() {
  pgs -c "ALTER SYSTEM RESET max_wal_size" -c "ALTER SYSTEM RESET min_wal_size" -c "SELECT pg_reload_conf()" >/dev/null || true
  pgs -c "CHECKPOINT" || true
}
trap restore_wal EXIT
"$HERE/wipe.sh"
pgs -c "ALTER SYSTEM SET max_wal_size = '2GB'" -c "ALTER SYSTEM SET min_wal_size = '512MB'" -c "SELECT pg_reload_conf()" >/dev/null
pgs -c "CHECKPOINT"
runuser -u "$OWNER" -- "$HERE/seed.sh"
df -h /data | tail -1
