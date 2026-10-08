#!/usr/bin/env bash
# DELETE ALL RECORDS: drop and re-create the empty "bench" database (frees the disk
# immediately, unlike DELETE/TRUNCATE + vacuum). Run db/seed.sh afterwards to load data again.
# Stops the app first so it does not reconnect into a half-created DB.
# Usage: sudo ./db/wipe.sh
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
systemctl stop bench7-app 2>/dev/null || true
pg() { runuser -u postgres -- psql -X -q -v ON_ERROR_STOP=1 "$@"; }
pg -c "DROP DATABASE IF EXISTS bench WITH (FORCE)" -c "CREATE DATABASE bench OWNER bench"
pg -d bench -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"
pg -c "CHECKPOINT"
echo "$(date +%T) wiped"
df -h /data | tail -1
