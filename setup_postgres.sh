#!/usr/bin/env bash
# Step 2, on the DB machine: install Postgres 18 (data dir on /data, the local NVMe) and start it.
# Then pick the settings profile and load the data:
#   ./tune_postgres1.sh     app on this same machine (1-VM setup)
#   ./tune_postgres2.sh     Postgres alone on this machine, the app connects over the private network (2-VM setup)
#   ./seed_db.sh --size-gb N
#
#   ./setup_postgres.sh [--data-gb N]    size of the /data partition (default 40; use 80 on a DB-only host
#                                        for the full 29 GB seed). Only used when /data is created.
# Safe to run again, e.g. after a VM restart: /data, the cluster and the last tune_postgres profile are restored.
set -euo pipefail
cd "$(dirname "$0")"
DATA_GB=${DATA_GB:-40}
while [ $# -gt 0 ]; do
  case "$1" in
    --data-gb) DATA_GB=${2:?--data-gb N}; shift ;;
    -h | --help) sed -n '2,11p' "$0"; exit 0 ;;
    *) echo "unknown option $1 (--data-gb N)" >&2; exit 2 ;;
  esac
  shift
done
STATE=/etc/bench7
seeding() { pgrep -f '[d]b/(re)?seed\.sh' >/dev/null; }

echo "== 1/3 secrets (bench7.env)"
[ -f bench7.env ] && echo "bench7.env exists" || ./scripts/gen-env.sh

echo "== 2/3 /data on the local NVMe"
if ! mountpoint -q /data || [ ! -f /etc/systemd/system/bench7-data.service ]; then
  dpkg -s xfsprogs parted >/dev/null 2>&1 || { sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq xfsprogs parted >/dev/null; }
  sudo install -d $STATE
  [ -f $STATE/data-size ] || echo "${DATA_GB}GiB" | sudo tee $STATE/data-size >/dev/null
  sudo ./scripts/prepare-data.sh
  sudo ./scripts/install-units.sh
fi
df -h /data | tail -1

echo "== 3/3 Postgres 18"
if seeding; then
  echo "a seed is running: leaving Postgres as it is (./seed_db.sh shows it)"
  exit 0
fi
if [ ! -x /usr/lib/postgresql/18/bin/postgres ] || [ ! -f /data/pg/18/main/PG_VERSION ]; then
  sudo ./scripts/install-pg.sh   # 1-VM settings until a tune_postgres script runs
fi
if [ -f $STATE/pg-tune ]; then
  # the last ./tune_postgres1.sh / ./tune_postgres2.sh command, applied again (also restarts the DB host sampler)
  mapfile -t T <$STATE/pg-tune
  echo "== applying the saved profile: ${T[*]}"
  "./${T[0]}" "${T[@]:1}"
else
  # huge pages are released when this machine ran an app against a remote Postgres (run_<fw>.sh --db-host)
  [ -f /etc/sysctl.d/91-bench7-hugepages.conf ] && sudo sysctl -q -p /etc/sysctl.d/91-bench7-hugepages.conf || true
  sudo systemctl start postgresql@18-main
  for _ in $(seq 1 30); do pg_isready -q -h 127.0.0.1 && break; sleep 1; done
  pg_isready -h 127.0.0.1
  echo
  echo "Postgres running. Next: ./tune_postgres1.sh (app on this machine) or ./tune_postgres2.sh (app elsewhere)"
fi
