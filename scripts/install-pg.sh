#!/usr/bin/env bash
# Installs Postgres 18 natively (PGDG apt repo, systemd), with its data dir on
# /data and the bench7 settings from db/postgresql.conf. Creates role + db
# "bench" with the password from bench7.env. Reserves explicit huge pages for
# shared_buffers. No resource limits.
# Usage: sudo ./scripts/install-pg.sh
#        sudo PG_ROLE=dbhost PG_ALLOW_CIDR=10.0.0.0/8 ./scripts/install-pg.sh   (Postgres alone on its own machine:
#        adds db/postgresql-dbhost.conf and a scram-sha-256 pg_hba rule for the app host's network)
#        PG_EXTRA="name=value<newline>name=value"   extra settings, loaded last (conf.d/zzz-bench7-extra.conf)
# Called by setup_postgres.sh and tune_postgres1.sh / tune_postgres2.sh.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ENV_FILE=${ENV_FILE:-$ROOT/bench7.env}
PG_ROLE=${PG_ROLE:-local}
PG_ALLOW_CIDR=${PG_ALLOW_CIDR:-10.0.0.0/8}
PGV=18
DATA=/data/pg/$PGV/main
CONF=/etc/postgresql/$PGV/main
[ -f "$ENV_FILE" ] || { echo "missing $ENV_FILE (run scripts/gen-env.sh)" >&2; exit 1; }
PG_PASSWORD=$(grep '^PG_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)

if ! command -v /usr/lib/postgresql/$PGV/bin/postgres >/dev/null; then
  # no default cluster in /var/lib: we create ours on /data
  mkdir -p /etc/postgresql-common
  echo "create_main_cluster = false" >/etc/postgresql-common/createcluster.conf
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-common >/dev/null
  /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-$PGV >/dev/null
fi

if [ -d "$CONF" ] && [ ! -f "$DATA/PG_VERSION" ]; then
  # /data (ephemeral local NVMe) came back blank: drop the stale cluster config so it is re-created
  echo "cluster config without data dir: re-creating the cluster"
  pg_dropcluster --stop $PGV main 2>/dev/null || true
  rm -rf "$CONF"
fi
if [ ! -d "$CONF" ]; then
  mkdir -p /data/pg && chown postgres:postgres /data/pg
  pg_createcluster $PGV main -d "$DATA" -- --data-checksums >/dev/null
fi
install -d -m 0755 -o postgres -g postgres "$CONF/conf.d"
grep -q "^include_dir = 'conf.d'" "$CONF/postgresql.conf" || echo "include_dir = 'conf.d'" >>"$CONF/postgresql.conf"
install -m 0644 -o postgres -g postgres "$ROOT/db/postgresql.conf" "$CONF/conf.d/bench7.conf"
if [ "$PG_ROLE" = dbhost ]; then
  # conf.d files load in name order and the last one wins; "bench7-dbhost" would sort before "bench7."
  rm -f "$CONF/conf.d/bench7-dbhost.conf"
  install -m 0644 -o postgres -g postgres "$ROOT/db/postgresql-dbhost.conf" "$CONF/conf.d/zz-bench7-dbhost.conf"
  rule="host bench bench $PG_ALLOW_CIDR scram-sha-256"
  grep -qxF "$rule" "$CONF/pg_hba.conf" || echo "$rule" >>"$CONF/pg_hba.conf"
else
  rm -f "$CONF/conf.d/bench7-dbhost.conf" "$CONF/conf.d/zz-bench7-dbhost.conf"
fi
rm -f "$CONF/conf.d/zzz-bench7-extra.conf"
if [ -n "${PG_EXTRA:-}" ]; then
  while IFS= read -r kv; do
    [ -n "$kv" ] || continue
    [[ $kv =~ ^([a-z_]+)=(.*)$ ]] || { echo "bad setting '$kv' (want name=value)" >&2; exit 2; }
    printf "%s = '%s'\n" "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]//\'/\'\'}"
  done <<<"$PG_EXTRA" >"$CONF/conf.d/zzz-bench7-extra.conf"
  chown postgres:postgres "$CONF/conf.d/zzz-bench7-extra.conf"
  echo "extra settings:"; sed 's/^/  /' "$CONF/conf.d/zzz-bench7-extra.conf"
fi

# explicit huge pages sized from what Postgres says it needs (+4 pages slack);
# -C on runtime-computed settings needs the server stopped
systemctl stop postgresql@$PGV-main 2>/dev/null || true
hp=$(sudo -u postgres /usr/lib/postgresql/$PGV/bin/postgres -D "$DATA" -c config_file="$CONF/postgresql.conf" \
       -C shared_memory_size_in_huge_pages 2>/dev/null || echo 0)
if [ "${hp:-0}" -gt 0 ]; then
  echo "vm.nr_hugepages = $((hp + 4))" >/etc/sysctl.d/91-bench7-hugepages.conf
  sysctl -q -p /etc/sysctl.d/91-bench7-hugepages.conf
fi

systemctl enable --now postgresql@$PGV-main >/dev/null 2>&1
systemctl restart postgresql@$PGV-main
for _ in $(seq 1 30); do pg_isready -q -h 127.0.0.1 && break; sleep 1; done

# role + db (password passed on stdin, never on the command line)
sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 <<SQL
\set pw '$PG_PASSWORD'
SELECT format('CREATE ROLE bench LOGIN PASSWORD %L', :'pw') WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bench') \gexec
SELECT format('ALTER ROLE bench PASSWORD %L', :'pw') \gexec
GRANT pg_checkpoint, pg_read_all_stats TO bench;
SELECT 'CREATE DATABASE bench OWNER bench' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'bench') \gexec
SQL
sudo -u postgres psql -X -q -d bench -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"

sudo -u postgres psql -X -tAc "SELECT version()"
sudo -u postgres psql -X -tAc "SELECT name, setting FROM pg_settings WHERE name IN ('data_directory','shared_buffers','huge_pages','max_connections','io_method',
  'synchronous_commit','fsync','full_page_writes','data_checksums','wal_compression','wal_buffers','max_wal_size','min_wal_size',
  'checkpoint_timeout','checkpoint_completion_target','commit_delay','commit_siblings') ORDER BY name"
grep -i '^HugePages_\(Total\|Free\)' /proc/meminfo
