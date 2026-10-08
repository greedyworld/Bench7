#!/usr/bin/env bash
# Step 3, on the DB machine: load the benchmark data (after ./tune_postgres1.sh or ./tune_postgres2.sh).
#   ./seed_db.sh [--size-gb N] [--reseed] [--no-follow] [--force]
#
#   Already seeded: deletes only the rows the last test wrote (seconds, db/reset.sql) instead of loading
#                 again, then checks every table has exactly the seed's row count.
#   --force       disconnect apps still connected to the database first (2-VM: the app on the app host)
#
#   --size-gb N   database size (default 29 = the full data set: 500k users, 7M posts, 1.4M messages, 8M likes).
#                 Every table is scaled by N/29. Rough load time on 2 vCPU: ~3 GB in 2 min, 29 GB in 33 min.
#                 Results are comparable only between runs on the same size.
#   --reseed      drop the database and seed it again (stops the app on this machine first)
#   --no-follow   start the seed and return (it runs detached and logs to seed.log)
# Ctrl-C or a dropped ssh session only stops the progress view; run ./seed_db.sh again to follow it.
set -euo pipefail
cd "$(dirname "$0")"
GB=29 RESEED=0 FOLLOW=1 FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --size-gb) GB=${2:?--size-gb N}; shift ;;
    --reseed) RESEED=1 ;;
    --no-follow) FOLLOW=0 ;;
    --force) FORCE=1 ;;
    -h | --help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown option $1 (--size-gb N, --reseed, --no-follow, --force)" >&2; exit 2 ;;
  esac
  shift
done
[[ $GB =~ ^[0-9]+([.][0-9]+)?$ ]] && awk -v g="$GB" 'BEGIN { exit !(g >= 0.1) }' || { echo "--size-gb: a number >= 0.1" >&2; exit 2; }
pgq() { sudo -u postgres psql -X -d bench -tAc "$1" 2>/dev/null || true; }
seeding() { pgrep -f '[d]b/(re)?seed\.sh' >/dev/null; }
pg_isready -q -h 127.0.0.1 || { echo "Postgres is not running: ./setup_postgres.sh first" >&2; exit 1; }

# Back to the seed state without loading again: the apps only INSERT (UUIDv7 ids, so newer than the
# seed's max ids) and bump counters, so db/reset.sql deletes the rows above the seed's max ids and
# recomputes the counters of the rows they touched.
clean_last_test() {
  pgx() { sudo -u postgres psql -X -d bench -v ON_ERROR_STOP=1 -tAc "$1"; }
  [ -f /etc/systemd/system/bench7-app.service ] && ./scripts/app.sh down >/dev/null 2>&1 || true
  # an app still writing would add rows after the delete and race the counter recompute
  local open="from pg_stat_activity where datname = 'bench' and backend_type = 'client backend' and pid <> pg_backend_pid()"
  local n; n=$(pgx "select count(*) $open")
  if [ "$n" != 0 ]; then
    [ "$FORCE" = 1 ] || { echo "$n open connection(s) to the database (an app is still running): ./kill_<framework>.sh on the app host, or ./seed_db.sh --force" >&2; exit 1; }
    pgx "select count(pg_terminate_backend(pid)) $open" >/dev/null; sleep 1
  fi
  local wm="(select v::uuid from bench_meta where k"
  local new; new=$(pgx "select (select count(*) from posts where id > $wm = 'posts_max_id'))
    + (select count(*) from likes where id > $wm = 'likes_max_id'))
    + (select count(*) from messages where id > $wm = 'messages_max_id'))
    + (select count(*) from conversations where id > $wm = 'conversations_max_id'))")
  # seeds made before seed_db_bytes existed: an untouched database is the seed size
  [ "$new" = 0 ] && pgx "insert into bench_meta (k, v) values ('seed_db_bytes', pg_database_size('bench')::text) on conflict (k) do nothing" >/dev/null
  echo "deleting the last test's rows: $new (db/reset.sql: delete, fix counters, vacuum)"
  local t0=$SECONDS
  sudo -u postgres psql -X -q -d bench -v ON_ERROR_STOP=1 <db/reset.sql >/dev/null
  local bad; bad=$(pgx "select coalesce(string_agg(format('%s %s (seed %s)', x.t, x.n, m.v), ', '), '') from (values
      ('users', (select count(*) from users)), ('posts', (select count(*) from posts)),
      ('conversations', (select count(*) from conversations)), ('messages', (select count(*) from messages)),
      ('likes', (select count(*) from likes))) x(t, n) join bench_meta m on m.k = x.t where x.n::text <> m.v")
  [ -z "$bad" ] || { echo "row counts differ from the seed: $bad (./seed_db.sh --reseed --size-gb N)" >&2; exit 1; }
  echo "back to the seed in $((SECONDS - t0)) s, row counts match the seed;" \
       "size $(pgx "select pg_size_pretty(pg_database_size('bench'))") (fresh seed: $(pgx "select coalesce(pg_size_pretty(v::bigint), '?') from bench_meta right join (select 1) o on k = 'seed_db_bytes'"))"
}

if ! seeding; then
  seeded=$(pgq "select count(*) from bench_meta where k = 'seeded_at'")
  if [ "$seeded" = 1 ] && [ "$RESEED" = 0 ]; then
    clean_last_test
  else
    [ -f /etc/systemd/system/bench7-app.service ] && ./scripts/app.sh down >/dev/null 2>&1 || true
    scale() { awk -v d="$1" -v g="$GB" -v m="$2" 'BEGIN { v = int(d * g / 29 + 0.5); print (v < m ? m : v) }'; }
    SEED_ENV=(SEED_USERS="$(scale 500000 1000)" SEED_POSTS="$(scale 7000000 1000)"
              SEED_MESSAGES="$(scale 1400000 500)" SEED_LIKES="$(scale 8000000 1000)")
    echo "seeding ~$GB GB: ${SEED_ENV[*]} -> seed.log"
    # a half-finished seed leaves rows behind: reseed.sh always starts from an empty database
    nohup setsid sudo env "${SEED_ENV[@]}" ./db/reseed.sh >seed.log 2>&1 </dev/null &
    sleep 1
  fi
fi
if seeding; then
  [ "$FOLLOW" = 1 ] || { echo "seed running in the background: tail -f seed.log (or ./seed_db.sh)"; exit 0; }
  echo "following seed.log (Ctrl-C stops only this view; the seed keeps running)"
  tail -n +1 -f seed.log --pid="$(pgrep -of '[d]b/(re)?seed\.sh')" || true
fi
[ "$(pgq "select count(*) from bench_meta where k = 'seeded_at'")" = 1 ] || { echo "seed failed: see seed.log" >&2; exit 1; }
[ "$(pgq "select count(*) from bench_meta where k = 'seed_export'")" = 1 ] || \
  echo "WARNING: seeded by an older bench7 (no seed_export row): ./seed_db.sh --reseed" >&2
echo "database: seeded, $(pgq "select pg_size_pretty(pg_database_size('bench'))")"
df -h /data | tail -1
if [ -f /etc/bench7/pg-tune ] && [ "$(head -1 /etc/bench7/pg-tune)" = tune_postgres2.sh ]; then
  echo "Next on the app host:  ./run_<framework>.sh --db-host <this-machine's private ip>"
else
  echo "Next:  ./run_<framework>.sh   (e.g. ./run_axum.sh)"
fi
