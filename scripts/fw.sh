#!/usr/bin/env bash
# Backend of run_<name>.sh / kill_<name>.sh / setup_toolchain.sh in the repo root (app host only).
#
#   run  <name> [--build] [--no-clean] [--db-url URL | --db-host IP] [--profile 1vm|2vm] [--set K=V]... [--java-opts "..."]
#        0. bench7.env (secrets) if missing, psql client if missing, results/seed-export.json from the DB
#        1. stop any running app + metrics sampler
#        2. delete the previous test's records (db/reset.sql)        skip: --no-clean
#        3. install the framework's toolchain if missing and build the app if it was never built  force: --build
#        4. start it (systemd unit bench7-app) and wait for /health
#        5. mint JWTs + write the load-generator bundle (/data/run/bundle.json, with versions.json)
#        6. start the metrics sampler on :41901 (/latest = host metrics, /bundle = k6 input)
#        7. print the command to run on the load generator
#        --db-url  postgres://bench:<pw>@<db-host>:5432/bench  -> Postgres on another machine: the local
#                  Postgres is stopped and its huge pages released (./setup_postgres.sh brings both back),
#                  the sampler skips Postgres (the DB host runs its own, started by ./tune_postgres2.sh)
#        --db-host IP  Postgres set up with ./tune_postgres2.sh on IP: asks once for its password
#                  and keeps it in bench7.env as PG_PASSWORD_REMOTE
#        --profile pool + batching profile (default: 1vm for a local DB, 2vm for a remote one)
#                  1vm  DB_POOL_TOTAL=12 BATCH_LANES=1 BATCH_MAX_ROWS=200  BATCH_WINDOW_MS=20
#                  2vm  DB_POOL_TOTAL=24 BATCH_LANES=4 BATCH_MAX_ROWS=1000 BATCH_WINDOW_MS=20
#        --set     extra app env (wins over the profile), e.g. --set BATCH_WINDOW_MS=200 --set GOGC=200
#        --java-opts  JVM flags for springboot (default in app.sh)
#   kill  <name>  stop sampler + app, keep app log + host metrics in results/host/<name>-<time>/
#   build <name>  install the toolchain (pinned versions) and build the app
#   toolchain <name>  install the toolchain only
#
# name: fastapi | bun | node | fastify_bun | springboot | gin | aspnet | axum
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CMD=${1:?usage: fw.sh run|kill|build <name> [options]}
NAME=${2:?framework name}
shift 2
case "$NAME" in
  fastapi | bun | gin | aspnet | axum) FW=$NAME ;;
  node) FW=fastify ;;                # Fastify on Node.js (cluster, one process per vCPU)
  fastify_bun) FW=fastify-bun ;;     # the same Fastify app on the Bun runtime
  springboot) FW=spring ;;
  *) echo "unknown framework '$NAME'" >&2; exit 2 ;;
esac
BUILD=0 CLEAN=1 DB_URL="" DB_HOST="" EXTRA="" PROFILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --build) BUILD=1 ;;
    --no-clean) CLEAN=0 ;;
    --db-url) DB_URL=${2:?--db-url URL}; shift ;;
    --db-host) DB_HOST=${2:?--db-host IP}; shift ;;
    --profile) PROFILE=${2:?--profile 1vm|2vm}; [[ $PROFILE == [12]vm ]] || { echo "--profile 1vm|2vm" >&2; exit 2; }; shift ;;
    --set) [[ ${2:-} == *=* ]] || { echo "--set K=V" >&2; exit 2; }; EXTRA="$EXTRA $2"; shift ;;
    --java-opts) export JAVA_OPTS=${2:?--java-opts "..."}; shift ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
  shift
done
export BENCH7_EXTRA_ENV=${EXTRA# }
RUN=/data/run
PORT=${PORT:-8080}
MPORT=${METRICS_PORT:-41901}
TOKENS=${TOKENS:-512}
# Background timers (apt, cloud patch agents, man-db...) that would steal CPU mid-test.
# The active ones are paused for the run, listed in $RUN/paused-timers, and started again by kill.
TIMER_RE='^(apt-daily|apt-daily-upgrade|.*[Pp]atch.*|fwupd-refresh|man-db|motd-news|update-notifier-.*)\.timer$'

pause_timers() {
  local t
  sudo mkdir -p "$RUN"
  for t in $(systemctl list-units --type=timer --state=active --no-legend --plain 2>/dev/null | awk '{print $1}' | grep -E "$TIMER_RE"); do
    sudo systemctl stop "$t" && echo "$t" | sudo tee -a "$RUN/paused-timers" >/dev/null
  done
}

resume_timers() {
  [ -f "$RUN/paused-timers" ] || return 0
  sort -u "$RUN/paused-timers" | xargs -r sudo systemctl start 2>/dev/null || true
  sudo rm -f "$RUN/paused-timers"
}

stop_sampler() { sudo pkill -f "[s]cripts/sampler\.py --port $MPORT" 2>/dev/null || true; }   # not the DB host one (:41902)

artifact() {
  case "$FW" in
    axum) echo apps/axum/target/release/bench7-axum ;;
    fastify | fastify-bun) echo apps/fastify/node_modules ;;
    bun) echo apps/bun/node_modules ;;
    spring) echo apps/spring/target/bench7-spring.jar ;;
    aspnet) echo apps/aspnet/out/Bench7 ;;
    fastapi) echo apps/fastapi/.venv/bin/python ;;
    gin) echo apps/gin/gin-bench7 ;;
  esac
}

host_ip() { ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "src") print $(i + 1)}'; }

build_() {
  echo "== toolchain for $FW"
  # shellcheck disable=SC2046
  sudo ./scripts/install-toolchains.sh base pgclient $(toolchain_parts)
  echo "== build $FW"
  ./scripts/app.sh build "$FW"
}

envget() { grep "^$1=" "$ROOT/bench7.env" | cut -d= -f2- || true; }

toolchain_parts() {
  case "$FW" in
    axum) echo rust ;;
    fastify) echo node ;;
    fastify-bun) echo node bun ;;   # npm installs the deps, bun runs them
    bun) echo bun ;;
    spring) echo java ;;
    aspnet) echo dotnet ;;
    fastapi) echo python ;;
    gin) echo go ;;
  esac
}

# --db-host IP: log in with PG_PASSWORD_REMOTE (or PG_PASSWORD when both machines share one bench7.env);
# ask for the DB host's password when neither works and keep it in bench7.env
remote_url() {
  local h=$1 pw tries=0
  pw=$(envget PG_PASSWORD_REMOTE)
  [ -n "$pw" ] || pw=$(envget PG_PASSWORD)
  until PGCONNECT_TIMEOUT=5 psql "postgres://bench:$pw@$h:5432/bench" -X -w -tAqc 'select 1' >/dev/null 2>&1; do
    pg_isready -q -t 5 -h "$h" || { echo "Postgres on $h is not reachable (run ./tune_postgres2.sh there)" >&2; exit 1; }
    [ -t 0 ] && [ "$tries" -lt 3 ] || { echo "cannot log in to Postgres on $h: put PG_PASSWORD_REMOTE=<its PG_PASSWORD> in bench7.env" >&2; exit 1; }
    [ "$tries" = 0 ] || echo "password rejected" >&2
    read -rsp "Postgres password of $h (on the DB host: grep PG_PASSWORD bench7.env): " pw; echo >&2
    pw=${pw#PG_PASSWORD=}
    tries=$((tries + 1))
  done
  if [ "$tries" -gt 0 ]; then
    sed -i '/^PG_PASSWORD_REMOTE=/d' "$ROOT/bench7.env"
    echo "PG_PASSWORD_REMOTE=$pw" >>"$ROOT/bench7.env"
    echo "saved as PG_PASSWORD_REMOTE in bench7.env"
  fi
  DB_URL="postgres://bench:$pw@$h:5432/bench"
}

resolve_db() {
  [ -f "$ROOT/bench7.env" ] || "$ROOT/scripts/gen-env.sh"
  if ! command -v psql >/dev/null || ! command -v pg_isready >/dev/null; then
    echo "== install the Postgres client (psql)"
    sudo "$ROOT/scripts/install-toolchains.sh" pgclient
  fi
  if [ -n "$DB_HOST" ]; then
    remote_url "$DB_HOST"
  elif [ -z "$DB_URL" ]; then
    DB_URL="postgres://bench:$(envget PG_PASSWORD)@127.0.0.1:5432/bench"
  fi
  dbh=$(sed -E 's#^[a-z]+://([^@/]*@)?(\[[^]]*\]|[^:/?]+).*#\2#' <<<"$DB_URL")
  case "$dbh" in 127.0.0.1 | localhost | ::1) REMOTE=0 ;; *) REMOTE=1 ;; esac
  export BENCH7_DB_URL=$DB_URL
  # pool + batching profile (app.sh defaults = 1vm); --set values are appended after these and win
  [ -n "$PROFILE" ] || { [ "$REMOTE" = 1 ] && PROFILE=2vm || PROFILE=1vm; }
  if [ "$PROFILE" = 2vm ]; then
    export DB_POOL_TOTAL=${DB_POOL_TOTAL:-24} BATCH_LANES=${BATCH_LANES:-4} BATCH_MAX_ROWS=${BATCH_MAX_ROWS:-1000}
  else
    export DB_POOL_TOTAL=${DB_POOL_TOTAL:-12} BATCH_LANES=${BATCH_LANES:-1} BATCH_MAX_ROWS=${BATCH_MAX_ROWS:-200}
  fi
  export BATCH_WINDOW_MS=${BATCH_WINDOW_MS:-20}
}

run() {
  cd "$ROOT"
  resolve_db
  sudo install -d -m 0777 "$RUN"
  if [ "$REMOTE" = 1 ]; then
    pg_isready -q -d "$DB_URL" || { echo "remote Postgres ($dbh) is not reachable" >&2; exit 1; }
    # the app host keeps only the app: no local Postgres, no reserved huge pages
    if systemctl is-active -q postgresql@18-main; then
      echo "== stop local Postgres (DB is on $dbh)"
      sudo systemctl stop postgresql@18-main
    fi
    sudo sysctl -qw vm.nr_hugepages=0
  else
    pg_isready -q -h 127.0.0.1 || { echo "Postgres is not running here: ./setup_postgres.sh (or --db-host IP)" >&2; exit 1; }
  fi
  # the ids k6 uses must come from the database the app talks to (they differ on every seed)
  mkdir -p results
  psql "$DB_URL" -X -w -tAqc "select v from bench_meta where k = 'seed_export'" >results/seed-export.json.tmp 2>/dev/null || true
  if [ -s results/seed-export.json.tmp ]; then
    mv results/seed-export.json.tmp results/seed-export.json
  else
    rm -f results/seed-export.json.tmp
    [ -f results/seed-export.json ] || { echo "database on $dbh is not seeded: ./seed_db.sh there" >&2; exit 1; }
    echo "WARNING: no seed_export in the database (older seed): using the local results/seed-export.json" >&2
  fi

  echo "== stop old app + sampler"
  stop_sampler
  ./scripts/app.sh down
  if [ "$CLEAN" = 1 ]; then
    # every row a load test wrote (ids newer than the seed), counters fixed, vacuum + analyze (~1-2 min)
    echo "== delete previous test records"
    psql "$DB_URL" -X -w -q -v ON_ERROR_STOP=1 -f db/reset.sql
  fi
  if [ "$BUILD" = 1 ] || [ ! -e "$(artifact)" ]; then
    build_
  fi
  pause_timers

  echo "== start $FW"
  ./scripts/app.sh up "$FW"

  TS=$(date -u +%Y%m%dT%H%M%SZ)
  OUT=$ROOT/results/host/$NAME-$TS
  mkdir -p "$OUT"
  echo "$OUT" >"$RUN/current-run"
  local pgv
  pgv=$(psql "$DB_URL" -X -tAc 'show server_version' 2>/dev/null || true)
  {
    echo "## framework"; echo "$NAME ($FW)"
    echo "## postgres host"; [ "$REMOTE" = 1 ] && echo "remote ($dbh)" || echo local
    echo "## uname"; uname -a
    echo "## cpu"; lscpu | grep -E 'Model name|^CPU\(s\)|Thread|MHz'
    echo "## mem"; free -m
    echo "## disk"; df -h /data
    echo "## app env (secrets removed)"; grep -vE 'SECRET|KEY|PASSWORD|DATABASE_URL' "$RUN/bench7-app.env"
    echo "## postgres"; psql "$DB_URL" -X -tAc 'select version()'
    psql "$DB_URL" -X -tAc "select name || ' = ' || setting || coalesce(' ' || unit, '') from pg_settings
      where source not in ('default', 'override') order by name"
    psql "$DB_URL" -X -tAc "select relname, pg_size_pretty(pg_total_relation_size(oid)) from pg_class
      where relkind = 'r' and relnamespace = 'public'::regnamespace order by pg_total_relation_size(oid) desc"
  } >"$OUT/host-info.txt" 2>&1
  python3 scripts/versions.py "$FW" --app-env "$RUN/bench7-app.env" --pg "$pgv" >"$OUT/versions.json" 2>"$OUT/versions.err" || true

  echo "== bundle for the load generator ($TOKENS JWTs)"
  python3 scripts/make_bundle.py --env bench7.env --seed results/seed-export.json --base "http://127.0.0.1:$PORT" \
    --tokens "$TOKENS" --framework "$NAME" --info "$OUT/versions.json" --out "$RUN/bundle.json"

  echo "== metrics sampler on :$MPORT"
  local nopg=""
  [ "$REMOTE" = 1 ] && nopg="--no-pg"
  sudo bash -c "setsid nohup python3 '$ROOT/scripts/sampler.py' --port $MPORT $nopg --bundle '$RUN/bundle.json' \
    --out '$OUT/host.ndjson' >'$OUT/sampler.log' 2>&1 </dev/null &"
  for _ in $(seq 1 20); do curl -fsS -m 1 "http://127.0.0.1:$MPORT/latest" 2>/dev/null | grep -q '"cpu"' && break; sleep 1; done
  curl -fsS -m 1 "http://127.0.0.1:$MPORT/latest" | grep -q '"cpu"' || { echo "sampler did not start (see $OUT/sampler.log)" >&2; exit 1; }

  local ip
  ip=$(host_ip)
  cat <<EOF

$NAME is running on http://$ip:$PORT  (metrics + bundle on :$MPORT)
profile $PROFILE: pool $DB_POOL_TOTAL, $BATCH_LANES lane(s), $BATCH_MAX_ROWS rows, $BATCH_WINDOW_MS ms window${BENCH7_EXTRA_ENV:+  (--set $BENCH7_EXTRA_ENV)}
Live view here:  ./watch_server.sh
On the main load generator (once: ./connect_agent.sh [user@<generator-2-ip>]):

  ./run_k6.sh --ip $ip --port $PORT$([ "$REMOTE" = 1 ] && echo " --db-ip $dbh") --rps 1000,5000,21000,50000 --times 10,30,120,285 --warmup 30
  (payload: --payload-kb 5 is the default; Group 2 routes: add --design normal)

When it is done:  ./kill_$NAME.sh
EOF
}

kill_() {
  cd "$ROOT"
  stop_sampler
  local out
  out=$(cat "$RUN/current-run" 2>/dev/null || true)
  if [ -n "$out" ] && [ -d "$out" ]; then
    ./scripts/app.sh logs 400 >"$out/app.log" 2>&1 || true
    sudo chown -R "$(id -un)" "$out"
    echo "host metrics + app log: $out"
  fi
  ./scripts/app.sh down
  sudo rm -f "$RUN/bundle.json" "$RUN/current-run"
  resume_timers
  echo "$NAME stopped"
}

case "$CMD" in
  run) run ;;
  kill) kill_ ;;
  build) cd "$ROOT"; build_ ;;
  toolchain) cd "$ROOT"; sudo ./scripts/install-toolchains.sh base pgclient $(toolchain_parts) ;;
  *) echo "usage: fw.sh run|kill|build|toolchain <name>" >&2; exit 2 ;;
esac
