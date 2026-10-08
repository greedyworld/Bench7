#!/usr/bin/env bash
# Native app runner: builds and runs one framework directly on the host as systemd
# unit "bench7-app" (cgroup accounting on, NO limits). Uses the persistent unit
# /etc/systemd/system/bench7-app.service when installed (scripts/install-units.sh),
# otherwise an equivalent transient unit.
# Usage: ./scripts/app.sh build <fw> | up <fw> | down | status | logs | env <fw> | exec [fw]
#   exec = run the framework in the foreground (the unit's ExecStart; fw from BENCH7_FRAMEWORK)
#   fw = axum | fastify | fastify-bun | bun | spring | aspnet | fastapi | gin
#        (fastify-bun = the same Fastify app executed by the Bun runtime)
# Env written for the app (same contract for all):
#   DATABASE_URL DB_POOL_TOTAL WORKERS HOST PORT CACHE_MAX_MB
#   CACHE_TTL_MIN_MS CACHE_TTL_MAX_MS CACHE_TTL_BINS   (hash-bucketed TTL)
#   BATCH_MAX_ROWS BATCH_WINDOW_MS
#   BATCH_LANES (flush lanes per table, rows routed by user / conversation / post id)
#   WRITE_QUEUE_MAX (queued rows per table, split over the lanes; full lane -> 503)
#   STATEMENT_TIMEOUT_MS (set on the app's connections) BATCH_STATS (1 = batch-stats log every 10 s)
#   JWT_SECRET JWT_ISS JWT_AUD JWT_TTL_S TOKEN_ISSUER_KEY
# Pool + batching defaults below are the 1-VM profile; fw.sh exports the 2-VM profile when the DB is remote.
# Overrides (from the caller's environment, see fw.sh --db-url / --set):
#   BENCH7_DB_URL     DATABASE_URL for the app (default: the local Postgres)
#   BENCH7_EXTRA_ENV  space separated K=V appended last (wins over the defaults), e.g. "GOGC=200 WORKERS=3"
#   JAVA_OPTS         JVM flags for spring (default below)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ENV_FILE=${ENV_FILE:-$ROOT/bench7.env}
UNIT=bench7-app
APP_ENV=/data/run/bench7-app.env
if [ "$(id -u)" = 0 ]; then U=${SUDO_USER:-root}; else U=$(id -un); fi   # SUDO_USER is stale when not root
FW=${2:-${BENCH7_FRAMEWORK:-}}
DIR=${FW%-bun}   # source dir (fastify-bun -> fastify)
JAVA_OPTS_DEFAULT="-XX:+UseParallelGC -XX:InitialRAMPercentage=40 -XX:MaxRAMPercentage=40 -Xss512k"

asuser() { sudo -u "$U" -H bash -lc "cd '$ROOT/apps/$DIR' && $1"; }

app_cmd() { # prints the run command for $FW
  case "$FW" in
    axum)    echo "$ROOT/apps/axum/target/release/bench7-axum" ;;
    fastify) echo "/usr/local/bin/node $ROOT/apps/fastify/server.js" ;;
    fastify-bun) echo "/usr/local/bin/bun $ROOT/apps/fastify/server.js" ;;
    bun)     echo "/usr/local/bin/bun $ROOT/apps/bun/server.ts" ;;
    spring)  echo "$(readlink -f "$(command -v java)") ${JAVA_OPTS:-$JAVA_OPTS_DEFAULT} -Xlog:gc:file=/data/run/spring-gc.log:uptime,level -jar $ROOT/apps/spring/target/bench7-spring.jar" ;;
    aspnet)  echo "$ROOT/apps/aspnet/out/Bench7" ;;
    fastapi) echo "$ROOT/apps/fastapi/.venv/bin/python $ROOT/apps/fastapi/run.py" ;;
    gin)     echo "$ROOT/apps/gin/gin-bench7" ;;
    *) echo "unknown framework '$FW'" >&2; exit 2 ;;
  esac
}

build() {
  case "$FW" in
    axum)    asuser '. ~/.cargo/env && cargo build --release --locked -q' ;;
    fastify|fastify-bun) asuser 'npm ci --omit=dev --silent' ;;
    bun)     asuser 'bun install --production --frozen-lockfile --silent' ;;
    spring)  asuser 'mvn -q -B -DskipTests package' ;;
    aspnet)  asuser 'dotnet publish -c Release -o out --nologo -v q' ;;
    fastapi) asuser 'uv venv -q -p 3.13.15 --allow-existing .venv && uv pip install -q -p .venv/bin/python -r requirements.txt' ;;
    gin)     asuser 'GOAMD64=v3 go build -trimpath -ldflags "-s -w" -o gin-bench7 .' ;;
    *) echo "unknown framework '$FW'" >&2; exit 2 ;;
  esac
  echo "built $FW"
}

write_env() {
  [ -f "$ENV_FILE" ] || { echo "missing $ENV_FILE" >&2; exit 1; }
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  umask 077
  sudo install -m 0600 -o "$U" /dev/null "$APP_ENV"
  sudo tee "$APP_ENV" >/dev/null <<EOF
DATABASE_URL=${BENCH7_DB_URL:-postgres://bench:${PG_PASSWORD}@127.0.0.1:5432/bench}
DB_POOL_TOTAL=${DB_POOL_TOTAL:-12}
WORKERS=${WORKERS:-$(nproc)}
HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8080}
CACHE_MAX_MB=${CACHE_MAX_MB:-64}
CACHE_TTL_MIN_MS=${CACHE_TTL_MIN_MS:-45000}
CACHE_TTL_MAX_MS=${CACHE_TTL_MAX_MS:-60000}
CACHE_TTL_BINS=${CACHE_TTL_BINS:-15}
BATCH_MAX_ROWS=${BATCH_MAX_ROWS:-200}
BATCH_WINDOW_MS=${BATCH_WINDOW_MS:-20}
BATCH_LANES=${BATCH_LANES:-1}
WRITE_QUEUE_MAX=${WRITE_QUEUE_MAX:-40000}
STATEMENT_TIMEOUT_MS=${STATEMENT_TIMEOUT_MS:-5000}
BATCH_STATS=${BATCH_STATS:-1}
JWT_SECRET=${JWT_SECRET}
JWT_ISS=bench7
JWT_AUD=bench7-api
JWT_TTL_S=${JWT_TTL_S:-86400}
TOKEN_ISSUER_KEY=${TOKEN_ISSUER_KEY}
NODE_ENV=production
ASPNETCORE_URLS=http://0.0.0.0:${PORT:-8080}
DOTNET_ROOT=${DOTNET_ROOT:-/usr/local/dotnet}
DOTNET_TieredPGO=1
DOTNET_gcServer=1
DOTNET_GCHeapAffinitizeMask=0
# .NET: no DATAS heap shrinking (it is on by default with Server GC since .NET 9) and no thread-pool
# spin-waiting (spinning on 2 vCPU steals CPU from Postgres)
DOTNET_GCDynamicAdaptationMode=0
DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0
# Go: fewer GC cycles (same idea as the presized JVM heap), soft cap well below RAM
GOGC=400
GOMEMLIMIT=1GiB
GIN_MODE=release
JAVA_OPTS=${JAVA_OPTS:-$JAVA_OPTS_DEFAULT}
BENCH7_FRAMEWORK=${FW}
EOF
  local kv
  for kv in ${BENCH7_EXTRA_ENV:-}; do
    [[ $kv == *=* ]] || { echo "bad BENCH7_EXTRA_ENV entry '$kv' (want K=V)" >&2; exit 2; }
    echo "$kv" | sudo tee -a "$APP_ENV" >/dev/null
  done
}

up() {
  app_cmd >/dev/null
  down >/dev/null 2>&1 || true
  write_env
  if [ -f /etc/systemd/system/$UNIT.service ]; then
    sudo systemctl start "$UNIT"
  else
  # shellcheck disable=SC2046
  sudo systemd-run --unit "$UNIT" --uid "$U" --gid "$(id -gn "$U")" --collect \
    --working-directory "$ROOT/apps/$DIR" \
    -p EnvironmentFile="$APP_ENV" -p LimitNOFILE=1048576 -p TasksMax=infinity \
    -p CPUAccounting=yes -p MemoryAccounting=yes -p IOAccounting=yes \
    -p KillMode=control-group -p TimeoutStopSec=15 \
    -E BENCH7_FRAMEWORK="$FW" \
    $(app_cmd) >/dev/null
  fi
  for i in $(seq 1 180); do
    if curl -fsS -m 2 "http://127.0.0.1:${PORT:-8080}/health" 2>/dev/null; then echo; echo "$FW up after ${i}s"; return 0; fi
    systemctl is-active -q "$UNIT" || { echo "$FW exited" >&2; journalctl -u "$UNIT" -n 40 --no-pager >&2; exit 1; }
    sleep 1
  done
  echo "$FW did not become healthy" >&2; exit 1
}

down() { sudo systemctl stop "$UNIT" 2>/dev/null || true; sudo systemctl reset-failed "$UNIT" 2>/dev/null || true; }

case "${1:-}" in
  build)  build ;;
  up)     up ;;
  down)   down ;;
  status) systemctl status "$UNIT" --no-pager | head -15 ;;
  logs)   journalctl -u "$UNIT" -n "${2:-100}" --no-pager ;;
  env)    app_cmd >/dev/null; write_env; echo "wrote $APP_ENV ($FW)" ;;
  # shellcheck disable=SC2046
  exec)   cd "$ROOT/apps/$DIR" && exec $(app_cmd) ;;
  *) echo "usage: $0 build <fw> | up <fw> | down | status | logs [n] | env <fw> | exec [fw]" >&2; exit 2 ;;
esac
