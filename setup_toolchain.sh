#!/usr/bin/env bash
# Step 4, on the app host: install the toolchains (pinned versions) and build the apps ahead of time,
# so ./run_<framework>.sh starts at once. Safe to run again (a rebuild picks up code changes).
#   ./setup_toolchain.sh axum springboot     only these
#   ./setup_toolchain.sh                     all: axum springboot aspnet gin fastapi node fastify_bun bun
#   ./setup_toolchain.sh --no-build axum     install the toolchain only
set -euo pipefail
cd "$(dirname "$0")"
ALL=(axum springboot aspnet gin fastapi node fastify_bun bun)
NOBUILD=0 FWS=()
for a in "$@"; do
  case "$a" in
    --no-build) NOBUILD=1 ;;
    -h | --help) sed -n '2,6p' "$0"; exit 0 ;;
    all) FWS+=("${ALL[@]}") ;;
    -*) echo "unknown option $a (--no-build)" >&2; exit 2 ;;
    *) [[ " ${ALL[*]} " == *" $a "* ]] || { echo "unknown framework '$a' (${ALL[*]})" >&2; exit 2; }; FWS+=("$a") ;;
  esac
done
[ ${#FWS[@]} -gt 0 ] || FWS=("${ALL[@]}")
[ -f bench7.env ] || ./scripts/gen-env.sh
for fw in "${FWS[@]}"; do
  if [ "$NOBUILD" = 1 ]; then
    ./scripts/fw.sh toolchain "$fw"
  else
    ./scripts/fw.sh build "$fw"
  fi
done
echo
echo "ready: ${FWS[*]}. Start one with ./run_<framework>.sh (e.g. ./run_${FWS[0]}.sh)"
