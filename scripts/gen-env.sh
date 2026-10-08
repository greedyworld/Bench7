#!/usr/bin/env bash
# Writes bench7.env with fresh random secrets (never committed; see .gitignore).
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
OUT=${1:-$HERE/bench7.env}
[ -f "$OUT" ] && { echo "$OUT exists; delete it first to rotate secrets" >&2; exit 1; }
rnd() { head -c "$1" /dev/urandom | base64 | tr -d '/+=\n' | head -c "$2"; }
umask 077
cat >"$OUT" <<EOF
PG_PASSWORD=$(rnd 32 32)
JWT_SECRET=$(rnd 48 48)
TOKEN_ISSUER_KEY=$(rnd 32 32)
EOF
echo "wrote $OUT"
