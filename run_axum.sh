#!/usr/bin/env bash
# App host: start axum (tokio, sqlx, moka) for a load test.
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run axum "$@"
