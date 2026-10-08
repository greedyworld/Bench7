#!/usr/bin/env bash
# App host: start Gin (pgx pool, ristretto cache, GOGC=400) for a load test.
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run gin "$@"
