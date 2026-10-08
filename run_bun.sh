#!/usr/bin/env bash
# App host: start the Bun app (Bun.serve + Bun.sql, 1 process per vCPU via SO_REUSEPORT) for a load test.
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run bun "$@"
