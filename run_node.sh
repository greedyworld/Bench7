#!/usr/bin/env bash
# App host: start Fastify on Node.js (node:cluster, 1 process per vCPU, postgres.js) for a load test.
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run node "$@"
