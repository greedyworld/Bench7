#!/usr/bin/env bash
# App host: start the Fastify app on the Bun runtime (same code as run_node.sh, different runtime).
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run fastify_bun "$@"
