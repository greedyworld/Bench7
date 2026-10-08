#!/usr/bin/env bash
# App host: stop Fastify-on-Bun + the metrics sampler (keeps app log + host metrics in results/host/).
exec "$(dirname "$0")/scripts/fw.sh" kill fastify_bun "$@"
