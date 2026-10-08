#!/usr/bin/env bash
# App host: stop Gin + the metrics sampler (keeps app log + host metrics in results/host/).
exec "$(dirname "$0")/scripts/fw.sh" kill gin "$@"
