#!/usr/bin/env bash
# App host: stop ASP.NET Core + the metrics sampler (keeps app log + host metrics in results/host/).
exec "$(dirname "$0")/scripts/fw.sh" kill aspnet "$@"
