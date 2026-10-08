#!/usr/bin/env bash
# Step 1, on EVERY machine (app host, DB host, load generators): Linux tuning for the benchmark
# (network/file limits, VM settings, THP, NVMe scheduler, background timers off). Safe to run again.
#   ./tune_linux.sh      log in again afterwards so the new open-file limit applies to your shell
# Undo everything bench7 did to a machine: sudo ./reset.sh
set -euo pipefail
exec sudo "$(cd "$(dirname "$0")" && pwd)/scripts/tune.sh" "$@"
