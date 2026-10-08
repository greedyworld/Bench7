#!/usr/bin/env bash
# App host: start ASP.NET Core (minimal API on Kestrel, Npgsql, IMemoryCache, Server GC) for a load test.
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run aspnet "$@"
