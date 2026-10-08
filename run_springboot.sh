#!/usr/bin/env bash
# App host: start Spring Boot (MVC + virtual threads, HikariCP, Caffeine, ParallelGC, presized heap) for a load test.
# Deletes the previous test's rows first. Options: --build (rebuild), --no-clean (keep rows).
exec "$(dirname "$0")/scripts/fw.sh" run springboot "$@"
