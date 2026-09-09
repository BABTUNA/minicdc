#!/bin/bash
# Beat 3 of the demo: measure end-to-end streaming latency the way Artie's own
# benchmarks repo does. Assumes the pipeline is already running (run
# scripts/demo-setup.sh first). Runs a burst of live changes, then reports how
# far behind the source each row landed, measured from timestamps stamped on
# every destination row.
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
why()  { printf '   \033[2m%s\033[0m\n' "$1"; }
run()  { printf '   $ %s\n' "$*"; "$@"; }

ITER="${1:-150}"
mkdir -p data
CSV="data/latency_$(date +%Y%m%d_%H%M%S).csv"

bold "1/3  Send a burst of live changes"
why "each row carries its source commit time; the writer stamps its apply time"
start=$SECONDS
run ./scripts/load.sh "$ITER"

bold "2/3  Let the stream drain"
why "waiting for the last events to reach the destination"
sleep 4

bold "3/3  Report end-to-end latency (streaming rows only)"
why "latency = destination apply time - source commit time, per row, bucketed by minute"
# Measure only rows applied since the burst began, which excludes the backfill
# rows (all stamped at setup time) and leaves a clean streaming number.
window=$(( SECONDS - start + 5 ))
run ./bin/cdcctl latency --table public.observations --since "${window}s" --csv "$CSV"

printf '\n\033[2mper-minute detail written to %s\033[0m\n' "$CSV"
printf '\033[1;32mDONE\033[0m  measured the same way Artie measures: source commit time vs destination load time.\n'
