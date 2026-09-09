#!/bin/bash
# Measure end-to-end latency under sustained write load, the way Artie's
# benchmarks repo does: each destination row carries its source commit time
# (__minicdc_commit_ts) and its apply time (__minicdc_updated_at); latency is
# the difference. Writes data/latency_<date>.csv and prints a summary line.
#
# These are laptop numbers (colima VM, single broker, Postgres to Postgres),
# meant to show the engine sustains a real rate, not to compare against Artie.
set -euo pipefail
cd "$(dirname "$0")/.."

ITERATIONS="${1:-300}"
mkdir -p data
CSV="data/latency_$(date +%Y%m%d_%H%M%S).csv"

echo "== build"
go build -o bin/reader ./cmd/reader
go build -o bin/writer ./cmd/writer
go build -o bin/cdcctl ./cmd/cdcctl

echo "== fresh stack"
./scripts/reset.sh

# Measure STREAMING latency, which is the number Artie publishes. Backfill rows
# all carry one commit timestamp and would distort the average, so clear the
# seed before the slot exists (same isolation the crash test uses).
echo "== truncate seed (isolate streaming latency)"
(cd deploy && docker compose exec -T source psql -U postgres -d terra -q \
  -c "TRUNCATE observations, animals, watering_holes;")

start_reader() { ./bin/reader > /tmp/minicdc-reader.log 2>&1 & READER_PID=$!; }
start_writer() { ./bin/writer > /tmp/minicdc-writer.log 2>&1 & WRITER_PID=$!; }
cleanup() { kill "$READER_PID" "$WRITER_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "== start pipeline"
start_writer
start_reader

echo "== sustained load: $ITERATIONS iterations"
./scripts/load.sh "$ITERATIONS" > /tmp/minicdc-load.log 2>&1

echo "== let the tail drain, then verify"
./bin/cdcctl verify --timeout 120s

echo "== latency report"
./bin/cdcctl latency --table public.observations --csv "$CSV"
