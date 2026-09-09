#!/bin/bash
# The phase 4 done-when: a fresh SEEDED source (Terra's 1430+ observations plus
# animals and watering_holes) is backfilled into an empty destination while
# live writes happen during and after the backfill. Proves the snapshot->stream
# seam is gapless and duplicate-free. Exit 0 iff verify prints MATCH.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== build"
go build -o bin/reader ./cmd/reader
go build -o bin/writer ./cmd/writer
go build -o bin/cdcctl ./cmd/cdcctl

echo "== fresh seeded stack (no truncate: the seed IS the test)"
./scripts/reset.sh

PSQL="docker compose exec -T source psql -U postgres -d terra -q"
seeded=$(cd deploy && $PSQL -tAc "SELECT count(*) FROM observations;")
echo "== source seeded with $seeded observations"

start_reader() { ./bin/reader > /tmp/minicdc-reader.log 2>&1 & READER_PID=$!; }
start_writer() { ./bin/writer > /tmp/minicdc-writer.log 2>&1 & WRITER_PID=$!; }
cleanup() { kill "$READER_PID" "$WRITER_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "== start writer, then reader (reader backfills on a fresh slot)"
start_writer
start_reader

# Write live changes WHILE the backfill is in flight: these must interleave
# correctly with the snapshot rows.
echo "== concurrent live load during backfill"
./scripts/load.sh "${1:-80}" > /tmp/minicdc-load.log 2>&1 &
LOAD_PID=$!

wait "$LOAD_PID"
echo "== load finished, verifying (backfill + stream must reconcile)"
./bin/cdcctl verify --timeout 120s
