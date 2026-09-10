#!/bin/bash
# The phase 2 done-when: mixed load, kill -9 the writer mid-stream, restart it,
# kill -9 the reader, restart it, then prove the destination converged to an
# exact copy. Exit 0 iff verify prints MATCH.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== build"
go build -o bin/reader ./cmd/reader
go build -o bin/writer ./cmd/writer
go build -o bin/cdcctl ./cmd/cdcctl

echo "== fresh stack"
(cd deploy && docker compose down -v --remove-orphans >/dev/null 2>&1; docker compose up -d)

PSQL="docker compose exec -T source psql -U postgres -d terra -q"
until (cd deploy && $PSQL -c 'SELECT 1' >/dev/null 2>&1); do sleep 1; done
until (cd deploy && docker compose exec -T dest pg_isready -U postgres >/dev/null 2>&1); do sleep 1; done

# Phase 2 proves streaming correctness; replicating pre-existing rows is phase
# 4 (backfill). Empty the seed BEFORE the slot exists so the test starts even.
echo "== truncate seed"
(cd deploy && $PSQL -c "TRUNCATE observations, animals, watering_holes;")

start_reader() { ./bin/reader > /tmp/bartie-reader.log 2>&1 & READER_PID=$!; }
start_writer() { ./bin/writer > /tmp/bartie-writer.log 2>&1 & WRITER_PID=$!; }
cleanup() { kill "$READER_PID" "$WRITER_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "== start pipeline"
start_reader
start_writer
sleep 3

echo "== start load"
./scripts/load.sh "${1:-100}" > /tmp/bartie-load.log 2>&1 &
LOAD_PID=$!

sleep 10
echo "== kill -9 writer (pid $WRITER_PID)"
kill -9 "$WRITER_PID"
sleep 3
start_writer
echo "== writer restarted (pid $WRITER_PID)"

sleep 10
echo "== kill -9 reader (pid $READER_PID)"
kill -9 "$READER_PID"
sleep 3
start_reader
echo "== reader restarted (pid $READER_PID)"

wait "$LOAD_PID"
echo "== load finished, verifying"
./bin/cdcctl verify --timeout 120s
