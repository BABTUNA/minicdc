#!/bin/bash
# Full reset: destroy volumes and recreate the stack from scratch. Use after a
# mid-backfill crash (the exported snapshot died with the connection and the
# slot cannot re-export it) or any time you want a clean seeded source.
set -euo pipefail
cd "$(dirname "$0")/../deploy"

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d

until docker compose exec -T source psql -U postgres -d terra -q -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done
until docker compose exec -T dest pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
echo "stack reset and seeded"
