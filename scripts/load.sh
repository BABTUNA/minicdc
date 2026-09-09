#!/bin/bash
# Mixed insert/update/delete workload against the source, including the TOAST
# case: observations get a big (TOASTed) notes value, then an update that does
# not touch notes. A pipeline that mishandles TOAST turns those notes NULL
# downstream, and verify's digest catches it.
set -euo pipefail
cd "$(dirname "$0")/../deploy"

ITERATIONS="${1:-100}"
PSQL="docker compose exec -T source psql -U postgres -d terra -q"

$PSQL <<'SQL'
INSERT INTO watering_holes (watering_hole_id, name, region, latitude, longitude)
VALUES (500, 'Load Pan', 'Load Region', -1.0, 30.0)
ON CONFLICT DO NOTHING;
SQL

# Unique id base per run (seconds since epoch, mod 1e6) so repeated demo runs
# do not collide on primary keys. Stays well under int4 range.
BASE=$(( 100000 + ($(date +%s) % 1000000) ))

for i in $(seq 1 "$ITERATIONS"); do
  id=$((BASE + i))
  $PSQL <<SQL
INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status)
VALUES ($id, 'load-$i', 'impala', 500, 'adult');
INSERT INTO observations (observation_id, animal_id, watering_hole_id, observed_at, notes)
VALUES ($id, $id, 500, now(),
        (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300)));
UPDATE animals SET status = 'collared' WHERE animal_id = $id;
UPDATE observations SET observed_at = observed_at + interval '1 second'
WHERE observation_id = $id; -- touches the row, not the TOASTed notes
SQL
  if (( i % 3 == 0 && i > 1 )); then
    $PSQL -c "DELETE FROM observations WHERE observation_id = $((id - 1));" \
          -c "DELETE FROM animals WHERE animal_id = $((id - 1));" >/dev/null
  fi
done
echo "load done: $ITERATIONS iterations"
