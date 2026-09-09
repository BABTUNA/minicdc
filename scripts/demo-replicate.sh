#!/bin/bash
# Beat 1 of the demo: watch a single row replicate through an insert, an update,
# and a delete. Assumes the pipeline is running (run scripts/demo-setup.sh
# first). Prints the source and destination tables side by side after each
# change so you can see the row appear, change, and disappear on both databases.
set -euo pipefail
cd "$(dirname "$0")/.."

bold() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
why()  { printf '   \033[2m%s\033[0m\n' "$1"; }
run()  { printf '   $ %s\n' "$*"; "$@"; }

# Unique id per run so repeated demos never collide on the primary key.
ID=$(( 900000 + ($(date +%s) % 90000) ))

SRC_EXEC() { docker exec minicdc-source psql -U postgres -d terra "$@"; }
DST_EXEC() { docker exec minicdc-dest   psql -U postgres -d warehouse "$@"; }

show_source() {
  printf '\n   \033[36mSOURCE  animals (id %s)\033[0m\n' "$ID"
  SRC_EXEC -c "SELECT animal_id, name, status FROM animals WHERE animal_id = $ID;"
}
show_dest() {
  printf '   \033[35mDESTINATION  animals (id %s)\033[0m\n' "$ID"
  DST_EXEC -c "SELECT animal_id, name, status, __minicdc_updated_at FROM animals WHERE animal_id = $ID;"
}
settle() { printf '   \033[2m(waiting ~2s for the change to stream through)\033[0m\n'; sleep 3; }

bold "Following one animal, id $ID, through the pipeline"
why "it does not exist yet on either side"
show_source
show_dest

bold "1/3  INSERT on the source"
why "a brand new row"
run SRC_EXEC -c "INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status) VALUES ($ID, 'Simba', 'lion', 1, 'juvenile');"
settle
show_source
show_dest
why "the row arrived in the destination on its own"

bold "2/3  UPDATE on the source"
why "promote Simba from juvenile to adult"
run SRC_EXEC -c "UPDATE animals SET status = 'adult' WHERE animal_id = $ID;"
settle
show_source
show_dest
why "the destination reflects the new status"

bold "3/3  DELETE on the source"
why "remove the row entirely"
run SRC_EXEC -c "DELETE FROM animals WHERE animal_id = $ID;"
settle
show_source
show_dest
why "gone from the destination too"

printf '\n\033[1;32mDONE\033[0m  insert, update and delete each replicated to the destination within seconds.\n'
