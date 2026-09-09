#!/bin/bash
# Clones artie-labs/terra next to the compose file. Terra has no license, so it
# stays an external dependency we run unmodified, never vendored into this repo.
set -euo pipefail
cd "$(dirname "$0")"

if [ -d terra ]; then
  echo "terra already present, skipping clone"
  exit 0
fi

git clone --depth 1 https://github.com/artie-labs/terra.git
echo "terra fetched. Start the stack with: docker compose up -d"
