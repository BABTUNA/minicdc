#!/bin/bash
# Open both databases in TablePlus, preconfigured. TablePlus registers a
# handler for postgresql:// URLs, so `open` on a connection string launches it
# straight into that database. Source is tagged green, destination purple.
set -euo pipefail

SRC="postgresql://postgres:minicdc@127.0.0.1:5410/terra?name=minicdc%20source&statusColor=2E9E4F&environment=local&tLSMode=0&usePrivateKey=false&safeModeLevel=0&advancedSafeModeLevel=0"
DST="postgresql://postgres:minicdc@127.0.0.1:5411/warehouse?name=minicdc%20dest&statusColor=8E44AD&environment=local&tLSMode=0&usePrivateKey=false&safeModeLevel=0&advancedSafeModeLevel=0"

open -a TablePlus "$SRC"
sleep 1
open -a TablePlus "$DST"
echo "Opened source (terra, :5410) and destination (warehouse, :5411) in TablePlus."
