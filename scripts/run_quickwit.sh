#!/usr/bin/env bash
set -euo pipefail

PORT="${QUICKWIT_PORT:-7280}"

docker run --init -it --rm -p "${PORT}:7280" quickwit/quickwit:edge run
