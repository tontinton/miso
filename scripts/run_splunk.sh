#!/usr/bin/env bash
set -euo pipefail

PORT="${SPLUNK_PORT:-8089}"
PASSWORD="${SPLUNK_PASSWORD:-testpassword123}"

docker run --init -it --rm \
  -p "${PORT}:8089" \
  -e SPLUNK_START_ARGS=--accept-license \
  -e SPLUNK_PASSWORD="${PASSWORD}" \
  splunk/splunk:9.2.3
