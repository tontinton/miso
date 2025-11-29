#!/usr/bin/env bash
set -euo pipefail

PORT="${OPENSEARCH_PORT:-9200}"
JAVA_OPTS="${OPENSEARCH_JAVA_OPTS:-"-Xms256m -Xmx256m"}"

docker run --init -it --rm \
  -p "${PORT}:9200" \
  -e discovery.type=single-node \
  -e DISABLE_SECURITY_PLUGIN=true \
  -e OPENSEARCH_JAVA_OPTS="${JAVA_OPTS}" \
  opensearchproject/opensearch:2.11.0
