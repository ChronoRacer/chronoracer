#!/usr/bin/env bash
# Fail if public Kafka port never binds (boot contract: no private-only broker).
set -euo pipefail

CHRONORACER_DIR="${CHRONORACER_DIR:-/chronoracer}"
PLATFORM_JSON="${CHRONORACER_DIR}/platform.json"
TRIES="${CHRONORACER_PUBLIC_LISTEN_TRIES:-60}"

PORT="$(jq -er --arg key "kafka_public_port" '.[$key] | select(. != null)' "${PLATFORM_JSON}")" || {
  echo "missing kafka_public_port in ${PLATFORM_JSON}" >&2
  exit 1
}

for _ in $(seq 1 "${TRIES}"); do
  if timeout 1 bash -c "true > /dev/tcp/127.0.0.1/${PORT}" 2>/dev/null; then
    echo "Public Kafka listener bound on ${PORT}"
    exit 0
  fi
  sleep 1
done

echo "public TLS listener did not bind on port ${PORT}" >&2
exit 1
