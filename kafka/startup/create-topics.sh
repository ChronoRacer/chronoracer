#!/usr/bin/env bash
# Create the operator event topic on the private listener. Idempotent.
set -euo pipefail

CHRONORACER_DIR="${CHRONORACER_DIR:-/chronoracer}"
PLATFORM_JSON="${CHRONORACER_DIR}/platform.json"
KAFKA_TOPICS="${KAFKA_TOPICS:-/opt/kafka/bin/kafka-topics.sh}"
TRIES="${CHRONORACER_CREATE_TOPICS_TRIES:-60}"

[[ -x "${KAFKA_TOPICS}" ]] || {
  echo "kafka-topics.sh not found: ${KAFKA_TOPICS}" >&2
  exit 1
}

json_get() {
  local value
  value="$(jq -er --arg key "$2" '.[$key] | select(. != null)' "$1")" || {
    echo "missing required key '$2' in $1" >&2
    exit 1
  }
  [[ -n "${value}" ]] || {
    echo "empty value for key '$2' in $1" >&2
    exit 1
  }
  printf '%s' "${value}"
}

PRIVATE_PORT="$(json_get "${PLATFORM_JSON}" kafka_private_port)"
EVENT_TOPIC="$(json_get "${PLATFORM_JSON}" kafka_event_topic)"
BOOTSTRAP="127.0.0.1:${PRIVATE_PORT}"

for _ in $(seq 1 "${TRIES}"); do
  if "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${EVENT_TOPIC}" --partitions 1 --replication-factor 1; then
    echo "Kafka topic '${EVENT_TOPIC}' present"
    exit 0
  fi
  sleep 1
done

echo "failed to create Kafka topic '${EVENT_TOPIC}' on ${BOOTSTRAP}" >&2
exit 1
