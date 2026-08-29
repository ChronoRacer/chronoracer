#!/usr/bin/env bash
# Source kafka.env after ExecStartPre wrote it, then exec the broker.
set -euo pipefail

CHRONORACER_DIR="${CHRONORACER_DIR:-/chronoracer}"
ENV_FILE="${CHRONORACER_DIR}/kafka.env"
PROPERTIES="${CHRONORACER_DIR}/server.properties"
KAFKA_START="${KAFKA_START:-/opt/kafka/bin/kafka-server-start.sh}"

[[ -f "${ENV_FILE}" ]] || {
  echo "kafka.env missing: ${ENV_FILE}" >&2
  exit 1
}
[[ -f "${PROPERTIES}" ]] || {
  echo "server.properties missing: ${PROPERTIES}" >&2
  exit 1
}
[[ -x "${KAFKA_START}" ]] || {
  echo "kafka-server-start.sh not found: ${KAFKA_START}" >&2
  exit 1
}

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

exec "${KAFKA_START}" "${PROPERTIES}"
