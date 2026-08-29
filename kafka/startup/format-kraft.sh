#!/usr/bin/env bash
# Format KRaft storage once. Safe to run as systemd ExecStartPre.
set -euo pipefail
umask 077

LOG_DIRS="${CHRONORACER_KAFKA_LOG_DIRS:-/var/lib/kafka/data}"
PROPERTIES="${CHRONORACER_DIR:-/chronoracer}/server.properties"
KAFKA_STORAGE="${KAFKA_STORAGE:-/opt/kafka/bin/kafka-storage.sh}"

[[ -x "${KAFKA_STORAGE}" ]] || {
  echo "kafka-storage.sh not found: ${KAFKA_STORAGE}" >&2
  exit 1
}
[[ -f "${PROPERTIES}" ]] || {
  echo "server.properties missing: ${PROPERTIES}" >&2
  exit 1
}

install -d -m 700 "${LOG_DIRS}"
if [[ -f "${LOG_DIRS}/meta.properties" ]]; then
  echo "KRaft log dir already formatted"
  exit 0
fi

CLUSTER_ID="$("${KAFKA_STORAGE}" random-uuid)"
"${KAFKA_STORAGE}" format --standalone -t "${CLUSTER_ID}" -c "${PROPERTIES}"
echo "Formatted KRaft storage cluster_id=${CLUSTER_ID}"
