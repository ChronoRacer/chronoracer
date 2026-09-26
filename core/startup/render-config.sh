#!/usr/bin/env bash
# Fetch operator YAML from GCS. Instance fields from the GCE metadata server
# (not Compute API). Merge: default-config.yaml <- operator YAML <- platform.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"
: "${CORE_ENV:?CORE_ENV not set}"

DEFAULT_CONFIG="${CHRONORACER_DIR}/default-config.yaml"
NAMES_JSON="${CHRONORACER_DIR}/names.json"
PLATFORM_JSON="${CHRONORACER_DIR}/platform.json"
MERGE_PY="${CHRONORACER_DIR}/merge_config.py"
OUTPUT="${CHRONORACER_DIR}/core-config.yaml"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need python3
need jq
need yq
python3 -c 'import yaml' >/dev/null 2>&1 || {
  echo "missing required python module: yaml (install python3-yaml in the Core image)" >&2
  exit 1
}

[[ -f "${DEFAULT_CONFIG}" ]] || {
  echo "default config not found: ${DEFAULT_CONFIG}" >&2
  exit 1
}
[[ -f "${NAMES_JSON}" && -f "${PLATFORM_JSON}" ]] || {
  echo "contract files missing: ${NAMES_JSON} ${PLATFORM_JSON}" >&2
  exit 1
}
[[ -f "${MERGE_PY}" ]] || {
  echo "merge script not found: ${MERGE_PY}" >&2
  exit 1
}

[[ -f "${CHRONORACER_DIR}/gce-metadata.sh" ]] || {
  echo "gce-metadata.sh missing: ${CHRONORACER_DIR}/gce-metadata.sh" >&2
  exit 1
}
# shellcheck disable=SC1091
source "${CHRONORACER_DIR}/gce-metadata.sh"

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

KAFKA_VM="$(json_get "${NAMES_JSON}" kafka_vm)"
RACE_BUCKET="$(json_get "${NAMES_JSON}" race_bucket)"
KAFKA_PRIVATE_PORT="$(json_get "${PLATFORM_JSON}" kafka_private_port)"
DATA_TOPIC="$(json_get "${PLATFORM_JSON}" kafka_data_topic)"
CONTROL_TOPIC="$(json_get "${PLATFORM_JSON}" kafka_control_topic)"
HEARTBEAT_TOPIC="$(json_get "${PLATFORM_JSON}" kafka_heartbeat_topic)"
EXPORT_DONE_TOPIC="$(json_get "${PLATFORM_JSON}" export_done_topic)"
INFLUX_HTTP_PORT="$(json_get "${PLATFORM_JSON}" influx_http_port)"
INFLUX_DATABASE="$(json_get "${PLATFORM_JSON}" influx_database)"
CORE_TELEMETRY_PORT="$(json_get "${PLATFORM_JSON}" core_telemetry_port)"

PROJECT="$(gce_project_id)" || exit 1
ZONE="$(gce_zone)" || exit 1
RACE_ID="$(gce_attr race-id)" || exit 1
GCS_URI="$(gce_attr core-config-gcs-uri)" || exit 1

OPERATOR_YAML="${WORKDIR}/operator.yaml"
gcloud storage cat "${GCS_URI}" >"${OPERATOR_YAML}"

BROKER="${KAFKA_VM}.${ZONE}.c.${PROJECT}.internal:${KAFKA_PRIVATE_PORT}"
PLATFORM_YAML="${WORKDIR}/platform.yaml"
cat >"${PLATFORM_YAML}" <<EOF
race:
  race_id: "${RACE_ID}"
kafka:
  brokers:
    - "${BROKER}"
  sensor_data_topic:
    name: "${DATA_TOPIC}"
  control_event_topic:
    name: "${CONTROL_TOPIC}"
  heartbeat_topic:
    name: "${HEARTBEAT_TOPIC}"
persistence:
  db_port: ${INFLUX_HTTP_PORT}
  db_name: "${INFLUX_DATABASE}"
telemetry:
  port: ${CORE_TELEMETRY_PORT}
gcp:
  enabled: true
  project_id: "${PROJECT}"
  export_bucket: "${RACE_BUCKET}"
  pubsub_topic_id: "${EXPORT_DONE_TOPIC}"
EOF

TOKEN_FILE="${CHRONORACER_DIR}/admin-token.json"
[[ -s "${TOKEN_FILE}" ]] || {
  echo "admin token file missing: ${TOKEN_FILE}" >&2
  exit 1
}

INFLUX_ADMIN_TOKEN="$(jq -e -r '.token // empty' "${TOKEN_FILE}")" || {
  echo "admin token file has empty token" >&2
  exit 1
}
[[ -n "${INFLUX_ADMIN_TOKEN}" ]] || {
  echo "admin token file has empty token" >&2
  exit 1
}
export INFLUX_ADMIN_TOKEN
yq -i '.persistence.db_password = strenv(INFLUX_ADMIN_TOKEN)' "${PLATFORM_YAML}"
unset INFLUX_ADMIN_TOKEN

python3 "${MERGE_PY}" \
  --defaults "${DEFAULT_CONFIG}" \
  --operator "${OPERATOR_YAML}" \
  --platform "${PLATFORM_YAML}" \
  --output "${OUTPUT}"

chmod 600 "${OUTPUT}"
printf 'CORE_CONFIG_PATH=%s\nAVRO_SENSOR_DATA_SCHEMA_PATH=%s\n' \
  "${OUTPUT}" "${CHRONORACER_DIR}/avro/sensor_data.avsc" >"${CORE_ENV}"
chmod 600 "$CORE_ENV"
echo "Exported configuration to: ${OUTPUT}"
