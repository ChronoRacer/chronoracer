#!/usr/bin/env bash
# Render /chronoracer/server.properties and kafka.env from metadata + contract.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

NAMES_JSON="${CHRONORACER_DIR}/names.json"
PLATFORM_JSON="${CHRONORACER_DIR}/platform.json"
RENDER_PY="${CHRONORACER_DIR}/render_server_properties.py"
OUTPUT="${CHRONORACER_DIR}/server.properties"
ENV_FILE="${CHRONORACER_DIR}/kafka.env"
KEYSTORE_PATH="${CHRONORACER_KEYSTORE_PATH:-/dev/shm/kafka-keystore.p12}"
PASS_FILE="${CHRONORACER_KEYSTORE_PASS_FILE:-/dev/shm/kafka-keystore.pass}"
LOG_DIRS="${CHRONORACER_KAFKA_LOG_DIRS:-/var/lib/kafka/data}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need python3
need jq

[[ -f "${NAMES_JSON}" && -f "${PLATFORM_JSON}" ]] || {
  echo "contract files missing: ${NAMES_JSON} ${PLATFORM_JSON}" >&2
  exit 1
}
[[ -f "${RENDER_PY}" ]] || {
  echo "render script not found: ${RENDER_PY}" >&2
  exit 1
}
[[ -s "${KEYSTORE_PATH}" && -s "${PASS_FILE}" ]] || {
  echo "keystore and password file are required before render" >&2
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
JWKS_URL="$(json_get "${PLATFORM_JSON}" kafka_oauth_jwks_url)"
PROJECT="$(gce_project_id)" || exit 1
ZONE="$(gce_zone)" || exit 1
PUBLIC_HOST="$(gce_attr kafka-fqdn)" || exit 1

PRIVATE_HOST="${KAFKA_VM}.${ZONE}.c.${PROJECT}.internal"

python3 "${RENDER_PY}" \
  --platform "${PLATFORM_JSON}" \
  --private-host "${PRIVATE_HOST}" \
  --public-host "${PUBLIC_HOST}" \
  --keystore-path "${KEYSTORE_PATH}" \
  --keystore-password-file "${PASS_FILE}" \
  --log-dirs "${LOG_DIRS}" \
  --output "${OUTPUT}"

chmod 600 "${OUTPUT}"
cat >"${ENV_FILE}" <<EOF
KAFKA_OPTS=-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${JWKS_URL}
EOF
chmod 600 "${ENV_FILE}"

echo "Wrote ${OUTPUT} and ${ENV_FILE}"
