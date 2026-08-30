#!/usr/bin/env bash
# Pull Core binary, Avro schema, and InfluxDB plugins from GCS (Private Google
# Access). Fail closed.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

NAMES_JSON="${CHRONORACER_DIR}/names.json"
PLUGIN_DIR="${CHRONORACER_DIR}/influxdb-plugins"
SCHEMA_DIR="${CHRONORACER_DIR}/avro"
SCHEMA_PATH="${SCHEMA_DIR}/sensor_data.avsc"
CORE_BIN="${CHRONORACER_DIR}/core"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need jq

[[ -f "${NAMES_JSON}" ]] || {
  echo "contract file missing: ${NAMES_JSON}" >&2
  exit 1
}

BUCKET="$(jq -er '.artifacts_bucket | select(. != null and . != "")' "${NAMES_JSON}")" || {
  echo "artifacts_bucket missing in ${NAMES_JSON}" >&2
  exit 1
}

PREFIX="gs://${BUCKET}/core/current"
SRC_BIN="${PREFIX}/core"
SRC_SCHEMA="${PREFIX}/avro/sensor_data.avsc"
SRC_PLUGINS="${PREFIX}/influxdb-plugins"

echo "Fetching Core payload from ${PREFIX}"

gcloud storage cp "${SRC_BIN}" "${WORKDIR}/core" || {
  echo "failed to copy Core binary from ${SRC_BIN}" >&2
  exit 1
}
[[ -s "${WORKDIR}/core" ]] || {
  echo "Core binary empty: ${SRC_BIN}" >&2
  exit 1
}
install -m 755 "${WORKDIR}/core" "${CORE_BIN}"
[[ -x "${CORE_BIN}" ]] || {
  echo "Core binary not executable: ${CORE_BIN}" >&2
  exit 1
}

gcloud storage cp "${SRC_SCHEMA}" "${WORKDIR}/sensor_data.avsc" || {
  echo "failed to copy Avro schema from ${SRC_SCHEMA}" >&2
  exit 1
}
[[ -s "${WORKDIR}/sensor_data.avsc" ]] || {
  echo "Avro schema empty: ${SRC_SCHEMA}" >&2
  exit 1
}
install -d -m 755 "${SCHEMA_DIR}"
install -m 644 "${WORKDIR}/sensor_data.avsc" "${SCHEMA_PATH}"
[[ -s "${SCHEMA_PATH}" ]] || {
  echo "Avro schema missing after install: ${SCHEMA_PATH}" >&2
  exit 1
}

rm -rf "${PLUGIN_DIR}"
install -d -m 755 "${PLUGIN_DIR}"
gcloud storage rsync "${SRC_PLUGINS}" "${PLUGIN_DIR}" || {
  echo "failed to copy plugins from ${SRC_PLUGINS}" >&2
  exit 1
}

shopt -s nullglob
py_files=("${PLUGIN_DIR}"/*.py)
[[ ${#py_files[@]} -gt 0 ]] || {
  echo "no plugin .py files in ${PLUGIN_DIR} (source ${SRC_PLUGINS})" >&2
  exit 1
}
[[ -f "${PLUGIN_DIR}/export_database.py" ]] || {
  echo "missing export_database.py in ${PLUGIN_DIR}" >&2
  exit 1
}

echo "Core payload ready (${CORE_BIN}, schema ${SCHEMA_PATH}, plugins ${PLUGIN_DIR})"
