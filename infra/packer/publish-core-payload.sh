#!/usr/bin/env bash
# Upload a locally built Linux Core binary, Avro schema, and InfluxDB plugins
# as one immutable release, then atomically publish its manifest at
# gs://{artifacts}/core/current.json.
# Compile x86_64-unknown-linux-gnu (rdkafka cmake-build needs a Linux toolchain).
#
# Usage: publish-core-payload.sh /path/to/chronoracer-core
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"
SCHEMA_SRC="${REPO_ROOT}/core/avro/sensor_data.avsc"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

[[ $# -eq 1 ]] || {
  echo "usage: $0 /path/to/linux/chronoracer-core" >&2
  exit 1
}

BIN="$1"
[[ -s "${BIN}" ]] || {
  echo "binary missing or empty: ${BIN}" >&2
  exit 1
}
[[ -s "${SCHEMA_SRC}" ]] || {
  echo "Avro schema missing or empty: ${SCHEMA_SRC}" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need jq

BUCKET="$(jq -er '.artifacts_bucket | select(. != null and . != "")' "${NAMES_JSON}")" || {
  echo "artifacts_bucket missing in ${NAMES_JSON}" >&2
  exit 1
}

RELEASE="core/releases/$(date -u +%Y%m%dT%H%M%SZ)-${RANDOM}"
DEST="gs://${BUCKET}/${RELEASE}"
MANIFEST="${WORKDIR}/current.json"

echo "Publishing Core payload to ${DEST}"

gcloud storage cp "${BIN}" "${DEST}/core"
gcloud storage cp "${SCHEMA_SRC}" "${DEST}/avro/sensor_data.avsc"

bash "${SCRIPT_DIR}/publish-core-influxdb-plugins.sh" "${DEST}"

jq -n --arg prefix "${RELEASE}" '{prefix: $prefix}' >"${MANIFEST}"
gcloud storage cp "${MANIFEST}" "gs://${BUCKET}/core/current.json"

echo "Published ${DEST}/core, ${DEST}/avro/sensor_data.avsc, and InfluxDB plugins"
echo "Published release pointer gs://${BUCKET}/core/current.json"
