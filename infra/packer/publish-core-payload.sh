#!/usr/bin/env bash
# Upload a built Core binary, Avro schema, and InfluxDB plugins
# as one immutable release, then atomically publish its manifest at
# gs://{artifacts}/core/current.json.
#
# Usage: publish-core-payload.sh release-id /path/to/chronoracer-core
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"
SCHEMA_SRC="${REPO_ROOT}/core/avro/sensor_data.avsc"

[[ -n "$1" ]] || {
  echo "Release ID not provided" >&2
  exit 1
}
RELEASE_ID="$1"

[[ -n "$2" ]] || {
  echo "Core binary path not provided" >&2
  exit 1
}

BIN="$2"
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

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

BUCKET="$(jq -er '.artifacts_bucket | select(. != null and . != "")' "${NAMES_JSON}")" || {
  echo "artifacts_bucket missing in ${NAMES_JSON}" >&2
  exit 1
}

RELEASE="core/releases/${RELEASE_ID}"
DEST="gs://${BUCKET}/${RELEASE}"
MANIFEST="${WORKDIR}/current.json"

echo "Publishing Core payload to ${DEST}"

gcloud storage cp "${BIN}" "${DEST}/core"
gcloud storage cp "${SCHEMA_SRC}" "${DEST}/avro/sensor_data.avsc"

jq -n --arg prefix "${RELEASE}" '{prefix: $prefix}' >"${MANIFEST}"
gcloud storage cp "${MANIFEST}" "gs://${BUCKET}/core/current.json"

echo "Published ${DEST}/core, ${DEST}/avro/sensor_data.avsc, and InfluxDB plugins"
echo "Published release pointer gs://${BUCKET}/core/current.json"
