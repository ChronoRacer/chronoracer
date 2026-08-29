#!/usr/bin/env bash
# Upload a locally built linux Core binary + InfluxDB plugins to gs://{artifacts}/core/current/.
# Compile x86_64-unknown-linux-gnu yourself (rdkafka cmake-build needs a Linux toolchain).
#
# Usage: publish-core-payload.sh /path/to/chronoracer-core
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"
PLUGIN_SRC="${REPO_ROOT}/core/influxdb-plugins"

[[ $# -eq 1 ]] || {
  echo "usage: $0 /path/to/linux/chronoracer-core" >&2
  exit 1
}

BIN="$1"
[[ -s "${BIN}" ]] || {
  echo "binary missing or empty: ${BIN}" >&2
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

DEST="gs://${BUCKET}/core/current"
echo "Publishing Core payload to ${DEST}"

gcloud storage cp "${BIN}" "${DEST}/core"

if gcloud storage ls "${DEST}/influxdb-plugins" >/dev/null 2>&1; then
  gcloud storage rm --recursive "${DEST}/influxdb-plugins" || true
fi

shopt -s nullglob
plugins=("${PLUGIN_SRC}"/*.py)
[[ ${#plugins[@]} -gt 0 ]] || {
  echo "no plugin .py files in ${PLUGIN_SRC}" >&2
  exit 1
}
for py in "${plugins[@]}"; do
  gcloud storage cp "${py}" "${DEST}/influxdb-plugins/$(basename "${py}")"
done

echo "Published ${DEST}/core and ${#plugins[@]} plugin(s)"
