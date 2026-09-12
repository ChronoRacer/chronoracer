#!/usr/bin/env bash
# Upload the InfluxDB plugin source files into an existing immutable Core
# release. The caller publishes the release pointer after all components exist.
#
# Usage: publish-core-influxdb-plugins.sh gs://bucket/core/releases/release-id
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PLUGIN_SRC="${REPO_ROOT}/core/influxdb-plugins"

[[ $# -eq 1 ]] || {
  echo "usage: $0 gs://bucket/core/releases/release-id" >&2
  exit 1
}

DEST="${1%/}"
[[ "${DEST}" == gs://*/core/releases/* ]] || {
  echo "release destination must be gs://.../core/releases/...: ${DEST}" >&2
  exit 1
}

command -v gcloud >/dev/null 2>&1 || {
  echo "missing required command: gcloud" >&2
  exit 1
}

shopt -s nullglob
plugins=("${PLUGIN_SRC}"/*.py)
[[ ${#plugins[@]} -gt 0 ]] || {
  echo "no plugin .py files in ${PLUGIN_SRC}" >&2
  exit 1
}

for py in "${plugins[@]}"; do
  gcloud storage cp "${py}" "${DEST}/influxdb-plugins/$(basename "${py}")"
done

echo "Published ${#plugins[@]} InfluxDB plugin(s) to ${DEST}/influxdb-plugins"
