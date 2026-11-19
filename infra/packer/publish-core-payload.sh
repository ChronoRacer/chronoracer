#!/usr/bin/env bash
# Upload a built Core binary as one immutable release,
# then atomically publish its manifest at gs://{artifacts}/core/current.json.
#
# Usage: publish-core-payload.sh release-id
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"

[[ -n "${1:-}" ]] || {
  echo "Release ID not provided" >&2
  exit 1
}
RELEASE_ID="$1"

BIN="./build/core-${RELEASE_ID}"
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

jq -n --arg prefix "${RELEASE}" '{prefix: $prefix}' >"${MANIFEST}"
gcloud storage cp "${MANIFEST}" "gs://${BUCKET}/core/current.json"

echo "Published ${DEST}/core"
echo "Published release pointer gs://${BUCKET}/core/current.json"
