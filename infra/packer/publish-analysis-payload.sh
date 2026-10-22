#!/usr/bin/env bash
# Upload Analysis module code as one immutable release,
# then atomically publish its manifest at gs://{artifacts}/analysis/current.json.
#
# Usage: publish-analysis-payload.sh release-id
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"
SRC="${REPO_ROOT}/analysis"
PACKAGES=(shared data_stream rules predictions)

[[ -n "${1:-}" ]] || {
  echo "Release ID not provided" >&2
  exit 1
}
RELEASE_ID="$1"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need jq

for pkg in "${PACKAGES[@]}"; do
  [[ -d "${SRC}/${pkg}" ]] || {
    echo "analysis package missing: ${SRC}/${pkg}" >&2
    exit 1
  }
done

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

BUCKET="$(jq -er '.artifacts_bucket | select(. != null and . != "")' "${NAMES_JSON}")" || {
  echo "artifacts_bucket missing in ${NAMES_JSON}" >&2
  exit 1
}

RELEASE="analysis/releases/${RELEASE_ID}"
DEST="gs://${BUCKET}/${RELEASE}"
MANIFEST="${WORKDIR}/current.json"
STAGE="${WORKDIR}/analysis"

mkdir -p "${STAGE}"
for pkg in "${PACKAGES[@]}"; do
  cp -r "${SRC}/${pkg}" "${STAGE}/${pkg}"
done
find "${STAGE}" -type d \( -name __pycache__ -o -name .mypy_cache -o -name test \) -prune -exec rm -rf {} +

echo "Publishing Analysis payload to ${DEST}"

gcloud storage rsync --recursive "${STAGE}" "${DEST}/analysis"

jq -n --arg prefix "${RELEASE}" '{prefix: $prefix}' >"${MANIFEST}"
gcloud storage cp "${MANIFEST}" "gs://${BUCKET}/analysis/current.json"

echo "Published ${DEST}/analysis"
echo "Published release pointer gs://${BUCKET}/analysis/current.json"
