#!/usr/bin/env bash
# Pull Analysis module code from GCS (Private Google Access). Fail closed.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

NAMES_JSON="${CHRONORACER_DIR}/names.json"
CODE_DIR="${CHRONORACER_DIR}/analysis"
PACKAGES=(shared data_stream rules predictions)
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

CURRENT_MANIFEST="gs://${BUCKET}/analysis/current.json"

echo "Fetching Analysis release pointer from ${CURRENT_MANIFEST}"
gcloud storage cat "${CURRENT_MANIFEST}" >"${WORKDIR}/current.json" || {
  echo "failed to read Analysis release pointer ${CURRENT_MANIFEST}" >&2
  exit 1
}

RELEASE="$(jq -er '.prefix | select(type == "string" and startswith("analysis/releases/") and (contains("..") | not))' "${WORKDIR}/current.json")" || {
  echo "invalid Analysis release pointer in ${CURRENT_MANIFEST}" >&2
  exit 1
}

SRC_CODE="gs://${BUCKET}/${RELEASE}/analysis"

echo "Fetching Analysis payload from ${SRC_CODE}"

mkdir -p "${WORKDIR}/analysis"
gcloud storage rsync --recursive "${SRC_CODE}" "${WORKDIR}/analysis" || {
  echo "failed to copy Analysis code from ${SRC_CODE}" >&2
  exit 1
}

for pkg in "${PACKAGES[@]}"; do
  [[ -d "${WORKDIR}/analysis/${pkg}" ]] || {
    echo "analysis package missing in ${SRC_CODE}: ${pkg}" >&2
    exit 1
  }
done

rm -rf "${CODE_DIR}"
cp -r "${WORKDIR}/analysis" "${CODE_DIR}"
chmod -R u=rwX,go=rX "${CODE_DIR}"

echo "Analysis payload ready (${CODE_DIR})"
