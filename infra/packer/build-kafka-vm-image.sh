#!/usr/bin/env bash
# Publish family chronoracer-kafka. Builder uses default VPC + public IP, e2-small.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ -z "${PKR_VAR_project_id:-}" ]]; then
  PKR_VAR_project_id="$(gcloud config get-value project 2>/dev/null || true)"
  export PKR_VAR_project_id
fi
[[ -n "${PKR_VAR_project_id}" && "${PKR_VAR_project_id}" != "(unset)" ]] || {
  echo "set PKR_VAR_project_id or gcloud config project" >&2
  exit 1
}
command -v jq >/dev/null 2>&1 || {
  echo "missing required command: jq" >&2
  exit 1
}

packer init "${SCRIPT_DIR}"
packer build -only=chronoracer-kafka.googlecompute.kafka "${SCRIPT_DIR}"

FAMILY="$(jq -er '.kafka_family | select(. != null and . != "")' "${SCRIPT_DIR}/../contract/names.json")"
bash "${SCRIPT_DIR}/scripts/cleanup-image-family.sh" "${FAMILY}"
