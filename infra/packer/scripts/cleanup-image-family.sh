#!/usr/bin/env bash
# Keep only the newest image in a Compute Engine image family.
set -euo pipefail

[[ $# -eq 1 ]] || {
  echo "usage: $0 IMAGE_FAMILY" >&2
  exit 1
}

PROJECT_ID="${PKR_VAR_project_id:-}"
[[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]] || {
  echo "PKR_VAR_project_id is required" >&2
  exit 1
}

command -v gcloud >/dev/null 2>&1 || {
  echo "missing required command: gcloud" >&2
  exit 1
}

FAMILY="$1"
[[ "${FAMILY}" =~ ^[a-z][a-z0-9-]*$ ]] || {
  echo "invalid image family: ${FAMILY}" >&2
  exit 1
}

IMAGE_LIST="$(gcloud compute images list \
  --project="${PROJECT_ID}" \
  --filter="family=${FAMILY}" \
  --sort-by="~creationTimestamp" \
  --format="value(name)")" || {
  echo "failed to list image family ${FAMILY}" >&2
  exit 1
}

IMAGES=()
if [[ -n "${IMAGE_LIST}" ]]; then
  mapfile -t IMAGES <<<"${IMAGE_LIST}"
fi

if (( ${#IMAGES[@]} <= 1 )); then
  echo "Image family ${FAMILY}: ${#IMAGES[@]} image(s), nothing to delete."
  exit 0
fi

echo "Image family ${FAMILY}: keeping ${IMAGES[0]} and deleting ${#IMAGES[@]} older image(s)."
for image in "${IMAGES[@]:1}"; do
  gcloud compute images delete "${image}" --project="${PROJECT_ID}" --quiet
done
