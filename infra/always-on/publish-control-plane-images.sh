#!/usr/bin/env bash
# Create Artifact Registry (always-on -target), then push Relay + controller.
# Cloud Run apply needs the Relay image to exist. Do this after bootstrap, before
# a full always-on apply.
#
#   ./infra/always-on/publish-control-plane-images.sh
#   ./infra/always-on/publish-control-plane-images.sh --remote
#   CONTROL_PLANE_IMAGE_TAG=v1 ./infra/always-on/publish-control-plane-images.sh relay
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"
TFVARS="${SCRIPT_DIR}/terraform.tfvars"
REGION="europe-west3"
REMOTE=0
IMAGES="all"
NO_PUSH=0

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

usage() {
  echo "usage: $0 [--remote] [all|relay|controller]" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remote) REMOTE=1; shift ;;
    all|relay|controller) IMAGES="$1"; shift ;;
    --nopush) NO_PUSH=1; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

need gcloud
need jq
need terraform

[[ -f "${NAMES_JSON}" ]] || {
  echo "names.json missing: ${NAMES_JSON}" >&2
  exit 1
}

AR_REPO="$(jq -er '.ar_repo' "${NAMES_JSON}")"
RELAY_NAME="$(jq -er '.ar_relay_image' "${NAMES_JSON}")"
CONTROLLER_NAME="$(jq -er '.ar_controller_image' "${NAMES_JSON}")"

PROJECT_ID="${PROJECT_ID:-}"
if [[ -z "${PROJECT_ID}" && -f "${TFVARS}" ]]; then
  PROJECT_ID="$(sed -n 's/^[[:space:]]*project_id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "${TFVARS}" | head -1)"
fi
if [[ -z "${PROJECT_ID}" ]]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
fi
[[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]] && {
  echo "set PROJECT_ID, gcloud config project, or project_id in ${TFVARS}" >&2
  exit 1
}

TAG="${CONTROL_PLANE_IMAGE_TAG:-latest}"
RELAY_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${RELAY_NAME}:${TAG}"
CONTROLLER_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${CONTROLLER_NAME}:${TAG}"

ensure_registry() {
  [[ -f "${TFVARS}" ]] || {
    echo "No tfvars" >&2
    exit 1
  }
  echo "Ensuring Artifact Registry ${AR_REPO} in ${PROJECT_ID} (${REGION})"
  local targets=(
    -target=google_artifact_registry_repository.chronoracer
  )
  if [[ "${REMOTE}" -eq 1 ]]; then
    targets+=(
      -target='google_project_service_identity.this["cloudbuild.googleapis.com"]'
      -target=google_artifact_registry_repository_iam_member.cloudbuild_push
      -target=google_artifact_registry_repository_iam_member.cloudbuild_agent_push
    )
  fi
  (
    cd "${SCRIPT_DIR}"
    terraform init -input=false
    terraform apply -input=false -auto-approve -var-file=terraform.tfvars "${targets[@]}"
  )
}

push_docker() {
  need docker
  if [[ "${NO_PUSH}" -eq 0 ]]; then
    echo "Ensuring registry exists"
    gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
    ensure_registry
  fi
  if [[ "${IMAGES}" == "all" || "${IMAGES}" == "controller" ]]; then
    echo "Building ${CONTROLLER_URI}"
    docker build --platform linux/amd64 -f "${REPO_ROOT}/controller/Dockerfile" -t "${CONTROLLER_URI}" "${REPO_ROOT}"
    if [[ "${NO_PUSH}" -eq 1 ]]; then
      echo "Skipping push for $CONTROLLER_URI"
    else
      docker push "${CONTROLLER_URI}"
    fi
  fi
  if [[ "${IMAGES}" == "all" || "${IMAGES}" == "relay" ]]; then
    echo "Building ${RELAY_URI}"
    docker build --platform linux/amd64 -f "${REPO_ROOT}/relay/Dockerfile" -t "${RELAY_URI}" "${REPO_ROOT}/relay"
    if [[ "${NO_PUSH}" -eq 1 ]]; then
      echo "Skipping push for $RELAY_URI"
    else
      docker push "${RELAY_URI}"
    fi
  fi
}

push_remote() {
  [[ "${IMAGES}" == "all" ]] || {
    echo "--remote publishes both images; omit relay|controller" >&2
    exit 1
  }
  if [[ "${NO_PUSH}" -eq 1 ]]; then
    echo "Cloud Build will always push"
    exit 1
  fi
  ensure_registry
  echo "Submitting Cloud Build for ${RELAY_URI} and ${CONTROLLER_URI} in ${REGION}"
  gcloud builds submit "${REPO_ROOT}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --config="${SCRIPT_DIR}/cloudbuild.control-plane.yaml" \
    --substitutions="_TAG=${TAG},_REGION=${REGION},_REPO=${AR_REPO},_RELAY_IMAGE=${RELAY_NAME},_CONTROLLER_IMAGE=${CONTROLLER_NAME}"
}

if [[ "${REMOTE}" -eq 1 ]]; then
  push_remote
else
  push_docker
fi

echo
echo "Images:"
echo "  relay_image        = \"${RELAY_URI}\""
echo "  controller_image   = \"${CONTROLLER_URI}\""
echo "Put those exact strings in ${TFVARS}, then: terraform apply -var-file=terraform.tfvars"
