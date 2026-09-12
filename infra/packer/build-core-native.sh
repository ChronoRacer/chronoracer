#!/usr/bin/env bash
# Build Core on a disposable Spot c4d VM and publish an immutable release - needed for CPU native compilation.
#
# Optional environment variables:
#   PKR_VAR_project_id  GCP project (otherwise the active gcloud project)
#   CORE_BUILD_ZONE     builder zone (default: europe-west3-a)
#   CORE_BUILD_NETWORK  builder network (default: default)
#
# Usage: build-core-native.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMES_JSON="${REPO_ROOT}/infra/contract/names.json"
WORKDIR="$(mktemp -d)"
INSTANCE="chronoracer-core-build-$(date -u +%Y%m%d%H%M%S)-${RANDOM}"
ZONE="${CORE_BUILD_ZONE:-europe-west3-a}"
NETWORK="${CORE_BUILD_NETWORK:-default}"
PROJECT_ID="${PKR_VAR_project_id:-$(gcloud config get-value project 2>/dev/null || true)}"

cleanup() {
  gcloud compute instances delete "${INSTANCE}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet >/dev/null 2>&1 || true
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need jq
[[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]] || {
  echo "set PKR_VAR_project_id or configure an active gcloud project" >&2
  exit 1
}

BUCKET="$(jq -er '.artifacts_bucket | select(. != null and . != "")' "${NAMES_JSON}")" || {
  echo "artifacts_bucket missing in ${NAMES_JSON}" >&2
  exit 1
}

echo "Creating disposable Spot builder ${INSTANCE} (${ZONE}, c4d-highcpu-2)"
gcloud compute instances create "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type=c4d-highcpu-2 \
  --provisioning-model=SPOT \
  --instance-termination-action=DELETE \
  --maintenance-policy=TERMINATE \
  --network-interface="network=${NETWORK},network-tier=PREMIUM" \
  --image-family=debian-13 \
  --image-project=debian-cloud \
  --boot-disk-size=30GB \
  --quiet

echo "Installing build dependencies and Rust"
gcloud compute ssh "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --quiet \
  --command='set -euo pipefail
sudo apt-get update
sudo apt-get install -y --no-install-recommends build-essential cmake pkg-config libssl-dev curl ca-certificates
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source "$HOME/.cargo/env"
rustup default stable'

echo "Uploading Core source"
gcloud compute scp --recurse \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  "${REPO_ROOT}/core" \
  "${INSTANCE}:/tmp/chronoracer-core" \
  --quiet

echo "Building with C4D-native CPU features"
gcloud compute ssh "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --quiet \
  --command='set -euo pipefail
source "$HOME/.cargo/env"
cd /tmp/chronoracer-core/core
export CARGO_BUILD_JOBS=2
export RUSTFLAGS="-C target-cpu=native -C codegen-units=1"
cargo build --locked --release --target x86_64-unknown-linux-gnu
test -s target/x86_64-unknown-linux-gnu/release/chronoracer-core'

echo "Downloading the native binary"
gcloud compute scp \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  "${INSTANCE}:/tmp/chronoracer-core/core/target/x86_64-unknown-linux-gnu/release/chronoracer-core" \
  "${WORKDIR}/chronoracer-core" \
  --quiet

[[ -s "${WORKDIR}/chronoracer-core" ]] || {
  echo "downloaded Core binary is empty" >&2
  exit 1
}
[[ -s "${REPO_ROOT}/core/avro/sensor_data.avsc" ]] || {
  echo "Avro schema missing or empty" >&2
  exit 1
}

RELEASE="core/releases/$(date -u +%Y%m%dT%H%M%SZ)-${RANDOM}"
DEST="gs://${BUCKET}/${RELEASE}"
MANIFEST="${WORKDIR}/current.json"

echo "Publishing Core binary and schema to ${DEST}"
gcloud storage cp "${WORKDIR}/chronoracer-core" "${DEST}/core"
gcloud storage cp "${REPO_ROOT}/core/avro/sensor_data.avsc" "${DEST}/avro/sensor_data.avsc"
bash "${SCRIPT_DIR}/publish-core-influxdb-plugins.sh" "${DEST}"

jq -n --arg prefix "${RELEASE}" '{prefix: $prefix}' >"${MANIFEST}"
gcloud storage cp "${MANIFEST}" "gs://${BUCKET}/core/current.json"

echo "Published Core release ${DEST}"
