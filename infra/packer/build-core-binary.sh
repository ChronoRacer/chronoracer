#!/usr/bin/env bash
# Build Core on a disposable Spot VM builder for native CPU build.
#
# Optional environment variables:
#   PKR_VAR_project_id  GCP project (otherwise the active gcloud project)
#   CORE_BUILD_ZONE     builder zone (default: europe-west3-a)
#   CORE_BUILD_NETWORK  builder network (default: default)
#
# Usage: build-core-binary.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
INSTANCE="chronoracer-core-build"
ZONE="${CORE_BUILD_ZONE:-europe-west3-a}"
NETWORK="${CORE_BUILD_NETWORK:-default}"
PROJECT_ID="${PKR_VAR_project_id:-$(gcloud config get-value project 2>/dev/null || true)}"

cleanup() {
  gcloud compute instances delete "${INSTANCE}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet >/dev/null 2>&1
}
#trap cleanup EXIT

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

echo "Creating source code bundle"
core_bundle="core_bundle.tar.gz"
tar -czvf "$core_bundle" \
  -C "${REPO_ROOT}" \
  core/src core/Cargo.lock core/Cargo.toml core/.cargo

MACHINE_TYPE="c4d-highcpu-2"
echo "Creating disposable Spot builder ${INSTANCE} (${ZONE}, ${MACHINE_TYPE})"
gcloud compute instances create "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="$MACHINE_TYPE" \
  --provisioning-model=SPOT \
  --instance-termination-action=DELETE \
  --maintenance-policy=TERMINATE \
  --network-interface="network=${NETWORK},network-tier=PREMIUM" \
  --image-family=debian-13 \
  --image-project=debian-cloud \
  --boot-disk-size=10GB \
  --quiet

echo "Installing build dependencies and Rust"
# TODO: Install additional libraries for compression types
gcloud compute ssh "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --quiet \
  --command='set -euo pipefail
sudo dd if=/dev/zero of=/swapfile bs=1M count=4096 status=progress
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
sudo apt-get update
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
  build-essential \
  cmake \
  pkg-config \
  ca-certificates \
  curl \
  python3 \
  libssl-dev \
  libcurl4-openssl-dev \
  mold \
  clang

curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source "$HOME/.cargo/env"
rustup default stable'

echo "Uploading Core source"
gcloud compute scp --recurse \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  "$core_bundle" \
  "${INSTANCE}:/tmp/chronoracer-core.tar.gz" \
  --quiet

echo "Building... "
gcloud compute ssh "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --quiet \
  --command='set -euo pipefail
source "$HOME/.cargo/env"
mkdir -p /tmp/chronoracer-core
tar -xzvf /tmp/chronoracer-core.tar.gz -C /tmp/chronoracer-core
cd /tmp/chronoracer-core/core
export CARGO_BUILD_JOBS=2
export RUSTFLAGS="-C target-cpu=native -C codegen-units=1"
cargo build --locked --release --target x86_64-unknown-linux-gnu
test -s target/x86_64-unknown-linux-gnu/release/chronoracer-core'

echo "Downloading the native binary"
download_dest="build/chronoracer-core"
gcloud compute scp \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  "${INSTANCE}:/tmp/chronoracer-core/core/target/x86_64-unknown-linux-gnu/release/chronoracer-core" \
  "./build/chronoracer-core" \
  --quiet

echo "Built Core release ${download_dest}"
