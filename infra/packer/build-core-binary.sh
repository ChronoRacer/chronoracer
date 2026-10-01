#!/usr/bin/env bash
# Build Core on a disposable Spot VM builder for native CPU build.
#
# Optional environment variables:
#   PKR_VAR_project_id  GCP project (otherwise the active gcloud project)
#   CORE_BUILD_ZONE     builder zone (default: europe-west1-b)
#   CORE_BUILD_NETWORK  builder network (default: default)
#
# Note: build may take a long time - compilation itself is around a few minutes. If something crashes, SSH into VM and fix manually and delete builder later.
#
# Usage: build-core-binary.sh release-id
# TODO: this script is overall not really good. VM image not worth it for builder, maybe VM can be upscaled to finish faster.
set -euo pipefail

[[ -n "${1:-}" ]] || {
  echo "Release ID parameter is required" >&2
  exit 1
}
RELEASE_ID="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
INSTANCE="chronoracer-core-build"
ZONE="${CORE_BUILD_ZONE:-europe-west1-b}"
NETWORK="${CORE_BUILD_NETWORK:-default}"
PROJECT_ID="${PKR_VAR_project_id:-$(gcloud config get-value project 2>/dev/null || true)}"
core_bundle="core_bundle.tar.gz"
DOWNLOAD_DEST="build/core-${RELEASE_ID}"

[[ -f "$DOWNLOAD_DEST" ]] && {
  echo "Release $RELEASE_ID already seems to be built at $DOWNLOAD_DEST" >&2
  exit 1
}

version_file="${REPO_ROOT}/core/version.txt"
[[ -f "$version_file" ]] || {
  echo "Version file $version_file does not exists" >&2
  exit 1
}

version=$(cat "$version_file" | head -n 1)

if [[ $version != $RELEASE_ID ]]; then
  echo "Version file version $version does not match release ID $RELEASE_ID - if rebuilding disable this check manually" >&2
  exit 1
fi

cleanup() {
    local exit_code=$?
    rm -f "$core_bundle"
    if [ "$exit_code" -eq 0 ]; then
         echo "Removing VM instance..."
         gcloud compute instances delete "${INSTANCE}" \
           --project="${PROJECT_ID}" \
           --zone="${ZONE}" \
           --quiet >/dev/null 2>&1
    else
      echo "Build failed with error code ${exit_code}. VM and its disk might not deleted!!!"
      echo "PROJECT_ID=${PROJECT_ID}"
      echo "ZONE=${ZONE}"
      echo "INSTANCE=${INSTANCE}"
      echo "DOWNLOAD_DEST=${DOWNLOAD_DEST}"
    fi
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

start=$(date +%s)
echo "Creating source code bundle"
tar -czvf "$core_bundle" \
  -C "${REPO_ROOT}" \
  core/src core/Cargo.lock core/Cargo.toml core/.cargo core/build.rs core/proto

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
sleep 20
echo "Installing build dependencies and Rust"
# TODO: Install additional libraries for compression types (currently: snappy, lz4)
gcloud compute ssh "${INSTANCE}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --quiet \
  --command='set -euo pipefail
sudo dd if=/dev/zero of=/swapfile bs=1M count=4096 status=progress
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
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
gcloud compute scp \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  "${INSTANCE}:/tmp/chronoracer-core/core/target/x86_64-unknown-linux-gnu/release/chronoracer-core" \
  "./$DOWNLOAD_DEST" \
  --quiet

echo "Built Core release ${DOWNLOAD_DEST}"
end=$(date +%s);
diff=$((end - start));
printf 'Time taken: %02dm %02ds' $((diff/60)) $((diff%60))