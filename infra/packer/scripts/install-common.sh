#!/usr/bin/env bash
# Packages shared by race VM Packer builders. Runs as root on Debian 13.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  gnupg \
  apt-transport-https \
  python3 \
  jq \
  openssl \
  unzip

install -d -m 755 /usr/share/keyrings
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
  >/etc/apt/sources.list.d/google-cloud-sdk.list
apt-get update
# apt-get install -y --no-install-recommends google-cloud-cli # Gcloud should be installed by default on the VMs

command -v gcloud >/dev/null
command -v python3 >/dev/null
command -v jq >/dev/null
command -v openssl >/dev/null
command -v curl >/dev/null
