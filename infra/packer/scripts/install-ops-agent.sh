#!/usr/bin/env bash
# Google Cloud Ops Agent. Shared by Kafka and Core Packer builders. Runs as root.
# Starts disabled: race boot copies overlay config then enables the unit.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

: "${OPS_AGENT_VERSION:?OPS_AGENT_VERSION not set}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

curl -fsSL -o "${WORKDIR}/add-google-cloud-ops-agent-repo.sh" \
  https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash "${WORKDIR}/add-google-cloud-ops-agent-repo.sh" --also-install --version="${OPS_AGENT_VERSION}"

dpkg-query --show --showformat '${Package} ${Version}\n' google-cloud-ops-agent
systemctl cat google-cloud-ops-agent.service >/dev/null

# Overlay config is applied at first race boot. Do not ship the default syslog pipeline.
systemctl disable --now google-cloud-ops-agent.service
