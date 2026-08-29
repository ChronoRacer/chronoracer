#!/usr/bin/env bash
# InfluxDB 3 Core, yq, rpk. No Core rust binary. Runs as root.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

: "${YQ_VERSION:?YQ_VERSION not set}"
: "${RPK_VERSION:?RPK_VERSION not set}"
: "${INFLUXDB3_PACKAGE:?INFLUXDB3_PACKAGE not set}"

curl --silent --location -O https://repos.influxdata.com/influxdata-archive.key
gpg --show-keys --with-fingerprint --with-colons ./influxdata-archive.key 2>&1 \
  | grep -q '^fpr:\+24C975CBA61A024EE1B631787C3D57159FC2F927:$'
install -d -m 755 /usr/share/keyrings
cat influxdata-archive.key | gpg --dearmor >/usr/share/keyrings/influxdata-archive.gpg
echo 'deb [signed-by=/usr/share/keyrings/influxdata-archive.gpg] https://repos.influxdata.com/debian stable main' \
  >/etc/apt/sources.list.d/influxdata.list
rm -f influxdata-archive.key
apt-get update

if [[ -n "${INFLUXDB3_PACKAGE_VERSION:-}" ]]; then
  apt-get install -y --no-install-recommends \
    "${INFLUXDB3_PACKAGE}=${INFLUXDB3_PACKAGE_VERSION}" \
    python3-yaml
else
  apt-get install -y --no-install-recommends \
    "${INFLUXDB3_PACKAGE}" \
    python3-yaml
fi

systemctl disable --now influxdb3-core.service 2>/dev/null || true
systemctl mask influxdb3-core.service 2>/dev/null || true

curl -fsSL "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_amd64" \
  -o /usr/local/bin/yq
chmod 755 /usr/local/bin/yq

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT
curl -fsSL "https://github.com/redpanda-data/redpanda/releases/download/${RPK_VERSION}/rpk-linux-amd64.zip" \
  -o "${WORKDIR}/rpk.zip"
unzip -o "${WORKDIR}/rpk.zip" -d "${WORKDIR}"
install -m 755 "${WORKDIR}/rpk" /usr/local/bin/rpk

command -v influxdb3 >/dev/null
command -v yq >/dev/null
command -v rpk >/dev/null
command -v python3 >/dev/null
command -v jq >/dev/null
command -v gcloud >/dev/null
python3 -c 'import yaml'
