#!/usr/bin/env bash
# InfluxDB 3 Core, its plugin venv, yq, rpk, and Redpanda Connect. No Core rust binary. Runs as root.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

: "${YQ_VERSION:?YQ_VERSION not set}"
: "${RPK_VERSION:?RPK_VERSION not set}"
: "${REDPANDA_CONNECT_VERSION:?REDPANDA_CONNECT_VERSION not set}"

INFLUXDB3_PYTHON="/usr/lib/influxdb3/python/bin/python3"
PLUGIN_VENV="/chronoracer/influxdb-plugin-venv"

install -d -m 755 /usr/share/keyrings
curl --silent --location -O https://repos.influxdata.com/influxdata-archive.key
gpg --show-keys --with-fingerprint --with-colons ./influxdata-archive.key 2>&1 \
| grep -q '^fpr:\+24C975CBA61A024EE1B631787C3D57159FC2F927:$' \
&& cat influxdata-archive.key \
| gpg --dearmor \
| tee /usr/share/keyrings/influxdata-archive.gpg > /dev/null \
&& echo 'deb [signed-by=/usr/share/keyrings/influxdata-archive.gpg] https://repos.influxdata.com/debian stable main' \
| tee /etc/apt/sources.list.d/influxdata.list
apt-get update
apt-get install -y --no-install-recommends \
  influxdb3-core \
  python3-yaml \
  python3-venv
rm -f influxdata-archive.key


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

# The race VM has no NAT, so Connect must be installed here
HOME=/root rpk connect install \
  --connect-version "${REDPANDA_CONNECT_VERSION}" \
  --force

command -v influxdb3 >/dev/null
command -v yq >/dev/null
command -v rpk >/dev/null
HOME=/root rpk connect list >/dev/null
command -v python3 >/dev/null
command -v jq >/dev/null
command -v gcloud >/dev/null
python3 -c 'import yaml'

[[ -x "${INFLUXDB3_PYTHON}" ]] || {
  echo "InfluxDB bundled Python not found: ${INFLUXDB3_PYTHON}" >&2
  exit 1
}

install -d -m 755 /chronoracer
"${INFLUXDB3_PYTHON}" -m venv "${PLUGIN_VENV}"
"${PLUGIN_VENV}/bin/python" -m pip install --no-cache-dir pyarrow
"${PLUGIN_VENV}/bin/python" -c 'import pyarrow'
