#!/usr/bin/env bash
# Packer bake only: needs internet. Analysis venv. Runs as root.
# The race VM has no NAT, so nothing here may run at boot.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

SRC="${ANALYSIS_SRC:-/tmp/analysis-src}"
REQUIREMENTS="${SRC}/requirements.txt"
VENV="/chronoracer/analysis-venv"

[[ -f "${REQUIREMENTS}" ]] || {
  echo "requirements missing: ${REQUIREMENTS}" >&2
  exit 1
}

apt-get update
apt-get install -y --no-install-recommends python3-venv libgomp1

install -d -m 755 /chronoracer
python3 -m venv "${VENV}"
"${VENV}/bin/python" -m pip install --no-cache-dir -r "${REQUIREMENTS}"
"${VENV}/bin/python" -m pip check

"${VENV}/bin/python" -c '
import grpc, orjson, pathway, pydantic, zen, zmq, lightgbm
from google.cloud import storage
from loguru import logger
'

rm -rf "${SRC}"
