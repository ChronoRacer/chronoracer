#!/usr/bin/env bash
# Packer bake only: needs internet. Analysis venv and module code. Runs as root.
# The race VM has no NAT, so nothing here may run at boot.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

SRC="${ANALYSIS_SRC:-/tmp/analysis-src}"
REQUIREMENTS="${SRC}/requirements-todo.txt"
VENV="/chronoracer/analysis-venv"
CODE_DIR="/chronoracer/analysis"
PACKAGES=(shared data_stream rules predictions)

[[ -f "${REQUIREMENTS}" ]] || {
  echo "requirements missing: ${REQUIREMENTS}" >&2
  exit 1
}
for pkg in "${PACKAGES[@]}"; do
  [[ -d "${SRC}/${pkg}" ]] || {
    echo "analysis package missing: ${SRC}/${pkg}" >&2
    exit 1
  }
done

apt-get update
apt-get install -y --no-install-recommends python3-venv

install -d -m 755 /chronoracer "${CODE_DIR}"
python3 -m venv "${VENV}"
"${VENV}/bin/python" -m pip install --no-cache-dir -r "${REQUIREMENTS}"
"${VENV}/bin/python" -m pip check

for pkg in "${PACKAGES[@]}"; do
  rm -rf "${CODE_DIR:?}/${pkg}"
  cp -r "${SRC}/${pkg}" "${CODE_DIR}/${pkg}"
done
find "${CODE_DIR}" -type d \( -name __pycache__ -o -name .mypy_cache \) -prune -exec rm -rf {} +
chmod -R u=rwX,go=rX "${CODE_DIR}"

PYTHONPATH=/chronoracer "${VENV}/bin/python" -c '
import grpc, orjson, pathway, pydantic, zen, zmq
from google.cloud import storage
from loguru import logger
from analysis.shared.proto import telemetry_v1_pb2, telemetry_v1_pb2_grpc
'

rm -rf "${SRC}"
