#!/usr/bin/env bash
# Baked onto the Core image at /chronoracer/entrypoint.sh. Executed automatically on VM startup.
set -euo pipefail
umask 077

export CHRONORACER_DIR="/chronoracer"
export CORE_ENV="${CHRONORACER_DIR}/core.env"
TOKEN_FILE="${CHRONORACER_DIR}/admin-token.json"

install -m 600 /dev/null "${CORE_ENV}"
install -m 644 "${CHRONORACER_DIR}/influxdb3.service" /etc/systemd/system/influxdb3.service
install -m 644 "${CHRONORACER_DIR}/chronoracer-core.service" /etc/systemd/system/chronoracer-core.service
systemctl daemon-reload

if [[ ! -s "${TOKEN_FILE}" ]]; then
  influxdb3 create token --admin --offline --output-file "${TOKEN_FILE}"
fi
chmod 600 "${TOKEN_FILE}"

"${CHRONORACER_DIR}/fetch-payload.sh"
"${CHRONORACER_DIR}/render-config.sh"
"${CHRONORACER_DIR}/start-influx.sh"

systemctl enable --now chronoracer-core.service
