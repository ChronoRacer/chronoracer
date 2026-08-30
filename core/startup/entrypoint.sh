#!/usr/bin/env bash
# Baked onto the Core image at /chronoracer/entrypoint.sh. Executed automatically on VM startup.
set -euo pipefail
umask 077

export CHRONORACER_DIR="/chronoracer"
export CORE_ENV="${CHRONORACER_DIR}/core.env"

install -m 644 /dev/null "${CORE_ENV}"
install -m 644 "${CHRONORACER_DIR}/influxdb3.service" /etc/systemd/system/influxdb3.service
install -m 644 "${CHRONORACER_DIR}/chronoracer-core.service" /etc/systemd/system/chronoracer-core.service
systemctl daemon-reload

"${CHRONORACER_DIR}/fetch-payload.sh"
"${CHRONORACER_DIR}/start-influx.sh"
"${CHRONORACER_DIR}/render-config.sh"

systemctl enable --now chronoracer-core.service
