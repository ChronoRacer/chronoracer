#!/usr/bin/env bash
# Baked onto the Kafka image at /chronoracer/entrypoint.sh. Executed automatically on VM startup.
set -euo pipefail
umask 077

export CHRONORACER_DIR="/chronoracer"

/chronoracer/adjust-limits.sh

install -m 644 "${CHRONORACER_DIR}/kafka.service" /etc/systemd/system/kafka.service
systemctl daemon-reload
"${CHRONORACER_DIR}/enable-ops-agent.sh"
systemctl enable --now kafka.service
