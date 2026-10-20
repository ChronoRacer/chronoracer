#!/usr/bin/env bash
# Baked onto the Analysis image at /chronoracer/entrypoint.sh. Executed automatically on VM startup.
set -euo pipefail
umask 077

export CHRONORACER_DIR="/chronoracer"
export ANALYSIS_ENV="${CHRONORACER_DIR}/analysis.env"

UNITS=(
  chronoracer-analysis-stream.service
  chronoracer-analysis-rules.service
  chronoracer-analysis-predictions.service
)

[[ -x "${CHRONORACER_DIR}/analysis-venv/bin/python" ]] || {
  echo "baked Analysis venv missing: ${CHRONORACER_DIR}/analysis-venv" >&2
  exit 1
}

install -m 600 /dev/null "${ANALYSIS_ENV}"
for unit in "${UNITS[@]}"; do
  install -m 644 "${CHRONORACER_DIR}/${unit}" "/etc/systemd/system/${unit}"
done
systemctl daemon-reload

"${CHRONORACER_DIR}/render-env.sh"
"${CHRONORACER_DIR}/enable-ops-agent.sh"

systemctl enable --now "${UNITS[@]}"
