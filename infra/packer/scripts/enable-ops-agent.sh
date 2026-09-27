#!/usr/bin/env bash
# Copy overlay Ops Agent config and start the unit. Called from VM entrypoints.
# Overlay yaml must already be complete
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

SRC="${CHRONORACER_DIR}/ops-agent.yaml"
DEST="/etc/google-cloud-ops-agent/config.yaml"

[[ -f "${SRC}" ]] || {
  echo "missing Ops Agent config: ${SRC}" >&2
  exit 1
}

# Refuse any leftover __TOKEN__ from a template. Token names stay in renderers, not here.
if grep -Eq '__[A-Z0-9_]+__' "${SRC}"; then
  echo "unsubstituted template token in ${SRC}" >&2
  exit 1
fi

install -d -m 755 /etc/google-cloud-ops-agent
install -m 644 "${SRC}" "${DEST}"

systemctl enable --now google-cloud-ops-agent.service
