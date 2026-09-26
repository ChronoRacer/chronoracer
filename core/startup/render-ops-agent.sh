#!/usr/bin/env bash
# Copy the Ops Agent template. It has no boot-time substitutions.
# Overlay template stays immutable; generated yaml is what enable-ops-agent.sh installs.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

TMPL="${CHRONORACER_DIR}/ops-agent.yaml.tmpl"
OUT="${CHRONORACER_DIR}/ops-agent.yaml"

[[ -f "${TMPL}" ]] || {
  echo "missing Ops Agent template: ${TMPL}" >&2
  exit 1
}

if grep -Eq '__[A-Z0-9_]+__' "${TMPL}"; then
  echo "unsubstituted template token in ${TMPL}" >&2
  exit 1
fi

install -m 644 "${TMPL}" "${OUT}"
