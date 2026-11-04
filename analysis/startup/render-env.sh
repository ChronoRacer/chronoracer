#!/usr/bin/env bash
# Render the Analysis environment file from GCE instance attributes. Metadata server only.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"
: "${ANALYSIS_ENV:?ANALYSIS_ENV not set}"

[[ -f "${CHRONORACER_DIR}/gce-metadata.sh" ]] || {
  echo "gce-metadata.sh missing: ${CHRONORACER_DIR}/gce-metadata.sh" >&2
  exit 1
}
# shellcheck disable=SC1091
source "${CHRONORACER_DIR}/gce-metadata.sh"

RACE_ID="$(gce_attr race-id)" || exit 1
CORE_TELEMETRY_URL="$(gce_attr core-telemetry-url)" || exit 1
TELEMETRY_URL="$(gce_attr telemetry-url)" || exit 1
KAFKA_PRIVATE_BOOTSTRAP="$(gce_attr kafka-private-bootstrap)" || exit 1

{
  printf 'RACE_ID=%s\n' "${RACE_ID}"
  printf 'CORE_TELEMETRY_URL=%s\n' "${CORE_TELEMETRY_URL}"
  printf 'TELEMETRY_URL=%s\n' "${TELEMETRY_URL}"
  printf 'KAFKA_PRIVATE_BOOTSTRAP=%s\n' "${KAFKA_PRIVATE_BOOTSTRAP}"
} >"${ANALYSIS_ENV}"
chmod 600 "${ANALYSIS_ENV}"
echo "Rendered Analysis environment: ${ANALYSIS_ENV}"
