#!/usr/bin/env bash
# Render the Analysis environment file from GCE instance attributes. Metadata server only.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"
: "${ANALYSIS_ENV:?ANALYSIS_ENV not set}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud

[[ -f "${CHRONORACER_DIR}/gce-metadata.sh" ]] || {
  echo "gce-metadata.sh missing: ${CHRONORACER_DIR}/gce-metadata.sh" >&2
  exit 1
}
# shellcheck disable=SC1091
source "${CHRONORACER_DIR}/gce-metadata.sh"

RACE_ID="$(gce_attr race-id)" || exit 1
CORE_TELEMETRY_URL="$(gce_attr core-telemetry-url)" || exit 1
TELEMETRY_URL="$(gce_attr telemetry-url)" || exit 1

ANALYSIS_INGEST_SECRET_ID="$(gce_attr analysis-ingest-secret-id)" || {
  echo "missing instance metadata analysis-ingest-secret-id" >&2
  exit 1
}
PROJECT="$(gce_project_id)" || exit 1
ANALYSIS_INGEST_TOKEN="$(
  gcloud secrets versions access latest --secret="${ANALYSIS_INGEST_SECRET_ID}" --project="${PROJECT}" | tr -d '\r\n'
)"
[[ ${#ANALYSIS_INGEST_TOKEN} -ge 32 ]] || {
  echo "analysis ingest secret ${ANALYSIS_INGEST_SECRET_ID} missing or too short" >&2
  exit 1
}

{
  printf 'RACE_ID=%s\n' "${RACE_ID}"
  printf 'CORE_TELEMETRY_URL=%s\n' "${CORE_TELEMETRY_URL}"
  printf 'TELEMETRY_URL=%s\n' "${TELEMETRY_URL}"
  printf 'ANALYSIS_INGEST_TOKEN=%s\n' "${ANALYSIS_INGEST_TOKEN}"
} >"${ANALYSIS_ENV}"
chmod 600 "${ANALYSIS_ENV}"
echo "Rendered Analysis environment: ${ANALYSIS_ENV}"
