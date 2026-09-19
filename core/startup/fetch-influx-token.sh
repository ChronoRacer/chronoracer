#!/usr/bin/env bash
# Render Influx's admin token file from the shared Secret Manager token.
# Fail closed: without the token neither Core nor the Dashboard can reach the database.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

NAMES_JSON="${CHRONORACER_DIR}/names.json"
TOKEN_FILE="${CHRONORACER_DIR}/admin-token.json"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need jq

[[ -f "${NAMES_JSON}" ]] || {
  echo "contract file missing: ${NAMES_JSON}" >&2
  exit 1
}

[[ -f "${CHRONORACER_DIR}/gce-metadata.sh" ]] || {
  echo "gce-metadata.sh missing: ${CHRONORACER_DIR}/gce-metadata.sh" >&2
  exit 1
}
# shellcheck disable=SC1091
source "${CHRONORACER_DIR}/gce-metadata.sh"

SECRET_ID="$(jq -er '.influx_token_secret' "${NAMES_JSON}")" || {
  echo "missing required key 'influx_token_secret' in ${NAMES_JSON}" >&2
  exit 1
}
PROJECT="$(gce_project_id)" || exit 1

TOKEN_TXT="${WORKDIR}/influx-token.txt"
gcloud secrets versions access latest --secret="${SECRET_ID}" --project="${PROJECT}" >"${TOKEN_TXT}"

TOKEN="$(tr -d '\r\n' <"${TOKEN_TXT}")"
[[ -n "${TOKEN}" ]] || {
  echo "Influx token secret ${SECRET_ID} is empty; upload a version (docs/operator-influx-token.md)" >&2
  exit 1
}

jq -n --arg token "${TOKEN}" \
  '{token: $token, name: "admin", description: "Shared Chronoracer Influx admin token"}' \
  >"${TOKEN_FILE}"
chmod 600 "${TOKEN_FILE}"

echo "Wrote Influx admin token file ${TOKEN_FILE} from secret ${SECRET_ID}"
