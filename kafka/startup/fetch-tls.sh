#!/usr/bin/env bash
# Fetch Kafka TLS PEM from Secret Manager and write a PKCS12 keystore on tmpfs.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

KEYSTORE_PATH="${CHRONORACER_KEYSTORE_PATH:-/dev/shm/kafka-keystore.p12}"
PASS_FILE="${CHRONORACER_KEYSTORE_PASS_FILE:-/dev/shm/kafka-keystore.pass}"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need gcloud
need openssl

[[ -f "${CHRONORACER_DIR}/gce-metadata.sh" ]] || {
  echo "gce-metadata.sh missing: ${CHRONORACER_DIR}/gce-metadata.sh" >&2
  exit 1
}
# shellcheck disable=SC1091
source "${CHRONORACER_DIR}/gce-metadata.sh"

SECRET_RES="$(gce_attr tls-secret-id)" || exit 1

PEM="${WORKDIR}/tls.pem"
if [[ "${SECRET_RES}" =~ ^projects/([^/]+)/secrets/([^/]+)(/.*)?$ ]]; then
  gcloud secrets versions access latest --secret="${BASH_REMATCH[2]}" --project="${BASH_REMATCH[1]}" >"${PEM}"
else
  PROJECT="$(gce_project_id)" || exit 1
  gcloud secrets versions access latest --secret="${SECRET_RES}" --project="${PROJECT}" >"${PEM}"
fi
[[ -s "${PEM}" ]] || {
  echo "TLS secret ${SECRET_RES} is empty" >&2
  exit 1
}

openssl rand -base64 32 | tr -d '\n' >"${PASS_FILE}"
chmod 600 "${PASS_FILE}"

openssl pkcs12 -export \
  -inkey "${PEM}" \
  -in "${PEM}" \
  -name kafka \
  -out "${KEYSTORE_PATH}" \
  -passout "file:${PASS_FILE}"

chmod 600 "${KEYSTORE_PATH}"
echo "Wrote PKCS12 keystore ${KEYSTORE_PATH}"
