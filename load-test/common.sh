#!/usr/bin/env bash
# Shared operator defaults and race identity validation. No infrastructure mutations.
# shellcheck disable=SC2034 # Used by scripts sourcing this file.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="${PROJECT:-chronoracer}"
REGION="${REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
PYTHON="${PYTHON:-/tmp/lt-venv/bin/python}"
TELEMETRY_SERVICE="${TELEMETRY_SERVICE:-telemetry}"
require_race() {
  : "${RACE_ID:?set RACE_ID}"
  [[ "${RACE_ID}" =~ ^[a-z0-9_-]{1,12}$ ]] || { echo 'invalid RACE_ID' >&2; return 2; }
  local vm actual
  for vm in core kafka; do
    actual="$(gcloud compute instances describe "${vm}" --project="${PROJECT}" --zone="${ZONE}" \
      --format='value(labels.race-id)')" || return
    [[ "$actual" == "$RACE_ID" ]] || { echo "$vm belongs to race $actual, expected $RACE_ID" >&2; return 1; }
  done
}
ssh_core() {
  gcloud compute ssh core --project="${PROJECT}" --zone="${ZONE}" --tunnel-through-iap --command "$1"
}
