#!/usr/bin/env bash
# Usage: run-scenario.sh <RUN_ID> <TOTAL_RATE> <DURATION_S> <INSTANCES>
# Requires: RACE_ID, OKTA_CLIENT_SECRET; run from repo root or any cwd.
set -uo pipefail

: "${RACE_ID:?set RACE_ID}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-/tmp/lt-venv/bin/python}"

ID="$1"
RATE="$2"
DUR="$3"
INST="$4"
RUN="${SCRIPT_DIR}/runs/${ID}"
mkdir -p "${RUN}/artifacts"

clock() {
  {
    echo "== $1 $(date -u +%FT%TZ)"
    chronyc tracking | grep -E 'System time'
    for vm in core kafka; do
      echo "$vm: $(gcloud compute ssh "$vm" --zone=europe-west1-b --tunnel-through-iap \
        --command 'timedatectl timesync-status | grep -E "Offset"' 2>/dev/null | tr -s ' ')"
    done
  } >>"${RUN}/clock.txt"
}

clock before
"${SCRIPT_DIR}/run-generators.sh" "${RATE}" "${DUR}" "${INST}" "${RUN}" 2>&1 | tail -n $((INST * 3 + 4))
echo "gen_exit=${PIPESTATUS[0]}"
clock after
echo "waiting 190 s for log/metric lag"
sleep 175
RACE_ID="${RACE_ID}" "${SCRIPT_DIR}/collect-artifacts.sh" "${RUN}" >"${RUN}/collect.log" 2>&1
echo "collect_exit=$?"
read -r S E < <("${PYTHON}" - "${RUN}/run-metadata.json" <<'PY'
import json, sys
from datetime import datetime, timedelta
m = json.load(open(sys.argv[1]))
p = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
f = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
print(f(p(m["start_utc"]) - timedelta(seconds=1)), f(p(m["end_utc"]) + timedelta(seconds=1)))
PY
)
gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap \
  --command "sudo bash -s -- ${S} ${E}" < "${SCRIPT_DIR}/influx_count.sh" \
  > "${RUN}/artifacts/influx-count.csv" 2>"${RUN}/artifacts/influx-count.err"
echo "count_exit=$?"
"${PYTHON}" "${SCRIPT_DIR}/steady_summary.py" "${RUN}"
