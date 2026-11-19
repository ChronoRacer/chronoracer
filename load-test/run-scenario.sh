#!/usr/bin/env bash
# Usage: run-scenario.sh RUN_ID TOTAL_RATE DURATION_S INSTANCES [freeze|stop|restart]
set -euo pipefail
# shellcheck source=load-test/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
[[ $# -ge 4 && $# -le 5 ]] || { echo 'usage: run-scenario.sh RUN_ID RATE DURATION INSTANCES [freeze|stop|restart]' >&2; exit 2; }
ID="$1" RATE="$2" DUR="$3" INST="$4" FAULT="${5:-}"
[[ "$ID" =~ ^[a-zA-Z0-9_-]+$ ]] || { echo 'invalid RUN_ID' >&2; exit 2; }
[[ -z "$FAULT" || "$FAULT" =~ ^(freeze|stop|restart)$ ]] || { echo 'invalid fault mode' >&2; exit 2; }
if [[ -n "$FAULT" ]]; then
  "$PYTHON" -c 'import sys; sys.exit(float(sys.argv[1]) < 300)' "$DUR" || { echo 'fault runs require at least 300 s' >&2; exit 2; }
fi
require_race
RUN="${SCRIPT_DIR}/runs/${ID}"
[[ ! -e "${RUN}/run-metadata.json" ]] || { echo "run already exists: $RUN" >&2; exit 2; }
mkdir -p "$RUN/artifacts"
clock() {
  {
    echo "$1 $(date -u +%FT%TZ)"
    chronyc tracking 2>/dev/null || true
    for vm in core kafka; do
      echo "$vm"
      gcloud compute ssh "$vm" --project="$PROJECT" --zone="$ZONE" --tunnel-through-iap \
        --command 'timedatectl timesync-status' || true
    done
  } >>"$RUN/clock.txt" 2>&1
}
clock before
fault_pid=''
# shellcheck disable=SC2329 # Invoked through signal traps.
cleanup() {
  if [[ -n "$fault_pid" ]]; then
    kill "$fault_pid" 2>/dev/null || true
    wait "$fault_pid" 2>/dev/null || true
  fi
}
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
status=0
if [[ -n "$FAULT" ]]; then
  "$SCRIPT_DIR/fault-inject.sh" "$FAULT" "$RUN/fault.log" &
  fault_pid=$!
fi
"$SCRIPT_DIR/run-generators.sh" "$RATE" "$DUR" "$INST" "$RUN" >"$RUN/generators.log" 2>&1 || status=1
if [[ -n "$fault_pid" ]]; then
  wait "$fault_pid" || status=1
  fault_pid=''
fi
# Do not try to analyze an aborted run without end_utc.
[[ -f "$RUN/run-metadata.json" ]] || exit 1
clock after
# Default: 180 s drain padding + up to 240 s native metric visibility lag.
# collect-artifacts also checks the padded end is not in the future.
echo 'waiting for drain window and Monitoring visibility'
sleep "${METRIC_WAIT_S:-420}"
"$SCRIPT_DIR/collect-artifacts.sh" "$RUN" >"$RUN/collect.log" 2>&1 || status=1
if [[ "${INFLUX_COUNT:-0}" == 1 ]]; then
  read -r START END < <("$PYTHON" - "$RUN/run-metadata.json" <<'PY'
import json, sys
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]).resolve().parents[2]))
from collect_metrics import parse_rfc3339
m = json.load(open(sys.argv[1]))
print((parse_rfc3339(m['start_utc']) - timedelta(seconds=1)).isoformat(),
      (parse_rfc3339(m['end_utc']) + timedelta(seconds=1)).isoformat())
PY
)
  gcloud compute ssh core --project="$PROJECT" --zone="$ZONE" --tunnel-through-iap \
    --command "sudo bash -s -- '$START' '$END'" <"$SCRIPT_DIR/influx_count.sh" \
    >"$RUN/artifacts/influx-count.csv" 2>"$RUN/artifacts/influx-count.err" || status=1
fi
"$PYTHON" "$SCRIPT_DIR/steady_summary.py" "$RUN" || status=1
echo "run artifacts: $RUN (status=$status)"
exit "$status"
