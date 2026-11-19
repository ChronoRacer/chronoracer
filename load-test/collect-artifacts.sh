#!/usr/bin/env bash
# Usage: RACE_ID=<id> collect-artifacts.sh RUN_DIR [PAD_BEFORE_S=120] [PAD_AFTER_S=180]
set -euo pipefail
# shellcheck source=load-test/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
RUN_DIR="$(cd "${1:?RUN_DIR required}" && pwd)"
require_race
read -r START END < <("$PYTHON" - "$RUN_DIR/run-metadata.json" "${2:-120}" "${3:-180}" "$RACE_ID" <<'PY'
import json, sys
from datetime import datetime, timedelta, timezone
m = json.load(open(sys.argv[1]))
if m.get('race_id') != sys.argv[4]:
    sys.exit('RACE_ID does not match run metadata')
p = lambda s: datetime.fromisoformat(s.replace('Z', '+00:00'))
if not m.get('end_utc'):
    sys.exit('run has no end_utc; generators aborted or still running')
start = p(m['start_utc']) - timedelta(seconds=int(sys.argv[2]))
end = p(m['end_utc']) + timedelta(seconds=int(sys.argv[3]))
if end > datetime.now(timezone.utc):
    sys.exit('padded end is in the future; wait before collecting')
print(start.isoformat(), end.isoformat())
PY
)
[[ -n "${START:-}" && -n "${END:-}" ]] || exit 1
OUT="$RUN_DIR/artifacts"
mkdir -p "$OUT/logs" "$OUT/metrics"
CORE_ID="$(gcloud compute instances describe core --project="$PROJECT" --zone="$ZONE" --format='value(id)')"
KAFKA_ID="$(gcloud compute instances describe kafka --project="$PROJECT" --zone="$ZONE" --format='value(id)')"
WINDOW="timestamp>=\"$START\" AND timestamp<=\"$END\""
status=0
journal() {
  gcloud logging read "resource.type=\"gce_instance\" AND resource.labels.instance_id=\"$2\" AND jsonPayload._SYSTEMD_UNIT=\"$3\" AND $WINDOW" \
    --project="$PROJECT" --order=asc --format='value(timestamp,jsonPayload.MESSAGE)' >"$OUT/logs/$1"
}
journal core.log "$CORE_ID" chronoracer-core.service || status=1
journal kafka.log "$KAFKA_ID" kafka.service || status=1
gcloud logging read "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"$TELEMETRY_SERVICE\" AND resource.labels.location=\"$REGION\" AND $WINDOW" \
  --project="$PROJECT" --order=asc --format=json >"$OUT/logs/telemetry.json" || status=1
grep -E 'Unsettled offsets|Commit position has not advanced' "$OUT/logs/core.log" >"$OUT/logs/offset-lag.log" || true
grep -E 'WARN|ERROR|GCM write failed|publisher skipped' "$OUT/logs/core.log" >"$OUT/logs/core-warn-error.log" || true
"$PYTHON" "$SCRIPT_DIR/collect_metrics.py" --project "$PROJECT" --region "$REGION" \
  --telemetry-service "$TELEMETRY_SERVICE" --race-id "$RACE_ID" \
  --start "$START" --end "$END" --output-dir "$OUT/metrics" >"$OUT/metrics-status.txt" 2>&1 || status=1
"$PYTHON" "$SCRIPT_DIR/summarize_generators.py" "$RUN_DIR"/gen-*.log \
  --csv "$OUT/generators.csv" --json "$OUT/generators.json" >"$OUT/summary.txt" || status=1
cat "$OUT/summary.txt"
echo "artifacts: $OUT (status=$status)"
exit "$status"
