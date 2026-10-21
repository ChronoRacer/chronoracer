#!/usr/bin/env bash
# Pull Core/Influx/Kafka/Telemetry logs and Cloud Monitoring series for one generator run.
# Usage: RACE_ID=<id> collect-artifacts.sh RUN_DIR [PAD_BEFORE_S] [PAD_AFTER_S]
# RUN_DIR is the directory run-generators.sh wrote (needs run-metadata.json with start/end).
# Run it >= 3 min after the run ends: Ops Agent and the 60 s write counters lag behind.
set -euo pipefail

RUN_DIR="$(cd "${1:?RUN_DIR required}" && pwd)"
PAD_BEFORE_S="${2:-60}"
PAD_AFTER_S="${3:-180}"
RACE_ID="${RACE_ID:?set RACE_ID}"
PROJECT="${PROJECT:-chronoracer}"
REGION="${REGION:-europe-west1}"
PYTHON="${PYTHON:-/tmp/lt-venv/bin/python}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

read -r START END < <("${PYTHON}" - "${RUN_DIR}/run-metadata.json" "${PAD_BEFORE_S}" "${PAD_AFTER_S}" <<'PY'
import json, sys
from datetime import datetime, timedelta
meta = json.load(open(sys.argv[1]))
if not meta.get("end_utc"):
    sys.exit("run-metadata.json has no end_utc (run still going or aborted)")
parse = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
fmt = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
print(fmt(parse(meta["start_utc"]) - timedelta(seconds=int(sys.argv[2]))),
      fmt(parse(meta["end_utc"]) + timedelta(seconds=int(sys.argv[3]))))
PY
)
echo "window ${START} .. ${END}"

instance_id() {
  gcloud compute instances list --project="${PROJECT}" \
    --filter="labels.race-id=${RACE_ID} AND name=$1" --format='value(id)'
}
CORE_ID="$(instance_id core)"
KAFKA_ID="$(instance_id kafka)"
[[ -n "${CORE_ID}" && -n "${KAFKA_ID}" ]] || { echo "core/kafka VM not found for race-id=${RACE_ID}" >&2; exit 1; }

OUT="${RUN_DIR}/artifacts"
mkdir -p "${OUT}/logs" "${OUT}/metrics"
WINDOW="timestamp>=\"${START}\" AND timestamp<=\"${END}\""

read_logs() { # output-name filter format
  gcloud logging read "$2 AND ${WINDOW}" --project="${PROJECT}" --order=asc --format="$3" >"${OUT}/logs/$1"
  echo "logs/$1: $(wc -l <"${OUT}/logs/$1") lines"
}
journal() { # output-name instance-id systemd-unit
  read_logs "$1" \
    "resource.type=\"gce_instance\" AND resource.labels.instance_id=\"$2\" AND jsonPayload._SYSTEMD_UNIT=\"$3\"" \
    'value(timestamp,jsonPayload.MESSAGE)'
}

journal core.log "${CORE_ID}" chronoracer-core.service
journal influxdb3.log "${CORE_ID}" influxdb3.service
journal kafka.log "${KAFKA_ID}" kafka.service
read_logs telemetry.log \
  "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"telemetry\" AND resource.labels.location=\"${REGION}\"" \
  'value(timestamp,jsonPayload.type,jsonPayload,textPayload)'

grep 'Pipeline report:' "${OUT}/logs/core.log" >"${OUT}/logs/pipeline-report.log" || true
grep -E 'Unsettled offsets|Commit position has not advanced' "${OUT}/logs/core.log" >"${OUT}/logs/offset-lag.log" || true
grep -E 'WARN|ERROR' "${OUT}/logs/core.log" >"${OUT}/logs/core-warn-error.log" || true

"${PYTHON}" "${SCRIPT_DIR}/collect_metrics.py" --project "${PROJECT}" --region "${REGION}" \
  --race-id "${RACE_ID}" --start "${START}" --end "${END}" --output-dir "${OUT}/metrics"

{
  echo "== Core pipeline report"
  "${PYTHON}" "${SCRIPT_DIR}/parse_pipeline_report.py" "${OUT}/logs/pipeline-report.log" -o "${OUT}/pipeline-report.csv" || true
  echo
  echo "== Generators"
  "${PYTHON}" "${SCRIPT_DIR}/summarize_generators.py" "${RUN_DIR}"/gen-*.log --csv "${OUT}/generators.csv" || true
  echo
  echo "== Core warnings/errors: $(wc -l <"${OUT}/logs/core-warn-error.log")"
  echo "== Stuck-partition warnings: $(grep -c 'Commit position has not advanced' "${OUT}/logs/offset-lag.log" || true)"
} | tee "${OUT}/summary.txt"

echo "artifacts in ${OUT}"
