#!/usr/bin/env bash
# Parallel load_test.py against the public Kafka listener (operator laptop).
# Usage: run-generators.sh [TOTAL_RATE] [DURATION_S] [INSTANCES] [OUT_DIR]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOAD_TEST="${REPO_ROOT}/curls/load_test/load_test.py"
CONFIG="${SCRIPT_DIR}/generator-config.json"
PYTHON="${PYTHON:-/tmp/lt-venv/bin/python}"

# One instance encodes ~170k pts/s locally (bench_generator.py); 4 keeps each far below that even with librdkafka cost.
DEFAULT_TOTAL_RATE=15000
DEFAULT_DURATION_S=600
DEFAULT_INSTANCES=4

TOTAL_RATE="${1:-${DEFAULT_TOTAL_RATE}}"
DURATION_S="${2:-${DEFAULT_DURATION_S}}"
INSTANCES="${3:-${DEFAULT_INSTANCES}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${4:-${SCRIPT_DIR}/runs/${STAMP}-${TOTAL_RATE}}"

if ! [[ "$TOTAL_RATE" =~ ^[0-9]+$ ]] || ! [[ "$INSTANCES" =~ ^[0-9]+$ ]] || [[ "$INSTANCES" -lt 1 ]]; then
  echo "TOTAL_RATE and INSTANCES must be positive integers" >&2
  exit 2
fi
if ! [[ "$DURATION_S" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "DURATION_S must be a positive number" >&2
  exit 2
fi

SECRET_VAR="$(python3 -c "import json; print(json.load(open('${CONFIG}'))['oauth']['client_secret_env_var'])")"
if [[ -z "${!SECRET_VAR:-}" ]]; then
  echo "Set ${SECRET_VAR} before running (Auth0 client secret for chronoracer-relay)." >&2
  echo "  export ${SECRET_VAR}=\$(gcloud secrets versions access latest --secret=chronoracer-okta-oauth --project=chronoracer)" >&2
  exit 1
fi

# Core latencies are measured from the record timestamps this laptop sets: a skewed clock shifts all of them.
if [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null)" != "yes" ]]; then
  echo "Clock not NTP-synchronized. Fix: sudo timedatectl set-ntp true && chronyc waitsync 30 0.01" >&2
  if [[ "${ALLOW_UNSYNCED:-0}" != "1" ]]; then
    exit 1
  fi
  echo "ALLOW_UNSYNCED=1: continuing; Core preview_ms/persist_ms are not trustworthy for this run." >&2
fi
chronyc tracking 2>/dev/null | grep -E 'System time|Leap status' || true

mkdir -p "${OUT_DIR}"
PER_RATE=$((TOTAL_RATE / INSTANCES))
REMAINDER=$((TOTAL_RATE % INSTANCES))
if [[ "$PER_RATE" -lt 1 ]]; then
  echo "TOTAL_RATE too small for INSTANCES=${INSTANCES}" >&2
  exit 2
fi

GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
META="${OUT_DIR}/run-metadata.json"
python3 - <<PY
import json
from pathlib import Path
meta = {
    "start_utc": "${START_TS}",
    "end_utc": None,
    "total_rate": int("${TOTAL_RATE}"),
    "duration_s": float("${DURATION_S}"),
    "instances": int("${INSTANCES}"),
    "per_instance_base_rate": int("${PER_RATE}"),
    "git_commit": "${GIT_COMMIT}",
    "config": "${CONFIG}",
    "seeds": [1000 + i for i in range(int("${INSTANCES}"))],
}
Path("${META}").write_text(json.dumps(meta, indent=2) + "\n")
PY

pids=()
fail=0
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup INT TERM

for i in $(seq 0 $((INSTANCES - 1))); do
  rate=$((PER_RATE + (i < REMAINDER ? 1 : 0)))
  seed=$((1000 + i))
  log="${OUT_DIR}/gen-${i}.log"
  control_args=()
  if [[ "$i" -ne 0 ]]; then
    control_args+=(--no-control)
  fi
  "${PYTHON}" "${LOAD_TEST}" \
    --config "${CONFIG}" \
    --rate "${rate}" \
    --duration "${DURATION_S}" \
    --seed "${seed}" \
    --report-interval 10 \
    "${control_args[@]}" \
    >"${log}" 2>&1 &
  pids+=("$!")
  echo "started gen-${i} rate=${rate} seed=${seed} pid=${!} log=${log}"
done

for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    fail=1
  fi
done
trap - INT TERM

END_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
python3 - <<PY
import json
from pathlib import Path
p = Path("${META}")
meta = json.loads(p.read_text())
meta["end_utc"] = "${END_TS}"
p.write_text(json.dumps(meta, indent=2) + "\n")
PY

echo ""
echo "=== run ${START_TS} .. ${END_TS} out=${OUT_DIR} ==="
total_points=0
for i in $(seq 0 $((INSTANCES - 1))); do
  log="${OUT_DIR}/gen-${i}.log"
  echo "--- gen-${i} ---"
  grep '\[Summary\]' "${log}" || echo "(no [Summary])"
  grep '\[Sensors\]' "${log}" || echo "(no [Sensors])"
  pts="$(grep '\[Sensors\]' "${log}" | sed -n 's/.* \([0-9][0-9]*\) points in.*/\1/p' | tail -1)"
  if [[ -n "$pts" ]]; then
    total_points=$((total_points + pts))
  fi
done
echo "=== total sensor points (sum of [Sensors] lines): ${total_points} ==="

if [[ "$fail" -ne 0 ]]; then
  echo "One or more generator instances failed" >&2
  exit 1
fi
