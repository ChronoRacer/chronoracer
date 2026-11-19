#!/usr/bin/env bash
# Parallel load_test.py against the public Kafka listener (operator laptop).
# Usage: run-generators.sh [TOTAL_RATE] [DURATION_S] [INSTANCES] [OUT_DIR]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOAD_TEST="${REPO_ROOT}/curls/load_test/load_test.py"
CONFIG="${GENERATOR_CONFIG:-${SCRIPT_DIR}/generator-config.json}"
PYTHON="${PYTHON:-/tmp/lt-venv/bin/python}"

# Validate generator headroom from achieved rates and pacing; local encoding alone is insufficient.
DEFAULT_TOTAL_RATE=100000
DEFAULT_DURATION_S=600
DEFAULT_INSTANCES=4

TOTAL_RATE="${1:-${DEFAULT_TOTAL_RATE}}"
DURATION_S="${2:-${DEFAULT_DURATION_S}}"
INSTANCES="${3:-${DEFAULT_INSTANCES}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${4:-${SCRIPT_DIR}/runs/${STAMP}-${TOTAL_RATE}}"

if ! [[ "$TOTAL_RATE" =~ ^[0-9]+$ ]] || ! [[ "$INSTANCES" =~ ^[0-9]+$ ]] || [[ "$INSTANCES" -lt 1 ]] || [[ "$TOTAL_RATE" -lt 1 ]]; then
  echo "TOTAL_RATE and INSTANCES must be positive integers" >&2
  exit 2
fi
if ! [[ "$DURATION_S" =~ ^[0-9]+(\.[0-9]+)?$ ]] || ! python3 -c 'import sys; sys.exit(float(sys.argv[1]) <= 0)' "$DURATION_S"; then
  echo "DURATION_S must be a positive number" >&2
  exit 2
fi

python3 - "$CONFIG" <<'PYCONFIG'
import json, sys
config = json.load(open(sys.argv[1]))
if config.get('shutdown', {}).get('enabled', False):
    sys.exit('generator shutdown must be disabled; use export_flow.py after all runs')
PYCONFIG
SECRET_VAR="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["oauth"]["client_secret_env_var"])' "$CONFIG")"
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
  echo "ALLOW_UNSYNCED=1: continuing; Core persistence/probe latencies are not trustworthy for this run." >&2
fi
chronyc tracking 2>/dev/null | grep -E 'System time|Leap status' || true

mkdir -p "${OUT_DIR}"
[[ ! -e "${OUT_DIR}/run-metadata.json" ]] || { echo "run directory already used: ${OUT_DIR}" >&2; exit 2; }
PER_RATE=$((TOTAL_RATE / INSTANCES))
REMAINDER=$((TOTAL_RATE % INSTANCES))
if [[ "$PER_RATE" -lt 1 ]]; then
  echo "TOTAL_RATE too small for INSTANCES=${INSTANCES}" >&2
  exit 2
fi

GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
META="${OUT_DIR}/run-metadata.json"
python3 - "${META}" "${START_TS}" "${TOTAL_RATE}" "${DURATION_S}" "${INSTANCES}" "${PER_RATE}" "${GIT_COMMIT}" "${CONFIG}" "${SCRIPT_DIR}/load-testing-core-config.yaml" <<'PYMETA'
import hashlib, json, os, sys
from pathlib import Path
path, start, rate, duration, instances, per_rate, commit, config, baseline_path = sys.argv[1:]
baseline = Path(baseline_path)
meta = {
    "start_utc": start, "end_utc": None, "total_rate": int(rate),
    "duration_s": float(duration), "instances": int(instances),
    "per_instance_base_rate": int(per_rate), "git_commit": commit,
    "race_id": os.environ.get("RACE_ID"), "config": config,
    "generator_config": json.loads(Path(config).read_text()),
    "core_overlay": baseline.read_text(),
    "core_overlay_sha256": hashlib.sha256(baseline.read_bytes()).hexdigest() if baseline.exists() else None,
    "seeds": [1000 + i for i in range(int(instances))],
}
Path(path).write_text(json.dumps(meta, indent=2) + "\n")
PYMETA

pids=()
fail=0
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

for i in $(seq 0 $((INSTANCES - 1))); do
  rate=$((PER_RATE + (i < REMAINDER ? 1 : 0)))
  seed=$((1000 + i))
  log="${OUT_DIR}/gen-${i}.log"
  control_args=()
  if [[ "$i" -ne 0 ]]; then
    control_args+=(--no-control)
  fi
  "${PYTHON}" -u "${LOAD_TEST}" \
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
"${PYTHON}" "${SCRIPT_DIR}/summarize_generators.py" "${OUT_DIR}"/gen-*.log \
  --json "${OUT_DIR}/generator-summary.json" >"${OUT_DIR}/generator-summary.txt" || fail=1
python3 - "${META}" "${END_TS}" "${fail}" <<'PYMETA'
import json, sys
from pathlib import Path
p = Path(sys.argv[1])
meta = json.loads(p.read_text())
meta.update(end_utc=sys.argv[2], generator_exit=int(sys.argv[3]))
p.write_text(json.dumps(meta, indent=2) + "\n")
PYMETA

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
