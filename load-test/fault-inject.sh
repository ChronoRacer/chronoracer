#!/usr/bin/env bash
# S5 fault injection while generators run. Logs UTC times of each action to LOG.
# Usage: fault-inject.sh <stop|freeze> <LOG>
#   stop   — systemctl stop influxdb3 30s then start (also stops Core: chronoracer-core Requires=influxdb3)
#   freeze — SIGSTOP/SIGCONT influxdb3 30s (Core keeps running; preferred DB stall test)
set -euo pipefail

MODE="${1:?mode: stop or freeze}"
LOG="${2:?log path required}"
PROJECT="${PROJECT:-chronoracer}"
ZONE="${ZONE:-europe-west1-b}"

ssh_core() {
  gcloud compute ssh core --project="${PROJECT}" --zone="${ZONE}" --tunnel-through-iap --command "$1" 2>&1 \
    | grep -vE 'NumPy|increase the perf|^$|tcp-forwarding|^WARNING: $'
}

t0=$(date +%s)
wait_until() { while (( $(date +%s) - t0 < $1 )); do sleep 0.5; done; }

case "${MODE}" in
  stop)
    wait_until 120
    echo "$(date -u +%FT%T.%3NZ) stop influxdb3" >>"$LOG"
    ssh_core "sudo systemctl stop influxdb3; date -u +%FT%T.%3NZ; sleep 30; sudo systemctl start influxdb3; date -u +%FT%T.%3NZ; systemctl is-active influxdb3" >>"$LOG"
    echo "$(date -u +%FT%T.%3NZ) influxdb3 start returned" >>"$LOG"
    ;;
  freeze)
    wait_until 120
    echo "$(date -u +%FT%T.%3NZ) SIGSTOP influxdb3" >>"$LOG"
    ssh_core "sudo systemctl kill -s SIGSTOP influxdb3; date -u +%FT%T.%3NZ; sleep 30; sudo systemctl kill -s SIGCONT influxdb3; date -u +%FT%T.%3NZ; systemctl is-active influxdb3 chronoracer-core" >>"$LOG"
    echo "$(date -u +%FT%T.%3NZ) influxdb3 SIGCONT returned" >>"$LOG"
    ;;
  *)
    echo "mode must be stop or freeze" >&2
    exit 2
    ;;
esac

wait_until 210
echo "$(date -u +%FT%T.%3NZ) restart chronoracer-core" >>"$LOG"
ssh_core "sudo systemctl restart chronoracer-core; date -u +%FT%T.%3NZ; systemctl is-active chronoracer-core" >>"$LOG"
echo "$(date -u +%FT%T.%3NZ) done" >>"$LOG"
