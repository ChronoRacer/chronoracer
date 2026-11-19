#!/usr/bin/env bash
# Usage: RACE_ID=<id> fault-inject.sh <freeze|stop|restart> LOG
# One fault per run: at 120 s, DB stall for 30 s or Core restart.
set -euo pipefail
# shellcheck source=load-test/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
MODE="${1:?mode required}" LOG="${2:?log path required}"
[[ "$MODE" =~ ^(freeze|stop|restart)$ ]] || { echo 'mode must be freeze, stop or restart' >&2; exit 2; }
require_race
mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
# Recovery executes on the VM too, so a local interruption does not leave Influx
# suspended/stopped. A lost SSH connection triggers the remote EXIT/HUP trap.
case "$MODE" in
  freeze)
    COMMAND='set -e; trap "systemctl kill -s SIGCONT influxdb3" EXIT HUP INT TERM; systemctl kill -s SIGSTOP influxdb3; date -u +%FT%TZ; sleep 30; systemctl kill -s SIGCONT influxdb3; trap - EXIT HUP INT TERM; systemctl is-active influxdb3 chronoracer-core'
    ;;
  stop)
    COMMAND='set -e; trap "systemctl start influxdb3" EXIT HUP INT TERM; systemctl stop influxdb3; date -u +%FT%TZ; sleep 30; systemctl start influxdb3; trap - EXIT HUP INT TERM; systemctl is-active influxdb3 chronoracer-core'
    ;;
  restart)
    COMMAND='set -e; systemctl restart chronoracer-core; systemctl is-active chronoracer-core'
    ;;
esac
sleep 120
echo "$(date -u +%FT%TZ) begin $MODE"
ssh_core "sudo bash -c '$COMMAND'"
echo "$(date -u +%FT%TZ) end $MODE"
