#!/usr/bin/env bash
# Count persisted points per sensor table in a reading-time window. Runs ON the Core VM (as root).
# From the laptop:
#   gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap \
#     --command 'sudo bash -s -- <START> <END>' < load-test/influx_count.sh
# START/END: RFC3339 UTC. Reading times trail the produce time by up to batch_window_ms, so pad START by 1 s.
# Queries run one table at a time (a single UNION over all tables can exhaust Core VM memory on large DBs).
set -euo pipefail

START="${1:?START required}"
END="${2:?END required}"
DB="$(yq -r '.persistence.db_name' /chronoracer/core-config.yaml)"
PORT="$(yq -r '.persistence.db_port' /chronoracer/core-config.yaml)"
TOKEN="$(jq -r .token /chronoracer/admin-token.json)"
q() { influxdb3 query --host "http://127.0.0.1:${PORT}" --token "${TOKEN}" --database "${DB}" --format csv "$1"; }

mapfile -t TABLES < <(q "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY table_name" | tail -n +2 | awk NF)
[[ ${#TABLES[@]} -gt 0 ]] || { echo "no tables in ${DB}" >&2; exit 1; }

echo "sensor,points"
total=0
for t in "${TABLES[@]}"; do
  points="$(q "SELECT count(*) AS points FROM \"${t}\" WHERE time >= '${START}' AND time < '${END}'" | tail -n +2 | head -1 | tr -d '"')"
  echo "${t},${points}"
  total=$((total + points))
done
echo "TOTAL,${total}"
