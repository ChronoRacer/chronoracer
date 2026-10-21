#!/usr/bin/env bash
# Count persisted points per sensor table in a reading-time window. Runs ON the Core VM (as root).
# From the laptop:
#   gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap \
#     --command 'sudo bash -s -- <START> <END>' < scratch/load-test/influx_count.sh
# START/END: RFC3339 UTC. Reading times trail the produce time by up to batch_window_ms, so pad START by 1 s.
set -euo pipefail

START="${1:?START required}"
END="${2:?END required}"
DB="$(yq -r '.persistence.db_name' /chronoracer/core-config.yaml)"
PORT="$(yq -r '.persistence.db_port' /chronoracer/core-config.yaml)"
TOKEN="$(jq -r .token /chronoracer/admin-token.json)"
q() { influxdb3 query --host "http://127.0.0.1:${PORT}" --token "${TOKEN}" --database "${DB}" --format csv "$1"; }

mapfile -t TABLES < <(q "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY table_name" | tail -n +2)
[[ ${#TABLES[@]} -gt 0 ]] || { echo "no tables in ${DB}" >&2; exit 1; }

SQL=""
for t in "${TABLES[@]}"; do
  [[ -n "${SQL}" ]] && SQL+=" UNION ALL "
  SQL+="SELECT '${t}' AS sensor, count(*) AS points FROM \"${t}\" WHERE time >= '${START}' AND time < '${END}'"
done
q "${SQL} ORDER BY sensor" | tee /dev/stderr | awk -F, 'NR > 1 { total += $2 } END { print "TOTAL," total }'
