#!/bin/sh

/run.sh &
GRAFANA_PID=$!

echo "Waiting for Grafana to be ready..."
until curl -s -f -u "${GF_SECURITY_ADMIN_USER:-admin}:${GF_SECURITY_ADMIN_PASSWORD:-admin}" \
  http://localhost:3000/api/health > /dev/null; do
    sleep 1
done

echo "Importing v2 dashboards..."
v2dir="/etc/grafana/to_provision/v2"
ADMIN_AUTH="${GF_SECURITY_ADMIN_USER:-admin}:${GF_SECURITY_ADMIN_PASSWORD:-admin}"
API_URL="http://localhost:3000/apis/dashboard.grafana.app/v2/namespaces/default/dashboards"

FOLDER_UID="chronoracer"

for v2dash in "$v2dir"/*.json; do
    [ -f "$v2dash" ] || continue
    dash_name=$(basename "$v2dash" .json | tr '[:upper:]' '[:lower:]' | tr '_' '-')
    existing=$(curl -s -u "$ADMIN_AUTH" "$API_URL/$dash_name")
    resource_version=$(echo "$existing" | jq -r '.metadata.resourceVersion // empty')

    if [ -n "$resource_version" ]; then
        echo "Updating existing dashboard $dash_name..."
        payload=$(jq -n \
          --arg name "$dash_name" \
          --arg folder "$FOLDER_UID" \
          --arg rv "$resource_version" \
          --slurpfile spec "$v2dash" \
          '{
            apiVersion: "dashboard.grafana.app/v2",
            kind: "Dashboard",
            metadata: {
              name: $name,
              resourceVersion: $rv,
              annotations: { "grafana.app/folder": $folder }
            },
            spec: $spec[0]
          }')

        response=$(curl -s -w "\n%{http_code}" -X PUT \
          -u "$ADMIN_AUTH" \
          -H "Content-Type: application/json" \
          -d "$payload" \
          "$API_URL/$dash_name")
    else
        echo "Creating dashboard $dash_name..."
        payload=$(jq -n \
          --arg name "$dash_name" \
          --arg folder "$FOLDER_UID" \
          --slurpfile spec "$v2dash" \
          '{
            apiVersion: "dashboard.grafana.app/v2",
            kind: "Dashboard",
            metadata: {
              name: $name,
              annotations: { "grafana.app/folder": $folder }
            },
            spec: $spec[0]
          }')

        response=$(curl -s -w "\n%{http_code}" -X POST \
          -u "$ADMIN_AUTH" \
          -H "Content-Type: application/json" \
          -d "$payload" \
          "$API_URL")
    fi

    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')

    if [ "$http_code" -eq 200 ] || [ "$http_code" -eq 201 ]; then
        echo "Successfully imported $dash_name (HTTP $http_code)."
    else
        echo "Failed to import $dash_name (HTTP $http_code): $body"
        exit 1
    fi
done

curl -s -f -X PUT \
  -u "$ADMIN_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"homeDashboardUID": "race-fleet"}' \
  "http://localhost:3000/api/org/preferences"

wait $GRAFANA_PID