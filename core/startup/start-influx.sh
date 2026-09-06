#!/usr/bin/env bash
# Start InfluxDB 3 and ensure the configured database + export trigger.
set -euo pipefail
umask 077

: "${CHRONORACER_DIR:?CHRONORACER_DIR not set}"

TOKEN_FILE="${CHRONORACER_DIR}/admin-token.json"
CONFIG="${CHRONORACER_DIR}/core-config.yaml"
PLUGIN_DIR="${CHRONORACER_DIR}/influxdb-plugins"
DATA_DIR="/var/lib/influxdb3"
HOST="http://127.0.0.1:8181"
TRIES="${CHRONORACER_INFLUX_TRIES:-60}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need influxdb3
need jq
need yq

[[ -f "${CONFIG}" ]] || {
  echo "rendered Core config not found: ${CONFIG}" >&2
  exit 1
}
[[ -s "${TOKEN_FILE}" ]] || {
  echo "admin token file missing: ${TOKEN_FILE}" >&2
  exit 1
}
[[ -d "${PLUGIN_DIR}" ]] || {
  echo "plugin dir not found: ${PLUGIN_DIR}" >&2
  exit 1
}

install -d -m 700 "${DATA_DIR}"

chmod 600 "${TOKEN_FILE}"

TOKEN="$(jq -e -r '.token // empty' "${TOKEN_FILE}")" || {
  echo "admin token file has no token: ${TOKEN_FILE}" >&2
  exit 1
}
[[ -n "${TOKEN}" ]] || {
  echo "admin token file has no token: ${TOKEN_FILE}" >&2
  exit 1
}

DB_NAME="$(yq -e -r '.persistence.db_name' "${CONFIG}")"
[[ -n "${DB_NAME}" && "${DB_NAME}" != "null" ]] || {
  echo "persistence.db_name missing in ${CONFIG}" >&2
  exit 1
}

systemctl enable --now influxdb3.service

ok=0
for _ in $(seq 1 "${TRIES}"); do
  if influxdb3 show databases --host "${HOST}" --token "${TOKEN}" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 1
done
[[ "${ok}" -eq 1 ]] || {
  echo "InfluxDB 3 did not become ready on ${HOST}" >&2
  exit 1
}

db_exists() {
  influxdb3 show databases --host "${HOST}" --token "${TOKEN}" 2>/dev/null \
    | grep -F -- "${DB_NAME}" >/dev/null
}

if db_exists; then
  echo "Database '${DB_NAME}' already exists."
else
  influxdb3 create database "${DB_NAME}" --host "${HOST}" --token "${TOKEN}"
fi

if influxdb3 show database-triggers --host "${HOST}" --database "${DB_NAME}" --token "${TOKEN}" 2>/dev/null \
  | grep -Fwq -- "database_export"; then
  echo "Trigger 'database_export' already exists. Dropping"
  influxdb3 delete trigger --force --host "${HOST}" --database "${DB_NAME}" --token "${TOKEN}" database_export
fi

influxdb3 create trigger \
  --host "${HOST}" \
  --database "${DB_NAME}" \
  --token "${TOKEN}" \
  --plugin-filename export_database.py \
  --trigger-spec "request:export_database" \
  database_export

echo "InfluxDB 3 ready (database ${DB_NAME})"
