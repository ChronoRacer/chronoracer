#!/usr/bin/env bash
set -eou pipefail

root_dir="$1"
[[ -z "$root_dir" ]] && echo "Pass directory as first arg" >&2 && exit 2

CORE_EXE="$root_dir/bin/chronoracer-core"
INFLUX_DB_EXE="$root_dir/bin/influxdb3-core/influxdb3"

CORE_CONFIG_PATH=$(realpath "../default-config.yaml")
AVRO_SCHEMA_PATH=$(realpath "../avro/sensor_data.avsc")
INFLUX_DB_PLUGIN_DIR=$(realpath "../influxdb-plugins")
TEMPLATES_PATH=$(realpath "../redpanda")

[ -f "$CORE_EXE" ] || (echo "Compile Core first with compile-release.sh" && exit 1)
[ -f "$INFLUX_DB_EXE" ] || (echo "InfluxDB not found at $INFLUX_DB_EXE" && exit 1)

cp -r "$TEMPLATES_PATH" "$root_dir/"

# This script will run Core and InfluxDB with resource restrictions so that a mock VM environment can be tested

# Core

disk_block=$(df -P "$CORE_EXE" | tail -n 1 | awk '{print $1}' | cut -d/ -f3) # /dev/X -> X
min_maj=$(lsblk | grep "$disk_block" | awk '{print $2}')

# Calculate IO limits
disk_size_gb=80
iops=$(( 6 * disk_size_gb ))
bandwidth=$(echo "scale=2;$disk_size_gb*0.28*1048576" | bc | cut -d. -f1)


cat > /etc/systemd/system/corevm.slice << EOF
[Unit]
Description=Shared Resource Pool for InfluxDB and Core
DefaultDependencies=no
Before=slices.target

[Slice]
CPUQuota=400%
MemoryMax=16G
MemoryHigh=15G
IOReadIOPSMax=$min_maj $iops
IOWriteIOPSMax=$min_maj $iops
IOReadBandwidthMax=$min_maj $bandwidth
IOWriteBandwidthMax=$min_maj $bandwidth
EOF


INFLUXDB_SETUP_SCRIPT="$root_dir/bin/setup-influxdb.sh"
cat > "$INFLUXDB_SETUP_SCRIPT" << EOF
#!/usr/bin/env bash
TOKEN=$(sed -n 's/.*"token": *\([^,}]*\).*/\1/p' /etc/influxdb3/admin-token.json | tr -d '" ')
sleep 20
echo "Checking if database 'chrono_db' exists..."
if $INFLUX_DB_EXE show databases --host http://localhost:8181 --token "\$TOKEN" 2>/dev/null | grep -q "chrono_db"; then
  echo "Database 'chrono_db' already exists."
else
  echo "Creating database 'chrono_db'..."
  $INFLUX_DB_EXE create database chrono_db \
    --host http://localhost:8181 \
    --token "\$TOKEN"
fi

if $INFLUX_DB_EXE show database-triggers --host http://localhost:8181 --database chrono_db --token "\$TOKEN" 2>/dev/null | grep -q "database_export"; then
  echo "Trigger 'database_export' already exists."
else
  echo "Registering 'database_export' trigger..."
  $INFLUX_DB_EXE create trigger \
    --host http://localhost:8181 \
    --database chrono_db \
    --token "\$TOKEN" \
    --plugin-filename export_database.py \
    --trigger-spec "request:export_database" \
    database_export
fi
EOF
chmod +x "$INFLUXDB_SETUP_SCRIPT"

cat > /etc/systemd/system/influxdb-core.service << EOF
[Unit]
Description=InfluxDB Service

[Service]
Slice=corevm.slice
Type=exec
ExecStart=$INFLUX_DB_EXE serve \
  --node-id core \
  --plugin-dir $INFLUX_DB_PLUGIN_DIR \
  --disable-package-management \
  --object-store file \
  --data-dir /var/lib/influxdb3 \
  --admin-token-file /etc/influxdb3/admin-token.json
#ExecStartPost=$INFLUXDB_SETUP_SCRIPT
Restart=on-failure
RestartSec=3s
SyslogIdentifier=influxdb

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/app-core.service << EOF
[Unit]
Description=Core Application

BindsTo=influxdb-core.service
After=influxdb-core.service

[Service]
Slice=corevm.slice
WorkingDirectory=$root_dir
Environment="CORE_CONFIG_PATH=$CORE_CONFIG_PATH"
Environment="AVRO_SENSOR_DATA_SCHEMA_PATH=$AVRO_SCHEMA_PATH"
Type=exec
ExecStart=$CORE_EXE
Restart=on-failure
RestartSec=3s
SyslogIdentifier=core

[Install]
WantedBy=multi-user.target
EOF
