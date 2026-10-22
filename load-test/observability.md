# Load test observability (15k pts/s baseline)

Project `chronoracer`, region `europe-west1`, zone `europe-west1-b`. Placeholders: `<RACE_ID>`, `<START>`, `<END>` (RFC3339, e.g. `2026-10-21T18:00:00+02:00`).

Race bucket: `chronoracer-race` ([`infra/contract/names.json`](../infra/contract/names.json)). VM **names** are always `core` and `kafka`; disambiguate races with label `race-id=<RACE_ID>` or the test time window.

## 1. Core operator overlay (`core-config.yaml`)

### Merge order

[`render-config.sh`](../core/startup/render-config.sh): `default-config.yaml` ← operator GCS object ← platform YAML (platform wins). [`merge_config.py`](../core/startup/merge_config.py) deep-merges dicts.

**Required:** `gs://chronoracer-race/<RACE_ID>/core-config.yaml` must exist; boot runs `gcloud storage cat` on metadata `core-config-gcs-uri` (fail closed if missing). Empty mapping `{}` is valid YAML but leaves `race.initial_driver_id` at default `placeholder` (valid); Relay `/race/start` injects `race.initial_driver_id` from `initialDriverId` and overrides operator values.

**Platform overlay forces** (operator cannot override): `race.race_id`, `kafka.brokers`, `kafka.sensor_data_topic.name`, `kafka.control_event_topic.name`, `kafka.heartbeat_topic.name`, `persistence.db_port`, `persistence.db_name`, `persistence.db_password` (Influx admin token from VM secret), `telemetry.port`, `gcp.enabled`, `gcp.project_id`, `gcp.export_bucket`, `gcp.pubsub_topic_id`.

**Core validation** ([`configuration.rs`](../core/src/configuration.rs)): non-empty `kafka.group_id`; `race.initial_driver_id` driver-id rules; telemetry subscriber bounds; persistence retry backoff ordering. Operator YAML is not schema-validated by Relay.

**Minimal overlay for this test** (see [`core-config.yaml`](./core-config.yaml)):

```yaml
kafka:
  sensor_data_consumer:
    offset_lag_log_interval_s: 10
race:
  initial_driver_id: loadtest
```

Upload (do not run from automation; read-only rule):

```bash
gcloud storage cp load-test/core-config.yaml \
  gs://chronoracer-race/<RACE_ID>/core-config.yaml
```

After upload, restart Core or re-run race apply so `render-config.sh` runs if the VM already booted with an older object.

---

## 2. Logs (Cloud Logging + journal fallback)

### Path to Cloud Logging

- Core/Kafka images: [`enable-ops-agent.sh`](../infra/packer/scripts/enable-ops-agent.sh) installs [`core/startup/ops-agent.yaml`](../core/startup/ops-agent.yaml) / [`kafka/startup/ops-agent.yaml`](../kafka/startup/ops-agent.yaml).
- **Receivers:** `systemd_journald` only (no syslog files).
- **Processors:** exclude everything except `chronoracer-core.service` + `influxdb3.service` (Core VM) or `kafka.service` (Kafka VM).
- Core stdout (Rust `tracing` → journal) lands in `jsonPayload.MESSAGE` with `jsonPayload._SYSTEMD_UNIT`.
- Project log **exclusions** drop Ops Agent self-noise on GCE ([`infra/always-on/logging.tf`](../infra/always-on/logging.tf)); they do not drop application journal lines.

Resolve Core `instance_id` for tight filters (recommended when VM names are reused):

```bash
CORE_ID="$(gcloud compute instances list --project=chronoracer \
  --filter='labels.race-id=<RACE_ID> AND name=core' \
  --format='value(id)')"
```

Shared filter prefix:

```text
resource.type="gce_instance"
resource.labels.project_id="chronoracer"
resource.labels.instance_id="${CORE_ID}"
timestamp>="<START>" AND timestamp<="<END>"
```

If `${CORE_ID}` is empty, fall back to `resource.labels.instance_id` from the test window only, or `labels."compute.googleapis.com/resource_name"="core"` plus time bounds (may include other races if names were reused in retention).

**Format** (timestamp + message):

```bash
--format='table(timestamp.date("%Y-%m-%dT%H:%M:%E*S%:z"),jsonPayload.MESSAGE)'
```

If `MESSAGE` is empty, pipe JSON and `jq -r '[.timestamp, (.jsonPayload.MESSAGE // .textPayload)] | @tsv'`.

### (a) Pipeline report lines

Emitted every `offset_lag_log_interval_s` after lag logging ([`message_sink.rs`](../core/src/messaging/message_sink.rs), [`component-core.md#pipeline-report`](../docs/component-core.md#pipeline-report)).

```bash
gcloud logging read \
'resource.type="gce_instance"
 resource.labels.project_id="chronoracer"
 resource.labels.instance_id="'"${CORE_ID}"'"
 jsonPayload._SYSTEMD_UNIT="chronoracer-core.service"
 jsonPayload.MESSAGE=~"Pipeline report:"
 timestamp>="<START>" AND timestamp<="<END>"' \
  --project=chronoracer --limit=10000 --order=asc \
  --format='table(timestamp.date("%Y-%m-%dT%H:%M:%E*S%:z"),jsonPayload.MESSAGE)'
```

Local parse helper: [`parse_pipeline_report.py`](./parse_pipeline_report.py).

### (b) Per-partition lag + stuck partitions

- **INFO** `Unsettled offsets (pN: pending=… position=…, …)` — consumer name `sensor_data` in structured field `consumer` (may appear before `MESSAGE` in JSON).
- **WARN** `Commit position has not advanced since the last report` — stalled partitions (`offset_ledger.rs`).

```bash
gcloud logging read \
'resource.type="gce_instance"
 resource.labels.project_id="chronoracer"
 resource.labels.instance_id="'"${CORE_ID}"'"
 jsonPayload._SYSTEMD_UNIT="chronoracer-core.service"
 (jsonPayload.MESSAGE=~"Unsettled offsets" OR jsonPayload.MESSAGE=~"Commit position has not advanced")
 timestamp>="<START>" AND timestamp<="<END>"' \
  --project=chronoracer --limit=10000 --order=asc \
  --format='table(timestamp.date("%Y-%m-%dT%H:%M:%E*S%:z"),jsonPayload.MESSAGE)'
```

### (c) Core errors and warnings

```bash
gcloud logging read \
'resource.type="gce_instance"
 resource.labels.project_id="chronoracer"
 resource.labels.instance_id="'"${CORE_ID}"'"
 jsonPayload._SYSTEMD_UNIT="chronoracer-core.service"
 severity>=WARNING
 timestamp>="<START>" AND timestamp<="<END>"' \
  --project=chronoracer --limit=10000 --order=asc \
  --format='table(timestamp.date("%Y-%m-%dT%H:%M:%E*S%:z"),severity,jsonPayload.MESSAGE)'
```

Include **ERROR** ingest/DLQ/Kafka lines at INFO if needed: add `OR jsonPayload.MESSAGE=~"Kafka error"` / DLQ patterns below.

### (d) InfluxDB logs (`influxdb3.service`)

```bash
gcloud logging read \
'resource.type="gce_instance"
 resource.labels.project_id="chronoracer"
 resource.labels.instance_id="'"${CORE_ID}"'"
 jsonPayload._SYSTEMD_UNIT="influxdb3.service"
 timestamp>="<START>" AND timestamp<="<END>"' \
  --project=chronoracer --limit=10000 --order=asc \
  --format='table(timestamp.date("%Y-%m-%dT%H:%M:%E*S%:z"),severity,jsonPayload.MESSAGE)'
```

### (e) DLQ handler aggregated lines (10 s window)

Messages ([`dlq_handler.rs`](../core/src/messaging/dlq_handler.rs)):

- `Redirected messages to DLQ`
- `Failed to write to DLQ (first failure of the window logged above)`
- `DLQ queue full: discarded invalid messages without a DLQ record`

```bash
gcloud logging read \
'resource.type="gce_instance"
 resource.labels.project_id="chronoracer"
 resource.labels.instance_id="'"${CORE_ID}"'"
 jsonPayload._SYSTEMD_UNIT="chronoracer-core.service"
 (jsonPayload.MESSAGE=~"Redirected messages to DLQ"
  OR jsonPayload.MESSAGE=~"Failed to write to DLQ"
  OR jsonPayload.MESSAGE=~"DLQ queue full")
 timestamp>="<START>" AND timestamp<="<END>"' \
  --project=chronoracer --limit=10000 --order=asc \
  --format='table(timestamp.date("%Y-%m-%dT%H:%M:%E*S%:z"),jsonPayload.MESSAGE)'
```

### Journal fallback (Core VM)

Units: [`chronoracer-core.service`](../core/startup/chronoracer-core.service), [`influxdb3.service`](../core/startup/influxdb3.service). Kafka: [`kafka.service`](../kafka/startup/kafka.service).

```bash
gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap -- \
  sudo journalctl -u chronoracer-core --since '<START>' --until '<END>' --no-pager -o short-iso

gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap -- \
  sudo journalctl -u influxdb3 --since '<START>' --until '<END>' --no-pager -o short-iso
```

`journalctl` accepts systemd time specs; for strict RFC3339 use quoted ISO strings as above.

---

## 3. Metrics

Ops Agent on Core/Kafka: **host** metrics only (60 s interval); process scraper excluded ([`ops-agent.yaml`](../core/startup/ops-agent.yaml)). Dashboard uses Compute CPU + Agent memory/disk/network ([`race-fleet.json`](../dashboard/dashboard_files/v2/race-fleet.json)).

| Area | Metric type | Notes |
|------|-------------|--------|
| Core CPU | `compute.googleapis.com/instance/cpu/utilization` | Native Compute; filter `instance_id` for race |
| Core memory | `agent.googleapis.com/memory/percent_used` | `metric.label.state="used"` for panels |
| Kafka CPU / memory / disk / net | same families + `agent.googleapis.com/disk/percent_used`, `agent.googleapis.com/interface/traffic` | |
| Influx writes | `custom.googleapis.com/chronoracer/influx_write_lines`, `influx_write_bytes`, `influx_write_requests` | Cumulative on `gce_instance`; `outcome` label on requests |
| Telemetry | `run.googleapis.com/container/instance_count`, `request_count`, `request_latencies` | Service name `telemetry` ([`names.json`](../infra/contract/names.json)) |

Core publishes write counters every **60 s** while running ([`component-core.md`](../docs/component-core.md#cloud-monitoring-write-counters)) — coarser than 10 s pipeline reports.

### Script export

```bash
pip install google-cloud-monitoring
python3 load-test/collect_metrics.py \
  --race-id <RACE_ID> \
  --start '<START>' \
  --end '<END>' \
  --output-dir load-test/metrics-out
```

Writes one CSV per metric family under `--output-dir` plus `meta.json` with resolved `instance_id`s.

### Persisted point count (loss check) {#persisted-point-count-loss-check}

**Primary:** compare generator totals with Core `WriteMetricSnapshot` / accepted write-line counters in `core.log` and
pipeline `written_lines_s` (no upward `unconsumed` trend). **Secondary:** [`influx_count.sh`](influx_count.sh) piped
over IAP SSH for the run window (`run-metadata.json` start/end, pad ±1 s). It runs **one `count(*)` per sensor table**
(safe on large DBs). A single UNION across all tables can hang the Core VM at ~100M+ points.

```bash
gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap \
  --command "sudo bash -s -- <START> <END>" < load-test/influx_count.sh \
  > load-test/runs/<ID>/artifacts/influx-count.csv
```

### Metrics Explorer (MQL examples)

Core CPU (replace instance id):

```text
fetch gce_instance
| metric 'compute.googleapis.com/instance/cpu/utilization'
| filter (resource.instance_id == '<CORE_NUMERIC_INSTANCE_ID>')
| group_by 1m, [value_utilization_mean: mean(value.utilization)]
```

Influx accepted lines **rate** (cumulative counter):

```text
fetch gce_instance
| metric 'custom.googleapis.com/chronoracer/influx_write_lines'
| filter (resource.instance_id == '<CORE_NUMERIC_INSTANCE_ID>')
| align rate(1m)
| every 1m
```

Telemetry request rate:

```text
fetch cloud_run_revision
| metric 'run.googleapis.com/request_count'
| filter (resource.service_name == 'telemetry')
| align rate(1m)
| every 1m
```

### `gcloud monitoring` (alpha)

If installed:

```bash
gcloud monitoring time-series list \
  --project=chronoracer \
  --filter='metric.type="compute.googleapis.com/instance/cpu/utilization" AND resource.labels.instance_id="<CORE_ID>"' \
  --start-time='<START>' --end-time='<END>'
```

Prefer `collect_metrics.py` for bulk CSV; alpha CLI output is awkward for many series.

---

## 4. Kafka VM checks

**Broker:** KRaft single node, `/opt/kafka`, systemd [`kafka.service`](../kafka/startup/kafka.service). **Listeners:** PRIVATE `0.0.0.0:9092` (plaintext), PUBLIC `9094` (SASL_SSL). **No JMX** in repo startup/properties — broker health via logs, consumer groups, and host metrics only.

**Bootstrap on VM (private listener):** `127.0.0.1:9092` ([`create-topics.sh`](../kafka/startup/create-topics.sh)).

**Core consumer group** ([`component-kafka.md`](../docs/component-kafka.md#consumer-groups)): prefix `kafka.group_id` default `chrono-core` → data group **`chrono-core-data`**, control **`chrono-core-control`**.

**Topic `data`:** name from `platform.json` → `data`, 4 partitions (Core creates at runtime).

SSH via IAP (no public IP on Core; Kafka has ephemeral public IP but IAP is consistent):

```bash
gcloud compute ssh kafka --project=chronoracer --zone=europe-west1-b --tunnel-through-iap
```

On the Kafka VM:

```bash
# Consumer group lag (Core data path)
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --describe --group chrono-core-data

# Topic layout
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --describe --topic data

# Host snapshot
top -b -n 1 | head -20
vmstat 1 5
```

Optional **public** SSH (only if IAP blocked): use Kafka's ephemeral IP from `gcloud compute instances describe kafka --format='get(networkInterfaces[0].accessConfigs[0].natIP)'` with the same `kafka` user/OS login.

---

## 5. Gaps and doc notes

| Gap | Detail |
|-----|--------|
| Pipeline vs metrics cadence | Pipeline report every **10 s**; Cloud Monitoring write counters every **60 s** — use logs for sub-minute Influx diagnosis. |
| Memory metric source | Dashboard mixes **Compute CPU** with **Ops Agent** memory; Agent must be running (`google-cloud-ops-agent`). |
| No Kafka JMX | No broker JVM metrics in Monitoring; use consumer-group lag + VM CPU/disk/net. |
| VM name reuse | Logging/Monitoring must filter **`instance_id`** or **`labels.race-id`**, not hostname alone. |
| Ops Agent log fields | Rely on `jsonPayload.MESSAGE`; structured tracing fields may also appear as separate JSON keys. |
| `unsettled` quiet periods | `No unsettled offsets` is **DEBUG** — usually absent in Cloud Logging. |

**Discrepancies spotted:** [`core/startup/README.md`](../core/startup/README.md) references `enable-ops-agent.sh` under `/chronoracer` (image copy of packer script) — behavior matches packer source. [`component-core.md`](../docs/component-core.md) says platform sets topic names only; partition counts remain from defaults/operator (not platform) — consistent with `render-config.sh`. Relay docs require `race.initial_driver_id` injection on start; manual GCS upload must set it explicitly (this overlay).
