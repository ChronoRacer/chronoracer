# Deep dive - InfluxDB

IMPORTANT: **Running InfluxDB Core, version 3**

Where / lifecycle / shared-VM tradeoff: [Core](component-core.md). Access: private, VPC-internal. No public NIC.

Responsibility:

- store sensor measurements for the race
- serve queries from Core on the same VM
- expose local Prometheus-compatible server metrics for Ops Agent
- dump tables on export (plugin) so Core can upload to GCS

Core owns Kafka consume and control flow. The Influx Kafka plugin is unused.

## Access model

The HTTP listener binds localhost only. Core writes and queries locally; Ops Agent on the same VM scrapes `/metrics`.
No Cloud Run service or other VM can reach Influx directly. [Dashboard](component-dashboard.md) reads the resulting
metrics from Cloud Monitoring instead of querying Influx.

Port and database name live in [`platform.json`](../infra/contract/platform.json), because the systemd bind, Ops Agent
scrape config, and Core's own client all need the same values. The
[`core/default-config.yaml`](../core/default-config.yaml) entries are local-development defaults that the boot-time
platform overlay wins over, the same way the `localhost:9092` broker works ([Core](component-core.md)).

Authentication uses one admin token from Secret Manager. Core is its only consumer and boot renders Influx's JSON
token file from it: [Core startup](../core/startup/README.md). Creation and rotation:
[Influx token runbook](operator-influx-token.md).

InfluxDB 3 Core supports admin tokens only; read-scoped tokens are an Enterprise feature. Localhost binding and
Core-only secret access keep that privilege off browser-facing services.

## Operational metrics

InfluxDB 3 Core exposes Prometheus-compatible metrics on unauthenticated `GET /metrics` on the same localhost listener.
Ops Agent's Prometheus receiver scrapes them and sends them to Cloud Monitoring. Required Dashboard signals are ingest
lines/bytes counters, HTTP write outcomes, and available server errors. Exact names and labels must be verified against
the pinned InfluxDB version before a Dashboard image is accepted; per-table labels are not assumed.

## Startup

Boot starts InfluxDB 3 Core before the Core process: [Core startup](../core/startup/README.md). Image + plugin
payload: [Compute images](component-images.md).

## Plugin Python environment

Core VM has no internet access. The Core image build installs all required plugin python packages before
the image is published. It uses the Python bundled with the installed InfluxDB package to create
`/chronoracer/influxdb-plugin-venv` and installs the package into that venv with its `pip`.

The InfluxDB systemd unit sets `VIRTUAL_ENV` to this venv and disables InfluxDB bundled package manager. The venv is
kept outside `/chronoracer/influxdb-plugins` because boot fetch replaces that directory with the current plugin
payload. This makes the preinstalled dependency available to every fetched plugin without any boot-time package
installation.

## Tables

Schema: `iox` (InfluxDB default)

One table per sensor, created lazily on first write. Table name = sensor id.

Line protocol Core writes (`SimplePoint`):

```
{sensorId},race={raceId},driver={driverId} val={value} {timestampNanos}
```

| Column   | Kind      | Meaning                                                    |
|----------|-----------|------------------------------------------------------------|
| `race`   | tag       | race id (constant for one race; used after joining tables) |
| `driver` | tag       | driver id (may change mid-race via `CHDRV`)                |
| `val`    | field     | measurement                                                |
| `time`   | timestamp | nanosecond Unix time                                       |

## Plugins

Runtime files: [plugin directory](../core/influxdb-plugins/README.md). Payload layout: [GCS](component-gcs_buckets.md).

| Plugin          | Code at                                                           |
|-----------------|-------------------------------------------------------------------|
| Export database | [export_database.py](../core/influxdb-plugins/export_database.py) |
