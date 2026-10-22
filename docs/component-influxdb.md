# Deep dive - InfluxDB

IMPORTANT: **Running InfluxDB Core, version 3**

Where / lifecycle / shared-VM tradeoff: [Core](component-core.md). Access: private, VPC-internal. No public NIC.

Responsibility:

- store sensor measurements for the race
- serve queries from Core on the same VM
- dump tables on export (plugin) so Core can upload to GCS

Core owns Kafka consume and control flow. The Influx Kafka plugin is unused.

## Access model

The HTTP listener binds localhost only. Core writes and queries locally. No Cloud Run service or other VM can reach
Influx directly. [Dashboard](component-dashboard.md) reads Core's write counters from Cloud Monitoring instead of
querying Influx: [Core](component-core.md#cloud-monitoring-write-counters).

Port and database name live in [`platform.json`](../infra/contract/platform.json), because the systemd bind and Core's
own client need the same values. The
[`core/default-config.yaml`](../core/default-config.yaml) entries are local-development defaults that the boot-time
platform overlay wins over, the same way the `localhost:9092` broker works ([Core](component-core.md)).

Authentication uses one admin token from Secret Manager. Core is its only consumer and boot renders Influx's JSON
token file from it: [Core startup](../core/startup/README.md). Creation and rotation:
[Influx token runbook](operator-influx-token.md).

InfluxDB 3 Core supports admin tokens only; read-scoped tokens are an Enterprise feature. Localhost binding and
Core-only secret access keep that privilege off browser-facing services.

## Operational metrics

Dashboard ingest signals are Core write counters, not this process: [Core](component-core.md#cloud-monitoring-write-counters).

## Startup

Boot starts InfluxDB 3 Core before the Core process: [Core startup](../core/startup/README.md). Image + plugin
payload: [Compute images](component-images.md).

## Memory budget

[`influxdb3.service`](../core/startup/influxdb3.service) runs InfluxDB in its own cgroup: `MemoryHigh=70%`,
`MemoryMax=80%` of RAM. Under pressure the kernel throttles and then OOM-kills only InfluxDB; `Restart=on-failure`
restarts it and replays the WAL while Core keeps its batches and Kafka buffers. Core has `OOMScoreAdjust=-900`.
Inside the limit: write buffer `--force-snapshot-mem-size 30%`, `--exec-mem-pool-size 20%`, `--file-cache-size 10%`
(Core never queries at runtime), `--force-snapshot-max-age 10m`. `--wal-flush-interval 100ms` shortens write
acknowledgement. Values are starting points pending the L150 plus full-count-query verification run
([proposal](proposals/runtime-performance.md)).

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
