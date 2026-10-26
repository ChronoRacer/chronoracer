# Deep dive - InfluxDB

IMPORTANT: **Running InfluxDB Core, version 3**

Where / lifecycle / shared-VM tradeoff: [Core](component-core.md). Access: private, VPC-internal. No public NIC.

Responsibility:

- store sensor measurements for the race
- serve queries from Core on the same VM
- serve export queries (Arrow Flight) to Core: [InfluxDB export](component-core.md#influxdb-export)

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

Boot starts InfluxDB 3 Core before the Core process: [Core startup](../core/startup/README.md). Image:
[Compute images](component-images.md). No `--plugin-dir`, so no plugins are loaded.

## Memory budget

[`influxdb3.service`](../core/startup/influxdb3.service) runs InfluxDB in its own cgroup: `MemoryHigh=70%`,
`MemoryMax=80%` of RAM. Under pressure the kernel throttles and then OOM-kills only InfluxDB; `Restart=on-failure`
restarts it and replays the WAL while Core keeps its batches and Kafka buffers. Core has `OOMScoreAdjust=-900`.
Inside the limit: write buffer `--force-snapshot-mem-size 15%`, `--exec-mem-pool-size 20%`, `--file-cache-size 10%`
(Core never queries at runtime), `--force-snapshot-max-age 10m`. `--wal-flush-interval 100ms` shortens write
acknowledgement. `--snapshot-concurrency-limit` is the vCPU count minus one (at least 1), derived at boot by
[`start-influx.sh`](../core/startup/start-influx.sh): with the default of all vCPUs, every forced snapshot starved Core
and ingest for ~20 s. The 15% buffer halves the forced-snapshot cycle and the anonymous memory peak, at the cost of
more overlapping Parquet files, which the export's row budget absorbs. Load-tested at 800K points/s (setting L800d):
[Core 0.3.2 campaign](load-test-results/core-0.3.2/runs/campaign.md#periodic-spikes-at-800k-influxdb-forced-snapshots).
Export queries run inside the same limits, one bounded window at a time:
[InfluxDB export](component-core.md#influxdb-export).

## Tables

Schema: `iox` (InfluxDB default)

One table per sensor, created lazily on first write. Table name = sensor id.

Line protocol Core writes (formatted by `LineWriter` in the batcher, one line per reading):

```
{sensorId},race={raceId},driver={driverId} val={value} {timestampNanos}
```

`value` is always written as a float (shortest round-trip form with a decimal point or exponent, for example `3.0`, `12.5`, `1e21`).

| Column   | Kind      | Meaning                                                    |
|----------|-----------|------------------------------------------------------------|
| `race`   | tag       | race id (constant for one race; used after joining tables) |
| `driver` | tag       | driver id (may change mid-race via `CHDRV`)                |
| `val`    | field     | measurement                                                |
| `time`   | timestamp | nanosecond Unix time                                       |
