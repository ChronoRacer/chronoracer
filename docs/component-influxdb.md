# Deep dive - InfluxDB

IMPORTANT: **Running InfluxDB Core, version 3**

Where / lifecycle / shared-VM tradeoff: [Core](component-core.md). Access: private, VPC-internal. No public NIC.

Responsibility:

- store sensor measurements for the race
- serve queries from Core on the same VM
- serve read queries from the [Dashboard](component-dashboard.md) over the VPC
- dump tables on export (plugin) so Core can upload to GCS

Core owns Kafka consume and control flow. The Influx Kafka plugin is unused.

## Access model

The HTTP listener binds all interfaces on the Core VM, not only localhost, so the Dashboard can query it over Direct
VPC Egress. Reachability is cut down by firewall instead: only the Dashboard Cloud Run subnet may reach the Influx HTTP
port on tag `core`, which excludes the Kafka VM and Relay. Subnet and firewall
resource: [Terraform](component-terraform.md).

Port and database name live in [`platform.json`](../infra/contract/platform.json), because the firewall rule, the
Dashboard datasource, the systemd bind, and Core's own client all need the same values. The
[`core/default-config.yaml`](../core/default-config.yaml) entries are local-development defaults that the boot-time
platform overlay wins over, the same way the `localhost:9092` broker works ([Core](component-core.md)).

Authentication uses one shared admin token from Secret Manager, the same value for Core and the Dashboard. Core boot
renders Influx's JSON token file from it: [Core startup](../core/startup/README.md). Creation and rotation
window: [Influx token runbook](operator-influx-token.md). Rationale for not generating it on the
VM: [ADR - Dashboard](adr-dashboard.md).

InfluxDB 3 Core supports admin tokens only; read-scoped tokens are an Enterprise feature. Every token holder can write
and delete. Dashboard-side mitigation: [Dashboard](component-dashboard.md).

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
