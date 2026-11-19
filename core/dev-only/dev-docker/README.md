# Local docker deployments

# InfluxDB

[Images here](influx_db)

Run for InfluxDB in local docker, use access token in [token file](influx_db/admin-token.json)

Core writes the database export itself, so `persistence.export.vm_export_dir` must be writable by the local Core
process: [InfluxDB export](../../../docs/component-core.md#influxdb-export).

## Kafka

[Images here](kafka)

Run for Kafka in local docker. Management UI at `localhost:8080`.
