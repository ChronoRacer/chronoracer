# Local docker deployments

# InfluxDB

[Images here](influx_db)

Run for InfluxDB in local docker, use access token in [token file](influx_db/admin-token.json)

The Compose stack creates the plugin venv with InfluxDB's bundled Python, installs `pyarrow` before starting InfluxDB,
and starts with package management disabled. This mirrors the production setup documented in the
[InfluxDB component](../../../docs/component-influxdb.md#plugin-python-environment).

## Kafka

[Images here](kafka)

Run for Kafka in local docker. Management UI at `localhost:8080`.
