# Influx DB plugin directory

Plugins inside this directory will be loaded into the InfluxDB instance during a race.

Race image does not bake these files. Publish path: [Compute images](../../docs/component-images.md). Object
prefix: [GCS](../../docs/component-gcs_buckets.md).

## `export_database.py`

Caller: Core When: Core shutdown sequence Purpose: export tables from InfluxDB, compress into Parquet files, ensuring
all WAL is flushed Why: avoid additional traffic between Core and database

## TODO Warning:

This plugin is currently executed inside an HTTP call and may timeout. If the data volume is too large for quick
execution, it must be redesigned.

When load testing on a local machine with about 3 million rows in total, export from InfluxDB to parquet files on disk
took less than 1 second (really, really fast). So unless strictly needed in the future, execution over HTTP can stay for
now.