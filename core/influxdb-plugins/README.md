# Influx DB plugin directory

Plugins inside this directory will be loaded into the InfluxDB instance during a race.

## `export_database.py`

Caller: Core
When: Core shutdown sequence
Purpose: export tables from InfluxDB, compress into Parquet files, ensuring all WAL is flushed
Why: avoid additional traffic between Core and database

TODO Warning: this plugin is currently executed inside an HTTP call and may timeout. If data volume is too large for quick execution, it must be redesigned.