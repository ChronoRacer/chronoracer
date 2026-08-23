# Deep dive - InfluxDB

Where: one paid VM. Core and InfluxDB 3 Core (self-managed, single node) share that VM.
Shared VM pain points: Core and database share similar load spikes which means that VM usage can spike greatly.

Lifecycle: race-scoped. Created at the start. After export succeeds, VM and disk are destroyed.

Access: private

Responsibility: 
- ingest data from Kafka
- process, validate data
- batch and write to database
- serve as a buffer for database
- handle shutdown sequence export flow (for both database and Kafka)

## Tables

Schema: `iox` (InfluxDB default)

### Sensor data tables:

One table for each sensor during the race. Tables are created during runtime, lazily on first write.

Each sensor data table has the exact same columns:
- `race` - race during which the measurement was taken (constant in the scope of one race, used after joining all data)
- `driver` - ID of the driver to which the measurement belongs (may change during the race)
- `val` - measurement value
- `{timeColumnName}` - timestamp of measurement with nanosecond accuracy

## Plugins

### Reason for not using InfluxDB Kafka plugin

InfluxDB offers a Kafka plugin that would, with proper configuration, consume messages and automatically write them to InfluxDB.
This is more or less what a part of the Core would be doing. This plugin is mainly not used because:

1. Vendor lock-in: 
	If we use the plugin, we are locked to InfluxDB. If we write it ourselves, we can swap the database with changes restricted to one layer only.

2. Our own consumers are needed anyway: 
	Since we will need at least part of the data for the analysis, the need for our own set of consumers exists. If so, then the benefits of the Kafka plugin are non-existent and we actually need to listen for messages twice.

3. Control flow: 
    Handling internal control flow custom logic is not possible with the plugin, especially runtime configuration changes


### Custom plugins

README: [plugin directory README](../core/influxdb-plugins/README.md)

| Plugin          | Code at                                                           |
|-----------------|-------------------------------------------------------------------|
| Export database | [export_database.py](../core/influxdb-plugins/export_database.py) |
