# Reason for not using InfluxDB Kafka plugin

InfluxDB offers a Kafka plugin that would basically after being configured consume messages and automatically write them to InfluxDB. This is more or less what a part of the Core would be doing. So the question is, why are we not using it?

1. Vendor lock-in
	If we use the plugin, we are locked to InfluxDB. If we write it ourselves, we can swap the database with changes restricted to one layer only.

2. Our own consumers are needed anyway
	Since we will need at least part of the data for the analysis, the need for our own set of consumers exists. If so, then the benefits of the Kafka plugin are non-existent and we actually need to listen for messages twice. 