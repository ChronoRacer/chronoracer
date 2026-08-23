# Part 7. Kafka

## Sensors

One topics for all sensors with multiple partitions. 

Topic name: `data`
Partition count: `TODO`
Message key: `sensor_id` (reference `ID` column in `sensors.xlsx`)
Delivery semantics: `exactly-once`
Compression: `yes`, type: `TODO`
Avro enabled: `yes`

### Avro Message schema:

To reduce bloat, sensor data can be batched as long as the following perquisite is met:

Perquisite:  immediate warnings are evaluated on the Car Computer. This means that slight delays in transmission only mean that dashboard data is updated a bit slower. Analysis also demands data from an extended period of time so batching at message is acceptable. Of course, the batch size can be reduced depending on the read frequency.

Chosen batching frequency: 100 ms per batch (`TODO` adjust based on load and feed needs)

Message schema:
```avro
{
  "namespace": "com.chronoracer.sensor",
  "type": "record",
  "name": "SensorBatch",
  "doc": "Batch of sensor readings",
  "fields": [
    {
      "name": "sensor_id",
      "type": "string",
      "doc": "Sensor ID"
    },
    {
        "name": "timestamp",
        "type": {
          "type": "long",
          "logicalType": "timestamp-nanos"
        }
        "doc": "Timestamp of the first read in the batch"
    },
    {
      "name": "readings",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "SensorReading",
          "fields": [
            {
              "name": "delta",
              "type":  "int",
              "doc": "Nanosecond offset from the first_read_timestamp. Must be at most 2 seconds (2000000000 ns)."
            },
            {
              "name": "value",
              "type": "double",
              "doc": "Reading value"
            }
          ]
        }
      },
      "default": [],
      "doc": "An array of readings collected during the batch window."
    }
  ]
}
```

*Including `sensor_id` in message*: It is not required. The `sensor_id` can be derived from the message key in the topic. However, if the message is dumped or processed in any other way, unlikely but still, without the message key, it is an orphaned data and it is impossible to determine where it came from if it is a senor like front left brake temperature. So including it is not without merit. If we have real problems with throughput during parsing (`TODO` verify flame graphs during benchmarks) then we can remove it.

*Reading record arrays*: There are three methods of storing the data (most info taken from the docs https://avro.apache.org/docs/1.11.1/specification/):
1. Store as an array of records with fields `value` and `delta`.
	Seems the most logical and clean honestly.
2. Store as two parallel arrays of same length. First array is values, second deltas.
    This is actually worse for short arrays (like 2 values for example). Avro sets array markers which are of 4 bytes.
3. Store as one array. Even indices are `value`, odd `delta`. Each reading is `i` and `i+1`.
	This option is bad. We are losing bytes because of the type differences. Avro int is 4 bytes, custom type `i16type` is 2 bytes, but using it does not matter really as Avro will encode it with ZigZag to smaller size if it is smaller. Double is 8 bytes. If we store it in one array, we are losing at the very least half of the space and need to deal with the indices.


## Control data

One topic for control data.

Topic name: `control`
Partition count: `TODO`
Message key: `event` (even name)
Delivery semantics: `exactly-once`
Message order: `does not matter` - control messages are sent rarely enough for this not to matter
Compression: `yes`, type: `TODO`
Avro enabled: `no` - too violative schema depending on message key

#### Driver change Control message

Message sent when the driver changes during the race. Driver ID changed from the moment of message acknowledgement. Slight dicrepancy during swap is accepted.
Message key: `CHDRV`
Message schema: none, just new driver Id in the body. Core trusts as long as valid UFT-8 string.

#### Shutdown Control message

Message sent when the shutdown process starts. Upon receiving, Core should drain Kafka and start InfluxDB and Kafka exporting process.
Message key: `SHUTDOWN`
Message schema: none


## DLQ

One topic for invalid messages.

Topic name: `dlq`
Partition count: `TODO`
Message key: original message key
Delivery semantics: `does not matter`
Message order: `does not matter`
Headers: original message partition and offset
Compression: `yes`, type: `TODO`
Avro enabled: `no` - original messages either compressed bytes already or not worth the compression.

Message body is the body of the original message.

## Heartbeat

One topic for heartbeats messages.

Topic name: `heartbeat`
Partition count: `1`
Message key: `CORE` - for Core heartbeat
Delivery semantics: `exactly-once`
Message order: ensured as 1 partition
Compression: `no`
Avro enabled: `no`

Message body may contain additional status information.
