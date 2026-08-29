# Deep dive - Kafka

Where: one VM. Single node, not cluster.

Lifecycle: race-scoped. Created at the start. Destroyed after Core exports Influx and selected topics.

Access: both public and private (two listeners)

- Public: ingest from track devices. TLS terminated on Kafka. Okta OAuth required (client id and client secret on each
  device). Firewall opens the Kafka port only. No VPN. No IP allowlist.
- Private: Core and Relay. VPC firewall only. No Okta on this listener. If Okta is down, devices cannot write, but Core
  can still drain the broker into Influx.

Boot contract: broker does not start unless the public TLS listener is bound with the Secret Manager cert. Private-only
Kafka is not a running Kafka. Script sequence: [Kafka startup](../kafka/startup/README.md). Image
pipeline: [Compute images](component-images.md).

Responsibility:

- serve as data sink
- handle internal control events
- provide buffer for Core

## Listeners

KRaft combined broker+controller, single node. Ports and OAuth URLs: [`platform.json`](../infra/contract/platform.json). 
Apache Kafka at `/opt/kafka` on family `chronoracer-kafka`.

| Name       | Bind             | Advertised                                 | Protocol                                                                                                                                                                                       |
|------------|------------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PRIVATE    | `0.0.0.0:9092`   | `{kafka}.{zone}.c.{project}.internal:9092` | PLAINTEXT (Core, Relay). No Okta.                                                                                                                                                              |
| PUBLIC     | `0.0.0.0:9094`   | `{kafka-fqdn}:9094`                        | SASL_SSL. TLS from the PKCS12 keystore. SASL/OAUTHBEARER validated with Apache `OAuthBearerValidatorCallbackHandler` against Auth0 JWKS (`kafka_oauth_*` in platform.json). No ACL authorizer. |
| CONTROLLER | `127.0.0.1:9093` | not advertised                             | PLAINTEXT. Inter-broker uses PRIVATE.                                                                                                                                                          |

## Public hostname and cert

Zone, FQDN, A record, secret **resource**: [Terraform](component-terraform.md). Issue and upload
PEM: [Kafka TLS runbook](operator-kafka-tls.md).

When ready, Relay returns hostname, port, and the applied public IP. Track clients may connect to the IP with SNI/verify
on the hostname so DNS lag is not the critical path. Do not use that address unless status says ready
([Race provisioning](flow-race_provisioning.md)).

Topics are created by Core on the private listener, not at Kafka boot.

## Topics

All message keys are expected to be uppercase. Compact keys prefered.

### Sensors

One topic for all sensors with multiple partitions.

- Topic name: `data`
- Partition count: `4`
- Message key: `{sensorId}` (reference `ID` column in `sensors.xlsx` document)
- Delivery semantics: `exactly-once`
- Compression: `yes`, type: `lz4` (**compression enforced by producer, not topic**)
- Avro enabled: `yes`

#### Avro Message schema:

To reduce bloat, sensor data can be batched as long as the following perquisite is met:

Perquisite: immediate warnings are evaluated on the Car Computer. This means that slight delays in transmission only
mean that dashboard data is updated a bit slower. Analysis also demands data from an extended period of time so batching
at message is acceptable. Of course, the batch size can be reduced depending on the read frequency.

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

*Including `sensor_id` in message*: It is not required. The `sensor_id` can be derived from the message key in the
topic. However, if the message is dumped or processed in any other way, unlikely but still, without the message key, it
is an orphaned data and it is impossible to determine where it came from if it is a senor like front left brake
temperature. So including it is not without merit. If we have real problems with throughput during parsing (`TODO`
verify flame graphs during benchmarks) then we can remove it.

*Reading record arrays*: There are three methods of storing the data (most info taken from the
docs https://avro.apache.org/docs/1.11.1/specification/):

1. Store as an array of records with fields `value` and `delta`. Seems the most logical and clean honestly.
2. Store as two parallel arrays of same length. First array is values, second deltas. This is actually worse for short
   arrays (like 2 values for example). Avro sets array markers which are of 4 bytes.
3. Store as one array. Even indices are `value`, odd `delta`. Each reading is `i` and `i+1`. This option is bad. We are
   losing bytes because of the type differences. Avro int is 4 bytes, custom type `i16type` is 2 bytes, but using it
   does not matter really as Avro will encode it with ZigZag to smaller size if it is smaller. Double is 8 bytes. If we
   store it in one array, we are losing at the very least half of the space and need to deal with the indices.

### Control data

One topic for control data.

- Topic name: `control`
- Partition count: `1` (low traffic)
- Message key: `{eventName}`
- Delivery semantics: `exactly-once`
- Message order: `ensured as 1 partition`
- Compression: `yes`, type: `l4z`  (**compression enforced by producer, not topic**)
- Avro enabled: `no` - too violative schema depending on message key

##### Driver change Control message

Message sent when the driver changes during the race. Driver ID changed from the moment of message acknowledgement.
Slight dicrepancy during swap is accepted.

- Message key: `CHDRV`
- Message schema: none, just new driver ID in the body. Core trusts as long as valid UFT-8 string.

##### Shutdown Control message

Message sent when the shutdown process starts. Upon receiving, Core should drain Kafka and start the export process.

- Message key: `SHUTDOWN`
- Message schema: none

### DLQ

One topic for invalid messages.

- Topic name: `dlq`
- Partition count: `3`
- Message key: original message key
- Delivery semantics: `does not matter`
- Message order: `does not matter`
- Headers: original message partition and offset
- Compression: `yes`, type: `snappy`  (**compression enforced by producer, not topic**)
- Avro enabled: `no` - original messages either compressed bytes already or not worth the compression.

Message body is the body of the original message.

### Heartbeat

One topic for all heartbeat messages.

- Topic name: `heartbeat`
- Partition count: `1` (low traffic)
- Message keys: `CORE` - for Core heartbeat
- Delivery semantics: `does not matter`
- Message order: ensured as 1 partition
- Compression: `yes`, type: `snappy`  (**compression enforced by producer, not topic**)
- Avro enabled: `no`

Message body may contain additional status information.
