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
PEM: [Kafka TLS runbook](operator-domain-and-tls.md).

When ready, Relay returns hostname, port, and the applied public IP. Track clients may connect to the IP with SNI/verify
on the hostname so DNS lag is not the critical path. Do not use that address unless status says ready
([Race provisioning](flow-race_provisioning.md)).

## Topics

Core creates `data`, `control`, `heartbeat`, and `dlq` on the private listener at runtime. Kafka boot creates `event`
([create-topics.sh](../kafka/startup/create-topics.sh)) after the public listener binds. Core does not create, consume,
or export `event`.

| Topic       | Name owner                                                                                         | Created by | Partitions | Replication |
|-------------|----------------------------------------------------------------------------------------------------|------------|------------|-------------|
| `data`      | [`platform.json`](../infra/contract/platform.json) `kafka_data_topic`                              | Core       | 4          | 1           |
| `control`   | [`platform.json`](../infra/contract/platform.json) `kafka_control_topic`                           | Core       | 1          | 1           |
| `heartbeat` | [`platform.json`](../infra/contract/platform.json) `kafka_heartbeat_topic`                         | Core       | 1          | 1           |
| `event`     | [`platform.json`](../infra/contract/platform.json) `kafka_event_topic`                             | Kafka boot | 1          | 1           |
| `dlq`       | [`core/default-config.yaml`](../core/default-config.yaml) (not in `platform.json`)                 | Core       | 3          | 1           |

Partition counts for Core-created topics live with Core config. Device and control produce wait until Core has created
those topics. `event` is available as soon as Kafka boot finishes.

Shared rules:

- Keys are uppercase. Compact keys preferred.
- Compression is a producer setting, not a topic setting. Core and Relay produce `snappy`. Device producers may use
  `lz4` (when using a different compression type than `snappy` or `lz4` ensure Core binary supports the codec).
- Delivery is at-least-once. No Kafka transactions. Core's commit rules: [Core delivery](component-core.md#kafka-delivery).

### Consumer groups

`kafka.group_id` in [`core/default-config.yaml`](../core/default-config.yaml) is a prefix. Core consumes `data` as
`<group_id>-data` and `control` as `<group_id>-control`, so the two positions are independent. Redpanda Connect exports
with its own group, `<race_id>-export`, and never shares one with Core.

A group with no committed offset starts at the earliest offset (`auto.offset.reset=earliest`). Core creates the topics
before anything is produced, so a new group reads everything produced before it subscribed. Committed offsets survive
a restart for the broker's offsets retention (1440 minutes by default, [`kafka/`](../kafka)). Changing `group_id`
replays both topics from the start.

### `data`

- Key: `{sensorId}` (`ID` column in `sensors.xlsx`). At most 12 ASCII characters.
- Avro: yes. Canonical schema: [`core/avro/sensor_data.avsc`](../core/avro/sensor_data.avsc)

Car computer evaluates immediate warnings, so a short send delay only slows the Telemetry preview. Analysis wants a window, so
the producer may batch readings in one message. Each `delta` is nanoseconds from `timestamp` and must be at most 2
seconds (`2000000000`). That cap is a producer rule: Core rejects a negative `delta` and does not check the 2 second
limit ([Core](component-core.md#data-pipeline-batch-contract)).

`sensor_id` is in the body as well as the key so a dump without the Kafka key still identifies the sensor.

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
      },
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
              "type": "int",
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

### `control`

- Key: `{eventName}`
- Avro: no. Payload shape depends on the key.

| Key        | Body                                      | Effect                                                                                          |
|------------|-------------------------------------------|-------------------------------------------------------------------------------------------------|
| `CHDRV`    | UTF-8 driver ID, no Avro                  | Driver ID changes from acknowledgement. Small swap discrepancy is accepted. Invalid ids go to `dlq` and leave the current driver unchanged: [Core](component-core.md#control-event-handling). |
| `SHUTDOWN` | empty                                     | Core drains Kafka and starts export. Relay publishes this after `close_ingest` succeeds.        |

Unknown keys are ignored. Parse failures (missing key, missing `CHDRV` body, non-UTF-8, or a driver id Core rejects) go to `dlq`.

### `dlq`

- Key: original message key, or Core `dlq_unknown_key` (`unknown`) if the original key is missing
- Body: original message body
- Headers written by Core: `o_partition`, `o_offset`, `o_error`
- Under overload Core may discard a record instead of stalling ingest: [Core](component-core.md#data-pipeline-batch-contract)

### `heartbeat`

- Key: `CORE` for the Core heartbeat
- Body: informational (Core sends the race id); consumers read only the record timestamp
- Relay reads the last retained record on status. Detection: [Race provisioning](flow-race_provisioning.md).

### `event`

- Key: operator-supplied Kafka key (uppercase compact keys preferred, same as other topics)
- Avro: no. Body is opaque bytes. Core does not read this topic.
- Producer: Relay operator HTTP. Size and HTTP contract: [Relay](component-relay.md).
