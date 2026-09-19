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

Core creates topics on the private listener at runtime, not at Kafka boot.

| Topic       | Name owner                                                                                         | Partitions | Replication |
|-------------|----------------------------------------------------------------------------------------------------|------------|-------------|
| `data`      | [`platform.json`](../infra/contract/platform.json) `kafka_data_topic`                              | 4          | 1           |
| `control`   | [`platform.json`](../infra/contract/platform.json) `kafka_control_topic`                           | 1          | 1           |
| `heartbeat` | [`platform.json`](../infra/contract/platform.json) `kafka_heartbeat_topic`                         | 1          | 1           |
| `dlq`       | [`core/default-config.yaml`](../core/default-config.yaml) (not in `platform.json`)                 | 3          | 1           |

Partition counts live with Core config because Core is the topic creator. This also means that sending to Kafka is effectively blocked until Core has created the topics.

Shared rules:

- Keys are uppercase. Compact keys preferred.
- Compression is a producer setting, not a topic setting. Core and Relay produce `snappy`. Device producers may use
  `lz4` (when using a different compression type than `snappy` or `lz4` ensure Core binary supports the codec).
- Delivery is at-least-once. No Kafka transactions.

### `data`

- Key: `{sensorId}` (`ID` column in `sensors.xlsx`)
- Avro: yes. Canonical schema: [`core/avro/sensor_data.avsc`](../core/avro/sensor_data.avsc)

Car computer evaluates immediate warnings, so a short send delay only slows the dashboard. Analysis wants a window, so
the producer may batch readings in one message. Each `delta` is nanoseconds from `timestamp` and must be at most 2
seconds (`2000000000`). Core also batches internally: [Core](component-core.md#data-pipeline-batch-contract).

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
| `CHDRV`    | UTF-8 driver ID, no Avro                  | Driver ID changes from acknowledgement. Small swap discrepancy is accepted.                     |
| `SHUTDOWN` | empty                                     | Core drains Kafka and starts export. Relay publishes this after `close_ingest` succeeds.        |

Unknown keys are ignored. Parse failures (missing key, missing `CHDRV` body, non-UTF-8) go to `dlq`.

### `dlq`

- Key: original message key, or Core `dlq_unknown_key` (`unknown`) if the original key is missing
- Body: original message body
- Headers written by Core: `o_partition`, `o_offset`, `o_error`

### `heartbeat`

- Key: `CORE` for the Core heartbeat
- Body: optional status
- Relay reads the last retained record on status. Detection: [Race provisioning](flow-race_provisioning.md).
