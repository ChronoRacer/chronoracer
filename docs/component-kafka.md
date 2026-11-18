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
| PRIVATE    | `0.0.0.0:9092`   | `{kafka}.{zone}.c.{project}.internal:9092` | PLAINTEXT (Core, Relay, Telemetry). No Okta.                                                                                                                                                   |
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

- Key: `{sensorId}`, a `name` from [`infra/contract/sensors.json`](../infra/contract/sensors.json). At most 12 ASCII
  characters.
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

- Key: producer-supplied Kafka key (uppercase compact keys preferred, same as other topics). The
  [driver display](#driver-display-records) keys are reserved.
- Avro: no. Body is opaque bytes, except driver display records. Core does not read this topic.
- Producers: Relay operator HTTP, any key and body (size and HTTP contract: [Relay](component-relay.md)); Telemetry
  [driver messages](component-telemetry.md#driver-messages), only driver display records.

#### Driver display records

Operator flags and messages for the driver's display, one record per operator action. Consumer: the car display in
the `chronoracer-dashboard` repository, whose `docs/specs/2026-11-16-flagi-i-komunikaty-operatora.md` owns what the
display does with them. The car sends nothing back: no acknowledgement, no read receipt.

| Part      | Value                                                                                    |
|-----------|------------------------------------------------------------------------------------------|
| Key       | Message type from the table below, or `ALL` (only with `"action": "clear"`).             |
| Value     | UTF-8 JSON object, fields below.                                                         |
| Timestamp | Producer `CreateTime` (the client default). The car uses it to drop late timed messages. |

| Key         | Group   | `text`   | Car display background                         |
|-------------|---------|----------|------------------------------------------------|
| `YELLOW`    | flag    | optional | yellow                                         |
| `RED`       | flag    | optional | red                                            |
| `GREEN`     | flag    | optional | green                                          |
| `BLUE`      | flag    | optional | blue                                           |
| `SAFETY`    | flag    | optional | bright yellow; shows "SAFETY CAR" without text |
| `CHECKERED` | flag    | optional | chequered                                      |
| `BOX`       | message | required | brown                                          |
| `PACE`      | message | required | navy                                           |
| `MSG`       | message | required | dark                                           |

A flag is identified by its colour on the display, so the car shows no flag name. With no `text` a flag is a plain
colour field.

| Field        | Type    | Required                                           | Meaning                                                                                                       |
|--------------|---------|----------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `action`     | string  | yes                                                | `"show"` or `"clear"`.                                                                                        |
| `text`       | string  | `show` of `BOX`, `PACE`, `MSG`; optional for flags | One line for the display: 1 to 200 characters, no control characters. Ignored on `clear`.                     |
| `duration_s` | integer | no                                                 | Seconds on the driver's display: `1` to `3600`, or `-1` for "until cleared". Default `5`. Ignored on `clear`. |

```
key "YELLOW"   value {"action": "show", "text": "Wypadek w zakręcie 3", "duration_s": -1}
key "RED"      value {"action": "show", "duration_s": 30}
key "BOX"      value {"action": "show", "text": "BOX BOX — zjedź w tym okrążeniu"}
key "YELLOW"   value {"action": "clear"}
key "ALL"      value {"action": "clear"}
```

Rules:

1. At most one message per type is active. A `show` for a type that is already active replaces it: new text, time
   counted again.
2. `clear` with a type key removes that type. `clear` with key `ALL` removes every active message. Clearing a type
   that is not active does nothing.
3. Order is the contract: a `clear` must reach the car after the `show` it cancels. `event` has one partition, and
   Telemetry produces one record at a time from one producer.
4. A receiver ignores unknown JSON fields. It skips a record with an unknown key, a value that is not a JSON object,
   a missing or unknown `action`, a `duration_s` outside the range, or invalid `text`. A plain-text record in the
   earlier `MSG | BOX | FLAG | PACE` convention is therefore skipped, not shown.
5. The ten keys above are reserved on `event`. Other producers on this topic must not use them.
6. Time runs from the moment the record reaches the car. A timed message that reaches the car more than 10 seconds
   late is skipped. An "until cleared" message is shown however late it arrives.

Open:

- Rule 6 compares the record timestamp with the car's clock, which can be wrong after a start without network. The
  car side decides how to measure it.
- After the car application restarts its display is empty; earlier messages are not restored until the operator sends
  again. Re-sending the active set periodically from Telemetry would close this gap.
- 200 characters do not fit the 1280x400 car display at a readable size. A lower limit or a truncation rule is open.
- Relay operator events accept any key, so an operator could send a reserved key by hand.
