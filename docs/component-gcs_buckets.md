# Deep dive - GCS buckets

Layout and IAM intent for the three buckets. Who creates the buckets: [Terraform](component-terraform.md). Core payload
publish: [Compute images](component-images.md).

## Race bucket

- Name: chronoracer-race
- Lifespan: always-on
- Written to: during race
- Writers: Relay, Core
- Read from: always
- Readers: Cloud VM Core (`core-config.yaml` only); Cloud Build controller (`core-config.yaml` only); Operators via
  Relay (`export/` plus YAML Relay wrote)
- Storage class: `STANDARD`
- Access: private, read via signed URLs distributed by Relay. Relay mints 15-minute GET URLs with IAM `signBlob` (Cloud
  Run ADC has no private key). IAM: [Terraform](component-terraform.md). API: [Relay historical](component-relay.md).
- Purpose: store race data

Structure:

```
/{raceId} (for each race)
|---  /core-config.yaml
|---  /export
      |--- /db
            |--- /{table}.parquet (one file per exported Influx table; the table is the sensor measurement)
      |--- /kafka
            |--- /{topic}
                  |--- /part-….parquet (many objects per topic)
```

Under `{raceId}/`, `export/db` and `export/kafka` are `persistence.export.nested_path_in_bucket` and
`kafka.export.nested_path_in_bucket` in [`core/default-config.yaml`](../core/default-config.yaml). The Kafka object
name is the Redpanda template: [`export.gcs.yaml`](../core/redpanda/export.gcs.yaml).

## State bucket

- Name: chronoracer-state
- Lifespan: always-on
- Written to: whenever Terraform runs
- Writers: Cloud Build controller, Operators
- Read from: whenever Terraform runs
- Readers: Cloud Build controller, Operators
- Storage class: `STANDARD`
- Access: private
- Purpose: store Terraform state files

Structure (Terraform GCS backend layout):

```
/base/default.tfstate
/race/default.tfstate
```

## Artifacts bucket

- Name: chronoracer-artifacts
- Lifespan: always-on
- Written to: when an operator publishes a Core payload (Packer publish script)
- Writers: Operators (user credentials). Not Core. Not Relay. Not the controller.
- Read from: Core VM boot (`core/current.json` and the pointed immutable release only in practice)
- Readers: Core SA (`roles/storage.objectViewer` on the whole bucket)
- Storage class: `STANDARD`
- Access: private. No secrets. Not race data.
- Purpose: floating Core binary + InfluxDB plugins so the Core image family does not rebuild on every rust or plugin
  change

Structure:

```
/core/current.json
/core/releases/{release}
      |--- core (binary)
      |--- /influxdb-plugins
            |--- export_database.py
```

The publish script uploads a complete release first, then replaces `core/current.json` as its final operation. Core reads
the pointer once during boot and fetches the binary and plugins from that immutable release. Old releases are retained;
the image family retention policy is separate and is owned by [Compute images](component-images.md).

Do not put this payload on `chronoracer-race` (Core YAML IAM stays `endsWith("/core-config.yaml")`).
