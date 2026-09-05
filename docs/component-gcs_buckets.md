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
            |--- /{sensorId}.parquet (for each sensor in race)
      |--- /kafka
            |--- /{topicName}.parquet (for each exported topic)
```

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
- Read from: Core VM boot (`core/current/` only in practice)
- Readers: Core SA (`roles/storage.objectViewer` on the whole bucket)
- Storage class: `STANDARD`
- Access: private. No secrets. Not race data.
- Purpose: floating Core binary + InfluxDB plugins so the Core image family does not rebuild on every rust or plugin
  change

Structure:

```
/core/current/core
/core/current/avro/
      |--- sensor_data.avsc
/core/current/influxdb-plugins
      |--- export_database.py
```

Do not put this payload on `chronoracer-race` (Core YAML IAM stays `endsWith("/core-config.yaml")`).
