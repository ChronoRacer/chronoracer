# Deep dive - GCS buckets

## Race bucket

- Name: chronoracer-race
- Lifespan: always-on
- Written to: during race
- Writers: Relay, Core
- Read from: always
- Readers: Cloud Build controller, Operators via Relay
- Storage class: `TODO`
- Access: private, read via signed URLs distributed by Relay
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

Structure:
```
/base.tfstate
/race.tfstate
```
