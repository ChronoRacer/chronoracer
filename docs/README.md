# Chronoracer documentation

## Infrastructure overview

### Deployment basics

Deployment on Google Cloud Platform. 

Whatever can be in the free tier is in the free tier (Cloud Run scale-to-zero; Firestore, Pub/Sub, Cloud Build have generous free tier limits).

Not free: Cloud Run VPC access to Kafka, Cloud DNS, Artifact Registry, GCS buckets. Race VMs are paid and sized to be reliable.

Region: all GCP resources in `europe-west3` (Frankfurt). Do not split regions.

### Topology

```mermaid
flowchart LR
  subgraph alwaysOn [Always on]
    Okta[Okta]
    Operators[Operators]
    Relay[Relay Cloud Run]
    FS[Firestore]
    GcsRace[GCS race bucket]
    GcsState[GCS state bucket]
    PS[PubSub]
    CB[Cloud Build Controller]
    TF[Terraform]
  end
  subgraph raceVpc [Race VPC]
    Kafka[Kafka VM]
    CoreDB[Core plus InfluxDB VM]
  end
  Devices[Track devices]
  Operators -->|HTTPS Okta, API| Relay
  Operators -->|manual| TF
  Relay <--> |private, status| FS
  FS --> |private, status| CB
  Relay --> |private, Core YAML| GcsRace
  GcsRace --> |private, exports| Relay
  PS --> |SA identity, push endpoints| Relay
  Relay --> |private, triggers| CB
  CB --> |private, push endpoint triggers| PS
  CB --> |private, manages| raceVpc
  GcsRace --> |private, Core YAML| CB
  Relay -->|private, control events| Kafka
  CoreDB -->|private, heartbeat| Kafka
  Devices -->|TLS Okta, data| Kafka
  Kafka -->|private, data, control events| CoreDB
  Kafka -->|private, heartbeat| Relay
  CoreDB -->|private, exports| GcsRace
  CoreDB -->|private, export-done| PS
  CB -->|private, Race infra| TF
  TF --> |TF state| GcsState
```

## Components

See graph above for connections. Check the dedicated documents for details about components.


| Component   | Document                                        |
|-------------|-------------------------------------------------|
| Core        | [Core - Deep dive](component-core.md)           |
| InfluxDB    | [InfluxDB - Deep dive](component-influxdb.md)   |
| Relay       | [Relay - Deep dive](component-relay.md)         |
| Kafka       | [Kafka - Deep dive](component-kafka.md)         |
| Firestore   | [Firestore - Deep dive](component-firestore.md) |
| GCS Buckets | [GCS - Deep dive](component-gcs_buckets.md)     |
| Terraform   | [Terraform - Deep dive](component-terraform.md) |
| Okta        | [Okta - Deep dive](component-okta.md)           |

## Flows

Some of the business logic flows deserve a dedicated document.

| Flow              | Document                                              |
|-------------------|-------------------------------------------------------|
| Race provisioning | [Flow - Race provisioning](flow-race_provisioning.md) |
| Race destruction  | [Flow - Race destruction](flow-race_destruction.md)   |

