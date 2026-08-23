# Context - locked in choices

Deployment on Google Cloud Platform. Always-on pieces stay in the free tier where they fit (Cloud Run scale-to-zero, Firestore). Not free: Cloud Run VPC access to Kafka, Cloud DNS, Artifact Registry, Pub/Sub, Cloud Build when used. Race VMs are paid and sized to be reliable.

Concept: Race — when started, additional infrastructure is provisioned for a few hours only.

## How to read

Each concern has one source of truth. Other files point; they do not restate the matrix.

| Concern | File |
| --- | --- |
| Topology, network, identity, lifecycle inventory, ready meaning | [1-infrastructure.md](1-infrastructure.md) |
| Start flow, VM boot, heartbeat | [2-race-provisioning.md](2-race-provisioning.md) |
| Stop, export assignment, destroy | [2-race-destruction.md](2-race-destruction.md) |
| Firestore shape, writers, mutex, phase machine | [3-firestore.md](3-firestore.md) |
| Terraform stacks, resources, `close_ingest` invariant | [4-terraform.md](4-terraform.md) |
| Relay callers, I/O, reject rules, build-finished | [6-relay-api.md](6-relay-api.md) |
| Why locks exist; do not reopen | [7-architecture-review.md](7-architecture-review.md) |

No part 5. Numbering is historical.

Out of scope for this set: Core config YAML shape, Core internals including export flows, InfluxDB internals, Kafka internals, image pipelines.

## What is needed

### Relay

Management center. Stateless Cloud Run, public HTTPS, Okta (no Google invoker for operators). Scale-to-zero. Survives races. Only Cloud Build caller. Talks to Firestore, GCS, Cloud Build, Pub/Sub, and private Kafka.

Job: operator overview and control. Return Kafka hostname, port, and public IP when ready. Issue export download access.

### Core unit

Processes broker data into Influx. Race-scoped, private, no operator API, no Firestore, no Cloud Build. YAML at boot. Export under `{gcsPrefix}/export/`. Publishes export-done. IAM denies YAML write.

### Database

InfluxDB 3 Core, self-managed, single node, same VM as Core. Race-scoped. Private. Persist processed data, then export and destroy. Not InfluxDB Cloud.

### Message broker

Apache Kafka, single node, race-scoped. Public ingest (TLS + Okta). Private path for Core and Relay. Broker does not start unless public TLS bound. Forwards device data to Core. Control topic: shutdown key `shutdown`, compacted heartbeat key `core`.

## Flow

1. Operator starts race on Relay.
2. Race VMs provision until ready (apply ok, Kafka public TLS bound at boot, Core heartbeat fresh).
3. Status returns hostname, port, public IP. Devices produce. Core consumes privately.
4. Operator stops. Public ingest firewall closes, then Relay publishes shutdown.
5. Core exports to GCS, publishes export-done. Relay destroys race VMs. Relay stays. Operator downloads through Relay.
