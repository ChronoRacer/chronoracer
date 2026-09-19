# ADR - Dashboard

Status: accepted

Problem: data is ingested and persisted, but nothing shows it until the race is over and the export is downloaded. The
operator either trusts the Relay healthcheck or reads Core logs in Cloud Logging. Reliable black box, zero visibility.

Shape of the accepted solution: [Dashboard](component-dashboard.md).

## Decision

1. Scope is **read-only**. Any future write action (alerts, driver change, stop) belongs to
   [Relay](component-relay.md) or a new service, never to the dashboard.
2. No real-time requirement. Operators cannot influence a running race. Polling is sufficient, and Core's
   per-sensor-class batching already means fast-path
   sensors land in the database more often and can be polled more often.
3. Data comes from **polling InfluxDB**, not from Kafka and not from Core.
4. The component is **Grafana OSS**, dashboards and datasource provisioned as code.
5. It runs on **always-on Cloud Run**, stateless, scale-to-zero.
6. Influx becomes reachable from the Dashboard Cloud Run subnet only (not Relay, not the Kafka VM), authenticated with
   a **shared admin token in Secret Manager**.

## Why polling InfluxDB

The database already owns a query surface and the data is clean, ordered, and indexed. No Core code changes, and a
dashboard query storm can never corrupt the ingest pipeline - it can only compete for VM resources.

Rejected: **a second Kafka consumer** for charts. It duplicates roughly everything Core does except the Influx write,
in a second language, and introduces a second interpretation of the same messages.

Rejected: **Core pushes parsed data over WebSocket/SSE**. Core would gain a serving responsibility and a listening
socket on a VM that currently has no public NIC and no HTTP surface at all. The latency it buys is latency we do not
need (decision 2). If the latency requirement ever changes, this ADR needs to be revisited.

## Why Grafana

Because scope is read-only (decision 1), the usual objection to Grafana - limited extensibility once a UI needs
actions - costs nothing here. Grafana gives polling, charts, templating, and OIDC login with no code to own.

Rejected: **custom monolith**. Full chart control, but we would own query code, polling, chart rendering, login, config
delivery, and a deployment pipeline to look at a handful of time series. If Grafana ever blocks a required feature, the
data path (Influx SQL) is unchanged, so replacing it is a contained rewrite.

Rejected: **extending Core to serve the UI**. Core already shares a VM with its own database and spikes with it
([ADR - VM types](adr-vm-types.md)); adding a user-facing surface to the one component that must not fail is the wrong
trade.

Rejected: **extending Relay**. Relay is the race lifecycle entrypoint and monitor for operators, not an app accessible
to all users. Mixing a per-second read surface into the component that holds the Firestore mutex is a coupling we do not
want.

## Why always-on Cloud Run

Scale-to-zero means an idle dashboard is effectively free, there is no VM image to bake, no operator-renewed TLS
certificate, no public IP on the race VPC, and no new race-stack resource - the `ingest_open` invariant
([Terraform](component-terraform.md)) stays untouched. The mapped hostname and its Google-managed cert live in always-on
Terraform, same as Relay. Relay already proves the pattern: Cloud Run reaching the race VPC
privately over Direct VPC Egress.

Rejected: **race-scoped deployment** (created and destroyed per race). Pays race-stack complexity for a service that
costs nothing while idle.

Rejected: **separate public VM in the race VPC** (the Kafka VM pattern). Needs an image, boot scripts, a public IP, a
firewall opening, and a TLS certificate on an ephemeral IP, all to host a read-only viewer.

Rejected: **container on the Core VM**. Exposes the database VM to the public internet and puts the viewer inside the
one failure domain we are trying to observe.
