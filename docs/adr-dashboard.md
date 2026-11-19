# ADR - Dashboard and live telemetry

Status: accepted

Problem: operators need two different views during a race:

1. infrastructure health — whether the race fleet is running and keeping up
2. near-real-time sensor preview — what accepted values the car is producing

The previous Dashboard design mixed both concerns by polling InfluxDB from Grafana. Polling persisted batches cannot
provide a reliable near-real-time preview, gives Grafana an admin-scoped database token, and lets UI queries compete
with ingest on the Core VM. Additionally, a small Grafana instance could not handle the polling load of InfluxDB at 5 a
second refresh rate.

Current component shapes: [Dashboard](component-dashboard.md) and [Telemetry](component-telemetry.md).

## Decision

1. Keep **Dashboard** as always-on Grafana OSS for race-fleet health only.
2. Dashboard reads only Google Cloud Monitoring. It has no Influx token, no database route, and no race-sensor panels.
3. Install Ops Agent on all race VM. Each collects whole-VM host metrics only (CPU,
   memory, disk, network).
4. Add **Telemetry** as a race-scoped Cloud Run service for accepted sensor values.
5. Telemetry pulls a versioned Protobuf stream from Core. The protocol is
   [gRPC server streaming](adr-telemetry-stream.md). It does not consume Kafka and does not query InfluxDB.
6. Telemetry is read-only and operator-authenticated. Any lifecycle write remains on Relay.
7. Neither Dashboard nor Telemetry affects race readiness or ingest.

## Why split the components

Fleet metrics and race values have different lifecycles, sources, and failure meaning. Also, Grafana cannot provide live
preview of the data - InfluxDB polling was tested and breaks under load.

Dashboard survives races and answers whether VMs, Influx ingest, and Telemetry infrastructure are healthy.
Cloud Monitoring retains this operational data independently of the race VMs. Grafana remains useful because it
provides provisioned operational dashboards.

Telemetry exists only while a race exists and answers what accepted values are arriving now. It may keep ephemeral
preview state but owns no durable record. This isolates preview-specific load and behavior from the always-on
operations dashboard.

## Why a Core stream

The preview must contain Core-accepted, processed values with Core's driver assignment. Reading raw Kafka would require
Telemetry to duplicate Core validation and transformation and could disagree with persisted data.

A dedicated Kafka preview topic was rejected. It would add another producer and consumer path to the single-node broker
that is the critical ingest buffer. Current local load results leave headroom but do not prove GCP broker capacity, and
the preview does not need Kafka durability.

Core exposes one private server stream. Network authorization is owned by
[Terraform](component-terraform.md#always-on-stack). Protocol and rejected transports: [ADR - Core telemetry stream protocol](adr-telemetry-stream.md). What the stream carries, and how loss
works: [Core](component-core.md#telemetry-stream).

Rejected: **Core pushes outbound to Telemetry**. It makes the critical process own destination discovery, OAuth/IAM
calling behavior, and reconnect policy. Pulling keeps Core unaware of the consumer.

## Why race-scoped Cloud Run

Cloud Run supplies managed public HTTPS and WebSocket support without a public VM, image bake, public firewall, DNS
update, or per-race certificate issuance. The deterministic `run.app` URL is registered with Auth0 and returned by
Relay.

Telemetry uses one warm instance with always-allocated CPU for the race. This costs more than scale-to-zero but is
required to pull continuously from Core before an operator opens the UI. It remains through `stopping` and `exporting`
and is deleted by race destroy.

Rejected: **public race VM with ephemeral IP**. DNS must update before ACME issuance, certificate creation delays race
startup, and every race risks CA rate limits and another internet-facing host.

Rejected: **public race VM with reserved IP**. Stable DNS and a pre-issued certificate work, but the idle address costs
money and the service still needs VM patching, boot scripts, and TLS ownership.

Rejected: **always-on Telemetry scaled to zero**. It cannot retain points from race start without a warm instance and
complicates binding one service to changing race identity and state.

## Why Cloud Monitoring for Dashboard

Core writes custom metrics to GCM. Ops Agent host metrics, and native Compute Engine/Cloud Run
are also available there.

This removes:

- the admin-scoped Influx token from Dashboard
- Influx's VPC listener and Dashboard firewall rule
- Dashboard query load from the Core VM
- duplicate ownership of race-value presentation

## Security and failure boundaries

- Dashboard service account gets read-only Cloud Monitoring access.
- Telemetry gets shared browser OAuth secret access (same Secret Manager resource as Dashboard) and VPC access from its
  dedicated subnet to Core's telemetry port.
- Core's telemetry endpoint has no public route and no application credential; firewall source range is its
  authorization boundary.
- Dashboard or Telemetry failure never blocks Kafka, Core, Influx, shutdown, or export.
- Neither component evaluates alerts or offers browser write actions. Telemetry only displays the predictions and
  alerts analysis pushes to its token-authorized [analysis ingest](component-telemetry.md#analysis-ingest).
