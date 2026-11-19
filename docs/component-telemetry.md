# Deep dive - Telemetry

Near-real-time, read-only preview of accepted race sensor values.

Where: race-scoped Cloud Run service `telemetry`, public HTTPS on its deterministic `run.app` URL. The service uses
Direct VPC Egress through a dedicated Telemetry subnet to reach Core. It is created by race Terraform, remains present
through `stopping` and `exporting`, and is destroyed with the race stack.

Lifecycle and IAM: [Terraform](component-terraform.md). URL exposure: [Relay](component-relay.md). Core stream contract:
[Core](component-core.md#telemetry-stream).

Access: public Cloud Run invoker with application-level Auth0 authorization-code login. Only authenticated operators
may use it. Browser authentication is shared with the Dashboard: [Okta](component-okta.md). Setup:
[Browser OAuth runbook](operator-browser-oauth.md).

## Responsibility

- connect to Core's private gRPC `Watch` stream for the whole race
- retain and shape the received preview data for browser use
- present accepted sensor values with near-real-time target latency under one second
- expose read-only browser HTTPS and WebSocket surfaces
- make stream gaps visible rather than imply a complete record

Not its responsibility:

- race lifecycle actions; those remain on [Relay](component-relay.md)
- fleet or infrastructure health; that belongs to the [Dashboard](component-dashboard.md)
- durable storage, export, or post-race analysis
- validating or transforming sensor messages independently of Core
- race readiness or unhealthy detection

Telemetry failure never blocks ingest and never changes race `ready` or `unhealthy`. Relay returns its URL with the
other connection details once the existing Core-heartbeat readiness rule passes. The URL can still lead to an
unavailable service; readiness deliberately does not probe Telemetry.

## Runtime shape

Cloud Run uses one warm instance for the race:

- minimum instances: 1
- maximum instances: 1
- CPU allocated outside requests
- deterministic generated URL:
  `https://telemetry-{project number}.{region}.run.app`
- WebSocket-capable request timeout; browser clients must reconnect before or after Cloud Run's maximum request
  duration

The warm singleton is required because Telemetry pulls continuously from Core and owns any in-memory preview history.
Scale-to-zero would lose all points received before the first browser request. A Cloud Run restart loses component
memory and reconnects to Core; Core does not provide replay.

The generated URL is used instead of a custom domain. A sufficiently short Cloud Run service name receives the
deterministic URL, which can be registered in Auth0 before a race. Terraform obtains and outputs the actual service URI;
Relay returns that output rather than constructing the URL.

## Private Core connection

Telemetry and Analysis may reach Core's telemetry port. The network authorization boundary is owned by
[Terraform](component-terraform.md#always-on-stack). For Telemetry, always-on Terraform owns:

- a dedicated `/26` Direct VPC Egress subnet
- a firewall rule from that subnet to the `core` network tag on the telemetry port
- the Telemetry service account and access to the shared browser OAuth secret (same resource as Dashboard)

Race Terraform attaches those resources to the Cloud Run service. The port is owned by
[`platform.json`](../infra/contract/platform.json). No application credential or TLS is used on this private hop; the
network boundary linked above controls access, matching the private Kafka listener model.

Telemetry calls `Watch` as `client_id` `telemetry` and selects sensors with `TELEMETRY_SENSORS`. Selection rules, limits,
and what happens on a second call with the same id are owned by [Core](component-core.md#clients-and-sensor-selection).

Telemetry retries the private Core connection while the race service exists. Core may not be listening when Cloud Run
starts, and either process may restart. Disconnect and reconnect never affect Core ingestion. Stream ordering,
versioning, payload fields, and loss semantics are owned by [Core](component-core.md#telemetry-stream).

## Browser boundary

The component provides a read-only HTTPS UI and pushes near-real-time changes to authenticated browsers over
WebSocket. Browser API shape, chart selection, sensor discovery, aggregation, retention window, and implementation
language or framework are intentionally left to the Telemetry implementation.

The implementation must preserve these externally visible properties:

- only authenticated operators can establish browser sessions or WebSockets
- the UI distinguishes connected data, known dropped-point gaps, reconnect gaps, and Core restart boundaries
- it does not claim to be a complete or durable race record
- it does not query InfluxDB or Kafka

## Container contract

Environment-specific values are injected by race Terraform:

| Variable              | Source                                             | Use                                     |
|-----------------------|----------------------------------------------------|-----------------------------------------|
| `CORE_TELEMETRY_URL`  | Core internal name + telemetry port                | Private h2c gRPC `Watch`                |
| `RACE_ID`             | Relay/controller race input                        | Reject mismatched Core `hello` events   |
| `TELEMETRY_ROOT_URL`  | Cloud Run service output                           | OAuth callback and browser origin       |
| `OAUTH_CLIENT_ID`     | [`platform.json`](../infra/contract/platform.json) | Shared Auth0 application                |
| `OAUTH_CLIENT_SECRET` | Shared browser OAuth secret (Secret Manager)       | Authorization-code token exchange       |
| `OAUTH_ISSUER`        | [`platform.json`](../infra/contract/platform.json) | Auth0 authorization and token endpoints |

`TELEMETRY_SENSORS` is the exception: the stub image sets it (`*`, all sensors) and Terraform does not inject it. It
is unset or `*` for all sensors, otherwise a comma-separated sensor id list.

Resource names and ports remain owned by contract files. Cloud Run wiring belongs to
[Terraform](component-terraform.md); Auth0 values and callbacks belong to
[Browser OAuth](operator-browser-oauth.md).

## Architecture

Telemetry **MUST** not open multiple connections to Core, especially not one dedicated connection per user.
It should instead have a fan-out architecture model with one, permanent and reliable connection to Core with an internal
dedup and caching mechanism, later replaying the data to individual connected users. A monolith app is preferred over a
client/server setup.

```mermaid
graph LR
    subgraph telemetry["Telemetry component"]
        tel_ingest["Stream sink"]
        cd["Caching, deduplication (sorting if needed)"]
        fanout["Fan-out entrypoint"]
    end

    subgraph users["Users"]
        u1["User 1"]
        u2["User 2"]
        u3["User 3"]
    end

    core["Core telemetry server"] -->|private, single permanent connection| tel_ingest
    tel_ingest --> cd
    cd -->|prepared data| fanout
    fanout -->|Okta, data visualization| u1
    fanout -->|Okta, data visualization| u2
    fanout -->|Okta, data visualization| u3
```

## Accepted risks

- Preview delivery is best-effort. Core reports known loss but never retains history for replay.
- One warm Cloud Run instance costs for the race duration even with no browser connected.
- Runtime memory disappears on Telemetry restart.
- Core and Telemetry stream throughput must be load-tested on GCP before production use. Preview overload must lose
  preview points, never slow Core ingestion.
- Telemetry scaling issues with a single connection (CR locked to one instance) - needs load testing
