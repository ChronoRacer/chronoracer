# Flow - Race provisioning

Goal: start one race. Create the two race VMs and Telemetry Cloud Run service, ensure Kafka/Core readiness, and only
then hand operators the Kafka hostname, port, public IP, and Telemetry URL.

MVP: one active race. No queue. Always-on stack is a separate Terraform root; race apply must not manage it.

API: [Relay](component-relay.md). Pointer, mutex, phases: [Firestore](component-firestore.md). Race
apply: [Terraform](component-terraform.md). Controller process: [Controller](component-controller.md).

## Start flow

```mermaid
sequenceDiagram
    participant Op as Operator
    participant Relay
    participant FS as Firestore
    participant CB as CloudBuild
    participant Ctrl as Controller
    participant TF as RaceTerraform
    participant PS as PubSub
    participant Kafka
    participant Core
    participant Telemetry
    Op ->> Relay: start race Okta
    Relay ->> Relay: reject if race ongoing
    Relay ->> Relay: upload YAML then atomic pointer plus race doc
    Relay ->> FS: starting terraformRun apply in_flight UUID runToken buildId empty
    Relay ->> CB: trigger controller
    Relay ->> FS: write buildId
    CB ->> Ctrl: run
    Ctrl ->> FS: read request
    Ctrl ->> TF: apply ingest_open true
    TF ->> Kafka: Kafka VM plus ephemeral public IP
    TF ->> Core: Core plus Influx VM
    TF ->> Telemetry: Cloud Run min/max one plus VPC egress
    TF ->> TF: DNS A record TTL 30s plus public 9094 firewall
    Ctrl ->> FS: status applied
    Ctrl ->> CB: exit
    CB ->> PS: build finished
    PS ->> Relay: push wake
    Relay ->> FS: terraformRun succeeded
    Core ->> Kafka: retry until connected
    Core ->> Kafka: heartbeat
    Op ->> Relay: status
    Relay ->> FS: read applied
    Relay ->> CB: replay if build already terminal
    Relay ->> Kafka: read last heartbeat
    Relay ->> Op: ready plus hostname port IP telemetry URL
```

1. Operator start (Okta). New race vs retry from `failed`: [Relay start](component-relay.md).
2. Cloud Build runs controller `apply`. Both VMs and Telemetry service are in one apply. Core must tolerate Kafka not
   listening yet (currently Core dies on Kafka not listening, but runs as service so will restart). Telemetry retries
   until Core's private WebSocket listener is available.
3. Kafka boot: [Kafka startup](../kafka/startup/README.md). Core boot: [Core startup](../core/startup/README.md).
4. Pub/Sub wakes Relay, or status replays a terminal build: [Relay build-finished](component-relay.md).
5. Status returns hostname, port, public IP, and Telemetry URL only when Healthcheck says ready. Devices already have
   Okta client id and secret. They use that hostname/IP (TLS SNI/verify on the hostname). Operators use the Telemetry
   URL with browser Auth0 login.

IP may change every race; hostname and cert do not.

## Healthcheck

Canonical ready/unhealthy rules. Relay status only exposes them; Firestore does not store them.

Core has no health API. Health is a heartbeat on the private Kafka heartbeat topic, key `CORE`
([Kafka topics](component-kafka.md)). Relay on status reads the last retained record. It does not start a consumer at
latest and hope a beat arrives during the request.

Heartbeat means: Core process is up, private Kafka is reachable, Influx is reachable. Relay does not probe Influx or the
public listener. Kafka boot already refused to start without public TLS bind. If the heartbeat record can be read,
private Kafka accepted a connection.

- Phase `applied` and latest heartbeat age ≤ `service.heartbeat_freshness_seconds` (50 in
  [`relay-config.yaml.tftpl`](../infra/always-on/relay-config.yaml.tftpl); Core publishes every
  `runtime.heartbeat_interval_s` = 30s) → ready.
- Phase `applied`, no heartbeat record yet → not ready, not unhealthy.
- Phase `applied`, heartbeat went stale, or Kafka read fails → unhealthy. Phase stays `applied`. `lastError` on read
  fail. Still return phase, `startedAt`. No auto-destroy.

Provisioning (`starting`) cannot use Kafka. That phase is Firestore plus Cloud Build (push, or status bind/replay).

The [Dashboard](component-dashboard.md) and [Telemetry](component-telemetry.md) are not part of readiness. Neither
publishes or reads a heartbeat, and an unreachable UI says nothing about race health. Telemetry may still be connecting
to Core when Relay first reports ready; this can lose preview points but cannot delay ingest.

Relay scale-to-zero: heartbeat is read on status only. Unsolicited wakes: Cloud Build finished, and export-done. Missed
Cloud Build push is recovered on status. Export-done is not.

## Failure

Phase and mutex outcomes: [Firestore](component-firestore.md). API rejects: [Relay](component-relay.md).

Apply ok, heartbeat never fresh: phase stays `applied`; status not-ready or unhealthy. Force-destroy (nothing reliable
to export) or wait.
