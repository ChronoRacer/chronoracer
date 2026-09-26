# ADR - GCP region

Status: accepted

Problem: every GCP resource in this system lives in one region. The first choice was `europe-west3` (Frankfurt) for
physical proximity to Poland and Germany. That region does not offer Cloud Run domain mappings, so Relay and the
Dashboard stay on generated `run.app` URLs instead of names on `chronoracer.live`. It is also the more expensive of the
two realistic EU options.

Telemetry targets a two-to-five-second preview, but it is not a closed-loop control path and all of its components stay
inside one region. Dashboard fleet metrics tolerate Cloud Monitoring sampling delay. Neither requirement justifies a
more expensive or split region ([ADR - Dashboard and live telemetry](adr-dashboard.md)).

## Decision

Put **all** GCP resources in `europe-west1` (Belgium). Compute zone default: `europe-west1-b`, maybe `europe-west1-c` if
stockouts happen often for C4 and N4.
Do not split regions.

Terraform defaults (`var.region` / `var.zone` on bootstrap, always-on, race, and Packer) are the implementation. Current
value for operators: [docs README](README.md).

## Why `europe-west1`

1. Cloud Run domain mappings are available there. Relay and the Dashboard get `relay.chronoracer.live` and
   `dashboard.chronoracer.live` from always-on Terraform ([Terraform](component-terraform.md)). `europe-west3` cannot do
   that.
2. Same-region layout still holds: Artifact Registry, Cloud Build, Cloud Run, Firestore, Secret Manager replicas, GCS,
   and race VMs share one region. Image storage follows the Packer builder zone.
3. Cost is lower than Frankfurt for the paid SKUs this project uses (VMs, Artifact Registry, GCS, Cloud DNS is global
   and unchanged).
4. Proximity to Poland/Germany was the only argument for Frankfurt. It does not buy a property this system needs. If
   latency were critical, we would be deploying in `europe-central2` (Warsaw) instead.

## What does not move with the region

Cloud DNS managed zones are global. The `chronoracer.live` zone, Porkbun NS delegation, and the Kafka TLS hostname stay
where they are ([Domain and TLS](operator-domain-and-tls.md)).

## Migration shape

This is tear-down and recreate, not an in-place move:

- Artifact Registry images and Compute Engine image families copy or rebuild into the new region easily
  ([Control-plane images](operator-control-plane-images.md), [Compute images](component-images.md)).
- Regional resources (VPC/subnets, Cloud Run, Cloud Build trigger, Firestore, Secret Manager replicas, GCS including
  the state bucket) are replaced. Firestore `(default)` location is immutable; delete the database and let always-on
  create it in `europe-west1`.
- Race VMs are already ephemeral.

## Rejected

**Stay in `europe-west3`.** Keeps generated Cloud Run URLs and the higher-cost region for a latency benefit nobody
uses.

**Split regions** (VMs in `europe-west3`, Cloud Run in `europe-west1`). Cross-region Direct VPC Egress, two Artifact
Registry locations, and two image families. The whole point of one region is to avoid that.

**Load balancer + serverless NEG** as a substitute for domain mapping in `europe-west3`. Works, but adds a paid
always-on forwarding rule so a scale-to-zero service can have a pretty name. Domain mapping in `europe-west1` does the
same job without that. Race-scoped Telemetry deliberately uses its deterministic `run.app` URL and needs no mapping.
We cannot use Firebase Hosting for WebSocket proxying. Delegating DNS to Cloudflare for URL flattening would introduce
an unnecessary external dependency.
