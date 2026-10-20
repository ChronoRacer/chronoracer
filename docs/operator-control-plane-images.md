# Operator runbook — Container images (Relay + controller + Dashboard + Telemetry)

Start: empty Artifact Registry. End: `relay`, `controller`, `dashboard`, and `telemetry` tags exist in
`europe-west1-docker.pkg.dev/{project}/chronoracer/`. Always-on resources and race Terraform can apply.

Not Packer. Compute VM families: [Compute images](component-images.md).

Canonical names: [`infra/contract/names.json`](../infra/contract/names.json) (`ar_repo`, `ar_relay_image`,
`ar_controller_image`, `ar_dashboard_image`, `ar_telemetry_image`). Region: [docs README](README.md).

The Dashboard image is upstream Grafana OSS plus provisioned Google Cloud Monitoring datasource, dashboards, and
Grafana settings. It carries no secret; the Auth0 client secret arrives from Secret Manager. Full environment contract:
[Dashboard](component-dashboard.md).

Telemetry is an application container with no language or framework decision in this architecture. Its image contract:
[Telemetry](component-telemetry.md). Race Terraform requires its published image before starting a race.

## Why this is a separate step

Always-on Terraform **creates** the Artifact Registry repository and **requires** `var.relay_image` to exist at apply
(Cloud Run pulls it). Same apply cannot push the first image. Create the repo (`-target`), push, then full apply.

Controller image string is stored on the trigger; missing controller tag fails the first race start, not always-on
apply. Dashboard is pulled by always-on Cloud Run, so a missing tag fails always-on apply like Relay. Telemetry is
pulled by race Cloud Run, so a missing tag fails race apply. Publish all four.

## Prerequisites

1. GCP project, `gcloud auth application-default login`, `gcloud auth login`.
2. Bootstrap state bucket: [`infra/bootstrap`](../infra/bootstrap). Always-on backend is `gs://chronoracer-state/base/`.
3. [`infra/always-on/terraform.tfvars`](../infra/always-on/terraform.tfvars): `project_id`, `dns_zone_dns_name`,
   `kafka_fqdn`, and the four image URIs (`relay_image`, `controller_image`, `dashboard_image`, `telemetry_image`) whose
   **tag** matches `CONTROL_PLANE_IMAGE_TAG` (default `latest`).
4. Local path: Docker. Remote path: Cloud Build in `europe-west1` (`gcloud builds submit --region=europe-west1`; enabled
   when the always-on `-target` enables APIs). Do not submit the control-plane bake as a global build.

Operator is project Owner (or Artifact Registry writer + Cloud Build editor). No dedicated publish SA.

## Publish

From repository root:

```bash
export CONTROL_PLANE_IMAGE_TAG=latest
./infra/always-on/publish-control-plane-images.sh
# or through Cloud Build:
./infra/always-on/publish-control-plane-images.sh --remote
```

Script:

1. `terraform init` + `terraform apply -target` Artifact Registry. `--remote` also creates the Cloud Build service
   identity and `artifactregistry.writer` on that repo (legacy Cloud Build SA + Cloud Build agent).
2. Build `linux/amd64`: controller from repo root (`controller/Dockerfile`); Relay from `relay/` (`relay/Dockerfile`);
   Dashboard from `dashboard/` (`dashboard/Dockerfile`); Telemetry from `telemetry/` (`telemetry/Dockerfile`).
3. Push `{region}-docker.pkg.dev/{project}/{ar_repo}/{ar_relay_image}:{tag}` and the controller, Dashboard, and
   Telemetry images the same way.
4. Print the four `*_image` lines. Those strings must match Terraform inputs before always-on or race apply.

Rebuild one image: `./infra/always-on/publish-control-plane-images.sh relay`, `controller`, `dashboard`, or `telemetry`
(local Docker only). `--remote` always builds all of them.

A dashboard panel change is a dashboard image rebuild plus a new pinned tag — the service keeps no state
([Dashboard](component-dashboard.md)).

## After publish

```bash
cd infra/always-on
terraform apply -var-file=terraform.tfvars
```
