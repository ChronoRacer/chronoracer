# Operator runbook — Control-plane images (Relay + controller + dashboard)

Start: empty Artifact Registry. End: `relay`, `controller`, and dashboard tags exist in
`europe-west1-docker.pkg.dev/{project}/chronoracer/`. Always-on Cloud Run and the Cloud Build trigger can apply.

Not Packer. Kafka/Core VM families: [Compute images](component-images.md).

Canonical names: [`infra/contract/names.json`](../infra/contract/names.json) (`ar_repo`, `ar_relay_image`,
`ar_controller_image`, `ar_dashboard_image`). Region: [docs README](README.md).

The dashboard image is upstream Grafana OSS plus provisioning files (datasource, dashboard, Grafana settings), built
from `dashboard/Dockerfile` at the repository root. It carries no secret: the Influx token and Auth0
client secret arrive as Cloud Run environment variables from Secret Manager. Full env
contract: [Dashboard](component-dashboard.md).

## Why this is a separate step

Always-on Terraform **creates** the Artifact Registry repository and **requires** `var.relay_image` to exist at apply
(Cloud Run pulls it). Same apply cannot push the first image. Create the repo (`-target`), push, then full apply.

Controller image string is stored on the trigger; missing controller tag fails the first race start, not always-on
apply. The dashboard image is pulled by its Cloud Run service, so a missing dashboard tag fails always-on apply the
same way Relay does. Publish all three anyway.

## Prerequisites

1. GCP project, `gcloud auth application-default login`, `gcloud auth login`.
2. Bootstrap state bucket: [`infra/bootstrap`](../infra/bootstrap). Always-on backend is `gs://chronoracer-state/base/`.
3. [`infra/always-on/terraform.tfvars`](../infra/always-on/terraform.tfvars): `project_id`, `dns_zone_dns_name`,
   `kafka_fqdn`, and the three image URIs whose **tag** matches `CONTROL_PLANE_IMAGE_TAG` (default `latest`).
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
   dashboard from `dashboard/` (`dashboard/Dockerfile`).
3. Push `{region}-docker.pkg.dev/{project}/{ar_repo}/{ar_relay_image}:{tag}` and the controller and dashboard images the
   same way.
4. Print the three `*_image` lines. Those strings must match `terraform.tfvars` before a full always-on apply.

Rebuild one image: `./infra/always-on/publish-control-plane-images.sh relay`, `controller`, or `dashboard` (local
Docker only). `--remote` always builds all of them.

A dashboard panel change is a dashboard image rebuild plus a new pinned tag — the service keeps no state
([Dashboard](component-dashboard.md)).

## After publish

```bash
cd infra/always-on
terraform apply -var-file=terraform.tfvars
```
