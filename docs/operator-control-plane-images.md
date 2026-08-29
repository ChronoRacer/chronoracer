# Operator runbook — Control-plane images (Relay + controller)

Start: empty Artifact Registry. End: `relay` and `controller` tags exist in
`europe-west3-docker.pkg.dev/{project}/chronoracer/`. Always-on Cloud Run and the Cloud Build trigger can apply.

Not Packer. Kafka/Core VM families: [Compute images](component-images.md).

Canonical names: [`infra/contract/names.json`](../infra/contract/names.json) (`ar_repo`, `ar_relay_image`,
`ar_controller_image`). Region: [docs README](README.md).

## Why this is a separate step

Always-on Terraform **creates** the Artifact Registry repository and **requires** `var.relay_image` to exist at apply
(Cloud Run pulls it). Same apply cannot push the first image. Create the repo (`-target`), push, then full apply.

Controller image string is stored on the trigger; missing controller tag fails the first race start, not always-on
apply. Publish both anyway.

## Prerequisites

1. GCP project, `gcloud auth application-default login`, `gcloud auth login`.
2. Bootstrap state bucket: [`infra/bootstrap`](../infra/bootstrap). Always-on backend is `gs://chronoracer-state/base/`.
3. [`infra/always-on/terraform.tfvars`](../infra/always-on/terraform.tfvars.example): `project_id`, `dns_zone_dns_name`,
   `kafka_fqdn`, and image URIs whose **tag** matches `CONTROL_PLANE_IMAGE_TAG` (default `latest`).
4. Local path: Docker. Remote path: Cloud Build in `europe-west3` (`gcloud builds submit --region=europe-west3`; enabled
   when the always-on `-target` enables APIs). Do not submit the control-plane bake as a global build.

Operator is project Owner (or Artifact Registry writer + Cloud Build editor). No dedicated publish SA.

## Publish

From repository root:

```bash
cd infra/always-on
cp terraform.tfvars.example terraform.tfvars
# fill project_id, dns_zone_dns_name, kafka_fqdn; keep :latest or set the same tag below

cd ../..
export CONTROL_PLANE_IMAGE_TAG=latest
./infra/always-on/publish-control-plane-images.sh
# or, no local Docker:
./infra/always-on/publish-control-plane-images.sh --remote
```

Script:

1. `terraform init` + `terraform apply -target` Artifact Registry. `--remote` also creates the Cloud Build service
   identity and `artifactregistry.writer` on that repo (legacy Cloud Build SA + Cloud Build agent).
2. Build `linux/amd64`: controller from repo root (`controller/Dockerfile`); Relay from `relay/` (`relay/Dockerfile`).
3. Push `{region}-docker.pkg.dev/{project}/{ar_repo}/{ar_relay_image}:{tag}` and the controller image the same way.
4. Print the two `*_image` lines. Those strings must match `terraform.tfvars` before a full always-on apply.

Rebuild one image: `./infra/always-on/publish-control-plane-images.sh relay` or `controller` (local Docker only).
`--remote` always builds both.

Controller image freeze while VMs exist: [Terraform](component-terraform.md). Relay: Cloud Run uses `var.relay_image`;
new tag needs an always-on apply (or the same tag overwritten, then a new Cloud Run revision via apply).

## After publish

```bash
cd infra/always-on
terraform apply -var-file=terraform.tfvars
```

Then Packer, Core payload, TLS, DNS
NS: [Compute images](component-images.md), [Kafka TLS](operator-kafka-tls.md), [Always-on README](../infra/always-on/README.md).
