# Deep dive - Compute images

Packer templates live in [`infra/packer`](../infra/packer). Race Terraform does not build images. Family names: [
`names.json`](../infra/contract/names.json). Pin for race lifetime: [Terraform](component-terraform.md).

Relay, Dashboard, Telemetry, and the controller Cloud Build image are **not** Packer. Push those before the stack that
consumes each image can apply: [Control-plane images](operator-control-plane-images.md).

For target VMs used, see [ADR - VM types](adr-vm-types.md).

## Split

**Analysis:** hybrid. Packer bakes Debian 13 with the shared packages, Google Cloud Ops Agent, contract JSON, the
module's Python venv, and its startup overlay. It does **not** bake module code. Rebuild the family when
`requirements.txt`, overlay scripts, units, agent config, or contract JSON change. Publish code with
[`infra/packer/publish-analysis-payload.sh`](../infra/packer/publish-analysis-payload.sh). Fetch at boot:
[Analysis startup](../analysis/startup/README.md#payload).

**Kafka:** full bake. Overlay + Apache Kafka at `/opt/kafka`. Rebuild the family when Kafka, Java, agent config, overlay scripts, or contract JSON change.

**Core:** hybrid. Packer bakes Debian, InfluxDB 3 Core, `rpk` and its pinned Redpanda Connect package (do not install manually with sudo or it will fail), `yq`,
Google Cloud CLI, overlay scripts, `default-config.yaml`, Redpanda templates, and contract JSON. It does
**not** bake the Core rust binary.

Publish the binary with
[`infra/packer/publish-core-payload.sh`](../infra/packer/publish-core-payload.sh) to the artifacts prefix in
[GCS](component-gcs_buckets.md). Fetch at boot: [Core startup](../core/startup/README.md).

Both hybrid payloads are published under an immutable `{module}/releases/.../` prefix. The single GCS object
`{module}/current.json` is the release pointer; the next race reads that pointer without a new image. Publishing the
pointer is the final step, so a VM cannot combine files from two releases.

Do not download vendor packages from the public internet on the private race VMs (no Cloud NAT). Packer builders use the
**default VPC** and an ephemeral public IP so apt and tarball fetches work. Do not build on the race VPC.

## How to publish

Operator credentials (`gcloud auth application-default login`). Roles: create/delete Compute instances, disks, and
images. No dedicated Packer service account.

Builder: `e2-small`, 20 GB disk. The disk contents become the minimum image size; race VMs request their own 20 GB boot
disks. Target machine types: [Terraform](component-terraform.md). The builder zone determines the image storage region,
so use the same zone region as the always-on and race stacks.

Each build wrapper runs [`cleanup-image-family.sh`](../infra/packer/scripts/cleanup-image-family.sh) after Packer
finishes. It keeps the newest Compute Engine image in the family and deletes older images. Compute Engine has no native
image-family retention policy, so do not bypass the wrappers with a direct `packer build`. Do not build or clean up a
family while a race apply is creating VMs from that family.

```bash
gcloud auth application-default login
export PKR_VAR_project_id="$(gcloud config get-value project)"

./infra/packer/build-kafka-vm-image.sh
./infra/packer/build-core-vm-image.sh
./infra/packer/build-analysis-vm-image.sh

# Build and download Core binary on a native CPU builder (release ID = core/version.txt)
./infra/packer/build-core-binary.sh <release-id>

# Publish the Core VM release (Core binary from ./build/core-<release-id>, InfluxDB plugins)
./infra/packer/publish-core-payload.sh <release-id>

# Publish the Analysis VM release (module code)
./infra/packer/publish-analysis-payload.sh <release-id>
```

Version pins: [`infra/packer/versions.pkr.hcl`](../infra/packer/versions.pkr.hcl). Apply always-on once so bucket
`chronoracer-artifacts` exists before the first payload publish. Publish a payload before the first race that fetches
it; a missing `core/current.json` or `analysis/current.json` fails that VM's boot closed.

Always-on IAM for Core and Analysis to read artifacts: [Terraform](component-terraform.md).

## Bake contents

Family names come from [`names.json`](../infra/contract/names.json) (`kafka_family`, `core_family`, `analysis_family`). Packer reads that
file; do not hardcode family strings in templates.

Shared helper [`infra/packer/scripts/gce-metadata.sh`](../infra/packer/scripts/gce-metadata.sh) is baked into all race VM
overlays as `/chronoracer/gce-metadata.sh`. Boot scripts source it and read project, zone, and instance attributes from
the GCE metadata server. They do not call the Compute API. Race VM service accounts do not need
`compute.instances.get`.

**Time sync (Core and Kafka):** [`configure-timesync.sh`](../infra/packer/scripts/configure-timesync.sh) bakes a
`systemd-timesyncd` drop-in (`NTP=metadata.google.internal`, poll 16-32 s). Core latency metrics depend on
cross-VM clock offset.

**Kafka** `/chronoracer/`: overlay from [`kafka/startup`](../kafka/startup) (scripts, `kafka.service`, Ops Agent
configuration),
`gce-metadata.sh`, `enable-ops-agent.sh`, plus `names.json` and `platform.json`. No Core dirs (`redpanda`). Broker bits at
`/opt/kafka`. `need()`: `gcloud`, `openssl`, `jq`, `python3`, `curl`, plus `java`.

**Core** `/chronoracer/`: overlay from [`core/startup`](../core/startup) including `fetch-influx-token.sh`,
`fetch-payload.sh`, systemd units,
Ops Agent template plus renderer, `gce-metadata.sh`, `enable-ops-agent.sh`, `default-config.yaml`, `redpanda/*.yaml`, and contract JSON. Packer installs Debian
`python3-yaml` (for `merge_config.py`). `need()` after payload: `influxdb3`, `python3` + PyYAML,
`jq`, `yq`, `gcloud`, `rpk`, the pinned Redpanda Connect package, and `curl`. The package is installed during the image
build so `rpk connect run` does not download on the race VM. Binary arrives as `/chronoracer/core`. The Avro sensor
schema is compile-time only in the repo ([`core/avro/sensor_data.avsc`](../core/avro/sensor_data.avsc)); Core does not
read it at runtime ([Core](component-core.md#data-pipeline-batch-contract)).

**Analysis** `/chronoracer/`: overlay from [`analysis/startup`](../analysis/startup) (scripts, three systemd units, Ops
Agent configuration, `fetch-payload.sh`), `gce-metadata.sh`, `enable-ops-agent.sh`, contract JSON, and the build-time
`/chronoracer/analysis-venv`. Module code arrives at boot as `/chronoracer/analysis`. Venv and code layout:
[Analysis startup](../analysis/startup/README.md#image-bake).

Boot order on the VM: [Kafka startup](../kafka/startup/README.md), [Core startup](../core/startup/README.md),
[Analysis startup](../analysis/startup/README.md).

Payload layout and IAM: [GCS artifacts](component-gcs_buckets.md).
