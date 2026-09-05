# Deep dive - Compute images

Packer templates live in [`infra/packer`](../infra/packer). Race Terraform does not build images. Family names: [
`names.json`](../infra/contract/names.json). Pin for race lifetime: [Terraform](component-terraform.md).

Relay Cloud Run and the controller Cloud Build image are **not** Packer. Push those first so always-on can
apply: [Control-plane images](operator-control-plane-images.md).

For target VMs used, see [ADR - VM types](adr-vm-types.md).

## Split

**Kafka:** full bake. Overlay + Apache Kafka at `/opt/kafka`. Rebuild the family when Kafka, Java, overlay scripts, or
contract JSON change. That is rare.

**Core:** hybrid. Packer bakes Debian, InfluxDB 3 Core, `rpk`, `yq`, Google Cloud CLI, overlay scripts,
`default-config.yaml`, Redpanda templates, contract JSON, and the build-time InfluxDB plugin environment. It does
**not** bake the Core rust binary, Avro schema, or InfluxDB plugin source files.

Publish the binary, Avro schema, and plugins with
[`infra/packer/publish-core-payload.sh`](../infra/packer/publish-core-payload.sh) to the artifacts prefix in
[GCS](component-gcs_buckets.md). Fetch at boot: [Core startup](../core/startup/README.md).

The GCS prefix `core/current/` is floating: the next race picks the latest upload without a new image.

Do not download vendor packages from the public internet on the race Core VM (no Cloud NAT). Packer builders use the
**default VPC** and an ephemeral public IP so apt and tarball fetches work. Do not build on the race VPC.

## How to publish

Operator credentials (`gcloud auth application-default login`). Roles: create/delete Compute instances and disks, create
images. No dedicated Packer service account.

Builder: `e2-small`, 20 GB disk. Disk contents become the image; race VM machine
types: [Terraform](component-terraform.md).

```bash
# From a bash environment (Git Bash or WSL on Windows).
gcloud auth application-default login
export PKR_VAR_project_id="$(gcloud config get-value project)"

./infra/packer/build-kafka.sh
./infra/packer/build-core.sh

# Linux x86_64 Core binary (rdkafka cmake-build needs a Linux toolchain).
./infra/packer/publish-core-payload.sh /path/to/chronoracer-core
```

Version pins: [`infra/packer/versions.pkr.hcl`](../infra/packer/versions.pkr.hcl). Apply always-on once so bucket
`chronoracer-artifacts` exists before the first payload publish. Apply always-on before the first Core race that fetches
payload.

Always-on IAM for Core to read artifacts: [Terraform](component-terraform.md).

## Bake contents

Family names come from [`names.json`](../infra/contract/names.json) (`kafka_family`, `core_family`). Packer reads that
file; do not hardcode family strings in templates.

Shared helper [`infra/packer/scripts/gce-metadata.sh`](../infra/packer/scripts/gce-metadata.sh) is baked into both
overlays as `/chronoracer/gce-metadata.sh`. Boot scripts source it and read project, zone, and instance attributes from
the GCE metadata server. They do not call the Compute API. Core and Kafka service accounts do not need
`compute.instances.get`.

**Kafka** `/chronoracer/`: overlay from [`kafka/startup`](../kafka/startup) (scripts, `kafka.service`),
`gce-metadata.sh`, plus `names.json` and `platform.json`. No Core dirs (`influxdb-plugins`, `redpanda`). Broker bits at
`/opt/kafka`. `need()`: `gcloud`, `openssl`, `jq`, `python3`, `curl`, plus `java`.

**Core** `/chronoracer/`: overlay from [`core/startup`](../core/startup) including `fetch-payload.sh`, systemd units,
`gce-metadata.sh`, `default-config.yaml`, `redpanda/*.yaml`, contract JSON, empty `influxdb-plugins/`, and the
build-time `/chronoracer/influxdb-plugin-venv` (see [InfluxDB plugin environment](component-influxdb.md#plugin-python-environment)).
Packer installs Debian `python3-yaml` (for `merge_config.py`). `need()` after payload: `influxdb3`, `python3` + PyYAML,
`jq`, `yq`, `gcloud`, `rpk`, `curl`. Binary arrives as `/chronoracer/core`; the schema arrives as
`/chronoracer/avro/sensor_data.avsc`.

Boot order on the VM: [Kafka startup](../kafka/startup/README.md), [Core startup](../core/startup/README.md).

Payload objects: [GCS artifacts](component-gcs_buckets.md).
