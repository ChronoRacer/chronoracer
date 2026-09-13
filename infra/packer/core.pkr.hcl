source "googlecompute" "core" {
  project_id              = var.project_id
  source_image_family     = var.debian_source_image_family
  source_image_project_id = [var.debian_source_image_project]
  zone                    = var.zone
  machine_type            = var.builder_machine_type
  disk_size               = var.builder_disk_size_gb
  disk_type               = "pd-balanced"
  network                 = var.network
  omit_external_ip        = false
  preemptible             = true
  on_host_maintenance     = "TERMINATE"
  ssh_username            = "packer"
  image_name              = "${local.names.core_family}-{{timestamp}}"
  image_family            = local.names.core_family
  image_storage_locations = [local.image_storage_location]
  image_description       = "Chronoracer Core base. InfluxDB 3 plus overlay. Binary and plugins from GCS at boot."
  tags = ["packer"]
}

build {
  name = "chronoracer-core"
  sources = ["source.googlecompute.core"]

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "scripts/install-common.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    environment_vars = [
      "YQ_VERSION=${var.yq_version}",
      "RPK_VERSION=${var.rpk_version}",
      "REDPANDA_CONNECT_VERSION=${var.redpanda_connect_version}",
      "INFLUXDB3_PACKAGE_VERSION=${var.influxdb3_package_version}",
    ]
    script = "scripts/install-core-base.sh"
  }

  provisioner "shell" {
    inline = ["mkdir -p /tmp/overlay /tmp/contract /tmp/redpanda"]
  }

  provisioner "file" {
    sources = [
      "../../core/startup/entrypoint.sh",
      "../../core/startup/fetch-payload.sh",
      "../../core/startup/start-influx.sh",
      "../../core/startup/render-config.sh",
      "../../core/startup/merge_config.py",
      "../../core/startup/influxdb3.service",
      "../../core/startup/chronoracer-core.service",
      "scripts/gce-metadata.sh",
    ]
    destination = "/tmp/overlay/"
  }

  provisioner "file" {
    source      = "../../core/default-config.yaml"
    destination = "/tmp/default-config.yaml"
  }

  provisioner "file" {
    sources = [
      "../../core/redpanda/export.gcs.yaml",
      "../../core/redpanda/export.local.yaml",
    ]
    destination = "/tmp/redpanda/"
  }

  provisioner "file" {
    sources = [
      "../../infra/contract/names.json",
      "../../infra/contract/platform.json",
    ]
    destination = "/tmp/contract/"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "scripts/bake-overlay.sh"
  }

  provisioner "shell" {
    inline = [
      "command -v influxdb3",
      "command -v yq",
      "command -v rpk",
      "command -v redpanda-connect",
      "command -v gcloud",
      "command -v python3",
      "command -v jq",
      "command -v curl",
      "python3 -c 'import yaml'",
      "test -x /chronoracer/entrypoint.sh",
      "test -x /chronoracer/fetch-payload.sh",
      "test -x /chronoracer/start-influx.sh",
      "test -x /chronoracer/render-config.sh",
      "test -f /chronoracer/gce-metadata.sh",
      "test -f /chronoracer/merge_config.py",
      "test -f /chronoracer/influxdb3.service",
      "test -f /chronoracer/chronoracer-core.service",
      "test -f /chronoracer/default-config.yaml",
      "test -x /chronoracer/influxdb-plugin-venv/bin/python",
      "/chronoracer/influxdb-plugin-venv/bin/python -c 'import pyarrow'",
      "test -f /chronoracer/redpanda/export.gcs.yaml",
      "test -f /chronoracer/redpanda/export.local.yaml",
      "test -f /chronoracer/names.json",
      "test -f /chronoracer/platform.json",
      "test -d /chronoracer/influxdb-plugins",
      "test ! -e /chronoracer/core",
    ]
    execute_command = "chmod +x {{ .Path }}; sudo bash '{{ .Path }}'"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "scripts/finalize-image.sh"
  }
}
