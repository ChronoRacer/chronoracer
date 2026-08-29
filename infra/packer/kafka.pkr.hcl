source "googlecompute" "kafka" {
  project_id              = var.project_id
  source_image_family     = var.debian_source_image_family
  source_image_project_id = var.debian_source_image_project
  zone                    = var.zone
  machine_type            = var.builder_machine_type
  disk_size               = var.builder_disk_size_gb
  disk_type               = "pd-balanced"
  network                 = var.network
  omit_external_ip        = false
  ssh_username            = "packer"
  image_name              = "${local.names.kafka_family}-{{timestamp}}"
  image_family            = local.names.kafka_family
  image_storage_locations = [var.image_storage_location]
  image_description       = "Chronoracer Kafka. /opt/kafka plus overlay at /chronoracer."
  tags = ["packer"]
}

build {
  name = "chronoracer-kafka"
  sources = ["source.googlecompute.kafka"]

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "scripts/install-common.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    environment_vars = [
      "KAFKA_VERSION=${var.kafka_version}",
      "KAFKA_SCALA_BINARY=${var.kafka_scala_binary}",
    ]
    script = "scripts/install-kafka.sh"
  }

  provisioner "shell" {
    inline = ["mkdir -p /tmp/overlay /tmp/contract"]
  }

  provisioner "file" {
    sources = [
      "../../kafka/startup/entrypoint.sh",
      "../../kafka/startup/adjust-limits.sh",
      "../../kafka/startup/fetch-tls.sh",
      "../../kafka/startup/render-server-properties.sh",
      "../../kafka/startup/render_server_properties.py",
      "../../kafka/startup/format-kraft.sh",
      "../../kafka/startup/start-kafka.sh",
      "../../kafka/startup/wait-public-listener.sh",
      "../../kafka/startup/kafka.service",
      "scripts/gce-metadata.sh",
    ]
    destination = "/tmp/overlay/"
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
      "test -x /opt/kafka/bin/kafka-server-start.sh",
      "command -v java",
      "command -v gcloud",
      "command -v openssl",
      "command -v jq",
      "command -v python3",
      "command -v curl",
      "test -x /chronoracer/entrypoint.sh",
      "test -x /chronoracer/adjust-limits.sh"
      "test -x /chronoracer/fetch-tls.sh",
      "test -x /chronoracer/render-server-properties.sh",
      "test -x /chronoracer/format-kraft.sh",
      "test -x /chronoracer/start-kafka.sh",
      "test -x /chronoracer/wait-public-listener.sh",
      "test -f /chronoracer/gce-metadata.sh",
      "test -f /chronoracer/render_server_properties.py",
      "test -f /chronoracer/kafka.service",
      "test -f /chronoracer/names.json",
      "test -f /chronoracer/platform.json",
      "test ! -e /chronoracer/default-config.yaml",
      "test ! -d /chronoracer/influxdb-plugins",
      "test ! -d /chronoracer/redpanda",
    ]
    execute_command = "chmod +x {{ .Path }}; sudo bash '{{ .Path }}'"
  }
}
