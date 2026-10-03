source "googlecompute" "analysis" {
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
  image_name              = "${local.names.analysis_family}-{{timestamp}}"
  image_family            = local.names.analysis_family
  image_storage_locations = [local.image_storage_location]
  image_description       = "Chronoracer Analysis infrastructure base. Debian 13, startup overlay, and Ops Agent."
  tags                    = ["packer"]
}

build {
  name    = "chronoracer-analysis"
  sources = ["source.googlecompute.analysis"]

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "scripts/install-common.sh"
  }

  provisioner "shell" {
    execute_command  = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    environment_vars = ["OPS_AGENT_VERSION=${var.ops_agent_version}"]
    script           = "scripts/install-ops-agent.sh"
  }

  provisioner "shell" {
    inline = ["mkdir -p /tmp/overlay /tmp/contract"]
  }

  provisioner "file" {
    sources = [
      "../../analysis/startup/entrypoint.sh",
      "../../analysis/startup/ops-agent.yaml",
      "scripts/gce-metadata.sh",
      "scripts/enable-ops-agent.sh",
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
    execute_command = "chmod +x {{ .Path }}; sudo bash '{{ .Path }}'"
    inline = [
      "command -v gcloud",
      "command -v python3",
      "command -v jq",
      "command -v curl",
      "test -x /chronoracer/entrypoint.sh",
      "test -f /chronoracer/gce-metadata.sh",
      "test -f /chronoracer/ops-agent.yaml",
      "grep -Fq 'exclude_process_metrics' /chronoracer/ops-agent.yaml",
      "test -x /chronoracer/enable-ops-agent.sh",
      "systemctl cat google-cloud-ops-agent.service >/dev/null",
      "test -f /chronoracer/names.json",
      "test -f /chronoracer/platform.json",
    ]
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    script          = "scripts/finalize-image.sh"
  }
}
