locals {
  names    = jsondecode(file("${path.module}/../contract/names.json"))
  platform = jsondecode(file("${path.module}/../contract/platform.json"))

  kafka_public_port = local.platform.kafka_public_port
  kafka_hostname    = trimsuffix(var.kafka_fqdn, ".")

  core_config_gcs_uri = "gs://${data.google_storage_bucket.race.name}/${var.race_id}/core-config.yaml"
  core_telemetry_url  = "http://${local.names.core_vm}.${var.zone}.c.${var.project_id}.internal:${local.platform.core_telemetry_port}"
  telemetry_root_url  = "https://${local.names.telemetry_service}-${data.google_project.this.number}.${var.region}.run.app"

  analysis_ingest_secret_id = "chronoracer-${var.race_id}-analysis-ingest"

  # Inline so the controller image (copied *.tf only) does not need extra files at apply.
  overlay_startup = <<-EOT
#!/bin/bash
exec /chronoracer/entrypoint.sh
EOT
}
