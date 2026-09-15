locals {
  names    = jsondecode(file("${path.module}/../contract/names.json"))
  platform = jsondecode(file("${path.module}/../contract/platform.json"))

  iap_ssh_source_ranges = ["35.235.240.0/20"]

  kafka_private_port = local.platform.kafka_private_port
  kafka_public_port  = local.platform.kafka_public_port
  cloud_build_topic  = local.platform.cloud_build_topic

  kafka_private_bootstrap = "${local.names.kafka_vm}.${var.zone}.c.${var.project_id}.internal:${local.kafka_private_port}"
  kafka_hostname          = trimsuffix(var.kafka_fqdn, ".")

  build_finished_audience = "chronoracer-relay-build-finished"
  export_done_audience    = "chronoracer-relay-export-done"

  apis = [
    "compute.googleapis.com",
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "firestore.googleapis.com",
    "secretmanager.googleapis.com",
    "dns.googleapis.com",
    "pubsub.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "storage.googleapis.com",
    "iap.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "logging.googleapis.com",
  ]
}
