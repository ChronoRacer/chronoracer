locals {
  names    = jsondecode(file("${path.module}/../contract/names.json"))
  platform = jsondecode(file("${path.module}/../contract/platform.json"))

  iap_ssh_source_ranges = ["35.235.240.0/20"]

  kafka_private_port = local.platform.kafka_private_port
  kafka_public_port  = local.platform.kafka_public_port

  kafka_private_bootstrap = "${local.names.kafka_vm}.${var.zone}.c.${var.project_id}.internal:${local.kafka_private_port}"
  kafka_hostname          = trimsuffix(var.kafka_fqdn, ".")

  # HashiCorp Terraform has no cidrcontains (OpenTofu does). Overlap via IPv4 integers.
  cidr_ipv4 = {
    for key, ip in {
      subnet_first = cidrhost(var.subnet_cidr, 0)
      subnet_last  = cidrhost(var.subnet_cidr, -1)
      conn_first   = cidrhost(var.vpc_connector_cidr, 0)
      conn_last    = cidrhost(var.vpc_connector_cidr, -1)
      } : key => (
      tonumber(split(".", ip)[0]) * 16777216 +
      tonumber(split(".", ip)[1]) * 65536 +
      tonumber(split(".", ip)[2]) * 256 +
      tonumber(split(".", ip)[3])
    )
  }
  subnet_connector_no_overlap = (
    local.cidr_ipv4.conn_last < local.cidr_ipv4.subnet_first ||
    local.cidr_ipv4.conn_first > local.cidr_ipv4.subnet_last
  )

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
    "vpcaccess.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "storage.googleapis.com",
    "iap.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "logging.googleapis.com",
  ]
}
