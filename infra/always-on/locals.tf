locals {
  names    = jsondecode(file("${path.module}/../contract/names.json"))
  platform = jsondecode(file("${path.module}/../contract/platform.json"))

  iap_ssh_source_ranges = ["35.235.240.0/20"]

  kafka_private_port = local.platform.kafka_private_port
  kafka_public_port  = local.platform.kafka_public_port
  influx_http_port   = local.platform.influx_http_port
  cloud_build_topic  = local.platform.cloud_build_topic

  kafka_private_bootstrap = "${local.names.kafka_vm}.${var.zone}.c.${var.project_id}.internal:${local.kafka_private_port}"
  kafka_hostname          = trimsuffix(var.kafka_fqdn, ".")
  dns_zone_hostname       = trimsuffix(var.dns_zone_dns_name, ".")
  relay_hostname          = "relay.${local.dns_zone_hostname}"
  dashboard_hostname      = "dashboard.${local.dns_zone_hostname}"

  # Core VM is private-only and race-scoped, but its name and zone are stable, so the
  # Dashboard datasource URL never changes between races.
  influx_private_url = "http://${local.names.core_vm}.${var.zone}.c.${var.project_id}.internal:${local.influx_http_port}"

  build_finished_audience = "chronoracer-relay-build-finished"
  export_done_audience    = "chronoracer-relay-export-done"

  # Custom domain, not the generated run.app URL. Grafana needs root_url before OAuth can work.
  # Hostnames are derived from the DNS zone; mapping resources: domain_mappings.tf
  relay_uri     = "https://${local.relay_hostname}"
  dashboard_uri = "https://${local.dashboard_hostname}"

  # One Auth0 tenant, one application: see docs/component-okta.md. The kafka_oauth_* keys hold
  # the tenant-wide issuer
  oauth_issuer    = local.platform.kafka_oauth_issuer
  oauth_auth_url  = "${local.oauth_issuer}authorize"
  oauth_token_url = "${local.oauth_issuer}oauth/token"
  oauth_api_url   = "${local.oauth_issuer}userinfo"

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
