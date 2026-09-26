data "google_compute_network" "chronoracer" {
  name    = local.names.vpc
  project = var.project_id
}

data "google_compute_subnetwork" "chronoracer" {
  name    = local.names.subnet
  project = var.project_id
  region  = var.region
}

data "google_compute_subnetwork" "telemetry_run" {
  name    = local.names.telemetry_run_subnet
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {
  project_id = var.project_id
}

data "google_compute_zones" "available" {
  project = var.project_id
  region  = var.region
  status  = "UP"
}

data "google_dns_managed_zone" "chronoracer" {
  name    = local.names.dns_zone
  project = var.project_id
}

data "google_service_account" "kafka" {
  account_id = local.names.kafka_sa
  project    = var.project_id
}

data "google_service_account" "core" {
  account_id = local.names.core_sa
  project    = var.project_id
}

data "google_service_account" "telemetry" {
  account_id = local.names.telemetry_sa
  project    = var.project_id
}

data "google_secret_manager_secret" "okta_oauth" {
  secret_id = local.names.okta_oauth_secret
  project   = var.project_id
}

data "google_secret_manager_secret" "kafka_tls" {
  secret_id = local.names.tls_secret
  project   = var.project_id
}

data "google_storage_bucket" "race" {
  name = local.names.race_bucket
}

# Family into concrete image id at first apply. ignore_changes = all on VMs so a moved family is not a replace.
data "google_compute_image" "kafka" {
  family  = local.names.kafka_family
  project = var.project_id
}

data "google_compute_image" "core" {
  family  = local.names.core_family
  project = var.project_id
}
