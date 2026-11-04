resource "google_compute_network" "chronoracer" {
  name                    = local.names.vpc
  project                 = var.project_id
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"

  depends_on = [google_project_service.apis]
}

resource "google_compute_subnetwork" "chronoracer" {
  name                     = local.names.subnet
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.chronoracer.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  lifecycle {
    precondition {
      condition     = contains(data.google_compute_zones.available.names, var.zone)
      error_message = "var.zone must be an UP Compute zone in var.region"
    }
  }
}

resource "google_compute_subnetwork" "relay_run" {
  name                     = local.names.relay_run_subnet
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.chronoracer.id
  ip_cidr_range            = var.relay_run_subnet_cidr
  private_ip_google_access = true
  description              = "Relay Direct VPC Egress only. No VMs. No Influx."
}

resource "google_compute_subnetwork" "telemetry_run" {
  name                     = local.names.telemetry_run_subnet
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.chronoracer.id
  ip_cidr_range            = var.telemetry_run_subnet_cidr
  private_ip_google_access = true
  description              = "Telemetry Direct VPC Egress only. No VMs. Core telemetry port from this range."
}

locals {
  subnet_cidrs_disjoint = (
    cidrsubnet(format("%s/%s", cidrhost(var.relay_run_subnet_cidr, 0), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.relay_run_subnet_cidr, -1), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.telemetry_run_subnet_cidr, 0), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.telemetry_run_subnet_cidr, -1), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.telemetry_run_subnet_cidr, 0), split("/", var.relay_run_subnet_cidr)[1]), 0, 0) != var.relay_run_subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.telemetry_run_subnet_cidr, -1), split("/", var.relay_run_subnet_cidr)[1]), 0, 0) != var.relay_run_subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.relay_run_subnet_cidr, 0), split("/", var.telemetry_run_subnet_cidr)[1]), 0, 0) != var.telemetry_run_subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.relay_run_subnet_cidr, -1), split("/", var.telemetry_run_subnet_cidr)[1]), 0, 0) != var.telemetry_run_subnet_cidr
  )
}

check "subnet_cidrs_disjoint" {
  assert {
    condition     = local.subnet_cidrs_disjoint
    error_message = "subnet_cidr, relay_run_subnet_cidr, and telemetry_run_subnet_cidr must not overlap. Direct VPC Egress needs a /26; defaults are 10.20.0.0/24, 10.20.1.0/26, and 10.20.2.0/26."
  }
}

resource "google_compute_firewall" "iap_ssh" {
  name    = local.names.iap_fw
  project = var.project_id
  network = google_compute_network.chronoracer.name

  direction = "INGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = local.iap_ssh_source_ranges
  target_tags   = ["kafka", "core", "analysis"]
}

resource "google_compute_firewall" "kafka_private" {
  name    = local.names.kafka_priv_fw
  project = var.project_id
  network = google_compute_network.chronoracer.name

  direction = "INGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = [tostring(local.kafka_private_port)]
  }

  source_ranges = [var.subnet_cidr, var.relay_run_subnet_cidr, var.telemetry_run_subnet_cidr]
  target_tags   = ["kafka"]
}

resource "google_compute_firewall" "core_telemetry" {
  name    = local.names.core_telemetry_fw
  project = var.project_id
  network = google_compute_network.chronoracer.name

  description = "Core telemetry gRPC. Telemetry Direct VPC subnet only. Not Relay, not Kafka."
  direction   = "INGRESS"
  priority    = 1000

  allow {
    protocol = "tcp"
    ports    = [tostring(local.platform.core_telemetry_port)]
  }

  source_ranges = [var.telemetry_run_subnet_cidr]
  target_tags   = ["core"]
}

resource "google_dns_managed_zone" "chronoracer" {
  name        = local.names.dns_zone
  project     = var.project_id
  dns_name    = var.dns_zone_dns_name
  description = "Public DNS for Kafka ingest (race A record) and Cloud Run hostnames (always-on CNAMEs)."
  visibility  = "public"

  depends_on = [google_project_service.apis]
}

resource "google_compute_firewall" "analysis_core" {
  name    = local.names.analysis_core_fw
  project = var.project_id
  network = google_compute_network.chronoracer.name

  description = "Analysis to Core telemetry gRPC on the private VM subnet."
  direction   = "INGRESS"
  priority    = 1000

  allow {
    protocol = "tcp"
    ports    = [tostring(local.platform.core_telemetry_port)]
  }

  source_tags = ["analysis"]
  target_tags = ["core"]
}

# Needed so Analysis can access Telemetry
resource "google_dns_managed_zone" "run_private" {
  name        = local.names.run_private_dns_zone
  project     = var.project_id
  dns_name    = "run.app."
  description = "Cloud Run access from the race VPC through Private Google Access."
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.chronoracer.id
    }
  }
}

resource "google_dns_record_set" "run_private" {
  project      = var.project_id
  managed_zone = google_dns_managed_zone.run_private.name
  name         = "run.app."
  type         = "A"
  ttl          = 300
  # private.googleapis.com addresses
  rrdatas      = ["199.36.153.8", "199.36.153.9", "199.36.153.10", "199.36.153.11"]
}

# This maps Cloud Run services to the internal DNS zone
resource "google_dns_record_set" "run_private_wildcard" {
  project      = var.project_id
  managed_zone = google_dns_managed_zone.run_private.name
  name         = "*.run.app."
  type         = "CNAME"
  ttl          = 300
  rrdatas      = [google_dns_record_set.run_private.name]
}
