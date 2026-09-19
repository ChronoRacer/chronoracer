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

resource "google_compute_subnetwork" "dashboard_run" {
  name                     = local.names.dashboard_run_subnet
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.chronoracer.id
  ip_cidr_range            = var.dashboard_run_subnet_cidr
  private_ip_google_access = true
  description              = "Dashboard Direct VPC Egress only. No VMs. Sole Influx source range."
}

check "subnet_cidrs_disjoint" {
  assert {
    condition = (
    # 1. relay_run start/end not inside subnet_cidr
    cidrsubnet(format("%s/%s", cidrhost(var.relay_run_subnet_cidr, 0), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.relay_run_subnet_cidr, -1), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&

    # 2. dashboard_run start/end not inside subnet_cidr
    cidrsubnet(format("%s/%s", cidrhost(var.dashboard_run_subnet_cidr, 0), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&
    cidrsubnet(format("%s/%s", cidrhost(var.dashboard_run_subnet_cidr, -1), split("/", var.subnet_cidr)[1]), 0, 0) != var.subnet_cidr &&

    # 3. relay_run and dashboard_run do not overlap with each other
    cidrsubnet(format("%s/%s", cidrhost(var.dashboard_run_subnet_cidr, 0), split("/", var.relay_run_subnet_cidr)[1]), 0, 0) != var.relay_run_subnet_cidr
    )
    error_message = "subnet_cidr, relay_run_subnet_cidr, and dashboard_run_subnet_cidr must not overlap. Direct VPC Egress needs a /26 each; defaults are 10.20.0.0/24, 10.20.1.0/26, 10.20.1.64/26."
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
  target_tags   = ["kafka", "core"]
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

  source_ranges = [var.subnet_cidr, var.relay_run_subnet_cidr]
  target_tags   = ["kafka"]
}

# Dashboard reads Influx on the Core VM. Source is the Dashboard subnet only, so
# neither the Kafka VM nor Relay can reach the database.
resource "google_compute_firewall" "influx_private" {
  name    = local.names.influx_priv_fw
  project = var.project_id
  network = google_compute_network.chronoracer.name

  direction = "INGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = [tostring(local.influx_http_port)]
  }

  source_ranges = [var.dashboard_run_subnet_cidr]
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
