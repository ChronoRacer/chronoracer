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

  source_ranges = [var.subnet_cidr]
  target_tags   = ["kafka"]
}

resource "google_dns_managed_zone" "chronoracer" {
  name        = local.names.dns_zone
  project     = var.project_id
  dns_name    = var.dns_zone_dns_name
  description = "Always-on Kafka hostname zone. Race stack owns the A record."
  visibility  = "public"

  depends_on = [google_project_service.apis]
}
