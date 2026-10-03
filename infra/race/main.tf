resource "google_compute_instance" "kafka" {
  name         = local.names.kafka_vm
  project      = var.project_id
  zone         = var.zone
  machine_type = var.kafka_machine_type

  tags = ["kafka"]

  labels = {
    race-id = var.race_id
  }

  boot_disk {
    initialize_params {
      image = data.google_compute_image.kafka.id
      size  = var.kafka_boot_disk_size_gb
      type  = var.kafka_boot_disk_type
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.chronoracer.self_link

    access_config {
      # Ephemeral public IP. No reserved address.
    }
  }

  metadata = {
    tls-secret-id  = data.google_secret_manager_secret.kafka_tls.id
    kafka-fqdn     = trimsuffix(var.kafka_fqdn, ".")
    startup-script = local.overlay_startup
  }

  service_account {
    email  = data.google_service_account.kafka.email
    scopes = ["cloud-platform"]
  }

  scheduling {
    preemptible = false
  }

  lifecycle {
    ignore_changes = all
    precondition {
      condition     = contains(data.google_compute_zones.available.names, var.zone)
      error_message = "var.zone must be an UP Compute zone in var.region."
    }
    precondition {
      condition     = endswith(var.kafka_fqdn, data.google_dns_managed_zone.chronoracer.dns_name)
      error_message = "kafka_fqdn must be a hostname in always-on zone chronoracer (both with trailing dots)."
    }
  }
}

resource "google_compute_instance" "core" {
  name         = local.names.core_vm
  project      = var.project_id
  zone         = var.zone
  machine_type = var.core_machine_type

  tags = ["core"]

  labels = {
    race-id = var.race_id
  }

  boot_disk {
    initialize_params {
      image = data.google_compute_image.core.id
      size  = var.core_boot_disk_size_gb
      type  = var.core_boot_disk_type
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.chronoracer.self_link
  }

  metadata = {
    core-config-gcs-uri = local.core_config_gcs_uri
    race-id             = var.race_id
    startup-script      = local.overlay_startup
  }

  service_account {
    email  = data.google_service_account.core.email
    scopes = ["cloud-platform"]
  }

  scheduling {
    preemptible = false
  }

  lifecycle {
    ignore_changes = all
    precondition {
      condition     = contains(data.google_compute_zones.available.names, var.zone)
      error_message = "var.zone must be an UP Compute zone in var.region."
    }
  }
}

resource "google_compute_instance" "analysis" {
  name         = local.names.analysis_vm
  project      = var.project_id
  zone         = var.zone
  machine_type = var.analysis_machine_type

  tags = ["analysis"]

  labels = {
    race-id = var.race_id
  }

  boot_disk {
    auto_delete = true

    initialize_params {
      image = data.google_compute_image.analysis.id
      size  = var.analysis_boot_disk_size_gb
      type  = var.analysis_boot_disk_type
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.chronoracer.self_link
  }

  metadata = {
    race-id            = var.race_id
    core-telemetry-url = local.core_telemetry_url
    telemetry-url      = local.telemetry_root_url
    startup-script     = local.overlay_startup
  }

  service_account {
    email  = data.google_service_account.analysis.email
    scopes = ["cloud-platform"]
  }

  scheduling {
    preemptible = false
  }

  lifecycle {
    ignore_changes = all
    precondition {
      condition     = contains(data.google_compute_zones.available.names, var.zone)
      error_message = "var.zone must be an UP Compute zone in var.region."
    }
  }
}

resource "google_compute_firewall" "kafka_public" {
  count = var.ingest_open ? 1 : 0

  name    = local.names.public_fw
  project = var.project_id
  network = data.google_compute_network.chronoracer.name

  description = "Public Kafka ingest. Race stack only. Absent when ingest_open=false."
  direction   = "INGRESS"
  priority    = 1000

  allow {
    protocol = "tcp"
    ports    = [tostring(local.kafka_public_port)]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka"]
}

resource "google_dns_record_set" "kafka" {
  project      = var.project_id
  managed_zone = data.google_dns_managed_zone.chronoracer.name
  name         = var.kafka_fqdn
  type         = "A"
  ttl          = 30
  rrdatas      = [google_compute_instance.kafka.network_interface[0].access_config[0].nat_ip]
}
