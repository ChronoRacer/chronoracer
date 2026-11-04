resource "google_cloud_run_v2_service" "telemetry" {
  name                = local.names.telemetry_service
  project             = var.project_id
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  labels = {
    race-id = var.race_id
  }

  template {
    service_account = data.google_service_account.telemetry.email

    # Cloud Run max request duration. Browser clients reconnect before this elapses.
    timeout = "3600s"

    execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"

      network_interfaces {
        network    = data.google_compute_network.chronoracer.id
        subnetwork = data.google_compute_subnetwork.telemetry_run.id
      }
    }

    containers {
      image = var.telemetry_image

      ports {
        container_port = local.platform.telemetry_container_port
      }

      startup_probe {
        http_get {
          path = "/healthz"
          port = local.platform.telemetry_container_port
        }
        period_seconds    = 3
        failure_threshold = 10
        timeout_seconds   = 1
      }

      # TODO: [IMPORTANT!] This should be bumped after testing
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      env {
        name  = "CORE_TELEMETRY_URL"
        value = local.core_telemetry_url
      }

      env {
        name  = "RACE_ID"
        value = var.race_id
      }

      env {
        name  = "TELEMETRY_ROOT_URL"
        value = local.telemetry_root_url
      }

      env {
        name  = "KAFKA_PRIVATE_BOOTSTRAP"
        value = local.kafka_private_bootstrap
      }

      env {
        name  = "KAFKA_EVENT_TOPIC"
        value = local.platform.kafka_event_topic
      }

      env {
        name  = "OAUTH_CLIENT_ID"
        value = local.platform.oauth_client_id
      }

      env {
        name  = "OAUTH_ISSUER"
        value = local.platform.kafka_oauth_issuer
      }

      env {
        name = "OAUTH_CLIENT_SECRET"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.okta_oauth.secret_id
            version = "latest"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "google_cloud_run_v2_service_iam_member" "telemetry_public" {
  project  = var.project_id
  location = google_cloud_run_v2_service.telemetry.location
  name     = google_cloud_run_v2_service.telemetry.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
