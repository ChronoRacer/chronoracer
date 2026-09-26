resource "google_cloud_run_v2_service" "dashboard" {
  name                = local.names.dashboard_service
  project             = var.project_id
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account = google_service_account.dashboard.email

    timeout = "60s"

    execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

    scaling {
      min_instance_count = 0
      # Grafana keeps sessions in instance-local SQLite, so a second instance would split logins.
      max_instance_count = 1
    }

    max_instance_request_concurrency = 20

    containers {
      image = var.dashboard_image

      ports {
        container_port = local.platform.dashboard_container_port
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "GF_SERVER_HTTP_PORT"
        value = tostring(local.platform.dashboard_container_port)
      }

      env {
        name  = "GF_SERVER_ROOT_URL"
        value = local.dashboard_uri
      }

      env {
        name  = "GF_AUTH_GENERIC_OAUTH_CLIENT_ID"
        value = local.platform.oauth_client_id
      }

      env {
        name  = "GF_AUTH_GENERIC_OAUTH_AUTH_URL"
        value = local.oauth_auth_url
      }

      env {
        name  = "GF_AUTH_GENERIC_OAUTH_TOKEN_URL"
        value = local.oauth_token_url
      }

      env {
        name  = "GF_AUTH_GENERIC_OAUTH_API_URL"
        value = local.oauth_api_url
      }

      env {
        name = "GF_AUTH_GENERIC_OAUTH_CLIENT_SECRET"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.okta_oauth.secret_id
            version = "latest"
          }
        }
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    google_project_service_identity.this["run.googleapis.com"],
    google_secret_manager_secret_iam_member.okta_oauth,
    google_secret_manager_secret_iam_member.run_agent_okta_oauth,
    google_project_iam_member.dashboard_monitoring,
    google_artifact_registry_repository_iam_member.run_agent_reader,
    google_artifact_registry_repository_iam_member.dashboard_reader,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "dashboard_public" {
  project  = var.project_id
  location = google_cloud_run_v2_service.dashboard.location
  name     = google_cloud_run_v2_service.dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# GF_SERVER_ROOT_URL is the mapped hostname, not the generated run.app URL. Reading the
# service URI would point Grafana OAuth at the wrong host.
check "dashboard_root_url" {
  assert {
    condition     = "https://${google_cloud_run_domain_mapping.dashboard.name}" == local.dashboard_uri
    error_message = "Domain mapping ${google_cloud_run_domain_mapping.dashboard.name} does not match GF_SERVER_ROOT_URL ${local.dashboard_uri}. Update locals.dashboard_hostname and re-register the Auth0 callback URL."
  }
}
