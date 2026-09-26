resource "google_storage_bucket" "race" {
  name                        = local.names.race_bucket
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "artifacts" {
  name                        = local.names.artifacts_bucket
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "kafka_tls" {
  secret_id = local.names.tls_secret
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# Shared InfluxDB admin token. Core VM boot renders Influx's token file from it. Payload uploaded by operator: docs/operator-influx-token.md
resource "google_secret_manager_secret" "influx_token" {
  secret_id = local.names.influx_token_secret
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# Auth0 client secret for the Dashboard browser login. Payload uploaded by operator:
# docs/operator-dashboard-oauth.md
resource "google_secret_manager_secret" "okta_oauth" {
  secret_id = local.names.okta_oauth_secret
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_firestore_database" "default" {
  project     = var.project_id
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"

  depends_on = [google_project_service.apis]
}

resource "google_artifact_registry_repository" "chronoracer" {
  project                = var.project_id
  location               = var.region
  repository_id          = local.names.ar_repo
  format                 = "DOCKER"
  description            = "Controller, Relay, Dashboard, and Telemetry images. Not Kafka/Core VM families."
  cleanup_policy_dry_run = false

  cleanup_policies {
    id     = "keep-newest-image"
    action = "KEEP"
    most_recent_versions {
      keep_count = 1
    }
  }
  cleanup_policies {
    id     = "delete-older-images"
    action = "DELETE"
    condition {
      tag_state = "ANY"
    }
  }

  depends_on = [google_project_service.apis]
}
