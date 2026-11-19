resource "google_project_iam_member" "controller_compute" {
  project = var.project_id
  role    = google_project_iam_custom_role.controller_compute.id
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_firewall" {
  project = var.project_id
  role    = google_project_iam_custom_role.controller_firewall.id
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_dns" {
  project = var.project_id
  role    = google_project_iam_custom_role.controller_dns.id
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_secret_admin" {
  project = var.project_id
  role    = "roles/secretmanager.admin"
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_version_adder" {
  project = var.project_id
  role    = "roles/secretmanager.secretVersionAdder"
  member  = "serviceAccount:${google_service_account.controller.email}"

  condition {
    title       = "restrict_to_analysis_token_secret"
    description = "Allows adding secret versions only to secrets matching r-analysis-ingest-*"
    expression  = "resource.name.startsWith(\"projects/${var.project_id}/secrets/r-analysis-ingest-\")"
  }
}

resource "google_project_iam_member" "controller_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_sa_viewer" {
  project = var.project_id
  role    = "roles/iam.serviceAccountViewer"
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_artifact_registry_repository_iam_member" "controller_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.controller.email}"
}

# Control-plane image publish via `gcloud builds submit` (legacy + Cloud Build agent).
resource "google_artifact_registry_repository_iam_member" "cloudbuild_push" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${data.google_project.this.number}@cloudbuild.gserviceaccount.com"
}

resource "google_artifact_registry_repository_iam_member" "cloudbuild_agent_push" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.writer"
  member     = local.cloudbuild_agent
}

resource "google_service_account_iam_member" "controller_as_kafka" {
  service_account_id = google_service_account.kafka.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_service_account_iam_member" "controller_as_core" {
  service_account_id = google_service_account.core.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_service_account_iam_member" "controller_as_telemetry" {
  service_account_id = google_service_account.telemetry.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_run_create" {
  project = var.project_id
  role    = google_project_iam_custom_role.controller_run_create.id
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "controller_run" {
  project = var.project_id
  role    = google_project_iam_custom_role.controller_run.id
  member  = "serviceAccount:${google_service_account.controller.email}"

  # TODO: this condition does not work (does not match the service)
  # condition {
  #   title       = "telemetry-service-only"
  #   description = "Canonical race Telemetry Cloud Run service."
  #   expression  = "resource.name == \"projects/${var.project_id}/locations/${var.region}/services/${local.names.telemetry_service}\""
  # }
}

resource "google_project_iam_member" "controller_race_secrets" {
  project = var.project_id
  role    = google_project_iam_custom_role.controller_race_secrets.id
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_service_account_iam_member" "controller_self" {
  service_account_id = google_service_account.controller.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_service_account_iam_member" "cloudbuild_agent_uses_controller" {
  service_account_id = google_service_account.controller.name
  role               = "roles/iam.serviceAccountUser"
  member             = local.cloudbuild_agent
}

resource "google_service_account_iam_member" "pubsub_uses_build_push" {
  service_account_id = google_service_account.build_finished_push.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = local.pubsub_agent
}

resource "google_service_account_iam_member" "pubsub_uses_export_push" {
  service_account_id = google_service_account.export_done_push.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = local.pubsub_agent
}

resource "google_pubsub_topic_iam_member" "cloudbuild_legacy_publish" {
  project = var.project_id
  topic   = google_pubsub_topic.cloud_builds.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_project.this.number}@cloudbuild.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "cloudbuild_agent_publish" {
  project = var.project_id
  topic   = google_pubsub_topic.cloud_builds.name
  role    = "roles/pubsub.publisher"
  member  = local.cloudbuild_agent
}

resource "google_storage_bucket_iam_member" "controller_yaml_read" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.controller.email}"

  condition {
    title       = "core-config-yaml"
    expression  = "resource.name.endsWith(\"/core-config.yaml\")"
    description = "Read Core YAML only."
  }
}

resource "google_storage_bucket_iam_member" "controller_race_bucket_get" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_storage_bucket_iam_member" "controller_race_state" {
  bucket = data.google_storage_bucket.state.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.controller.email}"

  condition {
    title       = "race-state-prefix"
    expression  = "resource.name.startsWith(\"projects/_/buckets/${data.google_storage_bucket.state.name}/objects/race/\")"
    description = "race/default.tfstate and locks"
  }
}

resource "google_storage_bucket_iam_member" "controller_state_bucket_get" {
  bucket = data.google_storage_bucket.state.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_secret_manager_secret_iam_member" "controller_tls_meta" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.kafka_tls.secret_id
  role      = "roles/secretmanager.viewer"
  member    = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_secret_manager_secret_iam_member" "controller_okta_oauth_meta" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.okta_oauth.secret_id
  role      = "roles/secretmanager.viewer"
  member    = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_project_iam_member" "relay_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_project_iam_member" "relay_builds" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.editor"
  member  = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_service_account_iam_member" "relay_uses_controller" {
  service_account_id = google_service_account.controller.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.relay.email}"
}

# Cloud Run ADC cannot sign GCS URLs with a local key. Relay calls IAM signBlob as itself.
resource "google_service_account_iam_member" "relay_sign_blob" {
  service_account_id = google_service_account.relay.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_storage_bucket_iam_member" "relay_yaml_write" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.relay.email}"

  condition {
    title       = "core-config-yaml"
    expression  = "resource.name.endsWith(\"/core-config.yaml\")"
    description = "Write Core YAML only."
  }
}

resource "google_storage_bucket_iam_member" "relay_export_read" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.relay.email}"

  # condition { # TODO IAM is ..., need to flip the directory structure to have raceId nested instead on top
  #   title       = "export-subtree"
  #   description = "Read export objects only."
  #   expression  = <<-EOT
  #     resource.name.startsWith("projects/_/buckets/${google_storage_bucket.race.name}/objects/") &&
  #     resource.name.contains("/export/")
  #   EOT
  # }
}

resource "google_storage_bucket_iam_member" "relay_race_list" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_secret_manager_secret_iam_member" "kafka_tls" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.kafka_tls.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.kafka.email}"
}

resource "google_secret_manager_secret_iam_member" "core_influx_token" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.influx_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.core.email}"
}

resource "google_storage_bucket_iam_member" "core_yaml_read" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.core.email}"

  condition {
    title       = "core-config-yaml"
    expression  = "resource.name.endsWith(\"/core-config.yaml\")"
    description = "Read Core YAML only."
  }
}

resource "google_storage_bucket_iam_member" "core_artifacts_read" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.core.email}"
}

resource "google_storage_bucket_iam_member" "core_artifacts_list" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.core.email}"
}

resource "google_storage_bucket_iam_member" "analysis_artifacts_read" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.analysis.email}"
}

resource "google_storage_bucket_iam_member" "analysis_artifacts_list" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.analysis.email}"
}

resource "google_storage_bucket_iam_member" "core_export_write" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.core.email}"

  # condition { # TODO IAM is ..., need to flip the directory structure to have raceId nested instead on top
  #   title       = "export-subtree"
  #   description = "Core writes export objects only."
  #   expression  = <<-EOT
  #     resource.name.startsWith("projects/_/buckets/${google_storage_bucket.race.name}/objects/") &&
  #     resource.name.contains("/export/")
  #   EOT
  # }
}

resource "google_storage_bucket_iam_member" "core_race_list" {
  bucket = google_storage_bucket.race.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.core.email}"
}

resource "google_pubsub_topic_iam_member" "core_export_done" {
  project = var.project_id
  topic   = google_pubsub_topic.export_done.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.core.email}"
}

resource "google_artifact_registry_repository_iam_member" "relay_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_secret_manager_secret_iam_member" "relay_config" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.relay_config.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.relay.email}"
}

resource "google_artifact_registry_repository_iam_member" "dashboard_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_secret_manager_secret_iam_member" "okta_oauth" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.okta_oauth.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_secret_manager_secret_iam_member" "car_okta_oauth" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.okta_oauth.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.car.email}"
}

resource "google_artifact_registry_repository_iam_member" "telemetry_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.telemetry.email}"
}

resource "google_secret_manager_secret_iam_member" "telemetry_oauth" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.okta_oauth.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.telemetry.email}"
}

resource "google_secret_manager_secret_iam_member" "run_agent_okta_oauth" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.okta_oauth.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.run_agent
}

resource "google_project_iam_member" "dashboard_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

locals {
  race_vm_sa_emails = {
    kafka    = google_service_account.kafka.email
    core     = google_service_account.core.email
    analysis = google_service_account.analysis.email
  }
}

resource "google_project_iam_member" "vm_ops_agent_metrics" {
  for_each = local.race_vm_sa_emails

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "vm_ops_agent_logs" {
  for_each = local.race_vm_sa_emails

  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "iap_ssh" {
  for_each = toset(var.iap_ssh_members)
  project  = var.project_id
  role     = "roles/iap.tunnelResourceAccessor"
  member   = each.value
}

resource "google_secret_manager_secret_iam_member" "run_agent_relay_config" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.relay_config.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.run_agent
}

resource "google_compute_subnetwork_iam_member" "run_agent_direct_vpc" {
  for_each = {
    relay     = google_compute_subnetwork.relay_run.name
    telemetry = google_compute_subnetwork.telemetry_run.name
  }

  project    = var.project_id
  region     = var.region
  subnetwork = each.value
  role       = "roles/compute.networkUser"
  member     = local.run_agent
}

resource "google_compute_subnetwork_iam_member" "controller_telemetry_direct_vpc" {
  project    = var.project_id
  region     = var.region
  subnetwork = google_compute_subnetwork.telemetry_run.name
  role       = "roles/compute.networkUser"
  member     = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_artifact_registry_repository_iam_member" "run_agent_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.reader"
  member     = local.run_agent
}

resource "google_artifact_registry_repository_iam_member" "cloudbuild_agent_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.chronoracer.name
  role       = "roles/artifactregistry.reader"
  member     = local.cloudbuild_agent
}

resource "google_cloud_run_v2_service_iam_member" "push_invoker" {
  for_each = {
    build_finished = google_service_account.build_finished_push.email
    export_done    = google_service_account.export_done_push.email
  }

  project  = var.project_id
  location = google_cloud_run_v2_service.relay.location
  name     = google_cloud_run_v2_service.relay.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${each.value}"
}

resource "google_service_account_iam_member" "controller_as_analysis" {
  service_account_id = google_service_account.analysis.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.controller.email}"
}
