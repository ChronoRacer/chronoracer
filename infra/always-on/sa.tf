resource "google_service_account" "relay" {
  project      = var.project_id
  account_id   = local.names.relay_sa
  display_name = "Chronoracer Relay"

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "controller" {
  project      = var.project_id
  account_id   = local.names.controller_sa
  display_name = "Chronoracer race Terraform controller"

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "kafka" {
  project      = var.project_id
  account_id   = local.names.kafka_sa
  display_name = "Chronoracer Kafka VM"

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "core" {
  project      = var.project_id
  account_id   = local.names.core_sa
  display_name = "Chronoracer Core VM"

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "build_finished_push" {
  project      = var.project_id
  account_id   = local.names.build_push_sa
  display_name = "Pub/Sub push identity for build-finished"

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "export_done_push" {
  project      = var.project_id
  account_id   = local.names.export_push_sa
  display_name = "Pub/Sub push identity for export-done"

  depends_on = [google_project_service.apis]
}
