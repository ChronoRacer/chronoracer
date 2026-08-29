# Google-managed service agents. Creating them before IAM/bindings avoids first-apply
# "service account does not exist" on a fresh project (API enable is not enough).
locals {
  service_identity_apis = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "vpcaccess.googleapis.com",
  ])

  cloudbuild_agent = "serviceAccount:${google_project_service_identity.this["cloudbuild.googleapis.com"].email}"
  pubsub_agent     = "serviceAccount:${google_project_service_identity.this["pubsub.googleapis.com"].email}"
  run_agent        = "serviceAccount:${google_project_service_identity.this["run.googleapis.com"].email}"
}

resource "google_project_service_identity" "this" {
  for_each = local.service_identity_apis

  provider = google-beta
  project  = var.project_id
  service  = each.value

  depends_on = [google_project_service.apis]
}
