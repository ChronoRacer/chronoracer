data "google_project" "this" {
  project_id = var.project_id
}

# Fail plan if bootstrap did not create the state bucket.
data "google_storage_bucket" "state" {
  name = local.names.state_bucket

  depends_on = [google_project_service.apis]
}

# Fail plan if var.zone is not an UP zone in var.region.
data "google_compute_zones" "available" {
  project = var.project_id
  region  = var.region
  status  = "UP"

  depends_on = [google_project_service.apis]
}
