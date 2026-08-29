# State bucket lives outside always-on and race state (docs: bootstrap exception).
# Apply this root once with local state, then never put this bucket in another stack.

locals {
  names = jsondecode(file("${path.module}/../contract/names.json"))
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_storage_bucket" "state" {
  name                        = local.names.state_bucket
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  depends_on = [google_project_service.storage]
}
