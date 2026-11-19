terraform {
  required_version = ">= 1.16.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.40.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6.0"
    }
  }

  backend "gcs" {
    # Must match infra/contract/names.json state_bucket. Backend blocks cannot use variables.
    bucket = "chronoracer-state"
    prefix = "race"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
