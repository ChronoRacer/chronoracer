packer {
  required_plugins {
    googlecompute = {
      source  = "github.com/hashicorp/googlecompute"
      version = "~> 1.2"
    }
  }
}

locals {
  names                  = jsondecode(file("${path.root}/../contract/names.json"))
  image_storage_location = join("-", slice(split("-", var.zone), 0, 2))
}

variable "project_id" {
  type        = string
  description = "GCP project that stores image families from infra/contract/names.json."
}

variable "zone" {
  type        = string
  default     = "europe-west1-b"
  description = "Builder zone. The custom image is stored in this zone's region."
}

variable "builder_machine_type" {
  type    = string
  default = "e2-small"
}

variable "builder_disk_size_gb" {
  type    = number
  default = 20
}

variable "network" {
  type        = string
  default     = "default"
  description = "Must have internet (ephemeral public IP). Do not use the race VPC (no NAT)."
}
