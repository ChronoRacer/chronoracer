variable "project_id" {
  type        = string
  description = "GCP project id. Same project as always-on."
}

variable "region" {
  type        = string
  description = "GCP region. Controller passes always-on var.region (must match Relay subnet and Telemetry Cloud Run location)."
  default     = "europe-west1"
}

variable "zone" {
  type        = string
  description = "Compute zone. Must match always-on (GCE internal DNS kafka.{zone}.c.{project}.internal)."
  default     = "europe-west1-b"
}

variable "race_id" {
  type        = string
  description = "raceId. Operator-supplied. GCS Core YAML path and race-id label. 1–12 chars, label-safe. Not a resource name. Not a state prefix."

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9_-]{0,11})$", var.race_id))
    error_message = "race_id must be 1–12 chars, GCP label-safe: lowercase letters, digits, underscore, dash."
  }
}

variable "kafka_fqdn" {
  type        = string
  description = "Stable Kafka hostname (trailing dot). Must sit in always-on zone chronoracer. No fake default."

  validation {
    condition     = endswith(var.kafka_fqdn, ".")
    error_message = "kafka_fqdn must end with a dot."
  }
}

variable "ingest_open" {
  type        = bool
  description = "apply=true (public 9094 present). close_ingest=false (firewall absent). Same state both times."
}

variable "telemetry_image" {
  type        = string
  description = "Telemetry Cloud Run image (Artifact Registry). Publish before race apply: docs/operator-control-plane-images.md"
}

variable "kafka_machine_type" {
  type        = string
  description = "Kafka VM. Changing mid-race is forbidden."
  default     = "n4-standard-2"

  validation {
    condition     = startswith(var.kafka_machine_type, "n4-")
    error_message = "Kafka machine family must be aligned with the documentation"
  }
}

variable "core_machine_type" {
  type        = string
  description = "Core VM. Changing mid-race is forbidden."
  default     = "c4d-highcpu-4"

  validation {
    condition     = startswith(var.core_machine_type, "c4d-")
    error_message = "Core machine family must be aligned with the documentation"
  }
}

variable "kafka_boot_disk_type" {
  type        = string
  description = "Kafka boot disk. Changing mid-race is forbidden."
  default     = "hyperdisk-balanced"
}

variable "kafka_boot_disk_size_gb" {
  type        = number
  description = "Kafka boot disk size. Must be at least 20GB. Changing mid-race is forbidden. Ensure available on VM family"
  default     = 20

  validation {
    condition     = var.kafka_boot_disk_size_gb >= 20
    error_message = "kafka_boot_disk_size_gb must be at least 20 GB"
  }
}

variable "core_boot_disk_type" {
  type        = string
  description = "Core boot disk. Changing mid-race is forbidden. Ensure available on VM family"
  default     = "hyperdisk-balanced"
}

variable "core_boot_disk_size_gb" {
  type        = number
  description = "Core boot disk size. Must be at least the 20 GB. Changing mid-race is forbidden."
  default     = 20

  validation {
    condition     = var.core_boot_disk_size_gb >= 20
    error_message = "core_boot_disk_size_gb must be at least 20 GB"
  }
}

variable "analysis_machine_type" {
  type        = string
  description = "Analysis VM placeholder. Changing mid-race is forbidden."
  default     = "e2-micro"
}

variable "analysis_boot_disk_type" {
  type        = string
  description = "Analysis boot disk. Must be supported by its machine type. Changing mid-race is forbidden."
  default     = "pd-balanced"
}

variable "analysis_boot_disk_size_gb" {
  type        = number
  description = "Analysis boot disk size. Changing mid-race is forbidden."
  default     = 20

  validation {
    condition     = var.analysis_boot_disk_size_gb >= 20
    error_message = "analysis_boot_disk_size_gb must be at least 20 GB."
  }
}
