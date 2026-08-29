variable "project_id" {
  type        = string
  description = "GCP project id. Same project as always-on."
}

variable "region" {
  type        = string
  description = "All resources stay in this region."
  default     = "europe-west3"

  validation {
    condition     = var.region == "europe-west3"
    error_message = "Docs require europe-west3."
  }
}

variable "zone" {
  type        = string
  description = "Compute zone. Must match always-on (GCE internal DNS kafka.{zone}.c.{project}.internal)."
  default     = "europe-west3-a"
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

variable "kafka_machine_type" {
  type        = string
  description = "Kafka VM. Changing mid-race is forbidden."
  default     = "e2-standard-4"
}

variable "core_machine_type" {
  type        = string
  description = "Core VM. Changing mid-race is forbidden."
  default     = "n2-standard-4"
}

variable "kafka_boot_disk_type" {
  type        = string
  description = "Kafka boot disk. Changing mid-race is forbidden."
  default     = "pd-balanced"
}

variable "core_boot_disk_type" {
  type        = string
  description = "Core boot disk. Changing mid-race is forbidden."
  default     = "pd-ssd"
}
