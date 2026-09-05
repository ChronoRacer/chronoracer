variable "project_id" {
  type        = string
  description = "GCP project that owns gs://chronoracer-state."
}

variable "region" {
  type        = string
  description = "Must stay europe-west3."
  default     = "europe-west3"
}
