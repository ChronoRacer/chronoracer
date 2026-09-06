variable "project_id" {
  type        = string
  description = "GCP project that owns gs://chronoracer-state."
}

variable "region" {
  type    = string
  default = "europe-west3"
}
