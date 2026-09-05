variable "project_id" {
  type        = string
  description = "GCP project id."
}

variable "region" {
  type        = string
  default     = "europe-west3"
}

variable "zone" {
  type        = string
  description = "Compute zone for race VMs (GCE internal DNS uses this). Must be in var.region."
  default     = "europe-west3-a"
}

variable "dns_zone_dns_name" {
  type        = string
  description = "Cloud DNS zone dns_name, trailing dot required. Example: chronoracer.example."

  validation {
    condition     = endswith(var.dns_zone_dns_name, ".")
    error_message = "dns_zone_dns_name must end with a dot."
  }
}

variable "kafka_fqdn" {
  type        = string
  description = "Stable Kafka hostname (trailing dot). Must be a name in the DNS zone. Race stack owns the A record."

  validation {
    condition     = endswith(var.kafka_fqdn, ".") && endswith(var.kafka_fqdn, var.dns_zone_dns_name)
    error_message = "kafka_fqdn must be a hostname in dns_zone_dns_name (both with trailing dots)."
  }
}

variable "relay_image" {
  type        = string
  description = "Relay Cloud Run image (Artifact Registry). Publish before full apply: docs/operator-control-plane-images.md"
}

variable "controller_image" {
  type        = string
  description = "Controller image Cloud Build runs. Publish with Relay before the first race (same runbook)."
}

variable "iap_ssh_members" {
  type        = list(string)
  description = "IAM members granted IAP tunnel to VMs (user: or group:). Empty = no IAP SSH users."
  default     = []
}

variable "subnet_cidr" {
  type        = string
  description = "Race VPC subnet. Kafka/Core VMs land here."
  default     = "10.20.0.0/24"

  validation {
    condition     = can(cidrnetmask(var.subnet_cidr))
    error_message = "subnet_cidr must be a valid CIDR."
  }
}

variable "vpc_connector_cidr" {
  type        = string
  description = "/28 for Serverless VPC Access. Must not overlap subnet_cidr."
  default     = "10.20.1.0/28"

  validation {
    condition     = can(cidrnetmask(var.vpc_connector_cidr)) && endswith(var.vpc_connector_cidr, "/28")
    error_message = "vpc_connector_cidr must be a /28 CIDR."
  }
}
