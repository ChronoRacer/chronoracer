# Vendor pins for Packer builders. Bump here, then rebuild the matching family.

variable "debian_source_image_family" {
  type    = string
  default = "debian-13"
}

variable "debian_source_image_project" {
  type    = string
  default = "debian-cloud"
}

variable "kafka_scala_binary" {
  type    = string
  default = "2.13"
}

variable "kafka_version" {
  type    = string
  default = "4.3.1"
}

variable "influxdb3_package_version" {
  type        = string
  default     = "3.11.4-1"
  description = "Debian package version. Empty installs current InfluxData repo version."
}

variable "yq_version" {
  type    = string
  default = "v4.45.4"
}

variable "rpk_version" {
  type    = string
  default = "v26.2.2"
}
