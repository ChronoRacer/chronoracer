# Vendor pins for Packer builders. Bump here, then rebuild the matching family.
# InfluxDB 3: Debian package from InfluxData. Empty version = latest at build;
# set influxdb3_package_version after `apt-cache madison influxdb3-core`.

variable "debian_source_image_family" {
  type    = string
  default = "debian-12"
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
  default = "3.9.1"
}

variable "influxdb3_package" {
  type    = string
  default = "influxdb3-core"
}

variable "influxdb3_package_version" {
  type        = string
  default     = ""
  description = "Debian package version (e.g. 3.11.2-1). Empty installs current InfluxData repo version."
}

variable "yq_version" {
  type    = string
  default = "v4.45.4"
}

variable "rpk_version" {
  type    = string
  default = "v26.2.2"
}
