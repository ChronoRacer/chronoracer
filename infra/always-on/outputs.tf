output "vpc_name" {
  value = google_compute_network.chronoracer.name
}

output "subnet_name" {
  value = google_compute_subnetwork.chronoracer.name
}

output "dns_zone_name" {
  value = google_dns_managed_zone.chronoracer.name
}

output "dns_zone_name_servers" {
  value = google_dns_managed_zone.chronoracer.name_servers
}

output "kafka_fqdn" {
  value = var.kafka_fqdn
}

output "kafka_hostname" {
  value = local.kafka_hostname
}

output "kafka_private_bootstrap" {
  value = local.kafka_private_bootstrap
}

output "kafka_public_port" {
  value = local.kafka_public_port
}

output "kafka_sa_email" {
  value = google_service_account.kafka.email
}

output "core_sa_email" {
  value = google_service_account.core.email
}

output "controller_sa_email" {
  value = google_service_account.controller.email
}

output "relay_sa_email" {
  value = google_service_account.relay.email
}

output "tls_secret_id" {
  value = google_secret_manager_secret.kafka_tls.secret_id
}

output "race_bucket" {
  value = google_storage_bucket.race.name
}

output "artifacts_bucket" {
  value = google_storage_bucket.artifacts.name
}

output "relay_uri" {
  value = google_cloud_run_v2_service.relay.uri
}

output "cloud_build_trigger_id" {
  value = google_cloudbuild_trigger.race_controller.trigger_id
}

output "cloud_build_trigger_name" {
  value = google_cloudbuild_trigger.race_controller.name
}

output "cloud_build_location" {
  value       = google_cloudbuild_trigger.race_controller.location
  description = "Cloud Build region for the race-controller trigger. Same as var.region."
}

output "race_public_kafka_firewall_name" {
  value       = local.names.public_fw
  description = "Race stack must use this firewall name. Controller IAM is not name-conditioned on create."
}

output "artifact_registry_repository" {
  value = google_artifact_registry_repository.chronoracer.id
}
