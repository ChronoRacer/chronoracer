output "kafka_instance" {
  value       = google_compute_instance.kafka.id
  description = "Kafka instance identity for Firestore after apply."
}

output "core_instance" {
  value       = google_compute_instance.core.id
  description = "Core instance identity for Firestore after apply."
}

output "kafka_public_ip" {
  value       = google_compute_instance.kafka.network_interface[0].access_config[0].nat_ip
  description = "Ephemeral Kafka public IP."
}

output "kafka_hostname" {
  value       = local.kafka_hostname
  description = "Stable Kafka hostname (no trailing dot). Copied to Firestore on apply."
}

output "kafka_fqdn" {
  value = var.kafka_fqdn
}

output "race_id" {
  value = var.race_id
}

output "ingest_open" {
  value = var.ingest_open
}

output "public_firewall_present" {
  value       = length(google_compute_firewall.kafka_public) == 1
  description = "True when ingest_open created chronoracer-kafka-public."
}

output "telemetry_url" {
  value       = google_cloud_run_v2_service.telemetry.uri
  description = "Generated Telemetry Cloud Run URI. Controller copies this to Firestore as telemetryUrl."
}
