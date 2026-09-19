# This is onyl available in selected regions (europe-west3 does support it)

resource "google_cloud_run_domain_mapping" "relay" {
  location = var.region
  name     = local.relay_hostname
  project  = var.project_id

  metadata {
    namespace = var.project_id
  }

  spec {
    route_name       = google_cloud_run_v2_service.relay.name
    certificate_mode = "AUTOMATIC"
  }

  depends_on = [
    google_cloud_run_v2_service.relay,
    google_dns_managed_zone.chronoracer,
    google_project_service.apis,
  ]
}

resource "google_cloud_run_domain_mapping" "dashboard" {
  location = var.region
  name     = local.dashboard_hostname
  project  = var.project_id

  metadata {
    namespace = var.project_id
  }

  spec {
    route_name       = google_cloud_run_v2_service.dashboard.name
    certificate_mode = "AUTOMATIC"
  }

  depends_on = [
    google_cloud_run_v2_service.dashboard,
    google_dns_managed_zone.chronoracer,
    google_project_service.apis,
  ]
}

resource "google_dns_record_set" "relay_run" {
  project      = var.project_id
  managed_zone = google_dns_managed_zone.chronoracer.name
  name         = "${local.relay_hostname}."
  type         = "CNAME"
  ttl          = 300
  rrdatas      = ["ghs.googlehosted.com."]
}

resource "google_dns_record_set" "dashboard_run" {
  project      = var.project_id
  managed_zone = google_dns_managed_zone.chronoracer.name
  name         = "${local.dashboard_hostname}."
  type         = "CNAME"
  ttl          = 300
  rrdatas      = ["ghs.googlehosted.com."]
}
