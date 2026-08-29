locals {
  log_exclusions = {
    compute_firewall = {
      description = "VPC firewall allow/deny hit logs."
      filter      = "log_id(\"compute.googleapis.com/firewall\")"
    }

    vpc_flows = {
      description = "VPC flow logs when enabled on a subnet."
      filter      = "log_id(\"compute.googleapis.com/vpc_flows\")"
    }

    compute_network = {
      description = "Compute network control-plane noise."
      filter      = "log_id(\"compute.googleapis.com/network\")"
    }

    dns_queries = {
      description = "Cloud DNS query logs."
      filter      = "log_id(\"dns.googleapis.com/query\")"
    }

    gce_guest_agent = {
      description = "GCE guest agent and OS inventory chatter on kafka/core. Not startup-script output."
      filter      = <<-EOT
        resource.type="gce_instance"
        (
          log_id("OSConfigAgent")
          OR log_id("google-fluentd")
          OR (log_id("GCEGuestAgent") AND severity<=INFO)
        )
      EOT
    }

    vpc_access_connector = {
      description = "Serverless VPC Access connector operational logs."
      filter      = "resource.type=\"vpc_access_connector\""
    }

    gcp_health_checks = {
      description = "Internal Google health-check and probe HTTP noise."
      filter      = <<-EOT
        httpRequest.userAgent=~"GoogleHC|Google-Health-Check|GoogleHealthCheck|Google-Cloud-Run-Health-Checks"
        OR (
          httpRequest.requestUrl=~"(?i)(/healthz|/readyz|/health)(\\?|$)"
          AND httpRequest.status<400
        )
      EOT
    }
  }
}

resource "google_logging_project_exclusion" "default_sink" {
  for_each = local.log_exclusions

  project     = var.project_id
  name        = "chronoracer-${replace(each.key, "_", "-")}"
  description = each.value.description
  filter      = trimspace(each.value.filter)

  depends_on = [google_project_service.apis]
}
