resource "google_pubsub_topic" "cloud_builds" {
  name    = local.cloud_build_topic
  project = var.project_id

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic" "export_done" {
  name    = local.platform.export_done_topic
  project = var.project_id

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "relay_config" {
  secret_id = local.names.relay_config_secret
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "relay_config" {
  secret = google_secret_manager_secret.relay_config.id
  secret_data = templatefile("${path.module}/relay-config.yaml.tftpl", {
    project_id                     = var.project_id
    export_bucket                  = google_storage_bucket.race.name
    cloud_build_trigger_id         = google_cloudbuild_trigger.race_controller.trigger_id
    cloud_build_location           = var.region
    build_finished_audience        = local.build_finished_audience
    build_finished_service_account = google_service_account.build_finished_push.email
    export_done_audience           = local.export_done_audience
    export_done_service_account    = google_service_account.export_done_push.email
    kafka_private_bootstrap        = local.kafka_private_bootstrap
    kafka_public_port              = local.kafka_public_port
    kafka_control_topic            = local.platform.kafka_control_topic
    kafka_heartbeat_topic          = local.platform.kafka_heartbeat_topic
    cloud_build_topic              = local.cloud_build_topic
    export_done_topic              = local.platform.export_done_topic
  })
}

resource "google_cloud_run_v2_service" "relay" {
  name     = local.names.relay_service
  project  = var.project_id
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.relay.email

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    vpc_access {
      connector = google_vpc_access_connector.vpc_relay.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = var.relay_image

      ports {
        container_port = 8080
      }

      env {
        name  = "RELAY_CONFIG_FILE_PATH"
        value = "/var/relay/config.yaml"
      }

      volume_mounts {
        name       = "relay-config"
        mount_path = "/var/relay"
      }
    }

    volumes {
      name = "relay-config"
      secret {
        secret = google_secret_manager_secret.relay_config.secret_id
        items {
          version = "latest"
          path    = "config.yaml"
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_version.relay_config,
    google_project_service.apis,
    google_project_service_identity.this["run.googleapis.com"],
    google_secret_manager_secret_iam_member.run_agent_relay_config,
    google_secret_manager_secret_iam_member.relay_config,
    google_artifact_registry_repository_iam_member.run_agent_reader,
    google_artifact_registry_repository_iam_member.relay_reader,
    google_project_iam_member.relay_vpcaccess,
    google_project_iam_member.run_agent_vpcaccess,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  project  = var.project_id
  location = google_cloud_run_v2_service.relay.location
  name     = google_cloud_run_v2_service.relay.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_pubsub_subscription" "relay_push" {
  for_each = {
    build_finished = {
      name     = "relay-build-finished"
      topic    = google_pubsub_topic.cloud_builds.id
      path     = "/internal/pubsub/build-finished"
      sa_email = google_service_account.build_finished_push.email
      audience = local.build_finished_audience
    }
    export_done = {
      name     = "relay-export-done"
      topic    = google_pubsub_topic.export_done.id
      path     = "/internal/pubsub/export-done"
      sa_email = google_service_account.export_done_push.email
      audience = local.export_done_audience
    }
  }

  name    = each.value.name
  project = var.project_id
  topic   = each.value.topic

  ack_deadline_seconds = 60

  push_config {
    push_endpoint = "${google_cloud_run_v2_service.relay.uri}${each.value.path}"

    oidc_token {
      service_account_email = each.value.sa_email
      audience              = each.value.audience
    }
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  depends_on = [
    google_project_service_identity.this["pubsub.googleapis.com"],
    google_service_account_iam_member.pubsub_uses_build_push,
    google_service_account_iam_member.pubsub_uses_export_push,
    google_cloud_run_v2_service_iam_member.push_invoker,
  ]
}

resource "google_cloudbuild_trigger" "race_controller" {
  name        = local.names.trigger
  project     = var.project_id
  location    = var.region
  description = "Relay-invoked race Terraform in var.region. Substitutions _RACE_ID _ACTION _RUN_TOKEN. Env PROJECT_ID KAFKA_FQDN ZONE from always-on."

  service_account = google_service_account.controller.id

  substitutions = {
    _RACE_ID   = "unset"
    _ACTION    = "unset"
    _RUN_TOKEN = "unset"
  }

  build {
    timeout = "1800s"

    options {
      logging = "CLOUD_LOGGING_ONLY"
    }

    step {
      name = var.controller_image
      env = [
        "RACE_ID=$_RACE_ID",
        "ACTION=$_ACTION",
        "RUN_TOKEN=$_RUN_TOKEN",
        "PROJECT_ID=${var.project_id}",
        "KAFKA_FQDN=${var.kafka_fqdn}",
        "ZONE=${var.zone}",
      ]
    }
  }

  depends_on = [
    google_project_iam_member.controller_logging,
    google_service_account_iam_member.controller_self,
    google_service_account_iam_member.cloudbuild_agent_uses_controller,
    google_project_service.apis,
    google_project_service_identity.this["cloudbuild.googleapis.com"],
    google_artifact_registry_repository_iam_member.controller_reader,
    google_artifact_registry_repository_iam_member.cloudbuild_agent_reader,
  ]
}
