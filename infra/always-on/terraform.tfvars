project_id        = "chronoracer"
region            = "europe-west1"
zone              = "europe-west1-b"
dns_zone_dns_name = "chronoracer.live."
kafka_fqdn        = "ingest.chronoracer.live."
# Match CONTROL_PLANE_IMAGE_TAG (default latest). Publish first: docs/operator-control-plane-images.md
relay_image      = "europe-west1-docker.pkg.dev/chronoracer/chronoracer/relay:latest"
controller_image = "europe-west1-docker.pkg.dev/chronoracer/chronoracer/controller:latest"
dashboard_image  = "europe-west1-docker.pkg.dev/chronoracer/chronoracer/dashboard:latest"
iap_ssh_members  = ["user:ignacy.smolinski@gmail.com"]
