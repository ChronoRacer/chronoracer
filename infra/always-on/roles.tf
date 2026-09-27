resource "google_project_iam_custom_role" "controller_compute" {
  project     = var.project_id
  role_id     = "chronoracerControllerCompute"
  title       = "Chronoracer controller compute"
  description = "Race VMs and disks. Not firewall admin. Not project Editor."

  permissions = [
    "compute.disks.create",
    "compute.disks.delete",
    "compute.disks.get",
    "compute.disks.list",
    "compute.disks.resize",
    "compute.disks.setLabels",
    "compute.disks.use",
    "compute.disks.useReadOnly",
    "compute.globalOperations.get",
    "compute.images.get",
    "compute.images.getFromFamily",
    "compute.images.list",
    "compute.images.useReadOnly",
    "compute.instances.attachDisk",
    "compute.instances.create",
    "compute.instances.delete",
    "compute.instances.detachDisk",
    "compute.instances.get",
    "compute.instances.list",
    "compute.instances.reset",
    "compute.instances.setDeletionProtection",
    "compute.instances.setDiskAutoDelete",
    "compute.instances.setLabels",
    "compute.instances.setMachineType",
    "compute.instances.setMetadata",
    "compute.instances.setMinCpuPlatform",
    "compute.instances.setScheduling",
    "compute.instances.setServiceAccount",
    "compute.instances.setTags",
    "compute.instances.start",
    "compute.instances.stop",
    "compute.instances.update",
    "compute.machineTypes.get",
    "compute.machineTypes.list",
    "compute.networks.get",
    "compute.networks.use",
    "compute.networks.useExternalIp",
    "compute.projects.get",
    "compute.regionOperations.get",
    "compute.regions.get",
    "compute.subnetworks.get",
    "compute.subnetworks.list",
    "compute.subnetworks.use",
    "compute.subnetworks.useExternalIp",
    "compute.zoneOperations.get",
    "compute.zones.get",
    "compute.zones.list",
    "iam.serviceAccounts.get",
    "iam.serviceAccounts.list",
    "resourcemanager.projects.get",
  ]
}

resource "google_project_iam_custom_role" "controller_firewall" {
  project     = var.project_id
  role_id     = "chronoracerControllerFirewall"
  title       = "Chronoracer controller public Kafka firewall"
  description = "Firewall mutate. Conditions on create are unreliable; race module must only touch chronoracer-kafka-public."

  permissions = [
    "compute.firewalls.create",
    "compute.firewalls.delete",
    "compute.firewalls.get",
    "compute.firewalls.list",
    "compute.firewalls.update",
    "compute.networks.updatePolicy",
  ]
}

resource "google_project_iam_custom_role" "controller_dns" {
  project     = var.project_id
  role_id     = "chronoracerControllerDns"
  title       = "Chronoracer controller DNS records"
  description = "A records in existing zones. Not managedZones.delete."

  permissions = [
    "dns.changes.create",
    "dns.changes.get",
    "dns.changes.list",
    "dns.managedZones.get",
    "dns.managedZones.list",
    "dns.resourceRecordSets.create",
    "dns.resourceRecordSets.delete",
    "dns.resourceRecordSets.get",
    "dns.resourceRecordSets.list",
    "dns.resourceRecordSets.update",
  ]
}

resource "google_project_iam_custom_role" "controller_run_create" {
  project     = var.project_id
  role_id     = "chronoracerControllerRunCreate"
  title       = "Chronoracer controller Cloud Run create"
  description = "Create Cloud Run services. Conditions do not work well here."

  permissions = [
    "run.locations.get",
    "run.operations.get",
    "run.services.get",
    "run.services.create",
    "run.services.list",
  ]
}

resource "google_project_iam_custom_role" "controller_run" {
  project     = var.project_id
  role_id     = "chronoracerControllerRun"
  title       = "Chronoracer controller Telemetry Cloud Run"
  description = "Mutate the canonical Telemetry Cloud Run service, including public invoker IAM."

  permissions = [
    "run.revisions.get",
    "run.revisions.list",
    "run.services.delete",
    "run.services.get",
    "run.services.getIamPolicy",
    "run.services.setIamPolicy",
    "run.services.update",
  ]
}
