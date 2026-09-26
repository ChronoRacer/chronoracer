# Race stack

Controller-only. Module contract, `ingest_open`, image pin: [Terraform](../../docs/component-terraform.md). Overlay
bake: [Compute images](../../docs/component-images.md).

Do not apply from this README.

```bash
terraform init
terraform fmt
terraform validate
terraform plan -var-file=terraform.tfvars -lock-timeout=5m
```

Required vars: `project_id`, `region`, `race_id`, `kafka_fqdn`, `ingest_open`, and the published Telemetry image URI. Compute
image families and the Telemetry tag must exist or apply fails.
