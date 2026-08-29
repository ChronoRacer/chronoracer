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

Required vars: `project_id`, `race_id`, `kafka_fqdn`, `ingest_open`. Image families must exist or apply fails.
