# Always-on stack

Human/CI apply. What this stack creates, IAM, and residuals: [Terraform](../../docs/component-terraform.md).

Requires `gs://chronoracer-state` from [../bootstrap](../bootstrap). Relay and controller images before a full
apply: [Control-plane images](../../docs/operator-control-plane-images.md). VM
families: [Compute images](../../docs/component-images.md). TLS upload: [Kafka TLS](../../docs/operator-domain-and-tls.md).

```bash
# After bootstrap + terraform.tfvars (project_id, DNS, image URIs).
./publish-control-plane-images.sh
terraform init
terraform fmt
terraform validate
terraform plan -var-file=terraform.tfvars
```

`kafka_fqdn` and `dns_zone_dns_name` are required (trailing dots). No fake default domain.
