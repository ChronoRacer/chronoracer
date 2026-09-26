# Always-on stack

Operator apply. What this stack creates, IAM, and residuals: [Terraform](../../docs/component-terraform.md).

Requires `gs://chronoracer-state` from [../bootstrap](../bootstrap). Relay, controller, Dashboard, and Telemetry images
before the stack that consumes them: [Container images](../../docs/operator-control-plane-images.md). VM
families: [Compute images](../../docs/component-images.md). Secret payloads:
[Kafka TLS](../../docs/operator-domain-and-tls.md), [Influx token](../../docs/operator-influx-token.md),
[Browser OAuth](../../docs/operator-browser-oauth.md).

Dashboard Cloud Run mounts the browser OAuth secret at `latest`. Race Telemetry mounts the same secret; Core VM reads
the Influx token at boot. Create the secret resources, upload payloads, then full apply:

```bash
# After bootstrap + terraform.tfvars (project_id, DNS, image URIs).
./publish-control-plane-images.sh
terraform init
terraform fmt
terraform validate
terraform apply -var-file=terraform.tfvars \
  -target=google_secret_manager_secret.influx_token \
  -target=google_secret_manager_secret.okta_oauth
# Upload versions (runbooks above), then:
terraform apply -var-file=terraform.tfvars
```

`kafka_fqdn` and `dns_zone_dns_name` are required (trailing dots). No fake default domain.
