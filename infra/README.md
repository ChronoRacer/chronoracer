# Infra

For documentation see the main document: [Terraform](../docs/component-terraform.md).

| Root                   | Backend prefix | Who                                         |
|------------------------|----------------|---------------------------------------------|
| [bootstrap](bootstrap) | local (once)   | Operator. Creates `chronoracer-state` only. |
| [always-on](always-on) | `base`         | Operator.                                   |
| [race](race)           | `race`         | Controller only                             |
| [packer](packer)       | none           | Operator (Used to build VM images)          |

- Names: [contract/names.json](contract/names.json).
- Ports/topics/OAuth: [contract/platform.json](contract/platform.json).
- Control-plane images: [runbook](../docs/operator-control-plane-images.md).
- VM images: [Compute images](../docs/component-images.md).
- Secret payloads to upload: [Kafka TLS](../docs/operator-domain-and-tls.md),
  [Influx token](../docs/operator-influx-token.md),
  [Browser OAuth](../docs/operator-browser-oauth.md).

