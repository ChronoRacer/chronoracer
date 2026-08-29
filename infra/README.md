# Infra

How-to for these roots. Stacks, backends, IAM: [Terraform](../docs/component-terraform.md).

| Root | Backend prefix | Who |
| --- | --- | --- |
| [bootstrap](bootstrap) | local (once) | Human. Creates `chronoracer-state` only. |
| [always-on](always-on) | `base` | Human / CI |
| [race](race) | `race` | Controller only |

Names: [contract/names.json](contract/names.json). Ports/topics/OAuth: [contract/platform.json](contract/platform.json). Control-plane images: [runbook](../docs/operator-control-plane-images.md). VM images: [Compute images](../docs/component-images.md).
