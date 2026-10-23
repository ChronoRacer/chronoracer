# Load-test race stack — infra readiness (read-only audit)

**Project:** `chronoracer` · **Region/zone:** `europe-west1` / `europe-west1-b` · **Kafka FQDN:** `ingest.chronoracer.live.`

## Backend & variables

| Item | Value |
|------|--------|
| State bucket | `gs://chronoracer-state` |
| State prefix | `race` → object `race/default.tfstate` (not per-`race_id`) |
| Workspace | default only (`race/default.tfstate`; docs: no workspace per race) |
| Required apply vars | `project_id`, `region`, `zone`, `race_id`, `kafka_fqdn` (trailing dot), `ingest_open`, **`telemetry_image`** (no default) |
| `telemetry_image` | `europe-west1-docker.pkg.dev/chronoracer/chronoracer/telemetry:latest` (match always-on / publish script) |
| `terraform.tfvars` | Sets `project_id`, `kafka_fqdn`, `ingest_open=true`, placeholder `race_id` — override `race_id` + pass `telemetry_image` on CLI |

## Targeted apply / destroy (no Analysis VM)

**Resources (6):** `google_compute_instance.kafka`, `google_compute_instance.core`, `google_compute_firewall.kafka_public[0]` (requires `ingest_open=true`), `google_dns_record_set.kafka`, `google_cloud_run_v2_service.telemetry`, `google_cloud_run_v2_service_iam_member.telemetry_public`.

**Data sources with `-target`:** Verified via `terraform plan -target=…`: reads kafka/core images, kafka/core/telemetry SAs, VPC, VM + telemetry subnets, DNS zone, TLS + okta secrets, race bucket, zones, project. **Does not read** `data.google_service_account.analysis` or `data.google_compute_image.analysis`. A **non-targeted** full plan still reads analysis SA/image.

**Apply** (from `infra/race` after `terraform init`; use operator credentials with race-state write + compute/run/dns IAM):

```bash
cd infra/race
terraform apply -input=false -auto-approve -lock-timeout=5m \
  -var-file=terraform.tfvars \
  -var 'race_id=<RACE_ID>' \
  -var 'telemetry_image=europe-west1-docker.pkg.dev/chronoracer/chronoracer/telemetry:latest' \
  -target=google_compute_instance.kafka \
  -target=google_compute_instance.core \
  -target='google_compute_firewall.kafka_public[0]' \
  -target=google_dns_record_set.kafka \
  -target=google_cloud_run_v2_service.telemetry \
  -target=google_cloud_run_v2_service_iam_member.telemetry_public
```

**Destroy** (same targets; keep `ingest_open=true` so firewall `[0]` exists in state; `telemetry_image` still required):

```bash
terraform destroy -input=false -auto-approve -lock-timeout=5m \
  -var-file=terraform.tfvars \
  -var 'race_id=<RACE_ID>' \
  -var 'telemetry_image=europe-west1-docker.pkg.dev/chronoracer/chronoracer/telemetry:latest' \
  -var 'ingest_open=true' \
  -target=google_compute_instance.kafka \
  -target=google_compute_instance.core \
  -target='google_compute_firewall.kafka_public[0]' \
  -target=google_dns_record_set.kafka \
  -target=google_cloud_run_v2_service.telemetry \
  -target=google_cloud_run_v2_service_iam_member.telemetry_public
```

**Close ingest only** (optional, controller equivalent): `terraform apply` with `-var ingest_open=false` and `-target='google_compute_firewall.kafka_public[0]'` (removes rule; VMs/DNS/Telemetry unchanged).

## Manual steps (controller / Relay normally does these)

Order for a manual load test:

1. **Choose `<RACE_ID>`** (1–12 chars, `[a-z0-9_-]`, matches Terraform validation).
2. **Upload Core operator YAML** — required for boot (`render-config.sh` `gcloud storage cat` the metadata URI). Example: `load-test/core-config.yaml` → `gs://chronoracer-race/<RACE_ID>/core-config.yaml` (controller `apply` HEAD-check; Terraform does not).
3. **Core binary** — boot pulls `artifacts_bucket` via `core/current.json` (today `0.1.4-load`). If you ship your own build instead: publish to artifacts + bump pointer, **or** after IAP SSH to `core`, replace `/chronoracer/core` and restart `chronoracer-core` (bypasses controller/Relay).
4. **`terraform init` + targeted apply** (commands above). Expect state lock on `race/default.tfstate` (5m timeout; no force-unlock).
5. **Wait for boot** — Kafka: TLS from Secret Manager, topics (`event` at Kafka boot; `data`/`control`/`heartbeat`/`dlq` from Core). Core: Influx token secret, payload fetch, merged config. No manual Kafka OAuth/ACL setup (OAuth validator in broker config; clients use Auth0 per `platform.json`).
6. **DNS** — A record created by apply (TTL 30s). Load generators use `ingest.chronoracer.live:9094` (TLS + OAuth).
7. **SSH to Core (no public IP):** `gcloud compute ssh core --zone=europe-west1-b --tunnel-through-iap --project=chronoracer` then `journalctl -u chronoracer-core` (needs IAP tunnel + `iap_ssh_members` or equivalent).
8. **Readiness** — Relay/Firestore `applied` phase and heartbeat-gated status are **not** updated; verify Kafka listener, Core logs, and heartbeat on private topic yourself.
9. **Telemetry** — public `run.app` URL from terraform output `telemetry_url`; browser Auth0 login (same secret as Dashboard).

**Not required for this stack:** Analysis VM, Relay race start, Cloud Build controller, Firestore mutex/`runToken`, controller output persistence, Pub/Sub wiring (Core uses existing `export-done` topic if export runs).

## Notes / surprises

- Targeted plan: **6 adds**, analysis data sources skipped; `analysis_instance` output omitted from plan (resource not in graph).
- Full plan without `-target` still resolves analysis image/SA (would plan 7 VMs worth of deps).
- `outputs.tf` still defines `analysis_instance`; harmless if analysis never applied.
- Core config GCS path is **mandatory** at boot, not optional.
