# Operator runbook — Influx admin token

Start: the Influx token Secret Manager secret exists with zero versions (created by the first always-on apply
`-target` in [always-on README](../infra/always-on/README.md)). End: one enabled version holding the bare admin token
string.

Consumers of that version: Core VM boot renders Influx's JSON token file from it
([Core startup](../core/startup/README.md)), and the Dashboard reads it as an environment variable
([Dashboard](component-dashboard.md)). Why one shared token instead of a VM-generated
one: [ADR - Dashboard](adr-dashboard.md).

Secret name: [`names.json`](../infra/contract/names.json). Payload: the token string only, no JSON, no trailing
newline-sensitive formatting beyond what the boot script strips.

## When to run

- once, after the secret resource exists and **before** the full always-on apply that creates the Dashboard service
  (Cloud Run injects `latest`; a revision with no version does not start)
- also required before the first race (Core boot fail-closed with no version)
- on rotation

**Rotation must happen between races.** Influx stores the token at first serve on a fresh data directory, so a new
secret version does not reach a running race: the VM keeps the old token and the dashboard would start presenting the
new one. Rotating with no active race is safe - the next race VM and the next dashboard cold start both pick up
`latest`.

## Generate

Let Influx generate the value so the format is guaranteed valid, then keep only the token string:

```bash
influxdb3 create token --admin --offline --output-file ./admin-token.json
jq -r '.token' ./admin-token.json > ./influx-token.txt
```

Do not commit either file. Delete both after upload.

## Upload and verify

```bash
PROJECT_ID="chronoracer"
SECRET="<influx token secret name from names.json>"

gcloud secrets versions add "${SECRET}" \
  --project="${PROJECT_ID}" \
  --data-file=./influx-token.txt

gcloud secrets versions list "${SECRET}" --project="${PROJECT_ID}"
```

The latest version must be `ENABLED`. Disable older versions after the next race starts cleanly.

```bash
rm -f ./admin-token.json ./influx-token.txt
```

## After rotation

The dashboard resolves the secret at instance start. Force a fresh instance (or accept the next cold start) so a stale
token does not linger in a warm instance.
