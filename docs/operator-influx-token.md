# Operator runbook — Influx admin token

Start: the Influx token Secret Manager secret exists with zero versions (created by the first always-on apply
`-target` in [always-on README](../infra/always-on/README.md)). End: one enabled version holding the bare admin token
string.

Only consumer: Core VM boot renders Influx's JSON token file from it
([Core startup](../core/startup/README.md)). Dashboard and Telemetry never receive this secret.

Secret name: [`names.json`](../infra/contract/names.json). Payload: the token string only, no JSON, no trailing
newline-sensitive formatting beyond what the boot script strips.

## When to run

- once before the first race (Core boot fails closed with no enabled version)
- on rotation

**Rotation must happen between races.** Influx stores the token at first serve on a fresh data directory, so a new
secret version does not reach a running race: the VM keeps the old token. Rotating with no active race is safe; the next
Core VM picks up `latest`.

## Generate

Let Influx generate the value so the format is guaranteed valid, then keep only the token string:

```bash
influxdb3 create token --admin --offline --output-file ./admin-token.json
TOKEN_VALUE="$(jq -r '.token' ./admin-token.json)"
printf '%s' "${TOKEN_VALUE}" > ./influx-token.txt
```

Do not commit either file. Upload the token without a trailing newline.

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

Verify that the upload has no trailing line ending:

```bash
test "$(wc -c < ./influx-token.txt)" -eq "$(printf %s "${TOKEN_VALUE}" | wc -c)"
```

```bash
rm -f ./admin-token.json ./influx-token.txt
```

