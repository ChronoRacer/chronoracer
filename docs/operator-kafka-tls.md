# Operator rulebook — Kafka public TLS

Start: empty project knowledge. End: Secret Manager version exists, PEM parseable the same way Kafka boot parses it.
Race `apply` can then boot a public listener.

This is the only path that puts the private key in GCP. Do not put the key in Terraform, git, instance metadata, or the
image.

Canonical names: [`infra/contract/names.json`](../infra/contract/names.json).
Contract: [Kafka](component-kafka.md), [Terraform](component-terraform.md). Boot conversion: [
`kafka/startup/fetch-tls.sh`](../kafka/startup/fetch-tls.sh).

## What this cert is for

| Fact          | Value                                                                                       |
|---------------|---------------------------------------------------------------------------------------------|
| Listener      | Public ingest only (`SASL_SSL` on TCP **9094**)                                             |
| Hostname      | Always-on Terraform `kafka_fqdn` **without** trailing dot (example: `kafka.example.com`)    |
| Lifetime      | Long-lived secret, **one cert per hostname**, not per race                                  |
| IP            | Ephemeral. Devices verify the **hostname**. Cert SAN must be that hostname, never a race IP |
| Private Kafka | `9092` PLAINTEXT inside VPC. No public TLS                                                  |

Race stack creates/deletes the Cloud DNS **A** record. Between races the name must not resolve. The **certificate**
stays.

## Why Let's Encrypt + DNS-01

Paid public CAs and Google Certificate Manager are the wrong tools here. Certificate Manager issues for HTTPS load
balancers, not for TLS terminated on the Kafka VM.

Let's Encrypt is free. Use **DNS-01**, not HTTP-01:

- Kafka has no port 80 / no HTTP server.
- Between races there is no public A record, so HTTP-01 cannot prove the name.
- Always-on already owns Cloud DNS zone `chronoracer`. ACME writes `_acme-challenge.<kafka-hostname>` TXT, then deletes
  it.

HTTP-01, TLS-ALPN-01, self-signed, and “internal CA + device trust store” are out of scope unless you change the public
client contract.

Let's Encrypt leaf lifetime is **90 days**. Rotate the Secret Manager version before expiry. Staging ACME first
(`--staging` / `--test-cert`) so you do not burn production rate limits.

## Prerequisites (nothing → zone you can prove)

Do these in order. Stop if a gate fails.

1. **Registrar domain** any domain already owned. New cheap TLD is enough.
2. **GCP project** + region (see [docs README](README.md)).
3. **Always-on Terraform applied** with real `dns_zone_dns_name` and `kafka_fqdn` (both **trailing dots**). This
   creates:
    - Cloud DNS managed zone `chronoracer`
    - empty Secret Manager secret `chronoracer-kafka-tls` (no versions yet)
4. **Delegate NS** at the registrar to the always-on output `dns_zone_name_servers`. Wait until `dig NS <zone>` matches
   those servers. DNS-01 fails until this is true.
5. Confirm hostname: `terraform -chdir=infra/always-on output kafka_fqdn` → strip trailing dot. That string is the only
   SAN.

Race Terraform looking up the secret **resource** can succeed with **zero versions**. Kafka boot then fails closed.
Upload a version **before** the first race `apply`.

## Payload Kafka actually consumes

Secret `chronoracer-kafka-tls`, version `latest`.

Plain-text **PEM bundle** (UTF-8). Unencrypted private key. Concatenate:

1. Leaf certificate (`BEGIN CERTIFICATE`)
2. Issuer chain (Let's Encrypt intermediates; **not** the ISRG Root X1 that devices already trust)
3. Private key (`BEGIN PRIVATE KEY` or `BEGIN RSA PRIVATE KEY`)

Same file is passed to OpenSSL as both `-in` and `-inkey` (see `fetch-tls.sh`). Encrypted PKCS#8
(`Proc-Type: 4,ENCRYPTED`) fails at boot: no passphrase in Secret Manager.

Do **not** upload: PKCS12, JKS, `.crt` without key, key-only, JSON, base64-of-PEM-inside-JSON.

RSA 2048 is the compatibility default for Java on the Kafka image. ECDSA works on current Java; prefer RSA unless you
have a reason.

Leaf `subjectAltName` DNS name = Kafka hostname (no trailing dot). `CN` alone is not enough for modern clients.

## Issue (Let's Encrypt)

Run from a workstation with `gcloud` authenticated as a human or a **dedicated ACME** service account. Do not use the
Kafka VM SA. Do not grant the controller `secretAccessor`.

Minimum DNS IAM on zone `chronoracer` (or project) for ACME: create/list/delete resource record sets and create/get DNS
changes; get the managed zone. Secret IAM for the uploader: `secretmanager.secretVersionAdder` (or `secretmanager.admin`
on this secret only). Kafka SA already has `secretAccessor` from always-on.

### certbot + Google Cloud DNS plugin

```bash
# One-time:
#   pip install certbot-dns-google
# Credentials: user ADC is enough if your user can mutate the zone.
# Or a JSON key for a dedicated ACME SA — keep it off git and delete after.

PROJECT_ID="your-gcp-project"
HOSTNAME="kafka.example.com"   # no trailing dot; must equal always-on kafka_fqdn

certbot certonly \
  --dns-google \
  --dns-google-project "${PROJECT_ID}" \
  --dns-google-propagation-seconds 120 \
  -d "${HOSTNAME}" \
  --agree-tos \
  --email "ops@example.com" \
  --non-interactive
  # add --test-cert for staging
```

Live files (root-owned on Linux): `/etc/letsencrypt/live/${HOSTNAME}/fullchain.pem` and `privkey.pem`.

### Alternative: acme.sh (also free)

```bash
acme.sh --issue --dns dns_gcp -d "${HOSTNAME}"
# Staging: --staging
```

Export/install paths depend on acme.sh config. Still produce `fullchain` + `privkey`, then concatenate as below.

## Assemble and verify locally (same OpenSSL as boot)

```bash
umask 077
cat "/etc/letsencrypt/live/${HOSTNAME}/fullchain.pem" \
    "/etc/letsencrypt/live/${HOSTNAME}/privkey.pem" \
  > ./kafka-tls.pem

openssl x509 -in ./kafka-tls.pem -noout -subject -dates -ext subjectAltName
# SAN must list DNS:${HOSTNAME}

PASS="$(openssl rand -base64 32 | tr -d '\n')"
openssl pkcs12 -export \
  -inkey ./kafka-tls.pem \
  -in ./kafka-tls.pem \
  -name kafka \
  -out ./kafka-keystore.p12 \
  -passout "pass:${PASS}"
# Must exit 0. This is the Kafka boot conversion.
rm -f ./kafka-keystore.p12
unset PASS
```

If `pkcs12 -export` fails, do not upload.

## Upload to Secret Manager

Always-on already created secret id `chronoracer-kafka-tls`, replication `europe-west3`. You only add a **version**.

```bash
gcloud secrets versions add chronoracer-kafka-tls \
  --project="${PROJECT_ID}" \
  --data-file=./kafka-tls.pem
```

Confirm:

```bash
gcloud secrets versions list chronoracer-kafka-tls --project="${PROJECT_ID}"
gcloud secrets versions access latest \
  --secret=chronoracer-kafka-tls \
  --project="${PROJECT_ID}" | openssl x509 -noout -subject -dates -ext subjectAltName
```

**Ready** means: `latest` version `ENABLED`, local `openssl pkcs12 -export` succeeded on that exact bytes, SAN matches
`kafka_fqdn` without trailing dot.

Kafka picks up a new version on **next broker start** (`ExecStartPre` fetch). Mid-race rotation: add version, then
restart `kafka.service` (ingest blip). Prefer rotate **between races**.

After a good new version is in use, disable the previous Secret Manager version. Do not destroy all versions.

## Renewal calendar

| When                 | Action                                                                        |
|----------------------|-------------------------------------------------------------------------------|
| ~60 days after issue | Re-run ACME (certbot renew / acme.sh cron). Concatenate + `versions add`.     |
| After upload         | If a race VM is up, restart Kafka once so `latest` is fetched.                |
| Cert expired         | Public listener will not come up. Devices fail TLS. Fix secret, reboot Kafka. |

certbot `renew` still needs DNS-01 credentials. A workstation cron or Cloud Build **outside** the race controller is
fine. Do not teach the race controller to mint certificates.

## Do not

- Issue a cert per race or for the ephemeral public IP.
- Put the private key in Terraform state or `user-data`.
- Use HTTP-01 against the Kafka VM.
- Upload PKCS12 into Secret Manager (boot expects PEM).
- Grant Relay or controller `secretAccessor` on this secret.
- Skip registrar NS delegation and “hope” Let's Encrypt works.

## Failure map

| Symptom                             | Likely cause                                                                 |
|-------------------------------------|------------------------------------------------------------------------------|
| ACME `NXDOMAIN` / timeout on TXT    | Registrar NS not Cloud DNS, or wrong project/zone                            |
| `unauthorized` on `_acme-challenge` | IAM, wrong zone, or hostname not in `dns_zone_dns_name`                      |
| Kafka boot: empty secret            | No versions; race TF does not require a version                              |
| Kafka boot: OpenSSL pkcs12 fail     | Encrypted key, missing key, or not a PEM bundle                              |
| Devices: hostname mismatch          | SAN ≠ advertised `kafka-fqdn`                                                |
| Devices: untrusted issuer           | Uploaded leaf-only without Let's Encrypt intermediate (`fullchain` required) |
