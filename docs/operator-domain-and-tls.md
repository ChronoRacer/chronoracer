# Operator — Domain and Kafka public TLS

## Domain

We have a domain registered with Porkbun. Domain name is: `chronoracer.live`. Domain is paid for until `05.09.2027`.

NS servers point at the Google Cloud DNS managed zone, not Porkbun defaults.

DNS zone is **paid monthly**. Needed so the Kafka VM can present a public TLS name, and so Relay and the Dashboard can
use Cloud Run domain mappings on the same zone ([Terraform](component-terraform.md)).

## Cloud Run hostnames

Always-on Terraform maps:

| Host                         | Service   |
|------------------------------|-----------|
| `relay.chronoracer.live`     | Relay     |
| `dashboard.chronoracer.live` | Dashboard |

Plus `CNAME` records to `ghs.googlehosted.com.`.

Google requires the domain to be **verified on this GCP project** before the mapping resource applies, but after NS are
delegated from Porkbun to Cloud DNS. One Search Console flow (includes adding a TXT record in Cloud DNS zone).

The CNAME propagation will take a rather long time (might even be hours). Check with this:

```bash
curl -I https://relay.chronoracer.live/health
```

| Propagation stage | Output                                                                                    |
|-------------------|-------------------------------------------------------------------------------------------|
| 1                 | `curl: (35) TLS connect error: error:0A000126:SSL routines::unexpected eof while reading` |
| 2                 | `HTTP/1.1 302 Found`                                                                      |
| 3                 | `200 OK`                                                                                  |

## Kafka TLS certificate

Run these commands from a workstation authenticated to GCP as an operator with DNS record-set permissions on the
`chronoracer` Cloud DNS zone and permission to add Secret Manager versions to `chronoracer-kafka-tls`.

Existing infrastructure:

```bash
PROJECT_ID="chronoracer"
ZONE="chronoracer"
HOSTNAME="ingest.chronoracer.live"
SECRET="chronoracer-kafka-tls"
EMAIL="<cert notifications will go here>"
```

The public Kafka listener uses `SASL_SSL` on TCP `9094`. The certificate SAN must be exactly
`DNS:ingest.chronoracer.live`. The certificate is infrastructure-wide, not race-specific.

### 1. Confirm prerequisites

```bash
gcloud config set project "${PROJECT_ID}"
gcloud dns managed-zones describe "${ZONE}" \
  --project="${PROJECT_ID}" \
  --format="yaml(name,dnsName,nameServers)"

gcloud secrets describe "${SECRET}" \
  --project="${PROJECT_ID}" \
  --format="yaml(name,replication)"
```

The zone DNS name must be `chronoracer.live.` and Porkbun must delegate the domain to the four name servers shown
by the first command. The secret may have zero versions.

### 2. Install and authenticate Certbot

```bash
sudo python3 -m venv /opt/certbot/
sudo /opt/certbot/bin/pip install --upgrade pip
sudo /opt/certbot/bin/pip install certbot certbot-dns-google
sudo ln -s /opt/certbot/bin/certbot /usr/local/bin/certbot
sudo certbot plugins # This should include dns-google
sudo gcloud auth application-default login
```

Note: run the ADC commands on sudo – this is separate from normal user ADC.

### 3. Request a staging certificate

```bash
sudo certbot certonly \
  --staging \
  --dns-google \
  --dns-google-project "${PROJECT_ID}" \
  --dns-google-propagation-seconds 120 \
  --domain "${HOSTNAME}" \
  --agree-tos \
  --email "${EMAIL}" \
  --non-interactive
```

After staging succeeds, request the production certificate:

```bash
sudo certbot certonly \
  --dns-google \
  --dns-google-project "${PROJECT_ID}" \
  --dns-google-propagation-seconds 120 \
  --domain "${HOSTNAME}" \
  --agree-tos \
  --email "${EMAIL}" \
  --non-interactive
```

The production files are:

```text
/etc/letsencrypt/live/ingest.chronoracer.live/fullchain.pem
/etc/letsencrypt/live/ingest.chronoracer.live/privkey.pem
```

### 4. Build and verify the Kafka bundle

```bash
sudo cat \
  "/etc/letsencrypt/live/ingest.chronoracer.live/fullchain.pem" \
  "/etc/letsencrypt/live/ingest.chronoracer.live/privkey.pem" \
  > ./kafka-tls.pem

openssl x509 -in ./kafka-tls.pem -noout -subject -dates -ext subjectAltName
```

The SAN must contain `DNS:ingest.chronoracer.live`. Verify the same PKCS12 conversion Kafka performs at boot:

```bash
PASS="$(openssl rand -base64 32 | tr -d '\n')"
openssl pkcs12 -export \
  -inkey ./kafka-tls.pem \
  -in ./kafka-tls.pem \
  -name kafka \
  -out ./kafka-keystore.p12 \
  -passout "pass:${PASS}"
echo $? # This must be 0
rm -f ./kafka-keystore.p12
unset PASS
```

Stop if this fails. The bundle must contain the leaf, Let's Encrypt intermediate chain, and unencrypted private key.
Do not upload PKCS12, JSON, a key-only file, or a leaf-only certificate.

### 5. Add and verify the Secret Manager version

Run this only after bundle verification succeeds:

```bash
gcloud secrets versions add "${SECRET}" \
  --project="${PROJECT_ID}" \
  --data-file=./kafka-tls.pem

gcloud secrets versions list "${SECRET}" \
  --project="${PROJECT_ID}"

gcloud secrets versions access latest \
  --secret="${SECRET}" \
  --project="${PROJECT_ID}" \
  | openssl x509 -noout -subject -dates -ext subjectAltName
```

The latest version must be `ENABLED` and its SAN must be `DNS:ingest.chronoracer.live`.

Kafka reads `latest` only when the broker starts.

### Renewal

Let's Encrypt certificates expire after 90 days. Renew at about 60 days, rebuild and verify `kafka-tls.pem`, add a new
Secret Manager version, and restart Kafka if a broker is running:

```bash
certbot renew
```

The DNS plugin must retain permission to update the `chronoracer` zone during renewal.
