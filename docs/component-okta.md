# Deep dive - Okta

Auth0 by Okta (Customer Identity Cloud). Three public entry points use it; Pub/Sub push to Relay does not.

Where: Auth0 tenant `chronoracer.eu.auth0.com`. Not a GCP resource.

Lifecycle: always-on. Apps and API outlive races.

Access: operators and track devices present bearer tokens. Relay and Kafka validate locally (JWKS). No Google identity
for operators.

Responsibility:

- identify operators on Relay HTTPS
- identify operators logging in to the Dashboard in a browser
- identify track devices on the public Kafka listener
- mint operator tokens for CLI / scripts

Issuer, audience, JWKS: [`platform.json`](../infra/contract/platform.json) (`kafka_oauth_*`). Kafka overlay:
[Kafka](component-kafka.md). Relay HTTP checks: [Relay](component-relay.md).

## Apps

| Kind         | Name                | Value                                                                        |
|--------------|---------------------|------------------------------------------------------------------------------|
| API audience | Chronoracer API     | `kafka_oauth_audience` in [`platform.json`](../infra/contract/platform.json) |
| Application  | `chronoracer-relay` | `oauth_client_id` in [`platform.json`](../infra/contract/platform.json)      |
| Domain       | Auth0 tenant        | `chronoracer.eu.auth0.com`                                                   |

Issuer, audience, JWKS, and the client ID are consumed by Kafka boot, Relay, and the Dashboard, so the contract file
owns those strings. The `kafka_oauth_*` prefix predates the browser login flow and now also covers the Dashboard;
renaming it would touch Kafka boot scripts and is deliberately not bundled with the Dashboard.

MVP: no roles, no groups. Any valid token for that audience may call every Relay operator endpoint (read and write).
Write gating by group is future work.

### Grants and connections

The `chronoracer-relay` application carries both flows; the Dashboard does not get its own app.

| Flow               | Used by                              | Notes                                                                                                          |
|--------------------|--------------------------------------|----------------------------------------------------------------------------------------------------------------|
| Client credentials | Operator CLI / scripts against Relay | Shared client secret, no per-person identity. Token recipe below.                                              |
| Authorization code | Dashboard browser login              | Callback and logout URLs are the mapped Dashboard hostname Grafana uses ([Terraform](component-terraform.md)). |

Interactive login needs real users, which client credentials never required. An Auth0 database connection holds
operator users, created by hand in the Auth0 dashboard. No groups; the Dashboard maps every authenticated user to
Grafana Viewer ([Dashboard](component-dashboard.md)).

Because it is one application, rotating the client secret breaks operator CLI tokens too. Setup and rotation
steps: [Dashboard OAuth runbook](operator-dashboard-oauth.md).

Out of Okta:

- Pub/Sub push to Relay (Google OIDC)
- InfluxDB (shared admin token from Secret Manager; [InfluxDB](component-influxdb.md))

## Who may call what

| Caller                    | Token                                                       | Surface                                               |
|---------------------------|-------------------------------------------------------------|-------------------------------------------------------|
| Operator                  | Auth0 access token, audience `https://api.chronoracer.com`  | Relay start / status / history / stop / force-destroy |
| Operator (browser)        | Auth0 user login, authorization code                        | Dashboard, read-only                                  |
| Track device              | Auth0 client credentials (client id + secret on the device) | Kafka PUBLIC listener only                            |
| Cloud Build / export-done | Google OIDC                                                 | Relay push paths; not Okta                            |

## Operator token (no UI)

```bash
CLIENT_ID="QwcTbS7a6Tn70WZo58UIl57mDxaJZp0V"
OKTA_CLIENT_SECRET=""
OKTA_ACCESS_TOKEN="$(curl --request POST \
  --url https://chronoracer.eu.auth0.com/oauth/token \
  --header 'content-type: application/json' \
  --data "{
    \"client_id\":\"$CLIENT_ID\",
    \"client_secret\":\"$OKTA_CLIENT_SECRET\",
    \"audience\":\"https://api.chronoracer.com\",
    \"grant_type\":\"client_credentials\"
  }" | jq -r '.access_token')"
```

Dashboard mint (same API): https://manage.auth0.com/dashboard/eu/chronoracer/apis/6a8af66a9af427fbc46ef664/test
