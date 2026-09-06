# Deep dive - Okta

We have 2 entry points that are or need to be publicly accessible over the internet:

1. Kafka broker (via Kafka protocol)
2. Communication Relay (via API)

They can be separated into two groups:

1. Accessible to all authenticated users: Communication Relay
2. Accessible to predetermined users (services): Kafka

Additionally, there is the matter of InfluxDB access. This can be easily handled via database tokens and is internal
only. Note that the free version of InfluxDB does not support roles or token permissions.

## Accessible to all authenticated users

- Read-only Relay API (race history, active race status, etc.)

## Accessible to predetermined users

- Public Kafka broker: integration with Okta via SASL/OAUTHBEARER. Broker validates JWTs with Auth0 JWKS; URLs live in [
  `infra/contract/platform.json`](../infra/contract/platform.json). Overlay: [Kafka](component-kafka.md).
- Internal Relay push endpoints: out of Okta scope, handled via Google keys check and identity verification.
- Write Relay API (start/stop race, etc.): should be gated with Okta permissions/groups. In MVP, treated as accessible
  to all authenticated users.

## Okta setup

Auth0 by Okta (Auth0’s Customer Identity Cloud platform).

API:

- Chronoracer API (https://api.chronoracer.com)
  Applications:
- chronoracer-relay (client ID: `QwcTbS7a6Tn70WZo58UIl57mDxaJZp0V`, domain: `chronoracer.eu.auth0.com`)

MVP scope: currently application has no roles or groups assigned to it.

To generate a token without any UI interaction:

```bash
CLIENT_ID="QwcTbS7a6Tn70WZo58UIl57mDxaJZp0V"
CLIENT_SECRET=""
curl --request POST \
  --url https://chronoracer.eu.auth0.com/oauth/token \
  --header 'content-type: application/json' \
  --data '{
    "client_id":"$CLIENT_ID",
    "client_secret":"$CLIENT_SECRET",
    "audience":"https://api.chronoracer.com",
    "grant_type":"client_credentials"
  }'
```

Alternatively, you can mint here: https://manage.auth0.com/dashboard/eu/chronoracer/apis/6a8af66a9af427fbc46ef664/test
