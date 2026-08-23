# Part 5. Okta

We have 2 entry points that are or need to be publicly accessible over the internet:
1. Kafka broker (via Kafka protocol)
2. Communication Relay (via API)

They can be separated into two groups:
1. Accessible to all authenticated users: Communication Relay
2. Accessible to predetermined users (services): Kafka 

Additionally, there is the matter of InfluxDB access. This can be easily handled via tokens: https://docs.influxdata.com/influxdb3/core/admin/tokens/ and is internal only.

## Accessible to all authenticated users

- Read-only Relay API (race history, active race status, etc.)

## Accessible to predetermined users

- Public Kafka broker: integration with Okta via SASL/OAUTHBEARER auth method
- Internal Relay push endpoints: out of Okta scope, handled via Google keys check and identity verification)
- Write Relay API (start/stop race, etc.): should be gated with Okta permissions/groups. In MVP, treated as accessible to all authenticated users.

## Okta setup

Auth0 by Okta (Auth0’s Customer Identity Cloud platform).

API: https://api.chronoracer.com (Chronoracer API)
Applications: chronoracer-relay (client ID: `QwcTbS7a6Tn70WZo58UIl57mDxaJZp0V`, domain: `chronoracer.eu.auth0.com`)

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

