# Part 5. Okta

We have 2 entry points that are or need to be publicly accessible over the internet:
1. Kafka broker (via Kafka protocol)
2. Communication Relay (via API)

They can be separated into two groups:
1. Accessible to all authenticated users: Communication Relay
2. Accessible to predetermined users (services): Kafka 

Additionally, there is the matter of InfluxDB access. This can be easily handled via tokens: https://docs.influxdata.com/influxdb3/core/admin/tokens/ and is internal only.

## Accessible to all authenticated users

Generally speaking, we don't need explicit authorization here, only authentication. So to make it easier, we can have uniform privileges for all authenticated users.

## Accessible to predetermined users

Kafka has integration with Okta via SASL/OAUTHBEARER auth method.

Roles are not that important here as we can just allow `*` on every topic for the MVP.
