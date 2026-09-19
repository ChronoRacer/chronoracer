# Operator runbook — Dashboard Auth0 login

Start: the Dashboard OAuth Secret Manager secret exists with zero versions (created by the first always-on apply
`-target` in [always-on README](../infra/always-on/README.md)). End: operators can log in to the Dashboard in a
browser.

The Dashboard reuses the `chronoracer-relay` Auth0 application rather than getting its own. Apps, grants, users, and who
may call what: [Okta](component-okta.md). What the Dashboard does with the login: [Dashboard](component-dashboard.md).

Secret name and client ID: [`names.json`](../infra/contract/names.json),
[`platform.json`](../infra/contract/platform.json).

## 1. Client secret

Upload **before** the full always-on apply that creates the Cloud Run service. Cloud Run injects `latest`; a revision
with no version does not start.

```bash
PROJECT_ID="chronoracer"
SECRET="<dashboard oauth secret name from names.json>"

gcloud secrets versions add "${SECRET}" \
  --project="${PROJECT_ID}" \
  --data-file=- <<<"<client secret from the Auth0 application>"

gcloud secrets versions list "${SECRET}" --project="${PROJECT_ID}"
```

The latest version must be `ENABLED`. The Dashboard resolves it at instance start, so a rotated secret reaches a warm
instance only after it is replaced; force a new revision or accept the next cold start.

Rotating this secret also invalidates operator CLI tokens minted with the same application's client credentials, because
it is one Auth0 application. Tell operators before rotating.

## 2. Auth0 application

In the Auth0 dashboard, on the `chronoracer-relay` application:

1. Enable the **Authorization Code** grant. Keep Client Credentials enabled — operator CLI tokens still use it
   ([Okta](component-okta.md)).
2. After the full always-on apply, add the Dashboard callback and logout URLs:

```bash
cd infra/always-on
terraform output dashboard_oauth_callback_url
terraform output dashboard_uri
```

Register the callback output as an **Allowed Callback URL** and the service URI as an **Allowed Logout URL**.

The callback URL is the mapped Dashboard hostname. If the `dashboard_root_url` check in always-on ever fails, the
hostname changed and this registration must be redone.

## 3. Operator users

Must be created in Okta console, Grafana maps all to viewer roles.
