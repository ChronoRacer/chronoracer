# Operator runbook — Browser Auth0 login

Start: the shared browser OAuth Secret Manager secret exists with zero versions (created by the first always-on apply
`-target` in [always-on README](../infra/always-on/README.md)). End: operators can log in to
[Dashboard](component-dashboard.md) and [Telemetry](component-telemetry.md) in a browser.

Both services reuse the `chronoracer-relay` Auth0 application rather than getting separate applications. Apps, grants,
users, and callers: [Okta](component-okta.md).

Secret name and client ID: [`names.json`](../infra/contract/names.json) and
[`platform.json`](../infra/contract/platform.json). The secret concept is browser OAuth, not Dashboard ownership; use
the shared browser name when contract files are updated.

## 1. Client secret

Upload before creating either Cloud Run revision. Cloud Run injects `latest`; a revision with no enabled version does
not start.

```bash
PROJECT_ID="chronoracer"
SECRET="<browser oauth secret name from names.json>"

gcloud secrets versions add "${SECRET}" \
  --project="${PROJECT_ID}" \
  --data-file=- <<<"<client secret from the Auth0 application>"

gcloud secrets versions list "${SECRET}" --project="${PROJECT_ID}"
```

The latest version must be `ENABLED`. Dashboard resolves it at instance start; replacement or cold start is needed
after rotation. Every race-scoped Telemetry revision resolves the current version when created.

Rotating this secret also invalidates operator CLI tokens minted with the same application's client credentials. Tell
operators before rotating.

## 2. Auth0 application

In the Auth0 dashboard, on the `chronoracer-relay` application:

1. Enable **Authorization Code**. Keep Client Credentials enabled for operator CLI tokens
   ([Okta](component-okta.md)).
2. After the always-on apply, obtain Dashboard callback and logout URLs:

```bash
cd infra/always-on
terraform output dashboard_oauth_callback_url
terraform output dashboard_uri
```

3. Obtain Telemetry's deterministic URI from its short service name, project number, and region. Confirm it against
   race Terraform output after the first test apply:

```text
https://telemetry-{project number}.{region}.run.app
```

4. Register both callback URLs as **Allowed Callback URLs** and both service roots as **Allowed Logout URLs**.
   Telemetry's callback path and logout return URL: [container contract](component-telemetry.md#container-contract).

Dashboard uses its mapped hostname. A failing `dashboard_root_url` Terraform check means its registration must be
updated. Telemetry uses no domain mapping; Relay returns the actual Terraform service URI.

## 3. Operator users

Create operator users in the Auth0 database connection. Dashboard maps every authenticated user to Grafana Viewer.
Telemetry grants read-only browser access. MVP has no role or group distinction.
