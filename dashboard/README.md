# Dashboard

Architecture, datasource, panels, and container contract:
[Dashboard](../docs/component-dashboard.md). Decision:
[ADR - Dashboard and live telemetry](../docs/adr-dashboard.md).

This image is Grafana OSS with a provisioned Google Cloud Monitoring datasource and race-fleet dashboards. It must not
contain an Influx datasource or sensor-value panels.

Local development needs:

- Grafana environment variables from the component contract
- Google Application Default Credentials with read-only Cloud Monitoring access
- Auth0 localhost callback registration
- Cloud Monitoring test metrics from a race or fixture project

Do not use a downloaded service-account JSON key. Use operator ADC locally; production uses the Cloud Run attached
service identity. Production acceptance also requires the Cloud Run ADC proof in the component document.
