FROM grafana/grafana-oss:13.0.2

COPY grafana.ini /etc/grafana/grafana.ini
COPY provisioning /etc/grafana/provisioning
COPY provisioning/dashboards/core-ingestion.json /var/lib/grafana/dashboards/core-ingestion.json
