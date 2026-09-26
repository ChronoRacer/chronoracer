FROM grafana/grafana:13.0.2
USER root
ENV GCP_PROJECT_ID=chronoracer
RUN apk add --no-cache curl jq
# 472 is grafana
COPY --chown=472:0 grafana.ini /etc/grafana/grafana.ini
COPY --chown=472:0 provisioning /etc/grafana/provisioning
COPY --chown=472:0 dashboard_files/ /etc/grafana/to_provision
COPY --chown=472:0 entrypoint.sh/ /entrypoint.sh
RUN mkdir -p /usr/share/grafana/data && \
    chown -R 472:0 /usr/share/grafana/data /etc/grafana && \
    chmod -R 775 /usr/share/grafana/data /etc/grafana && \
    chmod +x /entrypoint.sh && \
    cp -R /etc/grafana/to_provision/v1 /etc/grafana/provisioning/dashboards/json/
USER 472
ENTRYPOINT ["/entrypoint.sh"]