#!/usr/bin/env bash
# Copy staged overlay into /chronoracer. Runs as root.
set -euo pipefail

install -d -m 755 /chronoracer

if [[ -d /tmp/overlay ]]; then
  while IFS= read -r -d '' f; do
    base="$(basename "${f}")"
    case "${base}" in
      *.sh | *.py) install -m 755 "${f}" "/chronoracer/${base}" ;;
      *) install -m 644 "${f}" "/chronoracer/${base}" ;;
    esac
  done < <(find /tmp/overlay -maxdepth 1 -type f -print0)
fi

if [[ -d /tmp/contract ]]; then
  while IFS= read -r -d '' f; do
    install -m 644 "${f}" "/chronoracer/$(basename "${f}")"
  done < <(find /tmp/contract -maxdepth 1 -type f -print0)
fi

if [[ -d /tmp/redpanda ]]; then
  install -d -m 755 /chronoracer/redpanda
  while IFS= read -r -d '' f; do
    install -m 644 "${f}" "/chronoracer/redpanda/$(basename "${f}")"
  done < <(find /tmp/redpanda -maxdepth 1 -type f -name '*.yaml' -print0)
fi

if [[ -f /tmp/default-config.yaml ]]; then
  install -m 644 /tmp/default-config.yaml /chronoracer/default-config.yaml
  install -d -m 755 /chronoracer/influxdb-plugins
fi

[[ -x /chronoracer/entrypoint.sh ]]
[[ -f /chronoracer/gce-metadata.sh ]]
[[ -f /chronoracer/names.json ]]
[[ -f /chronoracer/platform.json ]]
