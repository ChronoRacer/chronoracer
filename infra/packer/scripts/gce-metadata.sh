#!/usr/bin/env bash
# Sourced by overlay boot scripts. GCE metadata server only. No Compute API.
# Requires curl. Fail closed on missing or empty values.

gce_metadata() {
  local path="$1"
  local value
  command -v curl >/dev/null 2>&1 || {
    echo "missing required command: curl" >&2
    return 1
  }
  value="$(curl -fsS --connect-timeout 5 --max-time 15 \
    -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/${path}")" || {
    echo "failed to read GCE metadata ${path}" >&2
    return 1
  }
  [[ -n "${value}" ]] || {
    echo "empty GCE metadata ${path}" >&2
    return 1
  }
  printf '%s' "${value}"
}

gce_project_id() {
  gce_metadata "project/project-id"
}

gce_zone() {
  local zone_uri
  zone_uri="$(gce_metadata "instance/zone")" || return 1
  printf '%s' "${zone_uri##*/}"
}

gce_attr() {
  gce_metadata "instance/attributes/${1:?gce_attr requires a metadata key}"
}
