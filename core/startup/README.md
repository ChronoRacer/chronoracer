# Core startup scripts (executed VM boot)

Bake inventory: [Compute images](../../docs/component-images.md). Race metadata keys: [Terraform](../../docs/component-terraform.md).

1. Terraform `startup-script` (see [Terraform](../../docs/component-terraform.md)) execs `/chronoracer/entrypoint.sh`.

2. [`entrypoint.sh`](entrypoint.sh) sets `umask 077`, installs systemd units (mode `644`), then runs boot scripts before the Core app:

    - [`fetch-payload.sh`](fetch-payload.sh): copy linux binary `core` and `influxdb-plugins/*.py` from `gs://{artifacts_bucket}/core/current/` (Private Google Access). Fail closed if missing. Binary mode `755`.

    - [`start-influx.sh`](start-influx.sh): offline admin token at `/chronoracer/admin-token.json` (mode `600`), `systemctl enable --now influxdb3`, wait for localhost:8181, create database `chrono_db` and export trigger `database_export`. Data dir `/var/lib/influxdb3` is `700`.

      Token is merged into Core YAML as `persistence.db_password` (platform overlay wins). Rendered Core YAML is `600`.
      Core VM has no NAT - the build-time plugin venv and its all dependencies are documented with the [InfluxDB component](../../docs/component-influxdb.md#plugin-python-environment).

    - [`render-config.sh`](render-config.sh): render final Core YAML from the GCE metadata server (instance attributes + project/zone), operator YAML from GCS, and defaults (platform overlay wins). `yq strenv` needs the token **exported**. Writes `/chronoracer/core-config.yaml` and `/chronoracer/core.env` at mode `600`. Does not call the Compute API.

3. `systemctl enable --now chronoracer-core`. Unit `UMask=0077`, `EnvironmentFile=/chronoracer/core.env` (`CORE_CONFIG_PATH=/chronoracer/core-config.yaml`).

4. Core app reads config from one YAML at `CORE_CONFIG_PATH`. Units run as root; secret files are `600` so only root can read them.
