# Core startup scripts (executed VM boot)

Bake inventory: [Compute images](../../docs/component-images.md). Race metadata keys: [Terraform](../../docs/component-terraform.md).

1. Terraform `startup-script` (see [Terraform](../../docs/component-terraform.md)) execs `/chronoracer/entrypoint.sh`.

2. [`entrypoint.sh`](entrypoint.sh) sets `umask 077`, installs systemd units (mode `644`), then runs boot scripts before
   the Core app:

    - [`fetch-influx-token.sh`](fetch-influx-token.sh): read the shared Influx admin token from the Secret Manager
      secret named in `names.json` (project from the metadata server, Private Google Access, no NAT) and render
      `/chronoracer/admin-token.json` at mode `600`. Fail closed when the secret has no version. Token
      lifecycle: [Influx token runbook](../../docs/operator-influx-token.md).

    - [`fetch-payload.sh`](fetch-payload.sh): read `gs://{artifacts_bucket}/core/current.json`, then copy the Linux
      binary `core`, Avro schema `avro/sensor_data.avsc`, and `influxdb-plugins/*.py` from its immutable release (Private
      Google Access). Fail closed if missing. Binary mode `755`; schema mode `644`.

    - [`render-config.sh`](render-config.sh): render final Core YAML from the GCE metadata server (instance attributes + project/zone), operator YAML from GCS, and defaults (platform overlay wins). `yq strenv` needs the token **exported**. Writes `/chronoracer/core-config.yaml` and `/chronoracer/core.env` at mode `600`; the env file includes `CORE_CONFIG_PATH` and `AVRO_SENSOR_DATA_SCHEMA_PATH`. Does not call the Compute API. The platform overlay also pins `persistence.db_port` and `persistence.db_name` from `platform.json`, which is what keeps Core, the systemd bind, and the Dashboard datasource on one set of values.

      Token is merged into Core YAML as `persistence.db_password` (platform overlay wins). Rendered Core YAML and the
      Core environment file are `600`. That token is shared with the Dashboard; owner of its lifecycle:
      [Influx token runbook](../../docs/operator-influx-token.md).
      Core VM has no NAT - the build-time plugin venv and its all dependencies are documented with the [InfluxDB component](../../docs/component-influxdb.md#plugin-python-environment).

    - [`start-influx.sh`](start-influx.sh): read the final merged Core config, write `/chronoracer/influxdb3.env` with
      the bind address built from the `platform.json` port (the systemd unit reads it as `EnvironmentFile`),
      `systemctl enable --now influxdb3`, wait for that port on localhost, create its configured database and export
      trigger `database_export`. Data dir `/var/lib/influxdb3` is `700`. Influx binds all interfaces, not only
      localhost, so the Dashboard can query it;
      access model: [InfluxDB](../../docs/component-influxdb.md#access-model).

3. `systemctl enable --now chronoracer-core`. Unit `UMask=0077`, `HOME=/root`, and `EnvironmentFile=/chronoracer/core.env` (`CORE_CONFIG_PATH=/chronoracer/core-config.yaml`, `AVRO_SENSOR_DATA_SCHEMA_PATH=/chronoracer/avro/sensor_data.avsc`). Redpanda Connect is installed in the image; the VM does not download it.

4. Core app reads config from one YAML at `CORE_CONFIG_PATH`. Units run as root; secret files are `600` so only root can read them.


`ls` from parent directory inside the VM:
```terminaloutput
root@core:/chronoracer# ls  
admin-token.json          core-config.yaml     fetch-influx-token.sh  influxdb3.env      names.json  
avro                      core.env             fetch-payload.sh       influxdb3.service  platform.json  
chronoracer-core.service  default-config.yaml  gce-metadata.sh        influxdb-plugins   redpanda  
core                      entrypoint.sh        influxdb-plugin-venv   merge_config.py    render-config.sh  
                                                                                         start-influx.sh  
```