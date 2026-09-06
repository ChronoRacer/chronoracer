# Kafka startup scripts (executed at VM boot)

Listeners and boot contract: [Kafka](../../docs/component-kafka.md). Bake inventory: [Compute images](../../docs/component-images.md). TLS payload: [Kafka TLS runbook](../../docs/operator-domain-and-tls.md). Race metadata keys: [Terraform](../../docs/component-terraform.md).

1. Terraform `startup-script` (see [Terraform](../../docs/component-terraform.md)) execs `/chronoracer/entrypoint.sh`.
2. [`entrypoint.sh`](entrypoint.sh) only installs `kafka.service` and `systemctl enable --now kafka`. TLS + config happen in the unit on **every** start (including reboot): tmpfs keystore does not survive reboot.

`kafka.service` (`UMask=0077`, `Environment=CHRONORACER_DIR=/chronoracer`):

1. `ExecStartPre` [`fetch-tls.sh`](fetch-tls.sh): read `tls-secret-id` from the GCE metadata server, Secret Manager PEM (full resource name), PKCS12 + password on tmpfs mode `600`.
2. `ExecStartPre` [`render-server-properties.sh`](render-server-properties.sh): `server.properties` + `kafka.env` mode `600`. Public host from metadata `kafka-fqdn`; private advertised host from metadata project/zone. Keystore password read from file, not process argv.
3. `ExecStartPre` [`format-kraft.sh`](format-kraft.sh): format `/var/lib/kafka/data` (`700`) once.
4. `ExecStart` [`start-kafka.sh`](start-kafka.sh): source `kafka.env` then exec broker. Needed because systemd `EnvironmentFile=` is loaded **before** `ExecStartPre` would create the file.
5. `ExecStartPost` [`wait-public-listener.sh`](wait-public-listener.sh): fail closed if the public ingest port never binds.

Keystore password is random, local to the VM, never Terraform.
