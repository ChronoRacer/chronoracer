# Analysis VM startup

Packer bakes this overlay into `/chronoracer`; race Terraform invokes `entrypoint.sh` through VM startup metadata.
The entrypoint calls the shared [Ops Agent helper](../../infra/packer/scripts/enable-ops-agent.sh) to install the baked
agent configuration and enable its systemd unit on every boot. Host metrics are collected every 60 seconds; process
metrics and the default logging pipeline are disabled, matching Core's host-only setup.
