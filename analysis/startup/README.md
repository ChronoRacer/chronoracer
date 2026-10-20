# Analysis VM startup

Bake inventory: [Compute images](../../docs/component-images.md). Race metadata keys: [Terraform](../../docs/component-terraform.md).

The race VM has no internet access. Every Python dependency is installed while the image is built.

## Image bake

[`install-analysis.sh`](install-analysis.sh) runs on the Packer builder. It installs `python3-venv`, creates
`/chronoracer/analysis-venv`, installs [`requirements.txt`](../requirements.txt) into it, and fails the
build on `pip check` or a failed import. It copies the `shared`, `data_stream`, `rules`, and `predictions` packages to
`/chronoracer/analysis`, so the module imports as `analysis.*` with `PYTHONPATH=/chronoracer`. Changing code or
requirements means rebuilding the image family.

## Boot

Race Terraform invokes `/chronoracer/entrypoint.sh` through VM startup metadata.

1. [`entrypoint.sh`](entrypoint.sh) sets `umask 077`, fails closed when the baked venv is missing, and installs the
   systemd units (mode `644`) before the boot scripts.
2. [`render-env.sh`](render-env.sh) reads instance attributes from the GCE metadata server and writes
   `/chronoracer/analysis.env` at mode `600`:

   | Variable             | Metadata key         |
   |----------------------|----------------------|
   | `RACE_ID`            | `race-id`            |
   | `CORE_TELEMETRY_URL` | `core-telemetry-url` |
   | `TELEMETRY_URL`      | `telemetry-url`      |

   Values are owned by [Terraform](../../docs/component-terraform.md). Processes read them from the environment,
   loaded by systemd `EnvironmentFile=`.
3. The shared [Ops Agent helper](../../infra/packer/scripts/enable-ops-agent.sh) installs the baked agent
   configuration and enables its unit. Host metrics are collected every 60 seconds; process metrics and the default
   logging pipeline are disabled, matching Core's host-only setup.

4. `systemctl enable --now` starts three independent processes from the shared venv. Each unit has `UMask=0077`,
   `WorkingDirectory=/chronoracer`, `PYTHONPATH=/chronoracer`, `EnvironmentFile=/chronoracer/analysis.env`, and
   `Restart=on-failure`:

   | Unit                                                                               | Module                                 | Role                                                       |
   |------------------------------------------------------------------------------------|----------------------------------------|------------------------------------------------------------|
   | [`chronoracer-analysis-stream`](chronoracer-analysis-stream.service)               | `analysis.data_stream.pathway_stream_pub` | Only Core `Watch` caller; publishes windows on local ZMQ |
   | [`chronoracer-analysis-rules`](chronoracer-analysis-rules.service)                 | `analysis.rules.rules`                 | Subscribes to the local ZMQ windows                        |
   | [`chronoracer-analysis-predictions`](chronoracer-analysis-predictions.service)     | `analysis.predictions.predictions`     | Subscribes to the local ZMQ windows                        |

   Rules and predictions are ordered after the stream but do not require it. ZMQ subscribers reconnect on their own, so
   each process starts and restarts independently. Windows published before a subscriber connects are lost.
