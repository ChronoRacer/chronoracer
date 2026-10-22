# Race-stack load test harness

End-to-end load testing for the race stack (Kafka VM, Core + InfluxDB VM, Telemetry Cloud Run). The Kafka producer lives in [`curls/load_test/`](../curls/load_test/); this directory owns orchestration, Core operator overlays, artifact collection, and analysis helpers.

## Prerequisites

- `gcloud` with IAP SSH to race VMs (`core`, `kafka`), Terraform, and a laptop clock synced via NTP (`timedatectl`).
- Python venv (example): `/tmp/lt-venv` with  
  `pip install -r curls/load_test/requirements.txt google-cloud-monitoring grpcio`
- `export RACE_ID=<id>` (1–12 chars `[a-z0-9_-]`) and `OKTA_CLIENT_SECRET` (see [`curls/load_test/README.md`](../curls/load_test/README.md)).

## Quick start

1. Upload Core overlay — [runbook §1](runbook.md#1-ready-check-laptop).
2. Targeted apply — [infra-readiness.md](infra-readiness.md#targeted-apply--destroy-no-analysis-vm).
3. Run a scenario — e.g. `load-test/run-scenario.sh S0 1000 120 1` (needs `RACE_ID`, secret).
4. Teardown — same doc, targeted destroy.

Procedure, scenarios, and pass criteria: [runbook.md](runbook.md). Log filters and metrics: [observability.md](observability.md). Apply/destroy and GCP checklist: [infra-readiness.md](infra-readiness.md).

## Files

| File | Owns |
| --- | --- |
| [README.md](README.md) | This entry point |
| [runbook.md](runbook.md) | Scenarios, ready checks, pass criteria, teardown |
| [infra-readiness.md](infra-readiness.md) | Terraform targeted apply/destroy, manual controller steps |
| [observability.md](observability.md) | Log filters, metrics export, loss-check guidance |
| [watch_probe.md](watch_probe.md) | IAP SSH forward + Core `Watch` probe |
| [core-config.yaml](core-config.yaml) | Baseline operator overlay (10 s reports, initial driver) |
| [core-config-throughput.yaml](core-config-throughput.yaml) | Persistence tuned for S3+ throughput |
| [core-config-phase2.yaml](core-config-phase2.yaml) | Optional phase-2 tuning overlay |
| [generator-config.json](generator-config.json) | Laptop generator: public Kafka, OAuth, channels |
| [run-generators.sh](run-generators.sh) | Parallel `load_test.py` instances → `runs/<id>/` |
| [run-scenario.sh](run-scenario.sh) | Generators, lag wait, collect, Influx count, steady summary |
| [collect-artifacts.sh](collect-artifacts.sh) | Cloud Logging + Monitoring → run `artifacts/` |
| [collect_metrics.py](collect_metrics.py) | Monitoring API CSV export (used by collect-artifacts) |
| [parse_pipeline_report.py](parse_pipeline_report.py) | Pipeline report log → CSV |
| [summarize_generators.py](summarize_generators.py) | Generator logs → CSV |
| [steady_summary.py](steady_summary.py) | Steady-window JSON summary |
| [influx_count.sh](influx_count.sh) | Per-table Influx counts on Core VM (secondary loss check) |
| [fault-inject.sh](fault-inject.sh) | S5 fault timing (`stop` vs `freeze` InfluxDB) |
| [bench_generator.py](bench_generator.py) | Local encode throughput (no Kafka) |
| [watch_probe.py](watch_probe.py) | Slow `Watch` subscriber for S4 |
| [gen/](gen/) | Generated gRPC stubs for `watch_probe.py` |
| `runs/` | Per-run output (gitignored) |

## Caveats (from production runs)

- **VM NTP:** Core/Kafka use `systemd-timesyncd` with a 17–34 min poll by default. For latency tests, install a test-only drop-in on both VMs: `/etc/systemd/timesyncd.conf.d/99-loadtest.conf` with `NTP=metadata.google.internal`, `PollIntervalMinSec=16`, `PollIntervalMaxSec=32`, then `systemctl restart systemd-timesyncd`. `run-scenario.sh` records offsets in `runs/<id>/clock.txt`.
- **Generator logs:** stdout is block-buffered; `[Report]` lines often appear only after the process exits.
- **Influx WAL:** `--wal-flush-interval` via `/chronoracer/influxdb3.env` is wiped on VM reboot by `start-influx.sh`.
- **Influx restart:** Stopping or resetting InfluxDB makes Core crash-loop until InfluxDB is healthy again.
- **S5 faults:** `fault-inject.sh stop` stops InfluxDB and Core (`Requires=influxdb3`); use `freeze` to stall the DB without stopping Core.
