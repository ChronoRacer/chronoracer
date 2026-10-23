# Load test runbook: 15k points/s on the race stack (Kafka + Core + Telemetry, no Analysis)

Goal: find where ingest stops scaling and prove a stable ≥ 15k points/s with every point persisted, plus a
preview-latency baseline on current defaults. Option analysis this feeds: [core-latency-options.md](../scratch/core-latency-options.md).

| File | Owns |
| --- | --- |
| [infra-readiness.md](infra-readiness.md) | Prerequisite audit, targeted apply/destroy commands, controller steps done by hand |
| [observability.md](observability.md) | Log filters, metric types, Kafka VM commands, config merge rules |
| [watch_probe.md](watch_probe.md) | Direct Core `Watch` subscriber over an IAP SSH forward |
| [core-config.yaml](core-config.yaml) / [core-config-throughput.yaml](core-config-throughput.yaml) / [core-config-phase2.yaml](core-config-phase2.yaml) | Operator overlay: baseline (S0-S2) / persistence sized for throughput (S3 on) / phase-2 tuning |
| [generator-config.json](generator-config.json) | Laptop generator: public listener, OAuth, `shutdown` disabled, `CHDRV` after 60 s |
| [run-generators.sh](run-generators.sh) | Parallel generators, one run directory per run |
| [run-scenario.sh](run-scenario.sh) | One-shot: generators, wait, collect, Influx count, steady summary |
| [collect-artifacts.sh](collect-artifacts.sh) | Logs + metrics + summaries into the run directory |
| [steady_summary.py](steady_summary.py) | Steady-window JSON summary into `artifacts/steady-summary.json` |
| [influx_count.sh](influx_count.sh) | Per-table persisted point count on the Core VM (secondary loss check) |
| [fault-inject.sh](fault-inject.sh) | S5 timing: `stop` or `freeze` InfluxDB, then restart Core |


## 1. Ready check (laptop)

```bash
cd /home/leadman/central/chrono_racer/chronoracer-core
export RACE_ID=lt1021a                      # 1-12 chars [a-z0-9_-]
export OKTA_CLIENT_SECRET="$(gcloud secrets versions access latest --secret=chronoracer-okta-oauth --project=chronoracer)"
/tmp/lt-venv/bin/pip install -q -r curls/load_test/requirements.txt google-cloud-monitoring grpcio
timedatectl show -p NTPSynchronized --value  # must print yes
gcloud storage cp load-test/core-config.yaml "gs://chronoracer-race/${RACE_ID}/core-config.yaml"
```

The overlay is mandatory: Core fails boot without it. It only sets the report interval to 10 s and the initial driver.

## 2. Raise the stack

`cd infra/race && terraform init`, then the targeted apply in [infra-readiness.md](infra-readiness.md#targeted-apply--destroy-no-analysis-vm)
with `race_id=${RACE_ID}`. It creates 6 resources: Kafka VM, Core VM, public Kafka firewall, `ingest` DNS record,
Telemetry Cloud Run + public invoker. State is the shared `race/default.tfstate`: nobody else may apply meanwhile.
If apply errors on outputs after creating the resources (`analysis_instance` references the skipped VM), the stack is
fine: read the Kafka IP with `gcloud compute instances describe kafka --zone=europe-west1-b --format='get(networkInterfaces[0].accessConfigs[0].natIP)'`
and the UI with `gcloud run services describe telemetry --region=europe-west1 --format='value(status.url)'`.

Ready when all hold (allow ~5 min):

| Check | How |
| --- | --- |
| DNS points at the new Kafka IP | `dig +short ingest.chronoracer.live` equals `terraform output kafka_public_ip` |
| Core up, right release, idle reports | `gcloud compute ssh core --zone=europe-west1-b --tunnel-through-iap -- sudo journalctl -u chronoracer-core -o cat \| grep -E 'release\|Pipeline report' \| tail` |
| Topics created by Core | Kafka VM `kafka-topics.sh --describe --topic data` shows 4 partitions ([observability.md](observability.md#4-kafka-vm-checks)) |
| Telemetry subscribed to Core | Cloud Run logs show `telemetry_hello` (filter in [observability.md](observability.md)) |
| Telemetry UI | `terraform output telemetry_url`, Auth0 login |

## 3. Scenarios

Run in order. Pause ≥ 3 min between runs so the pipeline drains and windows stay separable. Every run:
`load-test/run-generators.sh <TOTAL_RATE> <DURATION_S> <INSTANCES> load-test/runs/<ID>` or
`load-test/run-scenario.sh <ID> <TOTAL_RATE> <DURATION_S> <INSTANCES>` (collect + count + summary).

| ID | Rate (pts/s) | Duration | Instances | Extra | Answers |
| --- | --- | --- | --- | --- | --- |
| S0 smoke | 1,000 | 120 s | 1 | check `CHDRV` applied in Core log | Auth, path, reports, artifact collection work |
| S1a-d ramp | 2,500 / 5,000 / 10,000 / 15,000 | 300 s each | 4 | | Where any metric starts to bend |
| S2 soak | 15,000 | 1,200 s | 4 | | **Target**: stable 15k |
| S3a-b headroom | 20,000 / 25,000 | 300 s each | 4 | before S3a: upload `core-config-throughput.yaml`, reset Core as in [Phase 2](#7-phase-2-optional-same-stack-after-s2); stop early if `unconsumed` climbs every window | Breaking point and which side breaks |
| S4 isolation | 15,000 | 300 s | 4 | `watch_probe.py --slow 0.2` + Telemetry UI open | A slow subscriber only gaps itself |
| S5 reliability (optional) | 15,000 | 300 s | 4 | `fault-inject.sh freeze` (or `stop`) in background; see [fault-inject.sh](fault-inject.sh) | No loss across DB stall and Core restart; does preview stall at 15k |

S4 probe, in two extra terminals: the SSH forward from [watch_probe.md](watch_probe.md), then
`/tmp/lt-venv/bin/python load-test/watch_probe.py --slow 0.2 | tee load-test/runs/S4/watch-probe.log`.
Core allows 4 subscribers (Telemetry + probe = 2).

S5: run `load-test/fault-inject.sh freeze runs/S5/fault.log` alongside generators (`freeze` stalls InfluxDB without
stopping Core; `stop` also stops Core because `chronoracer-core.service` `Requires=influxdb3.service`). 30 s stays
under the 60 s heartbeat write-failure threshold. Re-check the earlier rejection in
[core-latency-options.md](../scratch/core-latency-options.md#rejected): preview is expected to stall once
the ~5 s persistence buffer fills.

Live view during runs: generator `[Report]` lines in `runs/<ID>/gen-*.log`, and
`gcloud compute ssh core --zone=europe-west1-b --tunnel-through-iap -- sudo journalctl -fu chronoracer-core -o cat | grep --line-buffered -E 'Pipeline report|WARN|ERROR'`.

## 4. After each run

Prefer `load-test/run-scenario.sh` (sleep, collect, count, `steady_summary.py`). Manual steps: [observability.md](observability.md#persisted-point-count-loss-check).

**Primary loss check:** `WriteMetricSnapshot` accepted-line deltas in `core.log` (see observability). **Secondary:**
`influx-count.csv` from [influx_count.sh](influx_count.sh) — one query per table; do not use a single UNION on very
large databases (can exhaust Core VM memory).

## 5. Reading the result

Field meanings: Core [Pipeline report](../docs/component-core.md#pipeline-report), generator
[Report](../curls/load_test/README.md#report).

| Symptom | Bottleneck |
| --- | --- |
| `behind_max_s` grows, Kafka and Core idle | Generator: add instances |
| `queued` and `delivery_ms` grow, Kafka VM CPU or network high | Kafka (n4-standard-2) or laptop uplink |
| `unconsumed` grows, `sink_blocked_pct` ≈ 0 | Core consumption (fetch) |
| `unconsumed` grows, `sink_blocked_pct` high, `write_ms` up, `written_lines_s` < `decoded_pts_s` | InfluxDB / persistence |
| `unconsumed` grows, `sink_blocked_pct` high, `write_ms` flat, Core CPU high | Core decode |
| `telemetry_gap` in Cloud Run logs, Core fine | Telemetry service (1 CPU / 512 Mi, sizing TODO in `infra/race/telemetry.tf`) |

S2 passes ("stable 15k") when, over all windows after the first minute:

- summed generator `sent_pts_s` ≥ 15,000, `behind_max_s` < 1 s, `failed` = 0;
- Core `decoded_pts_s` and `written_lines_s` track the sent rate, `unconsumed` has no upward trend;
- no `Commit position has not advanced` warnings;
- InfluxDB count equals the generator total;
- Core and Kafka VM CPU below 80%.

Record per scenario: max sustained rate, `preview_ms` p50/p99, `persist_ms` p99, `sink_blocked_pct`, VM CPU,
Telemetry gaps. Final evidence goes to [adr-vm-types.md](../docs/adr-vm-types.md).

## 6. Artifacts (per run, `load-test/runs/<ID>/`)

| Path | Content |
| --- | --- |
| `run-metadata.json` | UTC start/end, rate, instances, seeds, git commit |
| `gen-<i>.log` | Generator `[Report]`, `[Summary]`, `[Sensors]` lines |
| `watch-probe.log` | S4 only: probe rate, gaps, end-to-end lag |
| `artifacts/logs/` | `core.log`, `pipeline-report.log`, `offset-lag.log`, `core-warn-error.log`, `influxdb3.log`, `kafka.log`, `telemetry.log` |
| `artifacts/metrics/` | CSV per metric: Core/Kafka CPU and memory, Kafka disk and network, `influx_write_*`, Telemetry Cloud Run |
| `artifacts/pipeline-report.csv`, `generators.csv` | Parsed per-window rows |
| `artifacts/influx-count.csv` | Per-sensor and total persisted points |
| `artifacts/summary.txt` | Medians, maxima, worst p99, warning counts |

## 7. Phase 2 (optional, same stack, after S2)

Upload `core-config-phase2.yaml` over the race overlay, then `gcloud compute instances reset core --zone=europe-west1-b`
(boot re-renders config; InfluxDB data survives on the disk). Re-run S2 for 600 s as `S2-p2` and compare
`preview_ms` and Kafka VM CPU with S2.

# 8. Absolute throughput testing and system overloading

Run with v1 or v2 config (pick better for throughput based on previous phases). Goal: test how many points per second
can the system handle and test behaviour system when overloaded.

## 8. Run the export flow (send SHUTDOWN to Core) - monitor if sensor data exported

## 9. Ensure all metrics collected from all VMs, GCP. Everything that can be checked was checked

##  10. Teardown

Targeted destroy in [infra-readiness.md](infra-readiness.md#targeted-apply--destroy-no-analysis-vm) (same target set,
`ingest_open=true`). Afterwards confirm no instances with `labels.race-id=${RACE_ID}`, no `telemetry` service, no
`chronoracer-kafka-public` rule, no `ingest` A record. Leave GCS bucket export content.
