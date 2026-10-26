# Race-stack load test harness

This directory owns load-test orchestration and evidence collection for Kafka, Core/InfluxDB and Telemetry.
The Kafka producer is in [`curls/load_test/`](../curls/load_test/README.md).

Use one operator overlay, [`load-testing-core-config.yaml`](load-testing-core-config.yaml), for the entire campaign.
Run **100K, 150K, 200K, 250K points/s**, then fault injection at a demonstrated sustainable rate, then race-end export.
Procedure and pass criteria belong to the [runbook](runbook.md). Metric interpretation and collection belong to
[observability](observability.md). Infrastructure commands belong to [infra-readiness](infra-readiness.md).

From the repository root:

```bash
python3 -m venv /tmp/lt-venv
/tmp/lt-venv/bin/pip install -r load-test/requirements.txt
gcloud auth application-default login
export PYTHON=/tmp/lt-venv/bin/python
export RACE_ID=<race-id>
export OKTA_CLIENT_SECRET="$(gcloud secrets versions access latest --secret=chronoracer-okta-oauth --project=chronoracer)"
```

Complete the runbook readiness checks before `load-test/run-scenario.sh L100 100000 600 4`.
Scripts use `PROJECT=chronoracer`, `REGION=europe-west1`, `ZONE=europe-west1-b` and `TELEMETRY_SERVICE=telemetry` by default.
An alternate environment also needs matching generator broker/OAuth settings via `GENERATOR_CONFIG`.

| File | Responsibility |
| --- | --- |
| `run-generators.sh` | Parallel generators, exact requested rate split, metadata and delivery summary |
| `run-scenario.sh` | One load run, optional fault, delayed collection and Monitoring summary |
| `fault-inject.sh` | Separate DB freeze, DB stop and Core restart experiments |
| `collect-artifacts.sh` | Monitoring series, diagnostic logs and generator evidence |
| `collect_metrics.py` | Raw CSV with resource/metric labels, counter epochs and complete distributions |
| `steady_summary.py` | Monitoring rates, lag, merged latency histogram and coverage |
| `summarize_generators.py` | Local producer reports and final delivery totals |
| `export_flow.py` | Explicit SHUTDOWN, export wait, GCS inventory and publish evidence |
| `influx_count.sh` | Optional per-table unique stored-row count; expensive at high volumes |
| `bench_generator.py` | Local encoding benchmark; does not establish end-to-end generator capacity |
| [watch_probe.md](watch_probe.md) | Direct Core subscriber setup and isolation check |
| `runs/` | Gitignored evidence; use a new run ID for every experiment |

Offline verification (no cloud operations):

```bash
/tmp/lt-venv/bin/python -m unittest discover -s load-test -p 'test_*.py'
bash -n load-test/run-scenario.sh
shellcheck -x load-test/*.sh  # optional local lint tool
```
