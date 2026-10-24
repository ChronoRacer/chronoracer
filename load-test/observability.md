# Load-test evidence and Monitoring interpretation

The metric contract, kinds, units, cadence and outcome semantics are owned by
[Core's Cloud Monitoring documentation](../docs/component-core.md#cloud-monitoring-write-counters).
This document owns how the harness collects and interprets that evidence.

## Collection

`run-scenario.sh` invokes `collect-artifacts.sh` after the run. Defaults are 120 s before generation and 180 s after
final producer flush, with a 420 s wait after generation (drain padding plus native metric visibility latency).
`METRIC_WAIT_S` overrides the wait; increasing the collection's after-padding also requires a longer wait.
These delays do not guarantee drain: inspect post-run lag and re-collect if needed.

Monitoring uses Application Default Credentials (`gcloud auth application-default login` for an operator).
Cloud Logging/SSH use gcloud credentials. The operator needs Monitoring time-series read, Logging read,
Compute instance discovery, IAP SSH, bucket read/write for baseline and evidence, plus the infrastructure permissions
in [infra-readiness](infra-readiness.md). Core's own metric-write permissions are part of the deployed service account.

```bash
# After the padded window and visibility lag have elapsed:
RACE_ID=<race-id> load-test/collect-artifacts.sh load-test/runs/L100
/tmp/lt-venv/bin/python load-test/steady_summary.py load-test/runs/L100

# Standalone raw Monitoring export:
/tmp/lt-venv/bin/python load-test/collect_metrics.py --race-id <race-id> \
  --start '<RFC3339-with-timezone>' --end '<RFC3339-with-timezone>' \
  --output-dir load-test/runs/L100/artifacts/metrics
```

All seven Core metric families are collected, along with Core/Kafka host CPU, memory, disk, network, and native
Telemetry Cloud Run instance count, request count and request latency. VMs are resolved by race label, then queries
use numeric instance IDs. Cloud Run queries use service and region and retain revision/resource labels.
No cross-series reduction or API alignment is applied.

CSV columns include interval start/end, metric kind/value type, resource type/labels, metric labels and value.
Labels are JSON, so `outcome`, memory `state`, disks, interfaces and Cloud Run revisions remain separate.
Distribution values retain bucket options/counts and moments as JSON. Empty CSVs are **missing data**.
`metrics/meta.json` records point counts, missing series and individual API errors; collection returns nonzero for
missing required Core scalar families or API errors. Missing idle latency or Cloud Run activity is recorded without
inventing samples. The steady summary separately requires usable in-run samples.

## Steady summary

The first 60 s are warmup. Cumulative rates use adjacent samples in the same resource/metric-label set and process
epoch. Only intervals fully inside the steady window are used, avoiding interpolation at the edges. Counter resets
are identified by `start_time`, including a restart whose new value exceeds the previous value.
Sink blocked milliseconds divided by elapsed seconds and by ten gives percentage.

Lag is a gauge: first/last/max describe sampled backlog, not a count of lost points. Missing lag remains unknown.
Post-run values help check drain; inspect the full raw series when a fault creates a short spike between samples.
Persistence latency distributions are merged by bucket counts, with count-weighted means. Percentiles are bucket
bounds, including an explicit overflow indication; they are not exact percentiles or averages of per-window p99s.
Gauge distributions carry only an end timestamp. The summary uses the co-published consumed-message counter's
previous sample (or process epoch start) as a conservative window start; without those boundaries, it excludes the
histogram. Counter rates spanning missing publications include idle time in that gap and cannot establish steady
throughput for a shorter run.
Samples represent batches, not every individual point. Missing distribution windows cannot be recovered from counters.

Current metrics do not provide decoded-point throughput, preview latency or write-RPC latency. Those historical
fields are listed as unavailable in the summary. Direct [Watch probe](watch_probe.md) lag includes generator reading
batch age and transport delay; it is a separate measure from Core persistence latency.

Producer evidence remains local because the producer does not publish Monitoring metrics. Final `[Summary]` counters
are authoritative delivery totals; periodic `failed` values are window counts. Never add summary totals to report
counts. Aggregate steady producer rate uses each process's elapsed report windows, not an assumed six reports per
minute. Report-index CSV output is diagnostic only; processes do not have identical UTC report boundaries.

## Loss checks

Under exclusive traffic, an idle pre-run accepted-lines sample and a fully drained post-run sample enclose the
producer's actual point total. `accepted_lines_check` records these timestamps and compares cumulative increments.
The enclosing Monitoring window is deliberately wider than generation; do not rate-interpolate it into an exact
per-run counter. A missing baseline, no final sample, unexplained reset, undrained backlog or competing traffic makes
an exact loss claim inconclusive. The summary never substitutes zero for missing boundaries.

Accepted lines count acknowledgements, including replayed writes. They are not unique stored points. Core restart
may replay unsettled messages, and abrupt termination may lose the previous process's unreported increments.
Counter epochs let the harness recover observed post-restart increments but cannot reconstruct those missing values.
Use persisted rows/Parquet reconciliation to prove loss or duplication through restart faults.

`INFLUX_COUNT=1 load-test/run-scenario.sh ...` opts into per-table DB queries. This is disabled by default because
large count queries previously exhausted the Core VM. Queries run sequentially; never replace them with a giant
UNION. Generation windows include a one-second reading-time pad for the configured 500 ms producer batch window.
Overlapping runs, changed batch windows and equal sensor/time/tag keys can invalidate a simple count comparison.
Influx deduplicates identical keys, so count mismatches need analysis rather than an automatic data-loss conclusion.

## Diagnostic logs

`artifacts/logs/` contains Core/Kafka journal messages from Cloud Logging, structured Telemetry JSON, offset-lag
warnings and Core warnings/errors. Logs explain retries, DLQ messages, restarts and exports; they are not the source
for throughput or latency summaries. A `Pipeline report:` line may still appear at publication time, but old
per-point fields are gone and the harness does not parse those lines.

Core and Kafka Ops Agent log routing is owned by their startup configurations. Influx journal logs are not shipped;
for a fault/export window use the Core VM journal directly:

```bash
gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap \
  --command "sudo journalctl -u influxdb3 --since '<START>' --until '<END>' --no-pager -o short-iso"
```

Inspect `core-warn-error.log` for publisher errors even when write counters exist. Export verification uses a journal
cursor captured immediately before SHUTDOWN, avoiding matches from earlier runs.

## Provider references

The collector follows Google's [timeSeries.list API](https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list):
no aggregation returns raw samples, and returned points must be sorted chronologically before differencing.
[Metric latency](https://cloud.google.com/monitoring/api/v3/latency-n-retention) distinguishes custom metric visibility
from native service lag; consult the [Compute metrics](https://cloud.google.com/monitoring/api/metrics_gcp_c) and
[Cloud Run metrics](https://cloud.google.com/monitoring/api/metrics_gcp_p_z) for sampling/visibility details.
