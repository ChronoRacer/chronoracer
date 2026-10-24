# Core load-test runbook

Use [`load-testing-core-config.yaml`](load-testing-core-config.yaml) throughout. Tune if necessary.
Configuration semantics: [Core startup](../core/startup/README.md). Evidence interpretation:
[observability](observability.md). Historical measurements: [previous results](../docs/load-test-results.md).

When testing, ALWAYS keep track of the progress in a separate PROGRESS.md file in the current workspace. Update it,
alongside with measurements from each stage before you start the next one.

## 1. Ready check

Install dependencies and credentials from [README](README.md). Run generators on an NTP-synchronized host with
sufficient CPU/uplink; use actual producer reports to establish capacity. Check `timedatectl` on the generator
and both VMs. Current images own VM clock configuration; no test-only NTP/WAL changes are required.

Upload the baseline **before** raising the stack:

```bash
gcloud storage cp load-test/load-testing-core-config.yaml "gs://chronoracer-race/${RACE_ID}/core-config.yaml"
```

Follow [infrastructure readiness](infra-readiness.md#targeted-apply--destroy-no-analysis-vm).
Use the current published Core payload and images. Record deployed release, effective non-secret configuration,
VM machine types and image IDs, Influx runtime settings, and generator host details with the campaign evidence.
Do not copy the merged config's DB password into artifacts. Restarting Core alone does not re-render its config;
if changing the GCS overlay on an existing VM, follow the [startup procedure](../core/startup/README.md).

Verify:

- DNS resolves to the current Kafka public IP; OAuth/TLS producer authentication works.
- Both VM labels match `RACE_ID`; Core is active and its `data` topic has the configured partition count.
- Telemetry logs show subscription to Core; open its UI for the campaign.
- Core Monitoring series appear for the current **numeric instance ID**, including idle write counters and lag.
  Wait at least two publication intervals before the first run to establish an idle counter baseline.
- No competing producer traffic, no old backlog, enough DB disk/memory headroom for the campaign and export.

## 2. Throughput steps

Run sequentially, inspecting each summary before increasing the rate:

| ID   | Points/s | Duration | Initial generator processes |
|------|---------:|---------:|----------------------------:|
| L100 |  100,000 |    600 s |                           4 |
| L200 |  200,000 |    600 s |                           4 |
| L250 |  250,000 |    600 s |                           4 |
| L350 |  350,000 |    600 s |                           5 |

```bash
load-test/run-scenario.sh L100 100000 600 4
load-test/run-scenario.sh L200 200000 600 4
load-test/run-scenario.sh L250 250000 600 4
load-test/run-scenario.sh L350 350000 600 5
```

Each command waits for a padded drain window and metric visibility, collects evidence and writes
`artifacts/steady-summary.json`. The first 60 seconds are warmup; only full metric intervals inside the remaining
window contribute rates/latency. A nonzero exit means generator, fault, collection or coverage failure; inspect the
logs before proceeding. A zero exit means the workflow completed; **evaluate acceptance below separately**.
Increase generator processes and repeat with a new ID if they miss the target; do not interpret generator limits as
Core capacity. The short local encoding benchmark cannot validate the OAuth/Kafka/network path.

For each step, require:

- Producer steady achieved rate tracks the requested rate (allow report rounding), pacing lateness stays below 1 s,
  all process summaries exist, final queues are empty, and failed deliveries are zero.
- Monitoring accepted-line rate tracks actual production, lag stays bounded and returns to its idle baseline after
  the run, and there are no unexplained partial/error writes, stuck-partition warnings or publisher failures.
- The padded-window accepted-lines delta equals generated points under exclusive traffic and drained boundaries.
  Missing boundary samples or resets with lost final samples make this check inconclusive. See [loss checks](observability.md#loss-checks).
- Record persistence latency histogram bounds, sink-blocked percentage, CPU, memory, disk, retries and Telemetry gaps.
  Use CPU below 80% as the campaign headroom criterion; record overload behavior separately from a passing step.

If lag is still growing or the VM is under memory/disk pressure, stop increasing load. Extend drain observation and
re-collect with a larger padding window before the next run; do not let an overload contaminate the next baseline.
At these rates the campaign can accumulate hundreds of millions of points. Avoid routine full DB counts.

Update the statistics in PROGRESS.md after each stage.

## 3. Fault injection

After the throughput steps, select the highest rate that met the criteria. Example commands below use 200K;
substitute the demonstrated rate. Each experiment is separate, at the **same baseline configuration**:

```bash
load-test/run-scenario.sh F-freeze 200000 300 4 freeze
load-test/run-scenario.sh F-stop 200000 300 4 stop
load-test/run-scenario.sh F-restart 200000 300 4 restart
```

Faults occur approximately 120 s after launching the workflow: freeze/stop Influx for 30 s, or restart Core once.
`fault.log` records actual action times. Current Core uses `Wants=influxdb3`; stopping Influx should leave Core running.
The DB recovery trap runs on the VM even if SSH disconnects. After an interrupted fault, explicitly verify that
Influx is running and not left suspended. Thirty seconds is shorter than the default heartbeat write-failure threshold;
check the deployed settings if this baseline changes.

Require producer delivery success, recovery without manual intervention, bounded backlog drain, continued Core
liveness during DB faults, and persistence progress after recovery. During a Core restart, counters start new epochs;
replay can increase accepted lines. Monitoring cannot prove exact unique-row loss across a missing final sample.
Use DB/Parquet reconciliation for that claim. Record retries, restarts, lag/latency recovery and any DLQ entries.

For subscriber isolation, optionally run the [slow Watch probe](watch_probe.md) during a repeated sustainable-rate
run, after the throughput steps. Gaps for the slow subscriber must not block persistence or healthy subscribers.
There is no Core preview-latency metric; the probe measures its own end-to-end delivery lag.

## 4. Export flow (last)

Stop all producers and confirm backlog has drained. This step ends the race; signals/restarts do not trigger export.
Use a unique race ID so earlier export objects cannot mask failure. Keep the manual stack available until evidence
has been collected; Relay/controller lifecycle testing is separate from this direct Core export test.

```bash
/tmp/lt-venv/bin/python load-test/export_flow.py load-test/runs/F-freeze
```

The script reads deployed export paths/topics, confirms SHUTDOWN delivery, waits for completion, and saves journal,
GCS inventory and checks in `artifacts/export/`. Its default one-hour wait is an operator observation budget and can
be changed with `--timeout-s`; it does not override Core's per-attempt timeouts. A failed or interrupted invocation
must be inspected before retrying SHUTDOWN because it may already have started export.

Require complete database table export, all reported DB objects present, successful configured Kafka export, and
confirmed export-done publish. Kafka exports only configured topics: current defaults select `dlq`, so an empty
Kafka export is legitimate for valid input. Export-done alone is insufficient because policy can permit notification
after Kafka export failure. The script checks both independently.

Download representative DB Parquet files and verify schema, nonempty rows, sensor/driver IDs, and time bounds.
For a full loss claim, reconcile per-table Parquet counts/content with stored-row counts and generator evidence,
accounting for timestamp collisions and restart replay. File presence and a publish log do not prove content equality.
After metric visibility catches up, re-run collection with padding covering the export duration to capture the final
Core sample and export host load; run the summary again (its steady ingest interval remains unchanged).

## 5. Evidence and teardown

Keep metadata, generator logs/final summaries, clocks, `fault.log`, `collect.log`, `artifacts/metrics/meta.json`, raw CSVs,
diagnostic logs, `steady-summary.json`, and export evidence. Record unavailable metrics as unavailable, never zero.
Summarize the new campaign separately from the [historical results](../docs/load-test-results.md).

Follow [targeted destroy](infra-readiness.md#targeted-apply--destroy-no-analysis-vm) after collecting evidence.
Confirm the race VMs, Telemetry service, public ingest firewall and DNS record are gone. Retain GCS export objects.
