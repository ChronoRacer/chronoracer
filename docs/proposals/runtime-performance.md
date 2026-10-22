# Proposal - Core and Kafka VM runtime performance

Status: **proposed, not implemented.** Items 1-4 were exercised in the load test as config or test-only overrides;
items 5-7 are derived from observed failures and need a verification run. The existing [docs](../README.md) remain the
source of truth for the running system.

Scope: runtime of the Core VM (Core + InfluxDB) and the Kafka VM. The export flow, Telemetry and Analysis are out of
scope. Evidence: [load test results](../load-test-results.md) (numbers below are quoted from there, run IDs in
brackets). Supersedes the working notes in [core-latency-options.md](../../scratch/core-latency-options.md).

Baseline: the shipped config already sustains the 15k pts/s target with every point persisted and Core below 10% CPU.
The changes below buy latency, Kafka headroom, and above all a bounded failure mode for InfluxDB memory.

## Summary

| # | Change | Where | Measured / expected effect | Priority |
| --- | --- | --- | --- | --- |
| 1 | Control consumer long-poll | Core code | Kafka idle CPU ~41% and ~2.6 MB/s idle traffic removed (expected) | High |
| 2 | Data path latency defaults | Core config | preview mean 37 -> 19 ms, p99 <=100 -> <=50 ms at 15k [S4 vs S2-p2] | High |
| 3 | Persistence batch 7,000 -> 50,000 | Core config | write ceiling ~98k -> ~700k lines/s; held 200k [L200] | High |
| 4 | InfluxDB WAL flush 1 s -> 100 ms | Core VM setup | write_ms mean ~600 -> ~125 ms [S4, S2-p2 vs L50-L200] | Medium |
| 5 | Bound InfluxDB memory | Core VM setup | turn the VM-wide hang at ~167M points into a contained InfluxDB restart | **High** |
| 6 | Decouple Core from the InfluxDB unit | Core VM setup + Core code | no Core outage on InfluxDB stop/restart [S5, L50 setup] | Medium |
| 7 | Tight time sync on both VMs | Images | Core clock offset +19.9 ms -> <0.2 ms; latency metrics trustworthy [S3a vs S4] | Medium |

Items 2-3 together are the tested `core-config-phase2.yaml` overlay of the [load-test harness](../../load-test/).

## 1. Control consumer long-poll

The control sink sets `fetch.min.bytes=1`, `fetch.wait.max.ms=0`
([`message_sink.rs`](../../core/src/messaging/message_sink.rs), `InstantMessageSink::new`). With wait 0 the broker
answers an empty fetch at once and librdkafka refetches in a tight loop.

Evidence: in every idle window of the test the Kafka VM sat at 40-43% CPU with 2.5-2.7 MB/s inbound, and the Core VM
at ~6%, unchanged between the baseline and phase-2 data-consumer settings, so the data consumer is not the source.
An idle single-broker KRaft node should be in single digits.

Change: `fetch.wait.max.ms=500`, keep `fetch.min.bytes=1`. The broker still answers as soon as one control record
exists, so CHDRV and SHUTDOWN latency do not change; only empty polls slow down. Update the control-path paragraph in
[component-core.md](../component-core.md#data-pipeline-batch-contract).

Expected: Kafka idle CPU to single digits and the same absolute drop under load (Kafka at 200k pts/s was ~76% [L200],
so roughly 40% after). Estimate; verify with an idle window plus a CHDRV smoke run.

## 2. Data path latency defaults

Make the tested phase-2 values the defaults in [`default-config.yaml`](../../core/default-config.yaml) and the
`KafkaConsumerConfig` `Default` impl in [`configuration.rs`](../../core/src/configuration.rs) (both carry the
consumer defaults today):

| Key | Today | Proposed |
| --- | --- | --- |
| `kafka.sensor_data_consumer.fetch_min_bytes` | 32768 | 1 |
| `kafka.sensor_data_consumer.fetch_wait_max_ms` | 25 | 100 (ceiling for an idle topic only) |
| `processing.message_batch_flush_interval_ms` | 15 | 5 |

Measured at 15k, same persistence config and synced clocks: preview mean 37 -> 19 ms flat over 10 min, p50 <=50 ->
<=20, p99 <=100 -> <=50 ms; cost +2.4 points Kafka CPU, +0.9 points Core CPU [S4 vs S2-p2]. Preview stayed at
~20 ms mean up to 200k pts/s [L50-L200]. The remaining ~19 ms is mostly outside Core (producer and network path).

## 3. Persistence batch size

`persistence.flush_max_combined_batch_size` 7,000 -> 50,000 in `default-config.yaml`.

InfluxDB acknowledges a write on its WAL flush, so `write_ms` is about the same at any batch size and throughput per
write permit is batch size divided by that wait. 50,000 lines are ~3.9 MB (~78 B/line), under InfluxDB's 10 MB
`--max-http-request-size`. With it Core accepted 200k pts/s with `sink_blocked` 0 [L200] and the bounded channels
absorbed a 30 s InfluxDB freeze without stalling the preview [S5b]. `max_inflight_db_writes` (7) stays: under one
write is in flight on average at 200k.

Side effect: fewer, larger WAL files mean InfluxDB snapshots less often and its write buffer grows larger between
snapshots. Ship together with item 5.

## 4. InfluxDB WAL flush interval

Add `--wal-flush-interval 100ms` to `ExecStart` in
[`influxdb3.service`](../../core/startup/influxdb3.service) (a value in `influxdb3.env` is lost on boot:
[`start-influx.sh`](../../core/startup/start-influx.sh) rewrites the file). The
[InfluxDB 3 Core docs](https://docs.influxdata.com/influxdb3/core/reference/config-options/#wal-flush-interval)
recommend 100 ms for local disks; we run the 1 s default.

Measured (L-runs at 50k-200k vs earlier runs at 15-25k): `write_ms` mean ~600 -> ~125 ms. The persist max drop
(~4.3 -> ~0.9 s) is partly the rate itself: a 50,000-line batch fills in ~1 s at 50k pts/s but in ~3.3 s at 15k.
Persistence speed is not a goal, but settling sooner shrinks the replay window after a crash and the points held in
Core's channels.

## 5. Bound InfluxDB memory

Failure: at 167,370,862 stored points, with InfluxDB's write buffer at its 50% force-snapshot cap, one count query
over all tables drove the Core VM into memory thrash (no swap, no OOM kill). Core, Ops Agent and SSH stopped and the VM
never recovered [L200 end]. Under normal load the VM already peaked at 73-81% [L150, L200]. InfluxDB runs on defaults:
write buffer 50% + query pool (`exec-mem-pool-size`) 20% + data file cache (`file-cache-size`) 20% = 90% of RAM before
Core, the Ops Agent and the OS.

Change, in order of importance:

1. Contain InfluxDB in its own cgroup: `MemoryHigh=` and `MemoryMax=` in `influxdb3.service` (start with 65% / 75% of
   RAM). Under pressure the kernel throttles, then OOM-kills InfluxDB only; `Restart=on-failure` restarts it and the WAL
   (writes acknowledged after WAL flush) is replayed. Core keeps its batches and retries, and Kafka buffers meanwhile
   (shown for a 30 s outage in [S5b]).
2. Protect Core: `OOMScoreAdjust=-900` in [`chronoracer-core.service`](../../core/startup/chronoracer-core.service).
3. Size InfluxDB below the cgroup limit: `--force-snapshot-mem-size 30%`, `--file-cache-size 10%` (Core never queries
   InfluxDB at runtime), `--exec-mem-pool-size` stays 20%, and `--force-snapshot-max-age 10m` so an idle node does
   not hold (and on restart replay) up to the default 1 h of data. InfluxDB startup with WAL replay took 12.6 s with the
buffer near its cap [L50 setup].

The values are starting points. Verification: repeat L150 for 10 min, then the count query from the
[harness](../../load-test/), and confirm the VM stays responsive (InfluxDB may restart; Core must not).
Document the resulting memory budget in [component-influxdb.md](../component-influxdb.md), which has no memory section
today.

## 6. Decouple Core from the InfluxDB unit

`chronoracer-core.service` has `Requires=influxdb3.service`. An explicit stop or restart of InfluxDB therefore stops
or restarts Core. On stop, Core exits cleanly and `Restart=on-failure` does not bring it back: Core was down 89 s until
restarted by hand [S5]. At runtime the coupling is not needed: Core rode through a 30 s InfluxDB freeze with no
preview impact [S5b].

Change: `Requires=` -> `Wants=` (keep `After=`). Then the start order still holds, but Core survives InfluxDB
restarts, including the OOM restarts from item 5.

Core startup: `create_influxdb_client` in
[`persistence_entrypoint.rs`](../../core/src/persistence/persistence_entrypoint.rs) fails init on the first refused
ping, so Core crash-loops every 5 s until InfluxDB has replayed its WAL (3 failed starts in [L50 setup]). Retry the
ping with backoff for a bounded time (for example 180 s, matching `data_pipeline_drain_timeout_s`) before failing.
Document the dependency in [core/startup/README.md](../../core/startup/README.md).

## 7. Time sync on both VMs

Both images use `systemd-timesyncd` against the metadata server with a 17-34 min poll. Core's clock drifted to
+19.9 ms (jitter 7.5 ms), and every latency Core reports is measured from producer record timestamps, so
`preview_ms` and `persist_ms` were off by tens of ms. The latency values from S0-S3b are not usable in absolute terms.
A drop-in with `NTP=metadata.google.internal`, `PollIntervalMinSec=16` and `PollIntervalMaxSec=32` held offsets
under 0.2 ms [S4 onward].

Change: bake that drop-in (or chrony with the metadata server) into the Core and Kafka images in
[`infra/packer/scripts`](../../infra/packer/scripts/), and note it in [component-images.md](../component-images.md).

## Not proposed (data says no gain)

| Option | Why not |
| --- | --- |
| Thread tuning, more partitions or consumers | Core CPU <=24% and `unconsumed` <=13 at 200k pts/s [L200] |
| Raising `max_inflight_db_writes` | Under one write in flight at 200k |
| Raising InfluxDB's 50% buffer cap | No ingest gain (ack is the WAL flush); eats the query and cache budget (item 5) |
| `CPUWeight` for InfluxDB vs Core | No CPU contention measured; Core VM CPU <=24% |
| Bigger Kafka VM | ~76% at 13x target today, ~40% of that is the idle busy fetch; re-measure after item 1 |
| Re-keying to fix the 2.8x partition skew | No measured effect with a single consumer |
| Core VM type change | CPU is idle, memory is the constraint; decide in [ADR - VM types](../adr-vm-types.md) after item 5 is measured |
