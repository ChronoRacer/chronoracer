# Scratch - Core latency and throughput options

Working notes, not documentation. Owners: pipeline behaviour in [component-core.md](../docs/component-core.md),
measurement fields in [Pipeline report](../docs/component-core.md#pipeline-report), generator report in
[load test README](../curls/load_test/README.md). Load-test evidence goes into
[adr-vm-types.md](../docs/adr-vm-types.md) once measured.

## Goal

- Every point eventually lands in InfluxDB (at-least-once). InfluxDB write speed itself does not matter.
- Points reach Telemetry and Analysis (the preview stream) as fast as possible.
- Stable 15k points/s total ingest (several generator instances in parallel).

Baseline evidence: at 2.5k points/s (30 sensors, 700k points) the whole VM stayed under 8% CPU and 11% memory, and
unsettled offsets stayed under 50 per partition. At about 500k messages over 5 min (7 sensors, fast path on), under
14% CPU and 50% memory. Core was not the bottleneck at those rates.

## Where preview latency comes from

Path: producer, Kafka, data consumer fetch, sink batch, rayon decode, preview offer. Persistence comes after the offer
and does not delay it unless the bounded channels are full.

| Stage | Wait today | Why |
| --- | --- | --- |
| Producer window | up to `batch_window_ms` (load test 500 ms, contract allows up to 2 s) | Readings are batched at the source. Dominates end-to-end latency, outside Core. |
| Data consumer fetch | about 12 ms median, 25 ms worst | `fetch_min_bytes: 32768` is never reached at these rates, so every fetch waits the full `fetch_wait_max_ms: 25`. |
| Sink batch timer | about 7.5 ms median, 15 ms worst | `message_batch_flush_interval_ms: 15`; `message_batch_size: 1000` is not reached. |
| Decode and offer | under 1 ms | Hand-written Avro decoder on rayon. |

Core's own fixed waits add up to about 20 ms median, about 40 ms worst. Everything else is sub-millisecond.

## Phase 2 options (parked until the load test shows the baseline)

Re-judge each against the pipeline report before applying.

1. **Data consumer long-poll.** `fetch_min_bytes: 1`, `fetch_wait_max_ms: 100` (100 is only a ceiling for an idle
   topic). The broker answers as soon as any data exists. Gain: about 12 ms median, 25 ms worst on `preview_ms`.
   Cost: more, smaller fetch requests; check Kafka VM CPU (already peaks around 50%).
2. **Sink timer 15 ms to 5 ms.** `message_batch_flush_interval_ms: 5`. Gain: about 5 ms median. Cost: smaller rayon
   batches, more tasks; irrelevant at about 1.5k msg/s.
3. **Control consumer busy fetch.** Docs state the control sink uses `fetch.min.bytes=1` / `fetch.wait.max.ms=0`
   ([component-core.md](../docs/component-core.md#data-pipeline-batch-contract)). With wait 0 the broker answers
   empty fetches immediately and librdkafka refetches in a tight loop. Verify via fetch request rate (librdkafka
   stats or broker request metrics). If confirmed: `fetch.wait.max.ms=500` with `fetch.min.bytes=1`. Same latency
   (the broker answers as soon as a record arrives), less CPU on Core and the Kafka VM. Update the control-path
   paragraph in component-core.md.
4. **systemd `CPUWeight`.** InfluxDB lower than Core in `core/startup/influxdb3.service` vs
   `core/startup/chronoracer-core.service`. Only acts under contention (InfluxDB snapshot/compaction spikes on 2
   physical cores). Protects `preview_ms` p99, no median gain. Document in component-core.md shared-VM paragraph.
5. **Only if the report shows InfluxDB limiting at 15k** (`sink_blocked_pct` high, `write_ms` high,
   `written_lines_s` below `decoded_pts_s`): raise `persistence.max_inflight_db_writes` (7) and
   `flush_max_combined_batch_size` (7000). Not a latency lever otherwise.

Expected total from 1 and 2: about 17 ms median off `preview_ms`. Small next to the producer window; worth it only
because it is config-only.

## Rejected

- **Parallel consumers or more partitions.** Consumption is not the limit at measured rates; ordering is not required,
  but adding consumers adds rebalance and offset-tracking surface for no measured gain. Revisit only if the report
  shows `unconsumed` growing with `sink_blocked_pct` near 0.
- **Thread tuning** (`rayon_threads`, `tokio_workers`, `tokio_blocking`). 4 vCPU / 2 physical cores, VM CPU at
  8-14%. No queueing on threads to remove.
- **Persistence micro-optimisations** (dropping `SimplePoint`, ryu/itoa formatting, prost `Bytes`, lock-free
  register). Off the preview path; persistence speed is not a goal.
- **InfluxDB tuning** (WAL interval, fast path, in-flight writes) for latency. Persistence does not delay the preview.
- **`no_sync: true`.** Offsets would settle before data is durable: breaks at-least-once.
- **Flush-on-drain in the sink.** About 2.5 ms better than a 5 ms timer, for new code.
- **Decoupling the preview from persistence backpressure.** At 2.5k points/s the bounded channels absorb about 28 s
  of InfluxDB stall before the preview stalls. Caveat: that buffer is point-bounded, so at 15k points/s it is about
  5 s. Re-check with the load test: if `preview_ms` tracks `persist_ms` during InfluxDB hiccups at 15k, reopen.

## Reliability flag (outside perf scope, unverified)

[component-core.md](../docs/component-core.md#data-pipeline-batch-contract) says reading values are trusted,
"including NaN and infinities". InfluxDB line protocol most likely rejects `NaN`/`inf` float fields, so those lines
come back as a partial write and are dropped (counted, logged, offsets settled). They appear on the preview but never
in InfluxDB, which contradicts "every point eventually lands in InfluxDB". Verify against InfluxDB 3 Core; if
confirmed, decide between rejecting such readings at decode (DLQ) or documenting the drop.

## Validation steps

1. Operator YAML for the run: `kafka.sensor_data_consumer.offset_lag_log_interval_s: 10`.
2. Generators on NTP-synced GCP VMs, several instances summing to 15k points/s, `--report-interval 10`.
3. Baseline run with current config. Read, per window:
   - generator `behind_max_s` flat and `sent_pts_s` at target (else add instances);
   - generator `delivery_ms` and `queued` stable (else Kafka);
   - Core `unconsumed` stable, `consumed_msg_s` matching sent, `sink_blocked_pct` low (else Core or InfluxDB, see
     [Pipeline report](../docs/component-core.md#pipeline-report));
   - `decoded_pts_s` and `written_lines_s` matching sent over the run;
   - `preview_ms` p50/p99 as the latency baseline; Core VM and Kafka VM CPU.
4. Apply options 1 and 2 (and 3 if confirmed), re-run, compare `preview_ms` and Kafka VM CPU.
5. Record before/after in adr-vm-types.md.
