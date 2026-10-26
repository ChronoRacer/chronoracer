# Load test report: Core v0.3.2

Campaign `lt032`, 2026-10-26 (17:52Z–22:20Z). Goal: 500K points/s and higher if headroom allows. Steady window =
generation start + 60 s through end; Core metrics at 60 s intervals. Detail:
[campaign](runs/campaign.md), [throughput runs](runs/throughput.md), [fault injection](runs/fault-injection.md),
[export](runs/export.md). Procedure: [runbook](../../../load-test/runbook.md). Previous campaign:
[0.3.1](../core-0.3.1/core-0.3.1-results.md) (L200–L350 passed; not repeated).

## Summary

- **800,000 points/s** passed every runbook criterion with zero loss on the baseline Core config. With two
  test-only InfluxDB snapshot settings (L800d) the sink had **0% backpressure** at 800K.
- **900K** met the criteria (lag 0, zero loss) with end-to-end latency degraded by the generator uplink.
  **1M overloaded** Core/InfluxDB persistence (~962K accepted lines/s, sink blocked 92%), without loss or errors.
- The generator host's **~100 Mbit/s uplink** caps producer → Kafka at ~13 MB/s; producer delivery latency rises
  from 800K. Not a stack limit.
- **Fault injection at 800K passed** for InfluxDB freeze, InfluxDB stop and Core restart: no loss, no producer
  errors, automatic recovery, backlogs of 1.4–1.7M messages drained within ~3.5 min.
- **Race-end export failed** (38/38 tables, two attempts) on InfluxDB query memory, caused by Core's per-table
  `min(time)/max(time)` query at ~47M rows per table. Fixed in Core 0.3.3: [follow-up](runs/export.md#follow-up-core-033-local-validation-only).
- InfluxDB forced snapshots explain the periodic CPU/sink spikes seen at 800K.

## Setup

Baseline: [campaign — deployed baseline](runs/campaign.md#deployed-baseline).

| Item | Value |
|------|-------|
| Core + InfluxDB 3 (3.12.1) VM | `c4d-highcpu-4`, 20 GB hyperdisk-balanced (3,120 IOPS / 170 MiB/s); WAL flush 100 ms, cgroup MemoryHigh 70% / MemoryMax 80% |
| Kafka VM | `n4-standard-2`, topic `data`, 4 partitions |
| Core config | processing batch 1000 / 5 ms, persistence flush 50,000 lines / 4 s, 7 in-flight DB writes |
| Generator | WSL2 (Ubuntu 26.04), 28 logical CPUs, **~100 Mbit/s uplink**, 8–18 processes, driver change after 60 s |

## Throughput steps

Canonical steady-window metrics for all runs. Extended evidence (network, 1 s sampling, snapshot A/B, producer
delivery buckets): [throughput runs](runs/throughput.md). Latency percentiles are histogram bucket upper bounds.
L800c–L1000 ran 300 s (operator choice); others 600 s.

| Run | Target pts/s | Procs | Influx settings | Points | Accepted lines (median) | Sink blocked median / max | Lag max / post | Persist mean / p99 bound | Core CPU median | Kafka CPU median | Verdict |
|-----|-------------:|------:|-----------------|-------:|------------------------:|--------------------------:|---------------:|-------------------------:|----------------:|-----------------:|---------|
| L500 | 500,000 | 8 | baseline | 300,000,017 | 500,060 | 0% / 0% | 11 / 0 | 372 ms / 1 s | 37.6% | 51.7% | pass |
| L650 | 650,000 | 11 | baseline | 390,000,023 | 650,080 | 0% / 0% | 0 / 0 | 492 ms / 2 s | 48.8% | 60.1% | pass (harness exit 1: Logging 500; re-collected) |
| L800 | 800,000 | 14 | baseline | 480,000,051 | 800,104 | 8.2% / 12.5% | 0 / 0 | 1,559 ms / 5 s | 59.5% | 53.5% | pass, backpressure |
| L800b | 800,000 | 14 | WAL files/snapshot 200 | 480,000,051 | 799,687 | 6.3% / 9.9% | 5 / 0 | 1,518 ms / 5 s | 59.4% | 53.0% | pass, no change |
| L800c | 800,000 | 14 | concurrency 3 | 240,000,035 | 791,769 | 0.04% / 0.3% | 500 / 19 | 1,458 ms / 5 s | 61.1% | 55.2% | pass |
| L800d | 800,000 | 14 | concurrency 3, snapshot 15% | 240,000,047 | 800,946 | **0% / 0%** | 60 / 0 | 1,543 ms / 5 s | 60.3% | 54.2% | **pass** |
| L900 | 900,000 | 16 | as L800d | 270,000,039 | 895,105 | 1.9% / 4.6% | 0 / 0 | 15,456 ms / 60 s | 68.9% | 44.9% | pass, latency degraded |
| L1000 | 1,000,000 | 18 | as L800d | 298,513,744 | 962,629 | 92.1% / 92.6% | 217,715 / 0 | 20,129 ms / 60 s | 71.7% | 28.5% | **fail: overload** |

Common: 0 failed deliveries, final queues empty, 0 partial/error/retry InfluxDB writes, loss check pass (accepted-line
delta equals generator total, one counter epoch) in every run. Pacing lateness ≤0.15 s except L1000 (17.35 s).
CPU below 80% on both VMs throughout. Sink blocked in 0.3.2 counts only full-channel waits
([metric](../../component-core.md#cloud-monitoring-write-counters)).

Latency above 650K is mostly upstream of Core: producer delivery p50 moves from ≤50 ms (L650) to 100–500 ms (L800)
and up to ≤30 s windows (L900) while Kafka inbound stays at ~13 MB/s ([transport plateau](runs/campaign.md#producer--kafka-transport-plateau)).

## Fault injection (800K, 300 s, fault at ~+120 s, L800d InfluxDB settings)

Pipeline reports, sampling and journal detail: [fault injection](runs/fault-injection.md).

| | F-freeze (`SIGSTOP`) | F-stop (`systemctl stop influxdb3`) | F-restart (`systemctl restart chronoracer-core`) |
|---|---|---|---|
| Fault window | 21:17:17–21:17:46Z (29 s) | 21:27:53–21:28:23Z (30 s) | 21:38:56–21:38:58Z; drain 150 ms, new process +0.4 s |
| Producer | 240,000,040 points, 0 failed | 240,000,030, 0 failed | 240,000,050, 0 failed |
| Loss check | pass | pass | Δ equals generator total over 2 epochs; inconclusive by design |
| Core liveness | alive | alive (`Wants=influxdb3`) | one planned restart |
| Retries / pauses | 7 write retries on resume | 10 write retries; 1 partition pause | none |
| Sink blocked, worst 60 s | 91.0% | 124.0% (one 74 s wait in one sample) | 0% |
| Lag peak → 0 | 1,427,438 → 0 at 21:21:14Z | 1,666,279 → 0 at 21:32:14Z | 7 → 0 |
| Persist mean (run) | 13.4 s | 19.4 s | 1.4 s |
| Verdict | pass | pass | pass |

Catch-up consumption reached 96.6K–99.9K msg/s (~1M pts/s), above the 800K input, so backlogs drained without
operator action.

## Export flow

**Fail.** Two attempts (InfluxDB `--exec-mem-pool-size` 20%, then 50%): Kafka `dlq` export succeeded; all 38 DB
tables failed at `Failed to read table time bounds` (`deduplicate batches ... Resources exhausted`), so nothing was
uploaded and no export-done was published (`notify_shutdown_on_failed_db_export: false`). Disk was not the limit.
Chronology, root cause and proposed Core fix: [export](runs/export.md).

## Resources and headroom

| Run | Core memory used mean / max | Core disk max | Kafka disk max | Influx anon max (1 s) |
|-----|----------------------------:|--------------:|---------------:|----------------------:|
| L500 | 39.5% / 41.9% | 29.3% | 33.8% | n/a |
| L650 | 45.1% / 54.0% | 43.3% | 59.3% | n/a |
| L800 | 46.5% / 54.0% | 57.5% | 53.6% | n/a |
| L800d | not extracted | 38% | not extracted | 2.51 GiB |
| L1000 | not extracted | 50% | 50% | 3.15 GiB |

The InfluxDB cgroup sits at its 4.74 GiB `MemoryHigh` page-cache ceiling under load; anonymous memory peaked at
3.50 GiB (L800c). Kafka disk required a 15 min `data` retention during the campaign
([deviations](runs/campaign.md#deviations)).

## Open items

| Item | Note |
|------|------|
| Export at high volume | Fixed in Core 0.3.3, validated locally only; VM re-test pending ([follow-up](runs/export.md#follow-up-core-033-local-validation-only)) |
| InfluxDB snapshot settings | L800d values adopted in 0.3.3 ([memory budget](../../component-influxdb.md#memory-budget)); VM re-test pending |
| Ceiling with a faster uplink | 900K limited by generator uplink; Core ceiling between 900K and ~962K |
| F-restart loss proof | Needs DB/Parquet reconciliation (export failed) |
| Graceful-restart WARNs | 5 WARN "loop terminated" lines per planned Core stop |
| Influx row counts | Not run |
