# Throughput runs — lt032 detail

Campaign comparison table (accepted lines, sink blocked, lag, persist, CPU, verdict):
[core-0.3.2-results — throughput steps](../core-0.3.2-results.md#throughput-steps). Timeline and deviations:
[campaign](campaign.md). Transport plateau and snapshot tuning narrative:
[campaign — transport](campaign.md#producer--kafka-transport-plateau),
[campaign — periodic spikes](campaign.md#periodic-spikes-at-800k-influxdb-forced-snapshots).

Steady window = generation start + 60 s through end unless noted.

## L500 (18:11:42Z–18:21:42Z)

| Extension | Value |
|-----------|-------|
| Consumed messages | median 47,616 msg/s |
| Influx write bytes / requests | 38.28 MB/s; ok 10.0/s (50k-line flush cap) |
| Producer delivery | p99 ≤500 ms, max 1,650 ms; pacing max 0.003 s |
| Network `ens3` | Core RX 8.61 / TX 12.97 MB/s; Kafka RX 9.83 / TX 9.58 MB/s |
| Telemetry | instance/request series 27–30 points |

Live monitor: Influx cgroup 1.99 GB → **5.03 GB (~86% MemoryMax) at 18:15Z** → 3.74 GB; influxd ~390% in one `top`
snapshot (bursty; 60 s mean ~38%). Producer queues ≤1,005 per process.

Scaling vs [0.3.1 L350](../../core-0.3.1/runs/L350.md): Core ~7.4% CPU per 100K; Kafka ~12.6% per 100K. Extrapolated
80% CPU: Kafka ~725K, Core ~1.07M.

## L650 (18:32:47Z–18:41:47Z)

Harness exit 1 on Telemetry Logging HTTP 500; re-collected (not a run failure). See results table for verdict note.

| Extension | Value |
|-----------|-------|
| Consumed messages | median 61,901 msg/s |
| Influx write bytes | 49.70 MB/s; ok 13.0/s |
| Producer delivery | p99 ≤1,000 ms, max 1,248 ms |
| Network `ens3` | Core RX 10.98 / TX 13.01 MB/s; Kafka RX 12.64 / TX 12.27 MB/s |

Post-run (18:55Z, before 15 min retention): Kafka `/` 65%, data dir 9.0 GB. Live: Influx **5.09 GB (~87%) at
18:33:12Z**; Core 15% iowait in one `top` sample; producer queue spike 4,535 (gen-9).

At 48 h retention, **Kafka disk** was the next binding limit (~92% for a hypothetical 600 s L700).

## L800 (19:01:40Z–19:10:40Z)

| Extension | Value |
|-----------|-------|
| Consumed messages | median 76,191 msg/s |
| Influx write bytes | 61.25 MB/s |
| Producer delivery | max 2,627 ms; some windows p99 ≤5,000 ms |
| Network | Kafka RX 13.21 / TX 13.31 MB/s |

Logs: Cloud Logging HTTP 500; journals in `artifacts/logs-journal/`. 15 forced Influx snapshots. Producer queues
1,553–8,446.

### 1 s Core disk sampling (steady 538 s)

| Metric (per second) | mean | p50 | p99 | max |
|---------------------|-----:|----:|----:|----:|
| Write MiB/s | 29.8 | 0.5 | 187.7 | 205.9 |
| Write IOPS | 173 | 38 | 865 | 985 |
| Device utilization % | 20.9 | 4.4 | 99.2 | 99.7 |
| Write await ms | 61.6 | 0.5 | 452.8 | 493.5 |
| vmstat iowait % | 4.7 | 1 | 38 | 62 |

51 bursts every ~10–12 s at the 170 MiB/s cap (49 s ≥90% util); IOPS well below provisioned — throughput-limited during
Influx bulk writes. Monitoring 2 min spike pattern: [campaign — periodic spikes](campaign.md#periodic-spikes-at-800k-influxdb-forced-snapshots).

## Snapshot tuning (L800b / L800c / L800d)

Steady-window outcomes are in the [results table](../core-0.3.2-results.md#throughput-steps). Below: tuning evidence
not captured there.

**L800b** (`wal-files-per-snapshot=200`): 15 WAL snapshots, **14 forced** — no change to forced-snapshot cycle
(inferred: regular snapshots need completed 10 min gen1 windows). Influx anon peak 3.14 GiB; file cache to 2.18 GiB.
1 s disk: util ≥90% for 30 s vs L800 49 s.

**L800c** (concurrency 3, 300 s): producer worst delivery p99 ≤10 s / max 7,311 ms (2 of 406 windows ≤10 s). Idle-free
seconds 5–8% → 1.7%; anon 3.14 → 3.50 GiB. Post-run Core `/` 32%, Influx 2.8 GB (purge reclaimed).

**L800d** (concurrency 3 + `--force-snapshot-mem-size 15%`): Core journal `sink_blocked_pct=0.0` every minute; 15 forced
snapshots ~20 s cadence; anon max **2.51 GiB** (vs 3.50 GiB L800c). Script: `runs/lt032-influx-L800d.sh`.

### Producer delivery p50 buckets (report windows)

| Run | ≤50 ms | ≤100 | ≤200 | ≤500 | ≤1,000 | >1,000 |
|-----|-------:|-----:|-----:|-----:|-------:|-------:|
| L500 | 471 | 1 | 0 | 0 | 0 | 0 |
| L650 | 649 | 0 | 0 | 0 | 0 | 0 |
| L800 | 138 | 168 | 214 | 264 | 39 | 3 |
| L800c | 65 | 96 | 112 | 115 | 14 | 4 |
| L800d | 90 | 110 | 94 | 84 | 22 | 6 |

At 800K, producer→Kafka median moves to 100–500 ms regardless of Influx settings; persist latency includes that wait
(producer record timestamps).

### 1 s sampling (steady windows)

| Run | CPU mean | s CPU ≥95% | s 0% idle | iowait p99 | Disk p99 / s util ≥90% | Burst period (median) | Influx anon max |
|-----|---------:|-----------:|----------:|-----------:|-----------------------:|----------------------:|----------------:|
| L800 | 60.1% | 18.6% | 5.4% | 38% | 188 MiB/s / 9.1% | 35 s | n/a |
| L800c | 59.9% | 20.7% | 1.7% | 50% | 201 MiB/s / 5.0% | 40 s | 3.50 GiB |
| L800d | 58.7% | 16.5% | **0.0%** | 38% | 204 MiB/s / 4.2% | **20 s** | **2.51 GiB** |

## L900 (21:02:42Z–21:07:45Z)

| Extension | Value |
|-----------|-------|
| Consumed msg/s median | 85,375 |
| Producer pacing max | 0.148 s |
| Producer delivery max | 33,786 ms |
| Delivery p50 buckets (steady) | ≤50: 9 … ≤30 s: 18 (full distribution in evidence) |

Uneven producer queues: gen-7 up to 97,144, gen-4 79,486; drained to 0. Persist mean 15 s with lag 0: dominated by
uplink queueing ([transport plateau](campaign.md#producer--kafka-transport-plateau)).

1 s steady: Core CPU mean 68.2%, ≥95% in 20.5% of seconds, iowait p99 43%, disk write mean 40.4 MiB/s, Influx anon
max 2.97 GiB; Kafka CPU mean 35.9%, max 46%.

## L1000 (20:52:08Z–20:57:13Z)

Generated 298,513,744 / 300M target (producers fell behind). Overload narrative:
[campaign — Core/Influx ceiling](campaign.md#coreinfluxdb-ceiling).

Live `Pipeline report`: 20:54Z unconsumed 63,371, sink_blocked 90.6%; 20:55Z 217,272, 92.6%. ~93K msg/s consumed vs
~95K produced → lag ~150K msg/min. Kafka CPU ~84% idle at 20:54Z while sink blocked ~90%+.

### L800d vs L1000 (1 s steady)

| Host | Metric | L800d | L1000 |
|------|--------|------:|------:|
| Core | CPU mean / p50 | 58.7% / 47% | 71.4% / 69% |
| Core | s CPU ≥95% | 16.5% | 21.1% |
| Core | iowait p99 | 38% | 48% |
| Core | disk write mean | 30.4 MiB/s | 44.2 MiB/s |
| Core | Influx anon max | 2.51 GiB | 3.15 GiB |
| Kafka | CPU mean / max | n/a | 22.2% / 46% |
