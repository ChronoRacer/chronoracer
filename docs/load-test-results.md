# Load test results

Test of the race stack on GCP, 2026-10-22, `RACE_ID=lt1021a`, 17:55-21:11Z. The stack was destroyed afterwards.
Machine types and their rationale: [ADR - VM types](adr-vm-types.md). Harness: [`../load-test/`](../load-test/README.md).
Field meanings: Core [Pipeline report](component-core.md#pipeline-report), generator
[Report](../curls/load_test/README.md#report).

Raw evidence (per run `run-metadata.json`, generator logs, `artifacts/steady-summary.json`, `summary.txt`,
`pipeline-report.csv`, `metrics/*.csv`, `logs/*`; whole-test window in `_full/` with both VM serial consoles and
`kafka-vm-state.txt`): `scratch/load-test/runs/<ID>/`.

## Setup

| Item | Value |
|------|-------|
| Core + InfluxDB VM | `c4d-highcpu-4` (4 vCPU, 7 GB), Core release `v0.1.4-load`, InfluxDB 3 Core on the same VM |
| Kafka VM | `n4-standard-2`, topic `data` with 4 partitions |
| Telemetry | Cloud Run, 1 CPU / 512Mi |
| Generator | Operator laptop, 1-24 parallel instances, public Kafka listener (TLS + OAuth), `CHDRV` driver change after 60 s |

The laptop path adds producer-side limits that are not Core limits: at 150k pts/s the laptop used ~18% CPU and ~5 MB/s
uplink, at 200k pts/s ~6.4 MB/s. Generator delivery p99 first reached 5 s once at 150k.

Core configuration is an operator overlay uploaded to `gs://chronoracer-race/lt1021a/core-config.yaml`:

| Overlay | Content | Runs |
|---------|---------|------|
| `core-config.yaml` | Baseline: report interval 10 s, initial driver; sink `flush_max_combined_batch_size` 7000 | S0-S2 |
| `core-config-throughput.yaml` | `flush_max_combined_batch_size` 50000 | S3a-S5b |
| `core-config-phase2.yaml` | Throughput overlay plus `fetch_min_bytes` 1, `fetch_wait_max_ms` 100, sink flush 5 ms | S2-p2, L50-L200 |

The overlay files were in `scratch/load-test/` at test time.

Test-only changes, not part of the shipped images:

- From S4: `systemd-timesyncd` drop-in `/etc/systemd/timesyncd.conf.d/99-loadtest.conf` on Core and Kafka VMs
  (`metadata.google.internal`, poll 16-32 s). See [clock offset](#clock-offset-before-s4).
- From L50: `INFLUXDB3_WAL_FLUSH_INTERVAL=100ms` appended to `/chronoracer/influxdb3.env` (the image default is 1 s;
  `start-influx.sh` rewrites the file on boot, so the VM was not reset afterwards).

## Testing flow

| # | Stage | Note |
|---|-------|------|
| 1-2 | Ready check, raise stack | 6 resources; Core idle reports at 10 s, `data` has 4 partitions, Telemetry subscribed |
| S0 | Smoke 1k | Auth, path, reports, artifact collection |
| S1a-d | Ramp 2.5k / 5k / 10k / 15k, 300 s, 4 instances | Where any metric bends |
| S2 | Soak 15k, 1200 s | Target |
| - | Config change 19:09Z | Throughput overlay uploaded, Core VM reset |
| S3a-b | 20k / 25k, 300 s | Headroom |
| - | Clock fix 19:31Z | timesyncd drop-in, see below |
| S4 | Isolation, 15k, 300 s | Slow `Watch` subscriber (`--slow 0.2`) plus Telemetry UI open |
| S5 | Reliability, 15k, 300 s | `systemctl stop influxdb3` did not test a DB stall, see [S5](#s5-and-s5b-database-stall-and-core-restart) |
| S5b | Reliability re-run | `SIGSTOP` on `influxdb3` for 30 s, then Core restart |
| S2-p2 | Phase 2, 15k, 600 s | Phase-2 overlay, Core VM reset |
| - | Setup for live ramp | WAL flush 100 ms, `influxdb3` restart |
| L50-L200 | Live ramp 50k / 75k / 100k / 150k / 200k, 180 s | 8 / 12 / 16 / 20 / 24 instances |
| - | Stopped | Core VM hung after L200, see [L200 end](#l200-end-count-query-hung-the-core-vm) |
| 8b | Backlog drain | Skipped: live ingest throughput already sufficient |
| 9 | Export flow (`SHUTDOWN`) | Skipped, see [L200 end](#l200-end-count-query-hung-the-core-vm) |
| 10 | Metric collection | `_full/`: logs, 13 metric CSVs, serial consoles, Kafka state |
| 11 | Teardown 21:20Z | Targeted destroy of the 6 resources (Core VM destroyed while hung); the overlay and local `runs/` were kept |

Pause between runs was at least 3 min, so windows are separable. Steady-state values below are the window
start+60 s..end.

**Loss verification.** Per run, the generator total (sum of `[Sensors]` lines) is compared with the Core accepted-lines
counter (`WriteMetricSnapshot` delta; it resets on Core restart, so S3 onward deltas start from 0) and with the InfluxDB
count of the run window. Accepted lines equal the generator total in every run that was checked (0 partial writes,
0 errors, DLQ empty). The InfluxDB count is lower only by InfluxDB deduplication of the same sensor at the same
nanosecond across independent generators; the birthday estimate matches (S2: -5 vs ~3.6 estimated; L75: -13 vs 13.3;
L50: -6 vs 5.9). L200 has no InfluxDB count (see below), only the accepted-lines counter: 67,500,331 to 103,500,507.

## Results

Total generated: **167,370,862 points** across all runs, all accepted by Core. Kafka consumer groups ended at lag 0 on
all partitions; DLQ 0.

| Run   | Rate (pts/s) | Duration (s) | Inst. | Points sent |               Influx count | preview_ms mean / p50 / p99 / max | write_ms mean | persist_ms max | sink_blocked max (%) | unconsumed max | Core CPU (med / max %) | Kafka CPU (med / max %) |
|-------|-------------:|-------------:|------:|------------:|---------------------------:|-----------------------------------|--------------:|---------------:|---------------------:|---------------:|------------------------|-------------------------|
| S0    |        1,000 |          120 |     1 |     120,004 |                    120,004 | 45 / ≤50 / ≤100 / 100             |           481 |          4,542 |                    0 |              0 | 6 / 6                  | 43.1 / 43.1             |
| S1a   |        2,500 |          300 |     4 |     750,010 |                    750,010 | 54 / ≤100 / ≤2000 / 1,211         |           489 |          4,565 |                    0 |              0 | 6.2 / 6.3              | 42.7 / 43.2             |
| S1b   |        5,000 |          300 |     4 |   1,500,036 |                  1,500,036 | 54 / ≤100 / ≤100 / 115            |           506 |          2,479 |                    0 |              0 | 6.3 / 6.4              | 43.6 / 43.7             |
| S1c   |       10,000 |          300 |     4 |   3,000,037 |                  3,000,037 | 65 / ≤100 / ≤100 / 123            |           513 |          1,786 |                    0 |              6 | 5.8 / 5.9              | 41.0 / 41.1             |
| S1d   |       15,000 |          300 |     4 |   4,500,036 |                  4,500,036 | 61 / ≤100 / ≤100 / 136            |           527 |          1,595 |                    0 |             13 | 5.8 / 6.7              | 41.5 / 41.6             |
| S2    |       15,000 |        1,200 |     4 |  18,000,030 |                 18,000,025 | 71 / ≤100 / ≤200 / 141            |           517 |          1,587 |                    0 |              0 | 6.5 / 8.7              | 45.8 / 46.1             |
| S3a   |       20,000 |          300 |     4 |   6,000,033 |                  6,000,030 | 21 / ≤50 / ≤50 / 77               |           654 |          3,463 |                    0 |              7 | 6.4 / 8.5              | 46.1 / 46.2             |
| S3b   |       25,000 |          300 |     4 |   7,500,033 |                  7,500,029 | 38 / ≤50 / ≤100 / 97              |           324 |          2,402 |                    0 |              0 | 6.5 / 6.6              | 46.4 / 46.9             |
| S4    |       15,000 |          300 |     4 |   4,500,036 |                  4,500,035 | 37 / ≤50 / ≤100 / 94              |           642 |          4,390 |                    0 |              0 | 6.3 / 6.3              | 45.1 / 45.3             |
| S5    |       15,000 |          300 |     4 |   4,500,036 |                  4,500,034 | 37 / ≤50 / ≤100 / 90,206          |           593 |         93,133 |                  1.6 |              0 | 6.1 / 7.0              | 34.0 / 44.9             |
| S5b   |       15,000 |          300 |     4 |   4,500,036 |                  4,500,035 | 37 / ≤50 / ≤2000 / 1,521          |           562 |         31,741 |                    0 |              0 | 6.5 / 6.8              | 45.0 / 45.3             |
| S2-p2 |       15,000 |          600 |     4 |   9,000,028 |                  9,000,025 | 19 / ≤20 / ≤50 / 103              |           615 |          4,347 |                    0 |              0 | 7.2 / 7.5              | 47.4 / 48.0             |
| L50   |       50,000 |          180 |     8 |   9,000,056 |                  9,000,050 | 19 / ≤20 / ≤50 / 63               |           131 |          1,209 |                    0 |              0 | 11.6 / 11.9            | 55.2 / 58.2             |
| L75   |       75,000 |          180 |    12 |  13,500,075 |                 13,500,062 | 19 / ≤20 / ≤50 / 78               |           125 |            904 |                    0 |              0 | 12.9 / 16.1            | 61.7 / 63.7             |
| L100  |      100,000 |          180 |    16 |  18,000,102 |                 18,000,075 | 19 / ≤20 / ≤50 / 99               |           126 |            740 |                    0 |              0 | 14.2 / 14.3            | 66.4 / 68.5             |
| L150  |      150,000 |          180 |    20 |  27,000,098 |                 27,000,033 | 20 / ≤20 / ≤100 / 133             |           129 |            914 |                    0 |              0 | 17.2 / 24.2            | 65.9 / 72.3             |
| L200  |      200,000 |          180 |    24 |  36,000,176 | not obtainable (est. 167M) | 20.5 / ≤20 / ≤200 / 216           |           124 |            702 |                    0 |              2 | 21.2 / 24.8            | 75.0 / 75.8             |

Columns: values are steady-state medians per 10 s Core window except `max`, `persist_ms max` and `unconsumed max`
(maxima). `≤N` is the histogram bucket bound. Source: `artifacts/steady-summary.json` per run. Absolute `preview_ms`
and `persist_ms` of S0-S3b carry a clock-offset error, see [clock offset](#clock-offset-before-s4).

Conclusions:

- **Max sustained live throughput 200k pts/s, 13x the 15k target**, with no Core-side bend: `decoded_pts_s` tracks
  the sent rate, `unconsumed` stays at most 2, `sink_blocked_pct` 0. Not tested above 200k. At 200k the laptop
  generators had one simultaneous ~1.0-1.4 s stall across instances in one window (summed rate dipped to 192,143,
  caught up the next window), so 200k is also near the limit of the laptop harness.
- **Backlog catch-up** reached ~148k pts/s decoded (142,987 written) in the first window after the Core restart in
  S5 (`sink_blocked` 1.6%, write mean 942 ms), ~10x the target. 8b was not run, so this is the only drain
  measurement.
- Write latency is set by InfluxDB's WAL flush wait: ~0.5 s with the 1 s default, ~0.13 s with 100 ms (L50 on).
  Batch size, not the write latency, is the throughput lever.
- **Phase-2 overlay** halved `preview_ms` mean at the same load (37 to 19 ms, p50 ≤50 to ≤20), the predicted gain
  of ~17 ms; Core CPU +0.9 pp, Kafka CPU +2.4 pp.
- **Throughput overlay (50k batches)** reduced the preview mean from 61-79 ms (S2) to 21 ms (S3a), but this step
  coincides with the Core VM reset and is confounded by the clock offset, see below.

## Resource usage

| Resource | Observation |
|----------|-------------|
| Core CPU | 6% at 1k-15k, 11.6% at 50k, 14% at 100k, 17-24% at 150k, 21-25% at 200k (VM, 4 vCPU). At 15k, `top`: Core 8% of one core, `influxd` 3% |
| Kafka CPU | ~40% idle (java ~50% of one core), 41-47% at 1k-25k, 55-58% at 50k, 62-64% at 75k, 66-69% at 100k, 66-72% at 150k, 75-76% at 200k (2 vCPU) |
| Kafka network | ~2.6 MB/s idle traffic; Kafka NIC (rx / tx) 3.1 / 2.7 MB/s at 15k, 6.1 / 5.7 at 100k, 6.3 / 5.9 at 200k, mostly the idle floor (from `kafka_net.csv`) |
| Laptop uplink | ~2.65 MB/s at 75k, ~5 MB/s at 150k, ~6.4 MB/s at 200k (~32-35 B per point) |
| Kafka disk | ~17 B per point; before L50 Kafka had 15 GB free and Core 14 GB free |
| Core VM memory | See below |
| Partition skew | End offsets of `data`: 5,038,798 / 2,930,793 / 5,873,126 / 2,098,758 (15,941,475 messages), 2.8x between the largest and smallest, from sensor-name keys |

Kafka idles at ~40% CPU with 2.6 MB/s traffic and no load. This is consistent with the control sink's
`fetch.wait.max.ms=0` busy fetch ([Core](component-core.md), control path; option 3 in
[core-latency-options.md](../scratch/core-latency-options.md)); not isolated by an experiment. It leaves less headroom
for the S2 criterion "CPU below 80%": at 200k Kafka reached ~76% median.

**Core VM memory** is dominated by `influxdb3` (RSS ~3.0 GB), not Core (RSS 58 MB, high-water mark 72 MB). InfluxDB
keeps unsnapshotted writes in memory until 600 WAL files (`--wal-snapshot-size`) or 50% of RAM
(`--force-snapshot-mem-size`). Memory therefore follows a sawtooth from 9% up to the 50% cap and a drop at each
snapshot. 50k batches cut the WAL file rate ~3x, so from S3 the buffer rides the 50% cap. Peaks: 73% at L150 (20:45,
snapshot at 20:45:23 dropped it to 61%) and 81% at L200 (20:54-20:56). An idle InfluxDB holds its memory (no WAL files,
no snapshot), and a restart replays the WAL into memory. Operator decision: keep the defaults; a bigger cap gives no
ingest gain (the ack is the WAL flush) and takes query and export headroom (budget: buffer 50% + `exec-mem-pool-size`
20% + `file-cache-size` 20% = 90%, plus Core and agents ~5%).

## Observed behaviour and failures

### S2 against the criteria

S2 (15k, 1200 s) passes all criteria from the runbook:

| Criterion | Result |
|-----------|--------|
| `sent_pts_s` ≥ 15,000, `behind_max_s` < 1 s, `failed` = 0 | min 14,997 / median 15,000, behind max 0.008 s, failed 0 |
| Core `decoded` / `written` track sent, `unconsumed` flat | decoded median 14,998, written median 14,716, `unconsumed` 0 in all windows, `sink_blocked` 0 |
| No `Commit position has not advanced` warnings | None |
| Influx count equals generator total | 18,000,025 vs 18,000,030: -5 is deduplication, accepted lines equal 18,000,030 |
| Core and Kafka CPU below 80% | 6.5 / 8.7% and 46% |

Open: with the baseline config `preview_ms` mean drifted linearly from 61 to 79 ms over the 20 min (+0.9 ms/min) and
reset between runs. The clock offset (below) is the identified cause, not a Core effect.

### Clock offset before S4

The Core and Kafka VMs use `systemd-timesyncd` against the metadata server with a 17-34 min poll. At 19:22Z the Core
offset was +19.9 ms (jitter 7.5 ms), Kafka +0.7 ms. Preview and persist latencies cross the VMs, so S0-S3b absolute
values carry roughly ±20-30 ms error and slow drift (also seen as an impossible 1 ms mean right after the S3a reset).
After the drop-in the Core offset was +0.13 ms (jitter 0.12 ms); per-run offsets are in `runs/<ID>/clock.txt` from S4.
Throughput, loss and relative results are unaffected; S0-S3b latencies are not comparable with S4 onward.

### S4 isolation

With a slow `Watch` subscriber (`--slow 0.2`) and the Telemetry UI open, the probe received ~2.3k pts/s, dropped 3.5M
points through Core `gap` events and lagged ~42 s (buffered in gRPC/SSH, beyond the Core 250 ms queue age). Gaps hit
only the probe: Telemetry ingested the full rate (`evictedPoints` ~14,983 pts/s, no `telemetry_gap`), and `preview_ms`
stayed flat (37 ms mean, max 94). No Core or Kafka effect.

### S5 and S5b: database stall and Core restart

- **S5 did not exercise a DB stall.** `systemctl stop influxdb3` (19:43:37Z) also stopped Core, because
  `chronoracer-core.service` has `Requires=influxdb3.service` and an explicit stop propagates. The exit was clean, so
  `Restart=on-failure` did not restart it, and `start influxdb3` did not start Core either. Core was down 89 s
  (19:43:39.7-19:45:08.7) until the scheduled restart. Graceful drain: unsettled 0, final commit. No loss: accepted
  1,828,305 + 2,671,731 = 4,500,036 = sent, 0 replay duplicates. Telemetry reconnected after 17 s (backoff), no gaps.
  Catch-up, see [Results](#results); preview max 90 s equals the backlog age.
- **S5b (`SIGSTOP`, 19:54:09.7-19:54:39.7Z) passes.** During the 30 s freeze `written` was 0 for 3 windows, 0 retries,
  write p99 ≤30 s, persist max 31.7 s, `sink_blocked` 0 and **`preview_ms` unaffected** (mean 37-38, max 92 ms), then
  ~60k lines/s catch-up. 3 `Commit position has not advanced` warnings are correct. Core restart (19:55:39-41Z) took
  ~2 s, first window preview max 1.5 s. No loss: accepted 3,167,412 + 1,332,624 = 4,500,036 = sent, 0 duplicates.
- With 50k batches the in-Core buffer holds ~850k points, ~57 s at 15k. This contradicts the "~5 s buffer at 15k"
  rejection in [core-latency-options.md](../scratch/core-latency-options.md): it holds only for 7k batches (~8 s).

### Core start order

After `systemctl restart influxdb3` (20:22:43Z) Core restarted too, hit `Connection refused` on `:8181` 3 times while
InfluxDB replayed its WAL (startup 12.6 s) and came up at 20:23:00 through `Restart=on-failure`. Core does not wait for
InfluxDB readiness; it crash-loops until the database is up.

### Telemetry and UI

Telemetry ingested the full rate up to L100 (~100k pts/s). Up to L150 `telemetry_gap` appeared only at ramp start
(2 at L50 and L75, 5 at L100, plus 1 at 20:45:07 during L150). At L200 gaps ran through the whole run (37 events
20:50-20:53), so the Telemetry service (Cloud Run, 1 CPU) is the first component to bend, between 150k and 200k. The
browser UI client dropped at 20:38:53 during L100 (clients 1 to 0).

### L200 end: count query hung the Core VM

L200 ingest passed: Core accepted all 36,000,176 points, decoded ~200k pts/s, `unconsumed` ≤2, write mean 103-147 ms.
At 20:56:47Z `influx_count.sh` started a single `count(*)` `UNION ALL` over 38 tables for the L200 window, with ~167M
points stored and the InfluxDB write buffer at its 50% cap (memory 81%). All Core VM logs stopped at 20:56:51, the
serial console showed `systemd-journald: Under memory pressure, flushing caches`, SSH timed out, and the VM stayed
RUNNING without an OOM kill. The VM has no swap, so it thrashed. It was still hung at 21:15Z (CPU flat at 26% since
20:58) and was destroyed in that state. Not obtainable: Core local journal and state after 20:56:51, the final
InfluxDB count, Ops Agent metrics after 20:56.

The breaking point is therefore on the **storage/query side**, not ingest. The export flow (stage 9, `SHUTDOWN`) was
intentionally not run: it reads the same data under the same memory budget, so it is expected to fail the same way at
this data volume. `rpk` export was not needed (DLQ empty, exporting the data topic is pointless).
