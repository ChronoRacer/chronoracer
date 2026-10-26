# Load test report: Core v0.3.1

Campaign 2026-10-24 (16:28Z–19:03Z). Steady window = generation start + 60 s through end; Core metrics at 60 s
intervals.
Per-run detail: [campaign](runs/campaign.md), [L200](runs/L200.md), [L250](runs/L250.md), [L350](runs/L350.md),
[F-freeze](runs/F-freeze.md), [F-stop](runs/F-stop.md).

## Summary

- Core v0.3.1 on a single `c4d-highcpu-4` ingested **350,000 points/s for 600 s** with zero loss, zero delivery failures
  and no lag, at 26.5% mean Core CPU. **Capacity ceiling not found**; the campaign stopped due to operator internet
  bandwidth not being able to support generating more load, not overload.
- Accepted-line counter delta matches generator totals in L200, L250, L350, F-freeze and F-stop (120.0M, 150.0M, 210.0M,
  60.0M, 60.0M points).
- A 32 s InfluxDB freeze or stop at 200k pts/s caused no loss, no producer errors and no Core restart; backlog drained
  without manual action. Peak impact is persistence latency and sink blocking (below).
- **L100 discarded** — initial harness setup issues; no 100k step in this report (see [campaign](runs/campaign.md)).
- `F-restart` was not run. Telemetry UI was not exercised under load.

## Setup

Baseline: [campaign — deployed baseline](runs/campaign.md#deployed-baseline).

| Item                          | Value                                                                                                                  |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------|
| Core + InfluxDB 3 (3.12.1) VM | `c4d-highcpu-4`, 20 GiB hyperdisk; Influx WAL flush 100 ms, cgroup MemoryMax ~5.4 GiB                                  |
| Kafka VM                      | `n4-standard-2`, topic `data`, 4 partitions                                                                            |
| Telemetry                     | Cloud Run, europe-west1 (revision `telemetry-00001-krt`)                                                               |
| Core config                   | processing batch 1000 / 5 ms, persistence flush 50,000 lines / 4 s, 7 in-flight DB writes                              |
| Generator                     | Fedora host, i7-12700H (20 logical CPUs), public Kafka listener (TLS + OAuth), 4–5 processes, driver change after 60 s |

## Throughput steps

Steady values are medians/means in the steady window. Latency percentiles are histogram bucket upper bounds.

| Run  | Target pts/s | Procs | Points generated | Producer steady | Accepted lines (median) | Lag max / post-run | Persist mean / p99 bound | Sink blocked | Core CPU mean / max | Kafka CPU mean / max | Verdict |
|------|-------------:|------:|-----------------:|----------------:|------------------------:|-------------------:|-------------------------:|-------------:|--------------------:|---------------------:|---------|
| L200 |      200,000 |     4 |      120,000,034 |         200,000 |                 200,026 |              0 / 0 |             407 ms / 1 s |  max 0.0017% |       16.5% / 18.8% |        28.6% / 29.3% | pass    |
| L250 |      250,000 |     4 |      150,000,032 |         250,000 |                 250,031 |             20 / 0 |             361 ms / 1 s |  max 0.0017% |       19.9% / 21.9% |        28.7% / 29.5% | pass    |
| L350 |      350,000 |     5 |      210,000,011 |         350,000 |                 350,044 |             43 / 0 |             317 ms / 1 s |      0.0017% |       26.5% / 29.4% |        32.8% / 33.8% | pass    |

Common: 0 failed deliveries, drained queues, pacing lateness ≤0.04 s, delivery p99 ≤100 ms (brief spikes after driver
change), 0 partial/error/retry Influx writes, loss check pass. Core CPU ~7.5% per 100k pts/s over 200k–350k.

### 100k step

Not reported. L100 attempts were **discarded due to initial harness setup issues**;
see [campaign timeline](runs/campaign.md#timeline-utc).

## Fault injection (200k pts/s, 300 s, fault at ~+120 s)

|                                   | F-freeze (`SIGSTOP`)                         | F-stop (`systemctl stop influxdb3`)                                 |
|-----------------------------------|----------------------------------------------|---------------------------------------------------------------------|
| Fault window                      | 18:33:24–18:33:56Z (32 s)                    | 18:47:50–18:48:22Z (32 s)                                           |
| Producer                          | 200,000 pts/s, 0 failures, 60,000,032 points | same                                                                |
| Loss check                        | pass, 60,000,032                             | pass, 60,000,032                                                    |
| Core liveness                     | alive, 3 write retries                       | alive, 7 flusher retries, 1 pipeline-full pause                     |
| Sink blocked, worst 60 s          | 50.7%                                        | 67.5%                                                               |
| Persist latency in fault interval | mean 3.7 s, p99 bound 30 s                   | mean 12.8 s, log max 44.9 s                                         |
| Lag gauge                         | 0 throughout                                 | 0 throughout                                                        |
| Recovery                          | automatic after freeze                       | writes OK from ~18:48:30Z                                           |
| Verdict                           | pass                                         | pass (F-stop metrics partly post-hoc; see [F-stop](runs/F-stop.md)) |

Lag gauge missed sub-minute backlog; sink-blocked and persist latency are the reliable signals.

## Resources and headroom

| Run  | Core memory used mean / max | Kafka memory used mean / max | Core disk used max | Kafka disk used max |
|------|----------------------------:|-----------------------------:|-------------------:|--------------------:|
| L200 |               45.0% / 57.9% |                20.9% / 21.0% |              26.1% |               26.2% |
| L250 |               64.9% / 82.4% |                75.6% / 85.7% |              30.5% |               36.6% |
| L350 |               42.2% / 53.6% |                22.1% / 22.4% |              37.5% |               49.3% |

CPU below 80% everywhere. L250 memory spike is an outlier vs L350 ([campaign](runs/campaign.md#l250-memory-snapshot)).

## Export flow

38/38 tables in 176.6 s (~3.75M pts/s on sample table); all checks passed. `throttle` failed once (Flight SQL memory
pool), succeeded on retry. Parquet vs generator totals not reconciled — [campaign export](runs/campaign.md#export-test).

## Open items

| Item              | Note                                                                         |
|-------------------|------------------------------------------------------------------------------|
| No ceiling found  | 350k pts/s at 26.5% Core CPU; more generator capacity needed to probe higher |
| F-restart         | Not executed                                                                 |
| F-stop collect    | Harness ended early; see [F-stop](runs/F-stop.md)                            |
| L250 memory       | Outlier; see Resources                                                       |
| Telemetry         | No request samples in steady windows                                         |
| Influx row counts | Not run                                                                      |
