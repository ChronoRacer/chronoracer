# F-freeze — lt301 load test evidence

Campaign `lt301`, Core **v0.3.1**. Baseline: [campaign](campaign.md). Summary: [core-0.3.1-results](../core-0.3.1-results.md). Steady window = start + 60 s through end.

## 1. Parameters

| Field | Value |
|-------|------:|
| Target rate | 200,000 pts/s |
| Duration | 300 s |
| Generator processes | 4 (50,000 pts/s each) |
| Run start (UTC) | 2026-10-24T18:31:23Z |
| Run end (UTC) | 2026-10-24T18:36:23Z |
| Scenario | Influx **freeze** (SIGSTOP-style suspend via harness) |
| Fault window (`fault.log`) | 2026-10-24T18:33:24Z begin → 2026-10-24T18:33:56Z end (~32 s) |
| Generator exit code | 0 |

Monitoring collection window: 2026-10-24T18:29:23Z – 2026-10-24T18:39:23Z.

## 2. Producer results

Final `[Summary]` lines (`gen-*.log`); steady rate from post-warmup periodic reports (`summarize_generators`).

| Process | Steady rate (pts/s) | Total points | Failed | Queue end | Behind max (s) | Delivery latency |
|---------|-------------------:|-------------:|-------:|----------:|---------------:|------------------|
| gen-0 | 50,000.0 | 15,000,008 | 0 | 0 | 0.034 | p50≤50, p99≤100, max=1311 ms |
| gen-1 | 50,000.0 | 15,000,004 | 0 | 0 | 0.007 | p50≤50, p99≤100, max=1381 ms |
| gen-2 | 50,000.0 | 15,000,005 | 0 | 0 | 0.013 | p50≤50, p99≤100, max=1298 ms |
| gen-3 | 50,000.09 | 15,000,015 | 0 | 0 | 0.010 | p50≤50, p99≤100, max=1346 ms |
| **Total** | **200,000.09** | **60,000,032** | **0** | **0** | **0.034** | worst p99≤100 ms |

Whole-run `[Summary]` `sent_pts_s` is 49,993 per process (~199,972 pts/s aggregate); steady-window weighted rate matches 200k target (rounding / warmup excluded).

## 3. Core / pipeline (steady window)

Accepted-line throughput uses `influx_write_lines` (Monitoring counter).

| Metric | Steady value |
|--------|-------------|
| Accepted lines (influx_write_lines) | median **200,025** pts/s (3 intervals; min 200,024, max 200,027) |
| Consumed messages | median **19,050** msg/s (min 19,028, max 19,055) |
| Lag (`pipeline_unconsumed_messages`) | max **0**, at end **0**; post-run max **0** (10 s samples; no idle return needed) |
| Persistence latency (merged histogram, 3 windows, 720 batches) | mean **3,658** ms; p50 upper bound **500** ms; p99 upper bound **30,000** ms; explicit max bucket **60,000** ms |
| Sink blocked | median **0%**, max **50.7%** (single 60 s interval spanning freeze recovery) |
| Influx write bytes | median **15.3** MB/s |
| Influx write requests (ok) | ~**4.0** req/s |
| Influx write requests (error / partial) | **0** / **0**; retry ≤0.05/s steady (3 retries total per `core.log` snapshots) |

Sink-blocked max **50.7%** in the 2026-10-24T18:33:12–18:34:12Z interval (freeze recovery).

## 4. Resources (steady window)

| Resource | Core VM | Kafka VM |
|----------|---------|----------|
| CPU mean / max | **15.7%** / **19.9%** | **28.1%** / **29.0%** |
| Memory `% used` mean / max | **48.3%** / **56.3%** | **22.3%** / **22.4%** |
| Disk `/dev/nvme0n1p1` `% used` mean / max | **38.8%** / **40.8%** | **54.4%** / **55.7%** |
| Network `ens3` rx mean / max | **3.55** / **3.64** MB/s | **4.08** / **4.16** MB/s |
| Network `ens3` tx mean / max | **7.57** / **8.78** MB/s | **4.01** / **4.25** MB/s |

**80% CPU headroom:** pass (both VMs well below 80%).

| Telemetry (Cloud Run) | Value |
|-----------------------|-------|
| Instance count (steady samples) | 0–1 |
| Request count (exported deltas) | **0** for all 1xx/2xx rows in collection window |
| Request latency | distributions present in CSV but empty (no countable samples in export) |

## 5. Loss check

| Field | Value |
|-------|------:|
| Baseline sample | 2026-10-24T18:31:12.190265+00:00 |
| Final sample | 2026-10-24T18:39:12.189716+00:00 |
| Generator points | 60,000,032 |
| Accepted-lines Δ (`influx_write_lines`) | 60,000,032 |
| Counter epochs | 1 |

**Verdict: pass** — delta equals generator total.

## 6. Acceptance

| Criterion | Result |
|-----------|--------|
| Producer steady rate ≈ target, pacing &lt;1 s, queues empty, zero failed deliveries | **pass** |
| Accepted-line rate tracks production; lag bounded, returns to idle | **pass** (lag 0 throughout samples) |
| No unexplained partial/error Influx writes | **pass** (0 error/partial; 3 retries during freeze) |
| Padded accepted-lines Δ vs generator total | **pass** |
| CPU &lt;80% headroom | **pass** |
| Producer delivery success through fault | **pass** |
| Recovery without manual intervention | **pass** (`fault.log` end freeze; Influx active) |
| Core liveness during DB fault | **pass** (process continued; write retries only) |
| Bounded backlog / drain after fault | **pass** (lag gauge 0; post-run lag 0) |
| Persistence progress after recovery | **pass** (accepted-lines counter advanced through 18:37Z) |
| Throughput-step-only criteria (e.g. increase ladder) | **n.a.** |

### Log highlights

- **`core-warn-error.log` / `core.log`:** three `Database write failed, retrying` WARNs at 2026-10-24T18:33:56Z (HTTP to local Influx); `WriteMetricSnapshot` shows `requests_retry: 3`, no errors/partials after recovery.
- **`kafka.log`:** no WARN/ERROR in export.
- **`offset-lag.log`:** periodic unsettled-offset INFO only (normal consumption).
- **`telemetry.json`:** no error/warn strings in export.

### Anomalies

- Freeze lasted **~32 s** (not nominal 30 s).
- **Sink blocked ~51%** in the monitoring interval containing the freeze (expected under DB stall).
- Merged persistence latency mean/p99 reflect fault-window batches; Kafka **unconsumed** gauge did not rise between 10 s samples (sub-minute backlog not visible on lag gauge).
- Whole-run generator `sent_pts_s` slightly below 50k/process in final summary vs 200k steady (see §2).

## 7. Fault impact and recovery

| Aspect | Observation |
|--------|-------------|
| Fault timing | 18:33:24Z–18:33:56Z (~121 s after run start) |
| Lag | `pipeline_unconsumed_messages` remained **0** at all exported samples |
| Latency | Histogram mean **3.7 s** steady merge; p99 bucket ≤**30 s**; freeze coincides with retry WARNs |
| Memory | Core `% used` peaked **56.3%** in steady window (Ops Agent 60 s cadence) |
| Sink blocked | **~51%** for 18:33:12–18:34:12Z interval, then **0%** |
| Core alive | Yes — logs and counters continue through and after freeze |
| Backlog drain | Lag **0** at 18:37Z–18:39Z post-run samples; accepted-lines Δ includes full generator total |

## 8. Data quality / caveats

- Steady pipeline metrics have only **3** full 60 s counter intervals inside the 240 s steady window (Monitoring cadence).
- Lag is a **sparse gauge**; brief Kafka backlog during freeze may fall between samples.
- Persistence percentiles are histogram bucket upper bounds.
- No per-table Influx row count.

### Missing data

| Item | Notes |
|------|--------|
| `decoded points/s`, preview latency, write RPC latency | Not published by Core v0.3.1 |
| Influx per-table row counts | Not collected (`INFLUX_COUNT` disabled) |
| Telemetry request latency percentiles | `run.googleapis.com/request_latencies` rows exported without usable distribution counts |
| InfluxDB journal during freeze | Not shipped to Cloud Logging; would need VM `journalctl -u influxdb3` for 2026-10-24T18:33:20Z–18:34:00Z UTC |
