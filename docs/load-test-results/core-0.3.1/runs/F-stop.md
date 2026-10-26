# F-stop — lt301 fault evidence (Influx `stop`)

Campaign `lt301`, Core **v0.3.1**. Baseline: [campaign](campaign.md). Summary: [core-0.3.1-results](../core-0.3.1-results.md).

**Harness:** generators completed (**exit 0**); metrics collection failed (padding). Pipeline/VM numbers in §3–7 are from a **post-hoc** Monitoring + Logging export (**2026-10-24T18:43:48Z**–**18:53:49Z**), not the failed run-dir collect.

## 1. Parameters

| Field | Value |
|-------|------:|
| Target rate | 200 000 pts/s (4×50 000) |
| Duration | 300 s |
| Instances | 4 |
| Generation UTC | **2026-10-24T18:45:48Z** → **2026-10-24T18:50:49Z** |
| Steady window UTC | **2026-10-24T18:46:48Z** → **2026-10-24T18:50:49Z** (60 s warmup) |
| Scenario | `stop` — `systemctl stop influxdb3` (~30 s) |
| Fault UTC (`fault.log`) | **begin 2026-10-24T18:47:50Z**, **end 2026-10-24T18:48:22Z** (32 s); Core/Influx both reported `active` at 18:47:52Z |
| Generator exit code | **0** |

## 2. Producer results

| Process | Steady rate (pts/s) | Total points | Failed | Queue end | `behind_max_s` | Delivery (steady window) |
|---------|--------------------:|-------------:|-------:|----------:|---------------:|--------------------------|
| gen-0 | 50 000.0 | 15 000 008 | 0 | 0 | 0.016 | p50≤50, p99≤100, max=1538 ms |
| gen-1 | 50 000.0 | 15 000 004 | 0 | 0 | 0.015 | p50≤50, p99≤100, max=1439 ms |
| gen-2 | 50 000.0 | 15 000 005 | 0 | 0 | 0.015 | p50≤50, p99≤100, max=1459 ms |
| gen-3 | 50 000.04 | 15 000 015 | 0 | 0 | 0.009 | p50≤50, p99≤100, max=1480 ms |
| **Total** | **200 000.04** | **60 000 032** | **0** | **0** | **0.016** | aggregate p99≤100 ms (`generator-summary.txt`) |

All four `[Summary]` lines present (`generators.complete=true`). Per-window `[Report]` lines during the fault interval (~62–94 s after generation start) show **50 000 pts/s**, **failed=0**, and delivery max **≤112 ms** (no producer-side stall visible). Run-wide delivery maxima (1.4–1.5 s) match the **first** 10 s report after the 60 s CHDRV sleep (OAuth/path warmup), not the fault window.

## 3. Core / pipeline (steady state)

From post-hoc Monitoring export; steady window **18:46:48Z**–**18:50:49Z**. Counter rates use adjacent 60 s intervals inside steady (3 samples).

| Metric | Steady (median / max) |
|--------|----------------------|
| `influx_write_lines` (accepted lines/s) | **200 026** / 265 870 pts/s |
| `influx_write_bytes` | **15.3 MB/s** / 20.3 MB/s |
| `influx_write_requests` ok / error / partial | **4.00** / **0** / **0** req/s; retry **0.23** / 0.92 req/s |
| `pipeline_consumed_messages` | **19 054** / 23 785 msg/s |
| `pipeline_unconsumed_messages` (lag) | **0** (min–max 0; post-run max **0**) |
| `pipeline_sink_blocked_ms` | **0 %** / **67.5 %** (max interval includes post-fault recovery minute) |
| `pipeline_persist_latency_ms` (merged histogram) | mean **5 893 ms**; p50 ≤**500** ms; p99 ≤**60 000** ms (720 batch samples / 3 windows) |
| Decoded pts/s, preview latency, write RPC latency | unavailable (not published) |

## 4. Resources

From **backfill** (steady window unless noted).

| Series | Median | Max |
|--------|-------:|----:|
| Core CPU % | 15.1 | 23.4 |
| Kafka CPU % | 27.2 | 28.3 |
| Core disk % used (all mounts) | 22.7 | 92.8 |
| Kafka disk % used | 21.6 | 92.8 |
| Core memory % `used` | 30.9 | 41.4 |
| Kafka memory % `used` | 22.2 | 22.3 |
| Telemetry instance count | 0.5 | 1.0 |
| Telemetry request count | 0 | 0 |

## 5. Loss check

From post-hoc `influx_write_lines` series.

| Check | Result |
|-------|--------|
| `accepted_lines_check` | **Pass** — baseline **2026-10-24T18:45:12.189749Z**, final **2026-10-24T18:53:12.190290Z**, observed **60 000 032** = generator **60 000 032** (1 counter epoch). |
| `influx_count_check` | **Inconclusive** — no per-table DB count artifact. |

**Offset ledger (backfill `core.log`):** unsettled `pending` totals ~6.6k/partition-scale before stop; spike **~92.7k** sum at **18:48:30Z** during write recovery, back to **~6.1k** by **18:49:12Z** and **~6.0k** at **18:50:12Z**.

## 6. Acceptance

| Criterion | Verdict | Notes |
|-----------|---------|-------|
| Producer steady rate vs target | **Pass** | 200 000.04 pts/s |
| Pacing lateness &lt; 1 s | **Pass** | 0.016 s max |
| All generator summaries, empty queues, zero failures | **Pass** | |
| Monitoring ingest tracks production; lag bounded / drains | **Pass** | Accepted lines median **200k** pts/s; lag **0** through padded window |
| No partial/error Influx writes (Monitoring) | **Pass** | error/partial **0** req/s in steady intervals |
| Accepted-lines loss check | **Pass** | §5 |
| CPU &lt; 80 % headroom | **Pass** | Core max **23.4 %** steady |
| Workflow collection / coverage | **Fail** | Harness never wrote run-dir `artifacts/metrics/` (`collect.log`) |
| Fault: delivery success | **Pass** | 0 failed deliveries |
| Fault: recovery without manual intervention | **Pass** | Influx restarted 18:48:22Z; DB writes recovered from **18:48:30Z** (backfill log) |
| Fault: Core liveness during DB stop | **Pass** | `heartbeat_handler: Alive` every 30 s through stop; process stayed up |
| Fault: bounded backlog drain / persistence after recovery | **Pass** | `unconsumed=0` in Monitoring; ledger pending normalized within ~1 min |
| DLQ / Core restart replay | **Pass** | No DLQ/restart lines in backfill `core.log`; single metric epoch |

**Anomalies:** steady-window persist mean inflated by the **18:48–18:49Z** recovery histogram; one 60 s interval at **67.5 %** sink-blocked. Core **`used` memory** fell **33.9 % → 13.7 %** between 18:47:08 and 18:48:08 samples (Influx stop), then rebounded (**28 %** at 18:49:08). `run-metadata.json` template `duration_s=600` in nested config vs actual **300 s**.

**Log warnings (backfill `core.log`, 18:43–18:54Z):** **7×** `sensor_data_flusher` DB write retry WARN from **18:47:52Z**; **1×** `message_sink` pipeline full / partition pause at **18:48:27Z**; **no** ERROR or GCM publisher failures. IAP NumPy hints in `clock.txt` / `fault.log` only.

## 7. Fault timing and impact

| Phase | UTC | Evidence |
|-------|-----|----------|
| Run start | 18:45:48 | `run-metadata.json` |
| Steady window start | 18:46:48 | `steady-summary.json` |
| Influx stop | 18:47:50 → 18:48:22 | `fault.log` (32 s) |
| First DB write failures | 18:47:52 | backfill `core.log` (HTTP to `:8181`) |
| Influx back `active` / stop end | 18:48:22 | `fault.log` |
| Pipeline sink pause | 18:48:27 | `message_sink` WARN |
| First `Database write recovered` | 18:48:30 | backfill `core.log` (~8 s after Influx end) |
| Persist/sink recovery (Monitoring minute) | by 18:50:12 | sink **0 %**, persist mean **411 ms** (`Pipeline report` + CSV window ending 18:50:12) |
| CHDRV `driver-b` | ~60 s after gen start | `gen-*.log` |
| Run end | 18:50:49 | |

**During stop (18:47:50–18:48:22Z):** producers unchanged (§2). Core kept consuming (ledger still advancing); lag gauge **0** at **18:48:12Z** sample (1 min granularity).

**Post-stop impact (backfill):** Monitoring window **18:48:12–18:49:12Z** — sink-blocked **67.5 %**; persist histogram mean **12 791 ms**, p99 bucket **60 000 ms** (Core log: `persist_ms` max **44 853 ms**, `sink_blocked_pct=67.5`, `unconsumed=0`). Prior window (overlapping pre-stop) mean **396 ms**; following window mean **412 ms**.

**Memory:** Core `used` **13.7 %** at **18:48:08Z** (Influx stop).

## 8. Data quality and caveats

Post-hoc export covered all 18 metric families for the window above. Lag and 60 s histograms can miss sub-minute spikes. No Influx row-count check or Telemetry JSON in this evidence set.
