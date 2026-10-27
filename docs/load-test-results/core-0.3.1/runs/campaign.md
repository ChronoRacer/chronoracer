# lt301 campaign notes

Companion to [core-0.3.1-results](../core-0.3.1-results.md). Per-run evidence:
[L200](L200.md), [L250](L250.md), [L350](L350.md), [F-freeze](F-freeze.md), [F-stop](F-stop.md).

Note: campaign name was supposed to be `lt031`, but was `lt301` due to a typo.

## Timeline (UTC)

| Phase | Start | End | Notes |
|-------|-------|-----|--------|
| Stack ready | ~2026-10-24T16:28:25Z | — | GCE `core` / `kafka`; Core **0.3.1** deployed. |
| **L100** | ~2026-10-24T16:34:00Z | ~2026-10-24T17:11:45Z | **Discarded** — initial harness setup issues; not in published results. |
| **L200** | 2026-10-24T17:11:46Z | 2026-10-24T17:21:47Z | Full 600 s; generator exit 0. |
| **L250** | 2026-10-24T17:36:10Z | 2026-10-24T17:46:10Z | Full run. |
| **L350** | 2026-10-24T17:55:57Z | 2026-10-24T18:05:57Z | 5 generators. |
| **F-freeze** | 2026-10-24T18:31:23Z | 2026-10-24T18:36:23Z | Influx freeze **18:33:24–18:33:56Z**. |
| **F-stop** | 2026-10-24T18:45:48Z | 2026-10-24T18:50:49Z | Influx stop **18:47:50–18:48:22Z**; harness metrics collect failed (padding). |
| **F-restart** | — | — | Not executed. |
| **Export** | 2026-10-24T18:58:56Z | 2026-10-24T19:02:15Z | 38/38 tables; `throttle` retried after Flight SQL OOM. |

Between-run gaps include the default Monitoring visibility wait (420 s) and VM recycling after the discarded L100 work.

## Deployed baseline

| Component | Version / image | Compute |
|-----------|-----------------|--------|
| **Core** | Release **0.3.1** | GCE `core`: **c4d-highcpu-4**, 20 GiB hyperdisk-balanced |
| **Kafka** | Campaign image | GCE `kafka`: **n4-standard-2**, 20 GiB hyperdisk-balanced |
| **InfluxDB 3** | **3.12.1** | On Core VM; cgroup MemoryMax ~5.4 GiB |
| **Telemetry** | Cloud Run `telemetry-00001-krt`, `europe-west1` | Revision from metrics export only |

**Core config (effective):** `race_id=lt301`, Kafka topic `data` (4 partitions), processing batch 1000 / 5 ms, persistence flush 50 000 lines / 4 s, `max_inflight_db_writes=7`. Influx WAL flush **100 ms**.

**Generator host:** Fedora 44, **Intel i7-12700H**, 20 logical CPUs; 4–5 processes for throughput steps.

Early campaign work fixed a Cloud Monitoring descriptor for `pipeline_persist_latency_ms` (DELTA kind rejected); accepted runs use the corrected GAUGE series.

## L250 memory snapshot

During L250 (17:43:20Z): accepted lines ~250k pts/s, unconsumed lag 0–20, sink blocked ~0%; Core service memory ~5.0 GiB on the live check — near the Influx cgroup limit. L350 at higher rate showed lower memory %; treat L250 as an outlier until reproduced.

## Export test

| Metric | Value |
|--------|--------|
| Duration | **176.6 s** for 38/38 tables |
| Throughput | **~3.75×10⁶ points/s** (operator estimate, sample table) |
| Outcome | All export checks passed; GCS Parquet + DLQ + Pub/Sub OK |
| Retry | Table **`throttle`**: first attempt OOM on deduplicate batches; succeeded on retry |

## Gaps

| Gap | Notes |
|-----|--------|
| **L100** | Discarded (harness setup); campaign acceptance starts at L200. |
| **F-restart** | Not run. |
| **F-stop** | Run-dir metrics empty; pipeline/VM numbers in [F-stop](F-stop.md) from post-hoc Monitoring/Logging export. |
| **Telemetry** | No HTTP load in steady windows of throughput/fault runs. |
| **Influx row counts** | Not collected; loss uses accepted-lines counter. |
