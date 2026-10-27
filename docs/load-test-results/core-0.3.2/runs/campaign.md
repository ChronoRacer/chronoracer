# lt032 campaign notes

Companion to [core-0.3.2-results](../core-0.3.2-results.md). Per-run detail:
[throughput](throughput.md), [fault injection](fault-injection.md), [export](export.md). Raw evidence (gitignored):
`load-test/runs/<run>/`.

**Goal:** 500K points/s minimum, higher if headroom remains. **Outcome:** 800K clean pass (L800d); 900K met criteria
with uplink-limited latency; 1M overloaded persistence without loss (about 17s producer stall, pass is under 1s).

## Timeline (UTC, 2026-10-26)

| Phase | Start | End | Notes |
|-------|-------|-----|-------|
| Stack apply | 17:52 | 17:56 | 6 targeted resources; Core 0.3.2 active 17:56:09Z |
| Ready check | 17:56 | 18:09 | WSL producer CA fix (below) |
| L500 | 18:11:42 | 18:21:42 | 600 s |
| L650 | 18:31:47 | 18:41:47 | 600 s |
| `data` retention → 15 min | 18:56:46 | — | deviation |
| L800 | 19:00:39 | 19:10:40 | 600 s; 1 s Core sampling |
| L800b | 19:54:01 | 20:04:01 | 600 s; `wal-files-per-snapshot=200` |
| Influx table purge | 20:26:15 | — | Core disk 75% |
| L800c | 20:29:10 | 20:34:11 | 300 s; snapshot concurrency 3 |
| L800d | 20:39:39 | 20:44:40 | 300 s; + forced snapshot at 15% |
| L1000 | 20:52:08 | 20:57:13 | 300 s; overload |
| L900 | 21:02:42 | 21:07:45 | 300 s |
| F-freeze / F-stop / F-restart | 21:15 / 21:26 / 21:36 | 21:20 / 21:31 / 21:41 | 300 s each at 800K |
| Export attempt 1 / 2 | 21:54:00 / 22:09:22 | 22:00:01 / 22:16:33 | both failed |
| Targeted destroy | 22:18:58 | 22:19:40 | 6 resources destroyed; no race VMs, no ingest DNS, no Telemetry service; public ingest firewall removed (`chronoracer-kafka-private` permanent rule retained) |

## Ready check (17:56Z–18:09Z)

| Item | Value |
|------|-------|
| Generator host | WSL2 (kernel 6.6.87.2), 28 logical CPUs, 15 GiB RAM; chrony NTP-synced (59 ms slow at 17:49Z, converging) |
| Stack | Applied ~17:52Z, race `lt032`; Kafka public IP **207.175.138.134** |
| Core VM | `c4d-highcpu-4`, image `chronoracer-core-1792842859`, instance ID **8980958802886142692**; Core **0.3.2** active 17:56:09Z |
| Kafka VM | `n4-standard-2`, image `chronoracer-kafka-1792842845`; topic `data`, 4 partitions |
| InfluxDB | 3.12.1; WAL flush 100 ms; MemoryMax 5,819,002,880 B |
| Core config (effective) | batch 1000 / 5 ms; flush 50,000 lines / 4000 ms; fast 3000 / 2000 ms; 7 in-flight DB writes |
| Telemetry | `telemetry-00001-zg8`, subscribed to Core (`raceId=lt032`) |
| Monitoring | All Core scalar families present; idle lag 0; persist latency absent at idle (expected) |
| VM clocks | Both VMs GCE metadata NTP, stratum 2 |
| Pre-campaign smoke | Three 5,007-point smoke runs after WSL CA fix (460 delivered, 0 failed on first smoke) |

WSL producer OIDC: see [ready-check issue](#ready-check-issue-wsl-producer-ca-path).

## Deployed baseline

| Component | Version / image | Compute |
|-----------|-----------------|---------|
| Core | Release **0.3.2** (`core/current.json` → `core/releases/0.3.2`) | GCE `core`: `c4d-highcpu-4`, image `chronoracer-core-1792842859`, 20 GB hyperdisk-balanced |
| Kafka | Image `chronoracer-kafka-1792842845` | GCE `kafka`: `n4-standard-2`, 20 GB hyperdisk-balanced |
| InfluxDB 3 Core | 3.12.1 | On Core VM; flags per [`influxdb3.service`](../../../../core/startup/influxdb3.service) |
| Telemetry | Cloud Run `telemetry-00001-zg8`, `europe-west1` | — |

Core config (effective, overlay [`load-testing-core-config.yaml`](../../../../load-test/load-testing-core-config.yaml)):
processing batch 1000 / 5 ms; persistence flush 50,000 lines / 4000 ms, fast 3000 / 2000 ms;
`max_inflight_db_writes=7`; `data` 4 partitions; DB export ZSTD level 6 to `/export/db`.

Generator: WSL2 on Windows, Ubuntu 26.04, kernel 6.6.87.2, 28 logical CPUs, 15 GiB RAM, chrony NTP-synced;
uplink ~100 Mbit/s (operator-confirmed). Generator host load ≤4 throughout.

## Deviations

All test-only; none are in the repository's service units or configs. VMs were destroyed afterwards.

| Time | Change | Reason |
|------|--------|--------|
| ready check | `curls/load_test/load_test.py`: `https.ca.location` = host OpenSSL CA file | WSL OIDC failure (below); committed code fix |
| 18:56:46 | Kafka `data` `retention.ms` 48 h → 15 min (`kafka-configs.sh --alter`) | Kafka disk 65% with no deletion during the campaign |
| 19:53:06 | Influx drop-in `INFLUXDB3_WAL_FILES_PER_SNAPSHOT=200` | Snapshot A/B (no effect) |
| 20:26:15 | Hard-delete all 38 `chrono_db` tables (`influxdb3 delete table --hard-delete now`) | Core disk 75%; export covers only later data |
| 20:28:26 | Drop-in `INFLUXDB3_SNAPSHOT_CONCURRENCY_LIMIT=3` (WAL files back to 600) | L800c |
| 20:38:56 | Drop-in `ExecStart` with `--force-snapshot-mem-size 15%` (+ concurrency 3) | L800d and all later runs |
| after L800b | 300 s runs, `METRIC_WAIT_S=180` (faults 240) | Operator: shorter tuning runs |
| 21:48:54 | Drop-in `INFLUXDB3_DELETE_GRACE_PERIOD=1m` | Purged tables still listed by the exporter |
| 22:06:47 | Drop-in `--exec-mem-pool-size 50%` | Export retry |

Fault injection ran at the L800d settings, not at the runbook's "same baseline configuration".

After L800b the operator used **300 s** generation and `METRIC_WAIT_S=180` for tuning (faults `METRIC_WAIT_S=240`):
Core can run on 1 vCPU during Influx bursts; export speed after the race was not a constraint (more Parquet files
acceptable). Short post-run wait can miss final native-metric samples — re-run `collect-artifacts.sh` later for the
loss check if needed.

## Ready-check issue: WSL producer CA path

Bundled librdkafka in the `confluent-kafka` 2.15.0 wheel looked for `/etc/pki/tls/certs/ca-bundle.crt` for the OIDC
token endpoint (`error adding trust anchors from file`), a path absent on Debian/Ubuntu. `https.ca.location=probe`
did not change it. The generator now passes `ssl.get_default_verify_paths().cafile` when the host has one.

## Periodic spikes at 800K: InfluxDB forced snapshots

At 800K the InfluxDB write buffer reaches `--force-snapshot-mem-size 30%` (1.75 GB) in ~37 s, so every snapshot is
forced (`forcing snapshot as buffer size > mem threshold`, then `snapshotting all`). Each writes ~30M rows to
Parquet across 38 tables and, with the default `--snapshot-concurrency-limit` (number of CPUs), occupies all
4 vCPUs for ~20 s of every 40 s (1 s `vmstat`). Core is starved, writes slow and the sink waits on full channels
(L800 sink blocked 8.2%). Monitoring's 60 s samples alias the 40 s cycle into a 2 min pattern.

- `--wal-files-per-snapshot 200` had no effect (L800b: 14 of 15 snapshots forced). Regular snapshots appear to
  persist only completed `--gen1-duration` (10 min) windows; inferred from logs, not documented.
- `--snapshot-concurrency-limit 3` removed the backpressure (L800c sink 0.04%); adding
  `--force-snapshot-mem-size 15%` halved the cycle to 20 s and the anon peak to 2.51 GiB (L800d sink 0%).
- Cost: more, smaller overlapping Parquet files, which contributed to the export failure below.
- Persistence latency was unchanged by any setting; it is dominated by upstream delivery latency.

Secondary: Core disk shows 1–6 s write bursts every 10–12 s at the 170 MiB/s provisioned cap (kernel dirty-page
writeback, `vm.dirty_background_ratio=10`); IOPS stays far below 3,120. A size increase adds little throughput on
hyperdisk-balanced (`1.5x + 140` MiB/s default); provisioned throughput can be raised separately up to the
400 MiB/s `c4d-*-4` limit. Not changed.

The InfluxDB cgroup `memory.current` sits at 4.74 GiB (`MemoryHigh`) under load because it counts reclaimable page
cache; anonymous memory peaked at 3.50 GiB. The dashboard's "used" memory excludes cache.

## Producer → Kafka transport plateau

| Run | Target | Kafka `ens3` RX | Bytes/point on the wire | Producer delivery p50 (most windows) |
|-----|-------:|----------------:|------------------------:|-------------------------------------:|
| L650 | 650K | 12.6–12.7 MB/s | 19.4 | ≤50 ms |
| L800 | 800K | 13.0–13.4 MB/s | 16.5 | 100–500 ms |
| L800d | 800K | 13.14–13.21 MB/s | 16.5 | 100–500 ms |
| L900 | 900K | 12.9–13.1 MB/s | 14.4 | 200 ms–30 s |
| L1000 | 1M | 12.1 MB/s | 12.1 | ≤500 ms–30 s |

Larger lz4 batches compress better, so throughput kept rising above the ~105 Mbit/s plateau, but records queue in
the producers (per-process queues up to 97K messages at L900). Persist latency is measured from producer record
timestamps and includes this wait: at L900 Core lag was 0 yet persist mean was 15.5 s. Producer delivery buckets and
per-run extensions: [throughput](throughput.md).

## Core/InfluxDB ceiling

At 1M (L1000) persistence saturates below target rate; see [results — L1000](../core-0.3.2-results.md#throughput-steps)
and [throughput — L1000](throughput.md#l1000-205208z205713z) for live pipeline and 1 s sampling.

## Export failure

Chronology, root cause, and proposed Core fix: [export](export.md).

## Monitoring and collection notes

- Cloud Logging returned HTTP 500 for every `gcloud logging read` during L800 and L800b collection (also once in
  L650); logs were recovered from VM journals into `artifacts/logs-journal/` - probably issue with the scripts.
- `pipeline_sink_blocked_ms` can exceed 100% of a sample when one long wait ends in it (F-stop 124%), as
  [documented](../../../component-core.md#cloud-monitoring-write-counters).
- systemd `NRestarts` does not count `systemctl restart`; use `ActiveEnterTimestamp` to detect F-restart.
- `fault.log` also captures the gcloud IAP "install NumPy" warning (cosmetic).
- A planned graceful Core stop logs 5 WARN "ingestion loop terminated" lines.
