# Export flow — lt032 detail

Outcome summary: [core-0.3.2-results — export flow](../core-0.3.2-results.md#export-flow). Campaign context:
[campaign](campaign.md).

## Pre-export state (operator chose to run as-is)

- Core `/` **79%** (4.0 GB free of 20 GB); Influx Parquet `core/dbs` 11 GB, WAL 1.3 GB.
- Export config: ZSTD level 6, `vm_export_dir: /export/db` (same root disk as Core). Exporter writes all tables locally
  before GCS upload (`monitor.rs`), so export could exhaust disk. Operator observed Core behaviour instead of resize or
  purge. Watcher: 2 s `df`, `/export/db` size, unit states, Core export journal.

### information_schema / purge interaction

Exporter lists tables via `SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox'`
([`database_exporter.rs`](../../../../core/src/persistence/database_exporter.rs)). After the 20:26Z hard-delete purge,
38 soft-deleted tables (`<table>-20261026T202615`) still listed until `--delete-grace-period` (24 h) while data files
were already gone — export would have targeted 76 tables, 38 artifacts.

**Test-only fix (21:48:54Z):** drop-in `INFLUXDB3_DELETE_GRACE_PERIOD=1m`; Influx restarted idle (ready 6 s). Hard
deleter ran at 60 s grace; `information_schema`: 0 deleted, 38 live.

## Attempt 1 (exec pool 20%, default)

`export_flow.py load-test/runs/F-restart` **21:54:00Z–22:00:01Z**, exit 1.

`artifacts/export/result.json`: `sequence_complete` true, `kafka_export_complete` true (dlq, 8.4 s),
**`database_tables_complete` false**, `database_objects_complete` false, `pubsub_published` false,
`no_export_errors` false; 0 GCS objects.

Core journal highlights:

- 21:54:01.524Z SHUTDOWN, draining; snapshot `lines 141,687,939`; 21:54:01.573Z database export start.
- Each table, 4 attempts (1/2/4 s backoff): `Failed to read table time bounds` — Tonic `Resources exhausted`,
  `deduplicate batches` / `ExternalSorter` OOM (~56–91 MB per consumer).
- 21:59:49Z export error, no upload; shutdown notification not sent (Kafka true, DB false).
- Then `max.poll.interval.ms` exceeded on `sensor_data` → `UnknownMemberId` offset commit failure; 21:59:49.681Z run
  complete.

Disk **not** the limit: `/export/db` ≤1 MB; Core `/` 78–80% (≥3.9 GB free); Influx `NRestarts=0`.

## Retry setup (22:06:47Z)

Drop-in `--exec-mem-pool-size 20%` → **50%** (`runs/lt032-influx-export-retry.sh`); Influx restarted; Core started idle
22:06:48Z. No ingest during export.

**22:08:10Z:** first retry aborted — `export_flow.py` refuses existing `artifacts/export` (`mkdir(exist_ok=False)`);
attempt 1 evidence copied to `artifacts/export-attempt1/`. Verified identical copy, removed `artifacts/export`, re-ran
~22:10Z (attempt 2). Core stayed active idle meanwhile.

Attempt 1 monitor: early `accel_lat` had Tonic connection errors before steady `deduplicate batches` OOM; Influx
`service_grpc_flight` WARNs; disk min avail 3,960 MB.

## Attempt 2 (exec pool 50%)

**22:09:22Z–22:16:33Z**, exit 1; same check failures as attempt 1. SHUTDOWN 22:09:23Z; Kafka export 7.38 s; 38/38 tables
failed at time bounds; no upload. Influx `MemoryCurrent` 3.04 GB after; disk unchanged.

Failure stage moved to non-spillable merge: `ExternalSorterMerge[0] ... (can spill: false) consumed 126.9 MB` per input;
example query on `"accel_lat"`. See [root cause](#root-cause) for file layout.

## Root cause

Core's exporter runs `SELECT min(time) AS min_time, max(time) AS max_time FROM <table>` first
([`database_exporter.rs`](../../../../core/src/persistence/database_exporter.rs) ~line 211). InfluxDB 3 Core
deduplicates overlapping Parquet on read, so the aggregate sort-merges every row. At ~1.77B points since the purge
(~47M rows/table, 3,902 files, ~103/table) the Flight SQL memory pool exhausts on attempt 1 (`ExternalSorter`) and
on attempt 2 at 50% pool (`ExternalSorterMerge`, non-spillable). L800d `--force-snapshot-mem-size 15%` increased
overlapping file count. 0.3.1 exported 38/38 at lower volume; `throttle` hit the same pool once and retried OK.

## Proposed Core change (unverified; stack destroyed)

- Derive time bounds without row scans (e.g. min/max on `system.parquet_files` plus SHUTDOWN time for unpersisted data;
  verify columns on 3.12.1).
- Keep export window queries bounded; at this file density a gen1 window can still merge many files.
- Pause or close the data consumer before export, or raise `max.poll.interval.ms` (export exceeded 300 s →
  `UnknownMemberId` on final `sensor_data` commit after drain).

Volume context: ~54M points/hour total at 15K pts/s race rate; whether normal races hit this limit is untested.

## Follow-up: Core 0.3.3 (local validation only)

Implemented: windows planned from `system.parquet_files` with a row budget, and the data consumer is held in its group
through the export ([InfluxDB export](../../../component-core.md#influxdb-export),
[final commit](../../../component-core.md#kafka-delivery)). The service now ships `--snapshot-concurrency-limit`
(vCPUs − 1) and `--force-snapshot-mem-size 15%` ([memory budget](../../../component-influxdb.md#memory-budget)).

Local check, 2026-10-27: Docker `influxdb:3.12.1-core`, 6 GB container, `--exec-mem-pool-size 192MB`,
`--force-snapshot-mem-size 32MB` (denser overlap than 15% on the VM), concurrency 3. Six tables with one series
each over 50 min: five with 6M rows (12–17 files), `accel_lat` with 48M rows written (66 overlapping files,
47,999,875 unique).

| Query / export | Result |
|----------------|--------|
| `min(time)/max(time)` on `accel_lat` (0.3.2 bounds query) | `Resources exhausted` (`deduplicate batches`, `ExternalSorterMerge`) |
| `system.parquet_files` (`min_time`/`max_time` Int64, `row_count` UInt64) | sums match persisted rows; ~0.2 s |
| Export, `max_rows_per_window` 1,000,000 | pass, 41 s; every table matches its unique row count |
| Export, 2,000,000 | pass, 38 s; InfluxDB container peak 1.67 GiB |
| Export, 8,000,000 | fail on `accel_lat`, `Resources exhausted` |

The largest `ORDER BY time` window that passed at 192 MB held ~2.4M rows. The 1M default leaves a wide margin at the
VM's ~1.4 GB pool. Not yet re-run on the VM at load-test volume.

## Consequences

- Race data not exported; `notify_shutdown_on_failed_db_export: false` → no export-done Pub/Sub.
- F-restart loss proof via Parquet reconciliation not available.
- Retry requires Core restart after each failed export (`Run complete` ends the race). `export_flow.py` refuses an
  existing `artifacts/export` — move (not copy) evidence before retrying.
