# Fault injection — lt032 detail

Pass/fail summary (producer totals, loss check, lag peak, sink blocked, persist mean):
[core-0.3.2-results — fault injection](../core-0.3.2-results.md#fault-injection-800k-300-s-fault-at-120-s-l800d-influxdb-settings).
Campaign: [campaign](campaign.md).

800K pts/s, 300 s each, fault at ~+120 s, `METRIC_WAIT_S=240`, **L800d Influx settings** (not runbook baseline). At
this rate producer delivery p50 is already 100–500 ms. Backlog estimate for a 30 s DB stall: ~24M points; drain
headroom ~160K pts/s → ~150 s recovery.

## F-freeze (21:15Z run)

Fault: SIGSTOP 21:17:17Z, SIGCONT 21:17:46Z (29 s).

| Extension | Value |
|-----------|-------|
| Producer delivery | p99 ≤5 s, max 4,041 ms; pacing max 0.005 s |
| Write retries | 7× on resume (`retries_so_far=0`); max 0.12/s |
| Core CPU max | 68.8% |

`Pipeline report`: 21:18Z consumed 51,715 msg/s, sink_blocked 88.7%, persist mean 25.3 s; 21:19Z 96,615 / 91.0% /
1,427,438 unconsumed; 21:20Z 79,271 / 20.9% / 224,682; 21:21Z idle. Catch-up peaked 96.6K msg/s (~1M pts/s). Producer
unaffected (Kafka buffered).

1 s sampling: pre-fault CPU 59%; frozen 5%; catch-up 74%, anon ≤3.05 GiB, iowait ≤46%; tail 28%. Influx `Ssl` after
SIGCONT (not left stopped).

WARN: `offset_ledger: Commit position has not advanced ... stalled=[2]` at 21:15:14Z (idle pre-run).

## F-stop (21:26Z run)

Fault: stop 21:27:53Z, start 21:28:23Z (30 s).

| Extension | Value |
|-----------|-------|
| Producer delivery | p99 ≤5 s, max 3,857 ms |
| Write retries | 10× `Database write failed, retrying`; 1× partition pause 30 s |
| Core disk peak | 77% ~21:31Z during catch-up |
| Core CPU max | 71.9% |

`Pipeline report`: 21:29Z consumed 72,533 msg/s, sink_blocked 124.0%, unconsumed 200,003, persist mean 32.3 s (max
44.0 s); 21:30Z 99,850 / 90.2% / 1,666,279; 21:31Z 50,476 / 15.9% / 262,768; 21:32Z idle. Catch-up 99.9K msg/s.
124% sink blocked: one ~74 s wait in one 60 s sample ([metric](../../../component-core.md#cloud-monitoring-write-counters)).

1 s sampling: stopped CPU 5%; catch-up 73%, anon ≤3.00 GiB, iowait ≤37%. `fault.log` includes IAP NumPy warning
(cosmetic).

## F-restart (21:36Z run)

Fault: `systemctl restart chronoracer-core` 21:38:56Z → active 21:38:58Z.

| Extension | Value |
|-----------|-------|
| Producer delivery | p99 ≤2 s, max 2,452 ms |
| Accepted lines | 2 counter epochs; Δ = generator total; harness `matches: null` |
| Core CPU max | 60.8% |

Journal: 21:38:58.276Z drain signal → 21:38:58.426Z pipeline drained (150 ms) → 21:38:58.457Z final `Pipeline report`
→ 21:38:58.701Z Core v0.3.2 → 21:38:58.828Z Influx connected. `ActiveEnterTimestamp` 17:56:09Z → 21:38:58Z;
`NRestarts=0` (manual restart). Producer queues ≤8,890, cleared ~20–30 s. Core disk 72% → 79%.

Five WARN lines on graceful stop (`… ingestion loop terminated`, etc.) — would page a WARN-based alert. Final counters
published at shutdown; no visible gap between epochs in accepted lines. Parquet reconciliation not done (export failed).
