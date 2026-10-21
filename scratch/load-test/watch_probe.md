# Core `Watch` probe (load test)

## Reachability

| Path | Works? |
|------|--------|
| Telemetry Cloud Run → Core internal DNS (`core.<zone>.c.chronoracer.internal:9470`) | Yes (firewall `chronoracer-core-telemetry`, source `10.20.2.0/26`) |
| Laptop `gcloud compute start-iap-tunnel core 9470 …` | **No** — `chronoracer-iap-ssh` allows `35.235.240.0/20` → **tcp:22** only, not 9470 |
| Laptop via IAP **SSH** local forward | Yes — SSH is allowed; forward Core loopback telemetry port |

```bash
# From a host with IAP tunnel accessor on the core VM (assume operator account running this tests has permissions)
gcloud compute ssh core --project=chronoracer --zone=europe-west1-b --tunnel-through-iap -- \
  -N -L 9470:127.0.0.1:9470
```

Alternatively run the probe **on the Core VM** (`127.0.0.1:9470`) after copying the script and using `/tmp/lt-venv`.

Regenerate Python stubs (optional):

```bash
python3 -m venv /tmp/lt-venv
/tmp/lt-venv/bin/pip install grpcio grpcio-tools
/tmp/lt-venv/bin/python -m grpc_tools.protoc \
  -I../../core/proto \
  --python_out=gen --grpc_python_out=gen \
  ../../core/proto/telemetry_v1.proto
```

## Python probe

```bash
/tmp/lt-venv/bin/python watch_probe.py --help
/tmp/lt-venv/bin/python watch_probe.py --target localhost:9470 --client-id loadprobe
/tmp/lt-venv/bin/python watch_probe.py --target localhost:9470 --client-id loadprobe --slow 2
```

Uses `client_id` **`loadprobe`**, **all sensors** (`AllSensors`). Every 10 s prints points/s, cumulative `dropped_point_count` from Core `gap` events, and end-to-end lag `now - timestamp_nanos` (p50/p99/max). Lag includes producer batching (timestamps are reading times).

**Subscriber budget:** Core default `telemetry.max_subscribers: 4`. Race stack uses `telemetry` (1) + `loadprobe` (1) = 2; room for Analysis or one more probe.

## grpcurl (alternative)

With port forward active and [grpcurl](https://github.com/fullstorydev/grpcurl) installed:

```bash
grpcurl -plaintext -d '{"client_id":"loadprobe","all":{}}' \
  -import-path ../../core/proto -proto telemetry_v1.proto \
  localhost:9470 chronoracer.telemetry.v1.TelemetryService/Watch
```

Streaming output is raw JSON per message; use the Python probe for periodic aggregates.

## Correlating with Core

Compare probe lag with Core `Pipeline report` **`preview_ms`** (record timestamp → telemetry handoff). A slow `--slow` probe should increase probe gaps/lag without raising Core persistence/preview metrics for other subscribers.
