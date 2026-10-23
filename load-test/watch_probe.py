#!/usr/bin/env python3
"""Direct Core TelemetryService.Watch subscriber for load-test isolation checks."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

GEN = Path(__file__).resolve().parent / "gen"
sys.path.insert(0, str(GEN))

import grpc  # noqa: E402
import telemetry_v1_pb2 as pb2  # noqa: E402
import telemetry_v1_pb2_grpc as pb2_grpc  # noqa: E402

REPORT_INTERVAL_S = 10.0


def percentile(sorted_ms: list[float], p: float) -> float | None:
    if not sorted_ms:
        return None
    idx = min(len(sorted_ms) - 1, int(round((p / 100.0) * (len(sorted_ms) - 1))))
    return sorted_ms[idx]


def main() -> int:
    parser = argparse.ArgumentParser(description="Subscribe to Core telemetry Watch (plaintext gRPC).")
    parser.add_argument(
        "--target",
        default="localhost:9470",
        help="host:port (use SSH -L after IAP SSH; IAP TCP tunnel to 9470 is not in firewall rules)",
    )
    parser.add_argument("--client-id", default="loadprobe", help="Watch client_id (must differ from telemetry)")
    parser.add_argument(
        "--slow",
        type=float,
        metavar="SECONDS",
        default=0.0,
        help="Sleep this long after each WatchResponse (simulate a slow subscriber)",
    )
    args = parser.parse_args()

    channel = grpc.insecure_channel(args.target)
    stub = pb2_grpc.TelemetryServiceStub(channel)
    request = pb2.WatchRequest(client_id=args.client_id, all=pb2.AllSensors())

    window_points = 0
    window_latencies_ms: list[float] = []
    total_gaps_dropped = 0
    window_start = time.monotonic()
    next_report = window_start + REPORT_INTERVAL_S

    print(f"watch_probe client_id={args.client_id} target={args.target} slow={args.slow}s", flush=True)

    try:
        for response in stub.Watch(request):
            if args.slow > 0:
                time.sleep(args.slow)

            which = response.WhichOneof("payload")
            if which == "hello":
                hello = response.hello
                print(
                    f"hello race_id={hello.race_id} stream_id={hello.stream_id} "
                    f"selected_sensor_count={hello.selected_sensor_count}",
                    flush=True,
                )
            elif which == "gap":
                dropped = int(response.gap.dropped_point_count)
                total_gaps_dropped += dropped
                print(
                    f"gap first_sequence={response.gap.first_sequence} dropped={dropped} "
                    f"total_dropped={total_gaps_dropped}",
                    flush=True,
                )
            elif which == "point_batch":
                now_ns = time.time_ns()
                for point in response.point_batch.points:
                    window_points += 1
                    lag_ms = (now_ns - int(point.timestamp_nanos)) / 1_000_000.0
                    window_latencies_ms.append(lag_ms)

            now = time.monotonic()
            if now >= next_report:
                elapsed = now - window_start
                pps = window_points / elapsed if elapsed > 0 else 0.0
                lat_sorted = sorted(window_latencies_ms)
                p50 = percentile(lat_sorted, 50)
                p99 = percentile(lat_sorted, 99)
                pmax = lat_sorted[-1] if lat_sorted else None
                print(
                    f"stats interval_s={elapsed:.1f} points_per_s={pps:.1f} "
                    f"gaps_dropped_sum={total_gaps_dropped} "
                    f"latency_ms p50={p50} p99={p99} max={pmax}",
                    flush=True,
                )
                window_points = 0
                window_latencies_ms.clear()
                window_start = now
                next_report = now + REPORT_INTERVAL_S
    except grpc.RpcError as err:
        print(f"rpc_error code={err.code()} details={err.details()}", flush=True)
        return 1
    finally:
        channel.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
