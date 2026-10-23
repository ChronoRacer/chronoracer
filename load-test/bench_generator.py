#!/usr/bin/env python3
"""Measure local encode + drive-model throughput (no real Kafka)."""

import json
import os
import random
import sys
import time
from collections import deque
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LOAD_TEST_DIR = REPO / "curls" / "load_test"
sys.path.insert(0, str(LOAD_TEST_DIR))

import load_test as lt  # noqa: E402

CONFIG_PATH = (Path(__file__).resolve().parent / "generator-config.json").resolve()


class FakeProducer:
    """Minimal stand-in for confluent_kafka.Producer used by produce_sensor_messages."""

    def __init__(self, queue_limit=100_000):
        self._queue = []
        self._queue_limit = queue_limit

    def produce(self, on_delivery=None, **kwargs):
        if len(self._queue) >= self._queue_limit:
            raise BufferError("fake queue full")
        self._queue.append(kwargs)

    def poll(self, _timeout=0):
        # load_test.produce retries on BufferError until poll frees space, so poll must drain.
        drained = len(self._queue)
        self._queue.clear()
        return drained

    def flush(self, _timeout=None):
        self._queue.clear()

    def __len__(self):
        return len(self._queue)


def bench_fixed_point_count(config, channels, model, rng, load, point_count, iterations):
    """Time model step + Avro for messages with exactly `point_count` readings."""
    window_ns = int(config["batch_window_ms"]) * 1_000_000
    tick_s = load["tick_ms"] / 1000.0
    names = list(channels)
    history = deque(maxlen=int(window_ns / 1e9 / tick_s) + 2)
    model_ticks = 0
    first_tick = 0
    elapsed_s = 5.0
    started_ns = time.time_ns()
    lap_offset_s = rng.uniform(0.0, model.lap_duration_s)

    t0 = time.perf_counter()
    for _ in range(iterations):
        while model_ticks * tick_s <= elapsed_s:
            if len(history) == history.maxlen:
                first_tick += 1
            history.append(model.step(lap_offset_s + model_ticks * tick_s, tick_s))
            model_ticks += 1
        name = rng.choice(names)
        channel = channels[name]
        count = point_count
        deltas = sorted(rng.sample(range(1, window_ns + 1), count)) if count <= window_ns else []
        now_ns = started_ns + int(elapsed_s * 1e9)
        base_ns = now_ns - window_ns
        readings = []
        for delta in deltas:
            point_elapsed_s = elapsed_s - (window_ns - delta) / 1e9
            index = lt.clamp(int(point_elapsed_s / tick_s) - first_tick, 0, len(history) - 1)
            readings.append({"delta": delta, "value": lt.finish_value(channel, history[index][name])})
        lt.serialize_record({"sensor_id": name, "timestamp": base_ns, "readings": readings})
        elapsed_s += count / load["points_per_second"]
    elapsed = time.perf_counter() - t0
    total_pts = iterations * point_count
    return total_pts / elapsed, iterations / elapsed


def bench_full_loop(duration_s=8.0, target_rate=10_000_000):
    """Run produce_sensor_messages with FakeProducer at an unrealistically high target rate."""
    lt.CONFIG_PATH = CONFIG_PATH
    config = json.loads(CONFIG_PATH.read_text())
    load = dict(config["load"])
    load["points_per_second"] = target_rate
    load["duration_s"] = duration_s
    load["seed"] = 42
    rng = random.Random(load["seed"])
    channels = lt.load_channels(config)
    model = lt.build_model(config, load["seed"])
    producer = FakeProducer()
    stats = lt.SendStats()

    lt.produce_sensor_messages(
        producer, config, channels, model, rng, load, stats, report_interval_s=1e9
    )
    producer.flush()
    return stats


def main():
    measure_s = float(sys.argv[1]) if len(sys.argv) > 1 else 12.0
    lt.CONFIG_PATH = CONFIG_PATH
    config = json.loads(CONFIG_PATH.read_text())
    load = dict(config["load"])
    seed = 42
    rng = random.Random(seed)
    channels = lt.load_channels(config)
    model = lt.build_model(config, seed)

    print(f"config={CONFIG_PATH}")
    print(f"nproc={len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else '?'}")

    iterations = max(300, int(measure_s * 1500))
    print("\nper_message_cost (fixed points/msg):")
    for pts in (1, 5, 10, 20):
        pps, mps = bench_fixed_point_count(config, channels, model, random.Random(seed), load, pts, iterations)
        print(f"  pts/msg={pts:2d}  points/s={pps:,.0f}  messages/s={mps:,.0f}  us/point={1e6/pps:.2f}")

    loop_s = min(measure_s, 10.0)
    stats = bench_full_loop(duration_s=loop_s)
    with stats.lock:
        elapsed = max(time.monotonic() - stats.total["started"], 1e-3)
        pts = stats.total["points"]
        msgs = stats.total["messages"]
    print(f"\nfull_loop_fake_producer: duration_s={loop_s:.1f} behind_max_s={stats.total['behind_s']:.3f}")
    print(f"  achieved points/s={pts/elapsed:,.0f} messages/s={msgs/elapsed:,.0f}")
    print(f"  sustainable_15k_with_40pct_headroom_per_instance≈{pts/elapsed/1.4:,.0f} pts/s target")


if __name__ == "__main__":
    main()
