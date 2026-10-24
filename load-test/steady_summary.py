#!/usr/bin/env python3
"""Summarize raw Monitoring samples; missing data never becomes a zero/pass."""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from collect_metrics import CORE_METRICS, parse_rfc3339
from summarize_generators import generator_summary


def read_metric(directory, name):
    path = directory / f"{name}.csv"
    if not path.exists():
        return []
    with path.open() as fh:
        return list(csv.DictReader(fh))


def identity(row):
    return row["resource_type"], row["resource_labels"], row["metric_labels"]


def counter_intervals(rows):
    """Difference only adjacent points in the same process epoch and label set.

    The first point of an epoch has an implicit zero at its start_time. This
    recovers post-restart writes without subtracting the previous process total.
    """
    groups = defaultdict(list)
    for row in rows:
        groups[identity(row)].append(row)
    intervals = []
    for group in groups.values():
        previous = None
        for row in sorted(group, key=lambda r: r["end_time"]):
            end = parse_rfc3339(row["end_time"])
            epoch = parse_rfc3339(row["start_time"])
            value = float(row["value"])
            if previous and previous["start_time"] == row["start_time"]:
                begin = parse_rfc3339(previous["end_time"])
                delta = value - float(previous["value"])
            else:
                begin, delta = epoch, value
            if end > begin and delta >= 0:
                intervals.append({"start": begin, "end": end, "delta": delta,
                                  "seconds": (end - begin).total_seconds(),
                                  "labels": json.loads(row["metric_labels"])})
            previous = row
    return sorted(intervals, key=lambda r: r["end"])


def stats(values):
    return {"samples": len(values), "min": min(values), "median": statistics.median(values),
            "max": max(values)} if values else None


def distribution_summary(rows, start, end):
    count, weighted_sum = 0, 0.0
    buckets, bounds = None, None
    windows = 0
    for row in rows:
        if not start <= parse_rfc3339(row["start_time"]) < parse_rfc3339(row["end_time"]) <= end:
            continue
        dist = json.loads(row["value"])
        n = int(dist.get("count", 0))
        if not n:
            continue
        options = dist.get("bucket_options", {})
        explicit = options.get("explicit_buckets")
        if explicit is None:
            raise ValueError("Core persistence latency requires explicit histogram buckets")
        current_bounds = explicit.get("bounds", [])
        counts = [int(v) for v in dist.get("bucket_counts", [])]
        counts += [0] * (len(current_bounds) + 1 - len(counts))
        if bounds is not None and bounds != current_bounds:
            raise ValueError("histogram bucket boundaries changed during the run")
        bounds = current_bounds
        if buckets is None:
            buckets = [0] * len(counts)
        for i, value in enumerate(counts):
            buckets[i] += value
        count += n
        weighted_sum += n * float(dist.get("mean", 0))
        windows += 1
    if not count:
        return None

    def bound(q):
        rank, seen = math.ceil(q * count), 0
        for i, n in enumerate(buckets):
            seen += n
            if seen >= rank:
                return {"upper_bound_ms": bounds[i], "overflow": False} if i < len(bounds) else {
                    "lower_bound_ms": bounds[-1], "overflow": True}
        raise ValueError("histogram count does not match bucket counts")

    return {"windows": windows, "batch_samples": count, "mean_ms": weighted_sum / count,
            "p50": bound(0.5), "p99": bound(0.99)}


def summarize(run, warmup):
    meta = json.loads((run / "run-metadata.json").read_text())
    start, end = parse_rfc3339(meta["start_utc"]), parse_rfc3339(meta["end_utc"])
    steady_start = start + timedelta(seconds=warmup)
    art = run / "artifacts"
    metrics = art / "metrics"
    out = {"id": run.name, "run": meta, "steady_start": steady_start.isoformat(),
           "generators": generator_summary(sorted(run.glob("gen-*.log")), warmup),
           "metrics": {}, "unavailable": ["decoded points/s", "preview latency", "write RPC latency"]}
    for name in CORE_METRICS:
        rows = read_metric(metrics, name)
        if name == "pipeline_persist_latency_ms":
            out["metrics"][name] = distribution_summary(rows, steady_start, end)
        elif name == "pipeline_unconsumed_messages":
            selected = sorted((r for r in rows if steady_start <= parse_rfc3339(r["end_time"]) <= end),
                              key=lambda r: r["end_time"])
            vals = [float(r["value"]) for r in selected]
            after = [float(r["value"]) for r in rows if parse_rfc3339(r["end_time"]) > end]
            out["metrics"][name] = {"steady": stats(vals), "first": vals[0] if vals else None,
                                    "last": vals[-1] if vals else None,
                                    "post_run_max": max(after, default=None),
                                    "post_run_last": float(max(rows, key=lambda r: r["end_time"])["value"]) if after else None}
        else:
            intervals = counter_intervals(rows)
            selected = [r for r in intervals if steady_start <= r["start"] < r["end"] <= end]
            groups = defaultdict(list)
            for interval in selected:
                key = json.dumps(interval["labels"], sort_keys=True)
                rate = interval["delta"] / interval["seconds"]
                groups[key].append(rate / 10 if name == "pipeline_sink_blocked_ms" else rate)
            out["metrics"][name] = {"steady_rates": {k: stats(v) for k, v in groups.items()},
                                      "unit": "percent" if name == "pipeline_sink_blocked_ms" else "per_second"}
            if name == "influx_write_lines":
                # Enclose the entire generation + drain window; never interpolate
                # 60 s counters into an exact run total.
                before = [r for r in rows if parse_rfc3339(r["end_time"]) <= start]
                after = [r for r in rows if parse_rfc3339(r["end_time"]) >= end]
                baseline = max(before, key=lambda r: r["end_time"], default=None)
                final = max(after, key=lambda r: r["end_time"], default=None)
                begin = parse_rfc3339(baseline["end_time"]) if baseline else start
                finish = parse_rfc3339(final["end_time"]) if final else end
                relevant = [r for r in intervals if begin <= r["start"] < r["end"] <= finish]
                observed = int(sum(r["delta"] for r in relevant)) if baseline and final else None
                expected = out["generators"]["total_points"]
                out["accepted_lines_check"] = {"baseline_time": baseline["end_time"] if baseline else None,
                    "final_time": final["end_time"] if final else None, "observed": observed,
                    "generator_points": expected,
                    "counter_epochs": len({r["start_time"] for r in rows}),
                    "matches": observed == expected if observed is not None and expected is not None and len({r["start_time"] for r in rows}) == 1 else None,
                    "scope": "padded Monitoring window; requires exclusive traffic and drained boundaries; accepted lines include replay"}
    for vm in ("core", "kafka"):
        rows = read_metric(metrics, vm + "_cpu")
        out[vm + "_cpu_pct"] = stats([100 * float(r["value"]) for r in rows
                                      if steady_start <= parse_rfc3339(r["end_time"]) <= end])
    out["generators"]["expected_instances"] = meta["instances"]
    out["generators"]["complete"] &= len(out["generators"]["instances"]) == meta["instances"]
    coverage_file = metrics / "meta.json"
    out["coverage"] = json.loads(coverage_file.read_text()) if coverage_file.exists() else None
    out["missing_steady_metrics"] = [k for k, v in out["metrics"].items()
        if v is None or ("steady_rates" in v and not v["steady_rates"]) or ("steady" in v and v["steady"] is None)]
    count_file = art / "influx-count.csv"
    totals = [line.split(",")[1] for line in count_file.read_text().splitlines() if line.startswith("TOTAL,")] if count_file.exists() else []
    total = int(totals[-1]) if totals else None
    expected = out["generators"]["total_points"]
    out["influx_count_check"] = {"observed": total, "matches": total == expected if total is not None and expected is not None else None,
                                  "scope": "unique stored rows; timestamp collisions may merge points"}
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("--warmup-s", type=float, default=60)
    args = parser.parse_args()
    out = summarize(args.run, args.warmup_s)
    text = json.dumps(out, indent=2) + "\n"
    (args.run / "artifacts").mkdir(exist_ok=True)
    (args.run / "artifacts/steady-summary.json").write_text(text)
    print(text, end="")
    return int(bool(out["missing_steady_metrics"]) or not out["generators"]["complete"]
               or bool(out["generators"]["total_failed"]) or bool(out["generators"]["queued_final"]))


if __name__ == "__main__":
    raise SystemExit(main())
