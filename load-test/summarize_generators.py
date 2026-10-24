#!/usr/bin/env python3
"""Parse load_test [Report]/[Summary] lines from generator logs into CSV."""

import argparse
import csv
import re
import json
import sys
from pathlib import Path

# Matches SendStats._line fields after the [Report]/[Summary] prefix.
LINE_RE = re.compile(
    r"^window_s=(?P<window_s>[-0-9.]+)\s+"
    r"sent_pts_s=(?P<sent_pts_s>[-0-9.]+)\s+"
    r"sent_msg_s=(?P<sent_msg_s>[-0-9.]+)\s+"
    r"behind_max_s=(?P<behind_max_s>[-0-9.]+)\s+"
    r"queued=(?P<queued>\d+)\s+"
    r"delivered=(?P<delivered>\d+)\s+"
    r"failed=(?P<failed>\d+)\s+"
    r"delivery_ms=(?P<delivery_ms>.+)$"
)

P99_RE = re.compile(r"p99(<=?\d+|>\d+)")


def parse_line(line: str) -> dict | None:
    line = line.strip()
    # Allow optional UTC timestamp prefix: 2026-01-01T12:00:00Z ...
    if "[Report]" in line:
        kind = "report"
        _, rest = line.split("[Report]", 1)
    elif "[Summary]" in line:
        kind = "summary"
        _, rest = line.split("[Summary]", 1)
    else:
        return None
    m = LINE_RE.match(rest.strip())
    if not m:
        return None
    row = m.groupdict()
    row["kind"] = kind
    row["source_line"] = line
    return row


def p99_rank(delivery_ms: str) -> int:
    """Higher rank means a worse latency bucket; unknown is -1."""
    m = P99_RE.search(delivery_ms)
    if not m:
        return -1
    label = m.group(1)
    if label.startswith("<="):
        return int(label[2:])
    if label.startswith(">"):
        return int(label[1:]) + 1
    return -1


def generator_summary(paths: list[Path], warmup_s: float = 60) -> dict:
    instances = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        parsed = [row for line in text.splitlines() if (row := parse_line(line))]
        summaries = [r for r in parsed if r["kind"] == "summary"]
        sensors = re.findall(r"\[Sensors\] Sent (\d+) messages, (\d+) points in ([0-9.]+)s", text)
        final = summaries[-1] if summaries else None
        elapsed, steady = 0.0, []
        for row in (r for r in parsed if r["kind"] == "report"):
            begin = elapsed
            elapsed += float(row["window_s"])
            if begin >= warmup_s:
                steady.append(row)
        instances.append({
            "log_file": str(path), "complete": bool(final and sensors),
            "total_points": int(sensors[-1][1]) if sensors else None,
            "failed": int(final["failed"]) if final else None,
            "queued_final": int(final["queued"]) if final else None,
            "delivered": int(final["delivered"]) if final else None,
            "behind_max_s": float(final["behind_max_s"]) if final else None,
            "delivery_ms": final["delivery_ms"] if final else None,
            "steady_rate": (sum(float(r["sent_pts_s"]) * float(r["window_s"]) for r in steady)
                            / sum(float(r["window_s"]) for r in steady)) if steady else None,
        })
    complete = bool(instances) and all(i["complete"] for i in instances)
    return {"complete": complete, "instances": instances,
            "total_points": sum(i["total_points"] for i in instances) if complete else None,
            "total_failed": sum(i["failed"] for i in instances) if complete else None,
            "queued_final": sum(i["queued_final"] for i in instances) if complete else None,
            "steady_total_pts_s": sum(i["steady_rate"] for i in instances)
                if instances and all(i["steady_rate"] is not None for i in instances) else None}


def summarize(paths: list[Path], csv_out: Path | None):
    rows: list[dict] = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            parsed = parse_line(line)
            if parsed:
                parsed["log_file"] = str(path)
                rows.append(parsed)

    reports = [r for r in rows if r["kind"] == "report"]
    summaries = [r for r in rows if r["kind"] == "summary"]

    if csv_out:
        with csv_out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "log_file",
                    "kind",
                    "window_s",
                    "sent_pts_s",
                    "sent_msg_s",
                    "behind_max_s",
                    "queued",
                    "delivered",
                    "failed",
                    "delivery_ms",
                ],
            )
            w.writeheader()
            for r in rows:
                w.writerow({k: r[k] for k in w.fieldnames})

    # Diagnostic only: reports are grouped by process order, not aligned UTC time.
    print("# Approximate report indices; use final summaries for delivery totals.")
    # Group reports by approximate window index using cumulative order per file.
    by_window: dict[int, list[dict]] = {}
    for path in paths:
        file_reports = [r for r in reports if r["log_file"] == str(path)]
        for i, r in enumerate(file_reports):
            by_window.setdefault(i, []).append(r)

    print("window_index,instances,total_sent_pts_s,max_behind_max_s,total_failed")
    for idx in sorted(by_window):
        group = by_window[idx]
        total_pts = sum(float(r["sent_pts_s"]) for r in group)
        max_behind = max(float(r["behind_max_s"]) for r in group)
        total_failed = sum(int(r["failed"]) for r in group)
        print(f"{idx},{len(group)},{total_pts:.0f},{max_behind:.3f},{total_failed}")

    if summaries:
        max_behind = max(float(r["behind_max_s"]) for r in summaries)
        total_failed = sum(int(r["failed"]) for r in summaries)
        worst_p99 = max((p99_rank(r["delivery_ms"]), r["delivery_ms"]) for r in summaries)
        print(
            f"\n[Aggregate Summary] instances={len(summaries)} "
            f"max_behind_max_s={max_behind:.3f} total_failed={total_failed} "
            f"worst_delivery_p99={worst_p99[1]}"
        )


def _self_test():
    synthetic = (
        "[Report] window_s=10.0 sent_pts_s=4200 sent_msg_s=380 behind_max_s=0.012 "
        "queued=3 delivered=3800 failed=0 delivery_ms=p50<=10,p99<=50,max=120,n=3800\n"
        "[Summary] window_s=60.0 sent_pts_s=4000 sent_msg_s=360 behind_max_s=0.050 "
        "queued=0 delivered=21600 failed=1 delivery_ms=p50<=20,p99<=200,max=500,n=21600\n"
    )
    for line in synthetic.strip().splitlines():
        p = parse_line(line)
        assert p and p["kind"] in ("report", "summary")
    print("summarize_generators: self-test ok", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="*", type=Path, help="generator log files")
    parser.add_argument("--csv", type=Path, help="write all parsed rows to CSV")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--json", type=Path, help="write aggregate generator delivery status")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return
    if not args.logs:
        parser.error("provide log paths or --self-test")
    summarize(args.logs, args.csv)
    result = generator_summary(args.logs)
    if args.json:
        args.json.write_text(json.dumps(result, indent=2) + "\n")
    return int(not result["complete"] or bool(result["total_failed"]) or bool(result["queued_final"]))


if __name__ == "__main__":
    sys.exit(main())
