#!/usr/bin/env python3
"""Parse Core 'Pipeline report:' log lines into CSV and a short summary."""

from __future__ import annotations

import argparse
import csv
import re
import statistics
import sys
from pathlib import Path
from typing import Any

REPORT_PREFIX = "Pipeline report: "
# Optional leading timestamp / severity prefix before the report body.
REPORT_RE = re.compile(
    r"(?:^.*)?Pipeline report:\s+"
    r"window_s=(?P<window_s>[-0-9.]+)"
    r"\s+consumed_msg_s=(?P<consumed_msg_s>\d+)"
    r"\s+decoded_pts_s=(?P<decoded_pts_s>\d+)"
    r"\s+written_lines_s=(?P<written_lines_s>\d+)"
    r"\s+sink_blocked_pct=(?P<sink_blocked_pct>[-0-9.]+)"
    r"\s+unconsumed=(?P<unconsumed>\d+|unknown)"
    r"\s+preview_ms=(?P<preview_ms>[^ ]+)"
    r"\s+write_ms=(?P<write_ms>[^ ]+)"
    r"\s+persist_ms=(?P<persist_ms>.*)$"
)

def parse_histogram(field: str) -> dict[str, Any]:
    if field == "n=0":
        return {"p50_ms": "", "p99_ms": "", "max_ms": 0, "mean_ms": "", "n": 0}
    parts = field.split(",")
    tail = {}
    head: list[str] = []
    for part in parts:
        if part.startswith(("max=", "mean=", "n=")):
            key, val = part.split("=", 1)
            tail[key] = val
        else:
            head.append(part)
    if "n" not in tail:
        raise ValueError(f"unrecognized histogram: {field!r}")
    p50_ms = ""
    p99_ms = ""
    for part in head:
        if part.startswith("p50<="):
            p50_ms = part.removeprefix("p50<=")
        elif part.startswith("p50>"):
            p50_ms = ">" + part.removeprefix("p50>")
        elif part.startswith("p99<="):
            p99_ms = part.removeprefix("p99<=")
        elif part.startswith("p99>"):
            p99_ms = ">" + part.removeprefix("p99>")
        else:
            raise ValueError(f"unrecognized histogram segment: {part!r}")
    return {
        "p50_ms": p50_ms,
        "p99_ms": p99_ms,
        "max_ms": int(tail.get("max", "0")),
        "mean_ms": int(tail.get("mean", "0")),
        "n": int(tail["n"]),
    }


def parse_line(line: str) -> dict[str, Any]:
    line = line.strip()
    if not line or REPORT_PREFIX not in line:
        raise ValueError("line does not contain a Pipeline report")
    m = REPORT_RE.match(line)
    if not m:
        raise ValueError(f"could not parse report: {line!r}")
    row: dict[str, Any] = {
        "raw_line": line,
        "window_s": float(m.group("window_s")),
        "consumed_msg_s": int(m.group("consumed_msg_s")),
        "decoded_pts_s": int(m.group("decoded_pts_s")),
        "written_lines_s": int(m.group("written_lines_s")),
        "sink_blocked_pct": float(m.group("sink_blocked_pct")),
        "unconsumed": m.group("unconsumed"),
    }
    for name in ("preview_ms", "write_ms", "persist_ms"):
        hist = parse_histogram(m.group(name))
        for key, val in hist.items():
            row[f"{name}_{key}"] = val
    return row


def summarize(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No pipeline reports parsed."
    rates = ("consumed_msg_s", "decoded_pts_s", "written_lines_s")
    lines = ["Summary ({} windows):".format(len(rows))]
    for key in rates:
        vals = [int(r[key]) for r in rows]
        lines.append(f"  {key}: median={statistics.median(vals):.0f} max={max(vals)}")
    sink = [float(r["sink_blocked_pct"]) for r in rows]
    lines.append(
        f"  sink_blocked_pct: median={statistics.median(sink):.1f} max={max(sink):.1f}"
    )
    numeric_unconsumed = [
        int(r["unconsumed"]) for r in rows if r["unconsumed"] != "unknown"
    ]
    if numeric_unconsumed:
        lines.append(f"  unconsumed: max={max(numeric_unconsumed)}")
    else:
        lines.append("  unconsumed: (all unknown)")
    for hist in ("preview_ms", "write_ms", "persist_ms"):
        p99s = [
            int(r[f"{hist}_p99_ms"])
            for r in rows
            if r.get(f"{hist}_p99_ms") not in (None, "",)
            and str(r[f"{hist}_p99_ms"]).isdigit()
        ]
        if p99s:
            lines.append(f"  worst {hist} p99 bound (ms): {max(p99s)}")
    return "\n".join(lines)


def fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key != "raw_line" and key not in keys:
                keys.append(key)
    return keys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        help="file with one log line per report (default: stdin)",
    )
    parser.add_argument("-o", "--csv", type=Path, help="write CSV here")
    args = parser.parse_args()

    text = args.input.read_text(encoding="utf-8") if args.input else sys.stdin.read()
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if REPORT_PREFIX not in line:
            continue
        try:
            rows.append(parse_line(line))
        except ValueError as exc:
            errors.append(f"line {lineno}: {exc}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1

    if args.csv:
        cols = fieldnames(rows)
        with args.csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    print(summarize(rows))
    return 0


def _self_test() -> None:
    # Built from Display formats in core/src/pipeline_stats.rs
    samples = [
        (
            "2026-10-21T10:00:00Z INFO Pipeline report: "
            "window_s=10.0 consumed_msg_s=1500 decoded_pts_s=15000 written_lines_s=15000 "
            "sink_blocked_pct=2.5 unconsumed=42 "
            "preview_ms=p50<=5,p99<=20,max=18,mean=6,n=1500 "
            "write_ms=p50<=10,p99<=50,max=120,mean=15,n=30 "
            "persist_ms=p50<=100,p99<=500,max=800,mean=120,n=30"
        ),
        (
            "Pipeline report: window_s=10.0 consumed_msg_s=0 decoded_pts_s=0 written_lines_s=0 "
            "sink_blocked_pct=0.0 unconsumed=unknown "
            "preview_ms=n=0 write_ms=n=0 persist_ms=n=0"
        ),
        (
            "Pipeline report: window_s=10.0 consumed_msg_s=100 decoded_pts_s=100 written_lines_s=100 "
            "sink_blocked_pct=0.0 unconsumed=1 "
            "preview_ms=p50>60000,p99>60000,max=90000,mean=1000,n=10 "
            "write_ms=n=0 persist_ms=n=0"
        ),
    ]
    for sample in samples:
        row = parse_line(sample)
        assert row["decoded_pts_s"] == int(
            re.search(r"decoded_pts_s=(\d+)", sample).group(1)  # type: ignore[union-attr]
        )
    empty_hist = parse_histogram("n=0")
    assert empty_hist["n"] == 0
    overflow = parse_histogram("p50>60000,p99>60000,max=90000,mean=1000,n=10")
    assert overflow["p50_ms"] == ">60000"
    print("parse_pipeline_report.py: self-test ok")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--self-test":
        _self_test()
        sys.exit(0)
    sys.exit(main())
