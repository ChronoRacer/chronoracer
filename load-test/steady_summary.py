#!/usr/bin/env python3
"""Compact per-run summary: steady-state windows (start+60s .. end) of Core report, generators, CPU, Influx count."""
import csv
import json
import re
import statistics
import sys
from datetime import datetime, timedelta
from pathlib import Path

run = Path(sys.argv[1])
meta = json.loads((run / "run-metadata.json").read_text())
p = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
start, end = p(meta["start_utc"]), p(meta["end_utc"])
ss = start + timedelta(seconds=60)
art = run / "artifacts"


def hist(v):
    d = {}
    for part in v.split(","):
        if "<=" in part:
            k, b = part.split("<=")
            d[k] = float(b)
        elif "=" in part:
            k, b = part.split("=")
            d[k] = float(b)
    return d


rows = []
for line in (art / "logs/pipeline-report.log").read_text().splitlines():
    ts, _, msg = line.partition("\t")
    try:
        t = p(ts.strip())
    except ValueError:
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", msg.split("Pipeline report:", 1)[-1]))
    rows.append((t, kv))

steady = [kv for t, kv in rows if ss <= t <= end]
after = [kv for t, kv in rows if t > end + timedelta(seconds=5)]
num = lambda kv, k: float(kv[k]) if kv.get(k, "unknown") not in ("unknown",) else None


def col(rs, k):
    return [x for x in (num(kv, k) for kv in rs) if x is not None]


def hcol(rs, k, f):
    out = []
    for kv in rs:
        h = hist(kv.get(k, "n=0"))
        if h.get("n", 0) > 0 and f in h:
            out.append(h[f])
    return out


def med(xs):
    return statistics.median(xs) if xs else None


def mx(xs):
    return max(xs) if xs else None


out = {
    "id": run.name,
    "rate": meta["total_rate"],
    "dur": meta["duration_s"],
    "inst": meta["instances"],
    "start": meta["start_utc"],
    "end": meta["end_utc"],
    "core_windows": len(steady),
}
u = col(steady, "unconsumed")
out.update(
    {
        "decoded_med": med(col(steady, "decoded_pts_s")),
        "decoded_min": min(col(steady, "decoded_pts_s"), default=None),
        "written_med": med(col(steady, "written_lines_s")),
        "consumed_msg_med": med(col(steady, "consumed_msg_s")),
        "sink_blocked_med": med(col(steady, "sink_blocked_pct")),
        "sink_blocked_max": mx(col(steady, "sink_blocked_pct")),
        "unconsumed_first_last_max": (u[0] if u else None, u[-1] if u else None, mx(u)),
        "preview_p50_med": med(hcol(steady, "preview_ms", "p50")),
        "preview_p99_max": mx(hcol(steady, "preview_ms", "p99")),
        "preview_mean_med": med(hcol(steady, "preview_ms", "mean")),
        "preview_max": mx(hcol(steady, "preview_ms", "max")),
        "write_p99_max": mx(hcol(steady, "write_ms", "p99")),
        "write_mean_med": med(hcol(steady, "write_ms", "mean")),
        "persist_p99_max": mx(hcol(steady, "persist_ms", "p99")),
        "persist_max": mx(hcol(steady, "persist_ms", "max")),
        "post_run_unconsumed_max": mx(col(after, "unconsumed")),
    }
)

# generators: sum per window index after the first 6 windows (60 s)
gens = {}
gtot = 0
for g in sorted(run.glob("gen-*.log")):
    reps = [l for l in g.read_text().splitlines() if l.startswith("[Report]")]
    gens[g.name] = [dict(re.findall(r"(\w+)=(\S+)", l)) for l in reps]
    m = re.findall(r" (\d+) points in", g.read_text())
    gtot += int(m[-1]) if m else 0
n = min((len(v) for v in gens.values()), default=0)
sums, behind, failed, qmax, dlv = [], [], 0, 0, []
for i in range(6, n):
    sums.append(sum(float(v[i]["sent_pts_s"]) for v in gens.values()))
    behind.append(max(float(v[i]["behind_max_s"]) for v in gens.values()))
    qmax = max(qmax, max(int(v[i]["queued"]) for v in gens.values()))
for v in gens.values():
    for r in v:
        failed += int(r["failed"])
        dlv.append(hist(r["delivery_ms"]).get("p99", 0))
out.update(
    {
        "gen_total_pts": gtot,
        "gen_sum_pts_s_min": min(sums, default=None),
        "gen_sum_pts_s_med": med(sums),
        "gen_behind_max": mx(behind),
        "gen_failed": failed,
        "gen_queued_max": qmax,
        "gen_delivery_p99_max": mx(dlv),
    }
)


def cpu(name):
    f = art / "metrics" / f"{name}_cpu.csv"
    if not f.exists():
        return None
    vals = []
    for r in csv.DictReader(f.open()):
        t = p(r["end_time"])
        if ss <= t <= end + timedelta(seconds=60):
            vals.append(float(r["value"]))
    return (round(med(vals) * 100, 1), round(mx(vals) * 100, 1)) if vals else None


out["core_cpu_med_max_pct"] = cpu("core")
out["kafka_cpu_med_max_pct"] = cpu("kafka")

ic = art / "influx-count.csv"
tot = None
if ic.exists():
    for l in ic.read_text().splitlines():
        if l.startswith("TOTAL,"):
            tot = int(l.split(",")[1] or 0)
out["influx_total"] = tot
out["influx_match"] = tot == gtot if tot is not None else None

warn = art / "logs/core-warn-error.log"
out["core_warn_err"] = len(warn.read_text().splitlines()) if warn.exists() else None
ol = art / "logs/offset-lag.log"
out["stuck_partition_warns"] = ol.read_text().count("Commit position has not advanced") if ol.exists() else None
tl = art / "logs/telemetry.log"
out["telemetry_gap_lines"] = tl.read_text().count("telemetry_gap") if tl.exists() else None

txt = json.dumps(out, indent=1, default=str)
(art / "steady-summary.json").write_text(txt + "\n")
print(txt)
