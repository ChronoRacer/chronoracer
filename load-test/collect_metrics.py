#!/usr/bin/env python3
"""Export Cloud Monitoring time series to CSV for a load-test window.

# pip install google-cloud-monitoring
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import monitoring_v3
from google.protobuf import timestamp_pb2


def parse_rfc3339(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_timestamp(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def point_value(val: monitoring_v3.TypedValue) -> str:
    pb = monitoring_v3.TypedValue.pb(val)
    kind = pb.WhichOneof("value")
    if kind == "distribution_value":
        dist = pb.distribution_value
        return f"count={dist.count},mean={dist.mean:.3f}"
    return "" if kind is None else str(getattr(pb, kind))


def instance_ids(project: str, race_id: str) -> dict[str, str]:
    """Map VM name (core, kafka) -> numeric instance_id for the race label."""
    cmd = [
        "gcloud",
        "compute",
        "instances",
        "list",
        f"--project={project}",
        f"--filter=labels.race-id={race_id}",
        "--format=json",
    ]
    raw = subprocess.check_output(cmd, text=True)
    rows = json.loads(raw or "[]")
    out: dict[str, str] = {}
    for row in rows:
        name = row.get("name")
        iid = str(row.get("id", ""))
        if name and iid:
            out[name] = iid
    return out


def write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)


def fetch_gce(
    client: monitoring_v3.MetricServiceClient,
    project: str,
    instance_id: str,
    metric_type: str,
    start: datetime,
    end: datetime,
) -> list[list[str]]:
    name = f"projects/{project}"
    interval = monitoring_v3.TimeInterval(
        {
            "start_time": to_timestamp(start),
            "end_time": to_timestamp(end),
        }
    )
    filt = (
        f'metric.type="{metric_type}" AND resource.type="gce_instance" '
        f'AND resource.labels.instance_id="{instance_id}"'
    )
    rows: list[list[str]] = []
    for series in client.list_time_series(
        request={
            "name": name,
            "filter": filt,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    ):
        metric_labels = ",".join(f"{k}={v}" for k, v in sorted(series.metric.labels.items()))
        for point in series.points:
            end_ts = point.interval.end_time
            v = point_value(point.value)
            rows.append(
                [
                    end_ts.isoformat(),
                    metric_type,
                    metric_labels,
                    v,
                ]
            )
    return rows


def fetch_cloud_run(
    client: monitoring_v3.MetricServiceClient,
    project: str,
    service: str,
    region: str,
    metric_type: str,
    start: datetime,
    end: datetime,
) -> list[list[str]]:
    name = f"projects/{project}"
    interval = monitoring_v3.TimeInterval(
        {
            "start_time": to_timestamp(start),
            "end_time": to_timestamp(end),
        }
    )
    filt = (
        f'metric.type="{metric_type}" AND resource.type="cloud_run_revision" '
        f'AND resource.labels.service_name="{service}" '
        f'AND resource.labels.location="{region}"'
    )
    rows: list[list[str]] = []
    for series in client.list_time_series(
        request={
            "name": name,
            "filter": filt,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    ):
        metric_labels = ",".join(f"{k}={v}" for k, v in sorted(series.metric.labels.items()))
        for point in series.points:
            end_ts = point.interval.end_time
            v = point_value(point.value)
            rows.append([end_ts.isoformat(), metric_type, metric_labels, v])
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="chronoracer")
    parser.add_argument("--race-id", required=True, help="GCE label race-id (disambiguates core/kafka VMs)")
    parser.add_argument("--region", default="europe-west1")
    parser.add_argument("--telemetry-service", default="telemetry")
    parser.add_argument("--start", required=True, help="RFC3339 window start")
    parser.add_argument("--end", required=True, help="RFC3339 window end")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    start = parse_rfc3339(args.start)
    end = parse_rfc3339(args.end)
    if end <= start:
        print("end must be after start", file=sys.stderr)
        return 1

    ids = instance_ids(args.project, args.race_id)
    for vm in ("core", "kafka"):
        if vm not in ids:
            print(f"missing GCE instance for race-id={args.race_id} name={vm}", file=sys.stderr)
            return 1

    client = monitoring_v3.MetricServiceClient()
    headers = ["end_time", "metric_type", "metric_labels", "value"]

    gce_metrics = [
        ("core_cpu", "core", "compute.googleapis.com/instance/cpu/utilization"),
        ("core_memory", "core", "agent.googleapis.com/memory/percent_used"),
        ("kafka_cpu", "kafka", "compute.googleapis.com/instance/cpu/utilization"),
        ("kafka_memory", "kafka", "agent.googleapis.com/memory/percent_used"),
        ("kafka_disk", "kafka", "agent.googleapis.com/disk/percent_used"),
        ("kafka_net", "kafka", "agent.googleapis.com/interface/traffic"),
    ]
    for stem, vm, mtype in gce_metrics:
        rows = fetch_gce(client, args.project, ids[vm], mtype, start, end)
        write_csv(args.output_dir / f"{stem}.csv", headers, rows)

    influx_metrics = [
        "custom.googleapis.com/chronoracer/influx_write_lines",
        "custom.googleapis.com/chronoracer/influx_write_bytes",
        "custom.googleapis.com/chronoracer/influx_write_requests",
    ]
    for mtype in influx_metrics:
        stem = mtype.rsplit("/", 1)[-1]
        rows = fetch_gce(client, args.project, ids["core"], mtype, start, end)
        write_csv(args.output_dir / f"{stem}.csv", headers, rows)

    run_metrics = [
        "run.googleapis.com/container/instance_count",
        "run.googleapis.com/request_count",
        "run.googleapis.com/request_latencies",
    ]
    for mtype in run_metrics:
        stem = mtype.rsplit("/", 1)[-1]
        rows = fetch_cloud_run(
            client,
            args.project,
            args.telemetry_service,
            args.region,
            mtype,
            start,
            end,
        )
        write_csv(args.output_dir / f"telemetry_{stem}.csv", headers, rows)

    meta = {
        "project": args.project,
        "race_id": args.race_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "instance_ids": ids,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
