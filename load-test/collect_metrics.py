#!/usr/bin/env python3
"""Export raw Cloud Monitoring series, preserving counter epochs and distributions."""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

CORE_METRICS = (
    "influx_write_lines", "influx_write_bytes", "influx_write_requests",
    "pipeline_consumed_messages", "pipeline_sink_blocked_ms",
    "pipeline_unconsumed_messages", "pipeline_persist_latency_ms",
)
HEADERS = ["start_time", "end_time", "metric_type", "metric_kind", "value_type",
           "resource_type", "resource_labels", "metric_labels", "value"]


def parse_rfc3339(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return dt.astimezone(timezone.utc)


def instance_ids(project: str, race_id: str) -> dict[str, str]:
    rows = json.loads(subprocess.check_output([
        "gcloud", "compute", "instances", "list", f"--project={project}",
        f"--filter=labels.race-id={race_id}", "--format=json",
    ], text=True))
    result = {}
    for vm in ("core", "kafka"):
        matches = [row for row in rows if row.get("name") == vm]
        if len(matches) != 1:
            raise ValueError(f"expected one {vm} VM for race-id={race_id}, got {len(matches)}")
        result[vm] = str(matches[0]["id"])
    return result


def fetch(client, project, metric_type, resource_type, labels, start, end):
    from google.cloud import monitoring_v3
    from google.protobuf.json_format import MessageToDict
    from google.api.metric_pb2 import MetricDescriptor

    filt = [f'metric.type="{metric_type}"', f'resource.type="{resource_type}"']
    filt.extend(f'resource.labels.{key}="{value}"' for key, value in labels.items())
    rows = []
    for series in client.list_time_series(request={
        "name": f"projects/{project}", "filter": " AND ".join(filt),
        "interval": {"start_time": start, "end_time": end},
        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    }):
        for point in series.points:
            pb = monitoring_v3.TypedValue.pb(point.value)
            kind = pb.WhichOneof("value")
            value = getattr(pb, kind) if kind else None
            if kind == "distribution_value":
                # Keep bucket boundaries/counts and moments; count+mean alone loses p99.
                value = json.dumps(MessageToDict(value, preserving_proto_field_name=True), sort_keys=True)
            rows.append([
                point.interval.start_time.isoformat() if point.interval.start_time else "",
                point.interval.end_time.isoformat(), metric_type,
                MetricDescriptor.MetricKind.Name(series.metric_kind),
                MetricDescriptor.ValueType.Name(series.value_type), series.resource.type,
                json.dumps(dict(series.resource.labels), sort_keys=True),
                json.dumps(dict(series.metric.labels), sort_keys=True), value,
            ])
    return sorted(rows, key=lambda row: (row[1], row[6], row[7]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="chronoracer")
    parser.add_argument("--race-id", required=True)
    parser.add_argument("--region", default="europe-west1")
    parser.add_argument("--telemetry-service", default="telemetry")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    start, end = parse_rfc3339(args.start), parse_rfc3339(args.end)
    if end <= start or end > datetime.now(timezone.utc):
        parser.error("require start < end <= now; wait for the padded window before collecting")

    from google.cloud import monitoring_v3
    from google.api_core.exceptions import GoogleAPICallError

    ids = instance_ids(args.project, args.race_id)
    client = monitoring_v3.MetricServiceClient()
    specs = []
    for vm in ("core", "kafka"):
        for short, metric in (
            ("cpu", "compute.googleapis.com/instance/cpu/utilization"),
            ("memory", "agent.googleapis.com/memory/percent_used"),
            ("disk", "agent.googleapis.com/disk/percent_used"),
            ("net", "agent.googleapis.com/interface/traffic"),
        ):
            specs.append((f"{vm}_{short}", metric, "gce_instance", {"instance_id": ids[vm]}))
    for metric in CORE_METRICS:
        specs.append((metric, f"custom.googleapis.com/chronoracer/{metric}",
                      "gce_instance", {"instance_id": ids["core"]}))
    for metric in ("container/instance_count", "request_count", "request_latencies"):
        specs.append(("telemetry_" + metric.rsplit("/", 1)[-1], "run.googleapis.com/" + metric,
                      "cloud_run_revision", {"service_name": args.telemetry_service, "location": args.region}))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    status = {}
    for stem, metric, resource, labels in specs:
        try:
            rows = fetch(client, args.project, metric, resource, labels, start, end)
            status[stem] = {"status": "ok" if rows else "missing", "points": len(rows)}
        except GoogleAPICallError as exc:
            rows = []
            status[stem] = {"status": "error", "error": str(exc), "points": 0}
        with (args.output_dir / f"{stem}.csv").open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(HEADERS)
            writer.writerows(rows)
    meta = {"project": args.project, "race_id": args.race_id,
            "start": start.isoformat(), "end": end.isoformat(), "instance_ids": ids,
            "metrics": status}
    (args.output_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    # Idle latency and Telemetry series may legitimately be absent. Core scalar
    # series must exist; missing host/Telemetry series remain visible in meta.json.
    required = [metric for metric in CORE_METRICS if metric != "pipeline_persist_latency_ms"]
    missing = [metric for metric in required if status[metric]["status"] != "ok"]
    print(json.dumps(status, indent=2))
    if missing:
        print("Missing required Core metrics: " + ", ".join(missing))
    return int(bool(missing) or any(s["status"] == "error" for s in status.values()))


if __name__ == "__main__":
    raise SystemExit(main())
