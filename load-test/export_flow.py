#!/usr/bin/env python3
"""Send SHUTDOWN after load/fault tests, wait for exports and save evidence.

Uses the existing generator's OAuth producer settings. This ends the race.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(args):
    return subprocess.check_output(args, text=True, timeout=120)


def export_checks(log, objects, config, race_id):
    db = f"gs://{config['bucket']}/{race_id}/{config['db_path'].strip('/')}/"
    report = re.search(r"Database export finished\. Exported (\d+) out of (\d+) tables", log)
    files = re.search(r"Database export complete with (\d+) files", log)
    uploaded = re.findall(r"Uploaded file: (.+)", log)
    expected = set()
    for line in uploaded:
        # The storage client logs its object path, not necessarily a gs:// URI.
        match = re.search(r"([^/\s]+\.parquet)\s*$", line)
        if match:
            expected.add(match[1])
    actual = {uri.rsplit('/', 1)[-1] for uri in objects if uri.startswith(db)}
    return {
        "sequence_complete": "Shutdown sequence complete" in log,
        "database_tables_complete": bool(report and int(report[1]) == int(report[2]) and int(report[2]) > 0),
        "database_objects_complete": bool(files and expected and len(expected) == int(files[1]) and expected <= actual),
        "kafka_export_complete": "Kafka export complete" in log,
        # Empty configured topics can produce no files; require exporter success.
        "pubsub_published": "Published shutdown message, id=" in log,
        "no_export_errors": not any(text in log for text in (
            "Error exporting", "Error uploading file", "Failed to send shutdown notification",
            "Database export failed", "Shutdown notification not sent")),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path, help="final completed run directory; export evidence goes in artifacts/export")
    parser.add_argument("--project", default=os.environ.get("PROJECT", "chronoracer"))
    parser.add_argument("--zone", default=os.environ.get("ZONE", "europe-west1-b"))
    parser.add_argument("--timeout-s", type=int, default=3600, help="operator wait budget, not a Core timeout override")
    args = parser.parse_args()
    meta = json.loads((args.run / "run-metadata.json").read_text())
    race_id = os.environ.get("RACE_ID")
    if not race_id or race_id != meta.get("race_id") or not meta.get("end_utc"):
        parser.error("RACE_ID must match a completed run")
    if args.timeout_s <= 0:
        parser.error("--timeout-s must be positive")
    vm = json.loads(command(["gcloud", "compute", "instances", "describe", "core",
                            f"--project={args.project}", f"--zone={args.zone}", "--format=json"]))
    if vm.get("labels", {}).get("race-id") != race_id:
        parser.error("Core VM belongs to another race")

    def ssh(remote):
        return command(["gcloud", "compute", "ssh", "core", f"--project={args.project}",
                        f"--zone={args.zone}", "--tunnel-through-iap", "--command", remote])

    # Read only export settings, never copy the merged config's DB password.
    expression = '{"bucket": .gcp.export_bucket, "db_path": .persistence.export.nested_path_in_bucket, "kafka_path": .kafka.export.nested_path_in_bucket, "topics": .kafka.export.export_topics, "control_topic": .kafka.control_event_topic.name, "race_id": .race.race_id}'
    config = json.loads(ssh("sudo yq -o=json " + shlex.quote(expression) + " /chronoracer/core-config.yaml"))
    if config["race_id"] != race_id:
        parser.error("deployed Core configuration belongs to another race")
    out = args.run / "artifacts/export"
    out.mkdir(parents=True, exist_ok=False)
    (out / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    # Follow only this invocation's journal entries, excluding earlier failures.
    cursor_log = ssh("sudo journalctl -u chronoracer-core -n 0 --show-cursor --no-pager")
    cursor = re.search(r"-- cursor: (.+)", cursor_log)
    if not cursor:
        raise RuntimeError("cannot capture Core journal cursor")

    sys.path.insert(0, str(ROOT / "curls/load_test"))
    from load_test import Producer, build_producer_config
    producer = Producer(build_producer_config(meta["generator_config"]))
    delivery = []
    started = datetime.now(timezone.utc).isoformat()
    producer.produce(topic=config["control_topic"], key=b"SHUTDOWN", value=b"",
                     on_delivery=lambda error, message: delivery.append(None if error is None else str(error)))
    remaining = producer.flush(30)
    (out / "shutdown.json").write_text(json.dumps({"sent_utc": started, "remaining": remaining,
                                                 "delivery": delivery}, indent=2) + "\n")
    if remaining or delivery != [None]:
        raise RuntimeError("SHUTDOWN delivery not confirmed; inspect shutdown.json before retrying")
    print("SHUTDOWN delivered; waiting for export completion", flush=True)
    deadline = time.monotonic() + args.timeout_s
    log = ""
    while time.monotonic() < deadline:
        log = ssh("sudo journalctl -u chronoracer-core --after-cursor=" + shlex.quote(cursor[1]) + " --no-pager -o cat")
        (out / "core.log").write_text(log)
        if "Shutdown sequence complete" in log:
            break
        print("export still running; journal saved", flush=True)
        time.sleep(10)
    prefix = f"gs://{config['bucket']}/{race_id}/"
    objects = command(["gcloud", "storage", "ls", "--recursive", prefix, f"--project={args.project}"])
    (out / "objects.txt").write_text(objects)
    uris = [line.strip() for line in objects.splitlines() if line.strip().endswith(".parquet")]
    checks = export_checks(log, uris, config, race_id)
    result = {"start": started, "end": datetime.now(timezone.utc).isoformat(), "checks": checks,
              "configured_kafka_topics": config["topics"], "objects": uris,
              "scope": "export completion, uploads and Pub/Sub publish; verify Parquet contents separately"}
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return int(not all(checks.values()))


if __name__ == "__main__":
    raise SystemExit(main())
