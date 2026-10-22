"""Check every JDM rule from the bucket against the production evaluation path.

Rules and sensors.json are downloaded into rules/test/files/ for the run and the
folder is removed afterwards. Every input is built the way RulesEngine builds it
(Pathway window -> _get_signals -> GoRulesSchema), so a passing rule behaves the
same in production.

    python -m analysis.rules.test.rule_test
    python -m analysis.rules.test.rule_test --rules-dir /path/to/rules --sensors /path/to/sensors.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import zen
from loguru import logger
from pydantic import ValidationError

from analysis.rules.rules import GoRulesSchema, RulesEngine

BUCKET = "chronoracer-artifacts"
RULES_PREFIX = "analysis/rules/"
SENSORS_BLOB = "analysis/sensors.json"
FILES_DIR = Path(__file__).with_name("files")

ALERT_STATES = ("YELLOW", "RED", "BLACK")
EPS = 0.01
SAMPLE_PERIOD_NS = 20_000_000
EVAL_TS_MS = 1_800_000_000_000
RANDOM_BUDGET = 4000
SEED = 1234


@dataclass
class RuleReport:
    name: str
    sensors: list[str] = field(default_factory=list)
    rows_total: int = 0
    rows_hit: set[str] = field(default_factory=set)
    states: dict[str, int] = field(default_factory=dict)
    evaluations: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def download(destination: Path) -> tuple[Path, Path]:
    from google.cloud import storage

    rules_dir = destination / "rules"
    rules_dir.mkdir(parents=True, exist_ok=True)
    client = storage.Client("chronoracer")
    bucket = client.bucket(BUCKET)
    count = 0
    for blob in client.list_blobs(bucket, prefix=RULES_PREFIX):
        if blob.name.endswith(".json"):
            blob.download_to_filename(str(rules_dir / blob.name.split("/")[-1]))
            count += 1
    if count == 0:
        raise FileNotFoundError(f"No rules in gs://{BUCKET}/{RULES_PREFIX}")
    sensors_path = destination / "sensors.json"
    bucket.blob(SENSORS_BLOB).download_to_filename(str(sensors_path))
    logger.info("Downloaded {} rules and sensors.json into {}", count, destination)
    return rules_dir, sensors_path


def _nodes(rule: dict, node_type: str) -> list[dict]:
    return [node for node in rule.get("nodes", []) if node.get("type") == node_type]


def _referenced_sensors(rule: dict) -> list[str]:
    found: list[str] = []
    for node in _nodes(rule, "functionNode"):
        for name in re.findall(r"signals\??\.(\w+)", node["content"].get("source", "")):
            if name not in found:
                found.append(name)
    return found


def _emitted_states(rule: dict) -> set[str]:
    states: set[str] = set()
    for node in _nodes(rule, "functionNode"):
        states.update(re.findall(r"state:\s*\"([A-Z_]+)\"", node["content"].get("source", "")))
    return states


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _read_inputs(rule: dict) -> set[str]:
    keys: set[str] = set()
    for node in _nodes(rule, "functionNode"):
        keys.update(re.findall(r"\binput\.(\w+)", node["content"].get("source", "")))
    return keys - {"eval_ts_ms", "signals"}


def _signals(sensors: list[str], values: dict[str, float], samples: int) -> dict[str, Any]:
    message = [
        {
            "sensor_id": sensor,
            "vals": [
                (EVAL_TS_MS * 1_000_000 - (samples - i) * SAMPLE_PERIOD_NS, values[sensor])
                for i in range(samples)
            ],
        }
        for sensor in sensors
    ]
    signals = RulesEngine._get_signals(message, set(sensors))
    return GoRulesSchema(eval_ts_ms=EVAL_TS_MS, signals=signals).model_dump()


class RuleChecker:
    def __init__(self, name: str, rule: dict, sensor_bounds: dict[str, tuple[float, float]]):
        self.name = name
        self.rule = rule
        self.sensor_bounds = sensor_bounds
        self.engine = zen.ZenEngine({"loader": lambda key: rule})
        self.report = RuleReport(name=name)
        self.seen_errors: set[str] = set()
        tables = _nodes(rule, "decisionTableNode")
        self.table = tables[0]["content"] if tables else None
        self.function_ids = {node["id"] for node in _nodes(rule, "functionNode")}

    def evaluate(self, payload: dict) -> tuple[dict, str | None]:
        response = self.engine.evaluate(self.name, payload, {"trace": True})
        row_id = None
        for trace in response.get("trace", {}).values():
            matched = (trace.get("traceData") or {}).get("rule") if isinstance(trace.get("traceData"), dict) else None
            if matched:
                row_id = matched.get("_id")
        self.report.evaluations += 1
        return response["result"], row_id

    def parameters(self, sensors: list[str]) -> dict[str, Any]:
        payload = _signals(sensors, {sensor: 0.0 for sensor in sensors}, 1)
        response = self.engine.evaluate(self.name, payload, {"trace": True})
        for node_id, trace in response.get("trace", {}).items():
            if node_id in self.function_ids:
                return {k: v for k, v in trace["input"].items() if k not in ("eval_ts_ms", "signals")}
        return {}

    def candidates(
        self, sensor: str, params: dict[str, Any], global_points: list[float]
    ) -> tuple[float, list[float]]:
        points: set[float] = set()
        for key, value in params.items():
            if sensor in key and _is_number(value):
                points.add(float(value))
        if sensor in self.sensor_bounds:
            points.update(self.sensor_bounds[sensor])
        if not points:
            points = {0.0, 100.0}
        ordered = sorted(points)
        low, high = params.get(f"{sensor}_min"), params.get(f"{sensor}_max")
        base = (low + high) / 2 if _is_number(low) and _is_number(high) else ordered[len(ordered) // 2]
        values = set(ordered) | {base}
        for point in ordered:
            values.update((point - EPS, point + EPS, point - 1, point + 1))
        for a, b in zip(ordered, ordered[1:]):
            values.add((a + b) / 2)
        for point in global_points:
            values.update((point, point - EPS, point + EPS, -point - EPS, -point + EPS))
            for offset in (point - EPS, point + EPS):
                values.update((base + offset, base - offset, base * (1 - offset)))
        return base, sorted(values)

    def static_checks(self, sensors: list[str], schema_sensors: list[str], params: dict[str, Any]) -> None:
        report = self.report
        undefined = sorted(_read_inputs(self.rule) - set(params))
        if undefined:
            report.warnings.append(
                f"function reads input.{{{', '.join(undefined)}}} but no Expression node sets them (undefined at runtime)"
            )
        missing_in_schema = [s for s in sensors if s not in schema_sensors]
        if missing_in_schema:
            report.warnings.append(
                f"function reads {missing_in_schema} but input schema does not require them; "
                "RulesEngine will evaluate this rule even when they are missing"
            )
        for key, value in params.items():
            if key == "sensor_id" or key.endswith("_sensor_id"):
                sensor_id = str(value).strip()
                if sensor_id not in sensors:
                    report.errors.append(f"{key}={sensor_id!r} is not a signal read by the rule {sensors}")
                elif self.sensor_bounds and sensor_id not in self.sensor_bounds:
                    report.errors.append(f"{key}={sensor_id!r} is not in sensors.json")
        for sensor in sensors:
            if self.sensor_bounds and sensor not in self.sensor_bounds:
                report.errors.append(f"signal {sensor!r} is not in sensors.json")
                continue
            rule_min, rule_max = params.get(f"{sensor}_min"), params.get(f"{sensor}_max")
            if sensor in self.sensor_bounds and (rule_min, rule_max) != (None, None):
                bounds = self.sensor_bounds[sensor]
                if (rule_min, rule_max) != bounds:
                    report.warnings.append(
                        f"{sensor} bounds in rule [{rule_min}, {rule_max}] differ from sensors.json {list(bounds)}"
                    )

    def check_result(self, result: dict, row_id: str | None, payload: dict) -> None:
        report = self.report
        state = result.get("state")
        report.states[state] = report.states.get(state, 0) + 1
        if row_id is not None:
            report.rows_hit.add(row_id)
        else:
            self._error_once(f"state {state!r} matched no decision table row", payload)
        for key in ("state", "analysis_message", "analysis_reason", "sensors"):
            if key not in result:
                self._error_once(f"result is missing {key!r} (state {state!r})", payload)
        if state in ALERT_STATES:
            try:
                RulesEngine._prepare_alert_json(self.name, {"result": result})
            except (ValidationError, KeyError, TypeError) as exc:
                details = "; ".join(
                    f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()
                ) if isinstance(exc, ValidationError) else repr(exc)
                self._error_once(f"{state} result breaks NewAlertJSON: {details}", payload)

    def _error_once(self, message: str, payload: dict) -> None:
        if message not in self.seen_errors:
            self.seen_errors.add(message)
            example = json.dumps(payload["signals"])
            self.report.errors.append(f"{message}\n           example signals: {example[:300]}")

    def run(self) -> RuleReport:
        report = self.report
        if self.table is None:
            report.errors.append("no decisionTableNode")
            return report
        try:
            schema_sensors = RulesEngine._get_mandatory_sensors(self.rule)
        except (KeyError, IndexError, json.JSONDecodeError) as exc:
            report.errors.append(f"RulesEngine cannot read input schema: {exc!r}")
            return report
        sensors = list(dict.fromkeys(schema_sensors + _referenced_sensors(self.rule)))
        report.sensors = sensors
        if not sensors:
            report.errors.append("rule reads no signals")
            return report

        rows = self.table.get("rules", [])
        report.rows_total = len(rows)
        row_ids = {row["_id"] for row in rows}

        params = self.parameters(sensors)
        self.static_checks(sensors, schema_sensors, params)

        persistence = max(
            [v for k, v in params.items() if k.endswith("persistence") and isinstance(v, int)] or [1]
        )
        sample_shapes = sorted({persistence, 1, 0}, reverse=True)
        global_points = sorted({
            float(value)
            for key, value in params.items()
            if _is_number(value)
            and not any(sensor in key for sensor in sensors)
            and not key.endswith("persistence")
        })
        base: dict[str, float] = {}
        candidates: dict[str, list[float]] = {}
        for sensor in sensors:
            base[sensor], candidates[sensor] = self.candidates(sensor, params, global_points)

        def attempt(values: dict[str, float], samples: int) -> None:
            payload = _signals(sensors, values, samples)
            result, row_id = self.evaluate(payload)
            self.check_result(result, row_id, payload)

        for samples in sample_shapes:
            for sensor in sensors:
                for value in candidates[sensor]:
                    attempt({**base, sensor: value}, samples)

        combos = 1
        for values in candidates.values():
            combos *= len(values)
        if len(sensors) > 1 and report.rows_hit != row_ids:
            if combos <= RANDOM_BUDGET:
                product = itertools.product(*(candidates[s] for s in sensors))
            else:
                rng = random.Random(SEED)
                product = (tuple(rng.choice(candidates[s]) for s in sensors) for _ in range(RANDOM_BUDGET))
            for combo in product:
                attempt(dict(zip(sensors, combo)), persistence)
                if report.rows_hit == row_ids:
                    break

        for row in rows:
            if row["_id"] not in report.rows_hit:
                cells = {k: v for k, v in row.items() if k != "_id"}
                report.errors.append(f"decision table row never matched: {cells}")
        not_emitted = _emitted_states(self.rule) - set(report.states)
        if not_emitted:
            report.warnings.append(f"function can return {sorted(not_emitted)} but no generated input reached it")
        return report


def load_sensor_bounds(path: Path | None) -> dict[str, tuple[float, float]]:
    if path is None:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {item["name"]: (item["min"], item["max"]) for item in data["sensors"]}


def print_summary(reports: list[RuleReport]) -> None:
    width = max(len(r.name) for r in reports)
    print()
    print(f"{'RULE'.ljust(width)}  RESULT  ROWS    EVALS  STATES")
    for r in reports:
        states = ", ".join(f"{k}:{v}" for k, v in sorted(r.states.items(), key=lambda i: str(i[0])))
        result = "PASS" if r.ok else "FAIL"
        rows = f"{len(r.rows_hit)}/{r.rows_total}"
        print(f"{r.name.ljust(width)}  {result:<6}  {rows:<6}  {r.evaluations:<5}  {states}")
    for r in reports:
        if r.errors or r.warnings:
            print(f"\n{r.name}  sensors={r.sensors}")
            for message in r.errors:
                print(f"  ERROR    {message}")
            for message in r.warnings:
                print(f"  WARNING  {message}")
    failed = sum(not r.ok for r in reports)
    rows_hit = sum(len(r.rows_hit) for r in reports)
    rows_total = sum(r.rows_total for r in reports)
    warnings = sum(len(r.warnings) for r in reports)
    print(
        f"\n{len(reports) - failed}/{len(reports)} rules passed, "
        f"{rows_hit}/{rows_total} decision table rows covered, {warnings} warnings"
    )


def run(rules_dir: Path, sensors_path: Path | None) -> int:
    files = sorted(rules_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No rule files in {rules_dir}")
    bounds = load_sensor_bounds(sensors_path)
    reports = []
    for path in files:
        logger.info("Checking {}", path.name)
        rule = json.loads(path.read_text(encoding="utf-8"))
        reports.append(RuleChecker(path.name, rule, bounds).run())
    print_summary(reports)
    return 0 if all(r.ok for r in reports) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rules-dir", type=Path, help="use local rule files instead of the bucket")
    parser.add_argument("--sensors", type=Path, help="local sensors.json used with --rules-dir")
    parser.add_argument("--keep-files", action="store_true", help=f"do not remove {FILES_DIR} after the run")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    if args.rules_dir:
        return run(args.rules_dir, args.sensors)

    try:
        rules_dir, sensors_path = download(FILES_DIR)
        return run(rules_dir, sensors_path)
    finally:
        if not args.keep_files and FILES_DIR.exists():
            shutil.rmtree(FILES_DIR)
            logger.info("Removed {}", FILES_DIR)


if __name__ == "__main__":
    raise SystemExit(main())
