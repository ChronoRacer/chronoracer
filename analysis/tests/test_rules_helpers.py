import json
from types import SimpleNamespace

import analysis.rules.rules as rules_module


def test_get_mandatory_sensors_reads_rule_input_schema():
    rule = {
        "nodes": [
            {
                "content": {
                    "schema": json.dumps(
                        {"properties": {"signals": {"properties": {"OIL_TEMP_C": {}, "RPM": {}}}}}
                    )
                }
            }
        ]
    }

    assert rules_module.RulesEngine._get_mandatory_sensors(rule) == ["OIL_TEMP_C", "RPM"]


def test_get_signals_filters_sensors_and_converts_timestamp_to_milliseconds():
    message = [
        {
            "sensor_id": "OIL_TEMP_C",
            "vals": [
                (1_767_225_600_000_000_000, 92.5),
                (1_767_225_601_000_000_000, 93.0),
            ],
        },
        {
            "sensor_id": "IGNORED",
            "vals": [(1_767_225_600_000_000_000, 1.0)],
        },
    ]

    signals = rules_module.RulesEngine._get_signals(message, {"OIL_TEMP_C"})

    assert signals["OIL_TEMP_C"].samples == 2
    assert [item.ts_ms for item in signals["OIL_TEMP_C"].history] == [
        1_767_225_600_000,
        1_767_225_601_000,
    ]
    assert set(signals) == {"OIL_TEMP_C"}


def test_prepare_alert_json_flattens_sensor_values(monkeypatch):
    monkeypatch.setattr(rules_module.time, "time_ns", lambda: 1_767_225_600_000_000_000)
    decision = {
        "result": {
            "state": "RED",
            "sensors": [{"OIL_TEMP_C": 130.5}, {"COOLANT_TEMP_C": 98.0}],
            "analysis_message": "Oil temperature is high",
            "analysis_reason": "Sustained high temperature",
        }
    }

    payload = rules_module.RulesEngine._prepare_alert_json("ENGINE_OIL_TEMPERATURE", decision)

    assert payload == {
        "timestamp": 1_767_225_600_000_000,
        "alert_id": "ENGINE_OIL_TEMPERATURE",
        "sensors": [
            {"sensor_id": "OIL_TEMP_C", "value": 130.5},
            {"sensor_id": "COOLANT_TEMP_C", "value": 98.0},
        ],
        "analysis_message": "Oil temperature is high",
        "analysis_reason": "Sustained high temperature",
    }


def test_gcs_retry_delay_doubles_then_caps_at_max():
    from analysis.shared.gcs_retry import next_gcs_retry_delay

    assert next_gcs_retry_delay(1) == 2
    assert next_gcs_retry_delay(16) == 30
    assert next_gcs_retry_delay(30) == 30


def test_load_rules_with_retry_retries_then_returns(monkeypatch):
    attempts = {"n": 0}
    sleeps = []

    def fake_factory(_project):
        return object()

    def fake_load(_client):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("GCS unavailable")
        return {"ENGINE_OIL_TEMPERATURE": object()}

    monkeypatch.setattr(rules_module, "load_rules_from_gcs", fake_load)

    rules = rules_module.load_rules_with_retry(client_factory=fake_factory, sleep=sleeps.append)

    assert attempts["n"] == 3
    assert sleeps == [1, 2]
    assert "ENGINE_OIL_TEMPERATURE" in rules


def _rule_json():
    return {
        "nodes": [
            {
                "content": {
                    "schema": json.dumps(
                        {"properties": {"signals": {"properties": {"OIL_TEMP_C": {}}}}}
                    )
                }
            }
        ]
    }


def test_load_rules_from_gcs_ignores_non_json_and_builds_sensor_index():
    class Blob:
        def __init__(self, name, contents):
            self.name = name
            self.contents = contents

        def download_as_text(self):
            return self.contents

    class Client:
        def bucket(self, name):
            assert name == "chronoracer-artifacts"
            return "bucket"

        def list_blobs(self, bucket, prefix):
            assert bucket == "bucket"
            assert prefix == "analysis/rules/"
            return [
                Blob("analysis/rules/ENGINE_OIL_TEMPERATURE.json", json.dumps(_rule_json())),
                Blob("analysis/rules/README.md", "not a rule"),
            ]

    rules = rules_module.load_rules_from_gcs(Client())

    assert list(rules) == ["ENGINE_OIL_TEMPERATURE"]
    assert rules["ENGINE_OIL_TEMPERATURE"].sensors == {"OIL_TEMP_C"}


def test_load_rules_from_gcs_rejects_empty_rule_prefix():
    class Client:
        def bucket(self, name):
            return "bucket"

        def list_blobs(self, bucket, prefix):
            return []

    try:
        rules_module.load_rules_from_gcs(Client())
    except RuntimeError as error:
        assert "No rule JSON objects" in str(error)
    else:
        raise AssertionError("empty rule prefixes must fail")


def test_handle_message_evaluates_only_when_all_rule_sensors_are_present(monkeypatch):
    rule = rules_module.RuleObj(rule_json=_rule_json(), sensors={"OIL_TEMP_C"})
    engine = object.__new__(rules_module.RulesEngine)
    engine.rules = {"ENGINE_OIL_TEMPERATURE": rule}
    evaluations = []

    class Engine:
        def evaluate(self, key, payload):
            evaluations.append((key, payload))
            return {"result": {"state": "GREEN"}}

    engine.engine = Engine()
    emitted = []
    engine.send_decision = lambda key, decision: emitted.append((key, decision))
    monkeypatch.setattr(rules_module.time, "time_ns", lambda: 1_767_225_600_000_000_000)

    message = [{"sensor_id": "OIL_TEMP_C", "vals": [(1_767_225_600_000_000_000, 130.0)]}]
    engine.handle_message(message, set())
    assert evaluations == []

    engine.handle_message(message, {"OIL_TEMP_C"})
    assert len(evaluations) == 1
    assert evaluations[0][1]["signals"]["OIL_TEMP_C"]["samples"] == 1
    assert len(emitted) == 1


def test_send_decision_filters_green_and_throttles_repeated_alerts(monkeypatch):
    now = [1_767_225_600_000_000_000]
    monkeypatch.setattr(rules_module.time, "time_ns", lambda: now[0])
    posts = []

    class Response:
        def raise_for_status(self):
            return None

    class Session:
        def __init__(self):
            self.headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def post(self, *args, **kwargs):
            posts.append((args, kwargs, dict(self.headers)))
            return Response()

    monkeypatch.setattr(rules_module.requests, "Session", Session)
    engine = object.__new__(rules_module.RulesEngine)
    engine.config = SimpleNamespace(
        ANALYSIS_INGEST_TOKEN="token",
        TELEMETRY_URL="http://telemetry",
    )
    engine.time_cache = {}
    decision = {
        "result": {
            "state": "RED",
            "sensors": [{"OIL_TEMP_C": 130.0}],
            "analysis_message": "High oil temperature",
            "analysis_reason": "Sustained high readings",
        }
    }

    engine.send_decision("ENGINE_OIL_TEMPERATURE", {"result": {"state": "GREEN"}})
    engine.send_decision("ENGINE_OIL_TEMPERATURE", decision)
    now[0] += 1_000_000_000
    engine.send_decision("ENGINE_OIL_TEMPERATURE", decision)
    assert len(posts) == 1

    now[0] += 2_000_000_000
    engine.send_decision("ENGINE_OIL_TEMPERATURE", decision)
    assert len(posts) == 2
    assert posts[0][2]["Authorization"] == "Bearer token"
