import json

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
