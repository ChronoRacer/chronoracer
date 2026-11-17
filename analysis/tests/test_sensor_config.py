import pytest

from analysis.rules.build_config import (
    load_required_sensors,
    prompt_mapping,
    sensor_config_is_populated,
)
from analysis.shared.sensor_map import (
    invert_map,
    load_physical_to_business_with_retry,
    load_sensor_config,
    to_business_id,
)


class FakeBlob:
    def __init__(self, text="", *, exists=True):
        self.text = text
        self._exists = exists

    def exists(self):
        return self._exists

    def download_as_text(self):
        return self.text


class FakeBucket:
    def __init__(self, blob):
        self._blob = blob

    def blob(self, name):
        assert name == "analysis/sensor_config.json"
        return self._blob


class FakeClient:
    def __init__(self, blob):
        self._blob = blob

    def bucket(self, name):
        assert name == "chronoracer-artifacts"
        return FakeBucket(self._blob)


def test_load_sensor_config_trims_mapping_entries():
    client = FakeClient(FakeBlob('{" CAR_SPEED_KMH ": " SPEED_PHYSICAL "}'))

    assert load_sensor_config(client) == {"CAR_SPEED_KMH": "SPEED_PHYSICAL"}


@pytest.mark.parametrize("text", ("[]", "{}", "null"))
def test_load_sensor_config_rejects_non_object_or_empty_mapping(text):
    client = FakeClient(FakeBlob(text))

    with pytest.raises(ValueError):
        load_sensor_config(client)


def test_load_sensor_config_reports_missing_blob():
    client = FakeClient(FakeBlob(exists=False))

    with pytest.raises(FileNotFoundError, match="sensor_config.json"):
        load_sensor_config(client)


def test_load_physical_to_business_retries_until_sensor_config_exists():
    class DelayedBlob(FakeBlob):
        def __init__(self):
            super().__init__('{"CAR_SPEED_KMH": "SPEED_PHYSICAL"}', exists=False)
            self.checks = 0

        def exists(self):
            self.checks += 1
            return self.checks >= 3

    sleeps: list[float] = []
    mapping = load_physical_to_business_with_retry(
        client=FakeClient(DelayedBlob()),
        sleep=sleeps.append,
    )

    assert mapping == {"SPEED_PHYSICAL": "CAR_SPEED_KMH"}
    assert sleeps == [1, 2]


def test_invert_map_rejects_duplicate_physical_ids():
    with pytest.raises(ValueError, match="duplicate physical id"):
        invert_map({"ENGINE_SPEED_RPM": "rpm", "RPM": "rpm"})


def test_to_business_id_returns_none_for_unmapped_physical_sensor():
    assert to_business_id("unknown_sensor", {"physical_speed": "CAR_SPEED_KMH"}) is None


def test_load_required_sensors_validates_and_trims_business_keys():
    blob = FakeBlob('{"RULES_REQUIRED_SENSOR_IDS": [" CAR_SPEED_KMH ", "OIL_TEMP_C"]}')

    assert load_required_sensors(blob) == ["CAR_SPEED_KMH", "OIL_TEMP_C"]


@pytest.mark.parametrize(
    "text",
    (
        "{}",
        '{"RULES_REQUIRED_SENSOR_IDS": []}',
        '{"RULES_REQUIRED_SENSOR_IDS": ["CAR_SPEED_KMH", "  "]}',
    ),
)
def test_load_required_sensors_rejects_invalid_lists(text):
    with pytest.raises(ValueError):
        load_required_sensors(FakeBlob(text))


@pytest.mark.parametrize(
    "text,expected",
    (("", False), ("{}", False), ("null", False), ('{"x":1}', True)),
)
def test_sensor_config_is_populated_handles_empty_and_non_empty_objects(text, expected):
    assert sensor_config_is_populated(FakeBlob(text)) is expected


def test_prompt_mapping_retries_blank_entry_and_returns_mapping(monkeypatch):
    answers = iter(("   ", "physical_speed"))
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert prompt_mapping(["CAR_SPEED_KMH"]) == {"CAR_SPEED_KMH": "physical_speed"}
