import pytest
from pydantic import ValidationError

from analysis.shared.contract import (
    NewAlertJSON,
    NewPredictionJSON,
    PredictionRunningJSON,
    PredictionUnavailableJSON,
)


MIN_TIMESTAMP_US = 1_767_225_600_000_000


def test_prediction_status_contracts_accept_optional_refresh_rate():
    running = PredictionRunningJSON(
        timestamp=MIN_TIMESTAMP_US,
        prediction_id="fuel_prediction",
        name="Fuel prediction",
        refresh_rate=None,
    )
    unavailable = PredictionUnavailableJSON(
        timestamp=MIN_TIMESTAMP_US,
        prediction_id="fuel_prediction",
        info="Missing required sensors",
    )

    assert running.model_dump(mode="json")["refresh_rate"] is None
    assert unavailable.model_dump(mode="json")["prediction_id"] == "fuel_prediction"


def test_new_prediction_contract_keeps_nullable_result_fields():
    prediction = NewPredictionJSON(
        timestamp=MIN_TIMESTAMP_US,
        prediction_id="lap_time_prediction",
        sensors_id=["CAR_SPEED_KMH"],
        instruction=None,
        value=None,
    )

    assert prediction.model_dump(mode="json")["value"] is None
    assert prediction.model_dump(mode="json")["instruction"] is None


def test_alert_contract_rejects_empty_sensor_list_and_non_finite_readings():
    with pytest.raises(ValidationError):
        NewAlertJSON(
            timestamp=MIN_TIMESTAMP_US,
            alert_id="ENGINE_OIL_TEMPERATURE",
            sensors=[],
            analysis_message="High oil temperature",
            analysis_reason="Sustained high readings",
        )

    with pytest.raises(ValidationError):
        NewAlertJSON(
            timestamp=MIN_TIMESTAMP_US,
            alert_id="ENGINE_OIL_TEMPERATURE",
            sensors=[{"sensor_id": "OIL_TEMP_C", "value": float("inf")}],
            analysis_message="High oil temperature",
            analysis_reason="Sustained high readings",
        )


def test_timestamp_contract_rejects_values_before_2026_utc():
    with pytest.raises(ValidationError):
        PredictionUnavailableJSON(
            timestamp=MIN_TIMESTAMP_US - 1,
            prediction_id="fuel_prediction",
            info="Missing required sensors",
        )
