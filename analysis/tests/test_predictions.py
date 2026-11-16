from analysis.predictions.predictions import PredObj, PredictionsEngine
from analysis.predictions.predictions_files import PredictionClass


class FakeLapDetector:
    sensors = set()


def test_get_signals_keeps_latest_sample_for_each_requested_sensor():
    message = [
        {"sensor_id": "CAR_SPEED_KMH", "ts": 10, "median": 80},
        {"sensor_id": "CAR_SPEED_KMH", "ts": 20, "median": 91.5},
        {"sensor_id": "IGNORED", "ts": 30, "median": 1},
    ]

    signals = PredictionsEngine._get_signals(message, {"CAR_SPEED_KMH"})

    assert set(signals) == {"CAR_SPEED_KMH"}
    assert signals["CAR_SPEED_KMH"].ts_ns == 20
    assert signals["CAR_SPEED_KMH"].val == 91.5


def test_prediction_state_is_announced_once_per_availability_transition():
    prediction_class = PredictionClass(
        name="Speed prediction",
        sensors={"CAR_SPEED_KMH"},
        refresh_rate="every lap",
    )
    prediction = PredObj("speed_prediction", False, prediction_class)
    engine = object.__new__(PredictionsEngine)
    engine.predictions = [prediction]
    announcements = []
    engine.send_prediction_state = lambda pred, refresh_rate=None: announcements.append(
        (pred.is_available, refresh_rate)
    )
    detector = FakeLapDetector()

    engine.handle_message([], set(), detector)
    engine.handle_message([], set(), detector)
    engine.handle_message([], {"CAR_SPEED_KMH"}, detector)

    assert announcements == [(False, None), (True, "every lap")]
