from analysis.predictions.predictions import PredObj, PredictionsEngine
from analysis.predictions.predictions_files import PredictionClass


class FakeLapDetector:
    sensors = set()
    curr_lap_num = 0
    lap_time = 0.0
    track_meters = None
    _project_to_meters = None
    turns_lst = None

    def __init__(self, start_set=False):
        self.start_set = start_set

    def is_start_set(self):
        return self.start_set

    def find_track_data(self):
        return None, None, None, None

    def get_start_point(self):
        return None


class RecordingPrediction(PredictionClass):
    def __init__(self, sensors, needs_lap):
        super().__init__(name="recording", sensors=sensors, needs_lap=needs_lap)
        self.calls = 0

    def run(self, signals, lap_ctx):
        self.calls += 1
        return None


def _engine_with(predictions):
    engine = object.__new__(PredictionsEngine)
    engine.predictions = predictions
    engine.send_prediction_state = lambda pred, refresh_rate=None: None
    return engine


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


def test_window_without_gps_still_runs_predictions_that_do_not_need_a_lap():
    oil = RecordingPrediction({"OIL_TEMP_C"}, needs_lap=False)
    fuel = RecordingPrediction({"OIL_TEMP_C"}, needs_lap=True)
    engine = _engine_with([PredObj("oil", False, oil), PredObj("fuel", False, fuel)])
    message = [{"sensor_id": "OIL_TEMP_C", "ts": 10, "median": 95.0}]

    engine.handle_message(message, {"OIL_TEMP_C"}, FakeLapDetector(start_set=False))

    assert oil.calls == 1
    assert fuel.calls == 0


def test_lap_predictions_run_without_gps_once_the_lap_start_is_known():
    fuel = RecordingPrediction({"OIL_TEMP_C"}, needs_lap=True)
    engine = _engine_with([PredObj("fuel", False, fuel)])
    message = [{"sensor_id": "OIL_TEMP_C", "ts": 10, "median": 95.0}]

    engine.handle_message(message, {"OIL_TEMP_C"}, FakeLapDetector(start_set=True))

    assert fuel.calls == 1
