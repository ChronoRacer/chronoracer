from types import SimpleNamespace

from shapely.geometry import LineString

from analysis.predictions.predictions_files import (
    BrakeFadePrediction,
    FuelPrediction,
    LapComparePrediction,
    LapTimePrediction,
    TyreWearPrediction,
)


class _IdentityTransform:
    def transform(self, x, y):
        return x, y


class _FixedPipeline:
    def fit(self, features, results):
        return None

    def predict(self, features):
        return [91.25]


def _signal(value):
    return SimpleNamespace(val=value, ts_ns=1)


class _MeasurementThenForecast:
    def __init__(self, history_name):
        setattr(self, history_name, [])
        self.trend_window = 3

    def fill(self, *args):
        pass

    def process_data(self, is_finish_line_crossed):
        return 82.0 if is_finish_line_crossed else None


def test_lap_time_wrapper_uses_metric_track_context():
    prediction = LapTimePrediction()
    prediction.model.y[25] = [90.0, 91.0]
    prediction.model.pipelines[25] = _FixedPipeline()
    signals = {
        "STATE_OF_CHARGE_PERC": _signal(80.0),
        "RPM": _signal(5000.0),
        "CAR_SPEED_KMH": _signal(100.0),
        "GPS_LON_DEG": _signal(25.0),
        "GPS_LAT_DEG": _signal(0.0),
    }
    lap_ctx = {
        "track_meters": LineString([(0.0, 0.0), (100.0, 0.0)]),
        "project_to_meters": _IdentityTransform(),
        "p25": object(),
        "p50": object(),
        "p75": object(),
        "lap_time": 20.0,
        "is_finish_line_crossed": False,
        "curr_lap": 4,
    }

    result = prediction.run(signals, lap_ctx)

    assert result is not None
    assert result.value == 91.25
    assert "25%" in result.instruction
    assert prediction.model.track is lap_ctx["track_meters"]
    assert prediction.model.project_to_meters is lap_ctx["project_to_meters"]


def test_fuel_wrapper_reports_current_lap_in_laps_to_empty():
    prediction = FuelPrediction()
    prediction.model.initial_laps = 0
    prediction.model.set_start_point(SimpleNamespace(lat=52.0, lon=21.0))
    prediction.model.features = [[1.0, 100.0], [2.0, 100.0]]
    prediction.model.fuels_arr = [9.0, 8.0]

    class _FuelRegressor:
        def fit(self, features, results):
            self.coef_ = [-1.0, 0.0]
            self.intercept_ = 10.0

        def predict(self, features):
            return [8.0]

    prediction.model.huber_reg = _FuelRegressor()
    prediction.is_prepared = True
    result = prediction.run(
        {"FUEL_TANK_L": _signal(8.0), "CAR_SPEED_KMH": _signal(100.0)},
        {"is_finish_line_crossed": True, "curr_lap": 4},
    )

    assert result is not None
    assert "7.0 laps to empty" in result.instruction


def test_tyre_and_brake_models_advance_on_finish_with_missing_window_sensors():
    tyre = TyreWearPrediction()
    brakes = BrakeFadePrediction()
    lap_ctx = {"is_finish_line_crossed": True, "curr_lap": 3}

    assert tyre.run({}, lap_ctx) is None
    assert brakes.run({}, lap_ctx) is None
    assert tyre.model.lap_num == 1
    assert brakes.model.lap_num == 1


def test_tyre_and_brake_titles_stay_neutral_for_measurements_and_forecasts():
    tyre = TyreWearPrediction()
    tyre.model = _MeasurementThenForecast("retention_history")
    brakes = BrakeFadePrediction()
    brakes.model = _MeasurementThenForecast("brake_fade_history")
    signals = {
        "STEER_ANGLE_DEG": _signal(10.0),
        "CAR_SPEED_KMH": _signal(100.0),
        "ACCEL_LON_MS2": _signal(-1.0),
        "ACCEL_LAT_MS2": _signal(0.8),
        "BRAKE_PRESSURE_PERC": _signal(40.0),
        "THROTTLE_PERC": _signal(60.0),
        "STATE_OF_CHARGE_PERC": _signal(80.0),
        "AMBIENT_TEMP_C": _signal(20.0),
    }
    lap_ctx = {"is_finish_line_crossed": True, "curr_lap": 4}

    tyre_measured = tyre.run(signals, lap_ctx)
    brakes_measured = brakes.run(signals, lap_ctx)
    assert tyre_measured is not None and "(measured)" in tyre_measured.instruction
    assert brakes_measured is not None and "(measured)" in brakes_measured.instruction

    tyre.model.retention_history = [(1, 80.0), (2, 79.0), (3, 78.0)]
    brakes.model.brake_fade_history = [(1, 80.0), (2, 79.0), (3, 78.0)]
    tyre_forecast = tyre.run(signals, lap_ctx)
    brakes_forecast = brakes.run(signals, lap_ctx)
    assert tyre_forecast is not None and "Projected" in tyre_forecast.instruction
    assert brakes_forecast is not None and "Projected" in brakes_forecast.instruction

    assert tyre.name == "TYRE GRIP RETENTION VS FRESH BASELINE"
    assert brakes.name == "BRAKE PERFORMANCE VS FRESH BASELINE"
    assert "PROJECTED" not in tyre.name and "CURRENT LAP" not in tyre.name
    assert "PROJECTED" not in brakes.name and "CURRENT LAP" not in brakes.name


def test_lap_comparison_discards_partial_samples_when_finish_sensors_are_missing():
    prediction = LapComparePrediction()
    prediction.model.process_data(False, 0.0, 2, 100.0, 80.0, 60.0)
    prediction.model.process_data(False, 0.0, 2, 110.0, 80.0, 0.0)

    result = prediction.run({}, {"is_finish_line_crossed": True, "lap_time": 90.0, "curr_lap": 3})

    assert result is None
    assert prediction.model._speed_partial_lst == []
    assert prediction.model.curr_brakes_count == 0
    assert prediction.model._hv_soc_start is None
