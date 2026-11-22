import numpy as np
import pytest
import time
from pyproj import Transformer
from shapely.geometry import LineString

import analysis.predictions.prediction_models as prediction_models
from analysis.predictions.prediction_models import (
    CarFluidsPred,
    CoolantTempPred,
    LapDetector,
    MPoint,
    MTurn,
    SpeedOnTurnsPred,
)


class _FixedPipeline:
    def __init__(self, prediction=50.0):
        self.prediction = prediction
        self.fit_data = None

    def fit(self, features, results):
        self.fit_data = (features, results)

    def predict(self, features):
        return np.full(len(features), self.prediction)


def test_lap_detector_start_accessors_and_preexisting_start():
    detector = LapDetector("lat", "lon")
    assert not detector.is_start_set()
    assert detector.get_start_point() is None
    assert not detector.determine_start_on_track(1.0, 2.0, 1)
    assert not detector.determine_start_on_track(1.0, 2.0, 1 + 1_499_999_999)
    assert detector.determine_start_on_track(1.0, 2.0, 1 + 1_500_000_000)
    assert detector.is_start_set()
    assert detector.determine_start_on_track(3.0, 4.0, 9_000_000_000)
    assert detector.get_start_point() == MPoint(lat=1.0, lon=2.0)
    detector.turns_lst = [
        MTurn(middle=MPoint(lat=0.0, lon=0.0), radius_m=50.0, is_left_turn=True, s_m=10.0)
    ]
    assert detector.is_turns_set()


def test_lap_detector_updates_elapsed_time_without_a_line_crossing():
    detector = LapDetector("lat", "lon")
    detector.lat_start = 0.0
    detector.lon_start = 0.0
    detector.previous_point = MPoint(lat=0.0, lon=0.0)
    detector.lap_start_ts_ns = 1_000_000_000
    detector.curr_lap_num = 1

    assert detector.find_next_lap(MPoint(lat=0.0, lon=0.001), 2_000_000_000) is None
    assert detector.find_next_lap(MPoint(lat=0.0, lon=0.002), 3_000_000_000) is None
    assert detector.lap_time == 2.0
    assert detector.curr_lap_num == 1


def test_lap_detector_does_not_arm_while_still_inside_start_radius():
    detector = LapDetector("lat", "lon")
    detector.lat_start = 0.0
    detector.lon_start = 0.0
    detector.previous_point = MPoint(lat=0.0, lon=0.0)
    detector.lap_start_ts_ns = 1_000_000_000

    assert detector.find_next_lap(MPoint(lat=0.0, lon=0.0), 2_000_000_000) is None
    assert not detector.departed_from_start


def test_turn_radius_and_threshold_estimation_paths(monkeypatch):
    detector = LapDetector("lat", "lon")
    assert detector._get_turn_radius((0, 0), (1, 1), (2, 2)) == float("inf")
    assert detector._get_turn_radius((0, 0), (1, 0), (1, 1)) == pytest.approx(0.70710678)
    assert detector._estimate_thresholds_bgm([1.0], [10.0]) == (10.0, 120.0)

    class _BGM:
        calls = 0

        def __init__(self, **kwargs):
            pass

        def fit(self, values):
            self.means_ = np.array([[0.0], [40.0]]) if values.max() > 100 else np.array([[0.0], [20.0]])
            _BGM.calls += 1

    monkeypatch.setattr(prediction_models, "BayesianGaussianMixture", _BGM)
    thresholds = detector._estimate_thresholds_bgm(
        [0.0, 1.0, 2.0, 20.0, 21.0, 22.0],
        [50.0, 60.0, 70.0, 100.0, 110.0, 120.0],
    )
    assert thresholds == (10.0, 40.0)
    assert _BGM.calls == 2
    assert detector._estimate_thresholds_bgm(
        [0.0, 1.0, 2.0, 20.0, 21.0, 22.0], [50.0, 60.0]
    ) == (10.0, 120.0)

    class _BrokenBGM:
        def __init__(self, **kwargs):
            raise ValueError("model fit failed")

    monkeypatch.setattr(prediction_models, "BayesianGaussianMixture", _BrokenBGM)
    assert detector._estimate_thresholds_bgm([0, 1, 2, 3, 4, 5], [1] * 6) == (10.0, 120.0)


def test_turn_detection_groups_nearby_candidates_and_converts_apex_points():
    detector = LapDetector("lat", "lon", distance_from_turns=100.0)
    to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    track = LineString([(0.0, 0.0), (100.0, 0.0), (100.0, 100.0), (200.0, 100.0)])

    detector._determine_turns(track, to_wgs84)

    assert detector.is_turns_set()
    assert len(detector.turns_lst) == 1
    assert detector.turns_lst[0].radius_m > 0
    assert detector.turns_lst[0].s_m > 0


def test_turn_detection_splits_distant_candidates_into_separate_clusters():
    detector = LapDetector("lat", "lon", distance_from_turns=10.0)
    to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    track = LineString([(0.0, 0.0), (100.0, 0.0), (100.0, 100.0), (200.0, 100.0)])

    detector._determine_turns(track, to_wgs84)

    assert detector.turns_lst is not None
    assert len(detector.turns_lst) == 2


def test_straight_track_does_not_create_turn_candidates():
    detector = LapDetector("lat", "lon")
    to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

    detector._determine_turns(LineString([(0.0, 0.0), (50.0, 0.0), (100.0, 0.0)]), to_wgs84)

    assert not detector.is_turns_set()


def test_fuel_fill_and_process_reject_missing_training_data():
    model = prediction_models.LackOfFuelPred(initial_laps=0)
    model.fill(True, 2, 8.0, 100.0)
    model.set_start_point(MPoint(lat=52.0, lon=21.0))
    model.fill(True, 0, 8.0, 100.0)
    assert model.process_data(True, 2) is None

    model.fuels_arr = np.array([9.0, 8.0])
    model.last_mean_speed = 100.0
    model.features = np.array([[1.0, 100.0]])
    assert model.process_data(True, 2) is None


def test_lap_time_split_waits_for_minimum_training_laps():
    model = prediction_models.LapTimePred(min_split_laps=3)
    model.pipelines[25] = _FixedPipeline()
    model.pipelines[50] = _FixedPipeline()
    model.features_this_lap[25] = np.array([1.0, 2.0, 3.0, 4.0])
    model.y[25] = np.array([90.0])
    assert model._predict_at_split(25) is None
    model.features_this_lap[50] = np.array([1.0, 2.0, 3.0, 4.0])
    assert model.process_data(True, 90.0) is None
    assert model.pipelines[25].fit_data is None
    assert model.pipelines[50].fit_data is None

    pending = prediction_models.LapTimePred(min_split_laps=3)
    pending.features_this_lap[25] = np.array([1.0, 2.0, 3.0, 4.0])
    pending.pending_split = 25
    assert pending.process_data(False, 0.0) is None


@pytest.mark.parametrize("model_type", [CarFluidsPred, CoolantTempPred])
def test_temperature_forecasters_bucket_samples_and_fit_only_once_per_bucket(model_type):
    if model_type is CarFluidsPred:
        model = model_type(check_time_s=1, fit_time_s=2, window_length_s=2)
        model.set_race_start_time(0)
        model.pipeline = _FixedPipeline(111.0)
        fill = lambda value: model.fill(value, 3000.0, 40.0, 100.0, 20.0)
        process = lambda: model.process_data(80.0, 3000.0, 40.0, 100.0, 20.0)
        set_clock = lambda seconds: setattr(model, "_elapsed_s", lambda: seconds)
        bucket_field = "_last_sample_bucket"
    else:
        model = model_type(check_time_s=1, fit_time_s=2, horizon_s=2)
        model.set_race_start_time(0)
        model.regressor = _FixedPipeline(91.0)
        fill = lambda value: model.fill(value, 80.0, 3000.0, 40.0, 100.0, 20.0)
        process = lambda: model.process_data(80.0, 80.0, 3000.0, 40.0, 100.0, 20.0)
        set_clock = lambda seconds: setattr(model, "_elapsed_s", lambda: seconds)
        bucket_field = "last_sample_bucket"

    for seconds in range(10):
        set_clock(seconds)
        fill(70.0 + seconds)
        fill(71.0 + seconds)

    assert getattr(model, bucket_field) == 9
    assert len(model.window) <= 7
    assert model.window_partial

    assert process() is not None
    assert process() is None
    set_clock(10)
    assert process() is not None


def test_temperature_forecasters_wait_for_enough_received_samples():
    oil = CarFluidsPred(check_time_s=1, fit_time_s=2, window_length_s=2)
    coolant = CoolantTempPred(check_time_s=1, fit_time_s=2, horizon_s=2)
    assert oil._horizon_steps() == coolant._horizon_steps() == 2
    assert oil.process_data(80.0, 3000.0, 40.0, 100.0, 20.0) is None
    assert coolant.process_data(80.0, 80.0, 3000.0, 40.0, 100.0, 20.0) is None


def test_temperature_forecasters_measure_elapsed_seconds_from_race_start(monkeypatch):
    monkeypatch.setattr(time, "time_ns", lambda: 5_000_000_000)
    oil = CarFluidsPred()
    coolant = CoolantTempPred()
    oil.set_race_start_time(1_000_000_000)
    coolant.set_race_start_time(1_000_000_000)

    assert oil._elapsed_s() == coolant._elapsed_s() == 4


class _CarProjection:
    def __init__(self):
        self.x = 50.0

    def transform(self, lon, lat):
        return self.x, 0.0


def test_speed_on_turn_model_activates_learns_finalizes_and_skips_visited_turns():
    model = SpeedOnTurnsPred(zone_m=35.0, hysteresis_m=5.0)
    projector = _CarProjection()
    turn = MTurn(middle=MPoint(lat=0.0, lon=0.0), radius_m=50.0, is_left_turn=True, s_m=50.0)
    model.set_turns([turn], LineString([(0.0, 0.0), (100.0, 0.0)]), projector)

    assert model.process_data(0.0, 0.0, 80.0, -0.6) == (None, None, None)
    assert model.active == 0
    model.process_data(0.0, 0.0, 70.0, -0.7)
    model.process_data(0.0, 0.0, 60.0, -0.8)
    projector.x = 95.0
    max_safe, apex_speed, turn_number = model.process_data(0.0, 0.0, 50.0, -0.9)

    assert max_safe > 0.0
    assert apex_speed == 70.0
    assert turn_number == 1
    assert model.visited_turns == {0}
    assert model.process_data(0.0, 0.0, 50.0, 0.0) == (None, None, None)


def test_speed_on_turn_model_empty_and_too_small_samples_are_ignored():
    model = SpeedOnTurnsPred()
    assert model.process_data(0.0, 0.0, 50.0, 0.0) == (None, None, None)
    model.active = 0
    model.turns_lst = [
        MTurn(middle=MPoint(lat=0.0, lon=0.0), radius_m=50.0, is_left_turn=True, s_m=50.0)
    ]
    model.track_meters = LineString([(0.0, 0.0), (100.0, 0.0)])
    model.project_to_meters = _CarProjection()
    model._dist1d = lambda car_s, turn_s: 100.0
    assert model.process_data(0.0, 0.0, 50.0, 0.0) == (None, None, None)
    assert model.active is None
    assert model.visited_turns == {0}


def test_speed_on_turn_model_keeps_nearest_unvisited_candidate():
    model = SpeedOnTurnsPred(zone_m=35.0)
    projector = _CarProjection()
    turns = [
        MTurn(middle=MPoint(lat=0.0, lon=0.0), radius_m=40.0, is_left_turn=True, s_m=40.0),
        MTurn(middle=MPoint(lat=0.0, lon=0.0), radius_m=50.0, is_left_turn=False, s_m=60.0),
    ]
    model.set_turns(turns, LineString([(0.0, 0.0), (100.0, 0.0)]), projector)
    model._dist1d = lambda car_s, turn_s: 10.0 if turn_s == 40.0 else 20.0

    model.process_data(0.0, 0.0, 80.0, 0.5)

    assert model.active == 0


def test_speed_on_turn_grip_history_is_capped():
    model = SpeedOnTurnsPred()
    model._update_grip_limit([])
    model._update_grip_limit([0.5] * 1001)
    assert len(model.learned_lat_accels) == 1000
    assert model.max_stable_lat_accel == pytest.approx(0.5)


def test_brake_baseline_extends_until_it_has_enough_samples():
    model = prediction_models.BrakeFadePred(good_brakes_laps=1, min_fit_sample=2)
    model.regressor = _FixedPipeline()
    model.fill(0.0, 80.0, -0.8, 40.0, 80.0, 20.0)

    assert model.process_data(True) is None
    assert model.laps_with_good_brakes == 1
    assert not model.is_fitted
