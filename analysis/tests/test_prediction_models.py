import math

import numpy as np
import pytest
from pyproj import Transformer
from shapely.geometry import LineString

from analysis.predictions.prediction_models import (
    EARTH_RADIUS_M,
    BrakeFadePred,
    LackOfFuelPred,
    LapComparePred,
    LapDetector,
    LapTimePred,
    MPoint,
    START_TIME_NS,
    TyreWearPred,
)


def _point_at_meters(*, north=0.0, east=0.0):
    return MPoint(
        lat=math.degrees(north / EARTH_RADIUS_M),
        lon=math.degrees(east / EARTH_RADIUS_M),
    )


def test_lap_detector_sets_start_using_elapsed_gps_timestamp():
    detector = LapDetector("GPS_LAT_DEG", "GPS_LON_DEG")
    first_ts = 10_000_000_000

    assert not detector.determine_start_on_track(52.0, 21.0, first_ts)
    assert not detector.determine_start_on_track(52.1, 21.1, first_ts + START_TIME_NS - 1)
    assert detector.determine_start_on_track(52.2, 21.2, first_ts + START_TIME_NS)

    assert detector.get_start_point() == MPoint(lat=52.2, lon=21.2)
    assert detector.curr_lap_num == 1
    assert detector.lap_start_ts_ns == first_ts + START_TIME_NS
    assert detector.is_start_set()
    assert not detector.is_turns_set()


def test_lap_detector_rejects_finish_checks_without_start_or_lap_clock():
    detector = LapDetector("GPS_LAT_DEG", "GPS_LON_DEG")
    point = _point_at_meters(east=25)
    assert detector.find_next_lap(point, 1_000_000_000) is None

    detector.lat_start = 0.0
    detector.lon_start = 0.0
    assert detector.find_next_lap(point, 2_000_000_000) is None
    assert detector.previous_point is None

    detector.lap_start_ts_ns = 1_000_000_000
    assert detector.find_next_lap(point, 2_000_000_000) is None
    assert detector.previous_point == point
    assert detector.lap_time == 1.0


def test_lap_detector_preserves_line_radius_for_stationary_gps_points():
    origin = MPoint(lat=0.0, lon=0.0)
    assert LapDetector._segment_reaches_start(origin, origin, origin)


def test_lap_detector_counts_sparse_segment_crossing_after_car_leaves_start():
    detector = LapDetector("GPS_LAT_DEG", "GPS_LON_DEG")
    detector.lat_start = 0.0
    detector.lon_start = 0.0
    detector.previous_point = MPoint(lat=0.0, lon=0.0)
    detector.lap_start_ts_ns = 1_000_000_000
    detector.curr_lap_num = 1

    assert detector.find_next_lap(_point_at_meters(east=25), 2_000_000_000) is None
    finish_point = _point_at_meters(east=-25)
    assert detector.find_next_lap(finish_point, 97_000_000_000) == finish_point

    assert detector.curr_lap_num == 2
    assert detector.lap_time == 96.0
    assert detector.lap_start_ts_ns == 97_000_000_000


def test_segment_crossing_uses_distance_to_whole_segment():
    origin = MPoint(lat=0.0, lon=0.0)

    assert LapDetector._segment_reaches_start(
        origin,
        _point_at_meters(east=30),
        _point_at_meters(east=-30),
    )
    assert not LapDetector._segment_reaches_start(
        origin,
        _point_at_meters(north=10, east=-20),
        _point_at_meters(north=10, east=20),
    )


def test_lap_detector_rejects_short_excursion_out_and_back():
    detector = LapDetector("GPS_LAT_DEG", "GPS_LON_DEG")
    detector.lat_start = 0.0
    detector.lon_start = 0.0
    detector.previous_point = _point_at_meters()
    detector.lap_start_ts_ns = 1_000_000_000
    detector.curr_lap_num = 1

    assert detector.find_next_lap(_point_at_meters(east=20), 2_000_000_000) is None
    assert detector.find_next_lap(_point_at_meters(), 3_000_000_000) is None

    assert detector.curr_lap_num == 1
    assert detector.lap_start_ts_ns == 3_000_000_000
    assert detector.lap_time == 0.0
    assert not detector.departed_from_start


def test_lap_detector_records_track_only_after_start_and_until_ready_lap():
    detector = LapDetector("GPS_LAT_DEG", "GPS_LON_DEG", track_ready_lap=2)

    detector.add_track_point(21.0, 52.0)
    assert detector.track_lon_lat == [(21.0, 52.0)]
    assert detector.find_track_data() == (None, None, None, None)

    detector.track_lon_lat = [(21.0, 52.0)]
    assert detector.find_track_data() == (None, None, None, None)

    detector.curr_lap_num = 2
    detector.add_track_point(21.001, 52.0)
    assert len(detector.track_lon_lat) == 1
    assert detector.find_track_data() == (None, None, None, None)

    detector.add_track_point(21.002, 52.0)
    assert len(detector.track_lon_lat) == 1


def test_lap_detector_builds_metric_track_once_track_is_ready():
    detector = LapDetector("GPS_LAT_DEG", "GPS_LON_DEG", track_ready_lap=2)
    detector.curr_lap_num = 1
    detector.add_track_point(21.0, 52.0)
    detector.add_track_point(21.001, 52.0)
    detector.curr_lap_num = 2

    track, p25, p50, p75 = detector.find_track_data()

    assert track is not None
    assert detector.track_meters is not None
    assert detector.track_meters.length > 0
    assert all(point is not None for point in (p25, p50, p75))
    assert detector.find_track_data() == (track, p25, p50, p75)


def test_fuel_laps_to_empty_counts_the_current_lap():
    model = LackOfFuelPred()
    model.huber_reg.coef_ = np.array([-1.0, 0.0])
    model.huber_reg.intercept_ = 10.0

    assert model._laps_to_empty(curr_lap=4, mean_speed=100.0) == (7.0, 1.0)
    assert model._laps_to_empty(curr_lap=12, mean_speed=100.0) == (0.0, 1.0)


def test_fuel_laps_to_empty_is_unavailable_without_decreasing_fuel_fit():
    model = LackOfFuelPred()
    assert model._laps_to_empty(curr_lap=1, mean_speed=100.0) == (None, None)


def test_fuel_model_waits_for_start_and_two_finished_laps_then_predicts():
    model = LackOfFuelPred(initial_laps=2)
    model.huber_reg = _FixedFuelRegressor()
    assert model.process_data(True, 2) is None
    model.set_start_point(MPoint(lat=52.0, lon=21.0))

    model.fill(False, 1, 10.0, 100.0)
    assert model.process_data(False, 1) is None
    model.fill(True, 2, 9.0, 100.0)
    assert model.process_data(True, 2) is None
    model.fill(True, 3, 8.0, 100.0)
    result = model.process_data(True, 3)

    assert result is not None
    laps_left, burn_per_lap, predicted_fuel = result
    assert laps_left == pytest.approx(8.0)
    assert burn_per_lap == pytest.approx(1.0)
    assert predicted_fuel == pytest.approx(8.0)
    assert model.speed_samples.size == 0


def test_fuel_model_resets_learning_after_refuelling():
    model = LackOfFuelPred(initial_laps=2)
    model.set_start_point(MPoint(lat=52.0, lon=21.0))
    model.fuels_arr = np.array([8.0])
    model.features = np.array([[1.0, 100.0]])
    model.speed_samples = np.array([100.0])
    model.last_mean_speed = 100.0

    model.fill(True, 2, 9.0, 100.0)

    assert model.fuels_arr.tolist() == [9.0]
    assert model.features.tolist() == [[1.0, 100.0]]
    assert model.initial_laps == 2

    model.huber_reg.coef_ = np.array([1.0, 0.0])
    model.huber_reg.intercept_ = 10.0
    assert model._laps_to_empty(curr_lap=1, mean_speed=100.0) == (None, None)


class _FixedRegressor:
    def __init__(self, prediction=1.0):
        self.prediction = prediction
        self.fit_rows = None

    def fit(self, features, results):
        self.fit_rows = (features, results)

    def predict(self, features):
        return np.full(len(features), self.prediction)


class _FixedFuelRegressor:
    def fit(self, features, results):
        self.coef_ = np.array([-1.0, 0.0])
        self.intercept_ = 10.0

    def predict(self, features):
        return np.array([8.0])


def test_lap_time_split_uses_metre_distance_not_degree_distance():
    to_meters = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    geographic_track = LineString([(0.0, 0.0), (0.0, 60.0)])
    metric_track = LineString([to_meters.transform(*point) for point in geographic_track.coords])
    model = LapTimePred()
    model.set_track(metric_track, to_meters)

    # Halfway in degrees of latitude is about 41% of the projected Mercator
    # track distance, rather than 50% of the geographic-degree line.
    assert 40 <= model._raw_perc(0.0, 30.0) <= 42


def test_lap_time_model_trains_at_finish_and_emits_once_at_split():
    model = LapTimePred(min_split_laps=1)
    model.set_track(LineString([(0.0, 0.0), (100.0, 0.0)]), _IdentityTransform())
    fixed = _FixedRegressor(prediction=92.5)
    model.pipelines[25] = fixed

    model.fill(25.0, 0.0, 20.0, 100.0, 80.0, 5000.0)
    model.fill(25.0, 0.0, 21.0, 101.0, 81.0, 5100.0)
    assert model.process_data(True, 90.0) is None
    assert len(model.y[25]) == 1
    assert model.taken_this_lap == set()

    model.fill(25.0, 0.0, 22.0, 102.0, 82.0, 5200.0)
    assert model.process_data(False, 0.0) == (92.5, 25)
    assert model.process_data(False, 0.0) is None


def test_lap_time_model_ignores_missing_track_and_unmatched_split():
    model = LapTimePred()
    assert model._raw_perc(0.0, 0.0) is None
    model.set_track(LineString(), _IdentityTransform())
    assert model._raw_perc(0.0, 0.0) is None
    assert model._split_at(40) is None
    model.fill(0.0, 0.0, 1.0, 1.0, 1.0, 1.0)
    assert model.process_data(False, 0.0) is None
    assert model._predict_at_split(25) is None


class _IdentityTransform:
    def transform(self, x, y):
        return x, y


def test_tyre_model_filters_small_steer_and_extends_baseline_until_sample_minimum():
    model = TyreWearPred(good_tyre_laps=1, steer_angle=5.0, min_fit_sample=2, trend_window=2)
    model.regressor = _FixedRegressor(prediction=1.0)

    model.fill(4.99, 100.0, 0.0, 0.0, 100.0, 0.5)
    assert model.good_features == []
    model.fill(5.0, 100.0, 0.0, 0.0, 100.0, 0.5)
    assert model.process_data(True) is None
    assert model.laps_with_good_tyre == 1
    assert not model.is_fitted

    model.fill(-5.0, 100.0, 0.0, 0.0, 100.0, 0.5)
    assert model.process_data(True) is None
    assert model.is_fitted
    assert len(model.regressor.fit_rows[0]) == 2

    model.fill(6.0, 100.0, 0.0, 0.0, 100.0, 0.8)
    assert model.process_data(False) is None
    assert model.process_data(True) == 80.0
    model.fill(6.0, 100.0, 0.0, 0.0, 100.0, 0.7)
    assert model.process_data(True) == pytest.approx(60.0)


def test_tyre_model_skips_nonfinish_and_handles_zero_expected_grip():
    model = TyreWearPred(good_tyre_laps=0, trend_window=1)
    model.regressor = _FixedRegressor(prediction=0.0)
    model.is_fitted = True
    model.fill(8.0, 100.0, 0.0, 0.0, 100.0, 0.8)

    assert model.process_data(False) is None
    assert model.lap_num == 0
    assert model._retention_for_finished_lap() is None
    assert model.process_data(True) is None
    assert model.last_retention is None
    assert model.current_lap_features == []


def test_brake_model_applies_straight_line_filters_and_forecast_window():
    model = BrakeFadePred(good_brakes_laps=1, min_fit_sample=2, trend_window=2)
    model.regressor = _FixedRegressor(prediction=1.0)

    model.fill(20.01, 80.0, 0.0, 40.0, 80.0, 20.0)
    model.fill(10.0, 49.99, 0.0, 40.0, 80.0, 20.0)
    model.fill(10.0, 80.0, 0.0, 19.99, 80.0, 20.0)
    assert model.good_features == []

    for _ in range(2):
        model.fill(20.0, 50.0, -0.8, 20.0, 80.0, 20.0)
    assert model.process_data(True) is None
    assert model.is_fitted
    assert len(model.regressor.fit_rows[0]) == 2

    model.fill(0.0, 80.0, -0.8, 40.0, 80.0, 20.0)
    assert model.process_data(True) == 80.0
    model.fill(0.0, 80.0, -0.7, 40.0, 80.0, 20.0)
    assert model.process_data(True) == pytest.approx(60.0)


def test_brake_model_skips_nonfinish_and_handles_empty_or_zero_expected_lap():
    model = BrakeFadePred(good_brakes_laps=0)
    model.regressor = _FixedRegressor(prediction=0.0)
    model.is_fitted = True
    assert model.process_data(False) is None
    assert model._brake_fade_for_finished_lap() is None

    model.fill(0.0, 80.0, -0.5, 40.0, 80.0, 20.0)
    assert model._brake_fade_for_finished_lap() is None
    assert model.process_data(True) is None
    assert model.last_brake_fade is None


def test_lap_compare_emits_deltas_after_first_lap_baseline():
    model = LapComparePred()

    model.process_data(False, 0.0, 1, 100.0, 80.0, 0.0)
    model.process_data(False, 0.0, 1, 120.0, 80.0, 60.0)
    assert model.process_data(True, 100.0, 2, 110.0, 70.0, 0.0) is None

    model.process_data(False, 0.0, 2, 100.0, 70.0, 60.0)
    result = model.process_data(True, 94.0, 3, 110.0, 60.0, 0.0)

    assert result is not None
    assert result.lap_num == 2
    assert result.avg_speed == 100.0
    assert result.hard_brakes == 1
    assert result.delta_lap_time == -6.0
    assert result.delta_best_lap_time == -6.0
    assert result.best_lap_num == 1
    assert model._lap_score_func(result) == 89.5
    assert model._lap_score_func(result._replace(best_lap_time=0.0)) == 0.0
    slower = result._replace(delta_lap_time=2.0, delta_best_lap_time=5.0)
    assert model._lap_score_func(slower) < 100.0
    assert model._lap_score_func(result._replace(hv_soc_delta=1.0)) > 0.0

    model.process_data(False, 0.0, 3, 100.0, 60.0, 0.0)
    model.process_data(True, 100.0, 4, 100.0, 60.0, 0.0)
    assert model.best_lap_num == 2


def test_lap_compare_empty_speed_average_is_zero():
    assert LapComparePred()._avg_speed() == 0.0
