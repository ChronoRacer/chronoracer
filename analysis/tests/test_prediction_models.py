import math

from analysis.predictions.prediction_models import (
    EARTH_RADIUS_M,
    LapComparePred,
    LapDetector,
    MPoint,
    START_TIME_NS,
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
