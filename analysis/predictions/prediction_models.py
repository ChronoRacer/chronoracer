import math
import numpy as np
from shapely.geometry.linestring import LineString
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import HuberRegressor
from pydantic import BaseModel
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV
from shapely.geometry import Point
import lightgbm as lgb
import time
# GPS aggregation cadence can change, so elapsed time must always come from
# GPS timestamps instead of an assumed samples-per-second value.
START_TIME_NS = 1_500_000_000
RADIUS_M = 8.0
EXIT_RADIUS_M = 20.0
EARTH_RADIUS_M = 6_371_000.0


class MPoint(BaseModel):
    lat: float
    lon: float


class LapDetector:
    def __init__(self, gps_lat: str, gps_lon: str):
        self.sensors: set[str] = {gps_lat, gps_lon}
        self.lat_start: float | None = None
        self.lon_start: float | None = None
        self.start_collection_ts_ns: int | None = None
        self.previous_point: MPoint | None = None
        self.departed_from_start = False
        self.track_lon_lat = []
        self.track : LineString | None =  None
        self.p25 = None
        self.p50 = None
        self.p75 = None
        self.curr_lap_num = 0
        self.lap_start_ts_ns: int | None = None
        self.lap_time : float = 0.0

    def is_start_set(self) -> bool:
        return self.lat_start is not None and self.lon_start is not None

    def get_start_point(self) -> MPoint | None:
        if self.is_start_set():
            return MPoint(lat=self.lat_start, lon=self.lon_start)
        return None

    def determine_start_on_track(
        self, gps_lat: float, gps_lon: float, gps_ts_ns: int
    ) -> bool:
        if self.is_start_set():
            return True

        if self.start_collection_ts_ns is None:
            self.start_collection_ts_ns = gps_ts_ns
            return False

        if gps_ts_ns - self.start_collection_ts_ns < START_TIME_NS:
            return False

        self.lat_start = gps_lat
        self.lon_start = gps_lon
        self.previous_point = MPoint(lat=gps_lat, lon=gps_lon)
        self.lap_start_ts_ns = gps_ts_ns
        self.lap_time = 0.0
        self.curr_lap_num = 1
        return True

    @staticmethod
    def _local_xy(origin: MPoint, point: MPoint) -> tuple[float, float]:
        north_m = math.radians(point.lat - origin.lat) * EARTH_RADIUS_M
        east_m = (
            math.radians(point.lon - origin.lon)
            * EARTH_RADIUS_M
            * math.cos(math.radians(origin.lat))
        )
        return north_m, east_m

    @classmethod
    def _segment_reaches_start(
        cls, start: MPoint, previous: MPoint, current: MPoint
    ) -> bool:
        ax, ay = cls._local_xy(start, previous)
        bx, by = cls._local_xy(start, current)
        dx, dy = bx - ax, by - ay
        length_squared = dx * dx + dy * dy
        if length_squared == 0.0:
            closest_x, closest_y = ax, ay
        else:
            fraction = max(0.0, min(1.0, -(ax * dx + ay * dy) / length_squared))
            closest_x = ax + fraction * dx
            closest_y = ay + fraction * dy
        return math.hypot(closest_x, closest_y) <= RADIUS_M

    def find_next_lap(self, test_point: MPoint, ts_ns: int) -> MPoint | None:
        start_point = self.get_start_point()
        if start_point is None or self.lap_start_ts_ns is None:
            return None

        elapsed_s = (ts_ns - self.lap_start_ts_ns) / 1e9
        previous_point = self.previous_point
        self.previous_point = test_point
        if previous_point is None:
            self.lap_time = elapsed_s
            return None

        north_m, east_m = self._local_xy(start_point, test_point)
        if not self.departed_from_start:
            if math.hypot(north_m, east_m) >= EXIT_RADIUS_M:
                self.departed_from_start = True
            self.lap_time = elapsed_s
            return None

        if self._segment_reaches_start(start_point, previous_point, test_point):
            self.departed_from_start = False
            self.lap_time = elapsed_s
            self.lap_start_ts_ns = ts_ns
            self.curr_lap_num += 1
            return test_point

        self.lap_time = elapsed_s
        return None

    def add_track_point(self,gps_lon: float, gps_lat : float):
        if self.curr_lap_num < 2:
            self.track_lon_lat.append((gps_lon, gps_lat))

    def find_track_data(self):
        if self.track_lon_lat is None or len(self.track_lon_lat) < 2 or self.curr_lap_num < 2:
            return None,None,None,None

        if self.track is not None:
            return self.track, self.p25, self.p50, self.p75

        self.track = LineString(self.track_lon_lat)
        self.p25  = self.track.interpolate(0.25, normalized=True)
        self.p50 = self.track.interpolate(0.5, normalized=True)
        self.p75 = self.track.interpolate(0.75, normalized=True)

        return self.track,self.p25,self.p50,self.p75


class LackOfFuelPred:
    def __init__(self, initial_laps: int = 2):
        self.features: np.ndarray = np.empty((0, 2))
        self.fuels_arr: np.ndarray = np.array([])
        self.speed_samples: np.ndarray = np.array([])
        self.last_mean_speed: float | None = None
        self.initial_laps: int = initial_laps
        self.initial_laps_const = initial_laps
        self.start_point: MPoint | None = None  # Point(Lat,Lon) on the track
        self.huber_reg = HuberRegressor(max_iter=100)

    def set_start_point(self, point: MPoint):
        self.start_point = point

    def _laps_to_empty(self, curr_lap: int, mean_speed: float):
        if not hasattr(self.huber_reg, "coef_"):
            return None, None

        # fuel ≈ a_lap * lap + a_speed * speed + b
        a_lap = float(self.huber_reg.coef_[0])
        a_speed = float(self.huber_reg.coef_[1])
        b = float(self.huber_reg.intercept_)

        if a_lap >= 0:
            return None, None
        target_lap = -(b + a_speed * mean_speed) / a_lap
        return max(0.0, target_lap - curr_lap), abs(a_lap)

    def fill(self, is_finish_line_crossed: bool, curr_lap: int, fuel: float, speed: float):
        if self.start_point is None:
            return

        self.speed_samples = np.append(self.speed_samples, [speed])
        if not is_finish_line_crossed:
            return

        # curr_lap is already the lap that just started; X lap index is the finished lap.
        finished_lap = curr_lap - 1
        if finished_lap < 1 or len(self.speed_samples) == 0:
            return

        if len(self.fuels_arr) > 0 and fuel > self.fuels_arr[-1]:
            self.features = np.empty((0, 2))
            self.fuels_arr = np.array([])
            self.last_mean_speed = None
            self.initial_laps = self.initial_laps_const

        mean_speed = float(np.mean(self.speed_samples))
        self.last_mean_speed = mean_speed
        self.features = np.vstack([self.features, [finished_lap, mean_speed]])
        self.fuels_arr = np.append(self.fuels_arr, [fuel])
        self.speed_samples = np.array([])

    def process_data(self, is_finish_line_crossed: bool, curr_lap: int):
        if self.start_point is None:
            return None

        if not is_finish_line_crossed:
            return None

        self.initial_laps -= 1
        if self.initial_laps > 0:
            return None
        if len(self.fuels_arr) < 2 or self.last_mean_speed is None:
            return None

        X_train = self.features
        y_train = self.fuels_arr
        if X_train.shape[0] != y_train.shape[0]:
            return None

        self.huber_reg.fit(X_train, y_train)
        # Same pace assumption for the lap now in progress.
        lap_X = np.array([[curr_lap, self.last_mean_speed]])
        predicted_fuel = self.huber_reg.predict(lap_X)[0]
        laps_to_empty, avg_fuel_consumption_per_lap = self._laps_to_empty(
            curr_lap, self.last_mean_speed
        )
        return laps_to_empty, avg_fuel_consumption_per_lap, predicted_fuel





class LapTimePred:
    """Full lap-time forecasts for the lap in progress, emitted at 25 / 50 / 75 %.

    At each split, features are elapsed time + speed/hv/rpm. On the finish the
    same-lap features are labelled with the measured lap time and the per-split
    Ridge models are refit. Next time the car hits that % , they predict the
    full time of the lap being driven.
    """

    SPLITS = (25, 50, 75)

    def __init__(self):
        self.track: LineString | None = None
        self.point25: MPoint | None = None
        self.point50: MPoint | None = None
        self.point75: MPoint | None = None
        self.split_window = 3
        self.features_this_lap: dict[int, np.ndarray] = {}
        self.taken_this_lap: set[int] = set()
        self.pending_split: int | None = None
        self.X: dict[int, np.ndarray] = {s: np.empty((0, 4)) for s in self.SPLITS}
        self.y: dict[int, np.ndarray] = {s: np.array([]) for s in self.SPLITS}
        self.pipelines = {
            s: make_pipeline(
                StandardScaler(),
                RidgeCV(alphas=np.logspace(-2, 3, 20), cv=None),
            )
            for s in self.SPLITS
        }

    def set_track(self, track):
        self.track = track

    def set_points(self, p25, p50, p75):
        self.point25 = p25
        self.point50 = p50
        self.point75 = p75

    def _raw_perc(self, lon, lat) -> int | None:
        if self.track is None:
            return None
        total_distance = self.track.length
        if total_distance <= 0:
            return None
        perc = int((self.track.project(Point(lon, lat)) / total_distance) * 100)
        return max(0, min(perc, 100))

    def _split_at(self, raw: int) -> int | None:
        for target in self.SPLITS:
            if abs(raw - target) <= self.split_window:
                return target
        return None

    def fill(self, lon: float, lat: float, lap_time: float, speed_kmh: float, hv_perc: float, rpm: float):
        raw = self._raw_perc(lon, lat)
        if raw is None:
            return
        split = self._split_at(raw)
        if split is None or split in self.taken_this_lap:
            return
        self.taken_this_lap.add(split)
        self.features_this_lap[split] = np.array(
            [lap_time, speed_kmh, hv_perc, rpm], dtype=float
        )
        self.pending_split = split

    def _predict_at_split(self, split: int) -> float | None:
        feat = self.features_this_lap.get(split)
        if feat is None:
            return None
        n = len(self.y[split])
        if n >= 2:
            return float(self.pipelines[split].predict(feat.reshape(1, -1))[0])
        if n == 1:
            return float(self.y[split][-1])
        return None

    def process_data(self, is_finish_line_crossed: bool, lap_time: float):
        if is_finish_line_crossed:
            for split, feat in self.features_this_lap.items():
                self.X[split] = np.vstack([self.X[split], feat.reshape(1, -1)])
                self.y[split] = np.append(self.y[split], lap_time)
                if len(self.y[split]) >= 2:
                    self.pipelines[split].fit(self.X[split], self.y[split])
            self.features_this_lap.clear()
            self.taken_this_lap.clear()
            self.pending_split = None
            return None

        if self.pending_split is None:
            return None
        split = self.pending_split
        self.pending_split = None
        predicted = self._predict_at_split(split)
        if predicted is None:
            return None
        return float(predicted), split


class TyreWearPred:
    def __init__(self, good_tyre_laps: int = 3, steer_angle: float = 0.25, min_fit_sample : int = 25,trend_window : int = 3):
        self.laps_with_good_tyre = good_tyre_laps
        self.steer_angle: float = steer_angle
        self.min_fit_sample : int = min_fit_sample
        self.trend_window : int = trend_window
        # MEASURES ON FIRST X LAPS WHEN TYRES ARE IN THE BEST CONDITION
        self.good_features : list[list[float]] = []
        self.good_results : list[float] =  []


        # CURRENT LAP MEASURES
        self.current_lap_features : list[list[float]] = []
        self.current_lap_actual_grip : list[float] = []

        self.lap_num = 0
        self.retention_history : list[tuple[int,float]] = []
        self.last_retention : float | None =  None

        self.is_fitted = False
        self.regressor = lgb.LGBMRegressor(
            n_estimators=100, learning_rate=0.05,max_depth=5,
            random_state=44, verbosity=-1
        )
    def _clear_current_lap(self):
        self.current_lap_features.clear()
        self.current_lap_actual_grip.clear()

    def fill(
        self,
        steer_angle: float,
        speed_kmh: float,
        accel_long: float,
        brake_prs: float,
        throttle: float,
        accel_lat: float,
    ):
        if abs(steer_angle) < self.steer_angle:
            return
        feature_row = [abs(steer_angle), speed_kmh, accel_long, brake_prs, throttle]
        lat_g = abs(accel_lat)
        if self.laps_with_good_tyre > 0:
            self.good_features.append(feature_row)
            self.good_results.append(lat_g)
        else:
            self.current_lap_features.append(feature_row)
            self.current_lap_actual_grip.append(lat_g)

    def _retention_for_finished_lap(self) -> float | None:
        if not self.is_fitted or len(self.current_lap_features) == 0:
            return None
        expected_grip = self.regressor.predict(np.array(self.current_lap_features))
        mean_expected = float(np.mean(expected_grip))
        mean_actual = float(np.mean(self.current_lap_actual_grip))
        if mean_expected <= 0:
            return None
        return max(0.0, min(100.0, (mean_actual / mean_expected) * 100.0))

    def process_data(self, is_finish_line_crossed: bool):
        if not is_finish_line_crossed:
            return None
        self.lap_num += 1
        if self.laps_with_good_tyre > 0:
            self.laps_with_good_tyre -= 1
            if self.laps_with_good_tyre == 0:
                if len(self.good_features) >= self.min_fit_sample:
                    self.regressor.fit(
                        np.array(self.good_features),
                        np.array(self.good_results)
                    )
                    self.is_fitted = True
                else:
                    self.laps_with_good_tyre = 1

        retention_finished = self._retention_for_finished_lap()
        self._clear_current_lap()
        self.last_retention = retention_finished
        if retention_finished is None:
            return None

        self.retention_history.append((self.lap_num,retention_finished))
        window = self.retention_history[-self.trend_window:]
        if len(window) < 3:
            return float(np.clip(retention_finished,0.0,100.0))

        x, y = zip(*window)
        slope, intercept = np.polyfit(x, y, 1)
        predicted = slope * (self.lap_num + 1) + intercept
        return float(np.clip(predicted, 0.0, 100.0))


class BrakeFadePred:
    def __init__(
        self,
        good_brakes_laps: int = 3,
        steer_angle: float = 0.25,
        speed: float = 50.0,
        brake_prs: float = 20.0,
        min_fit_sample: int = 25,
        trend_window: int = 3,
    ):
        self.laps_with_good_brakes = good_brakes_laps
        self.steer_angle = steer_angle
        self.speed = speed
        self.brake_prs = brake_prs
        self.min_fit_sample = min_fit_sample
        self.trend_window = trend_window

        # First X laps: baseline while brakes are still fresh.
        self.good_features: list[list[float]] = []
        self.good_results: list[float] = []

        self.current_lap_features: list[list[float]] = []
        self.current_lap_actual_brake: list[float] = []

        self.lap_num = 0
        self.brake_fade_history: list[tuple[int, float]] = []
        self.last_brake_fade: float | None = None

        self.is_fitted = False
        self.regressor = RandomForestRegressor(
            n_estimators=100,
            random_state=44,
            verbose=0,
            max_depth=5,
        )

    def _clear_current_lap(self):
        self.current_lap_features.clear()
        self.current_lap_actual_brake.clear()

    def fill(
        self,
        steer_angle: float,
        speed_kmh: float,
        accel_long: float,
        brake_prs: float,
        hv_soc: float,
        ambient_temp: float,
    ):
        # Straight-line braking samples only (low steer, enough speed and pressure).
        if abs(steer_angle) > self.steer_angle or speed_kmh < self.speed or brake_prs < self.brake_prs:
            return

        feature_row = [brake_prs, speed_kmh, hv_soc, ambient_temp]
        long_g = abs(accel_long)
        if self.laps_with_good_brakes > 0:
            self.good_features.append(feature_row)
            self.good_results.append(long_g)
        else:
            self.current_lap_features.append(feature_row)
            self.current_lap_actual_brake.append(long_g)

    def _brake_fade_for_finished_lap(self) -> float | None:
        if not self.is_fitted or len(self.current_lap_features) == 0:
            return None
        expected_brake = self.regressor.predict(np.array(self.current_lap_features))
        mean_expected = float(np.mean(expected_brake))
        mean_actual = float(np.mean(self.current_lap_actual_brake))
        if mean_expected <= 0:
            return None
        return max(0.0, min(100.0, (mean_actual / mean_expected) * 100.0))

    def process_data(self, is_finish_line_crossed: bool):
        if not is_finish_line_crossed:
            return None

        self.lap_num += 1

        if self.laps_with_good_brakes > 0:
            self.laps_with_good_brakes -= 1
            if self.laps_with_good_brakes == 0:
                if len(self.good_features) >= self.min_fit_sample:
                    self.regressor.fit(
                        np.array(self.good_features),
                        np.array(self.good_results),
                    )
                    self.is_fitted = True
                else:
                    self.laps_with_good_brakes = 1
            self._clear_current_lap()
            return None

        brake_fade_finished = self._brake_fade_for_finished_lap()
        self._clear_current_lap()
        self.last_brake_fade = brake_fade_finished
        if brake_fade_finished is None:
            return None

        self.brake_fade_history.append((self.lap_num, brake_fade_finished))
        window = self.brake_fade_history[-self.trend_window:]
        if len(window) < self.trend_window:
            # Not enough trend yet: report the just-finished lap as a guess for the next.
            return float(np.clip(brake_fade_finished, 0.0, 100.0))
        x, y = zip(*window)
        slope, intercept = np.polyfit(x, y, 1)
        predicted = slope * (self.lap_num + 1) + intercept
        return float(np.clip(predicted, 0.0, 100.0))



from collections import namedtuple
_feature_row_tuple = namedtuple("FeatureRow", ["oil_temp","oil_temp_delta","rpm","engine_load","speed_kmh","ambient_temp"])
class CarFluidsPred:
    def __init__(self, check_time_s : int = 2,fit_time_s : int = 15,window_length_s : int = 60):
        self.check_time_s : int = check_time_s
        self.window_length_s : int = window_length_s
        self.fit_time_s : int = fit_time_s
        self.window : list = []
        self.window_partial : list = []
        self._last_sample_bucket : int | None = None
        self._last_fit_bucket : int | None = None

        self.time_from_start_ns : float | None = None
        self.pipeline = make_pipeline(StandardScaler(), RidgeCV(alphas=np.logspace(-2, 3, 20), cv=None))


    def set_race_start_time(self, ts):
        self.time_from_start_ns = ts

    def _elapsed_s(self) -> int:
        return int((time.time_ns() - self.time_from_start_ns) / 1_000_000_000)

    def _horizon_steps(self) -> int:
        return max(1, self.window_length_s // self.check_time_s)

    def fill(self, oil_temp : float, rpm : float, engine_load : float, speed_kmh : float, ambient_temp : float ):
        # Bucket by check_time_s so each interval contributes one averaged row
        # (modulo-on-elapsed fired on every message during that second).
        sample_bucket = self._elapsed_s() // self.check_time_s
        delta = oil_temp - self.window[-1].oil_temp if self.window else 0.0
        row = _feature_row_tuple(
            oil_temp=oil_temp,
            oil_temp_delta=delta,
            rpm=rpm,
            engine_load=engine_load,
            speed_kmh=speed_kmh,
            ambient_temp=ambient_temp,
        )
        if self._last_sample_bucket is not None and sample_bucket == self._last_sample_bucket:
            self.window_partial.append(row)
            return
        if self.window_partial:
            self.window.append(_feature_row_tuple(
                oil_temp=float(np.mean([o.oil_temp for o in self.window_partial])),
                oil_temp_delta=float(np.mean([o.oil_temp_delta for o in self.window_partial])),
                rpm=float(np.mean([o.rpm for o in self.window_partial])),
                engine_load=float(np.mean([o.engine_load for o in self.window_partial])),
                speed_kmh=float(np.mean([o.speed_kmh for o in self.window_partial])),
                ambient_temp=float(np.mean([o.ambient_temp for o in self.window_partial])),
            ))
            self.window_partial.clear()
            max_rows = 2 * self._horizon_steps() + 1
            if len(self.window) > max_rows:
                self.window = self.window[-max_rows:]
        self.window_partial.append(row)
        self._last_sample_bucket = sample_bucket

    def process_data(self,oil_temp : float, rpm : float, engine_load : float, speed_kmh : float, ambient_temp : float ):
        horizon_steps = self._horizon_steps()
        # Need past rows paired with oil_temp horizon_steps ahead (= window_length_s).
        if len(self.window) <= horizon_steps or len(self.window) < self.window_length_s:
            return None
        fit_bucket = self._elapsed_s() // self.fit_time_s
        if self._last_fit_bucket is not None and fit_bucket == self._last_fit_bucket:
            return None
        self._last_fit_bucket = fit_bucket

        end_i = len(self.window) - horizon_steps
        start_i = max(0, end_i - horizon_steps)
        X_train = np.array([
            [row.oil_temp, row.oil_temp_delta, row.rpm, row.engine_load, row.speed_kmh, row.ambient_temp]
            for row in self.window[start_i:end_i]
        ])
        y_train = np.array([
            self.window[i + horizon_steps].oil_temp for i in range(start_i, end_i)
        ])
        self.pipeline.fit(X_train, y_train)
        delta = oil_temp - self.window[-1].oil_temp if self.window else 0.0
        next_oil_temp = self.pipeline.predict(
            np.array([[oil_temp, delta, rpm, engine_load, speed_kmh, ambient_temp]])
        )[0]
        return next_oil_temp


# TODO COS Z PREDYKCJA FANCY STALOSCI KIEROWCY czy np nie wymienic bo coraz gorzej jediz ewolniej itd