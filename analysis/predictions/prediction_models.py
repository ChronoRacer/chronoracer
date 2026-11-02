import math
import threading

import numpy as np
import pyproj
from shapely.geometry.linestring import LineString
from shapely import transform as shapely_transform
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import HuberRegressor
from pydantic import BaseModel
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV
from sklearn.mixture import BayesianGaussianMixture
from shapely.geometry import Point
import lightgbm as lgb
import time
import matplotlib.pyplot as plt

# GPS aggregation cadence can change, so elapsed time must always come from
# GPS timestamps instead of an assumed samples-per-second value.
START_TIME_NS = 1_500_000_000
RADIUS_M = 8.0
EXIT_RADIUS_M = 20.0
EARTH_RADIUS_M = 6_371_000.0


class MPoint(BaseModel):
    lat: float
    lon: float


class MTurn(BaseModel):
    middle: MPoint
    radius_m: float
    is_left_turn: bool
    s_m: float  # along-track distance on track_meters (EPSG:3857)

class LapDetector:
    def __init__(self,
                 gps_lat: str,
                 gps_lon: str,
                 turn_tolerance: float = 2.0,
                 sharp_turn_deg : float = 10.0,
                 turn_radius : float = 120.0,
                 distance_from_turns : float = 70.0
                 ):
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
        self.turn_tolerance : float = turn_tolerance
        self.sharp_turn_deg : float = sharp_turn_deg
        self.distance_from_turns : float = distance_from_turns
        self.turn_radius = turn_radius
        self.turns_lst : list[MTurn] | None  = None
        self.track_meters: LineString | None = None
        self._project_to_meters = None
        self.curr_lap_num = 0
        self.lap_start_ts_ns: int | None = None
        self.lap_time : float = 0.0


    def is_start_set(self) -> bool:
        return self.lat_start is not None and self.lon_start is not None

    def is_turns_set(self):
        return self.turns_lst is not None and len(self.turns_lst) > 0

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

        project_to_meters = pyproj.Transformer.from_crs(
            "EPSG:4326", "EPSG:3857", always_xy=True
        )

        project_to_wgs84 = pyproj.Transformer.from_crs(
            "EPSG:3857", "EPSG:4326", always_xy=True
        )

        track_wgs84 = LineString(self.track_lon_lat)
        track_meters = shapely_transform(track_wgs84, project_to_meters.transform, interleaved=False)

        p25_meters = track_meters.interpolate(0.25, normalized=True)
        p50_meters = track_meters.interpolate(0.50, normalized=True)
        p75_meters = track_meters.interpolate(0.75, normalized=True)

        p25_wgs84 = shapely_transform(p25_meters, project_to_wgs84.transform, interleaved=False, )
        p50_wgs84 = shapely_transform(p50_meters, project_to_wgs84.transform, interleaved=False, )
        p75_wgs84 = shapely_transform(p75_meters, project_to_wgs84.transform, interleaved=False, )

        self.track = track_wgs84
        self.track_meters = track_meters
        self._project_to_meters = project_to_meters
        self.p25 = MPoint(lat=p25_wgs84.y, lon=p25_wgs84.x)
        self.p50 = MPoint(lat=p50_wgs84.y, lon=p50_wgs84.x)
        self.p75 = MPoint(lat=p75_wgs84.y, lon=p75_wgs84.x)

        now = time.time_ns()
        #thread = threading.Thread(target=self._determine_turns,args=(track_meters,project_to_wgs84))
        #thread.start()
        self._determine_turns(track_meters,project_to_wgs84)
        print("LICZBA ZAKRETÓW:", len(self.turns_lst) if self.turns_lst is not None else 0)
        return self.track, self.p25, self.p50, self.p75

    def _get_turn_radius(self,ptA,ptB,ptC) -> float:
        ax, ay = ptA
        bx, by = ptB
        cx, cy = ptC

        a = math.hypot(cx - bx, cy - by)
        b = math.hypot(cx - ax, cy - ay)
        c = math.hypot(bx - ax, by - ay)

        area = 0.5 * abs((bx - ax) * (cy - ay) - (by - ay) * (cx - ax))
        if area < 1e-6:
            return float('inf')

        return (a * b * c) / (4 * area)

    def _estimate_thresholds_bgm(self, angles: list[float], radii: list[float]) -> tuple[float, float]:
        if len(angles) < 6:
            return self.sharp_turn_deg, self.turn_radius

        try:
            X_ang = np.array(angles).reshape(-1, 1)
            bgm_ang = BayesianGaussianMixture(n_components=2, random_state=42, max_iter=200)
            bgm_ang.fit(X_ang)
            ang_means = bgm_ang.means_.flatten()

            straight_ang_idx = int(np.argmin(ang_means))
            turn_ang_idx = int(np.argmax(ang_means))
            ang_thresh = float((ang_means[straight_ang_idx] + ang_means[turn_ang_idx]) / 2.0)

            finite_radii = [r for r in radii if 0 < r < 2000]
            if len(finite_radii) >= 6:
                X_rad = np.array(finite_radii).reshape(-1, 1)
                bgm_rad = BayesianGaussianMixture(n_components=2, random_state=42, max_iter=200)
                bgm_rad.fit(X_rad)
                rad_means = bgm_rad.means_.flatten()
                turn_rad_idx = int(np.argmin(rad_means))      # smaller radius = corners
                straight_rad_idx = int(np.argmax(rad_means))  # larger radius = straights
                rad_thresh = float((rad_means[turn_rad_idx] + rad_means[straight_rad_idx]) / 2.0)
            else:
                rad_thresh = self.turn_radius

            ang_thresh = float(np.clip(ang_thresh, 5.0, 35.0))
            rad_thresh = float(np.clip(rad_thresh, 40.0, 200.0))

            return ang_thresh, rad_thresh
        except Exception:
            return self.sharp_turn_deg, self.turn_radius

    def _determine_turns(self,track_meters,meters_to_wgs84):
        clean_track = track_meters.simplify(tolerance=self.turn_tolerance)
        points = list(clean_track.coords)

        # Pre-pass: collect all vertex geometric features across the track
        vertex_stats = []
        all_angles = []
        all_radii = []

        for i in range(1, len(points) - 1):
            point_A = points[i - 1]
            point_B = points[i]
            point_C = points[i + 1]

            dx_in = point_B[0] - point_A[0]
            dy_in = point_B[1] - point_A[1]
            direction_in = math.atan2(dy_in, dx_in)

            dx_out = point_C[0] - point_B[0]
            dy_out = point_C[1] - point_B[1]
            direction_out = math.atan2(dy_out, dx_out)

            angle_deg = math.degrees(direction_out - direction_in)
            angle_deg = (angle_deg + 180) % 360 - 180
            radius = self._get_turn_radius(point_A, point_B, point_C)

            vertex_stats.append((point_B, angle_deg, radius))
            all_angles.append(abs(angle_deg))
            if radius != float('inf'):
                all_radii.append(radius)

        # Estimate dynamic thresholds via 2-component Bayesian Gaussian Mixture (BGM)
        bgm_sharp_deg, bgm_turn_radius = self._estimate_thresholds_bgm(all_angles, all_radii)

        # 1. Collect candidates matching the BGM-calibrated thresholds
        candidates = []
        for point_B, angle_deg, radius in vertex_stats:
            if abs(angle_deg) >= bgm_sharp_deg and radius <= bgm_turn_radius:
                middle_point = Point(point_B)
                s_dist = track_meters.project(middle_point)
                candidates.append({
                    "point": middle_point,
                    "s_dist": s_dist,
                    "radius": radius,
                    "angle_deg": angle_deg,
                })

        # 2. Cluster candidates closer than distance_from_turns and pick true Apex (min radius)
        if candidates:
            # Sort along track distance
            candidates.sort(key=lambda c: c["s_dist"])
            clusters = []
            curr_cluster = [candidates[0]]

            for cand in candidates[1:]:
                # If within distance threshold of the cluster, belong to the same corner complex
                if (cand["s_dist"] - curr_cluster[-1]["s_dist"]) <= self.distance_from_turns:
                    curr_cluster.append(cand)
                else:
                    clusters.append(curr_cluster)
                    curr_cluster = [cand]
            if curr_cluster:
                clusters.append(curr_cluster)

            self.turns_lst = []
            for cluster in clusters:
                # Motorsport Apex: the point with minimum radius (tightest apex, slowest speed)
                apex = min(cluster, key=lambda c: c["radius"])
                p_wgs84 = shapely_transform(apex["point"], meters_to_wgs84.transform, interleaved=False)
                k = math.cos(math.radians(p_wgs84.y))  # skala 3857 -> prawdziwe metry
                self.turns_lst.append(
                    MTurn(
                        middle=MPoint(lat=p_wgs84.y, lon=p_wgs84.x),
                        radius_m=apex["radius"] * k,
                        is_left_turn=apex["angle_deg"] > 0,
                        s_m=float(apex["s_dist"]) * k,  # przybliżenie, wystarczy dla toru o rozmiarze kilku km
                    )
                )

        xs, ys = self.track.xy
        """
        
        plt.figure()
        plt.plot(xs, ys)  # linia toru
        # zakręty (mogą być puste gdy żaden kąt nie przekracza sharp_turn_deg):
        if self.turns_lst:
            for i, turn in enumerate(self.turns_lst):
                plt.scatter(turn.middle.lon, turn.middle.lat, c="red", zorder=3)
                plt.annotate(f"Turn {i+1}\nR={turn.radius_m:.0f}m", (turn.middle.lon, turn.middle.lat), textcoords="offset points", xytext=(0,10), ha='center', fontsize=8)
        plt.scatter(self.lon_start,self.lat_start, c="green", zorder=3)
        plt.xlabel("lon")
        plt.ylabel("lat")
        plt.axis("equal")
        plt.savefig("track_with_turns2.png", dpi=300)
        plt.close()
        
        """
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
        good_brakes_laps: int = 2,
        steer_angle: float = 20.0,
        speed: float = 50.0,
        brake_prs: float = 20.0,
        min_fit_sample: int = 20,
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
        # Keep only mostly straight, meaningful braking samples.
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
        self.pipeline = RandomForestRegressor()


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
            max_rows = 3 * self._horizon_steps() + 1
            if len(self.window) > max_rows:
                self.window = self.window[-max_rows:]
        self.window_partial.append(row)
        self._last_sample_bucket = sample_bucket

    def process_data(self,oil_temp : float, rpm : float, engine_load : float, speed_kmh : float, ambient_temp : float ):
        horizon_steps = self._horizon_steps()
        # Need one full horizon of input rows and their future-temperature labels.
        if len(self.window) < 2 * horizon_steps:
            return None
        fit_bucket = self._elapsed_s() // self.fit_time_s
        if self._last_fit_bucket is not None and fit_bucket == self._last_fit_bucket:
            return None
        self._last_fit_bucket = fit_bucket

        end_i = len(self.window) - horizon_steps
        start_i = 0
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



_coolant_feature_row = namedtuple(
    "CoolantFeatureRow",
    ["coolant_temp", "coolant_delta", "oil_temp", "rpm", "engine_load", "speed_kmh", "ambient_temp"],
)


class CoolantTempPred:
    """Forecast coolant temperature from recent thermal and driving conditions."""

    def __init__(self, check_time_s: int = 2, fit_time_s: int = 15, horizon_s: int = 60):
        self.check_time_s = check_time_s
        self.fit_time_s = fit_time_s
        self.horizon_s = horizon_s
        self.window: list = []
        self.window_partial: list = []
        self.last_sample_bucket: int | None = None
        self.last_fit_bucket: int | None = None
        self.time_from_start_ns: int | None = None
        self.regressor = RandomForestRegressor()

    def set_race_start_time(self, ts_ns: int):
        self.time_from_start_ns = ts_ns

    def _elapsed_s(self) -> int:
        return int((time.time_ns() - self.time_from_start_ns) / 1_000_000_000)

    def _horizon_steps(self) -> int:
        return max(1, self.horizon_s // self.check_time_s)

    def fill(
        self,
        coolant_temp: float,
        oil_temp: float,
        rpm: float,
        engine_load: float,
        speed_kmh: float,
        ambient_temp: float,
    ):
        sample_bucket = self._elapsed_s() // self.check_time_s
        previous_temp = self.window[-1].coolant_temp if self.window else coolant_temp
        row = _coolant_feature_row(
            coolant_temp=float(coolant_temp),
            coolant_delta=float(coolant_temp) - previous_temp,
            oil_temp=float(oil_temp),
            rpm=float(rpm),
            engine_load=float(engine_load),
            speed_kmh=float(speed_kmh),
            ambient_temp=float(ambient_temp),
        )
        if self.last_sample_bucket == sample_bucket:
            self.window_partial.append(row)
            return

        if self.window_partial:
            self.window.append(_coolant_feature_row(*(
                float(np.mean([sample[i] for sample in self.window_partial]))
                for i in range(len(_coolant_feature_row._fields))
            )))
            self.window_partial.clear()
            max_rows = 3 * self._horizon_steps() + 1
            if len(self.window) > max_rows:
                self.window = self.window[-max_rows:]

        self.window_partial.append(row)
        self.last_sample_bucket = sample_bucket

    def process_data(
        self,
        coolant_temp: float,
        oil_temp: float,
        rpm: float,
        engine_load: float,
        speed_kmh: float,
        ambient_temp: float,
    ) -> float | None:
        horizon_steps = self._horizon_steps()
        if len(self.window) < 2 * horizon_steps:
            return None

        fit_bucket = self._elapsed_s() // self.fit_time_s
        if self.last_fit_bucket == fit_bucket:
            return None
        self.last_fit_bucket = fit_bucket

        end_i = len(self.window) - horizon_steps
        feature_names = _coolant_feature_row._fields
        x_train = np.array([
            [getattr(row, name) for name in feature_names]
            for row in self.window[:end_i]
        ])
        y_train = np.array([
            self.window[i + horizon_steps].coolant_temp for i in range(end_i)
        ])
        self.regressor.fit(x_train, y_train)

        current_row = _coolant_feature_row(
            coolant_temp=float(coolant_temp),
            coolant_delta=(
                float(coolant_temp) - self.window[-1].coolant_temp
                if self.window else 0.0
            ),
            oil_temp=float(oil_temp),
            rpm=float(rpm),
            engine_load=float(engine_load),
            speed_kmh=float(speed_kmh),
            ambient_temp=float(ambient_temp),
        )
        forecast_temp = float(self.regressor.predict(np.array([[getattr(current_row, name) for name in feature_names]]))[0])
        return forecast_temp


class SpeedOnTurnsPred:
    def __init__(self,
                 default_max_g : float = 0.85,
                 zone_m : float = 35,
                 hysteresis_m : float = 5
                 ):
        self.default_max_lat_accel: float = default_max_g * 9.81
        self.learned_lat_accels : list[float] = []
        self.max_stable_lat_accel : float = self.default_max_lat_accel

        self.zone_m: float = zone_m
        self.hysteresis_m: float = hysteresis_m

        self.turns_lst: list[MTurn] | None = None
        self.track_meters : LineString | None = None
        self.project_to_meters = None
        self.track_length_m : float  = 0.0

        self.visited_turns: set[int] = set()
        self.active : int | None = None
        self.samples : list[tuple[float,float,float]] = []


    def set_turns(self, turns: list[MTurn], track_meters, project_to_meters):
        self.turns_lst = turns
        self.track_meters = track_meters
        self.project_to_meters = project_to_meters
        self.track_length_m = track_meters.length if track_meters is not None else 0.0
        self.reset_lap()

    def reset_lap(self):
        self.visited_turns.clear()
        self.active = None
        self.samples = []

    def _dist2d(self, turn: MTurn, lon : float, lat : float) :
        n , e = LapDetector._local_xy(turn.middle, MPoint(lat=lat, lon=lon))
        return math.hypot(n,e)
    def _dist1d(self, car_s : float,turn_s : float):
        d = abs(car_s - turn_s)
        return min(d, self.track_length_m - d)

    def _update_grip_limit(self, lat_accel_samples : list):
        high_g_samples =[g for g in lat_accel_samples]
        if not high_g_samples:
            return

        self.learned_lat_accels.extend(high_g_samples)

        if len(self.learned_lat_accels) > 1000:
            self.learned_lat_accels = self.learned_lat_accels[-1000:]

        self.max_stable_lat_accel = float(np.percentile(self.learned_lat_accels, 95))


    def _finalize(self):
        idx, samples = self.active, self.samples
        self.visited_turns.add(idx)
        self.active, self.samples = None, []
        if len(samples) < 3:
            return None,None,None

        turn = self.turns_lst[idx]

        samples.sort(key=lambda s: s[0])
        nearest_samples = samples[:5]

        apex_speed = float(np.median([s[1] for s in nearest_samples]))
        apex_lat_accels = [s[2] for s in nearest_samples]

        self._update_grip_limit(apex_lat_accels)

        max_safe_kmh = math.sqrt(self.max_stable_lat_accel * turn.radius_m)*3.6
        return max_safe_kmh, apex_speed, idx + 1

    def process_data(self, lon, lat, speed_kmh, accel_lat):
        if not self.turns_lst:
            return None, None, None

        x,y = self.project_to_meters.transform(lon, lat)
        car_s = self.track_meters.project(Point(x,y))
        abs_accel_lat =  abs(accel_lat)

        if self.active is not None:
            turn = self.turns_lst[self.active]

            d_s = self._dist1d(car_s, turn.s_m)

            if d_s > self.zone_m + self.hysteresis_m:
                return self._finalize()

            d_2d = self._dist2d(turn, lon, lat)
            self.samples.append((d_2d, speed_kmh, abs_accel_lat))
            return None, None, None

        best, best_d_s = None, float("inf")
        for idx, turn in enumerate(self.turns_lst):
            if idx in self.visited_turns:
                continue

            d_s = self._dist1d(car_s, turn.s_m)
            if d_s < best_d_s:
                best, best_d_s = idx, d_s

        if best is not None and best_d_s <= self.zone_m:
            self.active = best
            d_2d = self._dist2d(self.turns_lst[best], lon, lat)
            self.samples.append((d_2d, speed_kmh, abs_accel_lat))

        return None, None, None

        return None, None, None
        """
        if not self.turns_lst or self.track is None:
            return None

        car_perc = self._car_perc(lon, lat)
        if car_perc is None:
            return None

        for idx, (turn, turn_perc) in enumerate(zip(self.turns_lst, self._turn_percs)):
            if idx in self.visited_turns:
                continue

            if self._perc_delta(car_perc, turn_perc) > self.ZONE_PERC:
                continue

            self.visited_turns.add(idx)

            max_safe_kmh = math.sqrt(self.friction * self.gravity * turn.radius_m) * 3.6
            return max_safe_kmh, speed_kmh
        return None, None
        """


LapCompareResult = namedtuple(
    "LapCompareResult",
    [
        "lap_num",
        "lap_time",
        "avg_speed",
        "hard_brakes",
        "hv_soc_delta",
        "delta_lap_time",
        "delta_best_lap_time",
        "delta_lap_speed",
        "delta_best_lap_speed",
        "delta_hard_brakes",
        "delta_hv_soc",
        "best_lap_num",
        "best_lap_time",
    ],
)


class LapComparePred:

    def __init__(self, hard_brake_thres: float = 50.0):
        self.hard_brake_thres = hard_brake_thres

        self.last_lap_time: float | None = None
        self.last_lap_num: int | None = None
        self.last_lap_speed: float | None = None
        self.last_brakes_count: int | None = None
        self.last_hv_delta: float | None = None

        self.best_lap_time: float | None = None
        self.best_lap_num: int | None = None
        self.best_lap_speed: float | None = None

        self._speed_partial_lst: list[float] = []
        self.curr_brakes_count: int = 0
        self._brake_active: bool = False
        self._hv_soc_start: float | None = None

    def _avg_speed(self) -> float:
        if not self._speed_partial_lst:
            return 0.0
        return float(np.mean(self._speed_partial_lst))

    def _note_sample(self, speed_kmh: float, brake_prs: float, hv_soc: float) -> None:
        self._speed_partial_lst.append(speed_kmh)
        if self._hv_soc_start is None:
            self._hv_soc_start = hv_soc
        hard = brake_prs > self.hard_brake_thres
        if hard and not self._brake_active:
            self.curr_brakes_count += 1
        self._brake_active = hard

    def _reset_lap_accumulators(self, hv_soc: float) -> None:
        self._speed_partial_lst.clear()
        self.curr_brakes_count = 0
        self._brake_active = False
        self._hv_soc_start = hv_soc

    def _lap_score_func(
            self,
            result: LapCompareResult,
            w_time_rel: float = 150.0,
            w_brakes: float = 2.5,
            w_soc: float = 3.0,
            w_trend: float = 2.0,
            best_lap_bonus: float = 10,
    ) -> float:
        if result.best_lap_time <= 0 or result.lap_time <= 0:
            return 0.0
        score = 100.0

        time_deficit = max(0.0, result.delta_best_lap_time)
        relative_time_loss = time_deficit / result.best_lap_time
        score -= w_time_rel * relative_time_loss

        score -= w_brakes * max(0, result.hard_brakes)

        if result.hv_soc_delta < 0:
            soc_consumed = abs(result.hv_soc_delta)
            score -= w_soc * soc_consumed

        if result.lap_num == result.best_lap_num or time_deficit <= 1e-4:
            score += best_lap_bonus

        if result.delta_lap_time < 0:
            score += w_trend * abs(result.delta_lap_time)
        else:
            score -= (w_trend * 0.5) * result.delta_lap_time

        return round(score, 2)
    def process_data(
        self,
        is_finish_line_crossed: bool,
        lap_time: float,
        lap_num: int,
        speed_kmh: float,
        hv_soc: float,
        brake_prs: float,
    ) -> LapCompareResult | None:
        # Accumulate every tick; emit a card only when the finish line is crossed.
        if not is_finish_line_crossed:
            self._note_sample(speed_kmh, brake_prs, hv_soc)
            return None

        avg_speed = self._avg_speed()
        hard_brakes = self.curr_brakes_count
        hv_soc_delta = (
            hv_soc - self._hv_soc_start if self._hv_soc_start is not None else 0.0
        )
        # LapDetector increments curr_lap on crossing; card is for the lap just finished.
        finished_lap_num = max(1, lap_num - 1)

        if self.last_lap_time is None:
            self.last_lap_time = lap_time
            self.last_lap_num = finished_lap_num
            self.last_lap_speed = avg_speed
            self.last_brakes_count = hard_brakes
            self.last_hv_delta = hv_soc_delta
            self.best_lap_time = lap_time
            self.best_lap_num = finished_lap_num
            self.best_lap_speed = avg_speed
            self._reset_lap_accumulators(hv_soc)
            return None

        delta_lap_time = lap_time - self.last_lap_time
        delta_best_lap_time = lap_time - self.best_lap_time
        delta_lap_speed = avg_speed - self.last_lap_speed
        delta_best_lap_speed = avg_speed - self.best_lap_speed
        delta_hard_brakes = hard_brakes - (self.last_brakes_count or 0)
        delta_hv_soc = hv_soc_delta - (self.last_hv_delta if self.last_hv_delta is not None else 0.0)

        result = LapCompareResult(
            lap_num=finished_lap_num,
            lap_time=lap_time,
            avg_speed=avg_speed,
            hard_brakes=hard_brakes,
            hv_soc_delta=hv_soc_delta,
            delta_lap_time=delta_lap_time,
            delta_best_lap_time=delta_best_lap_time,
            delta_lap_speed=delta_lap_speed,
            delta_best_lap_speed=delta_best_lap_speed,
            delta_hard_brakes=delta_hard_brakes,
            delta_hv_soc=delta_hv_soc,
            best_lap_num=self.best_lap_num,
            best_lap_time=self.best_lap_time,
        )

        self.last_lap_time = lap_time
        self.last_lap_num = finished_lap_num
        self.last_lap_speed = avg_speed
        self.last_brakes_count = hard_brakes
        self.last_hv_delta = hv_soc_delta

        if lap_time < self.best_lap_time:
            self.best_lap_time = lap_time
            self.best_lap_num = finished_lap_num
            self.best_lap_speed = avg_speed

        self._reset_lap_accumulators(hv_soc)
        return result
