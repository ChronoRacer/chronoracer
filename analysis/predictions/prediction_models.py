import math
import numpy as np
from shapely.geometry.linestring import LineString
from sklearn.linear_model import HuberRegressor
from pydantic import BaseModel
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV
from shapely.geometry import  Point

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
        self.laps_arr: np.ndarray = np.array([])
        self.fuels_arr: np.ndarray = np.array([])
        self.initial_laps: int = initial_laps
        self.initial_laps_const = initial_laps
        self.start_point: MPoint | None = None  # Point(Lat,Lon) on the track
        self.huber_reg = HuberRegressor(max_iter=100)

    def set_start_point(self, point: MPoint):
        self.start_point = point

    def _laps_to_empty(self, curr_lap : int):
        if not hasattr(self.huber_reg, "coef_"):
            return None,None

        a = self.huber_reg.coef_[0] # negative slope
        b = self.huber_reg.intercept_

        if a >= 0:
            return None,None
        target_lap = -b/a

        return max(0,target_lap - curr_lap), abs(a)

    def fill(self, is_finish_line_crossed: bool, curr_lap : int, fuel: float):
        if self.start_point is None:
            return

        if not is_finish_line_crossed:
            return

        if len(self.fuels_arr) > 0 and fuel > self.fuels_arr[-1]:
            self.laps_arr = np.array([])
            self.fuels_arr = np.array([])
            self.initial_laps = self.initial_laps_const

        self.laps_arr = np.append(self.laps_arr, [curr_lap])
        self.fuels_arr = np.append(self.fuels_arr, [fuel])


    def process_data(self, is_finish_line_crossed: bool, curr_lap : int):
        if self.start_point is None:
            return None

        if not is_finish_line_crossed:
            return None

        self.initial_laps -= 1
        if self.initial_laps  > 0:
            return None
        X_train = self.laps_arr.reshape(-1, 1)
        y_train = self.fuels_arr

        self.huber_reg.fit(X_train, y_train)
        next_lap_X = np.array([[curr_lap + 1]])
        predicted_fuel = self.huber_reg.predict(next_lap_X)[0]
        laps_to_empty,avg_fuel_consumption_per_lap = self._laps_to_empty(curr_lap)
        return laps_to_empty,avg_fuel_consumption_per_lap,predicted_fuel





class LapTimePred:
    def __init__(self):
        self.track : LineString | None =  None
        self.point25 : MPoint | None   = None # lon lat
        self.point50 : MPoint | None   = None # lon lat
        self.point75 : MPoint | None   = None # lon lat
        self.lap_times_s : np.ndarray = np.array([])
        self.features_25_50_75_temp: dict[int,np.ndarray] = {}
        self.features_25_50_75 = np.empty((0, 12))
        self.pending_features: np.ndarray | None = None
        self.taken_this_lap: set[int] = set()
        self.split_window = 3

        self.pipeline = make_pipeline(StandardScaler(), RidgeCV(alphas=np.logspace(-2, 3, 20), cv=None))

    def set_track(self,track):
        self.track = track

    def set_points(self,p25,p50,p75):
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
        for target in (25, 50, 75):
            if abs(raw - target) <= self.split_window:
                return target
        return None

    def fill(self,lon : float, lat : float, lap_time:int, speed_kmh:float, hv_perc:float, rpm:int):
        raw = self._raw_perc(lon, lat)
        if raw is None:
            return
        split = self._split_at(raw)
        if split is None or split in self.taken_this_lap:
            return
        self.taken_this_lap.add(split)
        self.features_25_50_75_temp[split] = np.array([
            lap_time,speed_kmh,hv_perc,rpm
        ])

    def process_data(self, is_finish_line_crossed: bool, lap_time: int):
        if not is_finish_line_crossed:
            return None

        have_splits = (
            25 in self.features_25_50_75_temp
            and 50 in self.features_25_50_75_temp
            and 75 in self.features_25_50_75_temp
        )
        lap_time_pred = None
        if have_splits:
            curr = np.concatenate([
                self.features_25_50_75_temp[25],
                self.features_25_50_75_temp[50],
                self.features_25_50_75_temp[75]
            ]).reshape(1,-1)
            if self.pending_features is not None:
                self.features_25_50_75 = np.vstack([
                    self.features_25_50_75,
                    self.pending_features,
                ])
                self.lap_times_s = np.append(self.lap_times_s, lap_time)
                if len(self.lap_times_s) >= 2:
                    self.pipeline.fit(self.features_25_50_75, self.lap_times_s)
                    lap_time_pred = float(self.pipeline.predict(curr)[0])
                else:
                    lap_time_pred = float(self.lap_times_s[-1])
            self.pending_features = curr

        self.features_25_50_75_temp.clear()
        self.taken_this_lap.clear()
        return lap_time_pred


# ALBO ZMIANA ALO NOWE ZE WLASNIE W 50% TORU LICZE TYLKO KIEDY DOJADE DO 100%
# TODO COS Z PREDYKCJA FANCY STALOSCI KIEROWCY czy np nie wymienic bo coraz gorzej jediz ewolniej itd