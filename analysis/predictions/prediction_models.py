import math
import numpy as np
from sklearn.linear_model import HuberRegressor
from pydantic import BaseModel


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
        self.curr_lap_num = 0

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

    def find_next_lap(self, test_point: MPoint) -> MPoint | None:
        start_point = self.get_start_point()
        if start_point is None:
            return None

        previous_point = self.previous_point
        self.previous_point = test_point
        if previous_point is None:
            return None

        north_m, east_m = self._local_xy(start_point, test_point)
        if not self.departed_from_start:
            if math.hypot(north_m, east_m) >= EXIT_RADIUS_M:
                self.departed_from_start = True
            return None

        if self._segment_reaches_start(start_point, previous_point, test_point):
            self.departed_from_start = False
            self.curr_lap_num += 1
            return test_point
        return None



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
