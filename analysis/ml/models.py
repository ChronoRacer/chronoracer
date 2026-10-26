import math

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
        self.lap_counter = 0

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
        self.lap_counter = 1
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
            self.lap_counter += 1
            return test_point
        return None
