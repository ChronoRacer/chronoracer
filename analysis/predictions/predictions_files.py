
from analysis.predictions.prediction_models import LackOfFuelPred, LapTimePred, TyreWearPred


class PredictionClass:
    def __init__(self, name: str, sensors: set[str]):
        self.name = name
        self.sensors = sensors
        self.is_prepared = False

    def prepare(self, context: dict) -> None:
        pass

    def need_prepare(self) -> bool:
        return not self.is_prepared

    def fill(self, context: dict):
        pass

    def process_data(self, context: dict):
        pass


class FuelPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — projected fuel level and laps to empty",
            sensors={"fuel_l"},
        )
        self.model = LackOfFuelPred()

    def prepare(self, context: dict) -> None:
        if self.is_prepared:
            return
        point = context.get("point")
        if point is None:
            return
        self.model.set_start_point(point)
        if self.model.start_point:
            self.is_prepared = True

    def fill(self, context: dict):
        self.model.fill(
            context["is_finish_line_crossed"],
            context["curr_lap"],
            context["fuel"]
        )
    def process_data(self, context: dict):
        return self.model.process_data(
            context["is_finish_line_crossed"],
            context["curr_lap"],
        )


class LapTimePrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — time predicted from previous lap split performance",
            sensors={"speed", "hv_soc", "rpm"},
        )
        self.model = LapTimePred()

    def prepare(self, context: dict) -> None:
        if self.is_prepared:
            return
        p25 = context.get("p25")
        p50 = context.get("p50")
        p75 = context.get("p75")
        track = context.get("track")
        if p25 is None or p50 is None or p75 is None or track is None:
            return
        self.model.set_points(p25, p50, p75)
        self.model.set_track(track)
        self.is_prepared = True

    def fill(self, context: dict):
        self.model.fill(
            context["lon"],
            context["lat"],
            context["lap_time"],
            context["speed"],
            context["hv_perc"],
            context["rpm"],
        )

    def process_data(self, context: dict):
        return self.model.process_data(
            context["is_finish_line_crossed"],
            context["lap_time"]
        )

class TyreWearPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap - tyre grip retention vs fresh tyre baseline",
            sensors={
                "steer_angle",
                "speed",
                "accel_long",
                "accel_lat",
                "brake_prs",
                "throttle",
            },
        )
        self.model = TyreWearPred()

    def prepare(self, context: dict) -> None:
        if self.is_prepared:
            return
        self.is_prepared = True

    def fill(self, context: dict):
        self.model.fill(
            context["steer_angle"],
            context["speed"],
            context["accel_long"],
            context["brake_prs"],
            context["throttle"],
            context["accel_lat"],
        )

    def process_data(self, context: dict):
        return self.model.process_data(context["is_finish_line_crossed"])
