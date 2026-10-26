
from analysis.predictions.prediction_models import LackOfFuelPred


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


class FuelPred(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Fuel level prediction - for next lap",
            sensors={"fuel_l"},
        )
        self.model = LackOfFuelPred()

    def prepare(self, context: dict) -> None:
        if self.is_prepared:
            return
        self.model.set_start_point(context["point"])
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


class LapTimePred(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Lap time prediction - for next lap",
            sensors={"speed", "hv_soc", "rpm"},
        )


class TyreWearPred(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Tyre wear prediction - for next lap",
            sensors={
                "steer_angle",
                "speed",
                "accel_long",
                "accel_lat",
                "brake_prs",
                "throttle",
            },
        )
