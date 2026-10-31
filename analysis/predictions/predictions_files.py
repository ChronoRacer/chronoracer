from __future__ import annotations
from loguru import logger

from analysis.predictions.prediction_models import (
    BrakeFadePred,
    CarFluidsPred,
    LackOfFuelPred,
    LapTimePred,
    TyreWearPred,
    SpeedOnTurnsPred,
)


class RunResult:
    """Value returned by ``PredictionClass.run`` when there is something to send."""

    def __init__(self, value: float, instruction: str):
        self.value = value
        self.instruction = instruction


class PredictionClass:
    def __init__(self, name: str, sensors: set[str]):
        self.name = name
        self.sensors = sensors
        self.is_prepared = False

    def need_prepare(self) -> bool:
        return not self.is_prepared

    # Subclasses override this with prediction-specific logic.
    # ``signals`` maps sensor_id -> Value(ts_ns, val).
    # ``lap_ctx`` carries lap detector state shared by every prediction.
    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        return None


class FuelPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — projected fuel level and laps to empty",
            sensors={"fuel_l", "speed"},
        )
        self.model = LackOfFuelPred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        fuel = signals.get("fuel_l")
        speed = signals.get("speed")
        if fuel is None or speed is None:
            logger.warning("Fuel or speed signal missing, skipping fuel prediction")
            return None

        if self.need_prepare():
            point = lap_ctx.get("start_point")
            if point is None:
                return None
            self.model.set_start_point(point)
            if self.model.start_point:
                self.is_prepared = True

        is_crossed = lap_ctx["is_finish_line_crossed"]
        curr_lap = lap_ctx["curr_lap"]

        self.model.fill(is_crossed, curr_lap, fuel.val, speed.val)
        result = self.model.process_data(is_crossed, curr_lap)
        if result is None:
            return None

        laps_left, burn_per_lap, predicted_fuel = result
        predicted_for_lap = curr_lap

        if laps_left is not None and burn_per_lap is not None:
            logger.info(
                "Fuel: {:.2f} L at end of lap {} | {:.1f} laps to empty | {:.3f} L/lap",
                float(predicted_fuel),
                predicted_for_lap,
                float(laps_left),
                float(burn_per_lap),
            )
            instruction = (
                f"{float(laps_left):.1f} laps to empty. "
                f"Fuel at end of lap {predicted_for_lap}: {float(predicted_fuel):.2f} L "
                f"({float(burn_per_lap):.3f} L/lap)"
            )
        else:
            logger.info(
                "Fuel: {:.2f} L at end of lap {} | laps-to-empty unavailable",
                float(predicted_fuel),
                predicted_for_lap,
            )
            instruction = (
                f"Fuel at end of lap {predicted_for_lap}: "
                f"{float(predicted_fuel):.2f} L"
            )

        return RunResult(value=float(predicted_fuel), instruction=instruction)


class LapTimePrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — full lap time projected at 25/50/75% distance",
            sensors={"speed", "hv_soc", "rpm", "gps_lon", "gps_lat"},
        )
        self.model = LapTimePred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        hv_soc = signals.get("hv_soc")
        rpm = signals.get("rpm")
        speed = signals.get("speed")
        gps_lon = signals.get("gps_lon")
        gps_lat = signals.get("gps_lat")
        if hv_soc is None or rpm is None:
            logger.warning("HV SOC or RPM signal missing, skipping lap time prediction")
            return None

        track = lap_ctx.get("track")
        p25 = lap_ctx.get("p25")
        p50 = lap_ctx.get("p50")
        p75 = lap_ctx.get("p75")
        if track is None or p25 is None or p50 is None or p75 is None:
            return None
        if gps_lon is None or gps_lat is None or speed is None:
            return None

        if self.need_prepare():
            self.model.set_points(p25, p50, p75)
            self.model.set_track(track)
            self.is_prepared = True

        lap_time = lap_ctx["lap_time"]
        is_crossed = lap_ctx["is_finish_line_crossed"]

        self.model.fill(
            gps_lon.val, gps_lat.val, lap_time,
            speed.val, hv_soc.val, rpm.val,
        )
        result = self.model.process_data(is_crossed, lap_time)
        if result is None:
            return None

        predicted_time, split_pct = result
        predicted_for_lap = lap_ctx["curr_lap"]
        logger.info(
            "Lap time: {:.3f} s projected for lap {} at {}% distance",
            float(predicted_time),
            predicted_for_lap,
            split_pct,
        )
        instruction = (
            f"At {split_pct}% of lap {predicted_for_lap}: "
            f"projected full lap {float(predicted_time):.3f} s"
        )
        return RunResult(value=float(predicted_time), instruction=instruction)


class TyreWearPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — projected tyre grip retention vs fresh baseline",
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

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        steer_angle = signals.get("steer_angle")
        accel_long = signals.get("accel_long")
        brake_prs = signals.get("brake_prs")
        throttle = signals.get("throttle")
        accel_lat = signals.get("accel_lat")
        speed = signals.get("speed")
        if steer_angle is None or accel_long is None or brake_prs is None or throttle is None or accel_lat is None:
            logger.warning("Tyre wear signals missing, skipping tyre wear prediction")
            return None

        if self.need_prepare():
            self.is_prepared = True

        self.model.fill(
            steer_angle.val, speed.val, accel_long.val,
            brake_prs.val, throttle.val, accel_lat.val,
        )
        result = self.model.process_data(lap_ctx["is_finish_line_crossed"])
        if result is None:
            return None

        predicted_for_lap = lap_ctx["curr_lap"]
        logger.info(
            "Tyre grip: {:.1f}% retention projected at end of lap {}",
            float(result),
            predicted_for_lap,
        )
        instruction = (
            f"Projected tyre grip at end of lap {predicted_for_lap}: "
            f"{float(result):.1f}% of baseline"
        )
        return RunResult(value=float(result), instruction=instruction)


class BrakeFadePrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — projected brake performance vs fresh baseline",
            sensors={
                "steer_angle",
                "speed",
                "accel_long",
                "brake_prs",
                "hv_soc",
                "ambient_temp",
            },
        )
        self.model = BrakeFadePred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        steer_angle = signals.get("steer_angle")
        accel_long = signals.get("accel_long")
        brake_prs = signals.get("brake_prs")
        hv_soc = signals.get("hv_soc")
        ambient_temp = signals.get("ambient_temp")
        speed = signals.get("speed")
        if (
            steer_angle is None
            or accel_long is None
            or speed is None
            or brake_prs is None
            or hv_soc is None
            or ambient_temp is None
        ):
            logger.warning("Brake fade sensors missing, skipping brake fade prediction")
            return None

        if self.need_prepare():
            self.is_prepared = True

        self.model.fill(
            steer_angle.val, speed.val, accel_long.val,
            brake_prs.val, hv_soc.val, ambient_temp.val,
        )
        result = self.model.process_data(lap_ctx["is_finish_line_crossed"])
        if result is None:
            return None

        predicted_for_lap = lap_ctx["curr_lap"]
        logger.info(
            "Brake fade: {:.1f}% performance projected at end of lap {}",
            float(result),
            predicted_for_lap,
        )
        instruction = (
            f"Projected brake performance at end of lap {predicted_for_lap}: "
            f"{float(result):.1f}% of baseline"
        )
        return RunResult(value=float(result), instruction=instruction)


class CarFluidsPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Fluids in next delta time window",
            sensors={
                "oil_temp", "rpm", "engine_load",
                "speed", "ambient_temp",
            },
        )
        self.model = CarFluidsPred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        oil_temp = signals.get("oil_temp")
        rpm = signals.get("rpm")
        engine_load = signals.get("engine_load")
        ambient_temp = signals.get("ambient_temp")
        speed = signals.get("speed")
        if oil_temp is None or rpm is None or engine_load is None or ambient_temp is None:
            logger.warning("Car fluids sensors missing, skipping car fluids prediction")
            return None

        if self.need_prepare():
            import time
            self.model.set_race_start_time(time.time_ns())
            self.is_prepared = True

        self.model.fill(
            oil_temp.val, rpm.val, engine_load.val,
            speed.val, ambient_temp.val,
        )
        result = self.model.process_data(
            oil_temp.val, rpm.val, engine_load.val,
            speed.val, ambient_temp.val,
        )
        if result is None:
            return None

        window = self.model.window_length_s
        logger.info(
            "Car fluids: oil temp in next {} s: {:.1f} C",
            window,
            float(result),
        )
        instruction = (
            f"Predicted oil temperature in next {window} seconds: "
            f"{float(result):.1f}\u00b0C"
        )
        return RunResult(value=float(result), instruction=instruction)










# NEW
class SpeedOnTurnsPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="Current lap — recommended speed for upcoming turns",
            sensors={"steer_angle", "speed", "accel_long", "accel_lat", "brake_prs", "throttle", "gps_lon", "gps_lat"},
        )
        self.model = SpeedOnTurnsPred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        speed = signals.get("speed")
        gps_lon = signals.get("gps_lon")
        gps_lat = signals.get("gps_lat")
        accel_lat = signals.get("accel_lat")

        if speed is None or gps_lon is None or gps_lat is None or accel_lat is None:
            logger.warning("Speed on turns sensors missing, skipping speed on turns prediction")
            return None

        if lap_ctx.get("is_finish_line_crossed"):
            self.model.reset_lap()

        if self.need_prepare():
            turns = lap_ctx.get("turns_lst")
            track_meters = lap_ctx.get("track_meters")
            project_to_meters = lap_ctx.get("project_to_meters")
            if turns is None or track_meters is None:
                return None
            self.model.set_turns(turns, track_meters,project_to_meters)
            self.is_prepared = True

        max_safe_kmh, real_speed_then,turn_idx = self.model.process_data(
            gps_lon.val, gps_lat.val,
            speed.val, accel_lat.val
        )
        if max_safe_kmh is None or real_speed_then is None or turn_idx is None:
            return None

        logger.info(
            "Speed on turn {:.1f}: recommended max speed {:.1f} km/h but real speed {:.1f} km/h",
            float(turn_idx),
            float(max_safe_kmh),
            float(real_speed_then),
        )
        instruction = (
            f"Recommended max speed at upcoming turn: "
            f"{float(max_safe_kmh):.1f} km/h (real speed then: {float(real_speed_then):.0f} m)"
        )
        return RunResult(value=float(max_safe_kmh), instruction=instruction)
