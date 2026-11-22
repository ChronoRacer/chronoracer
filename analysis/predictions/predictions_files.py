from __future__ import annotations
from loguru import logger

from analysis.predictions.prediction_models import (
    BrakeFadePred,
    CarFluidsPred,
    CoolantTempPred,
    LackOfFuelPred,
    LapTimePred,
    TyreWearPred,
    SpeedOnTurnsPred,
    LapComparePred
)

class RunResult:
    """Value returned by ``PredictionClass.run`` when there is something to send."""

    def __init__(self, value: float, instruction: str):
        self.value = round(float(value), 3) if value is not None else None
        self.instruction = instruction


class PredictionClass:
    def __init__(
        self,
        name: str,
        sensors: set[str],
        refresh_rate: str | None = None,
        needs_lap: bool = False,
    ):
        self.name = name
        self.sensors = sensors
        self.needs_lap = needs_lap
        self.refresh_rate = refresh_rate
        self.is_prepared = False

    def need_prepare(self) -> bool:
        return not self.is_prepared

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        return None


class FuelPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="CURRENT LAP - PROJECTED FUEL LEVEL AND LAPS TO EMPTY",
            sensors={"FUEL_TANK_L", "CAR_SPEED_KMH"},
            refresh_rate="every lap",
            needs_lap=True,
        )
        self.model = LackOfFuelPred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        fuel = signals.get("FUEL_TANK_L")
        speed = signals.get("CAR_SPEED_KMH")
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
                "Fuel: {:.3f} L at end of lap {} | {:.1f} laps to empty | {:.3f} L/lap",
                float(predicted_fuel),
                predicted_for_lap,
                float(laps_left),
                float(burn_per_lap),
            )
            instruction = (
                f"{float(laps_left):.1f} laps to empty. "
                f"Fuel at end of lap {predicted_for_lap}: {float(predicted_fuel):.3f} L "
                f"({float(burn_per_lap):.3f} L/lap)"
            )
        else:
            logger.info(
                "Fuel: {:.3f} L at end of lap {} | laps-to-empty unavailable",
                float(predicted_fuel),
                predicted_for_lap,
            )
            instruction = (
                f"Fuel at end of lap {predicted_for_lap}: "
                f"{float(predicted_fuel):.3f} L"
            )

        return RunResult(value=float(predicted_fuel), instruction=instruction)


class LapTimePrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="CURRENT LAP - FULL LAP TIME PROJECTED AT 25/50/75% DISTANCE",
            sensors={"CAR_SPEED_KMH", "STATE_OF_CHARGE_PERC", "RPM", "GPS_LON_DEG", "GPS_LAT_DEG"},
            refresh_rate="every 25/50/75% distance",
            needs_lap=True,
        )
        self.model = LapTimePred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        hv_soc = signals.get("STATE_OF_CHARGE_PERC")
        rpm = signals.get("RPM")
        speed = signals.get("CAR_SPEED_KMH")
        gps_lon = signals.get("GPS_LON_DEG")
        gps_lat = signals.get("GPS_LAT_DEG")
        if hv_soc is None or rpm is None:
            logger.warning("HV SOC or RPM signal missing, skipping lap time prediction")
            return None

        track = lap_ctx.get("track_meters")
        project_to_meters = lap_ctx.get("project_to_meters")
        p25 = lap_ctx.get("p25")
        p50 = lap_ctx.get("p50")
        p75 = lap_ctx.get("p75")
        if track is None or project_to_meters is None or p25 is None or p50 is None or p75 is None:
            return None
        if gps_lon is None or gps_lat is None or speed is None:
            return None

        if self.need_prepare():
            self.model.set_points(p25, p50, p75)
            self.model.set_track(track, project_to_meters)
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
            name="TYRE GRIP RETENTION VS FRESH BASELINE",
            sensors={
                "STEER_ANGLE_DEG",
                "CAR_SPEED_KMH",
                "ACCEL_LON_MS2",
                "ACCEL_LAT_MS2",
                "BRAKE_PRESSURE_PERC",
                "THROTTLE_PERC",
            },
            refresh_rate="every lap",
            needs_lap=True,
        )
        self.model = TyreWearPred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        steer_angle = signals.get("STEER_ANGLE_DEG")
        accel_long = signals.get("ACCEL_LON_MS2")
        brake_prs = signals.get("BRAKE_PRESSURE_PERC")
        throttle = signals.get("THROTTLE_PERC")
        accel_lat = signals.get("ACCEL_LAT_MS2")
        speed = signals.get("CAR_SPEED_KMH")
        if (
            steer_angle is None
            or accel_long is None
            or brake_prs is None
            or throttle is None
            or accel_lat is None
            or speed is None
        ):
            self.model.process_data(bool(lap_ctx["is_finish_line_crossed"]))
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

        curr_lap = lap_ctx["curr_lap"]
        instruction = f"Projected tyre grip on lap {curr_lap}: {float(result):.1f}% of fresh"
        logger.info("Tyre grip: {}", instruction)
        return RunResult(value=float(result), instruction=instruction)


class BrakeFadePrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="BRAKE PERFORMANCE VS FRESH BASELINE",
            sensors={
                "STEER_ANGLE_DEG",
                "CAR_SPEED_KMH",
                "ACCEL_LON_MS2",
                "BRAKE_PRESSURE_PERC",
                "STATE_OF_CHARGE_PERC",
                "AMBIENT_TEMP_C",
            },
            refresh_rate="every lap",
            needs_lap=True,
        )
        self.model = BrakeFadePred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        steer_angle = signals.get("STEER_ANGLE_DEG")
        accel_long = signals.get("ACCEL_LON_MS2")
        brake_prs = signals.get("BRAKE_PRESSURE_PERC")
        hv_soc = signals.get("STATE_OF_CHARGE_PERC")
        ambient_temp = signals.get("AMBIENT_TEMP_C")
        speed = signals.get("CAR_SPEED_KMH")
        if (
            steer_angle is None
            or accel_long is None
            or speed is None
            or brake_prs is None
            or hv_soc is None
            or ambient_temp is None
        ):
            self.model.process_data(bool(lap_ctx["is_finish_line_crossed"]))
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

        curr_lap = lap_ctx["curr_lap"]
        instruction = f"Projected brake performance on lap {curr_lap}: {float(result):.1f}% of fresh"
        logger.info("Brake fade: {}", instruction)
        return RunResult(value=float(result), instruction=instruction)


class CoolantTemperaturePrediction(PredictionClass):
    def __init__(self):
        self.model = CoolantTempPred()
        super().__init__(
            name="COOLANT TEMPERATURE FORECAST",
            sensors={"COOLANT_TEMP_C", "OIL_TEMP_C", "RPM", "ENGINE_LOAD_PERC", "CAR_SPEED_KMH", "AMBIENT_TEMP_C"},
            refresh_rate=f"every {self.model.fit_time_s} s",
        )

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        coolant_temp = signals.get("COOLANT_TEMP_C")
        oil_temp = signals.get("OIL_TEMP_C")
        rpm = signals.get("RPM")
        engine_load = signals.get("ENGINE_LOAD_PERC")
        speed = signals.get("CAR_SPEED_KMH")
        ambient_temp = signals.get("AMBIENT_TEMP_C")
        if any(value is None for value in (coolant_temp, oil_temp, rpm, engine_load, speed, ambient_temp)):
            return None

        if self.need_prepare():
            import time
            self.model.set_race_start_time(time.time_ns())
            self.is_prepared = True

        self.model.fill(
            coolant_temp.val, oil_temp.val, rpm.val, engine_load.val,
            speed.val, ambient_temp.val,
        )
        result = self.model.process_data(
            coolant_temp.val, oil_temp.val, rpm.val, engine_load.val,
            speed.val, ambient_temp.val,
        )
        if result is None:
            return None

        forecast_temp = result

        horizon = self.model.horizon_s
        logger.info(
            "Coolant: {:.1f} C in next {} s",
            forecast_temp,
            horizon,
        )
        instruction = (
            f"Coolant temperature in {horizon} s: {forecast_temp:.1f}°C; "
            "Forecast from recent telemetry."
        )
        return RunResult(value=float(forecast_temp), instruction=instruction)


class CarFluidsPrediction(PredictionClass):
    def __init__(self):
        self.model = CarFluidsPred()
        super().__init__(
            name="OIL TEMPERATURE FORECAST",
            sensors={
                "OIL_TEMP_C", "RPM", "ENGINE_LOAD_PERC",
                "CAR_SPEED_KMH", "AMBIENT_TEMP_C",
            },
            refresh_rate=f"every {self.model.fit_time_s} s",
        )

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        oil_temp = signals.get("OIL_TEMP_C")
        rpm = signals.get("RPM")
        engine_load = signals.get("ENGINE_LOAD_PERC")
        ambient_temp = signals.get("AMBIENT_TEMP_C")
        speed = signals.get("CAR_SPEED_KMH")
        if (
            oil_temp is None
            or rpm is None
            or engine_load is None
            or ambient_temp is None
            or speed is None
        ):
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


class SpeedOnTurnsPrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="CURRENT LAP - RECOMMENDED SPEED FOR UPCOMING TURNS",
            sensors={
                "STEER_ANGLE_DEG",
                "CAR_SPEED_KMH",
                "ACCEL_LON_MS2",
                "ACCEL_LAT_MS2",
                "BRAKE_PRESSURE_PERC",
                "THROTTLE_PERC",
                "GPS_LON_DEG",
                "GPS_LAT_DEG",
            },
            refresh_rate="at each turn",
            needs_lap=True,
        )
        self.model = SpeedOnTurnsPred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        speed = signals.get("CAR_SPEED_KMH")
        gps_lon = signals.get("GPS_LON_DEG")
        gps_lat = signals.get("GPS_LAT_DEG")
        accel_lat = signals.get("ACCEL_LAT_MS2")

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


class LapComparePrediction(PredictionClass):
    def __init__(self):
        super().__init__(
            name="LAP COMPARISON - FINISHED LAP VS PREVIOUS ONE",
            sensors={"CAR_SPEED_KMH", "BRAKE_PRESSURE_PERC", "STATE_OF_CHARGE_PERC"},
            refresh_rate="every lap",
            needs_lap=True,
        )
        self.model = LapComparePred()

    def run(self, signals: dict, lap_ctx: dict) -> RunResult | None:
        speed = signals.get("CAR_SPEED_KMH")
        brake_prs = signals.get("BRAKE_PRESSURE_PERC")
        hv_soc = signals.get("STATE_OF_CHARGE_PERC")

        if speed is None or brake_prs is None or hv_soc is None:
            if lap_ctx.get("is_finish_line_crossed"):
                self.model.skip_lap()
            logger.warning("Lap compare sensors missing, skipping lap compare prediction")
            return None

        if self.need_prepare():
            self.is_prepared = True

        result = self.model.process_data(
            is_finish_line_crossed=bool(lap_ctx.get("is_finish_line_crossed")),
            lap_time=float(lap_ctx["lap_time"]),
            lap_num=int(lap_ctx["curr_lap"]),
            speed_kmh=speed.val,
            hv_soc=hv_soc.val,
            brake_prs=brake_prs.val,
        )
        if result is None:
            return None
        score = self.model._lap_score_func(result)
        logger.info(
            "Score: {}, Lap compare L{}: {:.3f}s ({:+.3f} vs prev, {:+.3f} vs best L{}) | "
            "avg {:.1f} km/h ({:+.1f}) | hard brakes {} ({:+d}) | SoC Δ {:+.1f}% ({:+.1f})",
            score,
            result.lap_num,
            result.lap_time,
            result.delta_lap_time,
            result.delta_best_lap_time,
            result.best_lap_num,
            result.avg_speed,
            result.delta_lap_speed,
            result.hard_brakes,
            result.delta_hard_brakes,
            result.hv_soc_delta,
            result.delta_hv_soc,
        )
        instruction = (
            f"L{result.lap_num} · {result.lap_time:.3f}s "
            f"({result.delta_lap_time:+.3f} vs prev, "
            f"{result.delta_best_lap_time:+.3f} vs best L{result.best_lap_num}) · "
            f"avg {result.avg_speed:.0f} km/h ({result.delta_lap_speed:+.1f}) · "
            f"hard brakes {result.hard_brakes} ({result.delta_hard_brakes:+d}) · "
            f"SoC diff {result.hv_soc_delta:+.1f}% ({result.delta_hv_soc:+.1f} vs prev)"
        )
        res = score
        return RunResult(value=float(res), instruction=instruction)
