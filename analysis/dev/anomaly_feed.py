import io
import json
import math
import random
import threading
import time
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

schema_path = Path(__file__).resolve().parents[2] / 'core' / 'avro' / 'sensor_data.avsc'
PARSED_SCHEMA = parse_schema(json.loads(schema_path.read_text()))
CONFIG_PATH = Path(__file__).resolve().parent / 'config.json'
CONFIG = json.loads(CONFIG_PATH.read_text())

# Producer rule from docs/component-kafka.md: each delta is an offset from the batch
# timestamp and must not exceed 2 seconds.
MAX_DELTA_NS = 2_000_000_000

GRAVITY = 9.81
AIR_DENSITY = 1.2
WHEEL_POWER_REFERENCE_W = 75_000.0
METERS_PER_DEGREE_LAT = 111_320.0
# Stoichiometric air/fuel ratio and petrol density, for fuel burn from the air mass flow.
STOICH_AFR = 14.7
PETROL_G_PER_L = 740.0

# Channels reported as whole counts even though the wire type is double.
INTEGER_CHANNELS = frozenset({"gps_sats", "gps_fix"})


def clamp(value, low, high):
    return max(low, min(high, value))


@dataclass(frozen=True)
class Channel:
    name: str
    unit: str
    minimum: float | None
    maximum: float | None
    period_s: float


@dataclass(frozen=True)
class Segment:
    target_kph: float
    duration_s: float
    radius_m: float | None
    # 1 turns left, -1 turns right, 0 is a straight. Left is positive, as in the channel file.
    turn: int


@dataclass
class Anomaly:
    kind: str
    at_s: float
    duration_s: float
    sensors: tuple
    params: dict

    def progress(self, elapsed_s):
        """Position inside the scripted window as 0..1, or None when inactive."""
        if not self.at_s <= elapsed_s < self.at_s + self.duration_s:
            return None
        if self.duration_s <= 0.0:
            return 1.0
        return (elapsed_s - self.at_s) / self.duration_s


def load_channels():
    """Channel metadata from the car's channel file, paired with the configured rates.

    `debug.force_period_s` replaces every per-channel rate with one fixed period, and
    `debug.only_sensors` narrows the run to the listed channels. Both together turn the
    feed into a trickle: one sensor, one reading every few seconds, which is readable by
    hand while following a single value through Core and into the rules engine.

    Returns every name in the channel file next to the channels this run publishes, so
    the anomaly script can still be validated against channels debug left out.
    """
    channels_path = (CONFIG_PATH.parent / CONFIG["channels_file"]).resolve()
    definitions = json.loads(channels_path.read_text(encoding="utf-8"))["sensors"]
    rates = CONFIG["sample_rates_hz"]

    defined = {definition["name"] for definition in definitions}
    missing_rate = sorted(defined - rates.keys())
    unknown_rate = sorted(rates.keys() - defined)
    if missing_rate or unknown_rate:
        raise ValueError(
            f"sample_rates_hz does not match {channels_path.name}: "
            f"missing {missing_rate}, unknown {unknown_rate}"
        )

    # Kafka `data` keys are at most 12 ASCII characters (docs/component-kafka.md).
    too_long = sorted(name for name in defined if len(name.encode("ascii")) > 12)
    if too_long:
        raise ValueError(f"sensor ids too long for the Kafka key: {too_long}")

    debug = CONFIG.get("debug", {})
    forced_period_s = debug.get("force_period_s")
    if forced_period_s is not None:
        forced_period_s = float(forced_period_s)
        if forced_period_s <= 0.0:
            raise ValueError("debug.force_period_s must be a positive number of seconds")
    only_sensors = frozenset(debug.get("only_sensors", ()))
    unknown_only = sorted(only_sensors - defined)
    if unknown_only:
        raise ValueError(f"debug.only_sensors names unknown sensors: {unknown_only}")

    channels = {}
    for definition in definitions:
        name = definition["name"]
        if only_sensors and name not in only_sensors:
            continue
        channels[name] = Channel(
            name=name,
            unit=definition["unit"],
            minimum=definition["min"],
            maximum=definition["max"],
            period_s=forced_period_s if forced_period_s is not None else 1.0 / float(rates[name]),
        )
    if not channels:
        raise ValueError("no channels left to publish, check debug.only_sensors")
    return defined, channels


ALL_SENSOR_NAMES, CHANNELS = load_channels()


class DriveModel:
    """One lap of a front-wheel-drive hybrid, integrated from segment speed targets.

    Every channel is derived from the same longitudinal and cornering state, so the
    relations the rules engine looks at (wheel speeds against speed, brake pressure
    against deceleration, load against air mass flow) hold unless an anomaly breaks them.
    """

    def __init__(self, car, lap, seed):
        self.car = car
        self.lap = lap
        self.lap_duration_s = sum(segment.duration_s for segment in lap)
        self.rng = random.Random(seed)

        self.speed_mps = 0.0
        self.heading_deg = car["start_heading_deg"]
        self.latitude = car["start_lat"]
        self.longitude = car["start_lon"]
        self.distance_m = 0.0
        self.coolant_c = 42.0
        self.oil_c = 40.0
        self.hv_soc = 62.0
        self.fuel_l = car["fuel_start_l"]
        self.long_term_trim = 1.5

    def _segment_at(self, elapsed_s):
        phase = elapsed_s % self.lap_duration_s
        for segment in self.lap:
            if phase < segment.duration_s:
                return segment
            phase -= segment.duration_s
        return self.lap[-1]

    def _noise(self, scale):
        return self.rng.uniform(-scale, scale)

    def step(self, elapsed_s, dt):
        car = self.car
        segment = self._segment_at(elapsed_s)

        speed_error = segment.target_kph / 3.6 - self.speed_mps
        if speed_error >= 0.0:
            demand_mps2 = min(speed_error / 0.9, car["max_accel_mps2"])
        else:
            demand_mps2 = max(speed_error / 0.6, -car["max_brake_mps2"])
        self.speed_mps = max(0.0, self.speed_mps + demand_mps2 * dt)

        speed_mps = self.speed_mps
        speed_kph = speed_mps * 3.6

        drag_n = 0.5 * AIR_DENSITY * car["drag_area"] * speed_mps**2
        rolling_n = car["rolling_resistance"] * car["mass_kg"] * GRAVITY
        wheel_power_w = max(0.0, (car["mass_kg"] * demand_mps2 + drag_n + rolling_n) * speed_mps)
        power_fraction = min(1.0, wheel_power_w / WHEEL_POWER_REFERENCE_W)

        # The eCVT keeps the engine off while the battery can cover a light load.
        electric_only = wheel_power_w < 9_000.0 and speed_kph < 70.0 and self.hv_soc > 35.0
        if electric_only:
            rpm = 0.0
            engine_load = 0.0
        else:
            rpm = 1150.0 + power_fraction * 4_050.0 + self._noise(25.0)
            engine_load = clamp(8.0 + power_fraction * 92.0, 8.0, 100.0)
        load_fraction = engine_load / 100.0

        accel_pedal = clamp(max(0.0, demand_mps2) / car["max_accel_mps2"] * 100.0, 0.0, 100.0)
        brake_pressure = clamp(max(0.0, -demand_mps2) / car["max_brake_mps2"] * 100.0, 0.0, 100.0)
        throttle = 0.0 if rpm == 0.0 else max(6.0, accel_pedal * 0.92)

        if segment.radius_m:
            steer_angle = (
                math.degrees(math.atan(car["wheelbase_m"] / segment.radius_m))
                * car["steering_ratio"]
                * segment.turn
            )
            lateral_accel = speed_mps**2 / segment.radius_m * segment.turn
            yaw_rate_deg_s = math.degrees(speed_mps / segment.radius_m) * segment.turn
        else:
            steer_angle = self._noise(2.5)
            lateral_accel = self._noise(0.15)
            yaw_rate_deg_s = 0.0

        # Compass heading grows clockwise, so a left turn (positive) subtracts from it.
        self.heading_deg = (self.heading_deg - yaw_rate_deg_s * dt) % 360.0

        half_track_m = car["track_m"] / 2.0
        if segment.radius_m:
            outer_kph = speed_kph * (1.0 + half_track_m / segment.radius_m)
            inner_kph = speed_kph * (1.0 - half_track_m / segment.radius_m)
        else:
            outer_kph = inner_kph = speed_kph
        if segment.turn >= 0:
            right_kph, left_kph = outer_kph, inner_kph
        else:
            right_kph, left_kph = inner_kph, outer_kph
        # Front wheels are driven, so they carry a little slip under power.
        front_slip = 1.0 + (0.006 if demand_mps2 > 0.5 else 0.0)

        self.coolant_c += ((88.0 + 9.0 * load_fraction) - self.coolant_c) * dt / 45.0
        self.oil_c += ((95.0 + 22.0 * load_fraction) - self.oil_c) * dt / 90.0

        if demand_mps2 < -0.2:
            self.hv_soc += 0.9 * dt * min(1.0, speed_mps / 30.0)
        elif electric_only:
            self.hv_soc -= 0.8 * dt
        elif wheel_power_w > 45_000.0:
            self.hv_soc -= 0.5 * dt
        else:
            self.hv_soc += 0.15 * dt
        self.hv_soc = clamp(self.hv_soc, 22.0, 80.0)

        air_mass_flow = 0.0 if rpm == 0.0 else (rpm / 1000.0) * load_fraction * 18.0 + 2.0
        self.fuel_l = max(0.0, self.fuel_l - air_mass_flow / STOICH_AFR / PETROL_G_PER_L * dt)

        commanded_lambda = 1.0 if rpm == 0.0 else (0.88 if load_fraction > 0.85 else 1.0)
        short_term_trim = 2.5 * math.sin(elapsed_s * 1.7) + self._noise(0.8)
        self.long_term_trim += (1.8 - self.long_term_trim) * dt / 60.0
        measured_lambda = commanded_lambda + 0.012 * math.sin(elapsed_s * 6.0) + self._noise(0.004)

        self.distance_m += speed_mps * dt
        heading_rad = math.radians(self.heading_deg)
        self.latitude += speed_mps * dt * math.cos(heading_rad) / METERS_PER_DEGREE_LAT
        self.longitude += (
            speed_mps
            * dt
            * math.sin(heading_rad)
            / (METERS_PER_DEGREE_LAT * math.cos(math.radians(self.latitude)))
        )
        altitude_m = car["start_alt_m"] + 8.0 * math.sin(self.distance_m / 400.0)
        ambient_c = car["ambient_c"] + 0.6 * math.sin(elapsed_s / 120.0)

        return {
            "steer_angle": steer_angle,
            "steer_torque": lateral_accel * 28.0 + self._noise(4.0),
            "brake_prs": brake_pressure,
            "brake_prs_b": brake_pressure * 0.98 + self._noise(0.4),
            "wheel_fl": left_kph * front_slip,
            "wheel_fr": right_kph * front_slip,
            "wheel_rl": left_kph,
            "wheel_rr": right_kph,
            "accel_long": demand_mps2 + self._noise(0.12),
            "accel_lat": lateral_accel + self._noise(0.1),
            "accel_pedal": accel_pedal,
            "speed": speed_kph,
            "rpm": rpm,
            "hv_soc": self.hv_soc,
            "coolant_temp": self.coolant_c,
            "oil_temp": self.oil_c,
            "intake_temp": ambient_c
            + 6.0
            + 12.0 * load_fraction * (1.0 - min(0.8, speed_kph / 160.0)),
            "throttle": throttle,
            "engine_load": engine_load,
            "maf": air_mass_flow,
            "ign_timing": 0.0 if rpm == 0.0 else 14.0 + (rpm - 1600.0) / 260.0 - load_fraction * 16.0,
            "stft": short_term_trim,
            "ltft": self.long_term_trim,
            "lambda_cmd": commanded_lambda,
            "lambda": measured_lambda,
            "o2_current": (measured_lambda - 1.0) * 9.0,
            "baro": 101.3 - altitude_m * 0.0118,
            "batt_volt": (13.6 if rpm == 0.0 else 14.25 - 0.25 * load_fraction) + self._noise(0.04),
            "ambient_temp": ambient_c,
            "fuel_l": self.fuel_l,
            "gps_lat": self.latitude,
            "gps_lon": self.longitude,
            "gps_speed": speed_kph + self._noise(0.3),
            "gps_heading": self.heading_deg,
            "gps_alt": altitude_m,
            "gps_sats": 11.0 + 2.0 * math.sin(elapsed_s / 40.0),
            "gps_pdop": 1.05 + 0.25 * math.sin(elapsed_s / 25.0),
            "gps_fix": 3.0,
        }


def _apply_wheel_lockup(sample, anomaly, progress):
    for sensor in anomaly.sensors:
        sample[sensor] = anomaly.params.get("locked_kph", 2.0)


def _apply_brake_channel_divergence(sample, anomaly, progress):
    for sensor in anomaly.sensors:
        sample[sensor] = sample["brake_prs"] * anomaly.params.get("scale", 0.15)


def _apply_lean_mixture(sample, anomaly, progress):
    ramp = min(1.0, progress * 3.0)
    sample["lambda"] += (anomaly.params.get("lambda_value", 1.18) - sample["lambda"]) * ramp
    sample["stft"] += (anomaly.params.get("stft_value", 13.0) - sample["stft"]) * ramp
    sample["ltft"] += (anomaly.params.get("ltft_value", 18.0) - sample["ltft"]) * ramp


def _apply_rpm_flare(sample, anomaly, progress):
    if sample["rpm"] > 0.0:
        sample["rpm"] += anomaly.params.get("extra_rpm", 2600.0)


def _apply_coolant_overheat(sample, anomaly, progress):
    target = anomaly.params.get("target_c", 118.0)
    sample["coolant_temp"] += (target - sample["coolant_temp"]) * min(1.0, progress * 2.0)


def _apply_oil_temp_overheat(sample, anomaly, progress):
    target = anomaly.params.get("target_c", 136.0)
    sample["oil_temp"] += (target - sample["oil_temp"]) * min(1.0, progress * 2.0)


def _apply_gps_fix_loss(sample, anomaly, progress):
    sample["gps_fix"] = 0.0
    sample["gps_sats"] = anomaly.params.get("sats", 2.0)
    sample["gps_pdop"] = anomaly.params.get("pdop", 42.0)


def _apply_drive_wheel_slip(sample, anomaly, progress):
    slip = 1.0 + anomaly.params.get("slip_ratio", 0.28)
    for sensor in anomaly.sensors:
        sample[sensor] *= slip


def _apply_throttle_pedal_mismatch(sample, anomaly, progress):
    sample["throttle"] = anomaly.params.get("throttle_value", 62.0)
    sample["accel_pedal"] = 0.0


def _apply_batt_volt_sag(sample, anomaly, progress):
    sample["batt_volt"] = anomaly.params.get("volts", 11.3)


def _apply_speed_gps_mismatch(sample, anomaly, progress):
    sample["gps_speed"] = sample["speed"] * anomaly.params.get("scale", 0.55)


# Anomalies that rewrite values. stuck_sensor, sensor_dropout and uplink_dropout are not
# here because they change what is published rather than what is measured.
VALUE_ANOMALIES = {
    "wheel_lockup": _apply_wheel_lockup,
    "brake_channel_divergence": _apply_brake_channel_divergence,
    "lean_mixture": _apply_lean_mixture,
    "rpm_flare": _apply_rpm_flare,
    "coolant_overheat": _apply_coolant_overheat,
    "oil_temp_overheat": _apply_oil_temp_overheat,
    "gps_fix_loss": _apply_gps_fix_loss,
    "drive_wheel_slip": _apply_drive_wheel_slip,
    "throttle_pedal_mismatch": _apply_throttle_pedal_mismatch,
    "batt_volt_sag": _apply_batt_volt_sag,
    "speed_gps_mismatch": _apply_speed_gps_mismatch,
}

PUBLICATION_ANOMALIES = frozenset({"stuck_sensor", "sensor_dropout", "uplink_dropout"})

EXPECTATIONS = {
    "wheel_lockup": "one wheel reads near zero while the others and brake pressure stay high",
    "brake_channel_divergence": "the redundant brake channel no longer tracks the main one",
    "lean_mixture": "lambda stays lean while both fuel trims run positive",
    "rpm_flare": "engine speed jumps with no matching change in road speed",
    "coolant_overheat": "coolant holds far above its regulated temperature",
    "oil_temp_overheat": "oil temperature holds near the top of its range",
    "gps_fix_loss": "fix drops to none, satellites collapse and PDOP explodes",
    "drive_wheel_slip": "driven wheels overspeed the undriven pair under power",
    "throttle_pedal_mismatch": "throttle opens while the pedal reads zero",
    "batt_volt_sag": "12 V rail sits below the charging range with the engine running",
    "speed_gps_mismatch": "GPS ground speed disagrees with controller speed",
    "stuck_sensor": "channel keeps publishing one frozen value",
    "sensor_dropout": "channel stops publishing entirely, old state must not stay valid",
    "uplink_dropout": "the whole car stops producing, every channel goes silent at once",
    "clean": "ordinary driving, no alert expected",
}


def load_anomalies():
    anomalies = []
    for entry in CONFIG["anomalies"]:
        kind = entry["kind"]
        if kind not in VALUE_ANOMALIES and kind not in PUBLICATION_ANOMALIES:
            raise ValueError(f"unknown anomaly kind {kind!r}")
        for sensor in entry.get("sensors", ()):
            if sensor not in ALL_SENSOR_NAMES:
                raise ValueError(f"anomaly {kind!r} names unknown sensor {sensor!r}")
            if sensor not in CHANNELS:
                print(f"[Config] anomaly '{kind}' targets '{sensor}', "
                      f"which debug.only_sensors leaves out of this run.")
        anomalies.append(
            Anomaly(
                kind=kind,
                at_s=float(entry["at_s"]),
                duration_s=float(entry["duration_s"]),
                sensors=tuple(entry.get("sensors", ())),
                params=entry.get("params", {}),
            )
        )

    anomalies.sort(key=lambda anomaly: anomaly.at_s)
    run = CONFIG["run"]
    min_gap_s = float(run["min_clean_gap_s"])
    previous_end_s = 0.0
    previous_kind = "run start"
    for anomaly in anomalies:
        if anomaly.at_s - previous_end_s < min_gap_s:
            raise ValueError(
                f"only {anomaly.at_s - previous_end_s:.1f}s of clean driving between "
                f"{previous_kind} and {anomaly.kind}, min_clean_gap_s is {min_gap_s}"
            )
        previous_end_s = anomaly.at_s + anomaly.duration_s
        previous_kind = anomaly.kind
    if float(run["duration_s"]) - previous_end_s < min_gap_s:
        raise ValueError(
            f"run ends {float(run['duration_s']) - previous_end_s:.1f}s after {previous_kind}, "
            f"min_clean_gap_s is {min_gap_s}"
        )
    return anomalies


def clean_windows(anomalies):
    """Stretches with no scripted fault, where the rules engine must stay quiet."""
    windows = []
    cursor_s = 0.0
    for anomaly in anomalies:
        if anomaly.at_s > cursor_s:
            windows.append((cursor_s, anomaly.at_s))
        cursor_s = max(cursor_s, anomaly.at_s + anomaly.duration_s)
    duration_s = float(CONFIG["run"]["duration_s"])
    if duration_s > cursor_s:
        windows.append((cursor_s, duration_s))
    return windows


def build_model():
    lap = tuple(
        Segment(
            target_kph=float(segment["target_kph"]),
            duration_s=float(segment["duration_s"]),
            radius_m=float(segment["radius_m"]) if segment.get("radius_m") else None,
            turn=int(segment.get("turn", 0)),
        )
        for segment in CONFIG["lap"]
    )
    return DriveModel(car=CONFIG["car"], lap=lap, seed=int(CONFIG["run"]["seed"]))


def build_producer_config():
    producer_config = {
        'bootstrap.servers': CONFIG["broker"],
        'linger.ms': 5,
        'batch.num.messages': 500,
        'queue.buffering.max.messages': 10000,
        'compression.type': 'lz4'
    }

    oauth = CONFIG.get("oauth")
    if oauth and oauth.get("enabled", False):
        required_fields = ("client_id", "client_secret_env_var", "token_endpoint_url", "extension_audience", "broker")
        missing_fields = [field for field in required_fields if not oauth.get(field)]
        if missing_fields:
            raise ValueError(
                "OAuth is enabled but the following fields are missing: "
                + ", ".join(missing_fields)
            )
        secret = os.environ.get(oauth["client_secret_env_var"])
        if not secret:
            raise ValueError(
                f"OAuth is enabled but the secret environment variable '{oauth['client_secret_env_var']}' is not set."
            )

        producer_config.update({
            "bootstrap.servers": oauth["broker"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
            "sasl.oauthbearer.method": "oidc",
            "sasl.oauthbearer.client.id": oauth["client_id"],
            "sasl.oauthbearer.client.secret": secret,
            "sasl.oauthbearer.token.endpoint.url": oauth["token_endpoint_url"],
            "sasl.oauthbearer.extensions": f"audience={oauth["extension_audience"]}",
        })

    return producer_config


def serialize_record(record):
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, PARSED_SCHEMA, record)
    return bytes_writer.getvalue()


def delivery_report(err, msg):
    """Callback triggered by Kafka on successful or failed delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")


def finish_value(name, value):
    """Clamp to the channel's declared range so every point is one Core accepts."""
    channel = CHANNELS[name]
    if channel.minimum is not None and channel.maximum is not None:
        value = clamp(value, channel.minimum, channel.maximum)
    return float(round(value)) if name in INTEGER_CHANNELS else value


def random_value(name, rng, low, high):
    """Uniform reading that ignores the drive model and the channel's declared range."""
    value = rng.uniform(low, high)
    return float(round(value)) if name in INTEGER_CHANNELS else value


def publication_state(active):
    """Which channels are frozen, which are silent, and whether the uplink is down."""
    frozen = set()
    suppressed = set()
    uplink_down = False
    for anomaly, _ in active:
        if anomaly.kind == "stuck_sensor":
            frozen.update(anomaly.sensors)
        elif anomaly.kind == "sensor_dropout":
            suppressed.update(anomaly.sensors)
        elif anomaly.kind == "uplink_dropout":
            uplink_down = True
    return frozen, suppressed, uplink_down


def build_manifest(anomalies):
    """Scripted windows in run order, so a run can be scored against what it should find.

    Clean windows are part of the expectation: an alert raised inside one is a false
    positive, which matters as much as a fault the rules engine missed.

    Times are seconds from run start, so the file depends only on the config. A run's
    absolute window is its printed start in Unix nanoseconds plus these offsets.
    """
    def window(kind, start_s, end_s, sensors):
        return {
            "kind": kind,
            "sensors": sensors,
            "start_s": start_s,
            "end_s": end_s,
            "expect": EXPECTATIONS.get(kind, ""),
        }

    entries = [
        window("clean", start_s, end_s, []) for start_s, end_s in clean_windows(anomalies)
    ]
    entries += [
        window(
            anomaly.kind,
            anomaly.at_s,
            anomaly.at_s + anomaly.duration_s,
            list(anomaly.sensors),
        )
        for anomaly in anomalies
    ]
    entries.sort(key=lambda entry: entry["start_s"])
    return entries


def produce_sensor_messages(producer, model, anomalies, started_unix_ns):
    """Thread function for producing Avro sensor messages from the driving model.

    One message is one channel's readings over the batch window, keyed by sensor id, which
    is the shape Analysis wants: a window per sensor rather than one point per message.
    """
    topic = CONFIG["sensor_topic"]
    run = CONFIG["run"]
    tick_s = run["tick_ms"] / 1000.0
    duration_s = float(run["duration_s"])
    speed_factor = float(run["speed_factor"])
    window_ns = int(CONFIG["batch_window_ms"]) * 1_000_000
    if window_ns > MAX_DELTA_NS:
        raise ValueError("batch_window_ms must not exceed 2000, the delta cap on `data`")
    random_range = CONFIG.get("debug", {}).get("random_range")
    if random_range is not None:
        random_low, random_high = (float(bound) for bound in random_range)
        if not random_low < random_high:
            raise ValueError("debug.random_range must be [low, high] with low < high")
    value_rng = random.Random(int(run["seed"]))

    buffers = defaultdict(list)
    next_due_s = {name: 0.0 for name in CHANNELS}
    frozen_values = {}
    elapsed_s = 0.0
    started_monotonic = time.monotonic()
    messages = 0
    readings = 0
    suppressed_readings = 0

    def flush(sensor_id, buffer):
        base_ns = buffer[0][0]
        record = {
            "sensor_id": sensor_id,
            "timestamp": base_ns,
            "readings": [
                {"delta": timestamp_ns - base_ns, "value": value}
                for timestamp_ns, value in buffer
            ],
        }
        producer.produce(
            topic=topic,
            value=serialize_record(record),
            on_delivery=delivery_report,
            key=sensor_id,
        )

    while elapsed_s < duration_s:
        sample = model.step(elapsed_s, tick_s)
        active = []
        for anomaly in anomalies:
            progress = anomaly.progress(elapsed_s)
            if progress is not None:
                active.append((anomaly, progress))
        for anomaly, progress in active:
            handler = VALUE_ANOMALIES.get(anomaly.kind)
            if handler is not None:
                handler(sample, anomaly, progress)
        frozen, suppressed, uplink_down = publication_state(active)

        for name, channel in CHANNELS.items():
            while next_due_s[name] <= elapsed_s + 1e-9:
                timestamp_ns = started_unix_ns + int(next_due_s[name] * 1e9)
                next_due_s[name] += channel.period_s
                if uplink_down or name in suppressed:
                    suppressed_readings += 1
                    continue
                if name in frozen and name in frozen_values:
                    value = frozen_values[name]
                elif random_range is not None:
                    value = random_value(name, value_rng, random_low, random_high)
                else:
                    value = finish_value(name, sample[name])
                frozen_values[name] = value
                buffers[name].append((timestamp_ns, value))

        now_ns = started_unix_ns + int(elapsed_s * 1e9)
        for name, buffer in buffers.items():
            if buffer and now_ns - buffer[0][0] >= window_ns:
                flush(name, buffer)
                messages += 1
                readings += len(buffer)
                buffer.clear()

        producer.poll(0)

        elapsed_s += tick_s
        sleep_time = started_monotonic + elapsed_s / speed_factor - time.monotonic()
        if sleep_time > 0:
            time.sleep(sleep_time)

    for name, buffer in buffers.items():
        if buffer:
            flush(name, buffer)
            messages += 1
            readings += len(buffer)
            buffer.clear()

    print(f"[Sensors] Sent {messages} messages, {readings} readings, "
          f"{suppressed_readings} withheld by scripted dropouts.")


def produce_change_driver_messages(producer):
    """Thread function for producing change_driver messages."""
    change_driver = CONFIG.get("change_driver", {})
    if not change_driver.get("enabled", False):
        return

    driver_id = change_driver.get("driver_id", "")
    if not driver_id:
        print("[Control] change_driver is enabled but driver_id is empty.")
        return

    init_delay = change_driver.get("initial_delay", 0.0)
    topic = change_driver.get("topic", "control")

    print(f"[Control] Sleeping for initial delay of {init_delay} seconds...")
    time.sleep(init_delay)

    producer.produce(
        topic=topic,
        value=driver_id.encode('utf-8'),
        on_delivery=delivery_report,
        key="CHDRV"
    )
    producer.poll(0)

    print(f"[Control] Driver changed to '{driver_id}'.")


def produce_shutdown_message(producer):
    shutdown = CONFIG.get("shutdown", {})
    if not shutdown.get("enabled", False):
        return

    topic = shutdown.get("topic", "control")
    payload = ""

    producer.produce(
        topic=topic,
        value=payload,
        on_delivery=delivery_report,
        key="SHUTDOWN"
    )
    producer.poll(0)

    print("[Control] Sent shutdown message.")


def main():
    anomalies = load_anomalies()
    model = build_model()
    producer = Producer(build_producer_config())

    chdrv_enabled = CONFIG.get("change_driver", {}).get("enabled", False)
    shutdown_enabled = CONFIG.get("shutdown", {}).get("enabled", False)
    points_per_second = sum(1.0 / channel.period_s for channel in CHANNELS.values())
    debug = CONFIG.get("debug", {})
    forced_period_s = debug.get("force_period_s")

    print("Starting anomaly test configuration:")
    print(f"- Car: {CONFIG['car_name']}")
    print(f"- Channels: {len(CHANNELS)} of {len(ALL_SENSOR_NAMES)} at {points_per_second:.1f} readings/s")
    if forced_period_s is not None:
        print(f"- DEBUG rate override: every channel every {float(forced_period_s)}s, "
              f"declared sample_rates_hz ignored")
    if debug.get("random_range") is not None:
        low, high = debug["random_range"]
        print(f"- DEBUG random values: every reading uniform in [{low}, {high}], "
              f"drive model and value anomalies ignored")
    if debug.get("only_sensors"):
        print(f"- DEBUG sensor filter: {', '.join(sorted(CHANNELS))}")
    print(f"- Total Time: {CONFIG['run']['duration_s']}s (speed factor {CONFIG['run']['speed_factor']})")
    print(f"- Batch Window: {CONFIG['batch_window_ms']}ms")
    print(f"- Lap Length: {model.lap_duration_s:.1f}s")
    print(f"- Seed: {CONFIG['run']['seed']}")
    print(f"- CHDRV enabled: {chdrv_enabled}")
    print(f"- SHUTDOWN enabled: {shutdown_enabled}")
    print(f"- OAuth enabled: {CONFIG.get('oauth', {}).get('enabled', False)}")

    start_time = time.monotonic()
    started_unix_ns = time.time_ns()

    manifest = build_manifest(anomalies)
    manifest_path = CONFIG_PATH.parent / CONFIG["run"]["manifest_path"]
    print("Scripted timeline:")
    for entry in manifest:
        print(f"- {entry['start_s']:>6.1f}s to {entry['end_s']:>6.1f}s  "
              f"{entry['kind']:<26} {entry['expect']}")
    if manifest_path.exists() and json.loads(manifest_path.read_text(encoding="utf-8")) == manifest:
        print(f"Expected timeline unchanged in {manifest_path.name}")
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Expected timeline written to {manifest_path.name}")
    print(f"Run start: {started_unix_ns} Unix ns (add start_s/end_s for absolute windows)")

    sensor_thread = threading.Thread(
        target=produce_sensor_messages, args=(producer, model, anomalies, started_unix_ns)
    )
    sensor_thread.start()
    if chdrv_enabled:
        control_thread = threading.Thread(target=produce_change_driver_messages, args=(producer,))
        control_thread.start()

    sensor_thread.join()
    if chdrv_enabled:
        control_thread.join()

    print("Generation complete")
    produce_shutdown_message(producer)
    producer.flush()

    actual_time = time.monotonic() - start_time
    print(f"Anomaly test complete in {actual_time:.2f} seconds.")


if __name__ == "__main__":
    main()
