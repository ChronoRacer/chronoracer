#!/usr/bin/env python3
"""Kafka load generator with the real sensor names and a drive model behind the values.

Sends Avro sensor batches to the `data` topic at a configured number of points per
second. Each message is one sensor's readings (a random count per message), so the
message rate varies while the point rate holds. Several instances can run in parallel,
see README.md.
"""

import argparse
import io
import json
import math
import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

ROOT = Path(__file__).resolve().parents[2]
PARSED_SCHEMA = parse_schema(json.loads((ROOT / "core" / "avro" / "sensor_data.avsc").read_text()))
CONFIG_PATH = Path(__file__).resolve().parent / "config.json"

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
    minimum: float | None
    maximum: float | None


@dataclass(frozen=True)
class Segment:
    target_kph: float
    duration_s: float
    radius_m: float | None
    # 1 turns left, -1 turns right, 0 is a straight. Left is positive, as in the channel file.
    turn: int


def load_channels(config):
    """Sensors from the car's channel file, in file order."""
    channels_path = (CONFIG_PATH.parent / config["channels_file"]).resolve()
    definitions = json.loads(channels_path.read_text(encoding="utf-8"))["sensors"]
    # Kafka `data` keys are at most 12 ASCII characters (docs/component-kafka.md).
    too_long = sorted(d["name"] for d in definitions if len(d["name"].encode("ascii")) > 12)
    if too_long:
        raise ValueError(f"sensor ids too long for the Kafka key: {too_long}")
    return {
        d["name"]: Channel(name=d["name"], minimum=d["min"], maximum=d["max"])
        for d in definitions
    }


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


def build_model(config, seed):
    lap = tuple(
        Segment(
            target_kph=float(segment["target_kph"]),
            duration_s=float(segment["duration_s"]),
            radius_m=float(segment["radius_m"]) if segment.get("radius_m") else None,
            turn=int(segment.get("turn", 0)),
        )
        for segment in config["lap"]
    )
    return DriveModel(car=config["car"], lap=lap, seed=seed)


def build_producer_config(config):
    producer_config = {
        "bootstrap.servers": config["broker"],
        "linger.ms": 5,
        "batch.num.messages": 500,
        "queue.buffering.max.messages": 100000,
        "compression.type": "lz4",
    }

    oauth = config.get("oauth")
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
            "sasl.oauthbearer.extensions": f"audience={oauth['extension_audience']}",
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


# Same bucket bounds as Core's pipeline report (docs/component-core.md), so the two compare directly.
LATENCY_BOUNDS_MS = (1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000)


class LatencyHistogram:
    def __init__(self):
        self.buckets = [0] * (len(LATENCY_BOUNDS_MS) + 1)
        self.max_ms = 0.0

    def record(self, ms):
        index = next((i for i, bound in enumerate(LATENCY_BOUNDS_MS) if ms <= bound), len(LATENCY_BOUNDS_MS))
        self.buckets[index] += 1
        self.max_ms = max(self.max_ms, ms)

    def _bound(self, quantile):
        count = sum(self.buckets)
        rank = max(1, math.ceil(quantile * count))
        seen = 0
        for index, bucket in enumerate(self.buckets):
            seen += bucket
            if seen >= rank:
                return f"<={LATENCY_BOUNDS_MS[index]}" if index < len(LATENCY_BOUNDS_MS) else f">{LATENCY_BOUNDS_MS[-1]}"
        return "?"

    def __str__(self):
        count = sum(self.buckets)
        if count == 0:
            return "n=0"
        return f"p50{self._bound(0.5)},p99{self._bound(0.99)},max={self.max_ms:.0f},n={count}"


class SendStats:
    """Sensor message counters for the periodic report and the final summary.

    Delivery callbacks run inside `poll`/`flush`, which several threads call, hence the lock.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.total = self._fresh()
        self.window = self._fresh()

    @staticmethod
    def _fresh():
        return {
            "started": time.monotonic(),
            "messages": 0,
            "points": 0,
            "behind_s": 0.0,
            "delivered": 0,
            "failed": 0,
            "delivery": LatencyHistogram(),
        }

    def sent(self, points, behind_s):
        with self.lock:
            for counters in (self.total, self.window):
                counters["messages"] += 1
                counters["points"] += points
                counters["behind_s"] = max(counters["behind_s"], behind_s)

    def on_delivery(self, err, msg):
        with self.lock:
            if err is not None:
                if self.window["failed"] == 0:
                    print(f"Message delivery failed: {err} (further failures this window are only counted)")
                for counters in (self.total, self.window):
                    counters["failed"] += 1
                return
            latency_s = msg.latency()
            for counters in (self.total, self.window):
                counters["delivered"] += 1
                if latency_s is not None:
                    counters["delivery"].record(latency_s * 1000.0)

    @staticmethod
    def _line(counters, queued):
        elapsed = max(time.monotonic() - counters["started"], 1e-3)
        return (
            f"window_s={elapsed:.1f} sent_pts_s={counters['points'] / elapsed:.0f} "
            f"sent_msg_s={counters['messages'] / elapsed:.0f} behind_max_s={counters['behind_s']:.3f} "
            f"queued={queued} delivered={counters['delivered']} failed={counters['failed']} "
            f"delivery_ms={counters['delivery']}"
        )

    def report_window(self, queued):
        with self.lock:
            line = self._line(self.window, queued)
            self.window = self._fresh()
        print(f"[Report] {line}")

    def report_total(self, queued):
        with self.lock:
            line = self._line(self.total, queued)
        print(f"[Summary] {line}")


def produce(producer, on_delivery=delivery_report, **kwargs):
    """Produce, waiting for queue space instead of failing when Kafka lags behind."""
    while True:
        try:
            producer.produce(on_delivery=on_delivery, **kwargs)
            return
        except BufferError:
            producer.poll(0.05)


def finish_value(channel, value):
    """Clamp to the channel's declared range so every point is one Core accepts."""
    if channel.minimum is not None and channel.maximum is not None:
        value = clamp(value, channel.minimum, channel.maximum)
    return float(round(value)) if channel.name in INTEGER_CHANNELS else value


def produce_sensor_messages(producer, config, channels, model, rng, load, stats, report_interval_s):
    """Thread function for producing Avro sensor messages at the configured point rate.

    A message is one random sensor with a random number of readings spread over the
    batch window that ended now. The next message is due `points / rate` seconds after
    the previous one, so the total follows the rate whatever the per-message counts are.
    """
    topic = config["sensor_topic"]
    names = list(channels)
    rate = load["points_per_second"]
    min_points = load["min_points_per_message"]
    max_points = load["max_points_per_message"]
    duration_s = load["duration_s"]
    tick_s = load["tick_ms"] / 1000.0
    window_ns = int(config["batch_window_ms"]) * 1_000_000
    # Start mid-lap at a random point so parallel instances do not drive in lockstep.
    lap_offset_s = rng.uniform(0.0, model.lap_duration_s)

    history = deque(maxlen=int(window_ns / 1e9 / tick_s) + 2)
    first_tick = 0  # tick index of history[0]
    model_ticks = 0

    started = time.monotonic()
    started_ns = time.time_ns()
    next_due = started
    next_report = started + report_interval_s
    messages = 0
    points = 0

    while True:
        now = time.monotonic()
        elapsed_s = now - started
        if elapsed_s >= duration_s:
            break
        if now >= next_report:
            stats.report_window(len(producer))
            next_report += report_interval_s
        # How late this message is against the pacing schedule. Growing means this process
        # cannot generate the configured rate: add instances instead of reading it as Kafka or Core.
        behind_s = max(0.0, now - next_due)

        while model_ticks * tick_s <= elapsed_s:
            if len(history) == history.maxlen:
                first_tick += 1
            history.append(model.step(lap_offset_s + model_ticks * tick_s, tick_s))
            model_ticks += 1

        name = rng.choice(names)
        channel = channels[name]
        count = rng.randint(min_points, max_points)
        # Strictly increasing offsets inside (0, window], the last one at or before now.
        deltas = sorted(rng.sample(range(1, window_ns + 1), count)) if count <= window_ns else []
        now_ns = started_ns + int(elapsed_s * 1e9)
        base_ns = now_ns - window_ns
        readings = []
        for delta in deltas:
            point_elapsed_s = elapsed_s - (window_ns - delta) / 1e9
            index = clamp(int(point_elapsed_s / tick_s) - first_tick, 0, len(history) - 1)
            readings.append({"delta": delta, "value": finish_value(channel, history[index][name])})

        produce(
            producer,
            on_delivery=stats.on_delivery,
            topic=topic,
            value=serialize_record({"sensor_id": name, "timestamp": base_ns, "readings": readings}),
            key=name,
        )
        messages += 1
        points += count
        stats.sent(count, behind_s)
        producer.poll(0)

        next_due += count / rate
        sleep_time = next_due - time.monotonic()
        if sleep_time > 0:
            time.sleep(sleep_time)

    actual_s = time.monotonic() - started
    print(f"[Sensors] Sent {messages} messages, {points} points in {actual_s:.1f}s "
          f"({points / actual_s:.0f} points/s, target {rate:g}).")


def produce_change_driver_messages(producer, config):
    """Thread function for producing change_driver messages."""
    change_driver = config.get("change_driver", {})
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

    produce(producer, topic=topic, value=driver_id.encode("utf-8"), key="CHDRV")
    producer.poll(0)

    print(f"[Control] Driver changed to '{driver_id}'.")


def produce_shutdown_message(producer, config):
    shutdown = config.get("shutdown", {})
    if not shutdown.get("enabled", False):
        return

    produce(producer, topic=shutdown.get("topic", "control"), value="", key="SHUTDOWN")
    producer.poll(0)

    print("[Control] Sent shutdown message.")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="config file (default: config.json)")
    parser.add_argument("--rate", type=float, help="points per second, overrides load.points_per_second")
    parser.add_argument("--duration", type=float, help="seconds to run, overrides load.duration_s")
    parser.add_argument("--seed", type=int, help="random seed, overrides load.seed (default: random)")
    parser.add_argument("--no-control", action="store_true",
                        help="skip change_driver and shutdown messages (for extra parallel instances)")
    parser.add_argument("--report-interval", type=float, default=10.0,
                        help="seconds between [Report] lines (default: 10)")
    args = parser.parse_args()
    if args.report_interval <= 0:
        parser.error("--report-interval must be positive")
    return args


def main():
    global CONFIG_PATH
    args = parse_args()
    CONFIG_PATH = args.config.resolve()
    config = json.loads(CONFIG_PATH.read_text())

    load = dict(config["load"])
    if args.rate is not None:
        load["points_per_second"] = args.rate
    if args.duration is not None:
        load["duration_s"] = args.duration
    if args.seed is not None:
        load["seed"] = args.seed
    if load.get("seed") is None:
        load["seed"] = random.SystemRandom().randrange(2**32)
    if load["points_per_second"] <= 0:
        raise ValueError("points_per_second must be positive")
    if not 1 <= load["min_points_per_message"] <= load["max_points_per_message"]:
        raise ValueError("need 1 <= min_points_per_message <= max_points_per_message")
    if not 0 < int(config["batch_window_ms"]) * 1_000_000 <= MAX_DELTA_NS:
        raise ValueError("batch_window_ms must be in 1..2000, the delta cap on `data`")

    rng = random.Random(load["seed"])
    channels = load_channels(config)
    model = build_model(config, load["seed"])
    producer = Producer(build_producer_config(config))

    control = not args.no_control
    chdrv_enabled = control and config.get("change_driver", {}).get("enabled", False)
    shutdown_enabled = control and config.get("shutdown", {}).get("enabled", False)

    print("Starting load test configuration:")
    print(f"- Car: {config['car_name']}")
    print(f"- Sensors: {len(channels)}")
    print(f"- Rate: {load['points_per_second']:g} points/s, "
          f"{load['min_points_per_message']}-{load['max_points_per_message']} points per message")
    print(f"- Total Time: {load['duration_s']}s")
    print(f"- Batch Window: {config['batch_window_ms']}ms")
    print(f"- Seed: {load['seed']}")
    print(f"- CHDRV enabled: {chdrv_enabled}")
    print(f"- SHUTDOWN enabled: {shutdown_enabled}")
    print(f"- OAuth enabled: {config.get('oauth', {}).get('enabled', False)}")

    stats = SendStats()
    sensor_thread = threading.Thread(
        target=produce_sensor_messages,
        args=(producer, config, channels, model, rng, load, stats, args.report_interval),
    )
    sensor_thread.start()
    if chdrv_enabled:
        control_thread = threading.Thread(target=produce_change_driver_messages, args=(producer, config))
        control_thread.start()

    sensor_thread.join()
    if chdrv_enabled:
        control_thread.join()

    print("Generation complete")
    if shutdown_enabled:
        produce_shutdown_message(producer, config)
    producer.flush()
    stats.report_total(len(producer))


if __name__ == "__main__":
    main()
