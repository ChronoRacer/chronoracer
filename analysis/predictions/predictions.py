import tempfile
import time
from pathlib import Path
import orjson
import zmq
from bokeh.core.has_props import PropertyDef
from google.cloud import storage
import onnxruntime as ort
import shutil
from loguru import logger
from pydantic import BaseModel
from analysis.shared.config import load_config
from analysis.predictions.predictions_files import (
    BrakeFadePrediction,
    FuelPrediction,
    LapTimePrediction,
    PredictionClass,
    TyreWearPrediction,
    CarFluidsPrediction,
    CoolantTemperaturePrediction,
    LapComparePrediction,
    SpeedOnTurnsPrediction
)
from analysis.predictions.prediction_models import LapDetector, MPoint
from analysis.shared.contract import (
    NewPredictionJSON,
    PredictionRunningJSON,
    PredictionUnavailableJSON,
)
import requests


class Sensor(BaseModel):
    id: str
    min: float | None
    max: float | None
    desc: str


class Value(BaseModel):
    ts_ns: int
    val: float


class PredObj:
    def __init__(
            self,
            pred_id: str,
            is_available: bool,
            pred_class: PredictionClass,
    ):
        self.pred_id = pred_id
        self.is_available = is_available
        self.pred_class = pred_class
        self.state_announced = False
        self.missing_sensors_since: float | None = None


TELEMETRY_PREDICTIONS_ENDPOINT = "/analysis/v1/predictions"
TELEMETRY_RUNNING_PREDICTIONS_ENDPOINT = "/analysis/v1/predictions/running"
TELEMETRY_UNAVAILABLE_PREDICTIONS_ENDPOINT = "/analysis/v1/predictions/unavailable"
MISSING_SENSORS_GRACE_SECONDS = 3.0


class PredictionsEngine:
    def __init__(self, config):
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.local_dir: Path = Path(tempfile.mkdtemp(prefix="onnx_"))
        self.predictions: list[PredObj] = [
            PredObj(pred_id="fuel_prediction", is_available=False, pred_class=FuelPrediction()),
            PredObj(pred_id="lap_time_prediction", is_available=False, pred_class=LapTimePrediction()),
            PredObj(pred_id="tyre_wear_prediction", is_available=False, pred_class=TyreWearPrediction()),
            PredObj(pred_id="brake_fade_prediction", is_available=False, pred_class=BrakeFadePrediction()),
            PredObj(pred_id="car_fluids_prediction", is_available=False, pred_class=CarFluidsPrediction()),
            PredObj(pred_id="coolant_temp_prediction", is_available=False, pred_class=CoolantTemperaturePrediction()),
            PredObj(pred_id="lap_compare_prediction", is_available=False, pred_class=LapComparePrediction()),
            #PredObj(pred_id="speed_on_turns_prediction", is_available=False, pred_class=SpeedOnTurnsPrediction()),
        ]
        self.config = config

    def import_model(self, model_file: str, prefix: str = "analysis/predictions/models/v1/"):
        for blob in self.client.list_blobs(self.bucket, prefix=prefix):
            if blob.name.endswith(".onnx"):
                blob.download_to_filename(str(self.local_dir / Path(blob.name).name))
                logger.info("Downloaded model {}", blob.name)
                return ort.InferenceSession(
                    str(self.local_dir / model_file),
                    providers=["CPUExecutionProvider"],
                )
        logger.info("There is no model with name {}", model_file)
        return None

    def on_stop(self):
        if self.local_dir.exists():
            shutil.rmtree(self.local_dir)

    @staticmethod
    def _prepare_state_json(prediction: PredObj, refresh_rate: str | None = None):
        timestamp = time.time_ns() // 1_000
        if prediction.is_available:
            state_json = PredictionRunningJSON(
                timestamp=timestamp,
                prediction_id=prediction.pred_id,
                name=prediction.pred_class.name,
                refresh_rate=refresh_rate,
            )
        else:
            state_json = PredictionUnavailableJSON(
                timestamp=timestamp,
                prediction_id=prediction.pred_id,
                info="Prediction is unavailable due to missing required sensors",
            )
        return state_json.model_dump(mode="json")

    def _post(self, endpoint: str, payload: dict, what: str) -> None:
        try:
            response = requests.post(
                self.config.TELEMETRY_URL + endpoint,
                json=payload,
                headers={"Authorization": f"Bearer {self.config.ANALYSIS_INGEST_TOKEN}"},
                timeout=2,
            )
            response.raise_for_status()
        except Exception as ex:
            logger.warning("Failed to send {}: {}", what, ex)

    def send_prediction_state(self, prediction: PredObj, refresh_rate: str | None = None):
        endpoint = (
            TELEMETRY_RUNNING_PREDICTIONS_ENDPOINT
            if prediction.is_available
            else TELEMETRY_UNAVAILABLE_PREDICTIONS_ENDPOINT
        )
        self._post(endpoint, self._prepare_state_json(prediction, refresh_rate), prediction.pred_id)

    def send_prediction(
        self,
        prediction: PredObj,
        value: float | None,
        instruction: str | None,
    ) -> None:
        payload = NewPredictionJSON(
            timestamp=time.time_ns() // 1_000,
            prediction_id=prediction.pred_id,
            sensors_id=sorted(prediction.pred_class.sensors),
            instruction=instruction,
            value=None if value is None else round(float(value), 3),
        ).model_dump(mode="json")
        self._post(TELEMETRY_PREDICTIONS_ENDPOINT, payload, prediction.pred_id)

    @staticmethod
    def _get_signals(message_lst, sensors):
        signals: dict[str, Value] = {}
        for sensor_dct in message_lst:
            sensor_id = sensor_dct["sensor_id"]
            if sensor_id not in sensors:
                continue

            signal = Value(
                ts_ns=int(sensor_dct["ts"]),
                val=float(sensor_dct["median"]),
            )
            current_signal = signals.get(sensor_id)
            if current_signal is None or signal.ts_ns > current_signal.ts_ns:
                signals[sensor_id] = signal
        return signals

    def handle_message(self, message, sensors: set[str], lap_detector: LapDetector):
        now = time.monotonic()
        for pred in self.predictions:
            sensors_present = pred.pred_class.sensors.issubset(sensors)
            if sensors_present:
                pred.missing_sensors_since = None
                available = True
            else:
                if pred.missing_sensors_since is None:
                    pred.missing_sensors_since = now
                if now - pred.missing_sensors_since < MISSING_SENSORS_GRACE_SECONDS:
                    continue
                available = False
            if available == pred.is_available and pred.state_announced:
                continue
            pred.is_available = available
            pred.state_announced = True
            self.send_prediction_state(
                pred,
                refresh_rate=pred.pred_class.refresh_rate if available else None,
            )

        signals = self._get_signals(message, sensors)
        gps_lat = signals.get("GPS_LAT_DEG")
        gps_lon = signals.get("GPS_LON_DEG")
        point = None
        if gps_lat is None or gps_lon is None:
            logger.warning("No GPS in window - skipping lap detection")
        else:
            start_was_set = lap_detector.is_start_set()
            if not start_was_set:
                lap_detector.determine_start_on_track(
                    gps_lat.val,
                    gps_lon.val,
                    gps_lat.ts_ns,
                )
            if lap_detector.is_start_set():
                # Ignore the GPS warm-up used to choose the start point.
                lap_detector.add_track_point(gps_lon.val, gps_lat.val)
            if start_was_set:
                test_point = MPoint(lat=gps_lat.val, lon=gps_lon.val)
                point = lap_detector.find_next_lap(test_point, gps_lat.ts_ns)
                if point is not None:
                    logger.info("Lap detected at point: {}", point)

        lap_ready = lap_detector.is_start_set()
        track, p25, p50, p75 = lap_detector.find_track_data()
        lap_ctx = {
            "is_finish_line_crossed": point is not None,
            "curr_lap": lap_detector.curr_lap_num,
            "lap_time": lap_detector.lap_time,
            "start_point": lap_detector.get_start_point(),
            "track": track,
            "track_meters": lap_detector.track_meters,
            "project_to_meters" : lap_detector._project_to_meters,
            "p25": p25,
            "p50": p50,
            "p75": p75,
            "turns_lst": lap_detector.turns_lst,
        }

        for pred in self.predictions:
            if not pred.is_available:
                continue
            if pred.pred_class.needs_lap and not lap_ready:
                continue
            run_result = pred.pred_class.run(signals, lap_ctx)
            if run_result is not None:
                self.send_prediction(pred, value=run_result.value, instruction=run_result.instruction)


def main():
    logger.info("Starting predictions engine")
    config = load_config()
    if config is None:
        logger.error("Failed to load config in predictions engine, exiting")
        return
    pred_engine = PredictionsEngine(config)

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"sensor_pred")
    sub_socket.connect("tcp://127.0.0.1:6767")
    logger.info("Predictions engine subscribed to sensor_pred on tcp://127.0.0.1:6767")
    logger.info("Predictions engine running")

    lap_detector = LapDetector(gps_lat="GPS_LAT_DEG", gps_lon="GPS_LON_DEG")
    while True:
        try:
            start = time.time_ns()
            topic, raw_payload = sub_socket.recv_multipart()
            data = orjson.loads(raw_payload.decode("utf-8"))
            if topic == b"sensor_pred":
                available_sensors = set([msg["sensor_id"] for msg in data])
                pred_engine.handle_message(message=data, sensors=available_sensors, lap_detector=lap_detector)

        except Exception as ex:
            print(ex)
            logger.warning("Predictions engine timeout waiting for messages, retrying...")

    pred_engine.on_stop()
    logger.info("Predictions engine stopped")


if __name__ == "__main__":
    main()
