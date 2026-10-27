import tempfile
import time
from pathlib import Path
import orjson
import zmq
from google.cloud import storage
import onnxruntime as ort
import shutil
from loguru import logger
from pydantic import BaseModel
from analysis.shared.config import load_config
from analysis.predictions.predictions_files import FuelPrediction, TyreWearPrediction, LapTimePrediction
from analysis.predictions.predictions_files import PredictionClass
from analysis.predictions.prediction_models import LapDetector
from analysis.shared.contract import (
    NewPredictionJSON,
    PredictionRunningJSON,
    PredictionUnavailableJSON,
)
import requests

from analysis.predictions.prediction_models import MPoint


class Sensor(BaseModel):
    id: str
    min:float | None
    max:float | None
    desc : str

class Value(BaseModel):
    ts_ns : int
    val   : float

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


TELEMETRY_PREDICTIONS_ENDPOINT = "/analysis/v1/predictions"
TELEMETRY_RUNNING_PREDICTIONS_ENDPOINT = "/analysis/v1/predictions/running"
TELEMETRY_UNAVAILABLE_PREDICTIONS_ENDPOINT = "/analysis/v1/predictions/unavailable"

class PredictionsEngine:
    def __init__(self,config):
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.local_dir : Path =  Path(tempfile.mkdtemp(prefix="onnx_"))
        self.sensors  : dict[str,Sensor] = self.fetch_sensor()
        self.predictions : list[PredObj] = [
            PredObj(pred_id="fuel_prediction", is_available=False, pred_class=FuelPrediction()),
            PredObj(pred_id="lap_time_prediction", is_available=False, pred_class=LapTimePrediction()),
            PredObj(pred_id="tyre_wear_prediction", is_available=False, pred_class=TyreWearPrediction())

        ]
        self.config = config


    def fetch_sensor(self):
        sensors = {}
        blob = self.bucket.blob("analysis/sensors.json")
        data_str = blob.download_as_text()
        json_data = orjson.loads(data_str)
        for obj in json_data["sensors"]:
            sensors[obj["name"]] = Sensor(
                id=obj["name"],
                min=obj["min"],
                max=obj["max"],
                desc=obj["description"]
            )
        return sensors
    def import_model(self,model_file:str,prefix : str = "analysis/predictions/models/v1/"):
        for blob in self.client.list_blobs(self.bucket,prefix=prefix):
            if blob.name.endswith(".onnx"):
                blob.download_to_filename(str(self.local_dir / Path(blob.name).name))
                logger.info("Downloaded model {}", blob.name)
                return ort.InferenceSession(
                    str(self.local_dir / model_file ),
                    providers=["CPUExecutionProvider"]
                )
        logger.info("There is no model with name {}",model_file)
        return None

    def on_stop(self):
        if self.local_dir.exists():
            shutil.rmtree(self.local_dir)

    @staticmethod
    def _prepare_state_json(prediction: PredObj, refresh_rate : int | None = None):
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
                headers={"Authorization": "Bearer TOKEN"},
                timeout=2,
            )
            response.raise_for_status()
        except Exception as ex:
            logger.warning("Failed to send {}: {}", what, ex)

    def send_prediction_state(self, prediction: PredObj, refresh_rate : int | None = None):
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
            value=None if value is None else float(value),
        ).model_dump(mode="json")
        self._post(TELEMETRY_PREDICTIONS_ENDPOINT, payload, prediction.pred_id)


    @staticmethod
    def _get_signals(message_lst, sensors):
        signals : dict[str,Value] = {}
        for sensor_dct in message_lst:
            sensor_id = sensor_dct["sensor_id"]
            if sensor_id not in sensors:
                continue

            signal = Value(
                ts_ns=int(sensor_dct["ts"]),
                val=float(sensor_dct["median"])
            )
            current_signal = signals.get(sensor_id)
            if current_signal is None or signal.ts_ns > current_signal.ts_ns:
                signals[sensor_id] = signal
        return signals


    def handle_message(self,message, sensors : set[str],lap_detector: LapDetector):
        # somehow check if we can use prediction - do we have enough sensors
        for pred in self.predictions:
            if pred.pred_class.sensors.issubset(sensors):
                if not pred.is_available:
                    pred.is_available = True
                    self.send_prediction_state(pred, refresh_rate=None)
            else:
                if pred.is_available:
                    pred.is_available = False
                    self.send_prediction_state(pred, refresh_rate=None)

        signals = self._get_signals(message,sensors)
        gps_lat = signals.get("gps_lat")
        gps_lon = signals.get("gps_lon")
        speed   = signals.get("speed")
        # An empty window, a window without GPS
        if gps_lat is None or gps_lon is None or speed is None:
            logger.warning("Empty window, no GPS, or stopped car or there is no speed - skipping lap detection")
            return

        lap_detector.add_track_point(gps_lon.val,gps_lat.val)
        track,p25,p50,p75 = lap_detector.find_track_data()

        if not lap_detector.is_start_set():
            if lap_detector.sensors.issubset(sensors):
                lap_detector.determine_start_on_track(
                    gps_lat.val,
                    gps_lon.val,
                    gps_lat.ts_ns,
                )
        else:
            test_point = MPoint(lat=gps_lat.val,lon=gps_lon.val)
            point = lap_detector.find_next_lap(test_point, gps_lat.ts_ns)
            lap_time = lap_detector.lap_time
            if point is not None:
                logger.info("Lap detected at point: {}", point)
            for pred in self.predictions:
                match pred.pred_class:
                    case FuelPrediction():
                        fuel = signals.get("fuel_l")
                        if fuel is None:
                            logger.warning("Fuel signal is missing, skipping fuel prediction")
                            continue
                        if pred.is_available and fuel is not None:
                            if pred.pred_class.need_prepare():
                                pred.pred_class.prepare({"point": lap_detector.get_start_point()})
                            pred.pred_class.fill({
                                "is_finish_line_crossed": point is not None,
                                "curr_lap": lap_detector.curr_lap_num,
                                "fuel": fuel.val
                            })
                            result = pred.pred_class.process_data({
                                "is_finish_line_crossed": point is not None,
                                "curr_lap": lap_detector.curr_lap_num
                            })
                            if result is not None:
                                laps_left, burn_per_lap, predicted_fuel = result
                                predicted_for_lap = lap_detector.curr_lap_num
                                logger.info(
                                    "Fuel: {:.2f} L at end of lap {} | {:.1f} laps to empty | {:.3f} L/lap",
                                    float(predicted_fuel),
                                    predicted_for_lap,
                                    float(laps_left),
                                    float(burn_per_lap),
                                )
                                if laps_left is not None and burn_per_lap is not None:
                                    instruction = (
                                        f"{float(laps_left):.1f} laps to empty. "
                                        f"Fuel at end of lap {predicted_for_lap}: {float(predicted_fuel):.2f} L "
                                        f"({float(burn_per_lap):.3f} L/lap)"
                                    )
                                    self.send_prediction(
                                        pred,
                                        value=float(predicted_fuel),
                                        instruction=instruction,
                                    )

                    case LapTimePrediction():
                        hv_soc = signals.get("hv_soc")
                        rpm = signals.get("rpm")
                        if hv_soc is None or rpm is None:
                            logger.warning("HV SOC or RPM signal is missing, skipping lap time prediction")
                            continue
                        if pred.is_available and p25 is not None and p50 is not None and p75 is not None and track is not None and hv_soc is not None and rpm is not None:
                            if pred.pred_class.need_prepare():
                                pred.pred_class.prepare({
                                    "p25": p25,
                                    "p50": p50,
                                    "p75": p75,
                                    "track": track,
                                })
                            pred.pred_class.fill({
                                "lon": gps_lon.val,
                                "lat": gps_lat.val,
                                "lap_time": lap_time,
                                "speed": speed.val,
                                "hv_perc": hv_soc.val,
                                "rpm": rpm.val,
                            })
                            result = pred.pred_class.process_data({
                                "is_finish_line_crossed": point is not None,
                                "lap_time": lap_time,
                            })
                            if result is not None:
                                predicted_for_lap = lap_detector.curr_lap_num
                                source_lap = predicted_for_lap - 1
                                logger.info(
                                    "Lap time: lap {} projected at {:.3f} s (splits from lap {})",
                                    predicted_for_lap,
                                    float(result),
                                    source_lap,
                                )
                                instruction = (
                                    f"Lap {predicted_for_lap}: {float(result):.3f} s "
                                    f"(from lap {source_lap} splits)"
                                )
                                self.send_prediction(pred, value=float(result), instruction=instruction)


            # co sie dzieje z modelami keidyc jest przeciety start a kiedy leci nomralne okrazenie





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
    sub_socket.setsockopt(zmq.RCVTIMEO, 60 * 1_000)
    sub_socket.connect("tcp://127.0.0.1:6767")
    logger.info("Predictions engine subscribed to sensor_pred on tcp://127.0.0.1:6767")
    logger.info("Predictions engine running")

    lap_detector = LapDetector(gps_lat="gps_lat", gps_lon="gps_lon")
    while True:
        try:
            start = time.time_ns()
            topic, raw_payload = sub_socket.recv_multipart()
            data = orjson.loads(raw_payload.decode("utf-8"))
            if topic == b"sensor_pred":
                available_sensors = set([msg["sensor_id"] for msg in data])
                pred_engine.handle_message(message=data,sensors=available_sensors,lap_detector=lap_detector)
                # prepare dct sensor->value for windowto pass it to predictions??
                #print((time.time_ns() - start)/1000000000)

        except zmq.Again:
            logger.warning("DISCONNECT - NO MESSAGES FOR DELTA TIME")
            break

            # w jednym oknie moze byc np po dwa jakies wskazania z tego samego sensora??? - pewnie tu tez ostatni przeslny sygnal po ts sort
    pred_engine.on_stop()
    logger.info("Predictions engine stopped")
    # TODO HERE ALSO TIMEOUT FOR SOCKET sub_socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5 sekund
# TODO ASK CURSOR IF MY TABLES IN PATHWAY ARE CORRECT FOR MY  RULES AND PREDICTIONS PROBLEM
if __name__ == "__main__":
    main()
