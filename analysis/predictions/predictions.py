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
from analysis.predictions.predictions_files import FuelPred, TyreWearPred, LapTimePred
from analysis.predictions.predictions_files import PredictionClass
from analysis.ml.models import LapDetector
from analysis.shared.contract import PredictionRunningJSON, PredictionUnavailableJSON
import requests

class Sensor(BaseModel):
    id: str
    min:float | None
    max:float | None
    desc : str

class Value(BaseModel):
    ts_ns : int
    val   : float

class PredObj(BaseModel):
    pred_id : str
    is_available : bool
    pred_class : PredictionClass



TELEMETRY_PREDICTIONS_ENDPOINT = "/predictions"
TELEMETRY_RUNNING_PREDICTIONS_ENDPOINT = "/predictions/running"
TELEMETRY_UNAVAILABLE_PREDICTIONS_ENDPOINT = "/predictions/unavailable"

class PredictionsEngine:
    def __init__(self,config):
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.local_dir : Path =  Path(tempfile.mkdtemp(prefix="onnx_"))
        self.sensors  : dict[str,Sensor] = self.fetch_sensor()
        self.predictions : list[PredObj] = [
            PredObj(pred_id="fuel_prediction", is_available=False, pred_class=FuelPred()),
            PredObj(pred_id="lap_time_prediction", is_available=False, pred_class=LapTimePred()),
            PredObj(pred_id="tyre_wear_prediction", is_available=False, pred_class=TyreWearPred())

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

    def send_prediction_state(self, prediction: PredObj, refresh_rate : int | None = None):
        with requests.Session() as s:
            s.headers.update({"Authorization": "Bearer TOKEN"})
            if prediction.is_available:
                payload = self._prepare_state_json(prediction, refresh_rate)
                req = s.post(self.config.TELEMETRY_URL + TELEMETRY_RUNNING_PREDICTIONS_ENDPOINT, json=payload, timeout=2)
                try:
                    req.raise_for_status()
                except Exception as ex:
                    pass  # TODO
            else:
                payload = self._prepare_state_json(prediction, refresh_rate)
                req = s.post(self.config.TELEMETRY_URL + TELEMETRY_UNAVAILABLE_PREDICTIONS_ENDPOINT, json=payload, timeout=2)
                try:
                    req.raise_for_status()
                except Exception as ex:
                    pass  # TODO


    @staticmethod
    def _get_signals(message_lst,sensors):
        signals : dict[str,Value] = {}
        for sensor_dct in message_lst:
            if sensor_dct["sensor_id"] in sensors:
                signals[sensor_dct["sensor_id"]] = Value(
                    ts_ns=int(sensor_dct["ts"]),
                    val=float(sensor_dct["median"])
                )
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
        #if lap_detector.sensors.issubset(sensors):
        #    if lap_detector.determine_start_on_track(signals["gps_lat"],signals["gps_lon"],signals["speed"]):
        #        pass
        #    else:
        #        pass


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
    sub_socket.setsockopt(zmq.RCVTIMEO, 5000)
    sub_socket.connect("tcp://127.0.0.1:6767")
    logger.info("Predictions engine subscribed to sensor_agg on tcp://127.0.0.1:6767")
    logger.info("Predictions engine running")

    lap_detector = LapDetector(gps_lat="gps_lat",gps_lon="gps_lon",speed="speed")
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
