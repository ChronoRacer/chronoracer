import tempfile
from pathlib import Path
import orjson
import zmq
from google.cloud import storage
import onnxruntime as ort
import shutil
import numpy as np
from loguru import logger
from onnxruntime import InferenceSession
from pydantic import BaseModel
from sklearn.datasets import load_iris
from shared.config import load_config


class Sensor(BaseModel):
    id: str
    min:float | None
    max:float | None
    desc : str


TELEMETRY_PREDICTIONS_ENDPOINT = "/predictions"

class PredictionsEngine:
    def __init__(self,config):
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.local_dir : Path =  Path(tempfile.mkdtemp(prefix="onnx_"))
        self.sensors  : dict[str,Sensor] = self.fetch_sensor()
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
                return ort.InferenceSession(
                    str(self.local_dir / model_file ),
                    providers=["CPUExecutionProvider"]
                )
        logger.info("There is no model with name {}",model_file)
        return None

    def on_stop(self):
        if self.local_dir.exists():
            shutil.rmtree(self.local_dir)




def main():
    config = load_config()
    if config is None:
        logger.error("Missing config, exiting")
        return
    pred_engine = PredictionsEngine(config)

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"sensor_agg")
    sub_socket.connect("tcp://127.0.0.1:6767")

    pred_engine.on_stop()


if __name__ == "__main__":
    main()


"""

def get_available_sensors(messages:dict) -> set[str]:
    sensors = set()
    for msg in messages:
        sensors.add(msg["sensor_id"])
    return sensors


def handle_message(topic:str,message:dict):
    pass

while True:
    topic, raw_payload = sub_socket.recv_multipart()
    data = orjson.loads(raw_payload.decode("utf-8"))
    if topic == b"sensor_agg":
        available_sensors = get_available_sensors(data)
        handle_message(topic=str(topic),message=data)


"""