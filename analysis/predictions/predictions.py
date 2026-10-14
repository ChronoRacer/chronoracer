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
from sklearn.datasets import load_iris


class PredictionsEngine:
    def __init__(self):
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.local_dir : Path =  Path(tempfile.mkdtemp(prefix="onnx_"))
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


def use_model(model_name: str):
    model_session = pred_engine.import_model(model_name)
    if model_session is None:
        logger.error("STH")
        return

pred_engine = PredictionsEngine()

context = zmq.Context()
sub_socket = context.socket(zmq.SUB)
sub_socket.setsockopt(zmq.SUBSCRIBE,b"sensor_agg")
sub_socket.connect("tcp://127.0.0.1:6767")


pred_engine.on_stop()




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