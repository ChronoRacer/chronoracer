import json
import time
import zmq
import pathway as pw
import grpc
from analysis.shared.config import Config
from loguru import logger

from analysis.shared.proto import telemetry_v1_pb2_grpc
from analysis.shared.proto import  telemetry_v1_pb2
from analysis.shared.connector import GRPCSensorStream, GRPCSchema



context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
pub_socket.setsockopt(zmq.SNDHWM,10_000)
pub_socket.bind("tcp://127.0.0.1:6767")

# MOVE IT TO CONSTANTS
TOPIC_RULES = b"rules"
TOPIC_PREDICTIONS = b"predictions"


def publish_to_rules(key,row,time,is_addition):
    if is_addition:
        payload = json.dumps(row).encode("utf-8")
        pub_socket.send_multipart([TOPIC_RULES,payload])


def publish_to_predictions(key,row,time,is_addition):
    if is_addition:
        payload = json.dumps(row).encode("utf-8")
        pub_socket.send_multipart([TOPIC_PREDICTIONS,payload])

if __name__ == '__main__':
    print("WORKING")
    config = Config(CORE_TELEMETRY_URL="127.0.0.1:9470",CLIENT_ID="analysis_id")
    table = pw.io.python.read(
        GRPCSensorStream(config),schema=GRPCSchema,autocommit_duration_ms=100
    )
    pw.io.csv.write(table,"output.csv")

    pw.io.subscribe(table,publish_to_rules)
    pw.io.subscribe(table,publish_to_predictions)
    pw.run()
    print("TABLE",table)
    #connector = GRPCSensorStream(config)
    #connector.run()