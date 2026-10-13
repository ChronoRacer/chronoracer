import json
import time
import zmq
import pathway as pw
import grpc

from pathway.stdlib.temporal import inactivity_detection
from pathway.internals.datetime_types import Duration
from analysis.shared.config import Config
from loguru import logger

from analysis.shared.proto import telemetry_v1_pb2_grpc
from analysis.shared.proto import  telemetry_v1_pb2
from analysis.shared.connector import GRPCSensorStream

import os

class GRPCSchema(pw.Schema):
    sensor_id: str = pw.column_definition(primary_key=True)
    timestamp_nano: int
    value: float
    driver_id: str


context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
pub_socket.setsockopt(zmq.SNDHWM,10_000)
pub_socket.bind("tcp://127.0.0.1:6767")

# MOVE IT TO CONSTANTS
TOPIC_RULES = b"rules"
TOPIC_PREDICTIONS = b"predictions"
TOPIC_SENSORS = b"sensors"

def publish_to_rules(key,row,time,is_addition):
    if is_addition:
        payload = json.dumps(row).encode("utf-8")
        pub_socket.send_multipart([TOPIC_RULES,payload])


def publish_to_predictions(key,row,time,is_addition):
    if is_addition:
        payload = json.dumps(row).encode("utf-8")
        pub_socket.send_multipart([TOPIC_PREDICTIONS,payload])


def publish_unavailable_sensors(key,row,time,is_addition):
    if is_addition:
        payload = json.dumps({"unique_sensors":[]}).encode("utf-8")
        pub_socket.send_multipart([TOPIC_SENSORS,payload])




@pw.udf
def get_unique_elements(messages_tuple: tuple) -> list:
    return list(set(messages_tuple))


# EVERY AFTER DELTA I SEND NEW SENSORS LIST
# AND I RESET IT AND START ONCE AGAIN FROM EMPTY LIST
DURATION_5_SECONDS_NS = 5 * 1_000_000_000 # 5 SECONDS
DURATION_05_SECONDS_NS = 1 * 500_000_000 # 5 SECONDS
if __name__ == '__main__':
    os.environ["PATHWAY_LOG_STYLE"] = "plain"

    config = Config(CORE_TELEMETRY_URL="127.0.0.1:9470",CLIENT_ID="analysis_id")
    table = pw.io.python.read(
        GRPCSensorStream(config),schema=GRPCSchema,autocommit_duration_ms=100
    )
    logger.info("STARTING PATHWAY STREAMING JOB")

    # SENSORS DETECTION
    inactivity_table = table.inactivity_detection(
        allowed_inactivity_period=Duration(seconds=5),
        refresh_rate=Duration(milliseconds=500)
    )
    pw.io.subscribe(inactivity_table,publish_unavailable_sensors)


    sensor_window = table.windowby(
        pw.this.timestamp_nano,
        window=pw.temporal.tumbling(duration=DURATION_5_SECONDS_NS),
        behavior=pw.temporal.exactly_once_behavior()
    ).reduce(
        window_start=pw.this._pw_window_start,
        window_end=pw.this._pw_window_end,
        _all_messages=pw.reducers.tuple(pw.this.sensor_id),
    ).select(
        unique_sensors=get_unique_elements(pw.this._all_messages)
    )
    pw.io.subscribe(sensor_window,publish_unavailable_sensors)
    pw.run()
    print("TABLE",table)


"""
import threading
import time
import zmq


def heartbeat_worker(ctx, stop_event):
    # Socket jest tworzony i używany wyłącznie w tym wątku.
    sock = ctx.socket(zmq.PUSH)
    sock.connect("inproc://heartbeat")

    start = time.monotonic()

    try:
        while not stop_event.is_set():
            elapsed = time.monotonic() - start
            msg = f"heartbeat: uptime={elapsed:.1f}s"

            sock.send_string(msg)
            print(f"Sent: {msg}")

            # Można przerwać oczekiwanie natychmiast po stop_event.set()
            stop_event.wait(1.0)
    finally:
        sock.close(linger=0)
        print("Heartbeat worker stopped")


def main():
    ctx = zmq.Context()
    stop_event = threading.Event()

    # PULL musi być zbindowany przed uruchomieniem producenta.
    receiver = ctx.socket(zmq.PULL)
    receiver.bind("inproc://heartbeat")

    worker = threading.Thread(
        target=heartbeat_worker,
        args=(ctx, stop_event),
        daemon=False,
    )
    worker.start()

    try:
        # Odbieranie heartbeat przez 5 sekund.
        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:
            if receiver.poll(timeout=200):
                print("Received:", receiver.recv_string())

    except KeyboardInterrupt:
        print("Shutdown requested")

    finally:
        # Graceful shutdown
        stop_event.set()
        worker.join(timeout=3)

        if worker.is_alive():
            print("WARNING: worker did not stop in time")

        receiver.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()

"""