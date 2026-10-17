import orjson
from typing import Any
from enum import Enum, StrEnum
import zmq
import pathway as pw
import grpc
from panel.pane import media
from pathway import Pointer

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



class BatchedSensorObserver(pw.io.python.ConnectorObserver):
    def __init__(self):
        super().__init__()
        self.buffer : list[dict] = []
    def on_change(self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool) -> None:
        if not is_addition:
            return
        self.buffer.append(row)
    def on_end(self) -> None:
        self._flush()
    def on_time_end(self, time: int) -> None:
        self._flush()

    def _flush(self) -> None:
        batch_to_send = self.buffer.copy()
        self.buffer.clear()
        payload = orjson.dumps(batch_to_send)
        pub_socket.send_multipart([TOPIC_SENSOR_AGG,payload])




context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
pub_socket.setsockopt(zmq.SNDHWM,10_000)
pub_socket.bind("tcp://127.0.0.1:6767")

# MOVE IT TO CONSTANTS
TOPIC_SENSOR_AGG = b"sensor_agg"


def publish_sensor_agg(key,row,time,is_addition):
    if is_addition:
        payload = orjson.dumps(row)
        pub_socket.send_multipart([TOPIC_SENSOR_AGG,payload])


SLOW_CELL_NS =500_000_001
FAST_CELL_NS =1_000_000_001
def get_sensor_batch(slow:bool):
    duration = SLOW_CELL_NS if slow else FAST_CELL_NS
    return (
        table.windowby(
            pw.this.timestamp_nano,
            window=pw.temporal.tumbling(duration=duration),
            behavior=pw.temporal.exactly_once_behavior(),
            instance=pw.this.sensor_id
        ).reduce(
            sensor_id=pw.this._pw_instance,
            window_start_ns=pw.this._pw_window_start,
            window_end_ns=pw.this._pw_window_end,
            vals=pw.reducers.sorted_tuple(
                pw.make_tuple(
                    pw.this.timestamp_nano,
                    pw.this.value
                )
            ),
            window_min_val=pw.reducers.min(pw.this.value),
            window_max_val=pw.reducers.max(pw.this.value),
            # MIN AND MAX ARE TO CHECK IF WE HAVE TO SCAN WINDOW-VALUES
            # BECAUSE WHEN WE COMPARE MIN/MAX AND THERE WILL BE NO RULES VIOLATION
            # THERE OTHER VALUES BETWEEN ALSO WONT VIOLATE IT

            window_min_ts_ns=pw.reducers.min(pw.this.timestamp_nano),
            window_max_ts_ns=pw.reducers.max(pw.this.timestamp_nano),
            sample_count=pw.reducers.count()

        )
    )

if __name__ == '__main__':
    config = Config(CORE_TELEMETRY_URL="127.0.0.1:9470",CLIENT_ID="analysis_id")
    table = pw.io.python.read(
        GRPCSensorStream(config),schema=GRPCSchema,autocommit_duration_ms=100
    )
    logger.info("STARTING PATHWAY STREAMING JOB")
    # i can make other windows for predictions separate from these below


    # finally i have to get knowledge about slow and fast sensors
    # so map with sensors and kind ??
    #fast_sensor = get_sensor_batch(slow=False)



    slow_sensor = get_sensor_batch(slow=True)
    pw.io.python.write(slow_sensor,BatchedSensorObserver())
    pw.run()

#GRPC ---> PATHWAY ZBIERA ODSZUMIONY SYGNAŁ W PACZKI PO X NS ----> PACZKA IDZIE DO GORULES ---> WYKRYWANIE UTRZYAMNIA WARTOSC DLA PROGU PRZEZ DT
# napewno też min samples w oknie
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


