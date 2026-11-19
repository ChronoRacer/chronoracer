import numpy as np
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

from analysis.shared.config import load_config


class GRPCSchema(pw.Schema):
    sensor_id: str = pw.column_definition(primary_key=True)
    timestamp_nano: int
    value: float
    driver_id: str



class BatchedSensorObserver(pw.io.python.ConnectorObserver):
    def __init__(self,socket,topic):
        super().__init__()
        self.socket = socket
        self.topic = topic
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
        self.socket.send_multipart([self.topic,payload])




context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
pub_socket.setsockopt(zmq.SNDHWM,10_000)
pub_socket.bind("tcp://127.0.0.1:6767")
logger.info("ZMQ PUB bound on tcp://127.0.0.1:6767")

TOPIC_SENSOR_AGG = b"sensor_agg"
TOPIC_SENSOR_PRED = b"sensor_pred"

FAST_CELL_NS =500_000_001
SLOW_CELL_NS =1_000_000_001
TOLERANCE_NS = 0
def get_sensor_batch(table, duration: int):
    return (
        table.windowby(
            pw.this.timestamp_nano,
            window=pw.temporal.tumbling(duration=duration),
            behavior=pw.temporal.exactly_once_behavior(shift=TOLERANCE_NS),
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
            window_min_ts_ns=pw.reducers.min(pw.this.timestamp_nano),
            window_max_ts_ns=pw.reducers.max(pw.this.timestamp_nano),
            sample_count=pw.reducers.count()

        )
    )


def _median(vals: tuple[float, ...]) -> float:
    return np.median(vals)
def get_stream_batched_pred(table,duration : int):
    return (
        table.windowby(
            pw.this.timestamp_nano,
            window=pw.temporal.tumbling(duration=duration),
            behavior=pw.temporal.exactly_once_behavior(shift=TOLERANCE_NS),
            instance=pw.this.sensor_id
        ).reduce(
            sensor_id=pw.this._pw_instance,
            ts=pw.this._pw_window_end,
            vals = pw.reducers.sorted_tuple(pw.this.value),
            sample_count=pw.reducers.count()
        ).select(
            pw.this.sensor_id,
            pw.this.ts,
            median=pw.apply(_median,pw.this.vals)
        )
    )

def main():
    logger.info("Data stream starting")
    config = load_config()
    if config is None:
        logger.error("Config is NONE")
        return
    logger.info("Connecting to Core gRPC at {}", config.CORE_TELEMETRY_URL)

    table = pw.io.python.read(
        GRPCSensorStream(config),schema=GRPCSchema,autocommit_duration_ms=100
    )
    logger.info("STARTING PATHWAY STREAMING JOB")

    slow_sensor = get_sensor_batch(table,SLOW_CELL_NS)
    pred_sensor = get_stream_batched_pred(table,FAST_CELL_NS)

    pw.io.python.write(slow_sensor,BatchedSensorObserver(pub_socket,TOPIC_SENSOR_AGG))
    pw.io.python.write(pred_sensor,BatchedSensorObserver(pub_socket,TOPIC_SENSOR_PRED))

    logger.info("Data stream running")
    pw.run()

if __name__ == '__main__':
    main()

