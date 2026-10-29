import time

import pathway as pw
import grpc
from analysis.shared.config import Config
from loguru import logger

from analysis.shared.proto import telemetry_v1_pb2_grpc
from analysis.shared.proto import  telemetry_v1_pb2


class GRPCSensorStream(pw.io.python.ConnectorSubject):
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        self.retry_delay_seconds = 1
        self.max_retry_delay_seconds = 5

    def run(self):
        address_without_prefix = self.config.CORE_TELEMETRY_URL.removeprefix("http://")

        while True:
            channel = grpc.insecure_channel(address_without_prefix)
            try:
                stub = telemetry_v1_pb2_grpc.TelemetryServiceStub(channel)
                req = telemetry_v1_pb2.WatchRequest(
                    client_id=self.config.CLIENT_ID,
                    all=telemetry_v1_pb2.AllSensors()
                )
                call = stub.Watch(req)
                for event in call:
                    if event.HasField("hello"):
                        logger.info("Connected to Core: stream_id={}", event.hello.stream_id)
                        self.retry_delay_seconds = 1
                    elif event.HasField("point_batch"):
                        for point in event.point_batch.points:
                            self.next(
                                sensor_id=point.sensor_id,
                                timestamp_nano=point.timestamp_nanos,
                                value=point.value,
                                driver_id=point.driver_id
                            )
                    elif event.HasField("gap"):
                        pass
                logger.warning("gRPC stream from Core finished. Reconnecting...")
            except grpc.RpcError as ex:
                logger.warning(
                    "Cannot connect to Core ({}) - retrying in {}s...",
                    ex.code() if hasattr(ex, "code") else ex,
                    self.retry_delay_seconds,
                )
            except Exception as ex:
                logger.error(
                    "Unexpected error in gRPC stream: {} - retrying in {}s...",
                    ex,
                    self.retry_delay_seconds,
                )
            finally:
                channel.close()

            time.sleep(self.retry_delay_seconds)
            self.retry_delay_seconds = min(self.retry_delay_seconds * 2, self.max_retry_delay_seconds)

    def on_stop(self):
        pass

