import time

import pathway as pw
import grpc
from analysis.shared.config import Config
from loguru import logger

from analysis.shared.proto import telemetry_v1_pb2_grpc
from analysis.shared.proto import  telemetry_v1_pb2


class GRPCSensorStream(pw.io.python.ConnectorSubject):
    def __init__(self,config: Config) -> None:
        super().__init__()
        self.config = config
        self.max_retries  =  5
        self.retry_delay_seconds = 1
    def run(self):
        while self.max_retries >= 0:
            channel = grpc.insecure_channel(self.config.CORE_TELEMETRY_URL)
            stub = telemetry_v1_pb2_grpc.TelemetryStub(channel)
            try:
                req = telemetry_v1_pb2.WatchRequest(
                    client_id=self.config.CLIENT_ID,
                    all=telemetry_v1_pb2.AllSensors()
                )
                call = stub.Watch(req)
                for event in call:
                    if event.HasField("hello"):
                        logger.info("Connected {}",event.hello.stream_id)
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
                logger.info("GRPC CORE HAS ENDED STREAMING DATA")
                break
            except grpc.RpcError as ex:
                logger.error("Error with GRPC connection to Core {}. Try to reconnect - left {} tries",ex,self.max_retries)
                self.max_retries -= 1
                self.retry_delay_seconds *= 2
                time.sleep(self.retry_delay_seconds)

        logger.info("CLOSING PATHWAY CONNECTOR")
    def on_stop(self):
        # WHEN I USE TEMP FILES I CAN DELETE IT HERE BECAUSE
        # ON STOP SIGNALS WHEN PATHWAY HAS ENDED JOB SO WE DONT NEED TO KEEP THESE VALUES ANYMORE
        pass

