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
                        points = event.point_batch.points
                        if points:
                            now_ns = time.time_ns()
                            latencies_ms = [
                                (now_ns - p.timestamp_nanos) / 1_000_000
                                for p in points
                                if p.timestamp_nanos > 0
                            ]
                            if latencies_ms:
                                avg_latency = sum(latencies_ms) / len(latencies_ms)
                                max_latency = max(latencies_ms)
                                min_latency = min(latencies_ms)
                                if avg_latency > 1500.0:
                                    logger.warning(
                                        "High ingestion latency from Core: avg={:.1f}ms, max={:.1f}ms, min={:.1f}ms ({} points)",
                                        avg_latency, max_latency, min_latency, len(points),
                                    )
                                else:
                                    logger.debug(
                                        "Ingested point_batch ({} points) from Core, latency: avg={:.1f}ms, max={:.1f}ms",
                                        len(points), avg_latency, max_latency,
                                    )
                        for point in points:
                            try:
                                self.next(
                                    sensor_id=point.sensor_id,
                                    timestamp_nano=point.timestamp_nanos,
                                    value=point.value,
                                    driver_id=point.driver_id
                                )
                            except Exception as ex:
                                logger.error("Failed to forward point {} to Pathway: {}", point.sensor_id, ex)
                    elif event.HasField("gap"):
                        logger.warning("gRPC stream gap detected from Core")
                logger.warning("gRPC stream from Core finished. Reconnecting...")
            except grpc.RpcError as ex:
                logger.error(
                    "gRPC error from Core (code={}) - retrying in {}s... Details: {}",
                    ex.code() if hasattr(ex, "code") else "UNKNOWN",
                    self.retry_delay_seconds,
                    ex,
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

