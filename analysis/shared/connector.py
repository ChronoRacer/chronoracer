import pathway as pw
import grpc
from analysis.shared.config import Config
from loguru import logger

from analysis.shared.proto import telemetry_v1_pb2_grpc
from analysis.shared.proto import  telemetry_v1_pb2

class GRPCSchema(pw.Schema):
        sensor_id : str = pw.column_definition(primary_key=True)
        timestamp_nano : int
        value: float
        driver_id:str

class GRPCSensorStream(pw.io.python.ConnectorSubject):
    def __init__(self,config: Config) -> None:
        super().__init__()
        self.config = config
    def run(self):
        channel = grpc.insecure_channel("127.0.0.1:9470")
        while True:
            try:
                stub = telemetry_v1_pb2_grpc.TelemetryStub(channel)
                req = telemetry_v1_pb2.WatchRequest(
                    client_id="XD",
                    all=telemetry_v1_pb2.AllSensors()
                )
                resp = stub.Watch(req)
                for r in resp:
                    print(r)
            except grpc.RpcError as ex:
                logger.error("Error{}",ex)
            # TODO RECONNECT

if __name__ == '__main__':
    print("WORKING")
    config = Config(CORE_TELEMETRY_URL="URL")
    connector = GRPCSensorStream(config)
    connector.run()