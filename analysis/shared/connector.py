import pathway as pw
import grpc
from analysis.shared.config import Config


class GRPCSensorStream(pw.io.python.ConnectorSubject):
    def __init__(self,config: Config) -> None:
        super().__init__()
        self.config = config
    def run(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = greet_pb2_grpc.GreeterStub(channel)

        req = greet_pb2.SubReq(topic="nowy topic")
        resp = stub.Subscribe(req)
        for r in resp:
            print(r.message)