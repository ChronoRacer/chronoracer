from pathlib import Path

import zmq
import json
import zen

context = zmq.Context()
sub_socket = context.socket(zmq.SUB)
sub_socket.setsockopt(zmq.SUBSCRIBE,b"rules")
sub_socket.connect("tcp://127.0.0.1:6767")

project_root = Path(__file__).resolve().parents[2]
rules_path = project_root / "analysis" / "rules" / "rules_files"
print(rules_path)

engine = zen.ZenEngine({"loader":{"type":"fs","path":str(rules_path)}})
response = engine.evaluate("test.json",{"rpm": 10000, "temperature": 50000})
print(response)
while True:
    print("XD")
    topic, raw_payload = sub_socket.recv_multipart()
    data = json.loads(raw_payload.decode("utf-8"))
    print("Received data on topic:", topic.decode("utf-8"))
    print(data)

# TODO retries - when server is not up yet, we should retry to connect to it.
# bucket na reguly i modele