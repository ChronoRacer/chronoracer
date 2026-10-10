import zmq
import json

context = zmq.Context()
sub_socket = context.socket(zmq.SUB)

sub_socket.setsockopt(zmq.SUBSCRIBE,b"rules")
sub_socket.connect("tcp://127.0.0.1:6767")


while True:
    topic, raw_payload = sub_socket.recv_multipart()
    data = json.loads(raw_payload.decode("utf-8"))
    print("Received data on topic:", topic.decode("utf-8"))
    print(data)