import orjson

import zmq

class PredictionsEngine:
    def __init__(self):
        pass

context = zmq.Context()
sub_socket = context.socket(zmq.SUB)
sub_socket.setsockopt(zmq.SUBSCRIBE,b"sensor_agg")

sub_socket.connect("tcp://127.0.0.1:6767")

def get_available_sensors(messages:dict) -> set[str]:
    sensors = set()
    for msg in messages:
        sensors.add(msg["sensor_id"])
    return sensors


def handle_message(topic:str,message:dict):
    pass

while True:
    topic, raw_payload = sub_socket.recv_multipart()
    data = orjson.loads(raw_payload.decode("utf-8"))
    if topic == b"sensor_agg":
        available_sensors = get_available_sensors(data)
        handle_message(topic=str(topic),message=data)
