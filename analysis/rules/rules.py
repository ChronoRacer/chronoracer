from pathlib import Path
from collections import  defaultdict
import zmq
import json
import zen
from pprint import pformat

from google.cloud import storage
from pydantic import BaseModel
from datetime import timedelta
class RuleObj(BaseModel):
    is_active : bool = False
    rule_json: dict
    sensors : set[str]


class RulesEngine:
    def __init__(self):
        self.rules : dict[str,RuleObj] = defaultdict(RuleObj)
        rules = defaultdict(RuleObj)
        client = storage.Client("chronoracer")
        bucket = client.bucket("chronoracer-artifacts")
        for blob in client.list_blobs(bucket,prefix="analysis/rules/"):
            if blob.name.endswith(".json"):
                rule_json = json.loads(blob.download_as_text())
                sensors = set(self._get_mandatory_sensors(rule_json))
                rule = RuleObj(
                    rule_json=rule_json,
                    sensors=sensors
                )
                rules[str(blob.name).split("/")[-1]] = rule
        self.rules = rules

    @staticmethod
    def _get_mandatory_sensors(rules:dict):
        sensors = []
        nodes = rules["nodes"]
        for node in nodes:
            if node["name"] == "Table":
                inputs = node["content"]["inputs"]
                for input in inputs:
                    sensors.append(input["field"])
        return sensors

    def get_rules_files(self) -> dict[str,RuleObj]:
        return self.rules


    def loader(self,key:str):
        return self.rules[key].rule_json


context = zmq.Context()
sub_socket = context.socket(zmq.SUB)
sub_socket.setsockopt(zmq.SUBSCRIBE,b"sensor_agg")

sub_socket.connect("tcp://127.0.0.1:6767")
rules_engine = RulesEngine()

engine = zen.ZenEngine({"loader":rules_engine.loader})
response = engine.evaluate("test.json",{"rpm": 10000, "temperature": 50000})
print(response)


def get_available_sensors(messages:dict) -> set[str]:
    sensors = set()
    for msg in messages:
        sensors.add(msg["sensor_id"])
    return sensors


def handle_message(topic:str,message:dict):
    pass

while True:
    topic, raw_payload = sub_socket.recv_multipart()
    data = json.loads(raw_payload.decode("utf-8"))
    if topic == b"sensor_agg":
        available_sensors = get_available_sensors(data)
        handle_message(topic=str(topic),message=data)

# TODO retries - when server is not up yet, we should retry to connect to it.
# TODO - NO DATA TO CHANGE STATE == OLD STATE