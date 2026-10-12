from pathlib import Path
from collections import  defaultdict
import zmq
import json
import zen
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
        client = storage.Client()
        bucket = client.bucket("chronoracer-analysis")
        for blob in client.list_blobs(bucket,prefix="rules/"):
            if blob.name.endswith(".json"):
                rule_json = json.loads(blob.download_as_text())
                sensors = set(self._get_mandatory_sensors(rule_json))
                rule = RuleObj(
                    rule_json=rule_json,
                    sensors=sensors
                )
                rules[blob.name] = rule
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

    # THIS FUNC IS RESPONSIBLE FOR TURNING ON BUT ALSO TURING OFF RULES
    # BASED ON AVAILABLE SENSORS GATHERED FOR SOME DELTA TIME
    # AFTER DELTA TIME I GET NEW SET WITH AVAILABLE SENSORS
    # AND BASED ON THAT I DECIDE WHAT RULES CAN CHECK
    def synchronize_active_rules(self,set_of_sensors : set[str]):
        for rule_id in self.rules:
            rule_obj = self.rules[rule_id]
            if len(rule_obj.sensors & set_of_sensors) != len(rule_obj.sensors):
                self.rules[rule_id].is_active = False
            else:
                self.rules[rule_id].is_active = True


# EVERY AFTER DELTA I SEND NEW SENSORS LIST
# AND I RESET IT AND START ONCE AGAIN FROM EMPTY LIST
REFRESH_SENSOR_DELTA = timedelta(seconds=5)

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
# Bucket na reguly i modele

project_root = Path(__file__).resolve().parents[2]

rules_path = project_root / "analysis" / "rules" / "rules_files" / "rules_config.json"


