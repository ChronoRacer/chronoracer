import time
from pathlib import Path
from collections import  defaultdict
import zmq
import orjson
import zen
from pprint import pformat

from google.cloud import storage
from pydantic import BaseModel
from datetime import timedelta
class RuleObj(BaseModel):
    is_active : bool = False
    rule_json: dict
    sensors : set[str]

class Sensor(BaseModel):
    id: str
    min:float | None
    max:float | None
    desc : str


class SignalHistory(BaseModel):
    ts_nano : int
    val : float

class Signal(BaseModel):
    sensor_id : str
    first_val_ts_nano : int
    last_val_ts_nano : int
    min_val : float
    max_val : float
    history : list[SignalHistory]

class GoRulesSchema(BaseModel):
    unit_id : str
    eval_ts_nano : int
    signals : list[Signal]

class RulesEngine:
    def __init__(self):
        self.rules : dict[str,RuleObj] = defaultdict(RuleObj)
        rules = defaultdict(RuleObj)
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.engine = zen.ZenEngine({"loader": self.loader})

        for blob in self.client.list_blobs(self.bucket,prefix="analysis/rules/"):
            if blob.name.endswith(".json"):
                rule_json = orjson.loads(blob.download_as_text())
                sensors = set(self._get_mandatory_sensors(rule_json))
                rule = RuleObj(
                    rule_json=rule_json,
                    sensors=sensors
                )
                rules[str(blob.name).split("/")[-1]] = rule
        self.rules = rules
        self.sensors : dict[str,Sensor] = self.fetch_sensor()

    def fetch_sensor(self):
        sensors = {}
        blob = self.bucket.blob("analysis/sensors.json")
        data_str = blob.download_as_text()
        json_data = orjson.loads(data_str)
        for obj in json_data["sensors"]:
            sensors[obj["name"]] = Sensor(
                id=obj["name"],
                min=obj["min"],
                max=obj["max"],
                desc=obj["description"]
            )
        return sensors

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

    # TODO CHECK IF ZEROMQ QUEUE IS NOT BLOCKED BY MY FOR LOOP
    # AND IF WE  LOOSE MESSAGES BECAUSE OF ITERATING CONSTANT THROUGH RULES MAP
    def handle_message(self,message: dict, sensors: set[str]):
        for rule_name in self.rules.keys():
            rule_obj = self.rules[rule_name]

            if rule_obj.sensors == sensors:
                go_rules_schema = GoRulesSchema(
                    unit_id="rule_obj.rule_json[name]", # ??? TODO
                    eval_ts_nano=time.time_ns(),
                    signals=self._get_signals(message)
                )
                decision = self.engine.evaluate(rule_name,go_rules_schema.model_dump())
                print(decision)
    @staticmethod
    def _get_signals(message :dict) ->list[Signal]:
        signals : list[Signal] = []
        for sensor_dct in message:
            signals.append(Signal(
                sensor_id=sensor_dct["sensor_id"],
                first_val_ts_nano=sensor_dct["window_min_ts"],
                last_val_ts_nano=sensor_dct["window_max_ts"],
                min_val=sensor_dct["window_min_val"],
                max_val=sensor_dct["window_max_val"],
                history=[SignalHistory(ts_nano=ts,val=val) for ts,val in sensor_dct["vals"]]
            ))

        return signals
"""

            sensor_id=pw.this._pw_instance,
            window_start=pw.this._pw_window_start,
            window_end=pw.this._pw_window_end,
            vals=pw.reducers.sorted_tuple(
                pw.make_tuple(
                    pw.this.timestamp_nano,
                    pw.this.value
                )
            ),
            window_min_val=pw.reducers.min(pw.this.value),
            window_max_val=pw.reducers.max(pw.this.value),
            window_min_ts=pw.reducers.min(pw.this.timestamp_nano),
            window_max_ts=pw.reducers.max(pw.this.timestamp_nano),
            sample_count=pw.reducers.count()
"""


context = zmq.Context()
sub_socket = context.socket(zmq.SUB)
sub_socket.setsockopt(zmq.SUBSCRIBE,b"sensor_agg")

sub_socket.connect("tcp://127.0.0.1:6767")
rules_engine = RulesEngine()




def get_available_sensors(messages:dict) -> set[str]:
    sensors = set()
    for msg in messages:
        sensors.add(msg["sensor_id"])
    return sensors

while True:
    topic, raw_payload = sub_socket.recv_multipart()
    data = orjson.loads(raw_payload.decode("utf-8"))
    if topic == b"sensor_agg":
        available_sensors = get_available_sensors(data)
        rules_engine.handle_message(message=data,sensors=available_sensors)

# TODO retries - when server is not up yet, we should retry to connect to it.
# TODO - NO DATA TO CHANGE STATE == OLD STATE