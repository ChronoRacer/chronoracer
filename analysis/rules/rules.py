import time
from collections import  defaultdict
import zmq
import orjson
import zen
import requests
from google.cloud import storage
from loguru import logger
from pydantic import BaseModel
from analysis.shared.config import load_config
from analysis.shared.contract import NewAlertJSON, SensorValue
from analysis.shared.kafka_producer import EventProducer


class RuleObj(BaseModel):
    rule_json: dict
    sensors : set[str]

class Sensor(BaseModel):
    id: str
    min:float | None
    max:float | None
    desc : str


class SignalHistory(BaseModel):
    ts_ms : int
    val : float

class Signal(BaseModel):
    samples : int
    history : list[SignalHistory]

class GoRulesSchema(BaseModel):
    eval_ts_ms : int
    signals : dict[str,Signal] # sensor_id - Signal

TELEMETRY_RULES_ENDPOINT = "/analysis/v1/alerts"
TIME_CACHE_NS = 3 * 1_000_000_000

class RulesEngine:
    def __init__(self,config):
        self.config = config
        self.client = storage.Client("chronoracer")
        self.bucket = self.client.bucket("chronoracer-artifacts")
        self.engine = zen.ZenEngine({"loader": self.loader})
        self.event_producer = EventProducer(config.KAFKA_PRIVATE_BOOTSTRAP)

        self.rules : dict[str,RuleObj] = {}
        for blob in self.client.list_blobs(self.bucket,prefix="analysis/rules/"):
            if blob.name.endswith(".json"):
                rule_json = orjson.loads(blob.download_as_text())
                sensors = set(self._get_mandatory_sensors(rule_json))
                rule = RuleObj(
                    rule_json=rule_json,
                    sensors=sensors
                )
                self.rules[str(blob.name).split("/")[-1].removesuffix(".json")] = rule
        logger.info("Loaded {} rules: {}", len(self.rules), list(self.rules.keys()))
        self.sensors : dict[str,Sensor] = self.fetch_sensor()
        logger.info("Loaded {} sensors", len(self.sensors))
        self.time_cache : dict[str,int] = {} # (rule_id) ---> time in ns
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
    def _get_mandatory_sensors(rule_obj: dict) -> list[str]:
        schema = orjson.loads(rule_obj["nodes"][0]["content"]["schema"])
        return list(schema["properties"]["signals"]["properties"].keys())


    def loader(self,key:str):
        return self.rules[key].rule_json

    # TODO CHECK IF ZEROMQ QUEUE IS NOT BLOCKED BY MY FOR LOOP
    # AND IF WE  LOOSE MESSAGES BECAUSE OF ITERATING CONSTANT THROUGH RULES MAP
    def handle_message(self,message: dict, sensors: set[str]):
        for rule_name in self.rules.keys():
            rule_obj = self.rules[rule_name]
            if rule_obj.sensors.issubset(sensors):
                go_rules_schema = GoRulesSchema(
                    eval_ts_ms=time.time_ns() // 1_000_000 ,
                    signals=self._get_signals(message,sensors)
                )
                decision = self.engine.evaluate(rule_name,go_rules_schema.model_dump())
                self.send_decision(rule_name,decision)

    def send_decision(self,alert_id:str,decision:dict):
        if decision["result"]["state"] not in ("YELLOW","RED","BLACK", "SENSOR_ERROR"):
            return
        send_decision = False
        time_now = time.time_ns()
        if alert_id in self.time_cache:
            if (time_now - self.time_cache[alert_id]) < TIME_CACHE_NS:
                return
            else:
                self.time_cache[alert_id] = time_now
                send_decision = True
        else:
            send_decision = True
            self.time_cache[alert_id] = time_now

        if send_decision:
            with requests.Session() as s:
                s.headers.update({"Authorization": "Bearer TOKEN"})
                payload = self._prepare_alert_json(alert_id,decision)
                logger.info("Alert {} state {}: {}", alert_id, decision["result"]["state"], payload)
                req = s.post(self.config.TELEMETRY_URL + TELEMETRY_RULES_ENDPOINT,json=payload,timeout=2)
                try:
                    req.raise_for_status()
                except Exception as ex:
                    pass # TODO

            # Forward to the driver via the `event` topic — same data as Telemetry, no extra filter.
            sensors_dict: dict[str, float] = {}
            for dct in decision["result"].get("sensors", []):
                for k, v in dct.items():
                    sensors_dict[k] = float(v)

            alert_value: float | dict[str, float] | None = None
            if len(sensors_dict) == 1:
                alert_value = next(iter(sensors_dict.values()))
            elif len(sensors_dict) > 1:
                alert_value = sensors_dict

            self.event_producer.send(
                key=alert_id.upper(),
                name=alert_id,
                value=alert_value,
                state=decision["result"].get("state"),
            )

    @staticmethod
    def _prepare_alert_json(alert_id,decision:dict):
        def _get_sensors_pair() -> list[SensorValue]:
            res = []
            for dct in decision["result"]["sensors"]:
                for k, v in dct.items():
                    res.append(SensorValue(sensor_id=k,value=v))
            return res
        new_alert_json = NewAlertJSON(
            timestamp=time.time_ns() // 1000,
            alert_id=alert_id,
            sensors=_get_sensors_pair(),
            analysis_message=decision["result"]["analysis_message"],
            analysis_reason=decision["result"]["analysis_reason"]
        )
        return new_alert_json.model_dump(mode="json")

    @staticmethod
    def _get_signals(message :dict,sensors :set[str]) ->dict[str,Signal]:
        signals : dict[str,Signal] = {}
        for sensor_dct in message:
            if sensor_dct["sensor_id"] in sensors:
                signals[sensor_dct["sensor_id"]] = Signal(
                    samples=len(sensor_dct["vals"]),
                    history=[SignalHistory(ts_ms=int(ts) // 1_000_000 ,val=val) for ts,val in sensor_dct["vals"]]
                )
        return signals

def main():
    logger.info("Rules engine starting")
    config = load_config()
    if config is None:
        logger.error("Missing config, exiting")
        return
    rules_engine = RulesEngine(config)

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"sensor_agg")
    sub_socket.connect("tcp://127.0.0.1:6767")
    logger.info("Rules engine subscribed to sensor_agg on tcp://127.0.0.1:6767")
    logger.info("Rules engine running")

    while True:
        try:
            topic, raw_payload = sub_socket.recv_multipart()
            data = orjson.loads(raw_payload.decode("utf-8"))
            if topic == b"sensor_agg":
                available_sensors = set([msg["sensor_id"] for msg in data])
                rules_engine.handle_message(message=data,sensors=available_sensors)
        except Exception:
            logger.warning("Rules engine timeout waiting for messages, retrying...")
if __name__ == "__main__":
    main()


