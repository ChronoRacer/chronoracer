import sys
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
from analysis.shared.gcs_retry import retry_gcs_call

RULES_PREFIX = "analysis/rules/"


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


def load_rules_from_gcs(client: storage.Client) -> dict[str, RuleObj]:
    bucket = client.bucket("chronoracer-artifacts")
    rules: dict[str, RuleObj] = {}
    for blob in client.list_blobs(bucket, prefix=RULES_PREFIX):
        if not blob.name.endswith(".json"):
            continue
        rule_json = orjson.loads(blob.download_as_text())
        name = str(blob.name).split("/")[-1].removesuffix(".json")
        rules[name] = RuleObj(
            rule_json=rule_json,
            sensors=set(RulesEngine._get_mandatory_sensors(rule_json)),
        )
    if not rules:
        raise RuntimeError(f"No rule JSON objects under gs://chronoracer-artifacts/{RULES_PREFIX}")
    return rules


def load_rules_with_retry(client_factory=storage.Client, sleep=time.sleep) -> dict[str, RuleObj]:
    rules = retry_gcs_call(
        lambda: load_rules_from_gcs(client_factory("chronoracer")),
        what="load rules from GCS",
        sleep=sleep,
    )
    logger.info("Loaded {} rules: {}", len(rules), list(rules.keys()))
    return rules


class RulesEngine:
    def __init__(self,config):
        self.config = config
        self.engine = zen.ZenEngine({"loader": self.loader})
        self.rules : dict[str,RuleObj] = load_rules_with_retry()
        self.time_cache : dict[str,int] = {} # (rule_id) ---> time in ns


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
                s.headers.update({"Authorization": f"Bearer {self.config.ANALYSIS_INGEST_TOKEN}"})
                payload = self._prepare_alert_json(alert_id,decision)
                logger.info("Alert {} state {}: {}", alert_id, decision["result"]["state"], payload)
                try:
                    req = s.post(self.config.TELEMETRY_URL + TELEMETRY_RULES_ENDPOINT,json=payload,timeout=2)
                    req.raise_for_status()
                except Exception as ex:
                    logger.warning("Failed to send alert {}: {}", alert_id, ex)

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
        sys.exit(1)
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


