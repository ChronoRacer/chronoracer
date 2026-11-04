import orjson
from confluent_kafka import Producer, KafkaException
from loguru import logger

from analysis.shared.contract import DriverEventMessage

_EVENT_TOPIC = "event"


class EventProducer:
    def __init__(self, bootstrap: str) -> None:
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "linger.ms": 0,            # send immediately — driver needs low latency
            "compression.type": "snappy",  # matches Core/Relay convention
            "queue.buffering.max.messages": 1000,
        })
        logger.info("EventProducer connected to private Kafka at {}", bootstrap)

    def send(self, key: str, name: str, value: float | dict[str, float] | None, state: str | None = None) -> None:
        msg = DriverEventMessage(name=name, value=value, state=state)
        body = orjson.dumps(msg.model_dump(mode="json"))
        try:
            self._producer.produce(
                topic=_EVENT_TOPIC,
                key=key.encode(),
                value=body,
                on_delivery=_delivery_report,
            )
            self._producer.poll(0)
        except KafkaException as exc:
            logger.warning("EventProducer: failed to produce key={}: {}", key, exc)

    def flush(self) -> None:
        self._producer.flush()


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.warning(
            "EventProducer delivery error topic={} key={}: {}",
            msg.topic(),
            msg.key(),
            err,
        )
