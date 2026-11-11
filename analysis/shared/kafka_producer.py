import threading
import time

import orjson
from confluent_kafka import Producer, KafkaException
from loguru import logger

from analysis.shared.contract import DriverEventMessage

_EVENT_TOPIC = "event"
_POLL_INTERVAL_S = 0.1


class EventProducer:
    def __init__(self, bootstrap: str) -> None:
        try:
            self._producer = Producer({
                "bootstrap.servers": bootstrap,
                "linger.ms": 0,            # send immediately — driver needs low latency
                "compression.type": "snappy",  # matches Core/Relay convention
                "queue.buffering.max.messages": 1000,
            })
            logger.info("EventProducer connected to private Kafka at {}", bootstrap)
        except Exception as exc:
            logger.error("EventProducer failed to connect to Kafka at {}: {}", bootstrap, exc)
            raise

        self._stop = threading.Event()
        self._poller = threading.Thread(
            target=self._poll_loop,
            name="event-producer-poll",
            daemon=True,
        )
        self._poller.start()

    def _poll_loop(self) -> None:
        # Delivery callbacks (and accurate msg.latency) only run when poll() is called.
        while not self._stop.wait(_POLL_INTERVAL_S):
            try:
                self._producer.poll(0)
            except Exception as exc:
                logger.debug("EventProducer poll error: {}", exc)

    def send(self, key: str, name: str, value: float | dict[str, float] | None, state: str | None = None) -> None:
        msg = DriverEventMessage(name=name, value=value, state=state)
        body = orjson.dumps(msg.model_dump(mode="json"))
        t0 = time.perf_counter()
        try:
            self._producer.produce(
                topic=_EVENT_TOPIC,
                key=key.encode(),
                value=body,
                on_delivery=_delivery_report,
            )
            self._producer.poll(0)
            duration_ms = (time.perf_counter() - t0) * 1000
            logger.debug("EventProducer queued message key={} in {:.2f} ms", key, duration_ms)
        except KafkaException as exc:
            duration_ms = (time.perf_counter() - t0) * 1000
            logger.error("EventProducer: failed to produce key={} ({:.2f} ms): {}", key, duration_ms, exc)
        except Exception as exc:
            duration_ms = (time.perf_counter() - t0) * 1000
            logger.error("EventProducer: unexpected error producing key={} ({:.2f} ms): {}", key, duration_ms, exc)

    def flush(self, timeout: float = 5.0) -> int:
        t0 = time.perf_counter()
        remaining = self._producer.flush(timeout)
        duration_ms = (time.perf_counter() - t0) * 1000
        if remaining > 0:
            logger.error(
                "EventProducer flush timed out after {:.2f} ms with {} messages still in queue",
                duration_ms,
                remaining,
            )
        else:
            logger.debug("EventProducer flushed in {:.2f} ms", duration_ms)
        return remaining

    def close(self) -> None:
        self._stop.set()
        self._poller.join(timeout=1.0)
        self.flush()


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error(
            "EventProducer delivery error topic={} key={}: {}",
            msg.topic(),
            msg.key(),
            err,
        )
        return
    latency = msg.latency() if hasattr(msg, "latency") and callable(msg.latency) else None
    if latency is not None and latency >= 0:
        logger.debug(
            "EventProducer delivered message topic={} key={} (latency: {:.2f} ms)",
            msg.topic(),
            msg.key(),
            latency * 1000,
        )
