import asyncio
from typing import Protocol

from confluent_kafka import Producer, Consumer, Message, KafkaException
from datetime import datetime, timezone
from loguru import logger


class ShutdownPublisher(Protocol):
    async def publish_shutdown(self, race_id: str) -> None: ...

class HeartbeatReader(Protocol):
    async def read_last(self) -> datetime | None: ...

class KafkaHeartbeatReader:
    def __init__(self, bootstrap: str, topic: str = 'heartbeat'):
        self.bootstrap, self.topic = bootstrap, topic

    async def read_last(self) -> datetime | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._read_last_sync)

    def _read_last_sync(self) -> datetime | None:
        consumer = Consumer({'bootstrap.servers': self.bootstrap, 'group.id': 'relay-heartbeat-read',
                             'enable.auto.commit': False, 'auto.offset.reset': 'latest'})
        try:
            partitions = consumer.list_topics(self.topic, timeout=5).topics[self.topic].partitions
            if not partitions: return None
            from confluent_kafka import TopicPartition
            consumer.assign([TopicPartition(self.topic, p, -1) for p in partitions])
            records = consumer.consume(num_messages=100, timeout=5)
            records = [m for m in records if not m.error() and m.key() == b'CORE']
            if not records: return None
            timestamp_type, timestamp_ms = records[-1].timestamp()
            if timestamp_ms is None or timestamp_ms < 0: return None
            return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        finally:
            consumer.close()


class DevShutdownPublisher:
    def __init__(self, topic: str): self.topic, self.messages = topic, []

    async def publish_shutdown(self, race_id: str): self.messages.append(race_id)


class KafkaShutdownPublisher(ShutdownPublisher):

    def __init__(self, bootstrap: str, topic: str):
        self.topic = topic
        self.bootstrap = bootstrap

        self.producer = Producer({
            'bootstrap.servers': self.bootstrap,
            'client.id': 'relay',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 250,
            'delivery.timeout.ms': 7000,
        })

    async def publish_shutdown(self, race_id: str) -> None:
        logger.info("Publishing shutdown event for race {}", race_id)
        loop = asyncio.get_running_loop()
        delivery_future: asyncio.Future[Message] = loop.create_future()

        def _delivery_callback(err, msg: Message):
            if err is not None:
                loop.call_soon_threadsafe(
                    delivery_future.set_exception,
                    KafkaException(err)
                )
            else:
                loop.call_soon_threadsafe(
                    delivery_future.set_result,
                    msg
                )

        try:
            self.producer.produce(
                topic=self.topic,
                key="SHUTDOWN",
                value="",
                on_delivery=_delivery_callback
            )
            self.producer.poll(0)

        except (BufferError, KafkaException) as e:
            logger.error(f"Failed to enqueue shutdown event for race '{race_id}': {e}")
            raise

        try:
            msg = await delivery_future
            logger.info(f"Published shutdown for race '{race_id}' to {msg.topic()} (offset: {msg.offset()})")
        except KafkaException as e:
            logger.error(f"Kafka delivery failed for race '{race_id}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error waiting for Kafka delivery: {e}")
            raise
