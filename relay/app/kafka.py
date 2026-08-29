import asyncio
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from datetime import datetime, timezone
from loguru import logger
from typing import Protocol


class ShutdownPublisher(Protocol):
    async def publish_shutdown(self, race_id: str) -> None: ...


class HeartbeatReader(Protocol):
    async def read_last(self) -> datetime | None: ...


class KafkaHeartbeatReader:
    def __init__(self, bootstrap: str, topic: str):
        self.bootstrap, self.topic = bootstrap, topic

    async def read_last(self) -> datetime | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._read_last_sync)

    def _read_last_sync(self) -> datetime | None:
        # DO NOT set auto commit or do any manual commits here. Group ID can be random
        consumer = Consumer({'bootstrap.servers': self.bootstrap,
                             'enable.auto.commit': False,
                             'auto.offset.reset': 'error'}
                            )
        try:
            partitions = consumer.list_topics(self.topic, timeout=5).topics[self.topic].partitions
            if not partitions: return None
            records = []
            for partition in partitions:
                probe = TopicPartition(self.topic, partition)
                _, high = consumer.get_watermark_offsets(probe, timeout=5)
                if high <= 0:
                    continue
                consumer.assign([TopicPartition(self.topic, partition, high - 1)])
                message = consumer.poll(timeout=5)
                if message is not None and not message.error() and message.key() == b'CORE':
                    records.append(message)
            dated_records = [(message, message.timestamp()[1]) for message in records]
            dated_records = [(message, timestamp) for message, timestamp in dated_records
                             if timestamp is not None and timestamp >= 0]
            if not dated_records:
                return None
            record, timestamp_ms = max(dated_records, key=lambda item: item[1])
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
        self._publish_lock = asyncio.Lock()
        self._delivery_timeout_seconds = 7

        self.producer = Producer({
            'bootstrap.servers': self.bootstrap,
            'client.id': 'relay',
            'acks': 'all',
            'enable.idempotence': True,
            'retries': 3,
            'retry.backoff.ms': 250,
            'delivery.timeout.ms': self._delivery_timeout_seconds * 1000,
        })

    async def publish_shutdown(self, race_id: str) -> None:
        logger.info("Publishing shutdown event for race {}", race_id)
        async with self._publish_lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._publish_sync, race_id)

    def _publish_sync(self, race_id: str) -> None:
        delivery_error = None

        def _delivery_callback(error, _message):
            nonlocal delivery_error
            if error is not None:
                delivery_error = error

        try:
            self.producer.produce(
                topic=self.topic,
                key="SHUTDOWN",
                value="",
                on_delivery=_delivery_callback,
                compression_type='snappy',
            )
            remaining = self.producer.flush(self._delivery_timeout_seconds)
        except (BufferError, KafkaException) as exc:
            logger.error("Failed to publish shutdown for race '{}': {}", race_id, exc)
            raise

        if delivery_error is not None:
            logger.error("Kafka delivery failed for race '{}': {}", race_id, delivery_error)
            raise KafkaException(delivery_error)
        if remaining:
            error = TimeoutError(f'Kafka shutdown delivery timed out with {remaining} message(s) pending')
            logger.error("Kafka delivery failed for race '{}': {}", race_id, error)
            raise error

        logger.info("Published shutdown for race '{}'", race_id)
