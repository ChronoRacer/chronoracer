import asyncio
from typing import Protocol

from confluent_kafka import Producer, Message, KafkaException
from loguru import logger


class ShutdownPublisher(Protocol):
    async def publish_shutdown(self, race_id: str) -> None: ...


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
