from typing import Protocol


class ShutdownPublisher(Protocol):
    async def publish_shutdown(self, race_id: str) -> None: ...


class DevShutdownPublisher:
    def __init__(self, topic: str): self.topic, self.messages = topic, []

    async def publish_shutdown(self, race_id): self.messages.append((race_id, 'shutdown'))


class KafkaShutdownPublisher(DevShutdownPublisher):
    def __init__(self, bootstrap: str, topic: str): super().__init__(topic); self.bootstrap = bootstrap
