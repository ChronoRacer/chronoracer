import json

import analysis.shared.kafka_producer as kafka_module


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, **message):
        self.messages.append(message)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        return 0


def test_event_producer_serializes_driver_event_and_preserves_key(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(kafka_module, "Producer", lambda _config: fake)
    producer = kafka_module.EventProducer("kafka.internal:9092")

    producer.send(
        key="OIL_ALERT",
        name="oil_alert",
        value={"OIL_TEMP_C": 130.5, "COOLANT_TEMP_C": 98.0},
        state="RED",
    )
    producer.close()

    assert len(fake.messages) == 1
    message = fake.messages[0]
    assert message["topic"] == "event"
    assert message["key"] == b"OIL_ALERT"
    assert json.loads(message["value"]) == {
        "name": "oil_alert",
        "value": {"OIL_TEMP_C": 130.5, "COOLANT_TEMP_C": 98.0},
        "state": "RED",
    }
