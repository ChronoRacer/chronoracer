from app.kafka import KafkaEventPublisher, KafkaHeartbeatReader, KafkaShutdownPublisher
from confluent_kafka import KafkaException
from types import SimpleNamespace


def test_shutdown_publisher_does_not_connect_until_publish(monkeypatch):
    created = []

    def fake_producer(_config):
        created.append(True)
        return object()

    monkeypatch.setattr('app.kafka.Producer', fake_producer)
    publisher = KafkaShutdownPublisher('kafka.internal:9092', 'control')

    assert publisher.producer is None
    assert created == []


def test_shutdown_publisher_closes_producer(monkeypatch):
    closed = []

    class FakeProducer:
        def produce(self, **_kwargs):
            pass

        def flush(self, _timeout):
            return 0

        def close(self):
            closed.append(True)

    monkeypatch.setattr('app.kafka.Producer', lambda _config: FakeProducer())
    publisher = KafkaShutdownPublisher('kafka.internal:9092', 'control')
    publisher._publish_sync('race1')

    import asyncio
    asyncio.run(publisher.close())

    assert closed == [True]
    assert publisher.producer is None


def test_heartbeat_reader_configures_a_consumer_group(monkeypatch):
    created = []

    class FakeConsumer:
        def __init__(self, config):
            created.append(config)

        def list_topics(self, _topic, timeout):
            assert timeout == 5
            return SimpleNamespace(topics={'heartbeat': SimpleNamespace(partitions={})})

        def close(self):
            pass

    monkeypatch.setattr('app.kafka.Consumer', FakeConsumer)

    assert KafkaHeartbeatReader('kafka.internal:9092', 'heartbeat')._read_last_sync() is None
    assert created[0]['group.id'].startswith('relay-health-')
    assert created[0]['socket.timeout.ms'] == 5000
    assert created[0]['socket.connection.setup.timeout.ms'] == 5000


def test_heartbeat_reader_treats_missing_topic_as_no_heartbeat(monkeypatch):
    class FakeConsumer:
        def __init__(self, _config):
            pass

        def list_topics(self, _topic, timeout):
            assert timeout == 5
            return SimpleNamespace(topics={})

        def close(self):
            pass

    monkeypatch.setattr('app.kafka.Consumer', FakeConsumer)

    assert KafkaHeartbeatReader('kafka.internal:9092', 'heartbeat')._read_last_sync() is None


def test_heartbeat_reader_closes_consumer_when_metadata_fails(monkeypatch):
    closed = []

    class FakeConsumer:
        def __init__(self, _config):
            pass

        def list_topics(self, _topic, timeout):
            assert timeout <= 5
            raise RuntimeError('host resolution failed')

        def close(self):
            closed.append(True)

    monkeypatch.setattr('app.kafka.Consumer', FakeConsumer)

    reader = KafkaHeartbeatReader('missing-host.internal:9092', 'heartbeat')
    try:
        reader._read_last_sync()
    except RuntimeError:
        pass
    else:
        raise AssertionError('metadata failure should be propagated')

    assert closed == [True]


def test_event_publisher_does_not_connect_until_publish(monkeypatch):
    created = []

    def fake_producer(_config):
        created.append(True)
        return object()

    monkeypatch.setattr('app.kafka.Producer', fake_producer)
    KafkaEventPublisher('kafka.internal:9092', 'event')

    assert created == []


def test_event_publisher_sends_then_closes(monkeypatch):
    produced = []
    closed = []
    created = []

    class FakeProducer:
        def produce(self, **kwargs):
            produced.append(kwargs)
            kwargs['on_delivery'](None, None)

        def flush(self, _timeout):
            return 0

        def close(self):
            closed.append(True)

    def fake_producer(_config):
        created.append(True)
        return FakeProducer()

    monkeypatch.setattr('app.kafka.Producer', fake_producer)
    publisher = KafkaEventPublisher('kafka.internal:9092', 'event')
    publisher._publish_sync('FLAG', b'\x00raw')

    assert created == [True]
    assert produced[0]['topic'] == 'event'
    assert produced[0]['key'] == 'FLAG'
    assert produced[0]['value'] == b'\x00raw'
    assert closed == [True]


def test_event_publisher_closes_producer_on_send_failure(monkeypatch):
    closed = []

    class FakeProducer:
        def produce(self, **_kwargs):
            raise KafkaException('broker down')

        def flush(self, _timeout):
            return 0

        def close(self):
            closed.append(True)

    monkeypatch.setattr('app.kafka.Producer', lambda _config: FakeProducer())
    publisher = KafkaEventPublisher('kafka.internal:9092', 'event')
    try:
        publisher._publish_sync('FLAG', b'payload')
    except KafkaException:
        pass
    else:
        raise AssertionError('send failure should be propagated')

    assert closed == [True]
