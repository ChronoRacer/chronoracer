from types import SimpleNamespace

from app.kafka import KafkaHeartbeatReader, KafkaShutdownPublisher


def test_shutdown_publisher_does_not_connect_until_publish(monkeypatch):
    created = []

    def fake_producer(_config):
        created.append(True)
        return object()

    monkeypatch.setattr('app.kafka.Producer', fake_producer)
    publisher = KafkaShutdownPublisher('kafka.internal:9092', 'control')

    assert publisher.producer is None
    assert created == []


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
