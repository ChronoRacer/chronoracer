import base64
import json
from unittest.mock import AsyncMock

import pytest
import yaml
from fastapi.testclient import TestClient

from app.config import load_config
from app.main import create_app
from app.models import Phase, TerraformStatus
from app.service import MAX_OPERATOR_PAYLOAD_BYTES


@pytest.fixture
def app(monkeypatch):
    config = load_config()
    config.gcp.enabled = False
    config.okta.enabled = False
    monkeypatch.setattr('app.main.load_config', lambda: config)
    return create_app()


def envelope(data):
    return {'message': {'data': base64.b64encode(json.dumps(data).encode()).decode()}}


def test_start_injects_driver_and_preserves_other_config(app):
    supplied = {'race': {'initial_driver_id': 'old', 'other': 123}, 'runtime': {'rayon_threads': 8}}
    with TestClient(app) as client:
        response = client.post('/race/start', json={
            'raceId': 'race1', 'initialDriverId': 'driver1', 'config': supplied,
        })
    assert response.status_code == 202
    assert response.json() == {'raceId': 'race1', 'phase': 'starting'}
    stored = yaml.safe_load(app.state.service.object_store.objects['race1/core-config.yaml'])
    assert stored == {**supplied, 'race': {**supplied['race'], 'initial_driver_id': 'driver1'}}


@pytest.mark.parametrize('race', [None, [], 'driver', 123])
def test_start_rejects_non_mapping_race_without_writing(app, race):
    with TestClient(app) as client:
        response = client.post('/race/start', json={
            'raceId': 'race1', 'initialDriverId': 'driver1', 'config': {'race': race},
        })
        assert client.get('/race/status').json() == {'raceId': None}
    assert response.status_code == 422
    assert app.state.service.object_store.objects == {}


@pytest.mark.parametrize('path', ['/internal/pubsub/build-finished', '/internal/pubsub/export-done'])
@pytest.mark.parametrize('payload', [
    [], None, {}, {'message': []}, {'message': {'data': []}},
    {'message': {'data': 123}}, {'message': {'data': '%%%'}},
    {'message': {'data': 'é'}}, {'message': {'data': '/w=='}},
    envelope([]), envelope({'status': [], 'substitutions': {
        '_RACE_ID': 'race1', '_ACTION': 'apply', '_RUN_TOKEN': 'token',
    }}),
])
def test_malformed_push_is_acknowledged_after_authentication(app, monkeypatch, path, payload):
    auth = AsyncMock()
    monkeypatch.setattr('app.main.pubsub_push', auth)
    with TestClient(app) as client:
        response = client.post(path, content=json.dumps(payload), headers={'Content-Type': 'application/json'})
    assert response.status_code == 204
    auth.assert_awaited_once()


@pytest.mark.parametrize('path', ['/internal/pubsub/build-finished', '/internal/pubsub/export-done'])
def test_push_still_requires_authentication_in_dev_mode(app, path):
    with TestClient(app) as client:
        assert client.post(path, json=envelope({})).status_code == 401


def test_backend_push_failure_is_not_acknowledged(app, monkeypatch):
    monkeypatch.setattr('app.main.pubsub_push', AsyncMock())
    app.state.service.export_done = AsyncMock(side_effect=RuntimeError('Firestore unavailable'))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post('/internal/pubsub/export-done', json=envelope({'race_id': 'race1'}))
    assert response.status_code == 500


def test_event_size_limit_and_opaque_bytes(app):
    service = app.state.service
    with TestClient(app) as client:
        client.post('/race/start', json={'raceId': 'race1', 'initialDriverId': 'driver1'})
        # Simulate the controller's committed outputs.
        race = service.repository._races['race1']
        race.phase = Phase.APPLIED
        race.terraform_run.status = TerraformStatus.SUCCEEDED
        for body in (b'', b'\x00\xffraw', b'x' * MAX_OPERATOR_PAYLOAD_BYTES):
            assert client.post('/race/event?key=FLAG', content=body).status_code == 204
        assert client.post('/race/event?key=FLAG', content=b'x' * (MAX_OPERATOR_PAYLOAD_BYTES + 1)).status_code == 413
        assert client.post('/race/event?key=FLAG', content=b'x', headers={'Content-Length': '-1'}).status_code == 400
    assert service.events.messages == [('FLAG', b''), ('FLAG', b'\x00\xffraw'), ('FLAG', b'x' * MAX_OPERATOR_PAYLOAD_BYTES)]


@pytest.mark.asyncio
async def test_chunked_event_stops_reading_when_limit_is_exceeded(app):
    # Exercise the ASGI stream directly: HTTP clients may buffer their chunks.
    chunks = iter([
        {'type': 'http.request', 'body': b'x' * MAX_OPERATOR_PAYLOAD_BYTES, 'more_body': True},
        {'type': 'http.request', 'body': b'x', 'more_body': True},
    ])
    async def receive():
        return next(chunks)  # A third read would fail the test.

    sent = []
    async def send(message):
        sent.append(message)

    await app({
        'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1',
        'method': 'POST', 'scheme': 'http', 'path': '/race/event',
        'raw_path': b'/race/event', 'query_string': b'key=FLAG', 'root_path': '',
        'headers': [], 'client': ('127.0.0.1', 1234), 'server': ('test', 80),
    }, receive, send)
    assert sent[0]['status'] == 413
    assert app.state.service.events.messages == []
