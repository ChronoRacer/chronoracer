import asyncio
import pytest
import yaml
from app.auth import OktaUser
from app.gcp.repository import InMemoryRaceRepository
from app.gcp.storage import DevObjectStore
from app.models import BuildFinished, ExportDone, Phase, TerraformAction, TerraformStatus
from app.service import MAX_OPERATOR_PAYLOAD_BYTES, PayloadTooLargeError, RelayService
from datetime import datetime, timedelta, timezone


class FakeBuilds:
    def __init__(self, build_id='build-1', error=None):
        self.build_id = build_id
        self.error = error
        self.calls = []
        self.terminal = None

    async def trigger(self, race_id, action, run_token):
        self.calls.append((race_id, action, run_token))
        if self.error:
            raise self.error
        return self.build_id

    async def get(self, build_id):
        return self.terminal

    async def find(self, race_id, run_token):
        return self.terminal


class FakeShutdown:
    def __init__(self):
        self.calls = []
        self.close_calls = 0

    async def publish_shutdown(self, race_id):
        self.calls.append(race_id)

    async def close(self):
        self.close_calls += 1


class FakeEvents:
    def __init__(self):
        self.messages = []

    async def publish(self, key, value):
        self.messages.append((key, value))


class TrackingRepository(InMemoryRaceRepository):
    def __init__(self):
        super().__init__()
        self.save_calls = []

    async def save_race(self, race, fields=None):
        self.save_calls.append(set(fields or ()))
        return await super().save_race(race, fields)


class OrderTrackingShutdown(FakeShutdown):
    def __init__(self, repository: TrackingRepository):
        super().__init__()
        self.repository = repository

    async def publish_shutdown(self, race_id):
        assert any('phase' in fields and 'terraform_run' in fields for fields in self.repository.save_calls)
        await super().publish_shutdown(race_id)


class FakeHeartbeat:
    async def read_last(self):
        return datetime.now(timezone.utc)


async def started():
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds())
    await service.start('race1', {'x': 1}, OktaUser(sub='operator'))
    race = await service._active()
    race.terraform_run.build_id = 'b1'
    await service.repository.save_race(race)
    return service, race


@pytest.mark.asyncio
async def test_start_stamps_apply_and_holds_mutex():
    service, race = await started()
    assert race.phase is Phase.STARTING
    assert race.terraform_run.action is TerraformAction.APPLY
    assert race.terraform_run.locked


@pytest.mark.asyncio
async def test_mutex_rejects_stop_until_build_finishes():
    service, race = await started()
    with pytest.raises(ValueError): await service.stop()
    await service.build_finished(
        BuildFinished(race_id=race.race_id, action=TerraformAction.APPLY, run_token=race.terraform_run.run_token,
                      build_id='b1', success=True))
    current = await service._active()
    current.phase = Phase.APPLIED
    current.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(current)
    assert (await service.stop())['phase'] == 'stopping'


@pytest.mark.asyncio
async def test_destroy_success_clears_pointer():
    service, race = await started()
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    race.terraform_run.build_id = ''
    race.phase = Phase.FAILED
    await service.repository.save_race(race)
    await service.force_destroy()
    current = await service._active()
    current.terraform_run.build_id = 'b2'
    await service.repository.save_race(current)
    await service.build_finished(
        BuildFinished(race_id=race.race_id, action=TerraformAction.DESTROY, run_token=current.terraform_run.run_token,
                      build_id='b2', success=True))
    assert (await service.repository.active()).race_id == ''


@pytest.mark.asyncio
async def test_export_done_only_allowed_in_exporting():
    service, race = await started()
    with pytest.raises(ValueError): await service.export_done(ExportDone(race_id=race.race_id))


@pytest.mark.asyncio
async def test_retry_from_failed_keeps_race_and_yaml_unchanged():
    builds = FakeBuilds()
    service = RelayService(InMemoryRaceRepository(), builds=builds)
    first = await service.start('race1', {'port': 1}, OktaUser(sub='operator'))
    race = await service._active()
    race.phase = Phase.FAILED
    race.terraform_run.status = TerraformStatus.FAILED
    await service.repository.save_race(race)

    retried = await service.start('race1', {'port': 999}, OktaUser(sub='other-operator'))
    current = await service._active()
    assert retried['raceId'] == first['raceId']
    assert current.race_id == first['raceId']
    assert current.started_by == 'operator'
    assert current.phase is Phase.STARTING
    assert len(builds.calls) == 2


@pytest.mark.asyncio
async def test_retry_from_failed_clears_last_error():
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds())
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.FAILED
    race.terraform_run.status = TerraformStatus.FAILED
    race.last_error = 'terraform failed'
    await service.repository.save_race(race)

    await service.start('race1', {}, OktaUser(sub="operator"))
    current = await service._active()
    assert current.last_error is None
    assert current.terraform_run.locked


@pytest.mark.asyncio
async def test_new_start_persists_mutex_on_create():
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds())
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service.repository.race('race1')
    assert race.terraform_run is not None
    assert race.terraform_run.locked
    assert race.phase is Phase.STARTING


@pytest.mark.asyncio
async def test_start_rejects_invalid_and_duplicate_race_id():
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds())
    with pytest.raises(ValueError, match='label-safe'):
        await service.start('Race1', {}, OktaUser(sub="operator"))
    with pytest.raises(ValueError, match='label-safe'):
        await service.start('abcdefghijklm', {}, OktaUser(sub="operator"))
    with pytest.raises(ValueError, match='label-safe'):
        await service.start('', {}, OktaUser(sub="operator"))
    await service.start('race-1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.FAILED
    race.terraform_run.status = TerraformStatus.FAILED
    await service.repository.save_race(race)
    with pytest.raises(ValueError, match='does not match'):
        await service.start('other1', {}, OktaUser(sub="operator"))


@pytest.mark.asyncio
async def test_destroyed_race_id_cannot_be_reused():
    service, race = await started()
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    race.terraform_run.build_id = ''
    race.phase = Phase.FAILED
    await service.repository.save_race(race)
    await service.force_destroy()
    current = await service._active()
    current.terraform_run.build_id = 'b2'
    await service.repository.save_race(current)
    await service.build_finished(
        BuildFinished(race_id=race.race_id, action=TerraformAction.DESTROY, run_token=current.terraform_run.run_token,
                      build_id='b2', success=True))
    with pytest.raises(ValueError, match='already exists'):
        await service.start(race.race_id, {}, OktaUser(sub="operator"))


@pytest.mark.asyncio
async def test_trigger_failure_releases_mutex_and_marks_failed():
    builds = FakeBuilds(error=RuntimeError('unavailable'))
    service = RelayService(InMemoryRaceRepository(), builds=builds)

    with pytest.raises(RuntimeError):
        await service.start('race1', {'x': 1}, OktaUser(sub="operator"))
    race = await service._active()
    assert race.phase is Phase.FAILED
    assert race.terraform_run.status is TerraformStatus.FAILED
    assert not race.terraform_run.locked
    assert race.last_error == 'unavailable'


@pytest.mark.asyncio
async def test_apply_failure_marks_failed_and_clears_mutex():
    builds = FakeBuilds()
    service = RelayService(InMemoryRaceRepository(), builds=builds)
    await service.start('race1', {}, OktaUser(sub='operator'))
    race = await service._active()

    await service.build_finished(BuildFinished(
        race_id=race.race_id, action=TerraformAction.APPLY,
        run_token=race.terraform_run.run_token, build_id='build-1', success=False,
        error='terraform failed'))
    current = await service._active()
    assert current.phase is Phase.FAILED
    assert current.terraform_run.status is TerraformStatus.FAILED
    assert current.last_error == 'terraform failed'


@pytest.mark.asyncio
async def test_controller_applied_state_survives_late_build_failure():
    service, race = await started()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.IN_FLIGHT
    await service.repository.save_race(race)

    await service.build_finished(BuildFinished(
        race_id=race.race_id, action=TerraformAction.APPLY,
        run_token=race.terraform_run.run_token, build_id='b1', success=False))
    current = await service._active()
    assert current.phase is Phase.APPLIED
    assert current.terraform_run.status is TerraformStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_close_ingest_success_enters_exporting_and_publishes_shutdown():
    builds = FakeBuilds()
    shutdown = FakeShutdown()
    service = RelayService(InMemoryRaceRepository(), builds=builds, shutdown=shutdown)
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)
    await service.stop()
    race = await service._active()

    await service.build_finished(BuildFinished(
        race_id=race.race_id, action=TerraformAction.CLOSE_INGEST,
        run_token=race.terraform_run.run_token, build_id='build-1', success=True))
    current = await service._active()
    assert current.phase is Phase.EXPORTING
    assert current.terraform_run.status is TerraformStatus.SUCCEEDED
    assert not current.terraform_run.locked
    assert shutdown.calls == [race.race_id]
    assert shutdown.close_calls == 1


@pytest.mark.asyncio
async def test_close_ingest_success_persists_exporting_before_shutdown_publish():
    builds = FakeBuilds()
    repository = TrackingRepository()
    shutdown = OrderTrackingShutdown(repository)
    service = RelayService(repository, builds=builds, shutdown=shutdown)
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)
    await service.stop()
    race = await service._active()

    await service.build_finished(BuildFinished(
        race_id=race.race_id, action=TerraformAction.CLOSE_INGEST,
        run_token=race.terraform_run.run_token, build_id='build-1', success=True))
    assert shutdown.calls == [race.race_id]


@pytest.mark.asyncio
async def test_close_ingest_replay_republishes_shutdown_when_already_exporting():
    builds = FakeBuilds()
    shutdown = FakeShutdown()
    service = RelayService(InMemoryRaceRepository(), builds=builds, shutdown=shutdown)
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)
    await service.stop()
    race = await service._active()

    event = BuildFinished(
        race_id=race.race_id, action=TerraformAction.CLOSE_INGEST,
        run_token=race.terraform_run.run_token, build_id='build-1', success=True)
    await service.build_finished(event)
    await service.build_finished(event)
    assert shutdown.calls == [race.race_id, race.race_id]


@pytest.mark.asyncio
async def test_force_destroy_rejects_destroyed_phase():
    service, race = await started()
    race.phase = Phase.DESTROYED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)

    with pytest.raises(ValueError):
        await service.force_destroy()


@pytest.mark.asyncio
async def test_reconcile_expired_unbound_run_releases_mutex():
    builds = FakeBuilds()
    service = RelayService(InMemoryRaceRepository(), builds=builds)
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.terraform_run.build_id = ''
    race.terraform_run.stamped_at = datetime.now(timezone.utc) - timedelta(seconds=601)
    await service.repository.save_race(race)

    await service.reconcile()
    current = await service._active()
    assert current.terraform_run.status is TerraformStatus.FAILED
    assert current.phase is Phase.FAILED
    assert current.last_error == 'Cloud Build trigger did not start'


@pytest.mark.asyncio
async def test_start_uploads_operator_yaml_unchanged():
    object_store = DevObjectStore()
    service = RelayService(InMemoryRaceRepository(), object_store=object_store, builds=FakeBuilds())
    config = {'race': {'initial_driver_id': 'driver-1'}, 'runtime': {'rayon_threads': 8}}

    result = await service.start('race1', config, OktaUser(sub='operator'))

    stored = yaml.safe_load(object_store.objects[f"{result['raceId']}/core-config.yaml"])
    assert stored == config
    assert 'raceId' not in stored
    assert result['raceId'] == 'race1'


def test_cloud_build_notification_is_normalized_from_substitutions():
    event = BuildFinished.from_cloud_build({
        'id': 'build-1',
        'status': 'SUCCESS',
        'substitutions': {
            '_RACE_ID': 'race-1',
            '_ACTION': 'apply',
            '_RUN_TOKEN': 'token-1',
        },
    })

    assert event.race_id == 'race-1'
    assert event.action is TerraformAction.APPLY
    assert event.run_token == 'token-1'
    assert event.build_id == 'build-1'
    assert event.success is True


def test_cloud_build_notification_rejects_non_terminal_status():
    with pytest.raises(ValueError):
        BuildFinished.from_cloud_build({'id': 'build-1', 'status': 'WORKING', 'substitutions': {}})


def test_cloud_build_notification_requires_relay_substitutions():
    with pytest.raises(ValueError):
        BuildFinished.from_cloud_build({
            'id': 'build-1', 'status': 'SUCCESS',
            'race_id': 'race-1', 'action': 'apply', 'run_token': 'token-1',
        })


@pytest.mark.asyncio
async def test_ready_status_includes_configured_kafka_port():
    service = RelayService(
        InMemoryRaceRepository(), builds=FakeBuilds(), heartbeat=FakeHeartbeat(), kafka_public_port=19092
    )
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    race.kafka_hostname = 'kafka.example.test'
    race.kafka_public_ip = '192.0.2.10'
    await service.repository.save_race(race)

    result = await service.status()

    assert result['kafkaPort'] == 19092


@pytest.mark.asyncio
async def test_ready_status_includes_telemetry_url():
    service = RelayService(
        InMemoryRaceRepository(), builds=FakeBuilds(), heartbeat=FakeHeartbeat()
    )
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    race.kafka_hostname = 'kafka.example.test'
    race.kafka_public_ip = '192.0.2.10'
    race.telemetry_url = 'https://telemetry-1.europe-west1.run.app'
    await service.repository.save_race(race)

    result = await service.status()

    assert result['telemetryUrl'] == 'https://telemetry-1.europe-west1.run.app'


@pytest.mark.asyncio
async def test_start_rejects_oversized_core_config_yaml():
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds())
    with pytest.raises(PayloadTooLargeError):
        await service.start('race1', {'blob': 'x' * MAX_OPERATOR_PAYLOAD_BYTES}, OktaUser(sub="operator"))


@pytest.mark.asyncio
async def test_publish_event_requires_applied_race():
    events = FakeEvents()
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds(), events=events)
    await service.start('race1', {}, OktaUser(sub="operator"))
    with pytest.raises(ValueError, match='current state'):
        await service.publish_event('FLAG', b'payload')
    assert events.messages == []


@pytest.mark.asyncio
async def test_publish_event_sends_opaque_bytes():
    events = FakeEvents()
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds(), events=events)
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)

    await service.publish_event('FLAG', b'\x00raw')
    assert events.messages == [('FLAG', b'\x00raw')]


@pytest.mark.asyncio
async def test_publish_event_rejects_oversized_body():
    events = FakeEvents()
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds(), events=events)
    await service.start('race1', {}, OktaUser(sub="operator"))
    race = await service._active()
    race.phase = Phase.APPLIED
    await service.repository.save_race(race)
    with pytest.raises(PayloadTooLargeError):
        await service.publish_event('FLAG', b'x' * (MAX_OPERATOR_PAYLOAD_BYTES + 1))
    assert events.messages == []


@pytest.mark.asyncio
async def test_status_recovers_shutdown_publish_failure_after_exporting_commit():
    shutdown = FakeShutdown()
    service, race = await started()
    service.shutdown = shutdown
    race.phase = Phase.STOPPING
    race.terraform_run.action = TerraformAction.CLOSE_INGEST
    await service.repository.save_race(race)

    original_publish = shutdown.publish_shutdown
    async def fail(_race_id):
        raise RuntimeError('Kafka unavailable')
    shutdown.publish_shutdown = fail
    event = BuildFinished(
        race_id=race.race_id, action=TerraformAction.CLOSE_INGEST,
        run_token=race.terraform_run.run_token, build_id='b1', success=True,
    )
    with pytest.raises(RuntimeError, match='Kafka unavailable'):
        await service.build_finished(event)
    current = await service._active()
    assert current.phase is Phase.EXPORTING
    assert current.terraform_run.status is TerraformStatus.SUCCEEDED
    assert shutdown.close_calls == 1

    shutdown.publish_shutdown = original_publish
    assert (await service.status())['phase'] == 'exporting'
    assert shutdown.calls == [race.race_id]
    assert shutdown.close_calls == 2


@pytest.mark.asyncio
async def test_event_publish_finishes_before_stop_closes_ingest():
    entered, release = asyncio.Event(), asyncio.Event()
    class BlockingEvents(FakeEvents):
        async def publish(self, key, value):
            entered.set()
            await release.wait()
            await super().publish(key, value)

    service, race = await started()
    service.events = BlockingEvents()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)
    publishing = asyncio.create_task(service.publish_event('FLAG', b'event'))
    await asyncio.wait_for(entered.wait(), timeout=1)
    stopping = asyncio.create_task(service.stop())
    try:
        await asyncio.sleep(0)
        assert (await service._active()).phase is Phase.APPLIED
        assert not stopping.done()
    finally:
        release.set()
        await asyncio.gather(publishing, stopping)
    assert service.events.messages == [('FLAG', b'event')]
    assert (await service._active()).phase is Phase.STOPPING


@pytest.mark.asyncio
async def test_reconcile_binds_lost_build_response_and_replays_result():
    service, race = await started()
    race.terraform_run.build_id = ''
    await service.repository.save_race(race)
    service.builds.terminal = {'buildId': 'recovered', 'success': False, 'error': 'apply failed'}
    result = await service.status()
    assert result['phase'] == 'failed'
    assert result['terraformRun']['buildId'] == 'recovered'
    assert result['terraformRun']['status'] == 'failed'
    assert result['lastError'] == 'apply failed'


@pytest.mark.asyncio
async def test_force_destroy_from_exporting_does_not_require_kafka():
    service, race = await started()
    race.phase = Phase.EXPORTING
    race.terraform_run.action = TerraformAction.CLOSE_INGEST
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    await service.repository.save_race(race)
    # No shutdown publisher is configured, but force-destroy must still work.
    assert (await service.force_destroy())['phase'] == 'destroying'


def test_expired_build_notification_is_a_terminal_failure():
    event = BuildFinished.from_cloud_build({
        'id': 'build-1', 'status': 'EXPIRED',
        'substitutions': {'_RACE_ID': 'race1', '_ACTION': 'apply', '_RUN_TOKEN': 'token'},
    })
    assert event.success is False
