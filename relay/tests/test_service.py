from datetime import datetime, timedelta, timezone

import pytest

from relay.app.models import BuildFinished, ExportDone, Phase, TerraformAction, TerraformStatus
from relay.app.gcp.repository import InMemoryRaceRepository
from relay.app.service import RelayService


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

    async def publish_shutdown(self, race_id):
        self.calls.append(race_id)


class FakeHeartbeat:
    async def read_last(self):
        return datetime.now(timezone.utc)


async def started():
    service = RelayService(InMemoryRaceRepository(), builds=FakeBuilds())
    await service.start({'x': 1}, 'operator')
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
async def test_destroy_success_clears_pointer_and_writes_last():
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
    assert (await service.repository.last()).race_id == race.race_id


@pytest.mark.asyncio
async def test_export_done_only_allowed_in_exporting():
    service, race = await started()
    with pytest.raises(ValueError): await service.export_done(ExportDone(race_id=race.race_id))


@pytest.mark.asyncio
async def test_retry_from_failed_keeps_race_and_yaml_unchanged():
    builds = FakeBuilds()
    service = RelayService(InMemoryRaceRepository(), builds=builds)
    first = await service.start({'port': 1}, 'operator')
    race = await service._active()
    race.phase = Phase.FAILED
    race.terraform_run.status = TerraformStatus.FAILED
    await service.repository.save_race(race)

    retried = await service.start({'port': 999}, 'other-operator')
    current = await service._active()
    assert retried['raceId'] == first['raceId']
    assert current.race_id == first['raceId']
    assert current.started_by == 'operator'
    assert current.phase is Phase.STARTING
    assert len(builds.calls) == 2


@pytest.mark.asyncio
async def test_trigger_failure_releases_mutex_and_marks_failed():
    builds = FakeBuilds(error=RuntimeError('unavailable'))
    service = RelayService(InMemoryRaceRepository(), builds=builds)

    with pytest.raises(RuntimeError):
        await service.start({'x': 1})
    race = await service._active()
    assert race.phase is Phase.FAILED
    assert race.terraform_run.status is TerraformStatus.FAILED
    assert not race.terraform_run.locked
    assert race.last_error == 'unavailable'


@pytest.mark.asyncio
async def test_apply_failure_marks_failed_and_clears_mutex():
    builds = FakeBuilds()
    service = RelayService(InMemoryRaceRepository(), builds=builds)
    await service.start({}, 'operator')
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
    await service.start({})
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
    assert shutdown.calls == [race.race_id]


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
    await service.start({})
    race = await service._active()
    race.terraform_run.build_id = ''
    race.terraform_run.stamped_at = datetime.now(timezone.utc) - timedelta(seconds=601)
    await service.repository.save_race(race)

    await service.reconcile()
    current = await service._active()
    assert current.terraform_run.status is TerraformStatus.FAILED
    assert current.last_error == 'Cloud Build trigger did not start'


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


@pytest.mark.asyncio
async def test_ready_status_includes_configured_kafka_port():
    service = RelayService(
        InMemoryRaceRepository(), builds=FakeBuilds(), heartbeat=FakeHeartbeat(), kafka_public_port=19092
    )
    await service.start({})
    race = await service._active()
    race.phase = Phase.APPLIED
    race.terraform_run.status = TerraformStatus.SUCCEEDED
    race.kafka_hostname = 'kafka.example.test'
    race.kafka_public_ip = '192.0.2.10'
    await service.repository.save_race(race)

    result = await service.status()

    assert result['kafkaPort'] == 19092
