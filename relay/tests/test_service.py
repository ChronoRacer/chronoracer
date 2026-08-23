import pytest

from relay.app.models import BuildFinished, ExportDone, Phase, TerraformAction, TerraformStatus
from relay.app.gcp.repository import InMemoryRaceRepository
from relay.app.service import RelayService


async def started():
    service = RelayService(InMemoryRaceRepository())
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
