from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .models import ActivePointer, BuildFinished, ExportDone, LastRace, Phase, Race, TerraformAction, TerraformRun, \
    TerraformStatus
from .gcp.repository import RaceRepository
from .gcp.storage import ObjectStore
from .gcp.builds import BuildTrigger
from .gcp.pubsub import ShutdownPublisher


class RelayService:
    def __init__(self, repository: RaceRepository, object_store: ObjectStore | None = None, builds: BuildTrigger | None = None, shutdown: ShutdownPublisher | None = None):
        self.repository = repository
        self.object_store, self.builds, self.shutdown = object_store, builds, shutdown

    async def start(self, config: dict[str, Any], started_by='unknown'):
        pointer = await self.repository.active()
        if pointer.race_id:
            race = await self._active()
            if race.phase != Phase.FAILED or self._locked(race): raise ValueError(
                'race cannot be started in its current state')
            race.phase = Phase.STARTING
        else:
            now = datetime.now(timezone.utc)
            rid = str(uuid4())
            race = Race(race_id=rid, phase=Phase.STARTING, gcs_prefix=f'races/{rid}/', started_by=started_by,
                        started_at=now)
            if self.object_store:
                await self.object_store.put_yaml(f'{race.gcs_prefix}core-config.yaml', {'raceId': rid, **config})
            await self.repository.create_race(race)
        self._stamp(race, TerraformAction.APPLY)
        await self._trigger(race)
        await self.repository.save_race(race)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def status(self):
        pointer = await self.repository.active()
        if not pointer.race_id:
            last = await self.repository.last()
            return {'last': last.model_dump(mode='json') if last else None, 'billableVms': False}
        race = await self._active()
        return {'raceId': race.race_id, 'phase': race.phase.value, 'lastError': race.last_error,
                'terraformRun': race.terraform_run.model_dump(mode='json') if race.terraform_run else None,
                'billableVms': True, 'ready': False, 'unhealthy': False}

    async def stop(self):
        race = await self._active()
        if race.phase not in (Phase.APPLIED, Phase.STOPPING) or self._locked(race): raise ValueError(
            'race cannot be stopped in its current state')
        race.phase = Phase.STOPPING
        race.stopped_at = datetime.now(timezone.utc)
        self._stamp(race, TerraformAction.CLOSE_INGEST)
        await self._trigger(race)
        await self.repository.save_race(race)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def force_destroy(self):
        race = await self._active()
        if self._locked(race): raise ValueError('race cannot be destroyed while a build is in flight')
        race.phase = Phase.DESTROYING
        self._stamp(race, TerraformAction.DESTROY)
        await self._trigger(race)
        await self.repository.save_race(race)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def build_finished(self, event: BuildFinished | dict[str, Any]):
        event = event if isinstance(event, BuildFinished) else BuildFinished.model_validate(event)
        race = await self._active()
        run = race.terraform_run
        if event.race_id != race.race_id or not run or run.run_token != event.run_token or run.build_id != event.build_id or run.action != event.action:
            raise ValueError('build does not match active terraform run')
        if not run.locked: return
        run.status = TerraformStatus.SUCCEEDED if event.success else TerraformStatus.FAILED
        if event.action == TerraformAction.APPLY and not event.success and race.phase == Phase.STARTING:
            race.phase = Phase.FAILED
        elif event.action == TerraformAction.CLOSE_INGEST and event.success:
            race.phase = Phase.EXPORTING
            if self.shutdown:
                await self.shutdown.publish_shutdown(race.race_id)
        elif event.action == TerraformAction.DESTROY and event.success:
            race.phase = Phase.DESTROYED
            race.destroyed_at = datetime.now(timezone.utc)
            await self.repository.set_last(
                LastRace(race_id=race.race_id, gcs_prefix=race.gcs_prefix, destroyed_at=race.destroyed_at))
            await self.repository.set_active(ActivePointer())
        elif not event.success:
            race.last_error = event.error or 'terraform build failed'
        await self.repository.save_race(race)

    async def export_done(self, event: ExportDone | dict[str, Any]):
        event = event if isinstance(event, ExportDone) else ExportDone.model_validate(event)
        race = await self._active()
        if race.race_id != event.race_id or race.phase != Phase.EXPORTING or self._locked(race): raise ValueError(
            'export completion is not accepted')
        race.phase = Phase.DESTROYING
        self._stamp(race, TerraformAction.DESTROY)
        await self._trigger(race)
        await self.repository.save_race(race)

    async def _active(self):
        pointer = await self.repository.active()
        if not pointer.race_id: raise ValueError('no active race')
        race = await self.repository.race(pointer.race_id)
        if not race: raise ValueError('active race document missing')
        return race

    @staticmethod
    def _locked(race):
        return bool(race.terraform_run and race.terraform_run.locked)

    @staticmethod
    def _stamp(race, action):
        race.terraform_run = TerraformRun(run_token=str(uuid4()), stamped_at=datetime.now(timezone.utc), action=action)

    async def _trigger(self, race):
        if not self.builds:
            return
        try:
            run = race.terraform_run
            run.build_id = await self.builds.trigger(race.race_id, run.action.value, run.run_token)
        except Exception as exc:
            race.terraform_run.status = TerraformStatus.FAILED
            race.last_error = str(exc)
            await self.repository.save_race(race)
            raise
