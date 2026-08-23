from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
from loguru import logger

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
        logger.info("Starting race requested by {}", started_by)
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
        await self.repository.save_race(race)
        await self._trigger(race)
        await self.repository.save_race(race)
        logger.info("Race {} entered starting phase with {} run", race.race_id, race.terraform_run.action.value)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def status(self):
        pointer = await self.repository.active()
        if not pointer.race_id:
            logger.debug("Status requested with no active race")
            last = await self.repository.last()
            result = {'last': last.model_dump(mode='json') if last else None, 'billableVms': False,
                      'ready': False, 'unhealthy': False}
            if last and self.object_store:
                result['exportDownloadUrl'] = await self.object_store.signed_export_url(
                    f'{last.gcs_prefix}export/')
            return result
        race = await self._active()
        result = {'raceId': race.race_id, 'phase': race.phase.value, 'gcsPrefix': race.gcs_prefix,
                'startedBy': race.started_by, 'startedAt': race.started_at,
                'appliedAt': race.applied_at, 'stoppedAt': race.stopped_at,
                'destroyedAt': race.destroyed_at, 'lastError': race.last_error,
                'terraformRun': race.terraform_run.model_dump(mode='json') if race.terraform_run else None,
                'billableVms': True, 'ready': False, 'unhealthy': False}
        if self.object_store:
            result['exportDownloadUrl'] = await self.object_store.signed_export_url(
                f'{race.gcs_prefix}export/')
        if race.phase == Phase.APPLIED:
            result.update({k: v for k, v in {
                'kafkaInstance': race.kafka_instance,
                'coreInstance': race.core_instance,
                'kafkaPublicIp': race.kafka_public_ip,
                'kafkaHostname': race.kafka_hostname,
            }.items() if v is not None})
        return result

    async def stop(self):
        race = await self._active()
        logger.info("Stopping race {}", race.race_id)
        if race.phase not in (Phase.APPLIED, Phase.STOPPING) or self._locked(race): raise ValueError(
            'race cannot be stopped in its current state')
        race.phase = Phase.STOPPING
        race.stopped_at = datetime.now(timezone.utc)
        self._stamp(race, TerraformAction.CLOSE_INGEST)
        await self.repository.save_race(race)
        await self._trigger(race)
        await self.repository.save_race(race)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def force_destroy(self):
        race = await self._active()
        logger.warning("Force destroy requested for race {}", race.race_id)
        if self._locked(race): raise ValueError('race cannot be destroyed while a build is in flight')
        race.phase = Phase.DESTROYING
        self._stamp(race, TerraformAction.DESTROY)
        await self.repository.save_race(race)
        await self._trigger(race)
        await self.repository.save_race(race)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def build_finished(self, event: BuildFinished | dict[str, Any]):
        event = event if isinstance(event, BuildFinished) else BuildFinished.model_validate(event)
        logger.info("Build finished for race {}: action={}, success={}", event.race_id, event.action.value, event.success)
        race = await self._active()
        run = race.terraform_run
        if event.race_id != race.race_id or not run or run.run_token != event.run_token or run.build_id != event.build_id or run.action != event.action:
            raise ValueError('build does not match active terraform run')
        if not run.locked: return
        run.status = TerraformStatus.SUCCEEDED if event.success else TerraformStatus.FAILED
        if event.action == TerraformAction.APPLY:
            if event.success and race.phase == Phase.STARTING:
                race.phase = Phase.FAILED
                race.last_error = 'apply completed without controller outputs'
            elif not event.success and race.phase == Phase.STARTING:
                race.phase = Phase.FAILED
            elif not event.success and race.phase == Phase.APPLIED:
                run.status = TerraformStatus.SUCCEEDED
        elif event.action == TerraformAction.CLOSE_INGEST and event.success:
            race.phase = Phase.EXPORTING
            await self.repository.save_race(race)
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
        logger.info("Race {} transitioned to phase {}", race.race_id, race.phase.value)

    async def export_done(self, event: ExportDone | dict[str, Any]):
        event = event if isinstance(event, ExportDone) else ExportDone.model_validate(event)
        logger.info("Export completed for race {}", event.race_id)
        race = await self._active()
        if race.race_id != event.race_id or race.phase != Phase.EXPORTING or self._locked(race): raise ValueError(
            'export completion is not accepted')
        race.phase = Phase.DESTROYING
        self._stamp(race, TerraformAction.DESTROY)
        await self.repository.save_race(race)
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
            logger.info("Triggered {} build {} for race {}", run.action.value, run.build_id, race.race_id)
        except Exception as exc:
            logger.exception("Failed to trigger {} build for race {}", race.terraform_run.action.value, race.race_id)
            race.terraform_run.status = TerraformStatus.FAILED
            race.last_error = str(exc)
            await self.repository.save_race(race)
            raise
