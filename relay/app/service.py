import asyncio
import re
from datetime import datetime, timezone
from loguru import logger
from typing import Any
from uuid import uuid4

from .gcp.builds import BuildTrigger
from .gcp.repository import RaceRepository
from .gcp.storage import ObjectStore
from .kafka import ShutdownPublisher
from .models import BuildFinished, ExportDone, Phase, Race, TerraformAction, TerraformRun, \
    TerraformStatus

# GCP label-safe, Core cap 12 chars due to CompactString
GCP_LABEL_COMPACT_STRING_PATTERN = re.compile(r"^[a-z0-9]([a-z0-9_-]{0,11})$")

class RelayService:
    def __init__(self, repository: RaceRepository, object_store: ObjectStore | None = None,
                 builds: BuildTrigger | None = None, shutdown: ShutdownPublisher | None = None, heartbeat=None,
                 heartbeat_freshness_seconds: int = 50, build_bind_window_seconds: int = 600,
                 kafka_public_port: int = 9094):
        self.repository = repository
        self.object_store, self.builds, self.shutdown = object_store, builds, shutdown
        self.heartbeat, self.heartbeat_freshness_seconds = heartbeat, heartbeat_freshness_seconds
        self.build_bind_window_seconds = build_bind_window_seconds
        self.kafka_public_port = kafka_public_port
        # This lock is enough - cloud run instances capped at 1 in GCP
        self._operation_lock = asyncio.Lock()

    async def start(self, race_id: str, config: dict[str, Any], started_by='unknown'):
        async with self._operation_lock:
            return await self._start(race_id, config, started_by)

    async def _start(self, race_id: str, config: dict[str, Any], started_by='unknown'):
        logger.info("Starting race requested by {}", started_by)
        await self._reconcile()
        pointer = await self.repository.active()
        if pointer.race_id:
            if race_id != pointer.race_id:
                raise ValueError('raceId does not match the active race')
            race = await self._active()
            if race.phase != Phase.FAILED or self._locked(race): raise ValueError(
                'race cannot be started in its current state')
            race.phase = Phase.STARTING
            self._stamp(race, TerraformAction.APPLY)
            await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
        else:
            if await self.repository.race(race_id):
                raise ValueError('raceId already exists')
            now = datetime.now(timezone.utc)
            race = Race(race_id=race_id, phase=Phase.STARTING, started_by=started_by,
                        started_at=now)
            self._stamp(race, TerraformAction.APPLY)
            if self.object_store:
                await self.object_store.put_yaml(f'{race_id}/core-config.yaml', config)
            await self.repository.create_race(race)
        await self._trigger(race)
        await self.repository.save_race(race, {'terraform_run'})
        logger.info("Race {} entered starting phase with {} run", race.race_id, race.terraform_run.action.value)
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def status(self):
        async with self._operation_lock:
            await self._reconcile()
            pointer = await self.repository.active()
            if not pointer.race_id:
                logger.debug("Status requested with no active race")
                return {'raceId': None}
            race = await self._active()
            result = {'raceId': race.race_id, 'phase': race.phase.value,
                      'startedBy': race.started_by, 'startedAt': race.started_at,
                      'appliedAt': race.applied_at, 'stoppedAt': race.stopped_at,
                      'destroyedAt': race.destroyed_at, 'lastError': race.last_error,
                      'terraformRun': race.terraform_run.model_dump(mode='json',
                                                                    by_alias=True) if race.terraform_run else None,
                      'ready': False, 'unhealthy': False}
            if race.phase == Phase.APPLIED and self.heartbeat:
                try:
                    beat = await self.heartbeat.read_last()
                    if beat:
                        age = (datetime.now(timezone.utc) - beat).total_seconds()
                        heartbeat_fresh = age <= self.heartbeat_freshness_seconds
                        address_ready = bool(race.kafka_hostname and race.kafka_public_ip)
                        result['ready'] = heartbeat_fresh and address_ready
                        result['unhealthy'] = not result['ready']
                    else:
                        result['ready'] = result['unhealthy'] = False
                except Exception as exc:
                    result['unhealthy'] = True
                    result['lastError'] = str(exc)
            if race.phase == Phase.APPLIED and result['ready']:
                result.update({k: v for k, v in {
                    'kafkaInstance': race.kafka_instance,
                    'coreInstance': race.core_instance,
                    'kafkaPublicIp': race.kafka_public_ip,
                    'kafkaHostname': race.kafka_hostname,
                    'kafkaPort': race.kafka_port or self.kafka_public_port,
                }.items() if v is not None})
            return result

    async def history(self):
        pointer = await self.repository.active()
        races = await self.repository.races()
        historical = [race for race in races if race.race_id != pointer.race_id]
        historical.sort(key=lambda race: race.started_at, reverse=True)
        return [{'raceId': race.race_id, 'startedAt': race.started_at,
                 'destroyedAt': race.destroyed_at} for race in historical]

    async def historical_race(self, race_id: str, export_race: bool = False):
        race = await self.repository.race(race_id)
        if not race:
            raise ValueError('historical race not found')
        pointer = await self.repository.active()
        if pointer.race_id == race_id:
            raise ValueError('race is currently active, use active race status endpoint instead')
        result = race.model_dump(mode='json', by_alias=True)
        if export_race and self.object_store:
            result['exportUrls'] = await self.object_store.signed_export_url(f'{race.race_id}/export/')
        return result

    async def stop(self):
        async with self._operation_lock:
            return await self._stop()

    async def _stop(self):
        await self._reconcile()
        race = await self._active()
        logger.info("Stopping race {}", race.race_id)
        if race.phase not in (Phase.APPLIED, Phase.STOPPING) or self._locked(race): raise ValueError(
            'race cannot be stopped in its current state')
        race.phase = Phase.STOPPING
        race.stopped_at = datetime.now(timezone.utc)
        self._stamp(race, TerraformAction.CLOSE_INGEST)
        await self.repository.save_race(race, {'phase', 'stopped_at', 'terraform_run', 'last_error'})
        await self._trigger(race)
        await self.repository.save_race(race, {'terraform_run'})
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def force_destroy(self):
        async with self._operation_lock:
            return await self._force_destroy()

    async def _force_destroy(self):
        await self._reconcile()
        race = await self._active()
        logger.warning("Force destroy requested for race {}", race.race_id)
        if race.phase not in (Phase.FAILED, Phase.STARTING, Phase.APPLIED, Phase.STOPPING,
                              Phase.EXPORTING, Phase.DESTROYING) or self._locked(race):
            raise ValueError('race cannot be destroyed in its current state')
        race.phase = Phase.DESTROYING
        self._stamp(race, TerraformAction.DESTROY)
        await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
        await self._trigger(race)
        await self.repository.save_race(race, {'terraform_run'})
        return {'raceId': race.race_id, 'phase': race.phase.value}

    async def build_finished(self, event: BuildFinished | dict[str, Any]):
        async with self._operation_lock:
            return await self._build_finished(event)

    async def _build_finished(self, event: BuildFinished | dict[str, Any]):
        event = event if isinstance(event, BuildFinished) else BuildFinished.model_validate(event)
        logger.info("Build finished for race {}: action={}, success={}", event.race_id, event.action.value,
                    event.success)
        race = await self._active()
        run = race.terraform_run
        if event.race_id != race.race_id or not run or run.run_token != event.run_token or run.build_id != event.build_id or run.action != event.action:
            raise ValueError('build does not match active terraform run')
        if not run.locked:
            if (event.action == TerraformAction.CLOSE_INGEST and event.success
                    and race.phase == Phase.EXPORTING):
                await self._publish_shutdown(race.race_id)
            return
        run.status = TerraformStatus.SUCCEEDED if event.success else TerraformStatus.FAILED
        if event.action == TerraformAction.APPLY:
            if event.success and race.phase == Phase.STARTING:
                race.phase = Phase.FAILED
                race.last_error = 'apply completed without controller outputs'
            elif not event.success and race.phase == Phase.STARTING:
                race.phase = Phase.FAILED
                race.last_error = event.error or 'terraform build failed'
            elif not event.success and race.phase == Phase.APPLIED:
                run.status = TerraformStatus.SUCCEEDED
        elif event.action == TerraformAction.CLOSE_INGEST and event.success:
            race.phase = Phase.EXPORTING
            await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
            await self._publish_shutdown(race.race_id)
            logger.info("Race {} transitioned to phase {}", race.race_id, race.phase.value)
            return
        elif event.action == TerraformAction.DESTROY and event.success:
            race.phase = Phase.DESTROYED
            now = datetime.now(timezone.utc)
            race.destroyed_at = now
            await self.repository.complete_destroy(race)
            logger.info("Race {} transitioned to phase {}", race.race_id, race.phase.value)
            return
        elif not event.success:
            race.last_error = event.error or 'terraform build failed'
        await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
        logger.info("Race {} transitioned to phase {}", race.race_id, race.phase.value)

    async def export_done(self, event: ExportDone | dict[str, Any]):
        async with self._operation_lock:
            return await self._export_done(event)

    async def _export_done(self, event: ExportDone | dict[str, Any]):
        event = event if isinstance(event, ExportDone) else ExportDone.model_validate(event)
        logger.info("Export completed for race {}", event.race_id)
        race = await self._active()
        if race.race_id != event.race_id or race.phase != Phase.EXPORTING or self._locked(race): raise ValueError(
            'export completion is not accepted')
        race.phase = Phase.DESTROYING
        self._stamp(race, TerraformAction.DESTROY)
        await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
        await self._trigger(race)
        await self.repository.save_race(race, {'terraform_run'})

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
        race.last_error = None
        race.terraform_run = TerraformRun(run_token=str(uuid4()), stamped_at=datetime.now(timezone.utc), action=action)

    async def _trigger(self, race):
        if not self.builds:
            exc = RuntimeError('Cloud Build trigger is not configured')
            race.terraform_run.status = TerraformStatus.FAILED
            race.last_error = str(exc)
            if race.terraform_run.action == TerraformAction.APPLY:
                race.phase = Phase.FAILED
            await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
            raise exc
        try:
            run = race.terraform_run
            run.build_id = await self.builds.trigger(race.race_id, run.action.value, run.run_token)
            logger.info("Triggered {} build {} for race {}", run.action.value, run.build_id, race.race_id)
        except Exception as exc:
            logger.exception("Failed to trigger {} build for race {}", race.terraform_run.action.value, race.race_id)
            race.terraform_run.status = TerraformStatus.FAILED
            race.last_error = str(exc)
            if race.terraform_run.action == TerraformAction.APPLY:
                race.phase = Phase.FAILED
            await self.repository.save_race(race, {'phase', 'terraform_run', 'last_error'})
            raise

    async def reconcile(self):
        async with self._operation_lock:
            return await self._reconcile()

    async def _reconcile(self):
        """Recover missed pushes and bind builds whose trigger response was lost."""
        if not self.builds:
            return
        race = await self._active_or_none()
        if not race or not race.terraform_run or not race.terraform_run.locked:
            return
        run = race.terraform_run
        if not run.build_id:
            candidate = await self.builds.find(race.race_id, run.run_token)
            if candidate:
                run.build_id = candidate['buildId']
                await self.repository.save_race(race, {'terraform_run'})

        build = await self.builds.get(run.build_id) if run.build_id else None
        if build and run.build_id:
            await self._build_finished(BuildFinished(race_id=race.race_id, action=run.action,
                                                     run_token=run.run_token, build_id=run.build_id,
                                                     success=build['success'], error=build.get('error')))
        elif not run.build_id:
            age = (datetime.now(timezone.utc) - run.stamped_at).total_seconds()
            if age > self.build_bind_window_seconds:
                run.status = TerraformStatus.FAILED
                race.last_error = 'Cloud Build trigger did not start'
                fields = {'terraform_run', 'last_error'}
                if run.action == TerraformAction.APPLY and race.phase == Phase.STARTING:
                    race.phase = Phase.FAILED
                    fields.add('phase')
                await self.repository.save_race(race, fields)

    async def _active_or_none(self):
        pointer = await self.repository.active()
        return await self.repository.race(pointer.race_id) if pointer.race_id else None

    async def _publish_shutdown(self, race_id):
        if not self.shutdown:
            raise RuntimeError('shutdown publisher is not configured')
        try:
            await self.shutdown.publish_shutdown(race_id)
        finally:
            try:
                await self.shutdown.close()
            except Exception:
                logger.warning("Failed to close Kafka shutdown publisher")
