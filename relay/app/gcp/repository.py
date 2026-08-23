from typing import Any, Protocol

from google.cloud.firestore_v1 import AsyncClient
from loguru import logger

from relay.app.models import ActivePointer, LastRace, Phase, Race, TerraformAction, TerraformStatus


class RaceRepository(Protocol):
    async def active(self) -> ActivePointer: ...

    async def race(self, race_id: str) -> Race | None: ...

    async def create_race(self, race: Race) -> None: ...

    async def save_race(self, race: Race, fields: set[str] | None = None) -> None: ...

    async def set_active(self, pointer: ActivePointer) -> None: ...

    async def last(self) -> LastRace | None: ...

    async def set_last(self, last: LastRace) -> None: ...

    async def complete_destroy(self, race: Race, last: LastRace) -> None: ...


class InMemoryRaceRepository:
    def __init__(self): self._active = ActivePointer(); self._races = {}; self._last = None

    async def active(self): return self._active.model_copy(deep=True)

    async def race(self, race_id):
        race = self._races.get(race_id)
        return race.model_copy(deep=True) if race else None

    async def create_race(self, race):
        logger.debug("Creating race {} in memory", race.race_id)
        if self._active.race_id or race.race_id in self._races: raise ValueError('active race already exists')
        self._races[race.race_id] = race.model_copy(deep=True)
        self._active = ActivePointer(race_id=race.race_id)

    async def save_race(self, race, fields=None):
        logger.debug("Saving race {} in memory", race.race_id)
        self._races[race.race_id] = race.model_copy(deep=True)

    async def set_active(self, pointer): self._active = pointer.model_copy(deep=True)

    async def last(self): return self._last.model_copy(deep=True) if self._last else None

    async def set_last(self, last): self._last = last.model_copy(deep=True)

    async def complete_destroy(self, race, last):
        self._races[race.race_id] = race.model_copy(deep=True)
        self._last = last.model_copy(deep=True)
        self._active = ActivePointer()


class FirestoreRaceRepository:
    """Firestore adapter for the three Relay-owned document locations."""

    def __init__(self, project_id: str, database: str):
        self.project_id, self.database = project_id, database
        self._client = None

    def _db(self):
        if self._client is None:
            self._client = AsyncClient(project=self.project_id, database=self.database)
        return self._client

    async def active(self):
        snap = await self._db().collection('control').document('active').get()
        return ActivePointer.model_validate(snap.to_dict() or {})

    async def race(self, race_id):
        snap = await self._db().collection('races').document(race_id).get()
        return Race.model_validate(snap.to_dict()) if snap.exists else None

    async def create_race(self, race):
        logger.info("Creating race {} in Firestore", race.race_id)
        db = self._db()
        async with db.transaction() as tx:
            pointer = db.collection('control').document('active')
            pointer_data = (await pointer.get(transaction=tx)).to_dict() or {}
            if pointer_data.get('raceId') or pointer_data.get('race_id'):
                raise ValueError('active race already exists')
            tx.create(db.collection('races').document(race.race_id), race.model_dump(by_alias=True))
            tx.set(pointer, ActivePointer(race_id=race.race_id).model_dump(by_alias=True))

    async def save_race(self, race, fields=None):
        logger.debug("Saving race {} in Firestore", race.race_id)
        ref = self._db().collection('races').document(race.race_id)
        payload = race.model_dump(by_alias=True)
        if fields is None:
            await ref.set(payload)
            return

        updates = {Race.model_fields[field].alias or field: payload[Race.model_fields[field].alias or field]
                   for field in fields}
        async with self._db().transaction() as tx:
            snapshot = await ref.get(transaction=tx)
            current = snapshot.to_dict() or {}
            self._protect_controller_state(current, payload, updates)
            if updates:
                tx.update(ref, updates)

    async def set_active(self, pointer):
        await self._db().collection('control').document('active').set(pointer.model_dump(by_alias=True))

    async def last(self):
        snap = await self._db().collection('control').document('last').get()
        return LastRace.model_validate(snap.to_dict()) if snap.exists else None

    async def set_last(self, last):
        await self._db().collection('control').document('last').set(last.model_dump(by_alias=True))

    async def complete_destroy(self, race, last):
        db = self._db()
        async with db.transaction() as tx:
            tx.set(db.collection('races').document(race.race_id), race.model_dump(by_alias=True))
            tx.set(db.collection('control').document('last'), last.model_dump(by_alias=True))
            tx.set(db.collection('control').document('active'), ActivePointer().model_dump(by_alias=True))

    @staticmethod
    def _protect_controller_state(current: dict[str, Any], payload: dict[str, Any], updates: dict[str, Any]) -> None:
        """Do not let a stale Relay apply event replace Controller-committed state."""
        current_phase = current.get('phase')
        incoming_run = payload.get('terraformRun') or {}
        current_run = current.get('terraformRun') or {}
        same_apply = (incoming_run.get('action') == TerraformAction.APPLY.value and
                      current_run.get('runToken') == incoming_run.get('runToken'))
        if current_phase != Phase.APPLIED.value or not same_apply:
            return

        updates.pop('phase', None)
        updates.pop('lastError', None)
        desired_status = incoming_run.get('status')
        current_status = current_run.get('status')
        if desired_status == TerraformStatus.IN_FLIGHT.value and current_status != TerraformStatus.IN_FLIGHT.value:
            updates.pop('terraformRun', None)
        elif desired_status == TerraformStatus.FAILED.value:
            updates['terraformRun'] = {**incoming_run, 'status': TerraformStatus.SUCCEEDED.value}
