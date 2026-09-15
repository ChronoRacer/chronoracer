from google.cloud.firestore_v1 import DELETE_FIELD, AsyncClient
from google.cloud.firestore_v1.async_transaction import async_transactional
from loguru import logger
from typing import Any, Protocol

from ..models import ActivePointer, Phase, Race, TerraformAction, TerraformStatus


class RaceRepository(Protocol):
    async def active(self) -> ActivePointer: ...

    async def race(self, race_id: str) -> Race | None: ...

    async def races(self) -> list[Race]: ...

    async def create_race(self, race: Race) -> None: ...

    async def save_race(self, race: Race, fields: set[str] | None = None) -> None: ...

    async def set_active(self, pointer: ActivePointer) -> None: ...

    async def complete_destroy(self, race: Race) -> None: ...


class InMemoryRaceRepository(RaceRepository):
    def __init__(self):
        self._active = ActivePointer(); self._races = {}

    async def active(self):
        return self._active.model_copy(deep=True)

    async def race(self, race_id):
        race = self._races.get(race_id)
        return race.model_copy(deep=True) if race else None

    async def races(self):
        return [race.model_copy(deep=True) for race in self._races.values()]

    async def create_race(self, race):
        logger.debug("Creating race {} in memory", race.race_id)
        if race.race_id in self._races: raise ValueError('raceId already exists')
        if self._active.race_id: raise ValueError('active race already exists')
        self._races[race.race_id] = race.model_copy(deep=True)
        self._active = ActivePointer(race_id=race.race_id)

    async def save_race(self, race, fields=None):
        logger.debug("Saving race {} in memory", race.race_id)
        self._races[race.race_id] = race.model_copy(deep=True)

    async def set_active(self, pointer):
        self._active = pointer.model_copy(deep=True)

    async def complete_destroy(self, race):
        self._races[race.race_id] = race.model_copy(deep=True)
        self._active = ActivePointer()


class FirestoreRaceRepository(RaceRepository, ):
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

    async def races(self):
        snapshots = self._db().collection('races').stream()
        return [Race.model_validate(snapshot.to_dict()) async for snapshot in snapshots]

    async def create_race(self, race):
        logger.info("Creating race {} in Firestore", race.race_id)
        db = self._db()
        @async_transactional
        async def create_in_transaction(tx):
            pointer = db.collection('control').document('active')
            pointer_data = (await pointer.get(transaction=tx)).to_dict() or {}
            if pointer_data.get('raceId') or pointer_data.get('race_id'):
                raise ValueError('active race already exists')
            race_ref = db.collection('races').document(race.race_id)
            if (await race_ref.get(transaction=tx)).exists:
                raise ValueError('raceId already exists')
            tx.create(race_ref, race.model_dump(by_alias=True))
            tx.set(pointer, ActivePointer(race_id=race.race_id).model_dump(by_alias=True))

        await create_in_transaction(db.transaction())

    async def save_race(self, race, fields=None):
        logger.debug("Saving race {} in Firestore", race.race_id)
        ref = self._db().collection('races').document(race.race_id)
        payload = race.model_dump(by_alias=True)
        if fields is None:
            await ref.set(payload)
            return

        updates = {Race.model_fields[field].alias or field: payload[Race.model_fields[field].alias or field]
                   for field in fields}
        updates = {key: DELETE_FIELD if value is None else value for key, value in updates.items()}
        @async_transactional
        async def save_in_transaction(tx):
            snapshot = await ref.get(transaction=tx)
            current = snapshot.to_dict() or {}
            self._protect_controller_state(current, payload, updates)
            if updates:
                tx.update(ref, updates)

        await save_in_transaction(self._db().transaction())

    async def set_active(self, pointer):
        await self._db().collection('control').document('active').set(pointer.model_dump(by_alias=True))

    async def complete_destroy(self, race):
        db = self._db()
        @async_transactional
        async def complete_in_transaction(tx):
            tx.set(db.collection('races').document(race.race_id), race.model_dump(by_alias=True))
            tx.set(db.collection('control').document('active'), ActivePointer().model_dump(by_alias=True))

        await complete_in_transaction(db.transaction())

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
