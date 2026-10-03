from datetime import datetime, timezone

import pytest

from app.gcp.repository import FirestoreRaceRepository, InMemoryRaceRepository
from app.models import Phase, Race, TerraformAction, TerraformRun, TerraformStatus


class FakeSnapshot:
    def __init__(self, data=None, exists=True):
        self._data = data
        self.exists = exists

    def to_dict(self):
        return self._data


class FakeDocument:
    def __init__(self, path, snapshot=None):
        self.path = path
        self.snapshot = snapshot or FakeSnapshot({}, exists=False)

    async def get(self, *, transaction=None):
        assert transaction is not None
        return self.snapshot


class FakeCollection:
    def __init__(self, documents):
        self.documents = documents

    def document(self, name):
        return self.documents.setdefault(name, FakeDocument(name))


class FakeTransaction:
    def __init__(self):
        self._read_only = False
        self._max_attempts = 1
        self._id = None
        self.operations = []
        self.started = False
        self.committed = False

    def _clean_up(self):
        self.started = False

    async def _begin(self, retry_id=None):
        assert not self.started
        self.started = True
        self._id = b'transaction'

    async def _commit(self):
        assert self.started
        self.committed = True
        self.started = False
        self._id = None

    async def _rollback(self):
        self.started = False
        self._id = None

    def create(self, reference, data):
        assert self.started
        self.operations.append(('create', reference.path, data))

    def set(self, reference, data):
        assert self.started
        self.operations.append(('set', reference.path, data))

    def update(self, reference, data):
        assert self.started
        self.operations.append(('update', reference.path, data))


class FakeDatabase:
    def __init__(self):
        self.collections = {'control': {}, 'races': {}}
        self.transactions = []

    def collection(self, name):
        return FakeCollection(self.collections[name])

    def transaction(self):
        transaction = FakeTransaction()
        self.transactions.append(transaction)
        return transaction


@pytest.mark.asyncio
async def test_create_race_uses_an_active_firestore_transaction():
    database = FakeDatabase()
    repository = FirestoreRaceRepository('project', '(default)')
    repository._client = database
    race = Race(
        race_id='race1',
        phase=Phase.STARTING,
        started_by='operator',
        started_at=datetime.now(timezone.utc),
        terraform_run=TerraformRun(
            run_token='token',
            stamped_at=datetime.now(timezone.utc),
            action=TerraformAction.APPLY,
        ),
    )

    await repository.create_race(race)

    transaction = database.transactions[0]
    assert transaction.committed
    assert [operation[0] for operation in transaction.operations] == ['create', 'set']


@pytest.mark.asyncio
@pytest.mark.parametrize('adapter', ['memory', 'firestore'])
async def test_partial_apply_update_preserves_controller_commit(adapter):
    race = Race(
        race_id='race1', phase=Phase.STARTING, started_by='operator',
        started_at=datetime.now(timezone.utc), terraform_run=TerraformRun(
            run_token='token', stamped_at=datetime.now(timezone.utc),
            build_id='build1', action=TerraformAction.APPLY,
        ),
    )
    committed = race.model_copy(deep=True)
    committed.phase = Phase.APPLIED
    committed.kafka_hostname = 'kafka.example.test'
    committed.telemetry_url = 'https://telemetry.example.test'
    committed.applied_at = datetime.now(timezone.utc)
    stale = race.model_copy(deep=True)
    stale.phase = Phase.FAILED
    stale.last_error = 'late build failure'
    stale.terraform_run.status = TerraformStatus.FAILED
    if adapter == 'memory':
        repository = InMemoryRaceRepository()
        await repository.create_race(committed)
    else:
        database = FakeDatabase()
        document = FakeDocument('race1', FakeSnapshot(committed.model_dump(by_alias=True)))
        database.collections['races']['race1'] = document
        repository = FirestoreRaceRepository('project', '(default)')
        repository._client = database

    await repository.save_race(stale, {'phase', 'terraform_run', 'last_error'})
    if adapter == 'memory':
        result = await repository.race('race1')
    else:
        transaction = database.transactions[0]
        assert transaction.committed
        updates = transaction.operations[0][2]
        result = Race.model_validate({**committed.model_dump(by_alias=True), **updates})
    assert result.phase is Phase.APPLIED
    assert result.terraform_run.status is TerraformStatus.SUCCEEDED
    assert result.last_error is None
    assert result.kafka_hostname == committed.kafka_hostname
    assert result.telemetry_url == committed.telemetry_url
    assert result.applied_at == committed.applied_at


@pytest.mark.asyncio
async def test_partial_memory_save_does_not_overwrite_unselected_fields():
    repository = InMemoryRaceRepository()
    race = Race(race_id='race1', phase=Phase.STARTING, started_by='operator', started_at=datetime.now(timezone.utc))
    await repository.create_race(race)
    stale = await repository.race('race1')
    race.kafka_hostname = 'controller.example.test'
    await repository.save_race(race)
    stale.last_error = 'failure'
    await repository.save_race(stale, {'last_error'})
    assert (await repository.race('race1')).kafka_hostname == 'controller.example.test'
