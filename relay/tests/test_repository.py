from datetime import datetime, timezone

import pytest

from app.gcp.repository import FirestoreRaceRepository
from app.models import Phase, Race, TerraformAction, TerraformRun


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
