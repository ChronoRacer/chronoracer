import pytest
from app.gcp.builds import CloudBuildTrigger
from google.cloud.devtools import cloudbuild_v1
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

PARENT = "projects/proj/locations/europe-west1"


class _AsyncSeq:
    def __init__(self, items):
        self._items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


def _client(monkeypatch, **methods):
    fake = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    for name, value in methods.items():
        setattr(fake, name, value)
    monkeypatch.setattr(cloudbuild_v1, "CloudBuildAsyncClient", lambda: fake)
    return fake


@pytest.mark.asyncio
async def test_trigger_run_get_list_are_regional(monkeypatch):
    trigger = CloudBuildTrigger("proj", "race-controller", "europe-west1")
    op = SimpleNamespace(metadata=SimpleNamespace(build=SimpleNamespace(id="build-9")))
    run = AsyncMock(return_value=op)
    get = AsyncMock(return_value=SimpleNamespace(
        id="build-9",
        status=cloudbuild_v1.Build.Status.SUCCESS,
        failure_info=None,
    ))
    list_builds = MagicMock(return_value=_AsyncSeq([SimpleNamespace(id="build-9")]))
    _client(monkeypatch, run_build_trigger=run, get_build=get, list_builds=list_builds)

    assert await trigger.trigger("r1", "apply", "tok") == "build-9"
    assert run.await_args.kwargs["request"].name == f"{PARENT}/triggers/race-controller"
    assert run.await_args.kwargs["timeout"] == trigger.API_TIMEOUT_SECONDS

    got = await trigger.get("build-9")
    assert got == {"buildId": "build-9", "success": True, "error": None}
    assert get.await_args.kwargs["request"].name == f"{PARENT}/builds/build-9"
    assert get.await_args.kwargs["timeout"] == trigger.API_TIMEOUT_SECONDS

    assert await trigger.find("r1", "tok") == {"buildId": "build-9"}
    list_req = list_builds.call_args.kwargs["request"]
    assert list_req.parent == PARENT
    assert 'substitutions._RACE_ID="r1"' in list_req.filter
    assert 'substitutions._RUN_TOKEN="tok"' in list_req.filter
    assert list_builds.call_args.kwargs["timeout"] == trigger.API_TIMEOUT_SECONDS
