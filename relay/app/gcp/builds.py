from google.cloud.devtools import cloudbuild_v1
from loguru import logger
from typing import Protocol


class BuildTrigger(Protocol):
    async def trigger(self, race_id: str, action: str, run_token: str) -> str: ...

    async def get(self, build_id: str) -> dict | None: ...

    async def find(self, race_id: str, run_token: str) -> dict | None: ...


class DevBuildTrigger:
    def __init__(self): self.calls = []; self.next_id = 1

    async def trigger(self, race_id, action, run_token):
        build_id = f'dev-build-{self.next_id}'
        self.next_id += 1
        self.calls.append((build_id, race_id, action, run_token))
        logger.debug("Queued development build {} for race {} ({})", build_id, race_id, action)
        return build_id

    async def get(self, build_id): return None

    async def find(self, race_id, run_token): return None


class CloudBuildTrigger:
    # Cloud Run allows 45 seconds per request. Keep control-plane RPCs below
    # that limit so a failed trigger cannot strand an in-flight mutex until
    # the request is killed by the platform.
    API_TIMEOUT_SECONDS = 30

    def __init__(self, project_id: str, trigger_id: str, location: str):
        self.project_id = project_id
        self.trigger_id = trigger_id
        self.location = location

    def _parent(self) -> str:
        return f"projects/{self.project_id}/locations/{self.location}"

    def _trigger_name(self) -> str:
        return f"{self._parent()}/triggers/{self.trigger_id}"

    def _build_name(self, build_id: str) -> str:
        return f"{self._parent()}/builds/{build_id}"

    async def trigger(self, race_id, action, run_token):
        logger.info("Triggering Cloud Build for race {} ({}) in {}", race_id, action, self.location)
        async with cloudbuild_v1.CloudBuildAsyncClient() as client:
            source = cloudbuild_v1.RepoSource(
                substitutions={
                    '_RACE_ID': str(race_id),
                    '_ACTION': str(action),
                    '_RUN_TOKEN': str(run_token),
                }
            )

            request = cloudbuild_v1.RunBuildTriggerRequest(
                name=self._trigger_name(),
                source=source,
            )

            op = await client.run_build_trigger(request=request, timeout=self.API_TIMEOUT_SECONDS)
            build_id = op.metadata.build.id

        logger.info("Cloud Build {} triggered for race {}", build_id, race_id)
        return build_id

    async def get(self, build_id):
        async with cloudbuild_v1.CloudBuildAsyncClient() as client:
            build = await client.get_build(
                request=cloudbuild_v1.GetBuildRequest(name=self._build_name(build_id)),
                timeout=self.API_TIMEOUT_SECONDS,
            )
            if not self._is_terminal(build.status):
                return None
            return {'buildId': build.id, 'success': build.status == cloudbuild_v1.Build.Status.SUCCESS,
                    'error': build.failure_info.detail if build.failure_info else None}

    async def find(self, race_id, run_token):
        async with cloudbuild_v1.CloudBuildAsyncClient() as client:
            builds = client.list_builds(request=cloudbuild_v1.ListBuildsRequest(
                parent=self._parent(),
                filter=f'substitutions._RACE_ID="{race_id}" AND substitutions._RUN_TOKEN="{run_token}"',
            ), timeout=self.API_TIMEOUT_SECONDS)
            async for build in builds:
                return {'buildId': build.id}
        return None

    @staticmethod
    def _is_terminal(status):
        return status in (cloudbuild_v1.Build.Status.SUCCESS, cloudbuild_v1.Build.Status.FAILURE,
                          cloudbuild_v1.Build.Status.INTERNAL_ERROR, cloudbuild_v1.Build.Status.TIMEOUT,
                          cloudbuild_v1.Build.Status.CANCELLED)
