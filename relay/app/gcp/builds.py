from typing import Protocol
from google.cloud.devtools import cloudbuild_v1
from loguru import logger


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
    def __init__(self, project_id: str, trigger_id: str): self.project_id = project_id; self.trigger_id = trigger_id

    async def trigger(self, race_id, action, run_token):
        logger.info("Triggering Cloud Build for race {} ({})", race_id, action)
        client = cloudbuild_v1.CloudBuildAsyncClient()
        source = cloudbuild_v1.RepoSource(
            substitutions={
                '_RACE_ID': str(race_id),
                '_ACTION': str(action),
                '_RUN_TOKEN': str(run_token),
            }
        )

        request = cloudbuild_v1.RunBuildTriggerRequest(
            project_id=self.project_id,
            trigger_id=self.trigger_id,
            source=source,
        )

        op = await client.run_build_trigger(request=request)
        build_id = op.metadata.build.id

        logger.info("Cloud Build {} triggered for race {}", build_id, race_id)
        return build_id

    async def get(self, build_id):
        async with cloudbuild_v1.CloudBuildAsyncClient() as client:
            build = await client.get_build(project_id=self.project_id, id=build_id)
            if build.status not in (cloudbuild_v1.Build.Status.SUCCESS, cloudbuild_v1.Build.Status.FAILURE,
                                    cloudbuild_v1.Build.Status.TIMEOUT, cloudbuild_v1.Build.Status.CANCELLED):
                return None
            return {'buildId': build.id, 'success': build.status == cloudbuild_v1.Build.Status.SUCCESS,
                    'error': build.failure_info.detail if build.failure_info else None}

    async def find(self, race_id, run_token):
        async with cloudbuild_v1.CloudBuildAsyncClient() as client:
            builds = client.list_builds(project_id=self.project_id, filter=f'substitutions._RACE_ID="{race_id}" AND substitutions._RUN_TOKEN="{run_token}"')
            async for build in builds:
                if build.status not in (cloudbuild_v1.Build.Status.SUCCESS, cloudbuild_v1.Build.Status.FAILURE,
                                        cloudbuild_v1.Build.Status.TIMEOUT, cloudbuild_v1.Build.Status.CANCELLED):
                    continue
                return {'buildId': build.id, 'success': build.status == cloudbuild_v1.Build.Status.SUCCESS,
                        'error': build.failure_info.detail if build.failure_info else None}
        return None
