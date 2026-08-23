from typing import Protocol
from google.cloud.devtools import cloudbuild_v1
from loguru import logger


class BuildTrigger(Protocol):
    async def trigger(self, race_id: str, action: str, run_token: str) -> str: ...


class DevBuildTrigger:
    def __init__(self): self.calls = []; self.next_id = 1

    async def trigger(self, race_id, action, run_token):
        build_id = f'dev-build-{self.next_id}'
        self.next_id += 1
        self.calls.append((build_id, race_id, action, run_token))
        logger.debug("Queued development build {} for race {} ({})", build_id, race_id, action)
        return build_id


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
