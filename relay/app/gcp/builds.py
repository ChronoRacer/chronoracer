from typing import Protocol
from google.cloud.devtools import cloudbuild_v1


class BuildTrigger(Protocol):
    async def trigger(self, race_id: str, action: str, run_token: str) -> str: ...


class DevBuildTrigger:
    def __init__(self): self.calls = []; self.next_id = 1

    async def trigger(self, race_id, action, run_token):
        build_id = f'dev-build-{self.next_id}'
        self.next_id += 1
        self.calls.append((build_id, race_id, action, run_token))
        return build_id


class CloudBuildTrigger:
    def __init__(self, project_id: str, trigger_id: str): self.project_id = project_id; self.trigger_id = trigger_id

    async def trigger(self, race_id, action, run_token):
        client = cloudbuild_v1.CloudBuildClient()
        build = cloudbuild_v1.Build(substitutions={'_RACE_ID': race_id, '_ACTION': action, '_RUN_TOKEN': run_token})
        op = client.run_build_trigger(project_id=self.project_id, trigger_id=self.trigger_id, source=None,
                                      substitutions=build.substitutions)
        return op.metadata.build.id
