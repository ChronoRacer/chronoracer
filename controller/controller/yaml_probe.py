from __future__ import annotations

from google.cloud import storage
from loguru import logger
from typing import Protocol


class YamlProbe(Protocol):
    def core_config_exists(self, race_id: str) -> bool: ...


class GcsYamlProbe:
    def __init__(self, project_id: str, bucket: str):
        self.bucket = bucket
        self._client = storage.Client(project=project_id)

    def core_config_exists(self, race_id: str) -> bool:
        object_name = f"{race_id}/core-config.yaml"
        logger.info("Probing gs://{}/{}", self.bucket, object_name)
        return self._client.bucket(self.bucket).blob(object_name).exists()
