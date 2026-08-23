from typing import Protocol

import yaml
from google.cloud import storage
from loguru import logger


class ObjectStore(Protocol):
    async def put_yaml(self, object_name: str, value: dict) -> None: ...

    async def signed_export_url(self, prefix: str) -> str: ...


class DevObjectStore:
    def __init__(self): self.objects = {}

    async def put_yaml(self, object_name, value):
        logger.debug("Writing development object {}", object_name)
        self.objects[object_name] = yaml.safe_dump(value, sort_keys=False)

    async def signed_export_url(self, prefix): return f"http://dev-storage/{prefix}"


class GCSObjectStore:
    def __init__(self, project_id: str, bucket: str): self.project_id, self.bucket = project_id, bucket

    async def put_yaml(self, object_name, value):
        logger.info("Writing object {} to bucket {}", object_name, self.bucket)
        client = storage.Client(project=self.project_id)
        blob = client.bucket(self.bucket).blob(object_name)
        blob.upload_from_string(yaml.safe_dump(value, sort_keys=False), content_type='application/yaml')

    async def signed_export_url(self, prefix):
        logger.debug("Creating signed export URL for {}", prefix)
        client = storage.Client(project=self.project_id)
        blobs = list(client.bucket(self.bucket).list_blobs(prefix=prefix))
        if not blobs:
            return None
        # A prefix is not a GCS object. Return a short-lived URL for the first
        # export object; callers can use the manifest/index object when present.
        blob = next((b for b in blobs if not b.name.endswith('/')), blobs[0])
        return blob.generate_signed_url(version='v4', expiration=900, method='GET')
