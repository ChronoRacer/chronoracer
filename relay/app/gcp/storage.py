import asyncio
import datetime
import google.auth
import yaml
from google.auth.transport.requests import Request
from google.cloud import storage
from loguru import logger
from typing import Protocol


def iam_sign_params():
    """Cloud Run ADC has a token, not a private key. V4 signing then needs IAM signBlob.

    google-cloud-storage calls IAM only when both service_account_email and access_token
    are set. Otherwise it demands Signing credentials and raises AttributeError.
    """
    credentials, _ = google.auth.default()
    if not getattr(credentials, "token", None):
        credentials.refresh(Request())
    email = getattr(credentials, "service_account_email", None)
    token = getattr(credentials, "token", None)
    if not email or email == "default" or not token:
        raise RuntimeError(
            "cannot sign GCS URLs: ADC has no service account email and access token"
        )
    return credentials, email, token


class ObjectStore(Protocol):
    async def put_yaml(self, object_name: str, value: dict) -> None: ...

    async def signed_export_url(self, prefix: str) -> dict[str, str]: ...


class DevObjectStore:
    def __init__(self): self.objects = {}

    async def put_yaml(self, object_name, value):
        logger.debug("Writing development object {}", object_name)
        self.objects[object_name] = yaml.safe_dump(value, sort_keys=False)

    async def signed_export_url(self, prefix):
        return {"mock": f"http://dev-storage/{prefix}"}


class GCSObjectStore:
    def __init__(self, project_id: str, bucket: str):
        self.project_id, self.bucket = project_id, bucket

    async def put_yaml(self, object_name, value):
        await asyncio.to_thread(self._put_yaml_sync, object_name, value)

    def _put_yaml_sync(self, object_name, value):
        logger.info("Writing object {} to bucket {}", object_name, self.bucket)
        client = storage.Client(project=self.project_id)
        blob = client.bucket(self.bucket).blob(object_name)
        blob.upload_from_string(yaml.safe_dump(value, sort_keys=False), content_type='application/yaml')

    async def signed_export_url(self, prefix: str) -> dict[str, str]:
        return await asyncio.to_thread(self._signed_export_url_sync, prefix)

    def _signed_export_url_sync(self, prefix: str) -> dict[str, str]:
        credentials, email, token = iam_sign_params()
        client = storage.Client(project=self.project_id, credentials=credentials)
        bucket = client.bucket(self.bucket)
        blobs = bucket.list_blobs(prefix=prefix)

        signed_urls = {}
        for blob in blobs:
            if blob.name.endswith('/'):
                continue
            signed_urls[blob.name] = blob.generate_signed_url(
                version="v4",
                expiration=datetime.timedelta(minutes=15),
                method="GET",
                service_account_email=email,
                access_token=token,
            )
        return signed_urls
