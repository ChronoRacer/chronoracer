"""Load and invert analysis/sensor_config.json (BUSINESS_KEY -> physical_id)."""

from __future__ import annotations

import orjson
from google.cloud import storage
from loguru import logger

from analysis.shared.gcs_retry import retry_gcs_call

BUCKET_NAME = "chronoracer-artifacts"
SENSOR_CONFIG_BLOB = "analysis/sensor_config.json"


def load_sensor_config(client: storage.Client | None = None) -> dict[str, str]:
    """Return BUSINESS_KEY -> physical_id from GCS."""
    storage_client = client or storage.Client("chronoracer")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SENSOR_CONFIG_BLOB)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{BUCKET_NAME}/{SENSOR_CONFIG_BLOB} does not exist")
    data = orjson.loads(blob.download_as_text())
    if not isinstance(data, dict) or not data:
        raise ValueError(f"{SENSOR_CONFIG_BLOB} must be a non-empty object")
    mapping: dict[str, str] = {}
    for business_key, physical_id in data.items():
        if not isinstance(business_key, str) or not business_key.strip():
            raise ValueError(f"invalid business key in {SENSOR_CONFIG_BLOB}: {business_key!r}")
        if not isinstance(physical_id, str) or not physical_id.strip():
            raise ValueError(
                f"invalid physical id for {business_key!r} in {SENSOR_CONFIG_BLOB}: {physical_id!r}"
            )
        mapping[business_key.strip()] = physical_id.strip()
    logger.info("Loaded {} sensor mappings from {}", len(mapping), SENSOR_CONFIG_BLOB)
    return mapping


def invert_map(business_to_physical: dict[str, str]) -> dict[str, str]:
    """Return physical_id -> BUSINESS_KEY. Duplicate physical ids raise."""
    inverted: dict[str, str] = {}
    for business_key, physical_id in business_to_physical.items():
        if physical_id in inverted:
            raise ValueError(
                f"duplicate physical id {physical_id!r} for "
                f"{inverted[physical_id]!r} and {business_key!r}"
            )
        inverted[physical_id] = business_key
    return inverted


def to_business_id(physical_id: str, physical_to_business: dict[str, str]) -> str | None:
    return physical_to_business.get(physical_id)


def load_physical_to_business(client: storage.Client | None = None) -> dict[str, str]:
    return invert_map(load_sensor_config(client))


def load_physical_to_business_with_retry(
    client: storage.Client | None = None,
    sleep=None,
) -> dict[str, str]:
    kwargs = {} if sleep is None else {"sleep": sleep}
    return retry_gcs_call(
        lambda: load_physical_to_business(client),
        what=f"load gs://{BUCKET_NAME}/{SENSOR_CONFIG_BLOB}",
        **kwargs,
    )
