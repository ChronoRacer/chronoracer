"""Interactive CLI: map business sensor keys to physical car sensor_ids and upload.

Reads ``gs://chronoracer-artifacts/analysis/sensor_needed.json``,
prompts for each ``RULES_REQUIRED_SENSOR_IDS`` entry, writes
``gs://chronoracer-artifacts/analysis/sensor_config.json``.

    python -m analysis.rules.build_config
    python -m analysis.rules.build_config --force
"""

from __future__ import annotations

import argparse
import json
import sys

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import storage
from loguru import logger

BUCKET_NAME = "chronoracer-artifacts"
PREFIX = "analysis/"
SENSOR_NEEDED = "sensor_needed.json"
SENSOR_CONFIG = "sensor_config.json"
REQUIRED_KEY = "RULES_REQUIRED_SENSOR_IDS"


def connect_to_gcp() -> tuple[storage.Client, storage.Bucket]:
    client = storage.Client("chronoracer")
    bucket = client.bucket(BUCKET_NAME)
    return client, bucket


def load_required_sensors(blob: storage.Blob) -> list[str]:
    raw = blob.download_as_text()
    data = json.loads(raw)
    if not isinstance(data, dict) or REQUIRED_KEY not in data:
        raise ValueError(f"{SENSOR_NEEDED} must contain key {REQUIRED_KEY!r}")
    sensors = data[REQUIRED_KEY]
    if not isinstance(sensors, list) or not sensors:
        raise ValueError(f"{REQUIRED_KEY} must be a non-empty list")
    if not all(isinstance(s, str) and s.strip() for s in sensors):
        raise ValueError(f"{REQUIRED_KEY} entries must be non-empty strings")
    return [s.strip() for s in sensors]


def sensor_config_is_populated(blob: storage.Blob) -> bool:
    if not blob.exists():
        return False
    text = blob.download_as_text().strip()
    if not text or text in ("{}", "[]", "null"):
        return False
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return True
    return bool(parsed)


def prompt_mapping(logical_ids: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    total = len(logical_ids)
    logger.info("Map {} business sensor keys to physical car sensor_ids", total)
    print()
    for i, logical_id in enumerate(logical_ids, start=1):
        while True:
            try:
                value = input(f"[{i}/{total}] {logical_id} -> physical sensor_id: ").strip()
            except EOFError:
                print()
                logger.error("Interrupted; no config uploaded")
                sys.exit(1)
            if value:
                mapping[logical_id] = value
                break
            print("  physical sensor_id cannot be empty")
    print()
    return mapping


def upload_sensor_config(blob: storage.Blob, mapping: dict[str, str]) -> None:
    payload = json.dumps(mapping, indent=2, ensure_ascii=False) + "\n"
    blob.upload_from_string(payload, content_type="application/json")
    logger.info("Uploaded gs://{}/{}", BUCKET_NAME, blob.name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build analysis/sensor_config.json from analysis/sensor_needed.json"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing non-empty sensor_config.json",
    )
    args = parser.parse_args()

    needed_path = PREFIX + SENSOR_NEEDED
    config_path = PREFIX + SENSOR_CONFIG

    try:
        _client, bucket = connect_to_gcp()

        config_blob = bucket.blob(config_path)
        if sensor_config_is_populated(config_blob) and not args.force:
            logger.error(
                "{} already exists at gs://{}/{} — pass --force to overwrite",
                SENSOR_CONFIG,
                BUCKET_NAME,
                config_blob.name,
            )
            sys.exit(1)

        needed_blob = bucket.blob(needed_path)
        if not needed_blob.exists():
            logger.error("{} does not exist at gs://{}/{}", SENSOR_NEEDED, BUCKET_NAME, needed_path)
            sys.exit(1)

        logical_ids = load_required_sensors(needed_blob)
        logger.info("Loaded {} keys from {}", len(logical_ids), needed_path)

        mapping = prompt_mapping(logical_ids)
        upload_sensor_config(config_blob, mapping)
        logger.info("Done: {} mappings in {}", len(mapping), config_path)

    except Forbidden as ex:
        logger.error("Brak wymaganych uprawnień do GCP: {}", ex)
        sys.exit(1)
    except NotFound as ex:
        logger.error("Obiekt nie istnieje albo konto nie może go odczytać: {}", ex)
        sys.exit(1)
    except (ValueError, json.JSONDecodeError) as ex:
        logger.error("Invalid {}: {}", SENSOR_NEEDED, ex)
        sys.exit(1)
    except Exception as ex:
        logger.error("Nieoczekiwany błąd podczas budowania konfiguracji: {}", ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
