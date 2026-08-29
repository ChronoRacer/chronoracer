from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from loguru import logger

ACTIONS = frozenset({"apply", "close_ingest", "destroy"})
REQUIRED_ENV = ("RACE_ID", "ACTION", "RUN_TOKEN", "PROJECT_ID", "KAFKA_FQDN", "ZONE")


@dataclass(frozen=True)
class Config:
    race_id: str
    action: str
    run_token: str
    project_id: str
    kafka_fqdn: str
    zone: str
    terraform_dir: Path
    names_path: Path
    firestore_database: str


def load_config(env: dict[str, str] | None = None) -> Config | None:
    source = env if env is not None else os.environ
    missing = [name for name in REQUIRED_ENV if not source.get(name)]
    if missing:
        logger.error("Missing env: {}", ", ".join(missing))
        return None
    action = source["ACTION"]
    if action not in ACTIONS:
        logger.error("Invalid ACTION: {}", action)
        return None
    return Config(
        race_id=source["RACE_ID"],
        action=action,
        run_token=source["RUN_TOKEN"],
        project_id=source["PROJECT_ID"],
        kafka_fqdn=source["KAFKA_FQDN"],
        zone=source["ZONE"],
        terraform_dir=Path(source.get("TERRAFORM_DIR", "/app/infra/race")),
        names_path=Path(source.get("NAMES_JSON", "/app/infra/contract/names.json")),
        firestore_database=source.get("FIRESTORE_DATABASE", "(default)"),
    )
