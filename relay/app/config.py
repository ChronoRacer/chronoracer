import os
from pathlib import Path
from typing import Any

import yaml
from loguru import logger
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str
    port: int
    log_level: str


class OktaConfig(BaseModel):
    issuer: str
    audience: str


class GCPConfig(BaseModel):
    enabled: bool
    project_id: str
    firestore_database: str
    export_bucket: str
    cloud_build_trigger_id: str
    build_finished_audience: str
    build_finished_service_account: str
    export_done_audience: str
    export_done_service_account: str
    build_finished_topic: str
    export_done_topic: str
    kafka_private_bootstrap: str
    kafka_control_topic: str
    kafka_heartbeat_topic: str
    kafka_public_port: int = 9092


class ServiceConfig(BaseModel):
    heartbeat_freshness_seconds: int
    build_bind_window_seconds: int = 600


class RelayConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    okta: OktaConfig
    gcp: GCPConfig
    service: ServiceConfig = Field(default_factory=ServiceConfig)


RELAY_CONFIG_FILE_PATH_ENV = "RELAY_CONFIG_FILE_PATH"


def load_config(path: str | Path | None = None) -> RelayConfig:
    if path:
        config_path = Path(path)
    else:
        env_value = os.environ.get(RELAY_CONFIG_FILE_PATH_ENV)
        if env_value:
            config_path = Path(env_value)
        else:
            config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    with config_path.open(encoding="utf-8") as stream:
        values: dict[str, Any] = yaml.safe_load(stream) or {}
    config = RelayConfig.model_validate(values)
    logger.info("Loaded Relay configuration from {}", config_path)
    return config
