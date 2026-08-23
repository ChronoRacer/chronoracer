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


class Auth0Config(BaseModel):
    domain: str
    client_id: str
    client_secret: str
    app_base_url: str
    secret: str
    audience: str
    authorization_params: dict[str, Any] = Field(default_factory=dict)
    pushed_authorization_requests: bool
    mount_routes: bool
    cookie_name: str
    session_expiration: int


class GCPConfig(BaseModel):
    enabled: bool
    project_id: str
    firestore_database: str
    export_bucket: str
    cloud_build_trigger_id: str
    build_finished_audience: str
    export_done_audience: str
    build_finished_topic: str
    export_done_topic: str
    kafka_private_bootstrap: str
    kafka_control_topic: str


class ServiceConfig(BaseModel):
    heartbeat_freshness_seconds: int


class RelayConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    auth0: Auth0Config
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
