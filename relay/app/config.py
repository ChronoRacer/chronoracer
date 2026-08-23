import os
from pathlib import Path
from typing import Any

import yaml
from loguru import logger
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "info"
    config_file: str = "relay/config.yaml"


class Auth0Config(BaseModel):
    domain: str
    client_id: str
    client_secret: str
    app_base_url: str
    secret: str
    audience: str = ""
    authorization_params: dict[str, Any] = Field(default_factory=dict)
    pushed_authorization_requests: bool = False
    mount_routes: bool = True
    cookie_name: str = "_a0_session"
    session_expiration: int = 259200


class GCPConfig(BaseModel):
    enabled: bool = False
    project_id: str
    region: str = "europe-west3"
    firestore_database: str = "(default)"
    export_bucket: str
    cloud_build_trigger_id: str
    build_finished_audience: str = ""
    export_done_audience: str = ""
    build_finished_topic: str
    export_done_topic: str
    kafka_private_bootstrap: str
    kafka_control_topic: str


class ServiceConfig(BaseModel):
    heartbeat_freshness_seconds: int = 30


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
