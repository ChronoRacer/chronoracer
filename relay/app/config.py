from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "info"
    config_file: str = "relay/config.yaml"


class OktaConfig(BaseModel):
    issuer: str
    audience: str
    jwks_url: str
    algorithms: list[str] = Field(default_factory=lambda: ["RS256"])


class PubSubConfig(BaseModel):
    build_finished_audience: str = ""
    export_done_audience: str = ""


class ServiceConfig(BaseModel):
    heartbeat_freshness_seconds: int = 30


class RelayConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    okta: OktaConfig
    pubsub: PubSubConfig = Field(default_factory=PubSubConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)


def load_config(path: str | Path | None = None) -> RelayConfig:
    config_path = Path(path) if path else Path(__file__).resolve().parents[1] / "config.yaml"
    with config_path.open(encoding="utf-8") as stream:
        values: dict[str, Any] = yaml.safe_load(stream) or {}
    return RelayConfig.model_validate(values)
