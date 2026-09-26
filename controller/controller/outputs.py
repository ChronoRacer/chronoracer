from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TerraformOutputs:
    kafka_instance: str
    core_instance: str
    kafka_public_ip: str
    kafka_hostname: str
    telemetry_url: str
