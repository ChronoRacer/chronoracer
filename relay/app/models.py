from datetime import datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Phase(StrEnum):
    STARTING = 'starting'
    FAILED = 'failed'
    APPLIED = 'applied'
    STOPPING = 'stopping'
    EXPORTING = 'exporting'
    DESTROYING = 'destroying'
    DESTROYED = 'destroyed'


class TerraformAction(StrEnum): APPLY = 'apply'; CLOSE_INGEST = 'close_ingest'; DESTROY = 'destroy'


class TerraformStatus(StrEnum): IN_FLIGHT = 'in_flight'; SUCCEEDED = 'succeeded'; FAILED = 'failed'


class TerraformRun(BaseModel):
    model_config = ConfigDict(extra='forbid')
    run_token: str
    stamped_at: datetime
    build_id: str = ''
    action: TerraformAction
    status: TerraformStatus = TerraformStatus.IN_FLIGHT

    @property
    def locked(self): return self.status == TerraformStatus.IN_FLIGHT


class Race(BaseModel):
    model_config = ConfigDict(extra='forbid')
    race_id: str
    phase: Phase
    gcs_prefix: str
    started_by: str
    started_at: datetime
    terraform_run: TerraformRun | None = None
    kafka_instance: str | None = None
    core_instance: str | None = None
    kafka_public_ip: str | None = None
    kafka_hostname: str | None = None
    applied_at: datetime | None = None
    stopped_at: datetime | None = None
    destroyed_at: datetime | None = None
    last_error: str | None = None


class ActivePointer(BaseModel):
    race_id: str = ''
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LastRace(BaseModel): race_id: str; gcs_prefix: str; destroyed_at: datetime


class BuildFinished(BaseModel):
    race_id: str
    action: TerraformAction
    run_token: str
    build_id: str
    success: bool
    error: str | None = None


class ExportDone(BaseModel): race_id: str
