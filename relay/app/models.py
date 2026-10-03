from datetime import datetime, timezone
from enum import StrEnum
from pydantic import BaseModel, ConfigDict, Field
from typing import Any


class Phase(StrEnum):
    STARTING = 'starting'
    FAILED = 'failed'
    APPLIED = 'applied'
    STOPPING = 'stopping'
    EXPORTING = 'exporting'
    DESTROYING = 'destroying'
    DESTROYED = 'destroyed'


class TerraformAction(StrEnum):
    APPLY = 'apply'
    CLOSE_INGEST = 'close_ingest'
    DESTROY = 'destroy'


class TerraformStatus(StrEnum):
    IN_FLIGHT = 'in_flight'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


class TerraformRun(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True)
    run_token: str = Field(alias='runToken')
    stamped_at: datetime = Field(alias='stampedAt')
    build_id: str = Field('', alias='buildId')
    action: TerraformAction
    status: TerraformStatus = TerraformStatus.IN_FLIGHT

    @property
    def locked(self) -> bool:
        return self.status == TerraformStatus.IN_FLIGHT


class Race(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True)
    race_id: str = Field(alias='raceId')
    phase: Phase
    started_by: str = Field(alias='startedBy')
    started_at: datetime = Field(alias='startedAt')
    terraform_run: TerraformRun | None = Field(None, alias='terraformRun')
    kafka_instance: str | None = Field(None, alias='kafkaInstance')
    core_instance: str | None = Field(None, alias='coreInstance')
    kafka_public_ip: str | None = Field(None, alias='kafkaPublicIp')
    kafka_hostname: str | None = Field(None, alias='kafkaHostname')
    kafka_port: int | None = Field(None, alias='kafkaPort')
    telemetry_url: str | None = Field(None, alias='telemetryUrl')
    applied_at: datetime | None = Field(None, alias='appliedAt')
    stopped_at: datetime | None = Field(None, alias='stoppedAt')
    destroyed_at: datetime | None = Field(None, alias='destroyedAt')
    last_error: str | None = Field(None, alias='lastError')


class ActivePointer(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    race_id: str = Field('', alias='raceId')
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), alias='updatedAt')


class BuildFinished(BaseModel):
    race_id: str
    action: TerraformAction
    run_token: str
    build_id: str
    success: bool
    error: str | None = None

    @classmethod
    def from_cloud_build(cls, payload: dict[str, Any]) -> 'BuildFinished':
        """Adapt the Cloud Build notification shape to the Relay event contract."""
        substitutions = payload.get('substitutions') or {}
        if not isinstance(substitutions, dict):
            raise ValueError('invalid Cloud Build substitutions')

        required_substitutions = ('_RACE_ID', '_ACTION', '_RUN_TOKEN')
        if any(not isinstance(substitutions.get(key), str) or not substitutions[key]
               for key in required_substitutions):
            raise ValueError('Cloud Build substitutions are incomplete')

        status = payload.get('status')
        terminal_statuses = {'SUCCESS', 'FAILURE', 'INTERNAL_ERROR', 'TIMEOUT', 'CANCELLED', 'EXPIRED'}
        if status is not None and (not isinstance(status, str) or status not in terminal_statuses):
            raise ValueError('Cloud Build event is not terminal')
        if status is None and not isinstance(payload.get('success'), bool):
            raise ValueError('Cloud Build event has no terminal status')

        failure_info = payload.get('failureInfo') or payload.get('failure_info') or {}
        error = payload.get('error')
        if error is None and isinstance(failure_info, dict):
            error = failure_info.get('detail')

        return cls.model_validate({
            'race_id': substitutions['_RACE_ID'],
            'action': substitutions['_ACTION'],
            'run_token': substitutions['_RUN_TOKEN'],
            'build_id': payload.get('id') or payload.get('build_id') or payload.get('buildId'),
            'success': status == 'SUCCESS' if status is not None else payload.get('success'),
            'error': error,
        })


class ExportDone(BaseModel):
    race_id: str
