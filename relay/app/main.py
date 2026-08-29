import base64
import json
from typing import Any

from fastapi import Depends, FastAPI, Request, HTTPException, status, Query
from loguru import logger
from pydantic import BaseModel, Field

from .gcp.repository import FirestoreRaceRepository, InMemoryRaceRepository
from .auth import OktaAuthenticator, okta_user, pubsub_push
from .config import RelayConfig, load_config
from .gcp.builds import DevBuildTrigger, CloudBuildTrigger
from .gcp.pubsub import DevShutdownPublisher, KafkaShutdownPublisher
from .gcp.pubsub import KafkaHeartbeatReader
from .gcp.storage import DevObjectStore, GCSObjectStore
from .models import BuildFinished
from .service import RelayService


class StartRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)
    race_id: str = Field(..., alias='raceId', min_length=1)


def create_app() -> FastAPI:
    config = load_config()
    logger.info("Creating Relay application (GCP enabled: {})", config.gcp.enabled)
    app = FastAPI(title="ChronoRacer Relay", version="1.0.0")
    app.state.config = config
    app.state.okta = OktaAuthenticator(config.okta.issuer, config.okta.audience)
    app.state.pubsub_auth = {}
    if config.gcp.enabled:
        repository = FirestoreRaceRepository(config.gcp.project_id, config.gcp.firestore_database)
        object_store = GCSObjectStore(config.gcp.project_id, config.gcp.export_bucket)
        builds = CloudBuildTrigger(
            config.gcp.project_id, config.gcp.cloud_build_trigger_id, config.gcp.cloud_build_location,
        )
        shutdown = KafkaShutdownPublisher(config.gcp.kafka_private_bootstrap, config.gcp.kafka_control_topic)
        heartbeat = KafkaHeartbeatReader(config.gcp.kafka_private_bootstrap, topic=config.gcp.kafka_heartbeat_topic)
    else:
        repository, object_store, builds, shutdown, heartbeat = InMemoryRaceRepository(), DevObjectStore(), DevBuildTrigger(), DevShutdownPublisher(
            config.gcp.kafka_control_topic), None
    app.state.service = RelayService(
        repository, object_store, builds, shutdown, heartbeat,
        config.service.heartbeat_freshness_seconds,
        config.service.build_bind_window_seconds,
        config.gcp.kafka_public_port,
    )

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/race/start", status_code=status.HTTP_202_ACCEPTED)
    async def start(request: StartRequest, user: dict = Depends(okta_user)):
        logger.info("Received race start request")
        try:
            return await app.state.service.start(request.race_id, request.config, user.get('sub', 'unknown'))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/race/status")
    async def race_status(_: dict = Depends(okta_user)):
        return await app.state.service.status()

    @app.get("/race/history")
    async def history(_: dict = Depends(okta_user)):
        return await app.state.service.history()

    @app.get("/race/history/{race_id}")
    async def historical_race(race_id: str, _: dict = Depends(okta_user),
                              export_race: bool = Query(False)):
        try:
            return await app.state.service.historical_race(race_id, export_race)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/race/stop", status_code=status.HTTP_202_ACCEPTED)
    async def stop(_: dict = Depends(okta_user)):
        logger.info("Received race stop request")
        try:
            return await app.state.service.stop()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/race/force-destroy", status_code=status.HTTP_202_ACCEPTED)
    async def force_destroy(_: dict = Depends(okta_user)):
        logger.warning("Received race force-destroy request")
        try:
            return await app.state.service.force_destroy()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/internal/pubsub/build-finished", status_code=status.HTTP_204_NO_CONTENT)
    async def build_finished(request: Request):
        logger.info("Received build-finished Pub/Sub message")
        await pubsub_push(request, config.gcp.build_finished_audience,
                          config.gcp.build_finished_service_account)
        try:
            payload = await request.json()
            message = payload.get('message')
            if not isinstance(message, dict) or not message.get('data'):
                raise ValueError('invalid Pub/Sub envelope')
            data = json.loads(base64.b64decode(message['data']))
            if not isinstance(data, dict):
                raise ValueError('invalid Cloud Build event')
            await app.state.service.build_finished(BuildFinished.from_cloud_build(data))
        except ValueError as exc:
            logger.info("Acknowledging rejected build-finished event: {}", exc)

    @app.post("/internal/pubsub/export-done", status_code=status.HTTP_204_NO_CONTENT)
    async def export_done(request: Request):
        logger.info("Received export-done Pub/Sub message")
        await pubsub_push(request, config.gcp.export_done_audience,
                          config.gcp.export_done_service_account)
        try:
            payload = await request.json()
            message = payload.get('message')
            if not isinstance(message, dict) or not message.get('data'):
                raise ValueError('invalid Pub/Sub envelope')
            data = json.loads(base64.b64decode(message['data']))
            if not isinstance(data, dict):
                raise ValueError('invalid export-done event')
            await app.state.service.export_done(data)
        except ValueError as exc:
            logger.info("Acknowledging rejected export-done event: {}", exc)

    return app
