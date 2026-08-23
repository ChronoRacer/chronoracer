from typing import Any

from auth0_fastapi.config import Auth0Config
from auth0_fastapi.server.routes import register_auth_routes
from fastapi import APIRouter, Depends, FastAPI, Request, status
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware

from .auth import Auth0Authenticator, auth0_user, pubsub_push
from .config import RelayConfig, load_config
from relay.app.gcp.repository import FirestoreRaceRepository, InMemoryRaceRepository
from .service import RelayService
from .gcp.storage import DevObjectStore, GCSObjectStore
from .gcp.builds import DevBuildTrigger, CloudBuildTrigger
from .gcp.pubsub import DevShutdownPublisher, KafkaShutdownPublisher


class StartRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


def create_app(config: RelayConfig | None = None, service: RelayService | None = None) -> FastAPI:
    config = config or load_config()
    app = FastAPI(title="ChronoRacer Relay", version="1.0.0")
    app.state.config = config
    auth_config = Auth0Config(**config.auth0.model_dump())
    app.add_middleware(SessionMiddleware, secret_key=auth_config.secret,
                       session_cookie=auth_config.cookie_name,
                       max_age=auth_config.session_expiration, https_only=False)
    app.state.auth0 = Auth0Authenticator(auth_config)
    app.state.auth_client = app.state.auth0.client
    auth_router = APIRouter()
    register_auth_routes(auth_router, auth_config)
    app.include_router(auth_router)
    app.state.pubsub_auth = {}
    if config.gcp.enabled:
        repository = FirestoreRaceRepository(config.gcp.project_id, config.gcp.firestore_database)
        object_store = GCSObjectStore(config.gcp.project_id, config.gcp.export_bucket)
        builds = CloudBuildTrigger(config.gcp.project_id, config.gcp.cloud_build_trigger_id)
        shutdown = KafkaShutdownPublisher(config.gcp.kafka_private_bootstrap, config.gcp.kafka_control_topic)
    else:
        repository, object_store, builds, shutdown = InMemoryRaceRepository(), DevObjectStore(), DevBuildTrigger(), DevShutdownPublisher(config.gcp.kafka_control_topic)
    app.state.service = service or RelayService(repository, object_store, builds, shutdown)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/race/start", status_code=status.HTTP_202_ACCEPTED)
    async def start(request: StartRequest, _: dict = Depends(auth0_user)):
        return await app.state.service.start(request.config)

    @app.get("/race/status")
    async def race_status(_: dict = Depends(auth0_user)):
        return await app.state.service.status()

    @app.post("/race/stop", status_code=status.HTTP_202_ACCEPTED)
    async def stop(_: dict = Depends(auth0_user)):
        return await app.state.service.stop()

    @app.post("/race/force-destroy", status_code=status.HTTP_202_ACCEPTED)
    async def force_destroy(_: dict = Depends(auth0_user)):
        return await app.state.service.force_destroy()

    @app.post("/internal/pubsub/build-finished", status_code=status.HTTP_204_NO_CONTENT)
    async def build_finished(request: Request):
        await pubsub_push(request, config.gcp.build_finished_audience)
        await app.state.service.build_finished(await request.json())

    @app.post("/internal/pubsub/export-done", status_code=status.HTTP_204_NO_CONTENT)
    async def export_done(request: Request):
        await pubsub_push(request, config.gcp.export_done_audience)
        await app.state.service.export_done(await request.json())

    return app


app = create_app()
