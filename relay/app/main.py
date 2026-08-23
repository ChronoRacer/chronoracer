from typing import Any

from fastapi import Depends, FastAPI, Request, status
from pydantic import BaseModel, Field

from .auth import OktaAuthenticator, okta_user, pubsub_push
from .config import RelayConfig, load_config
from .service import RelayService


class StartRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


def create_app(config: RelayConfig | None = None, service: RelayService | None = None) -> FastAPI:
    config = config or load_config()
    app = FastAPI(title="ChronoRacer Relay", version="1.0.0")
    app.state.config = config
    app.state.okta = OktaAuthenticator(config.okta)
    app.state.pubsub_auth = {}
    app.state.service = service or RelayService()

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/race/start", status_code=status.HTTP_202_ACCEPTED)
    async def start(request: StartRequest, _: dict = Depends(okta_user)):
        return await app.state.service.start(request.config)

    @app.get("/race/status")
    async def race_status(_: dict = Depends(okta_user)):
        return await app.state.service.status()

    @app.post("/race/stop", status_code=status.HTTP_202_ACCEPTED)
    async def stop(_: dict = Depends(okta_user)):
        return await app.state.service.stop()

    @app.post("/race/force-destroy", status_code=status.HTTP_202_ACCEPTED)
    async def force_destroy(_: dict = Depends(okta_user)):
        return await app.state.service.force_destroy()

    @app.post("/internal/pubsub/build-finished", status_code=status.HTTP_204_NO_CONTENT)
    async def build_finished(request: Request):
        await pubsub_push(request, config.pubsub.build_finished_audience)
        await app.state.service.build_finished(await request.json())

    @app.post("/internal/pubsub/export-done", status_code=status.HTTP_204_NO_CONTENT)
    async def export_done(request: Request):
        await pubsub_push(request, config.pubsub.export_done_audience)
        await app.state.service.export_done(await request.json())

    return app


app = create_app()
