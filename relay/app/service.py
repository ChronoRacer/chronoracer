from typing import Any


class RelayService:
    """Lifecycle boundary; business and cloud integrations are intentionally deferred."""

    async def start(self, config: dict[str, Any]) -> dict[str, Any]:
        return {"raceId": None, "phase": "starting"}

    async def status(self) -> dict[str, Any]:
        return {"raceId": None, "phase": "destroyed", "ready": False, "unhealthy": False, "billableVms": False}

    async def stop(self) -> dict[str, Any]:
        return {"raceId": None, "phase": "stopping"}

    async def force_destroy(self) -> dict[str, Any]:
        return {"raceId": None, "phase": "destroying"}

    async def build_finished(self, event: dict[str, Any]) -> None:
        return None

    async def export_done(self, event: dict[str, Any]) -> None:
        return None

