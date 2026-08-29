from __future__ import annotations

from controller.outputs import TerraformOutputs
from google.cloud import firestore
from loguru import logger
from typing import Protocol

LAST_ERROR_MAX = 2048


def truncate_error(message: str) -> str:
    text = message.strip() or "unknown error"
    if len(text) <= LAST_ERROR_MAX:
        return text
    return text[: LAST_ERROR_MAX - 3] + "..."


class RaceStore(Protocol):
    def get(self, race_id: str) -> dict | None: ...

    def try_write_applied(
            self, race_id: str, run_token: str, outputs: TerraformOutputs
    ) -> bool: ...

    def try_write_failed(self, race_id: str, run_token: str, last_error: str) -> bool: ...


class FirestoreRaceStore:
    def __init__(self, project_id: str, database: str = "(default)"):
        self._db = firestore.Client(project=project_id, database=database)

    def _doc(self, race_id: str):
        return self._db.collection("races").document(race_id)

    def get(self, race_id: str) -> dict | None:
        snap = self._doc(race_id).get()
        return snap.to_dict() if snap.exists else None

    def try_write_applied(
            self, race_id: str, run_token: str, outputs: TerraformOutputs
    ) -> bool:
        return self._txn_update(
            race_id,
            run_token,
            {
                "phase": "applied",
                "appliedAt": firestore.SERVER_TIMESTAMP,
                "kafkaInstance": outputs.kafka_instance,
                "coreInstance": outputs.core_instance,
                "kafkaPublicIp": outputs.kafka_public_ip,
                "kafkaHostname": outputs.kafka_hostname,
                "lastError": firestore.DELETE_FIELD,
            },
        )

    def try_write_failed(self, race_id: str, run_token: str, last_error: str) -> bool:
        return self._txn_update(
            race_id,
            run_token,
            {"phase": "failed", "lastError": truncate_error(last_error)},
        )

    def _txn_update(self, race_id: str, run_token: str, payload: dict) -> bool:
        doc_ref = self._doc(race_id)
        transaction = self._db.transaction()

        @firestore.transactional
        def _update(txn: firestore.Transaction) -> bool:
            snap = doc_ref.get(transaction=txn)
            if not snap.exists:
                logger.warning("Race {} gone during Firestore write", race_id)
                return False
            data = snap.to_dict() or {}
            run = data.get("terraformRun") or {}
            if data.get("phase") != "starting" or run.get("runToken") != run_token:
                logger.warning(
                    "Skip Firestore write for {}: phase={} token_match={}",
                    race_id,
                    data.get("phase"),
                    run.get("runToken") == run_token,
                )
                return False
            txn.update(doc_ref, payload)
            return True

        wrote = _update(transaction)
        if wrote:
            logger.info("Wrote phase {} for race {}", payload["phase"], race_id)
        return wrote
