from __future__ import annotations

from loguru import logger

from controller.config import Config
from controller.race_store import RaceStore
from controller.terraform_cli import TerraformResult, TerraformRunner
from controller.yaml_probe import YamlProbe

YAML_MISSING = "core-config.yaml missing"


def bind_ok(doc: dict, run_token: str, action: str) -> bool:
    run = doc.get("terraformRun") or {}
    return (
        run.get("status") == "in_flight"
        and run.get("runToken") == run_token
        and run.get("action") == action
    )


def terraform_vars(cfg: Config) -> dict[str, str]:
    ingest_open = "true" if cfg.action == "apply" else "false"
    return {
        "race_id": cfg.race_id,
        "project_id": cfg.project_id,
        "kafka_fqdn": cfg.kafka_fqdn,
        "zone": cfg.zone,
        "ingest_open": ingest_open,
    }


def _finish_apply(cfg: Config, store: RaceStore, result: TerraformResult) -> int:
    if result.ok and result.outputs is not None:
        store.try_write_applied(cfg.race_id, cfg.run_token, result.outputs)
        return 0
    error = result.error or "terraform outputs unreadable"
    logger.error("Apply failed: {}", error)
    store.try_write_failed(cfg.race_id, cfg.run_token, error)
    return 1


def run(
    cfg: Config,
    store: RaceStore,
    yaml_probe: YamlProbe,
    terraform: TerraformRunner,
) -> int:
    doc = store.get(cfg.race_id)
    if doc is None:
        logger.error("Race doc {} missing", cfg.race_id)
        return 1
    if not bind_ok(doc, cfg.run_token, cfg.action):
        logger.info("Bind no-op for race {} action {}", cfg.race_id, cfg.action)
        return 0

    if cfg.action == "apply" and not yaml_probe.core_config_exists(cfg.race_id):
        result = TerraformResult(ok=False, error=YAML_MISSING)
    else:
        result = terraform.run(cfg.action, terraform_vars(cfg))

    if cfg.action != "apply":
        if result.ok:
            logger.info("Terraform {} succeeded", cfg.action)
            return 0
        logger.error("Terraform {} failed: {}", cfg.action, result.error)
        return 1

    return _finish_apply(cfg, store, result)
