from __future__ import annotations

from controller.config import load_config
from controller.names import race_bucket
from controller.race_store import FirestoreRaceStore
from controller.run import run
from controller.terraform_cli import CliTerraform
from controller.yaml_probe import GcsYamlProbe
from loguru import logger


def cli() -> int:
    cfg = load_config()
    if cfg is None:
        logger.error("Invalid env, see logs for details")
        return 1
    if not cfg.names_path.is_file():
        logger.error("names.json missing at {}", cfg.names_path)
        return 1
    if not cfg.terraform_dir.is_dir():
        logger.error("Terraform dir missing at {}", cfg.terraform_dir)
        return 1
    bucket = race_bucket(cfg.names_path)
    store = FirestoreRaceStore(cfg.project_id, cfg.firestore_database)
    probe = GcsYamlProbe(cfg.project_id, bucket)
    terraform = CliTerraform(cfg.terraform_dir)
    return run(cfg, store, probe, terraform)
