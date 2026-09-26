from controller.config import load_config


def test_load_config_rejects_unknown_action():
    env = {
        "RACE_ID": "r",
        "ACTION": "nope",
        "RUN_TOKEN": "t",
        "PROJECT_ID": "p",
        "REGION": "europe-west1",
        "KAFKA_FQDN": "ingest.chronoracer.live.",
        "ZONE": "europe-west1-b",
        "TELEMETRY_IMAGE": "europe-west1-docker.pkg.dev/p/chronoracer/telemetry:latest",
    }
    assert load_config(env) is None


def test_load_config_ok():
    env = {
        "RACE_ID": "r",
        "ACTION": "apply",
        "RUN_TOKEN": "t",
        "PROJECT_ID": "p",
        "REGION": "europe-west1",
        "KAFKA_FQDN": "ingest.chronoracer.live.",
        "ZONE": "europe-west1-b",
        "TELEMETRY_IMAGE": "europe-west1-docker.pkg.dev/p/chronoracer/telemetry:latest",
    }
    cfg = load_config(env)
    assert cfg is not None
    assert cfg.action == "apply"
