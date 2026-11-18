import analysis.shared.config as config_module


def _isolate_config_environment(monkeypatch):
    for name in (
        *config_module.REQUIRED_ENV,
        "ANALYSIS_INGEST_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config_module, "load_dotenv", lambda _path: None)
    monkeypatch.setattr(config_module, "find_dotenv", lambda: "")


def test_load_config_returns_none_when_required_environment_is_missing(monkeypatch):
    _isolate_config_environment(monkeypatch)
    monkeypatch.setenv("CORE_TELEMETRY_URL", "core:50051")

    assert config_module.load_config() is None


def test_load_config_normalizes_telemetry_url_and_applies_defaults(monkeypatch):
    _isolate_config_environment(monkeypatch)
    monkeypatch.setenv("CORE_TELEMETRY_URL", "core:50051")
    monkeypatch.setenv("TELEMETRY_URL", "telemetry.internal:8080")

    config = config_module.load_config()

    assert config is not None
    assert config.CORE_TELEMETRY_URL == "core:50051"
    assert config.TELEMETRY_URL == "http://telemetry.internal:8080"
    assert config.CLIENT_ID == "analysis_"
    assert config.ANALYSIS_INGEST_TOKEN == "TOKEN"


def test_load_config_preserves_explicit_url_scheme_and_optional_overrides(monkeypatch):
    _isolate_config_environment(monkeypatch)
    monkeypatch.setenv("CORE_TELEMETRY_URL", "core.internal:50051")
    monkeypatch.setenv("TELEMETRY_URL", "https://telemetry.internal")
    monkeypatch.setenv("ANALYSIS_INGEST_TOKEN", "ingest-secret")

    config = config_module.load_config()

    assert config is not None
    assert config.CORE_TELEMETRY_URL == "core.internal:50051"
    assert config.TELEMETRY_URL == "https://telemetry.internal"
    assert config.ANALYSIS_INGEST_TOKEN == "ingest-secret"
