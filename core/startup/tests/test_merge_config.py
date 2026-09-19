from pathlib import Path


from merge_config import deep_merge, merge_files


def test_deep_merge_nested_and_priority():
    defaults = {
        "race": {"race_id": "def", "initial_driver_id": "d"},
        "gcp": {"enabled": False, "max_retries": 2},
    }
    operator = {"race": {"initial_driver_id": "op"}, "gcp": {"max_retries": 9}}
    platform = {"race": {"race_id": "from-meta"}, "gcp": {"enabled": True}}
    merged = deep_merge(deep_merge(defaults, operator), platform)
    assert merged["race"]["race_id"] == "from-meta"
    assert merged["race"]["initial_driver_id"] == "op"
    assert merged["gcp"]["enabled"] is True
    assert merged["gcp"]["max_retries"] == 9


def test_platform_persistence_token_wins():
    defaults = {"persistence": {"db_password": "default", "db_name": "chrono_db"}}
    operator = {"persistence": {"db_password": "operator-secret"}}
    platform = {"persistence": {"db_password": "boot-token"}}
    merged = deep_merge(deep_merge(defaults, operator), platform)
    assert merged["persistence"]["db_password"] == "boot-token"
    assert merged["persistence"]["db_name"] == "chrono_db"


def test_platform_influx_endpoint_wins():
    defaults = {
        "persistence": {
            "db_port": 9999,
            "db_name": "dev_db",
            "fast_flush_interval_ms": 2000,
        }
    }
    operator = {"persistence": {"db_name": "operator_db"}}
    platform = {"persistence": {"db_port": 8181, "db_name": "chrono_db"}}
    merged = deep_merge(deep_merge(defaults, operator), platform)
    assert merged["persistence"]["db_port"] == 8181
    assert merged["persistence"]["db_name"] == "chrono_db"
    assert merged["persistence"]["fast_flush_interval_ms"] == 2000


def test_merge_files(tmp_path: Path):
    defaults = tmp_path / "d.yaml"
    operator = tmp_path / "o.yaml"
    platform = tmp_path / "p.yaml"
    defaults.write_text(
        "race:\n  race_id: def\n  initial_driver_id: d\nkafka:\n  brokers: [localhost:9092]\n"
    )
    operator.write_text("race:\n  initial_driver_id: driver-1\n")
    platform.write_text(
        "race:\n  race_id: race1\nkafka:\n  brokers: [kafka.zone.c.proj.internal:9092]\n"
    )
    merged = merge_files(defaults, operator, platform)
    assert merged["race"]["race_id"] == "race1"
    assert merged["race"]["initial_driver_id"] == "driver-1"
    assert merged["kafka"]["brokers"] == ["kafka.zone.c.proj.internal:9092"]
