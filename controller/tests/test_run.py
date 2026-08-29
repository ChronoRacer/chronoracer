from __future__ import annotations

from controller.config import Config
from controller.outputs import TerraformOutputs
from controller.run import YAML_MISSING, run
from controller.terraform_cli import TerraformResult
from dataclasses import dataclass, field
from pathlib import Path


def _cfg(**overrides) -> Config:
    base = dict(
        race_id="race-1",
        action="apply",
        run_token="tok",
        project_id="proj",
        kafka_fqdn="kafka.example.com.",
        zone="europe-west3-a",
        terraform_dir=Path("/tmp"),
        names_path=Path("/tmp/names.json"),
        firestore_database="(default)",
    )
    base.update(overrides)
    return Config(**base)


def _doc(status="in_flight", token="tok", action="apply", phase="starting"):
    return {
        "phase": phase,
        "terraformRun": {"status": status, "runToken": token, "action": action},
    }


class FakeStore:
    def __init__(self, doc):
        self.doc = doc
        self.applied = None
        self.failed = None

    def get(self, race_id):
        return self.doc

    def try_write_applied(self, race_id, run_token, outputs):
        self.applied = outputs
        return True

    def try_write_failed(self, race_id, run_token, last_error):
        self.failed = last_error
        return True


class FakeYaml:
    def __init__(self, exists=True):
        self.exists = exists

    def core_config_exists(self, race_id):
        return self.exists


@dataclass
class FakeTf:
    result: TerraformResult
    calls: list = field(default_factory=list)

    def run(self, action, vars_):
        self.calls.append((action, vars_))
        return self.result


OUTPUTS = TerraformOutputs(
    kafka_instance="projects/p/zones/z/instances/kafka",
    core_instance="projects/p/zones/z/instances/core",
    kafka_public_ip="1.2.3.4",
    kafka_hostname="kafka.example.com",
)


def test_missing_doc_nonzero():
    store = FakeStore(None)
    assert run(_cfg(), store, FakeYaml(), FakeTf(TerraformResult(ok=True))) == 1
    assert store.applied is None


def test_bind_mismatch_exit_0():
    store = FakeStore(_doc(token="other"))
    tf = FakeTf(TerraformResult(ok=True, outputs=OUTPUTS))
    assert run(_cfg(), store, FakeYaml(), tf) == 0
    assert tf.calls == []


def test_yaml_missing_writes_failed():
    store = FakeStore(_doc())
    tf = FakeTf(TerraformResult(ok=True, outputs=OUTPUTS))
    assert run(_cfg(), store, FakeYaml(exists=False), tf) == 1
    assert store.failed == YAML_MISSING
    assert tf.calls == []


def test_apply_success_writes_outputs():
    store = FakeStore(_doc())
    tf = FakeTf(TerraformResult(ok=True, outputs=OUTPUTS))
    assert run(_cfg(), store, FakeYaml(), tf) == 0
    assert store.applied == OUTPUTS
    assert tf.calls[0][1]["ingest_open"] == "true"


def test_apply_terraform_fail_writes_failed():
    store = FakeStore(_doc())
    tf = FakeTf(TerraformResult(ok=False, error="boom"))
    assert run(_cfg(), store, FakeYaml(), tf) == 1
    assert store.failed == "boom"


def test_close_ingest_no_firestore():
    store = FakeStore(_doc(action="close_ingest"))
    tf = FakeTf(TerraformResult(ok=True))
    assert run(_cfg(action="close_ingest"), store, FakeYaml(exists=False), tf) == 0
    assert store.applied is None
    assert store.failed is None
    assert tf.calls[0][1]["ingest_open"] == "false"


def test_destroy_fail_nonzero_no_write():
    store = FakeStore(_doc(action="destroy"))
    tf = FakeTf(TerraformResult(ok=False, error="lock"))
    assert run(_cfg(action="destroy"), store, FakeYaml(), tf) == 1
    assert store.failed is None
