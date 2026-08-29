from __future__ import annotations

import json
import subprocess
from controller.terraform_cli import CliTerraform, _output_values
from pathlib import Path


def test_output_values_strip_dot():
    raw = {
        "kafka_instance": {"value": "ki"},
        "core_instance": {"value": "ci"},
        "kafka_public_ip": {"value": "1.1.1.1"},
        "kafka_hostname": {"value": "kafka.example.com."},
    }
    values = _output_values(raw)
    assert values is not None
    assert values.kafka_hostname == "kafka.example.com"
    assert values.kafka_instance == "ki"


def test_output_values_missing_key():
    assert _output_values({"kafka_instance": {"value": "ki"}}) is None


def _script(monkeypatch, responses: list[tuple[int, str, str]]):
    seen: list[list[str]] = []
    queue = list(responses)

    def fake_run(cmd, **kwargs):
        seen.append(list(cmd))
        code, out, err = queue.pop(0)
        return subprocess.CompletedProcess(cmd, code, out, err)

    monkeypatch.setattr("controller.terraform_cli.subprocess.run", fake_run)
    return seen


TF_JSON = json.dumps(
    {
        "kafka_instance": {"value": "ki"},
        "core_instance": {"value": "ci"},
        "kafka_public_ip": {"value": "1.1.1.1"},
        "kafka_hostname": {"value": "kafka.example.com."},
    }
)


def test_apply_runs_init_apply_output(monkeypatch, tmp_path: Path, capsys):
    seen = _script(
        monkeypatch,
        [
            (0, "init-ok\n", ""),
            (0, "apply-ok\n", ""),
            (0, TF_JSON, ""),
        ],
    )
    result = CliTerraform(tmp_path).run(
        "apply",
        {
            "race_id": "r",
            "project_id": "p",
            "kafka_fqdn": "k.",
            "zone": "z",
            "ingest_open": "true",
        },
    )
    assert result.ok
    assert result.outputs is not None
    assert result.outputs.kafka_hostname == "kafka.example.com"
    assert seen[0][1] == "init"
    assert seen[1][1] == "apply"
    assert seen[2][1:] == ["output", "-json"]
    dumped = capsys.readouterr().out
    assert "init-ok" in dumped
    assert "apply-ok" in dumped
    assert TF_JSON in dumped


def test_close_ingest_uses_apply_not_destroy(monkeypatch, tmp_path: Path):
    seen = _script(monkeypatch, [(0, "", ""), (0, "", "")])
    result = CliTerraform(tmp_path).run("close_ingest", {"ingest_open": "false"})
    assert result.ok
    assert seen[1][1] == "apply"
    assert result.outputs is None


def test_destroy_uses_destroy_verb(monkeypatch, tmp_path: Path):
    seen = _script(monkeypatch, [(0, "", ""), (0, "", "")])
    result = CliTerraform(tmp_path).run("destroy", {"ingest_open": "false"})
    assert result.ok
    assert seen[1][1] == "destroy"


def test_init_failure_still_dumps_stdout(monkeypatch, tmp_path: Path, capsys):
    _script(monkeypatch, [(1, "", "init boom\n")])
    result = CliTerraform(tmp_path).run("apply", {})
    assert not result.ok
    assert "init boom" in result.error
    assert "init boom" in capsys.readouterr().out
