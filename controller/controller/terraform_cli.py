from __future__ import annotations

import json
import subprocess
import sys
from controller.outputs import TerraformOutputs
from dataclasses import dataclass
from loguru import logger
from pathlib import Path
from typing import Protocol

LOCK_TIMEOUT = "5m"


@dataclass(frozen=True)
class TerraformResult:
    ok: bool
    error: str = ""
    outputs: TerraformOutputs | None = None


class TerraformRunner(Protocol):
    def run(self, action: str, vars_: dict[str, str]) -> TerraformResult: ...


def _var_args(vars_: dict[str, str]) -> list[str]:
    return [f"-var={key}={value}" for key, value in vars_.items()]


def _tail_error(stderr: str, stdout: str) -> str:
    text = (stderr or stdout).strip()
    if not text:
        return "terraform failed"
    lines = text.splitlines()
    return "\n".join(lines[-20:])


def _output_values(raw: dict) -> TerraformOutputs | None:
    try:
        hostname = str(raw["kafka_hostname"]["value"]).removesuffix(".")
        return TerraformOutputs(
            kafka_instance=str(raw["kafka_instance"]["value"]),
            core_instance=str(raw["core_instance"]["value"]),
            kafka_public_ip=str(raw["kafka_public_ip"]["value"]),
            kafka_hostname=hostname,
        )
    except (KeyError, TypeError):
        return None


def _format_proc(cmd: list[str], proc: subprocess.CompletedProcess[str]) -> str:
    body = "".join(part for part in (proc.stdout, proc.stderr) if part)
    return f"$ {' '.join(cmd)}\n{body}".rstrip()


class CliTerraform:
    def __init__(self, workdir: Path, terraform_bin: str = "terraform"):
        self.workdir = workdir
        self.terraform_bin = terraform_bin

    def _exec(self, args: list[str]) -> tuple[list[str], subprocess.CompletedProcess[str]]:
        cmd = [self.terraform_bin, *args]
        logger.info("{}", " ".join(cmd))
        proc = subprocess.run(
            cmd,
            cwd=self.workdir,
            check=False,
            text=True,
            capture_output=True,
        )
        return cmd, proc

    def run(self, action: str, vars_: dict[str, str]) -> TerraformResult:
        chunks: list[str] = []
        try:
            cmd, init = self._exec(["init", "-input=false", f"-lock-timeout={LOCK_TIMEOUT}"])
            chunks.append(_format_proc(cmd, init))
            if init.returncode != 0:
                return TerraformResult(ok=False, error=_tail_error(init.stderr, init.stdout))

            verb = "destroy" if action == "destroy" else "apply"
            cmd, op = self._exec(
                [
                    verb,
                    "-input=false",
                    "-auto-approve",
                    f"-lock-timeout={LOCK_TIMEOUT}",
                    *_var_args(vars_),
                ]
            )
            chunks.append(_format_proc(cmd, op))
            if op.returncode != 0:
                return TerraformResult(ok=False, error=_tail_error(op.stderr, op.stdout))
            if action != "apply":
                return TerraformResult(ok=True)

            cmd, out = self._exec(["output", "-json"])
            chunks.append(_format_proc(cmd, out))
            if out.returncode != 0:
                return TerraformResult(ok=False, error="terraform outputs unreadable")
            try:
                parsed = json.loads(out.stdout)
            except json.JSONDecodeError:
                return TerraformResult(ok=False, error="terraform outputs unreadable")
            values = _output_values(parsed)
            if values is None:
                return TerraformResult(ok=False, error="terraform outputs unreadable")
            return TerraformResult(ok=True, outputs=values)
        finally:
            if chunks:
                sys.stdout.write("\n".join(chunks) + "\n")
                sys.stdout.flush()
