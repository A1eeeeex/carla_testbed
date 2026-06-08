from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .runtime_context import RuntimeContext


@dataclass(frozen=True)
class LegacyDispatchResult:
    status: str
    exit_code: int
    command: list[str] = field(default_factory=list)
    stdout: str = ""
    stderr: str = ""
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "command": list(self.command),
            "stdout": self.stdout,
            "stderr": self.stderr,
            "warnings": list(self.warnings),
        }


def dispatch_legacy_launch_plan(
    context: RuntimeContext,
    launch_plan: Mapping[str, Any],
    *,
    allow_online_env: str = "CARLA_TESTBED_ALLOW_LEGACY_DISPATCH",
) -> LegacyDispatchResult:
    commands = launch_plan.get("commands") if isinstance(launch_plan.get("commands"), list) else []
    command = [str(part) for part in commands[0]] if commands else []
    if context.dry_run:
        return LegacyDispatchResult(
            status="dry_run",
            exit_code=0,
            command=command,
            warnings=["dry-run: legacy command was not executed"],
        )
    if not context.legacy_dispatch:
        return LegacyDispatchResult(
            status="not_dispatched",
            exit_code=2,
            command=command,
            warnings=["legacy_dispatch flag is false"],
        )
    if os.environ.get(allow_online_env) != "1":
        return LegacyDispatchResult(
            status="blocked",
            exit_code=2,
            command=command,
            warnings=[
                f"set {allow_online_env}=1 to execute legacy runtime command",
                "safety block prevents accidental CARLA/Apollo/Autoware startup",
            ],
        )
    if not command:
        return LegacyDispatchResult(
            status="missing_command",
            exit_code=2,
            warnings=["launch plan has no command"],
        )
    completed = subprocess.run(
        command,
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, **{str(k): str(v) for k, v in dict(launch_plan.get("env") or {}).items()}},
    )
    return LegacyDispatchResult(
        status="completed" if completed.returncode == 0 else "failed",
        exit_code=int(completed.returncode),
        command=command,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
