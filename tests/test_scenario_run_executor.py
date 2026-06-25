from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.executor import execute_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_execute_run_plan_without_runtime_command_completes_offline_backend(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="dummy",
        algorithm="dummy",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="smoke",
        registry=PlatformRegistry(repo_root="."),
    )

    result = execute_run_plan(plan, run_dir=tmp_path / "run", dry_run=False)

    assert result.status == "completed"
    assert result.exit_code == 0
    assert result.dispatch["command"]["warnings"] == [
        "launch plan has no runtime command because backend is offline"
    ]
    assert result.preflight["backend"] == "dummy"
    assert result.backend_contract["backend"] == "dummy"
    assert (tmp_path / "run" / "preflight.json").is_file()
    manifest = json.loads((tmp_path / "run" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["backend_contract"]["backend"] == "dummy"
    assert manifest["claim_boundary"]["schema_version"] == "phase1_claim_boundary.v1"
    summary = json.loads((tmp_path / "run" / "summary.json").read_text(encoding="utf-8"))
    assert summary["platform_runtime_status"] == "completed"
    assert summary["preflight"]["backend"] == "dummy"


def test_cli_run_plan_default_uses_executor_instead_of_not_implemented(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="dummy",
        algorithm="dummy",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="smoke",
        registry=PlatformRegistry(repo_root="."),
    )
    plan_path = write_run_plan(plan, tmp_path / "plan.yaml")
    run_dir = tmp_path / "run"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "run",
            "--plan",
            str(plan_path),
            "--run-dir",
            str(run_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "runtime dispatch is not implemented" not in result.stderr
    assert "platform_execution_result.v1" in result.stdout
    assert (run_dir / "execution" / "runtime_adapter_result.json").exists()
    payload = json.loads((run_dir / "platform_execution_result.json").read_text(encoding="utf-8"))
    assert payload["preflight"]["backend"] == "dummy"
    assert payload["dispatch"]["cleanup"]["status"] == "not_applicable"
