from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.executor import execute_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_execute_run_plan_dry_run_writes_platform_artifacts(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    result = execute_run_plan(plan, run_dir=tmp_path / "run", dry_run=True)

    assert result.status == "dry_run"
    assert result.exit_code == 0
    assert (tmp_path / "run" / "plan.resolved.yaml").exists()
    manifest = json.loads((tmp_path / "run" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["platform"]["name"] == "apollo_cyberrt"
    assert manifest["compatibility_source"] == "tools/run_town01_capability_online_chain.py"


def test_cli_run_plan_dry_run_uses_executor(tmp_path: Path) -> None:
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
            "--dry-run",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "platform_execution_result.v1" in result.stdout
    assert (run_dir / "platform_execution_result.json").exists()
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["exit_reason"] == "platform_dry_run"


def test_cli_plan_show_launch_outputs_backend_launch_plan(tmp_path: Path) -> None:
    out = tmp_path / "plan.yaml"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "plan",
            "--platform",
            "apollo_cyberrt",
            "--algorithm",
            "apollo/apollo10_carla_gt",
            "--scenario",
            "town01/lane_keep_097",
            "--record",
            "none",
            "--gate",
            "diagnostic",
            "--out",
            str(out),
            "--show-launch",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "launch_plan_preview.v1" in result.stdout
    assert "legacy_apollo_cyberrt_compat" in result.stdout
