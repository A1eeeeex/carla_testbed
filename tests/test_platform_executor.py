from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.executor import _phase1_status_needs_refresh, execute_run_plan
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


def test_stale_missing_required_phase1_status_is_refreshed_after_nested_artifacts(tmp_path: Path) -> None:
    run = tmp_path / "run"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "lane_keep",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"run_id": "lane_keep", "success": True}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    (run / "analysis" / "v_t_gap").mkdir(parents=True)
    (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "not_applicable", "warnings": ["lane_keep"]}),
        encoding="utf-8",
    )
    status_path = run / "analysis" / "phase1_status" / "phase1_status.json"
    status_path.write_text(
        json.dumps(
            {
                "schema_version": "phase1_status.v1",
                "status": "invalid",
                "failure_reason": "missing_required_artifact",
            }
        ),
        encoding="utf-8",
    )

    assert _phase1_status_needs_refresh(status_path, artifact_normalization=None) is True
