from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml

from carla_testbed.platform.phase1_pair_runner import run_phase1_pair


def test_phase1_pair_runner_dry_run_writes_plans_runs_and_comparison(tmp_path: Path) -> None:
    result = run_phase1_pair(
        scenario="baguang/follow_stop_static_300m",
        out_dir=tmp_path / "pair",
        pair_id="pair_static",
        dry_run=True,
    )

    assert result.pair_id == "pair_static"
    assert len(result.plan_paths) == 2
    assert len(result.run_dirs) == 2
    assert all(path.exists() for path in result.plan_paths)
    assert all((run_dir / "platform_execution_result.json").exists() for run_dir in result.run_dirs)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "phase1_pair_run.v1"
    assert manifest["dry_run"] is True
    assert "comparison_outputs" in manifest
    assert Path(manifest["comparison_outputs"]["summary"]).exists()
    for run_dir in result.run_dirs:
        launch = json.loads((run_dir / "launch_plan.json").read_text(encoding="utf-8"))
        flattened = " ".join(" ".join(command) for command in launch["commands"])
        assert str(run_dir) in flattened


def test_phase1_pair_runner_dry_run_supports_baguang_spawn2m_mitigation(
    tmp_path: Path,
) -> None:
    result = run_phase1_pair(
        scenario="baguang/follow_stop_static_300m_spawn2m",
        out_dir=tmp_path / "pair_spawn2m",
        pair_id="pair_spawn2m",
        dry_run=True,
    )

    assert result.pair_id == "pair_spawn2m"
    assert len(result.plan_paths) == 2
    for plan_path in result.plan_paths:
        plan = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
        scenario = plan["scenario"]
        assert scenario["scenario_id"] == "baguang_follow_stop_static_300m_spawn2m"
        assert scenario["route_id"] == "straight_road_for_baguang_mainline_followstop_300m_spawn2m"
        assert scenario["fixed_scene"]["roles"]["ego"]["spawn"]["s_offset_m"] == 2.0
        assert (
            scenario["fixed_scene"]["roles"]["ego"]["spawn"]["reason"]
            == "avoid_baguang_road_start_lane_invasion_sensor_artifact"
        )

    apollo_run = tmp_path / "pair_spawn2m" / "runs" / "pair_spawn2m__apollo"
    launch = json.loads((apollo_run / "launch_plan.json").read_text(encoding="utf-8"))
    flattened = " ".join(" ".join(command) for command in launch["commands"])
    assert "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml" in flattened


def test_cli_phase1_run_pair_dry_run(tmp_path: Path) -> None:
    out = tmp_path / "pair_cli"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "phase1",
            "run-pair",
            "--scenario",
            "baguang/follow_stop_static_300m",
            "--out",
            str(out),
            "--pair-id",
            "pair_cli",
            "--dry-run",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "phase1_pair_run.v1" in completed.stdout
    assert (out / "phase1_pair_manifest.json").exists()
    assert (out / "comparison" / "comparison_summary.json").exists()
