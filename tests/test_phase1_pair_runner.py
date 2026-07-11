from __future__ import annotations

import json
import subprocess
import sys
import types
from pathlib import Path

import pytest
import yaml

from carla_testbed.platform.phase1_pair_runner import (
    DEFAULT_PHASE1_APOLLO_PLATFORM,
    run_phase1_pair,
)


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
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["legacy_dispatch"] is False
        assert manifest["backend_contract"]["backend"] in {"carla_builtin", "apollo_cyberrt"}
        assert manifest["claim_boundary"]["schema_version"] == "phase1_claim_boundary.v1"
        assert manifest["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
        if manifest["backend"] == "carla_builtin":
            assert manifest["backend_type"] == "planning_control_backend"
            assert manifest["input_contract"] == "scene_truth_direct"
            assert manifest["output_control_mode"] == "carla_vehicle_control"
        if manifest["backend"] == "apollo_cyberrt":
            assert manifest["backend_type"] == "apollo_reference_backend"
            assert manifest["input_contract"] == "apollo_truth_input_gt_replacement"
            assert manifest["output_control_mode"] == "apollo_control_to_carla_vehicle_control"


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


def test_phase1_pair_runner_applies_apollo_timeout_minimum_only_to_apollo(
    tmp_path: Path,
) -> None:
    result = run_phase1_pair(
        scenario="town01/lane_keep_097",
        out_dir=tmp_path / "pair_timeout",
        pair_id="pair_timeout",
        dry_run=True,
        timeout_s=180.0,
    )

    planning_launch = json.loads((result.run_dirs[0] / "launch_plan.json").read_text(encoding="utf-8"))
    apollo_launch = json.loads((result.run_dirs[1] / "launch_plan.json").read_text(encoding="utf-8"))
    planning_result = json.loads(
        (result.run_dirs[0] / "platform_execution_result.json").read_text(encoding="utf-8")
    )
    apollo_result = json.loads(
        (result.run_dirs[1] / "platform_execution_result.json").read_text(encoding="utf-8")
    )

    assert planning_launch["backend"] == "carla_builtin"
    assert planning_launch["requested_runtime_timeout_s"] == 180.0
    assert planning_launch["effective_runtime_timeout_s"] == 180.0
    assert planning_result["dispatch"]["command"]["timeout_s"] == 180.0

    assert apollo_launch["backend"] == "apollo_cyberrt"
    assert apollo_launch["requested_runtime_timeout_s"] == 180.0
    assert apollo_launch["effective_runtime_timeout_s"] == 300.0
    assert apollo_launch["runtime_timeout_policy"]["policy_applied"] is True
    assert apollo_result["dispatch"]["command"]["timeout_s"] == 300.0


def test_phase1_pair_runner_applies_apollo_probe_overrides_only_to_apollo(
    tmp_path: Path,
) -> None:
    override = "scenario.route_health.ego_offset_y_m=0.5089"
    result = run_phase1_pair(
        scenario="town01/lane_keep_097",
        out_dir=tmp_path / "pair_probe",
        pair_id="pair_probe",
        dry_run=True,
        apollo_overrides=(override,),
    )

    planning_launch = json.loads((result.run_dirs[0] / "launch_plan.json").read_text(encoding="utf-8"))
    apollo_launch = json.loads((result.run_dirs[1] / "launch_plan.json").read_text(encoding="utf-8"))
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    planning_flat = " ".join(" ".join(command) for command in planning_launch["commands"])
    apollo_flat = " ".join(" ".join(command) for command in apollo_launch["commands"])

    assert override not in planning_flat
    assert f"--override {override}" in apollo_flat
    assert manifest["apollo"]["runtime_overrides"] == [override]
    assert apollo_launch["backend"] == "apollo_cyberrt"


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


def test_cli_phase1_run_pair_accepts_apollo_probe_override(tmp_path: Path) -> None:
    out = tmp_path / "pair_cli_probe"
    override = "scenario.route_health.ego_offset_y_m=0.5089"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "phase1",
            "run-pair",
            "--scenario",
            "town01/lane_keep_097",
            "--out",
            str(out),
            "--pair-id",
            "pair_cli_probe",
            "--apollo-override",
            override,
            "--dry-run",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "phase1_pair_run.v1" in completed.stdout
    manifest = json.loads((out / "phase1_pair_manifest.json").read_text(encoding="utf-8"))
    assert manifest["apollo"]["runtime_overrides"] == [override]


def test_phase1_pair_runner_start_carla_dry_run_records_session_without_starting(tmp_path: Path) -> None:
    result = run_phase1_pair(
        scenario="baguang/follow_stop_static_300m_spawn2m",
        out_dir=tmp_path / "pair_start_carla",
        pair_id="pair_start_carla",
        dry_run=True,
        start_carla=True,
        carla_root="/tmp/nonexistent-cached-for-dry-run",
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["carla_session"]["requested"] is True
    assert manifest["carla_session"]["status"] == "dry_run_not_started"
    assert manifest["carla_session"]["town"] == "straight_road_for_baguang"
    session_path = Path(manifest["carla_session"]["path"])
    assert session_path.exists()
    session_payload = json.loads(session_path.read_text(encoding="utf-8"))
    assert session_payload["status"] == "dry_run_not_started"
    assert result.carla_session["status"] == "dry_run_not_started"


def test_cli_phase1_run_pair_start_carla_dry_run(tmp_path: Path) -> None:
    out = tmp_path / "pair_cli_start_carla"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "phase1",
            "run-pair",
            "--scenario",
            "baguang/follow_stop_static_300m_spawn2m",
            "--out",
            str(out),
            "--pair-id",
            "pair_cli_start_carla",
            "--dry-run",
            "--start-carla",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "dry_run_not_started" in completed.stdout
    manifest = json.loads((out / "phase1_pair_manifest.json").read_text(encoding="utf-8"))
    assert manifest["carla_session"]["status"] == "dry_run_not_started"


def test_phase1_pair_runner_start_carla_failure_materializes_invalid_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    carla_root = tmp_path / "CARLA_0.9.16"
    carla_root.mkdir()

    class FakeLauncher:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def start(self) -> None:
            return None

        def wait_ready(self, *, timeout_s: float, poll_s: float) -> bool:
            return False

        def diagnostics_snapshot(self, *, probe_rpc: bool) -> dict[str, object]:
            return {
                "probe_rpc": probe_rpc,
                "rpc_handshake_ready": False,
                "process_alive": True,
            }

        def stop(self) -> None:
            return None

    tbio_mod = types.ModuleType("tbio")
    carla_mod = types.ModuleType("tbio.carla")
    launcher_mod = types.ModuleType("tbio.carla.launcher")
    launcher_mod.CarlaLauncher = FakeLauncher
    monkeypatch.setitem(sys.modules, "tbio", tbio_mod)
    monkeypatch.setitem(sys.modules, "tbio.carla", carla_mod)
    monkeypatch.setitem(sys.modules, "tbio.carla.launcher", launcher_mod)

    result = run_phase1_pair(
        scenario="baguang/follow_stop_static_300m_spawn2m",
        out_dir=tmp_path / "pair_startup_failure",
        pair_id="pair_startup_failure",
        dry_run=False,
        start_carla=True,
        carla_root=carla_root,
        carla_timeout_s=0.01,
    )

    assert result.startup_error
    assert [item.status for item in result.execution_results] == [
        "blocked_by_carla_startup",
        "blocked_by_carla_startup",
    ]
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["startup_error"]
    assert manifest["carla_session"]["status"] == "not_ready"
    assert manifest["carla_session"]["stop"]["status"] == "stopped_after_startup_failure"
    comparison = json.loads(Path(manifest["comparison_outputs"]["summary"]).read_text(encoding="utf-8"))
    assert comparison["comparison_status"] == "invalid"
    assert comparison["reason"] == "all_runs_invalid"
    for run_dir in result.run_dirs:
        phase1_status = json.loads(
            (run_dir / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8")
        )
        assert phase1_status["status"] == "invalid"
        assert phase1_status["failure_reason"] == "backend_not_ready"
        platform_result = json.loads((run_dir / "platform_execution_result.json").read_text(encoding="utf-8"))
        assert platform_result["status"] == "blocked_by_carla_startup"


def test_phase1_pair_defaults_to_reference_runtime_platform(tmp_path: Path) -> None:
    result = run_phase1_pair(
        scenario="town01/lane_keep_097",
        out_dir=tmp_path / "pair",
        dry_run=True,
    )

    apollo_plan = yaml.safe_load(result.plan_paths[1].read_text(encoding="utf-8"))
    assert apollo_plan["platform"]["name"] == DEFAULT_PHASE1_APOLLO_PLATFORM
    assert apollo_plan["platform"]["params"]["town01_route_health_config"].endswith(
        "town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
