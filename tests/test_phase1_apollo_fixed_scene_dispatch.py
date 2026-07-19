from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.experiments.phase1_apollo_fixed_scene_dispatch import (
    analyze_apollo_fixed_scene_dispatch,
)
from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.registry import PlatformRegistry
from carla_testbed.scenario_player.manifest_contract import fixed_scene_manifest_fields_from_template_path


def test_static_follow_stop_dispatch_contract_exposes_guarded_transition() -> None:
    plan = _compile_plan("baguang_follow_stop_static_300m")
    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan).to_dict()
    report = analyze_apollo_fixed_scene_dispatch(
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_id=plan.scenario.scenario_id,
        scenario_class=plan.scenario.scenario_class,
        target_actor_contract=_target_contract(plan),
        launch_plan=launch,
    )

    assert report["status"] == "pass"
    assert report["dispatch_mode"] == "guarded_legacy_transition_available"
    assert report["starts_runtime"] is True
    assert report["commands_present"] is True
    assert report["missing_row_level_expected_artifacts"] == []
    assert "guarded_static_follow_stop_transition_not_generic_fixed_scene_runtime" in report["warnings"]
    assert "not online behavior evidence" in report["claim_boundary"]


def test_dynamic_lead_dispatch_contract_exposes_sidecar_runtime_command_without_overclaim() -> None:
    plan = _compile_plan("baguang_lead_decel_70_to_40_20m")
    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan).to_dict()
    report = analyze_apollo_fixed_scene_dispatch(
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_id=plan.scenario.scenario_id,
        scenario_class=plan.scenario.scenario_class,
        target_actor_contract=_target_contract(plan),
        launch_plan=launch,
    )

    assert report["status"] == "pass"
    assert report["dispatch_mode"] == "runtime_command_available"
    assert report["starts_runtime"] is True
    assert report["commands_present"] is True
    assert report["blocking_reasons"] == []
    assert "dynamic_fixed_scene_sidecar_transition_requires_online_artifacts" in report["warnings"]
    assert launch["commands"]
    command = launch["commands"][0]
    assert (
        "configs/io/examples/"
        "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
        in command
    )
    assert "--legacy-dispatch" in command
    assert "--override" in command
    assert "run.scenario_id=baguang_lead_decel_70_to_40_20m" in command
    assert "run.scenario_class=lead_vehicle_decel" in command
    assert "run.route_id=straight_road_for_baguang_mainline_lead_decel_20m" in command
    assert "run.capability_profile=phase1_fixed_scene_sidecar" in command
    assert "scenario.spawn_legacy_front=false" in command
    assert "runtime.fixed_scene_player.enabled=true" in command
    assert "runtime.fixed_scene_player.materialize_ego_initial_speed=false" in command
    assert "artifacts/apollo_control_route_established_wait.log" not in launch["expected_artifacts"]
    assert "artifacts/apollo_control_deferred_survival.json" not in launch["expected_artifacts"]
    assert (
        "runtime.fixed_scene_player.scenario_path=configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml"
        in command
    )
    assert report["missing_row_level_expected_artifacts"] == []
    assert report["missing_postprocess_expected_artifacts"] == []
    assert "speed_profile_non_ego_actor_control" in report["runtime_migration_requirements"]
    assert "target_speed_phase_transition_events" in report["runtime_migration_requirements"]
    assert "v_t_gap_from_target_actor_trace_and_timeseries" in report["runtime_migration_requirements"]


def test_cut_in_dispatch_contract_lists_lane_change_migration_requirements() -> None:
    plan = _compile_plan("baguang_cut_in_35kph_left_to_right_10m")
    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan).to_dict()
    report = analyze_apollo_fixed_scene_dispatch(
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_id=plan.scenario.scenario_id,
        scenario_class=plan.scenario.scenario_class,
        target_actor_contract=_target_contract(plan),
        launch_plan=launch,
    )

    assert report["status"] == "pass"
    assert report["dispatch_mode"] == "runtime_command_available"
    assert "lane_change_non_ego_actor_playback" in report["runtime_migration_requirements"]
    assert "lateral_offset_and_no_teleport_trace" in report["runtime_migration_requirements"]
    assert "target_actor_active_after_phase:cut_in_lane_change" in report["runtime_migration_requirements"]


def test_scaffold_writes_dispatch_contract_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "apollo_dynamic_scaffold"
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_scenario.py",
            "--scenario",
            "configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml",
            "--backend",
            "apollo_cyberrt",
            "--bridge-config",
            "configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml",
            "--run-dir",
            str(run_dir),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    preflight = json.loads((run_dir / "preflight.json").read_text(encoding="utf-8"))
    phase1_status = json.loads(
        (run_dir / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8")
    )
    dispatch_path = (
        run_dir
        / "analysis"
        / "phase1_apollo_fixed_scene_dispatch"
        / "phase1_apollo_fixed_scene_dispatch_report.json"
    )
    dispatch = json.loads(dispatch_path.read_text(encoding="utf-8"))

    assert preflight["offline_fixed_scene_artifacts"]["apollo_fixed_scene_dispatch"]["status"] == "pass"
    assert preflight["apollo_fixed_scene_dispatch_contract"]["status"] == "pass"
    assert preflight["apollo_fixed_scene_dispatch_contract"]["dispatch_mode"] == "runtime_command_available"
    assert phase1_status["apollo_fixed_scene_dispatch_contract"]["status"] == "pass"
    assert phase1_status["apollo_fixed_scene_dispatch_contract"]["dispatch_mode"] == "runtime_command_available"
    assert phase1_status["apollo_fixed_scene_dispatch_contract"]["blocking_reasons"] == []
    assert dispatch["status"] == "pass"
    assert dispatch["dispatch_mode"] == "runtime_command_available"
    assert dispatch["blocking_reasons"] == []
    assert "dynamic_fixed_scene_sidecar_transition_requires_online_artifacts" in dispatch["warnings"]


def test_dispatch_cli_reports_dynamic_sidecar_runtime_command_without_online_claim(tmp_path: Path) -> None:
    out = tmp_path / "dispatch"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_phase1_apollo_fixed_scene_dispatch.py",
            "--scenario",
            "baguang_lead_decel_70_to_40_20m",
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    report = json.loads((out / "phase1_apollo_fixed_scene_dispatch_report.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert report["status"] == "pass"
    assert report["dispatch_mode"] == "runtime_command_available"
    assert report["blocking_reasons"] == []
    assert "dynamic_fixed_scene_sidecar_transition_requires_online_artifacts" in report["warnings"]
    assert "not online behavior evidence" in report["claim_boundary"]


def _compile_plan(scenario: str):
    return compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario=scenario,
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )


def _target_contract(plan) -> dict[str, object]:
    return fixed_scene_manifest_fields_from_template_path(plan.source_profiles["scenario"])["target_actor_contract"]
