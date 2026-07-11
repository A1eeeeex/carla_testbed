from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs


def test_apollo_fixed_scene_scaffold_writes_invalid_backend_not_ready(tmp_path) -> None:
    run_dir = tmp_path / "apollo_fixed_scene"
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_scenario.py",
            "--scenario",
            "configs/scenarios/baguang/follow_stop_static_300m.yaml",
            "--backend",
            "apollo_cyberrt",
            "--run-dir",
            str(run_dir),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    preflight = json.loads((run_dir / "preflight.json").read_text(encoding="utf-8"))
    phase1_status = json.loads(
        (run_dir / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8")
    )

    assert manifest["backend"] == "apollo_cyberrt"
    assert manifest["backend_type"] == "apollo_reference_backend"
    assert manifest["backend_ready"] is False
    assert manifest["starts_runtime"] is False
    assert manifest["fixed_scene_case"] == "baguang_follow_stop_static_300m"
    assert manifest["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert manifest["target_actor_contract"]["status"] == "resolved"
    assert manifest["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert (run_dir / "artifacts" / "fixed_scene_resolved.json").exists()
    assert (run_dir / "analysis" / "fixed_scene_contract" / "fixed_scene_contract_report.json").exists()
    assert (run_dir / "analysis" / "scenario_actor_contract" / "scenario_actor_contract_report.json").exists()
    readiness_path = (
        run_dir
        / "analysis"
        / "phase1_apollo_fixed_scene_readiness"
        / "phase1_apollo_fixed_scene_readiness_report.json"
    )
    dispatch_path = (
        run_dir
        / "analysis"
        / "phase1_apollo_fixed_scene_dispatch"
        / "phase1_apollo_fixed_scene_dispatch_report.json"
    )
    assert readiness_path.exists()
    assert dispatch_path.exists()
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    dispatch = json.loads(dispatch_path.read_text(encoding="utf-8"))
    assert readiness["status"] == "fail"
    assert dispatch["status"] == "pass"
    assert dispatch["dispatch_mode"] == "guarded_legacy_transition_available"
    assert "target_role_not_in_front_obstacle_role_names" in readiness["blocking_reasons"]
    assert preflight["schema_version"] == "apollo_fixed_scene_preflight.v1"
    assert preflight["status"] == "backend_not_ready"
    assert "local CARLA/Apollo preflight is not executed by CI-safe backend facade" in preflight["reasons"]
    assert "apollo_fixed_scene_runtime_not_migrated" not in preflight["reasons"]
    assert preflight["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert preflight["offline_fixed_scene_artifacts"]["status"] == "static_only"
    assert preflight["offline_fixed_scene_artifacts"]["apollo_fixed_scene_readiness"]["status"] == "fail"
    assert preflight["offline_fixed_scene_artifacts"]["apollo_fixed_scene_dispatch"]["status"] == "pass"
    assert preflight["apollo_fixed_scene_dispatch_contract"]["status"] == "pass"
    assert (
        preflight["apollo_fixed_scene_dispatch_contract"]["dispatch_mode"]
        == "guarded_legacy_transition_available"
    )
    assert preflight["offline_fixed_scene_artifacts"]["fixed_scene_contract_status"] == "insufficient_data"
    assert preflight["offline_fixed_scene_artifacts"]["scenario_actor_contract_status"] == "insufficient_data"
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in preflight["expected_artifacts"]
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in preflight["missing_expected_artifacts"]
    assert "artifacts/fixed_scene_resolved.json" not in preflight["missing_expected_artifacts"]
    assert "analysis/fixed_scene_contract/fixed_scene_contract_report.json" not in preflight["missing_expected_artifacts"]
    assert "analysis/scenario_actor_contract/scenario_actor_contract_report.json" not in preflight[
        "missing_expected_artifacts"
    ]
    assert phase1_status["status"] == "invalid"
    assert phase1_status["failure_reason"] == "backend_not_ready"
    assert phase1_status["evaluable"] is False
    assert phase1_status["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert phase1_status["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert phase1_status["apollo_fixed_scene_dispatch_contract"]["status"] == "pass"
    assert (
        phase1_status["apollo_fixed_scene_dispatch_contract"]["dispatch_mode"]
        == "guarded_legacy_transition_available"
    )
    assert phase1_status["offline_fixed_scene_artifacts"]["apollo_fixed_scene_dispatch"].endswith(
        "analysis/phase1_apollo_fixed_scene_dispatch/phase1_apollo_fixed_scene_dispatch_report.json"
    )
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in phase1_status["expected_artifacts"]
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in phase1_status["missing_expected_artifacts"]
    assert "artifacts/fixed_scene_resolved.json" not in phase1_status["missing_expected_artifacts"]
    assert "analysis/fixed_scene_contract/fixed_scene_contract_report.json" not in phase1_status[
        "missing_expected_artifacts"
    ]
    assert "analysis/scenario_actor_contract/scenario_actor_contract_report.json" not in phase1_status[
        "missing_expected_artifacts"
    ]
    assert "manifest.json" not in phase1_status["missing_expected_artifacts"]
    assert "summary.json" not in phase1_status["missing_expected_artifacts"]


def test_apollo_static_follow_stop_launch_plan_exposes_guarded_compat_command() -> None:
    from carla_testbed.backends.registry import default_backend_registry
    from carla_testbed.platform.compiler import compile_run_plan
    from carla_testbed.platform.registry import PlatformRegistry

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="baguang/follow_stop_static_300m",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan)
    hint = backend.legacy_dispatch_hint(plan)

    assert launch.starts_runtime is True
    assert launch.compatibility_source == "phase1_static_follow_stop_legacy_transition"
    assert launch.commands
    command = launch.commands[0]
    assert command[:3] == ["python3", "-m", "carla_testbed"]
    assert (
        "configs/io/examples/phase1_baguang_apollo_followstop_static_control_overlay_paced_compat.yaml"
        in command
    )
    assert "--legacy-dispatch" in command
    assert "artifacts/obstacle_gt_contract.jsonl" in launch.expected_artifacts
    assert "artifacts/scenario_phase_events.jsonl" in launch.expected_artifacts
    assert "artifacts/apollo_control_deferred_start.log" in launch.expected_artifacts
    assert "artifacts/apollo_control_deferred_survival.json" in launch.expected_artifacts
    assert "artifacts/apollo_control_runtime_overlay_manifest.json" in launch.expected_artifacts
    assert "artifacts/apollo_control_runtime_overlay_restore.json" in launch.expected_artifacts
    assert "analysis/apollo_control_handoff/apollo_control_handoff_report.json" in launch.expected_artifacts
    assert (
        "analysis/phase1_apollo_fixed_scene_readiness/phase1_apollo_fixed_scene_readiness_report.json"
        in launch.expected_artifacts
    )
    assert hint["fixed_scene_compatibility"] is True
    assert (
        hint["fixed_scene_compatibility_config"]
        == "configs/io/examples/phase1_baguang_apollo_followstop_static_control_overlay_paced_compat.yaml"
    )
    assert any("diagnostic control-runtime overlay" in warning for warning in launch.warnings)
    assert any("not generic fixed-scene runtime migration" in warning for warning in launch.warnings)


def test_apollo_static_follow_stop_spawn2m_launch_plan_uses_shifted_compat_config() -> None:
    from carla_testbed.backends.registry import default_backend_registry
    from carla_testbed.platform.compiler import compile_run_plan
    from carla_testbed.platform.registry import PlatformRegistry

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="baguang/follow_stop_static_300m_spawn2m",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan)
    hint = backend.legacy_dispatch_hint(plan)

    assert launch.starts_runtime is True
    assert launch.commands
    command = launch.commands[0]
    assert (
        "configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml"
        in command
    )
    assert "configs/io/examples/phase1_baguang_apollo_followstop_static_compat.yaml" not in command
    assert hint["fixed_scene_compatibility"] is True
    assert (
        hint["fixed_scene_compatibility_config"]
        == "configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml"
    )


def test_apollo_town01_route_only_launch_plan_uses_carla_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from carla_testbed.backends.registry import default_backend_registry
    from carla_testbed.platform.compiler import compile_run_plan
    from carla_testbed.platform.registry import PlatformRegistry

    fake_python = tmp_path / "carla16_python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(fake_python.stat().st_mode | 0o111)
    monkeypatch.setenv("CARLA16_PYTHON", str(fake_python))
    monkeypatch.delenv("CARLA_TESTBED_CARLA_PYTHON", raising=False)

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan)

    assert launch.compatibility_source == "tools/run_town01_capability_online_chain.py"
    assert launch.commands[0][0] == str(fake_python)
    assert "--config" in launch.commands[0]
    config_index = launch.commands[0].index("--config")
    assert launch.commands[0][config_index + 1] == "configs/io/examples/town01_apollo_route_health.yaml"
    assert launch.minimum_runtime_timeout_s == 300.0
    assert launch.env["CARLA16_PYTHON"] == str(fake_python)
    assert launch.env["CARLA_TESTBED_CARLA_PYTHON"] == str(fake_python)
    assert any("at least 300s runtime timeout" in warning for warning in launch.warnings)
    assert any(str(fake_python) in warning for warning in launch.warnings)


def test_apollo_town01_behavior_recovery_platform_uses_explicit_diagnostic_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from carla_testbed.backends.registry import default_backend_registry
    from carla_testbed.platform.compiler import compile_run_plan
    from carla_testbed.platform.registry import PlatformRegistry

    fake_python = tmp_path / "carla16_python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(fake_python.stat().st_mode | 0o111)
    monkeypatch.setenv("CARLA16_PYTHON", str(fake_python))
    monkeypatch.delenv("CARLA_TESTBED_CARLA_PYTHON", raising=False)

    plan = compile_run_plan(
        platform="apollo_cyberrt_town01_behavior_recovery",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan)

    assert plan.platform.adapter == "apollo_cyberrt"
    assert plan.platform.params["diagnostic_only"] is True
    assert "--config" in launch.commands[0]
    config_index = launch.commands[0].index("--config")
    assert (
        launch.commands[0][config_index + 1]
        == "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
    assert launch.minimum_runtime_timeout_s == 300.0


def test_apollo_dynamic_fixed_scene_launch_plan_uses_sidecar_runtime_command() -> None:
    from carla_testbed.backends.registry import default_backend_registry
    from carla_testbed.platform.compiler import compile_run_plan
    from carla_testbed.platform.registry import PlatformRegistry

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="baguang/lead_decel_70_to_40_20m",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan)

    assert launch.starts_runtime is True
    assert launch.compatibility_source == "phase1_fixed_scene_runtime_sidecar_transition"
    assert launch.commands
    command = launch.commands[0]
    assert command[:3] == ["python3", "-m", "carla_testbed"]
    assert (
        "configs/io/examples/"
        "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
        in command
    )
    assert "run.scenario_id=baguang_lead_decel_70_to_40_20m" in command
    assert "run.scenario_class=lead_vehicle_decel" in command
    assert "run.route_id=straight_road_for_baguang_mainline_lead_decel_20m" in command
    assert "run.capability_profile=phase1_fixed_scene_sidecar" in command
    assert "scenario.spawn_legacy_front=false" in command
    assert "runtime.fixed_scene_player.enabled=true" in command
    assert "runtime.fixed_scene_player.materialize_ego_initial_speed=true" in command
    assert (
        "runtime.fixed_scene_player.scenario_path=configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml"
        in command
    )
    assert "artifacts/fixed_scene_runtime_hook.json" in launch.expected_artifacts
    assert "artifacts/ego_initial_state_materialization.json" in launch.expected_artifacts
    assert "analysis/v_t_gap/v_t_gap_report.json" in launch.expected_artifacts
    assert "artifacts/apollo_control_route_established_wait.log" not in launch.expected_artifacts
    assert "artifacts/apollo_control_deferred_survival.json" not in launch.expected_artifacts
    assert "artifacts/apollo_control_runtime_overlay_manifest.json" in launch.expected_artifacts
    assert "analysis/apollo_control_handoff/apollo_control_handoff_report.json" in launch.expected_artifacts
    assert any(command[:2] == ["python3", "tools/extract_v_t_gap.py"] for command in launch.postprocess_commands)
    extract_index = next(
        idx for idx, item in enumerate(launch.postprocess_commands) if item[:2] == ["python3", "tools/extract_v_t_gap.py"]
    )
    phase1_postprocess_index = next(
        idx
        for idx, item in enumerate(launch.postprocess_commands)
        if item[:2] == ["python3", "tools/postprocess_phase1_run.py"]
    )
    analyze_index = next(
        idx for idx, item in enumerate(launch.postprocess_commands) if item[:3] == ["python3", "-m", "carla_testbed"]
    )
    link_health_index = next(
        idx
        for idx, item in enumerate(launch.postprocess_commands)
        if item[:2] == ["python3", "tools/analyze_apollo_link_health.py"]
    )
    assert extract_index < phase1_postprocess_index < analyze_index < link_health_index
    assert any("sidecar hook" in warning for warning in launch.warnings)
    assert any("control-runtime overlay" in warning for warning in launch.warnings)


def test_apollo_baguang_cut_out_launch_plan_uses_sidecar_runtime_command() -> None:
    from carla_testbed.backends.registry import default_backend_registry
    from carla_testbed.platform.compiler import compile_run_plan
    from carla_testbed.platform.registry import PlatformRegistry

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="baguang/cut_out_35kph_right_to_left_25m",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan)

    assert launch.starts_runtime is True
    assert launch.compatibility_source == "phase1_fixed_scene_runtime_sidecar_transition"
    assert launch.commands
    command = launch.commands[0]
    assert (
        "configs/io/examples/"
        "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
        in command
    )
    assert "run.scenario_id=baguang_cut_out_35kph_right_to_left_25m" in command
    assert "run.scenario_class=cut_out" in command
    assert "runtime.fixed_scene_player.enabled=true" in command
    assert (
        "runtime.fixed_scene_player.scenario_path=configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml"
        in command
    )


def test_apollo_static_follow_stop_compat_config_defers_control_until_planning_ready() -> None:
    payload = yaml.safe_load(
        Path("configs/io/examples/phase1_baguang_apollo_followstop_static_compat.yaml").read_text(encoding="utf-8")
    )

    docker_cfg = payload["algo"]["apollo"]["docker"]
    assert docker_cfg["defer_control_until_planning_ready"] is True
    assert docker_cfg["control_start_gate"] == "planning_ready"
    assert docker_cfg["deferred_control_start_mode"] == "dag"
    assert docker_cfg["deferred_control_start_async"] is True
    assert docker_cfg["deferred_control_disable_bvar_dump"] is True


def test_apollo_baguang_compat_profile_freezes_verified_lateral_contract() -> None:
    payload = yaml.safe_load(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_compat.yaml"
        ).read_text(encoding="utf-8")
    )

    apollo = payload["algo"]["apollo"]
    assert apollo["control_mapping"]["steer_sign"] == -1.0
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_lqr"]["query_time_nearest_point_only"] is True
    assert apollo["control_lqr"]["enable_look_ahead_back_control"] is False

    dynamic = yaml.safe_load(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
        ).read_text(encoding="utf-8")
    )
    assert (
        dynamic["extends"]
        == "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml"
    )


def test_apollo_dynamic_sidecar_profile_starts_control_after_route_established() -> None:
    payload = yaml.safe_load(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_sidecar_route_established_control_overlay_low_capture_paced_compat.yaml"
        ).read_text(encoding="utf-8")
    )

    docker_cfg = payload["algo"]["apollo"]["docker"]
    assert docker_cfg["defer_control_until_planning_ready"] is False
    assert docker_cfg["control_start_gate"] == "route_established"
    assert docker_cfg["control_planning_ready_require_routing_success"] is True
    assert "dynamic_sidecar_route_established" in payload["run"]["profile_name"]


def test_apollo_dynamic_sidecar_default_profile_gates_first_publish_on_planning() -> None:
    payload = yaml.safe_load(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
        ).read_text(encoding="utf-8")
    )

    docker_cfg = payload["algo"]["apollo"]["docker"]
    assert payload["algo"]["apollo"]["start_after_scenario_spawn"] is True
    assert docker_cfg["defer_control_until_planning_ready"] is False
    assert docker_cfg["control_start_gate"] == "none"
    assert docker_cfg["control_planning_ready_require_routing_success"] is True
    assert payload["algo"]["apollo"]["control_mapping"][
        "require_valid_planning_before_first_publish"
    ] is True
    assert "dynamic_sidecar_eager_control" in payload["run"]["profile_name"]
    runtime = payload["runtime"]["fixed_scene_player"]
    assert runtime["start_gate"] == "ego_speed_ready"
    assert runtime["ego_speed_ready_mps"] == 2.0
    assert runtime["ego_speed_ready_hold_ticks"] == 10
    assert runtime["defer_role_spawn_until_arm"] is True
    assert runtime["reset_ego_pose_on_arm"] is True
    assert runtime["post_reset_ego_speed_ready_mps"] == 18.0
    assert runtime["post_reset_ego_speed_ready_hold_ticks"] == 10
    planning_cfg = payload["algo"]["apollo"]["planning"]
    assert planning_cfg["default_cruise_speed_mps"] == 19.44
    assert planning_cfg["planning_upper_speed_limit_mps"] == 23.61

    runner = Path("examples/run_followstop.py").read_text(encoding="utf-8")
    assert 'apollo_stack_cfg.get("start_after_scenario_spawn")' in runner
    assert "apollo_fixed_scene_armed_before_harness" in runner


def test_apollo_static_spawn2m_profile_uses_fixed_scene_actor_without_legacy_assist() -> None:
    from carla_testbed.config.rig_loader import load_rig_file

    payload = load_rig_file(
        "configs/io/examples/"
        "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml"
    )

    assert payload["scenario"]["spawn_legacy_front"] is False
    runtime = payload["runtime"]["fixed_scene_player"]
    assert runtime["enabled"] is True
    assert runtime["replace_legacy_front"] is True
    assert runtime["require_setup_success"] is True
    assert runtime["scenario_path"].endswith("follow_stop_static_300m_spawn2m.yaml")
    ledger = payload["assist_ledger"]
    assert ledger["active_assists"] == ["apollo_control_runtime_overlay"]
    assert ledger["blocking_assists"] == []
    assert "legacy_followstop" not in ledger["active_assists"]


def test_apollo_dynamic_sidecar_steer_sign_inverted_profile_is_diagnostic_only() -> None:
    payload = yaml.safe_load(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_sidecar_eager_control_steer_sign_inverted_diagnostic.yaml"
        ).read_text(encoding="utf-8")
    )

    assert (
        payload["extends"]
        == "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
    )
    assert "steer_sign_inverted_diagnostic" in payload["run"]["profile_name"]
    assert payload["run"]["capability_profile"] == "phase1_fixed_scene_compatibility_diagnostic_steer_sign_ab"
    assert payload["algo"]["apollo"]["control_mapping"]["steer_sign"] == -1.0
    assert any("compatibility alias" in note for note in payload["assist_ledger"]["notes"])
    assert any("not a natural-driving" in note for note in payload["assist_ledger"]["notes"])


def test_town01_lane_keep_steer_sign_inverted_profile_is_diagnostic_only() -> None:
    payload = yaml.safe_load(
        Path("configs/io/examples/town01_apollo_lane_keep_097_steer_sign_inverted_diagnostic.yaml").read_text(
            encoding="utf-8"
        )
    )

    assert payload["extends"] == "town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    assert "steer_sign_inverted_diagnostic" in payload["run"]["profile_name"]
    assert (
        payload["run"]["capability_profile"]
        == "town01_lane_keep_lateral_semantics_diagnostic_steer_sign_ab"
    )
    assert payload["algo"]["apollo"]["control_mapping"]["steer_sign"] == -1.0
    assert payload["assist_ledger"]["active_assists"] == ["steering_sign_diagnostic_override"]
    assert payload["assist_ledger"]["blocking_assists"] == ["steering_sign_diagnostic_override"]
    assert payload["assist_ledger"]["can_claim_unassisted_natural_driving"] is False
    assert any("diagnostic A/B" in note for note in payload["assist_ledger"]["notes"])
    assert any("not a default backend profile" in note for note in payload["assist_ledger"]["notes"])


def test_apollo_static_follow_stop_default_overlay_profile_declares_runtime_overlay() -> None:
    payload = yaml.safe_load(
        Path("configs/io/examples/phase1_baguang_apollo_followstop_static_control_overlay_paced_compat.yaml").read_text(
            encoding="utf-8"
        )
    )

    docker_cfg = payload["algo"]["apollo"]["docker"]
    assert len(docker_cfg["control_runtime_overlay_source_dirs"]) == 4
    assert payload["run"]["wall_time_pacing"]["enabled"] is True
    assert "apollo_control_runtime_overlay" in payload["assist_ledger"]["active_assists"]
    assert "apollo_control_runtime_overlay" in payload["assist_ledger"]["non_blocking_assists"]
    assert payload["assist_ledger"]["blocking_assists"] == ["legacy_followstop"]


def test_apollo_static_follow_stop_spawn2m_compat_config_declares_shifted_scenario() -> None:
    payload = yaml.safe_load(
        Path("configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_compat.yaml").read_text(
            encoding="utf-8"
        )
    )

    assert payload["run"]["scenario_id"] == "baguang_follow_stop_static_300m_spawn2m"
    assert payload["run"]["route_id"] == "straight_road_for_baguang_mainline_followstop_300m_spawn2m"
    assert payload["scenario"]["ego_pose_offset"]["s_offset_m"] == 2.0
    assert (
        payload["runtime"]["postprocess"]["phase1_scenario_path"]
        == "configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml"
    )
    assert (
        payload["recording"]["artifacts"]["phase1_scenario_path"]
        == "configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml"
    )
    assert payload["assist_ledger"]["can_claim_unassisted_natural_driving"] is False


def test_apollo_static_follow_stop_compat_configs_keep_300m_claim_route_goal() -> None:
    config_paths = [
        Path("configs/io/examples/phase1_baguang_apollo_followstop_static_compat.yaml"),
        Path("configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_compat.yaml"),
    ]

    for config_path in config_paths:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        routing = payload["algo"]["apollo"]["routing"]
        assert routing["scenario_goal_ahead_m"] == 300.0, config_path
        assert routing["scenario_goal_force_beyond_front"] is False, config_path
        assert routing["scenario_goal_min_front_margin_m"] == 0.0, config_path
        assert routing["snap_start_to_lane"] is True, config_path
        assert routing["snap_goal_to_lane"] is True, config_path
        assert routing["goal_validity_check_enabled"] is True, config_path

def test_apollo_fixed_scene_invalid_scaffold_is_comparison_ingestible(tmp_path) -> None:
    apollo = tmp_path / "apollo"
    subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_scenario.py",
            "--scenario",
            "configs/scenarios/baguang/follow_stop_static_300m.yaml",
            "--backend",
            "apollo_cyberrt",
            "--run-dir",
            str(apollo),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    builtin = tmp_path / "builtin"
    (builtin / "analysis" / "phase1_status").mkdir(parents=True)
    (builtin / "analysis" / "v_t_gap").mkdir(parents=True)
    (builtin / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "builtin",
                "scenario_id": "baguang_follow_stop_static_300m",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    (builtin / "summary.json").write_text(json.dumps({"success": True}), encoding="utf-8")
    (builtin / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "run_id": "builtin",
                "scenario_id": "baguang_follow_stop_static_300m",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
                "status": "success",
                "evaluable": True,
            }
        ),
        encoding="utf-8",
    )
    (builtin / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": []}),
        encoding="utf-8",
    )

    report = compare_scenario_runs([apollo, builtin])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "missing_evaluable_apollo_reference_backend"
    assert report["backend_coverage"]["apollo_reference_backend"] == 1
    assert report["backend_coverage"]["evaluable_apollo_reference_backend"] == 0


def test_apollo_fixed_scene_scaffold_accepts_explicit_readiness_bridge_config(tmp_path) -> None:
    run_dir = tmp_path / "apollo_fixed_scene_ready_config"
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_scenario.py",
            "--scenario",
            "configs/scenarios/baguang/follow_stop_static_300m.yaml",
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
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    preflight = json.loads((run_dir / "preflight.json").read_text(encoding="utf-8"))
    readiness = json.loads(
        (
            run_dir
            / "analysis"
            / "phase1_apollo_fixed_scene_readiness"
            / "phase1_apollo_fixed_scene_readiness_report.json"
        ).read_text(encoding="utf-8")
    )

    assert manifest["bridge_config_path"] == "configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml"
    assert preflight["bridge_config_path"] == "configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml"
    assert preflight["status"] == "backend_not_ready"
    assert readiness["status"] == "pass"
    assert readiness["actor_probe_enabled_effective"] is True
    assert readiness["target_role_covered_by_bridge_roles"] is True
    assert readiness["blocking_reasons"] == []
