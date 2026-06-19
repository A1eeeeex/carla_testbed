from __future__ import annotations

import subprocess
import sys

from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.backends.replay import ReplayBackend
from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_backend_contracts_are_metadata_only() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="claim",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    contract = backend.contract(plan)
    preflight = backend.preflight(plan)

    assert contract.backend == "apollo_cyberrt"
    assert contract.needs_local_carla is True
    assert contract.needs_local_apollo is True
    assert "/apollo/localization/pose" in contract.required_inputs
    assert preflight.starts_runtime is False
    assert preflight.status == "local_required"


def test_carla_builtin_backend_launches_diagnostic_fixed_scene_runner() -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="builtin/simple_acc_route_follower",
        scenario="baguang/follow_stop_static_300m",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    contract = backend.contract(plan)
    launch = backend.build_launch_plan(plan)

    assert contract.backend == "carla_builtin"
    assert contract.needs_local_carla is True
    assert contract.needs_local_apollo is False
    assert launch.backend == "carla_builtin"
    assert launch.commands[0][:2] == ["python3", "tools/run_builtin_ego_fixed_scene.py"]
    assert "artifacts/ego_control_trace.jsonl" in launch.expected_artifacts
    assert any("diagnostic-only" in warning for warning in launch.warnings)


def test_apollo_fixed_scene_launch_plan_requires_runtime_migration_before_dispatch() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="baguang/cut_in_35kph_left_to_right_10m",
        recording="none",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    launch = default_backend_registry().for_plan(plan).build_launch_plan(plan)

    assert launch.starts_runtime is False
    assert launch.commands == []
    assert launch.compatibility_source == "fixed-scene Apollo runtime migration required"
    assert "artifacts/obstacle_gt_contract.jsonl" in launch.expected_artifacts
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in launch.expected_artifacts
    assert any("fixed-scene runtime is not migrated" in warning for warning in launch.warnings)


def test_autoware_fixed_scene_launch_plan_requires_runtime_migration_before_dispatch() -> None:
    plan = compile_run_plan(
        platform="autoware_ros2",
        algorithm="autoware/universe_gt_localization",
        scenario="baguang/cut_in_35kph_left_to_right_10m",
        recording="none",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    launch = default_backend_registry().for_plan(plan).build_launch_plan(plan)

    assert launch.starts_runtime is False
    assert launch.commands == []
    assert launch.compatibility_source == "fixed-scene Autoware runtime migration required"
    assert "artifacts/obstacle_gt_contract.jsonl" in launch.expected_artifacts
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in launch.expected_artifacts
    assert any("fixed-scene runtime is not migrated" in warning for warning in launch.warnings)


def test_launch_plan_postprocess_commands_use_python3() -> None:
    registry = PlatformRegistry(repo_root=".")
    cases = [
        ("apollo_cyberrt", "apollo/apollo10_carla_gt", "baguang/follow_stop_static_300m_spawn2m"),
        ("autoware_ros2", "autoware/universe_gt_localization", "town01/lane_keep_097"),
        ("carla_builtin", "builtin/simple_acc_route_follower", "baguang/follow_stop_static_300m_spawn2m"),
        ("dummy", "dummy", "town01/lane_keep_097"),
    ]

    for platform, algorithm, scenario in cases:
        plan = compile_run_plan(
            platform=platform,
            algorithm=algorithm,
            scenario=scenario,
            recording="none",
            gate="scenario_validation",
            registry=registry,
        )
        launch = default_backend_registry().for_plan(plan).build_launch_plan(plan)
        assert launch.postprocess_commands, platform
        assert all(command and command[0] == "python3" for command in launch.postprocess_commands), platform

    replay_plan = compile_run_plan(
        platform="dummy",
        algorithm="dummy",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="scenario_validation",
        registry=registry,
    )
    replay_launch = ReplayBackend().build_launch_plan(replay_plan)
    assert replay_launch.postprocess_commands
    assert all(command and command[0] == "python3" for command in replay_launch.postprocess_commands)


def test_cli_lists_backends() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "carla_testbed", "list", "backends"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "apollo_cyberrt" in result.stdout
    assert "autoware_ros2" in result.stdout
    assert "carla_builtin" in result.stdout
