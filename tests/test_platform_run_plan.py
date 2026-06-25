from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.plan import RUN_PLAN_SCHEMA_VERSION
from carla_testbed.platform.registry import PlatformRegistry


def test_compile_apollo_demo_run_plan_is_ci_safe(tmp_path: Path) -> None:
    registry = PlatformRegistry(repo_root=".")

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="demo",
        gate="diagnostic",
        registry=registry,
    )

    assert plan.schema_version == RUN_PLAN_SCHEMA_VERSION
    assert plan.platform.name == "apollo_cyberrt"
    assert plan.algorithm.variant_id == "apollo10_carla_gt"
    assert plan.scenario.scenario_id == "town01_lane_keep_097"
    assert plan.truth_input.localization == "carla_gt_vrp"
    assert "apollo_dreamview_capture" in plan.recording.recorders
    assert plan.gate.can_claim_natural_driving is False
    assert plan.compatibility["runtime_dispatched"] is False

    output = write_run_plan(plan, tmp_path / "plan.resolved.yaml")
    payload = yaml.safe_load(output.read_text(encoding="utf-8"))

    assert payload["schema_version"] == RUN_PLAN_SCHEMA_VERSION
    assert payload["source_profiles"]["scenario"].endswith("configs/scenarios/town01/lane_keep_097.yaml")


def test_compile_from_run_request_yaml(tmp_path: Path) -> None:
    request = tmp_path / "request.yaml"
    request.write_text(
        yaml.safe_dump(
            {
                "schema_version": "run_request.v1",
                "run": {"id": "custom_apollo_lane097", "seed": 7, "max_ticks": 420},
                "include": {
                    "platform": "apollo_cyberrt",
                    "algorithm": "apollo/apollo10_carla_gt",
                    "scenario": "town01/lane_keep_097",
                    "recording": "metrics",
                    "gate": "diagnostic",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    plan = compile_run_plan(request, registry=PlatformRegistry(repo_root="."))

    assert plan.identity.run_id == "custom_apollo_lane097"
    assert plan.identity.seed == 7
    assert plan.world.max_ticks == 420
    assert plan.recording.profile == "metrics"


def test_cli_plan_writes_resolved_yaml(tmp_path: Path) -> None:
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
            "demo",
            "--gate",
            "diagnostic",
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "[plan] wrote" in result.stdout
    payload = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert payload["platform"]["name"] == "apollo_cyberrt"
    assert payload["recording"]["profile"] == "demo"


def test_cli_plan_carla_builtin_baguang_scenario_shows_launch() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "plan",
            "--platform",
            "carla_builtin",
            "--algorithm",
            "builtin/simple_acc_route_follower",
            "--scenario",
            "baguang/follow_stop_static_300m",
            "--record",
            "demo",
            "--gate",
            "scenario_validation",
            "--show-launch",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "tools/run_builtin_ego_fixed_scene.py" in result.stdout
    assert "baguang_follow_stop_static_300m" in result.stdout
    assert "carla_testbed_builtin_controller" in result.stdout
    assert '"backend_type": "planning_control_backend"' in result.stdout
    assert '"phase1_manifest_contract"' in result.stdout
    assert '"target_actor_role": "lead_vehicle"' in result.stdout


def test_apollo_town01_launch_plan_uses_carla_python_for_online_runner() -> None:
    from carla_testbed.backends.registry import default_backend_registry

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    launch = default_backend_registry().for_plan(plan).build_launch_plan(plan)

    assert launch.commands
    assert launch.commands[0][0] == launch.env["CARLA16_PYTHON"]
    assert launch.commands[0][0] == launch.env["CARLA_TESTBED_CARLA_PYTHON"]
    assert Path(launch.commands[0][0]).name.startswith("python")
    assert launch.commands[0][1] == "tools/run_town01_capability_online_chain.py"
    assert "--step" in launch.commands[0]
    assert "lane_keep:town01_rh_spawn097_goal046" in launch.commands[0]
    assert "--batch-root-parent" in launch.commands[0]


def test_apollo_curve_launch_plan_maps_curve_ref_to_capability_route_id() -> None:
    from carla_testbed.backends.registry import default_backend_registry

    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/curve217_diagnostic",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    launch = default_backend_registry().for_plan(plan).build_launch_plan(plan)

    assert launch.commands
    assert "--step" in launch.commands[0]
    assert "curve_lane_follow:town01_rh_spawn217_goal048" in launch.commands[0]
