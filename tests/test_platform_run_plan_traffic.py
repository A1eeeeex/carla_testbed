from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_run_plan_includes_traffic_flow_profile() -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm_2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    assert plan.traffic_flow.enabled is True
    assert plan.traffic_flow.provider == "carla_traffic_manager"
    assert plan.traffic_flow.seed == 42
    assert plan.traffic_flow.vehicles["count"] == 2
    assert "traffic_flow_contract" in plan.evidence.required_analyzers
    assert "apollo_link_health" not in plan.evidence.required_analyzers


def test_run_plan_yaml_preserves_traffic_flow(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm_2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    path = write_run_plan(plan, tmp_path / "plan.yaml")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert payload["traffic_flow"]["provider"] == "carla_traffic_manager"
    assert payload["traffic_flow"]["vehicles"]["count"] == 2


def test_cli_plan_accepts_traffic_profile(tmp_path: Path) -> None:
    out = tmp_path / "plan.yaml"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "plan",
            "--platform",
            "carla_builtin",
            "--algorithm",
            "simple_route_follower",
            "--scenario",
            "town01/lane_keep_097",
            "--traffic",
            "town01/random_tm_2",
            "--record",
            "demo",
            "--gate",
            "scenario_validation",
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "[plan] wrote" in result.stdout
    assert out.is_file()


def test_cli_list_traffic() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "carla_testbed", "list", "traffic"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "town01_random_tm_2" in result.stdout
