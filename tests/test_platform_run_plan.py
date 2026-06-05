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
