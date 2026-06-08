from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.evidence.bundle import build_evidence_bundle
from carla_testbed.evidence.gate_runner import run_gate
from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_run_plan_preserves_fixed_scene_and_requires_contracts() -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/follow_stop_097",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    assert plan.scenario.fixed_scene["template"] == "follow_stop"
    assert "fixed_scene_contract" in plan.evidence.required_analyzers
    assert "scenario_actor_contract" in plan.evidence.required_analyzers
    assert "obstacle_gt_contract" not in plan.evidence.required_analyzers
    assert any(rule["id"] == "fixed_scene_contract_pass" for rule in plan.gate.rules)


def test_apollo_fixed_scene_requires_obstacle_and_prediction_evidence() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/follow_stop_097",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    assert "fixed_scene_contract" in plan.evidence.required_analyzers
    assert "scenario_actor_contract" in plan.evidence.required_analyzers
    assert "obstacle_gt_contract" in plan.evidence.required_analyzers
    assert "prediction_evidence" in plan.evidence.required_analyzers


def test_evidence_bundle_indexes_fixed_scene_reports(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(json.dumps({"run_id": "fixed_scene_run"}), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps({"run_id": "fixed_scene_run"}), encoding="utf-8")
    fixed_dir = run_dir / "analysis" / "fixed_scene_contract"
    actor_dir = run_dir / "analysis" / "scenario_actor_contract"
    fixed_dir.mkdir(parents=True)
    actor_dir.mkdir(parents=True)
    (fixed_dir / "fixed_scene_contract_report.json").write_text(json.dumps({"status": "pass"}), encoding="utf-8")
    (actor_dir / "scenario_actor_contract_report.json").write_text(json.dumps({"status": "pass"}), encoding="utf-8")
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/follow_stop_097",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    bundle = build_evidence_bundle(run_dir, plan=plan)
    gate = run_gate(run_dir, plan=plan)

    assert bundle["scenario_actor_control_source"] == "fixed_scene_player"
    assert bundle["artifacts"]["fixed_scene_contract"]["status"] == "pass"
    assert bundle["artifacts"]["scenario_actor_contract"]["status"] == "pass"
    assert all(
        check["name"] not in {"fixed_scene_contract", "scenario_actor_contract"}
        for check in gate["checks"]
    )


def test_cli_list_fixed_scenes() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "carla_testbed", "list", "fixed-scenes"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "follow_stop" in result.stdout
    assert "cut_in" in result.stdout
