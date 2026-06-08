from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.evidence.bundle import build_evidence_bundle
from carla_testbed.evidence.gate_runner import run_gate
from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _fake_run(run_dir: Path) -> None:
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "097",
            "backend": "carla_builtin",
        },
    )
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "097",
            "backend": "carla_builtin",
        },
    )


def test_scenario_validation_gate_does_not_require_apollo(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm_2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    run_dir = tmp_path / "run"
    _fake_run(run_dir)

    bundle = build_evidence_bundle(run_dir, plan=plan)

    assert "apollo_link_health" not in bundle["required_evidence"]
    assert "traffic_flow_contract" in bundle["required_evidence"]


def test_diagnostic_gate_requires_traffic_contract_when_enabled(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm_2",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )
    run_dir = tmp_path / "run"
    _fake_run(run_dir)

    gate = run_gate(run_dir, plan=plan)

    assert "traffic_flow_contract" in gate["missing_required_evidence"]
    assert gate["status"] in {"fail", "insufficient_data"}


def test_scenario_validation_traffic_rule_uses_plan_count(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm_2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    run_dir = tmp_path / "run"
    _fake_run(run_dir)
    _write_json(
        run_dir / "analysis/traffic_flow_contract/traffic_flow_contract_report.json",
        {
            "schema_version": "traffic_flow_contract.v1",
            "status": "pass",
            "metrics": {
                "spawned_vehicle_count": 1,
                "tm_sync_matches_world": True,
                "ego_registered_to_tm": False,
            },
        },
    )

    gate = run_gate(run_dir, plan=plan)
    traffic_rule = next(rule for rule in gate["rules"] if rule["id"] == "traffic_flow_spawned_requested_count")

    assert traffic_rule["expected"] == 2
    assert traffic_rule["actual"] == 1
    assert traffic_rule["status"] == "fail"
    assert gate["ego_control_source"] == "carla_builtin"
    assert gate["background_traffic_control_source"] == "carla_traffic_manager"
    assert gate["control_source_boundary"]["scenario_actor_control_source"] == "none"
