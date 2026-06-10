from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.evidence.bundle import build_evidence_bundle
from carla_testbed.evidence.gate_runner import run_gate
from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
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
            "background_traffic_control_source": "carla_traffic_manager",
            "background_walker_control_source": "carla_walker_ai_controller",
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


def test_mixed_traffic_flow_plan_requires_vehicle_and_pedestrian_contracts() -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm2_walkers2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    assert plan.traffic_flow.provider == "mixed_carla_flow"
    assert plan.traffic_flow.vehicles["count"] == 2
    assert plan.traffic_flow.walkers["count"] == 2
    assert "traffic_flow_contract" in plan.evidence.required_analyzers
    assert "pedestrian_flow_contract" in plan.evidence.required_analyzers
    assert any(rule["id"] == "pedestrian_flow_trace_rows_present" for rule in plan.gate.rules)
    assert any(rule["id"] == "pedestrian_flow_walkers_moved" for rule in plan.gate.rules)


def test_mixed_plan_yaml_preserves_walkers(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm2_walkers2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    payload = write_run_plan(plan, tmp_path / "plan.yaml").read_text(encoding="utf-8")

    assert "mixed_carla_flow" in payload
    assert "walkers:" in payload


def test_evidence_bundle_reports_walker_control_source(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm2_walkers2",
        recording="demo",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )
    run_dir = tmp_path / "run"
    _fake_run(run_dir)

    bundle = build_evidence_bundle(run_dir, plan=plan)

    assert bundle["background_traffic_control_source"] == "carla_traffic_manager"
    assert bundle["background_walker_control_source"] == "carla_walker_ai_controller"
    assert "pedestrian_flow_contract" in bundle["required_evidence"]


def test_gate_requires_pedestrian_contract_for_mixed_flow(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm2_walkers2",
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
                "spawned_vehicle_count": 2,
                "tm_sync_matches_world": True,
                "ego_registered_to_tm": False,
            },
        },
    )

    gate = run_gate(run_dir, plan=plan)

    assert "pedestrian_flow_contract" in gate["missing_required_evidence"]
    assert gate["background_walker_control_source"] == "carla_walker_ai_controller"


def test_gate_rejects_pedestrian_contract_without_movement_metrics(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="simple_route_follower",
        scenario="town01/lane_keep_097",
        traffic="town01/random_tm2_walkers2",
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
                "spawned_vehicle_count": 2,
                "tm_sync_matches_world": True,
                "ego_registered_to_tm": False,
            },
        },
    )
    _write_json(
        run_dir / "analysis/pedestrian_flow_contract/pedestrian_flow_contract_report.json",
        {
            "schema_version": "pedestrian_flow_contract.v1",
            "status": "pass",
            "metrics": {
                "spawned_walker_count": 2,
                "controller_started_count": 2,
                "walker_trace_row_count": 0,
                "moving_walker_count": 0,
                "ego_registered_as_walker": False,
            },
            "claim_blocking_reasons": ["walker_flow_trace_missing"],
        },
    )

    gate = run_gate(run_dir, plan=plan)

    assert gate["status"] == "fail"
    failed_rules = {rule["id"] for rule in gate["rules"] if rule["status"] == "fail"}
    assert "pedestrian_flow_trace_rows_present" in failed_rules
    assert "pedestrian_flow_walkers_moved" in failed_rules
