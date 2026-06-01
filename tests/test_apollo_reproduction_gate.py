from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from carla_testbed.algorithms.reproduction import load_reproduction_report
from carla_testbed.algorithms.reproduction_gate import (
    evaluate_reproduction_gate,
    load_reproduction_gate_config,
)

REPORT_PATH = Path("configs/algorithms/apollo_reproduction_report.example.yaml")
GATE_CONFIG_PATH = Path("configs/algorithms/apollo_reproduction_gate.yaml")


def _reproduction_report() -> dict:
    return load_reproduction_report(REPORT_PATH)


def _route_health_report(status: str = "pass") -> dict:
    return {
        "schema_version": "route_health.v1",
        "verdict": {"status": status},
        "route_id": "town01_rh_spawn217_goal048",
    }


def _ab_report(status: str = "pass") -> dict:
    return {
        "schema_version": "ab_report.v1",
        "status": status,
        "verdict": {"status": status},
    }


def _calibration_report(status: str = "pass") -> dict:
    return {
        "schema_version": "calibration_report.v1",
        "status": status,
        "recommendation": {"enable_physical_mapping": False},
    }


def test_l0_l4_pass_with_route_health_and_ab_report_passes() -> None:
    report = evaluate_reproduction_gate(
        _reproduction_report(),
        route_health=_route_health_report(),
        ab_report=_ab_report(),
        calibration_report=_calibration_report(),
        config=load_reproduction_gate_config(GATE_CONFIG_PATH),
    )

    assert report["status"] == "pass"
    assert report["can_run_closed_loop_eval"] is True
    assert report["can_claim_algorithm_limitation"] is True
    assert report["blocking_reasons"] == []


def test_l3_fail_blocks_algorithm_capability_claim() -> None:
    reproduction = _reproduction_report()
    reproduction = deepcopy(reproduction)
    reproduction["levels"]["L3_carla_to_apollo_adapter_contract"]["status"] = "fail"
    reproduction["levels"]["L5_closed_loop"]["status"] = "blocked"

    report = evaluate_reproduction_gate(
        reproduction,
        route_health=_route_health_report(),
        ab_report=_ab_report(),
        calibration_report=_calibration_report(),
    )

    assert report["status"] == "blocked"
    assert report["can_run_closed_loop_eval"] is False
    assert report["can_claim_algorithm_limitation"] is False
    assert any("L3_carla_to_apollo_adapter_contract" in reason for reason in report["blocking_reasons"])
    assert "adapter_contract_not_proven" in report["blocking_reasons"]


def test_l4_fail_blocks_closed_loop_capability_claim() -> None:
    reproduction = _reproduction_report()
    reproduction = deepcopy(reproduction)
    reproduction["levels"]["L4_shadow_mode"]["status"] = "fail"
    reproduction["levels"]["L5_closed_loop"]["status"] = "blocked"

    report = evaluate_reproduction_gate(
        reproduction,
        route_health=_route_health_report(),
        ab_report=_ab_report(),
        calibration_report=_calibration_report(),
    )

    assert report["status"] == "blocked"
    assert report["can_run_closed_loop_eval"] is False
    assert "shadow_mode_not_proven" in report["blocking_reasons"]
    assert any("route/reference-line/planning semantics" in action for action in report["required_next_actions"])


def test_missing_route_health_blocks_curve_conclusion() -> None:
    report = evaluate_reproduction_gate(
        _reproduction_report(),
        route_health=None,
        ab_report=_ab_report(),
        calibration_report=_calibration_report(),
        requested_claims=["curve_lateral_semantics_conclusion"],
    )

    assert report["status"] == "warn"
    assert report["can_claim_algorithm_limitation"] is False
    assert "route_health_report" in report["missing_artifacts"]
    assert "route_health_required_for_curve_claim" in report["blocking_reasons"]


def test_missing_ab_report_blocks_direct_improvement_claim() -> None:
    report = evaluate_reproduction_gate(
        _reproduction_report(),
        route_health=_route_health_report(),
        ab_report=None,
        calibration_report=_calibration_report(),
        requested_claims=["carla_direct_improvement"],
    )

    assert report["status"] == "warn"
    assert report["can_claim_algorithm_limitation"] is False
    assert "ab_report" in report["missing_artifacts"]
    assert "ab_report_required_for_direct_improvement_claim" in report["blocking_reasons"]


def test_physical_mapping_promotion_without_calibration_fails() -> None:
    report = evaluate_reproduction_gate(
        _reproduction_report(),
        route_health=_route_health_report(),
        ab_report=_ab_report(),
        calibration_report=None,
        requested_claims=["physical_mapping_promotion"],
    )

    assert report["status"] == "fail"
    assert report["can_claim_algorithm_limitation"] is False
    assert "calibration_required_for_physical_mapping_promotion" in report["blocking_reasons"]


def test_l5_fail_l4_pass_without_calibration_requires_control_actuation_report() -> None:
    reproduction = _reproduction_report()
    reproduction = deepcopy(reproduction)
    reproduction["levels"]["L5_closed_loop"]["status"] = "fail"

    report = evaluate_reproduction_gate(
        reproduction,
        route_health=_route_health_report(),
        ab_report=_ab_report(),
        calibration_report=None,
    )

    assert report["status"] == "warn"
    assert report["can_claim_algorithm_limitation"] is False
    assert "control_actuation_report" in report["missing_artifacts"]
