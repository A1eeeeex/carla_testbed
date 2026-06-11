from __future__ import annotations

from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.evidence_resolver import resolve_evidence_for_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_autoware_diagnostic_does_not_require_apollo_link_health() -> None:
    plan = compile_run_plan(
        platform="autoware_ros2",
        algorithm="autoware/universe_gt_localization",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    resolution = resolve_evidence_for_plan(plan)

    assert "apollo_link_health" not in resolution.required_analyzers
    assert "apollo_link_health" in resolution.not_applicable_analyzers
    assert "autoware_evidence" in resolution.required_analyzers
    assert "autoware_control_command_health" in resolution.required_analyzers


def test_traffic_light_requirements_add_contract_and_behavior() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/traffic_light_red_stop",
        recording="claim",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    assert "traffic_light_contract" in plan.evidence.required_analyzers
    assert "traffic_light_behavior" in plan.evidence.required_analyzers
    assert "chassis_gt_contract" in plan.evidence.required_analyzers
    rule_ids = {rule["id"] for rule in plan.gate.rules}
    assert "traffic_light_force_green_blocked" in rule_ids
    assert "traffic_light_policy_actual" in rule_ids
    assert "chassis_gt_contract_claim_grade" in rule_ids


def test_apollo_claim_resolution_uses_pass_only_core_rules() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="claim",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )
    rules = {rule["id"]: rule for rule in plan.gate.rules}

    for rule_id in [
        "runtime_claim_boundary_pass",
        "apollo_route_contract_pass",
        "apollo_module_consumption_pass",
        "localization_contract_pass",
        "chassis_gt_contract_pass",
        "hdmap_projection_pass",
        "reference_line_contract_pass",
        "control_handoff_pass",
        "prediction_evidence_explicit",
    ]:
        rule = rules[rule_id]
        assert rule["op"] == "=="
    assert rules["prediction_evidence_explicit"]["value"] == "native_observed"
