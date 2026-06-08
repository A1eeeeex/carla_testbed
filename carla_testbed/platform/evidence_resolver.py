from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from carla_testbed.platform.plan import RunPlan


COMMON_ANALYZERS = {
    "artifact_completeness",
    "route_health",
    "control_health",
    "control_attribution",
    "assist_ledger",
}

APOLLO_ANALYZERS = {
    "channel_health",
    "localization_contract",
    "chassis_gt_contract",
    "apollo_hdmap_projection",
    "apollo_reference_line_contract",
    "planning_materialization",
    "apollo_control_handoff",
    "prediction_evidence",
    "apollo_link_health",
}

AUTOWARE_ANALYZERS = {
    "autoware_evidence",
    "autoware_tf_health",
    "autoware_route_state",
    "autoware_engage_state",
    "autoware_planning_trajectory_health",
    "autoware_control_command_health",
    "autoware_lanelet_map_contract",
}

TRAFFIC_LIGHT_ANALYZERS = {"traffic_light_contract", "traffic_light_behavior"}
OBSTACLE_ANALYZERS = {"obstacle_gt_contract", "prediction_evidence"}
TRAFFIC_FLOW_ANALYZERS = {"traffic_flow_contract"}
PEDESTRIAN_FLOW_ANALYZERS = {"pedestrian_flow_contract"}
FIXED_SCENE_ANALYZERS = {"fixed_scene_contract", "scenario_actor_contract"}


@dataclass(frozen=True)
class EvidenceResolution:
    required_analyzers: list[str]
    optional_analyzers: list[str]
    not_applicable_analyzers: list[str]
    rules: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "required_analyzers": list(self.required_analyzers),
            "optional_analyzers": list(self.optional_analyzers),
            "not_applicable_analyzers": list(self.not_applicable_analyzers),
            "rules": [dict(rule) for rule in self.rules],
        }


def resolve_evidence_for_plan(plan: RunPlan) -> EvidenceResolution:
    base_required = set(plan.evidence.required_analyzers or [])
    base_optional = set(plan.evidence.optional_analyzers or [])
    not_applicable: set[str] = set()

    if plan.platform.name in {"apollo_cyberrt", "carla_direct"} or plan.algorithm.stack == "apollo":
        base_required |= APOLLO_ANALYZERS if plan.gate.profile != "smoke" else {"artifact_completeness"}
        not_applicable |= AUTOWARE_ANALYZERS
    elif plan.platform.name == "autoware_ros2" or plan.algorithm.stack == "autoware":
        base_required |= (
            (COMMON_ANALYZERS | AUTOWARE_ANALYZERS)
            if plan.gate.profile != "smoke"
            else {"artifact_completeness"}
        )
        not_applicable |= APOLLO_ANALYZERS
        base_required.discard("channel_health")
        base_required.discard("apollo_link_health")
    else:
        base_required |= {"artifact_completeness"}
        not_applicable |= APOLLO_ANALYZERS | AUTOWARE_ANALYZERS

    requirements = plan.scenario.requirements or {}
    if requirements.get("traffic_light_required"):
        base_required |= TRAFFIC_LIGHT_ANALYZERS
        base_optional -= TRAFFIC_LIGHT_ANALYZERS
    if _vehicles_enabled(plan):
        base_required |= TRAFFIC_FLOW_ANALYZERS
    if _walkers_enabled(plan):
        base_required |= PEDESTRIAN_FLOW_ANALYZERS
    if _fixed_scene_enabled(plan):
        base_required |= FIXED_SCENE_ANALYZERS
        if plan.platform.name in {"apollo_cyberrt", "carla_direct", "autoware_ros2"} or plan.algorithm.stack in {
            "apollo",
            "autoware",
        }:
            base_required |= OBSTACLE_ANALYZERS
    if requirements.get("dynamic_obstacle_required"):
        base_required |= OBSTACLE_ANALYZERS
    if requirements.get("hdmap_projection_required"):
        if plan.platform.name == "autoware_ros2":
            base_required.add("autoware_lanelet_map_contract")
            base_required.discard("apollo_hdmap_projection")
        elif plan.platform.name in {"apollo_cyberrt", "carla_direct"}:
            base_required.add("apollo_hdmap_projection")
    if requirements.get("reference_line_required") and plan.platform.name in {"apollo_cyberrt", "carla_direct"}:
        base_required.add("apollo_reference_line_contract")

    rules = _rules_for_plan(plan)
    base_required -= not_applicable
    base_optional -= base_required
    base_optional -= not_applicable
    return EvidenceResolution(
        required_analyzers=sorted(base_required),
        optional_analyzers=sorted(base_optional),
        not_applicable_analyzers=sorted(not_applicable),
        rules=rules,
    )


def _rules_for_plan(plan: RunPlan) -> list[dict[str, Any]]:
    traffic_rules = _traffic_flow_rules(plan)
    fixed_scene_rules = _fixed_scene_rules(plan)
    if not plan.gate.can_claim_natural_driving:
        return [*traffic_rules, *fixed_scene_rules]
    if plan.platform.name == "autoware_ros2" or plan.algorithm.stack == "autoware":
        return [
            _rule("autoware_evidence_present", "autoware_evidence", "can_compare_with_apollo", "exists", True),
            _rule("no_blocking_assists", "assist_ledger", "blocking_assists", "==", []),
            *traffic_rules,
            *fixed_scene_rules,
        ]
    rules: list[dict[str, Any]] = [
        _rule(
            "planning_nonempty_ratio",
            "planning_materialization",
            "metrics.nonempty_trajectory_ratio",
            ">=",
            0.8,
        ),
        _rule(
            "route_established",
            "planning_materialization",
            "route_establishment.route_established",
            "==",
            True,
        ),
        _rule("hdmap_projection_claim_grade", "apollo_hdmap_projection", "claim_grade", "==", True),
        _rule("localization_contract_claim_grade", "localization_contract", "claim_grade", "==", True),
        _rule("chassis_gt_contract_claim_grade", "chassis_gt_contract", "claim_grade", "==", True),
        _rule("reference_line_contract_pass", "apollo_reference_line_contract", "status", "in", ["pass"]),
        _rule("control_handoff_pass", "apollo_control_handoff", "status", "in", ["pass", "warn"]),
        _rule("applied_control_source_apollo", "control_attribution", "applied_control_source", "==", "apollo"),
        _rule("no_blocking_assists", "assist_ledger", "blocking_assists", "==", []),
    ]
    requirements = plan.scenario.requirements or {}
    rules.extend(traffic_rules)
    rules.extend(fixed_scene_rules)
    if requirements.get("prediction_required") or requirements.get("dynamic_obstacle_required"):
        rules.append(
            _rule(
                "prediction_evidence_explicit",
                "prediction_evidence",
                "prediction_mode",
                "in",
                ["native_observed", "bypassed_with_gt_obstacles"],
            )
        )
    if requirements.get("traffic_light_required"):
        rules.extend(
            [
                _rule(
                    "traffic_light_policy_actual",
                    "traffic_light_contract",
                    "traffic_light_policy",
                    "==",
                    "carla_actual",
                ),
                _rule("traffic_light_force_green_blocked", "traffic_light_contract", "force_green", "==", False),
                _rule("traffic_light_behavior_present", "traffic_light_behavior", "status", "in", ["pass", "warn"]),
            ]
        )
    return rules


def _traffic_flow_rules(plan: RunPlan) -> list[dict[str, Any]]:
    if not plan.traffic_flow.enabled:
        return []
    rules: list[dict[str, Any]] = []
    if _vehicles_enabled(plan):
        rules.extend(
            [
                _rule(
                    "traffic_flow_spawned_requested_count",
                    "traffic_flow_contract",
                    "metrics.spawned_vehicle_count",
                    "==",
                    int(plan.traffic_flow.vehicles.get("count", 0) or 0),
                ),
                _rule(
                    "traffic_flow_tm_sync_matches_world",
                    "traffic_flow_contract",
                    "metrics.tm_sync_matches_world",
                    "==",
                    True,
                ),
                _rule(
                    "ego_not_registered_to_tm",
                    "traffic_flow_contract",
                    "metrics.ego_registered_to_tm",
                    "==",
                    False,
                ),
            ]
        )
    if _walkers_enabled(plan):
        rules.extend(
            [
                _rule(
                    "pedestrian_flow_spawned_requested_count",
                    "pedestrian_flow_contract",
                    "metrics.spawned_walker_count",
                    "==",
                    int(plan.traffic_flow.walkers.get("count", 0) or 0),
                ),
                _rule(
                    "pedestrian_controllers_started",
                    "pedestrian_flow_contract",
                    "metrics.controller_started_count",
                    "==",
                    int(plan.traffic_flow.walkers.get("count", 0) or 0),
                ),
                _rule(
                    "ego_not_registered_as_walker",
                    "pedestrian_flow_contract",
                    "metrics.ego_registered_as_walker",
                    "==",
                    False,
                ),
            ]
        )
    return rules


def _fixed_scene_rules(plan: RunPlan) -> list[dict[str, Any]]:
    if not _fixed_scene_enabled(plan):
        return []
    return [
        _rule("fixed_scene_contract_pass", "fixed_scene_contract", "status", "in", ["pass", "warn"]),
        _rule("scenario_actor_contract_pass", "scenario_actor_contract", "status", "in", ["pass", "warn"]),
    ]


def _vehicles_enabled(plan: RunPlan) -> bool:
    if not plan.traffic_flow.enabled:
        return False
    provider = plan.traffic_flow.provider
    vehicles = plan.traffic_flow.vehicles or {}
    return bool(vehicles.get("enabled", provider in {"carla_traffic_manager", "mixed_carla_flow"})) and int(
        vehicles.get("count", 0) or 0
    ) > 0


def _fixed_scene_enabled(plan: RunPlan) -> bool:
    fixed_scene = plan.scenario.fixed_scene or {}
    if not fixed_scene:
        return False
    return bool(fixed_scene.get("enabled", True))


def _walkers_enabled(plan: RunPlan) -> bool:
    if not plan.traffic_flow.enabled:
        return False
    provider = plan.traffic_flow.provider
    walkers = plan.traffic_flow.walkers or {}
    return bool(walkers.get("enabled", provider in {"carla_walker_ai_controller", "mixed_carla_flow"})) and int(
        walkers.get("count", 0) or 0
    ) > 0


def _rule(rule_id: str, report: str, path: str, op: str, value: Any) -> dict[str, Any]:
    return {
        "id": rule_id,
        "report": report,
        "path": path,
        "op": op,
        "value": value,
        "required_for": "claim",
    }
