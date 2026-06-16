from __future__ import annotations

from typing import Any, Mapping


TARGET_ACTOR_CONTRACT_SCHEMA_VERSION = "target_actor_contract.v1"

TARGET_NOT_REQUIRED_CLASSES = {
    "lane_keep",
    "lane_keep_straight",
    "lane_keep_curve",
    "curve_diagnostic",
    "junction",
    "junction_turn",
    "junction_turn_no_signal",
}

P0_TARGET_ROLE_FALLBACKS = {
    "follow_stop": "lead_vehicle",
    "follow_stop_static": "lead_vehicle",
    "static_lead_stop": "lead_vehicle",
    "lead_vehicle_accel": "lead_vehicle",
    "lead_vehicle_decel": "lead_vehicle",
    "lead_decel_accel": "lead_vehicle",
    "lead_vehicle_accel_decel": "lead_vehicle",
    "lead_hard_brake": "lead_vehicle",
    "cut_in": "cutin_vehicle",
    "cut_in_simple": "cutin_vehicle",
    "cut_out": "lead_vehicle",
    "cut_out_simple": "lead_vehicle",
}


def resolve_target_actor_contract(
    scenario: Mapping[str, Any],
    *,
    strict: bool = False,
) -> dict[str, Any]:
    """Resolve the Phase 1 target actor without guessing nearest front vehicle."""

    scenario_class = str(
        scenario.get("scenario_class")
        or scenario.get("template")
        or scenario.get("scenario_id")
        or "unknown"
    )
    roles = scenario.get("roles") if isinstance(scenario.get("roles"), Mapping) else {}
    explicit = _explicit_target_actor(scenario)
    warnings: list[str] = []
    role_aliases: dict[str, str] = {}
    activation = _activation_semantics(scenario_class, scenario)

    if explicit:
        role = explicit
        source = "scenario_case_explicit"
    elif scenario_class in TARGET_NOT_REQUIRED_CLASSES:
        return {
            "schema_version": TARGET_ACTOR_CONTRACT_SCHEMA_VERSION,
            "status": "not_required",
            "required": False,
            "scenario_class": scenario_class,
            "target_actor_role": None,
            "source": "scenario_class_not_required",
            "role_aliases": {},
            "activation": activation,
            "invalid_reason": None,
            "warnings": [],
        }
    else:
        role = str(P0_TARGET_ROLE_FALLBACKS.get(scenario_class) or "")
        source = "p0_fallback" if role else "missing"

    if role == "cutin_vehicle" and role not in roles and "lead_vehicle" in roles:
        role_aliases["cutin_vehicle"] = "lead_vehicle"
        role = "lead_vehicle"
        warnings.append("cutin_vehicle role aliased to lead_vehicle for current fixed_scene template")

    if not role or role not in roles:
        status = "missing"
        invalid_reason = "missing_target_actor"
        target_role = role or None
    else:
        status = "resolved"
        invalid_reason = None
        target_role = role

    if strict and status == "missing":
        raise ValueError(f"missing required target actor for {scenario_class}")

    return {
        "schema_version": TARGET_ACTOR_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "required": True,
        "scenario_class": scenario_class,
        "target_actor_role": target_role,
        "source": source,
        "role_aliases": role_aliases,
        "activation": activation,
        "invalid_reason": invalid_reason,
        "warnings": warnings,
    }


def _explicit_target_actor(scenario: Mapping[str, Any]) -> str | None:
    for container_key in ("scenario_case", "target_actor", "target"):
        value = scenario.get(container_key)
        if isinstance(value, Mapping):
            for key in ("role", "actor_role", "target_actor_role"):
                if value.get(key):
                    return str(value[key])
        elif isinstance(value, str) and value:
            return value
    if scenario.get("target_actor_role"):
        return str(scenario["target_actor_role"])
    return None


def _activation_semantics(scenario_class: str, scenario: Mapping[str, Any]) -> dict[str, Any]:
    if scenario_class in {"cut_in", "cut_in_simple"}:
        phases = []
        storyboard = scenario.get("storyboard") if isinstance(scenario.get("storyboard"), Mapping) else {}
        raw_phases = storyboard.get("phases") if isinstance(storyboard.get("phases"), list) else []
        for phase in raw_phases:
            if isinstance(phase, Mapping) and "cut_in" in str(phase.get("id", "")):
                phases.append(str(phase["id"]))
        return {
            "active_after_phase": phases[0] if phases else "cut_in_lane_change",
            "activation_semantics": "target becomes longitudinal target after lane-change activation phase",
        }
    return {
        "active_after_phase": None,
        "activation_semantics": "active_from_scenario_start",
    }
