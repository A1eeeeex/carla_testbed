from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml

FIXED_SCENE_TEMPLATE_SCHEMA_VERSION = "fixed_scene_template.v1"
FIXED_SCENE_STORYBOARD_SCHEMA_VERSION = "fixed_scene_storyboard.v1"

SUPPORTED_TEMPLATES = {
    "follow_stop",
    "static_lead_stop",
    "lead_vehicle_accel_decel",
    "cut_in",
    "cut_out",
}

SUPPORTED_ACTIONS = {
    "brake_to_stop",
    "assign_route",
    "follow_route",
    "gap_control",
    "hold_stop",
    "lane_change",
    "maintain_gap",
    "maintain_speed",
    "set_speed",
    "spawn_actor",
    "speed_profile",
}

SUPPORTED_TRIGGER_TYPES = {
    "actor_route_s",
    "actor_speed",
    "all",
    "any",
    "ego_route_s",
    "phase_completed",
    "relative_distance",
    "relative_longitudinal_distance",
    "route_s",
    "simulation_time",
    "world_frame",
}


def load_fixed_scene_template(path: str | Path) -> dict[str, Any]:
    data = _load_mapping(path)
    template = extract_template_config(data)
    validate_fixed_scene_template(template)
    return template


def load_fixed_scene_storyboard(path: str | Path) -> dict[str, Any]:
    storyboard = _load_mapping(path)
    validate_fixed_scene_storyboard(storyboard)
    return storyboard


def extract_template_config(data: Mapping[str, Any]) -> dict[str, Any]:
    """Return the fixed-scene template block from either a template or scenario file."""

    if data.get("schema_version") == FIXED_SCENE_TEMPLATE_SCHEMA_VERSION:
        return dict(data)
    fixed_scene = data.get("fixed_scene")
    if isinstance(fixed_scene, Mapping):
        template = dict(fixed_scene)
        template.setdefault("scenario_id", data.get("scenario_id"))
        template.setdefault("map", data.get("map"))
        template.setdefault("route_id", data.get("route_id"))
        template.setdefault("scenario_class", data.get("scenario_class"))
        template.setdefault("gate_role", data.get("gate_role"))
        if isinstance(data.get("target_actor"), Mapping):
            template.setdefault("target_actor", dict(data["target_actor"]))
        template.setdefault("schema_version", FIXED_SCENE_TEMPLATE_SCHEMA_VERSION)
        return template
    raise ValueError("expected fixed_scene_template.v1 or scenario file with fixed_scene block")


def validate_fixed_scene_template(cfg: Mapping[str, Any]) -> None:
    errors: list[str] = []
    if cfg.get("schema_version") != FIXED_SCENE_TEMPLATE_SCHEMA_VERSION:
        errors.append("schema_version must be fixed_scene_template.v1")
    template = cfg.get("template")
    if template not in SUPPORTED_TEMPLATES:
        errors.append(f"template must be one of {sorted(SUPPORTED_TEMPLATES)}")
    if not isinstance(cfg.get("roles"), Mapping):
        errors.append("roles mapping is required")
    else:
        roles = cfg["roles"]
        if "ego" not in roles:
            errors.append("roles.ego is required")
        if template in {"follow_stop", "static_lead_stop", "lead_vehicle_accel_decel", "cut_in", "cut_out"} and "lead_vehicle" not in roles:
            errors.append("roles.lead_vehicle is required")
    if not isinstance(cfg.get("params"), Mapping):
        errors.append("params mapping is required")
    if "success_criteria" in cfg and not isinstance(cfg["success_criteria"], Mapping):
        errors.append("success_criteria must be a mapping")
    if errors:
        raise ValueError("; ".join(errors))


def validate_fixed_scene_storyboard(storyboard: Mapping[str, Any]) -> None:
    errors: list[str] = []
    if storyboard.get("schema_version") != FIXED_SCENE_STORYBOARD_SCHEMA_VERSION:
        errors.append("schema_version must be fixed_scene_storyboard.v1")
    if not storyboard.get("scene_id"):
        errors.append("scene_id is required")
    roles = storyboard.get("roles")
    if not isinstance(roles, Mapping) or "ego" not in roles:
        errors.append("roles.ego is required")
    phases = storyboard.get("storyboard", {}).get("phases")
    if not isinstance(phases, list) or not phases:
        errors.append("storyboard.phases must be a non-empty list")
    else:
        for index, phase in enumerate(phases):
            if not isinstance(phase, Mapping):
                errors.append(f"storyboard.phases[{index}] must be a mapping")
                continue
            if not phase.get("id"):
                errors.append(f"storyboard.phases[{index}].id is required")
            _validate_trigger(phase.get("start"), errors, f"storyboard.phases[{index}].start")
            actions = phase.get("actions")
            if not isinstance(actions, list) or not actions:
                errors.append(f"storyboard.phases[{index}].actions must be a non-empty list")
            else:
                for action_index, action in enumerate(actions):
                    _validate_action(action, errors, f"storyboard.phases[{index}].actions[{action_index}]")
    stop = storyboard.get("storyboard", {}).get("stop")
    if stop is not None:
        _validate_trigger(stop, errors, "storyboard.stop")
    if errors:
        raise ValueError("; ".join(errors))


def _validate_action(action: Any, errors: list[str], field: str) -> None:
    if not isinstance(action, Mapping):
        errors.append(f"{field} must be a mapping")
        return
    if action.get("type") not in SUPPORTED_ACTIONS:
        errors.append(f"{field}.type must be one of {sorted(SUPPORTED_ACTIONS)}")
    if not action.get("role"):
        errors.append(f"{field}.role is required")


def _validate_trigger(trigger: Any, errors: list[str], field: str) -> None:
    if trigger is None:
        return
    if not isinstance(trigger, Mapping):
        errors.append(f"{field} must be a mapping")
        return
    if "all" in trigger:
        trigger = {"type": "all", "conditions": trigger.get("all")}
    elif "any" in trigger:
        trigger = {"type": "any", "conditions": trigger.get("any")}
    trigger_type = trigger.get("type")
    if trigger_type not in SUPPORTED_TRIGGER_TYPES:
        errors.append(f"{field}.type must be one of {sorted(SUPPORTED_TRIGGER_TYPES)}")
    if trigger_type in {"all", "any"}:
        children = trigger.get("conditions")
        if not isinstance(children, list) or not children:
            errors.append(f"{field}.conditions must be a non-empty list")
        else:
            for index, child in enumerate(children):
                _validate_trigger(child, errors, f"{field}.conditions[{index}]")


def _load_mapping(path: str | Path) -> dict[str, Any]:
    source = Path(path).expanduser()
    with source.open("r", encoding="utf-8") as handle:
        if source.suffix.lower() == ".json":
            data = json.load(handle)
        else:
            data = yaml.safe_load(handle)
    if not isinstance(data, Mapping):
        raise ValueError(f"{source} must contain a mapping")
    return dict(data)
