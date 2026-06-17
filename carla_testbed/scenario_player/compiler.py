from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping

from carla_testbed.scenario_player.schema import (
    FIXED_SCENE_STORYBOARD_SCHEMA_VERSION,
    validate_fixed_scene_storyboard,
    validate_fixed_scene_template,
)
from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract


def compile_fixed_scene_template(template_cfg: Mapping[str, Any]) -> dict[str, Any]:
    template = copy.deepcopy(dict(template_cfg))
    validate_fixed_scene_template(template)
    name = str(template["template"])
    params = dict(template.get("params", {}))
    scene_id = str(template.get("scenario_id") or params.get("scene_id") or name)
    if name == "follow_stop":
        storyboard = _compile_follow_stop(template, scene_id)
    elif name == "static_lead_stop":
        storyboard = _compile_static_lead_stop(template, scene_id)
    elif name == "lead_vehicle_accel_decel":
        storyboard = _compile_lead_vehicle_accel_decel(template, scene_id)
    elif name == "cut_in":
        storyboard = _compile_cut_in(template, scene_id)
    elif name == "cut_out":
        storyboard = _compile_cut_out(template, scene_id)
    else:
        raise ValueError(f"unsupported fixed scene template: {name}")
    _mark_required_phases(storyboard)
    validate_fixed_scene_storyboard(storyboard)
    return storyboard


def _base_storyboard(template: Mapping[str, Any], scene_id: str) -> dict[str, Any]:
    roles = copy.deepcopy(dict(template.get("roles", {})))
    params = copy.deepcopy(dict(template.get("params", {})))
    payload = {
        "schema_version": FIXED_SCENE_STORYBOARD_SCHEMA_VERSION,
        "scene_id": scene_id,
        "template": template.get("template"),
        "map": template.get("map"),
        "route_id": template.get("route_id"),
        "scenario_class": template.get("scenario_class", template.get("template")),
        "gate_role": template.get("gate_role", "diagnostic_gate"),
        "roles": roles,
        "params": params,
        "storyboard": {
            "phases": [],
            "stop": _stop_trigger_from_params(params),
        },
        "success_criteria": copy.deepcopy(dict(template.get("success_criteria", {}))),
        "compiled_from": {
            "schema_version": template.get("schema_version"),
            "template": template.get("template"),
            "hash": _stable_hash(template),
        },
        "compiled_at": datetime.now(timezone.utc).isoformat(),
        "runtime_boundary": {
            "ego_control_owner": "backend",
            "key_actor_control_owner": "fixed_scene_player",
            "background_traffic_owner": "traffic_flow_provider",
            "claim_boundary": "fixed_scene_playback_is_scenario_setup_not_ego_capability_evidence",
        },
    }
    if isinstance(template.get("target_actor"), Mapping):
        payload["target_actor"] = copy.deepcopy(dict(template["target_actor"]))
    payload["target_actor_contract"] = resolve_target_actor_contract(payload)
    return payload


def _mark_required_phases(storyboard: dict[str, Any]) -> None:
    phases = storyboard.get("storyboard", {}).get("phases")
    if not isinstance(phases, list):
        return
    for phase in phases:
        if isinstance(phase, dict):
            phase.setdefault("required", True)


def _stop_trigger_from_params(params: Mapping[str, Any]) -> dict[str, Any]:
    duration_s = float(params.get("duration_s", 45.0) or 45.0)
    if params.get("duration_policy") == "lead_reaches_road_end" and params.get("lead_route_end_s_m") is not None:
        return {
            "type": "any",
            "conditions": [
                {
                    "type": "actor_route_s",
                    "role": str(params.get("lead_role", "lead_vehicle")),
                    "op": ">=",
                    "value_m": float(params["lead_route_end_s_m"]),
                },
                {
                    "type": "simulation_time",
                    "op": ">=",
                    "value": duration_s,
                    "fallback": True,
                },
            ],
        }
    return {
        "type": "simulation_time",
        "op": ">=",
        "value": duration_s,
    }


def _compile_follow_stop(template: Mapping[str, Any], scene_id: str) -> dict[str, Any]:
    params = dict(template.get("params", {}))
    lead_role = str(params.get("lead_role", "lead_vehicle"))
    cruise_speed = float(params.get("lead_cruise_speed_mps", 16.0))
    brake_trigger_gap = float(params.get("brake_trigger_gap_m", 80.0))
    stop_gap = float(params.get("target_stop_gap_m", 12.0))
    hold_s = float(params.get("hold_s", 5.0))
    storyboard = _base_storyboard(template, scene_id)
    storyboard["storyboard"]["phases"] = [
        {
            "id": "lead_cruise",
            "start": {"type": "simulation_time", "op": ">=", "value": 0.0},
            "actions": [
                {
                    "role": lead_role,
                    "type": "follow_route",
                    "controller": "route_follower",
                    "target_speed_mps": cruise_speed,
                }
            ],
        },
        {
            "id": "lead_brake_to_stop",
            "start": {
                "type": "relative_distance",
                "from_role": "ego",
                "to_role": lead_role,
                "op": "<=",
                "value_m": brake_trigger_gap,
            },
            "actions": [
                {
                    "role": lead_role,
                    "type": "brake_to_stop",
                    "controller": "brake_to_stop",
                    "target_gap_m": stop_gap,
                    "target_speed_mps": 0.0,
                }
            ],
        },
        {
            "id": "lead_hold_stop",
            "start": {
                "type": "all",
                "conditions": [
                    {
                        "type": "relative_distance",
                        "from_role": "ego",
                        "to_role": lead_role,
                        "op": "<=",
                        "value_m": brake_trigger_gap,
                    },
                    {"type": "actor_speed", "role": lead_role, "op": "<=", "value_mps": 0.2},
                ],
            },
            "actions": [
                {
                    "role": lead_role,
                    "type": "hold_stop",
                    "controller": "brake_to_stop",
                    "duration_s": hold_s,
                    "target_speed_mps": 0.0,
                }
            ],
        },
    ]
    storyboard["success_criteria"].setdefault("lead_vehicle_stops", True)
    storyboard["success_criteria"].setdefault("min_stop_gap_m", stop_gap)
    return storyboard


def _compile_static_lead_stop(template: Mapping[str, Any], scene_id: str) -> dict[str, Any]:
    params = dict(template.get("params", {}))
    lead_role = str(params.get("lead_role", "lead_vehicle"))
    hold_s = float(params.get("hold_s", params.get("duration_s", 45.0)))
    storyboard = _base_storyboard(template, scene_id)
    storyboard["storyboard"]["phases"] = [
        {
            "id": "lead_hold_stop",
            "completion_required": False,
            "start": {"type": "simulation_time", "op": ">=", "value": 0.0},
            "actions": [
                {
                    "role": lead_role,
                    "type": "hold_stop",
                    "controller": "brake_to_stop",
                    "duration_s": hold_s,
                    "target_speed_mps": 0.0,
                }
            ],
        }
    ]
    storyboard["success_criteria"].setdefault("lead_vehicle_initial_state", "stopped")
    storyboard["success_criteria"].setdefault("lead_vehicle_stays_stopped", True)
    return storyboard


def _compile_lead_vehicle_accel_decel(template: Mapping[str, Any], scene_id: str) -> dict[str, Any]:
    params = dict(template.get("params", {}))
    lead_role = str(params.get("lead_role", "lead_vehicle"))
    storyboard = _base_storyboard(template, scene_id)
    storyboard["storyboard"]["phases"] = [
        {
            "id": "lead_speed_profile",
            "start": {"type": "simulation_time", "op": ">=", "value": 0.0},
            "actions": [
                {
                    "role": lead_role,
                    "type": "speed_profile",
                    "controller": "speed_profile",
                    "interpolation": str(params.get("speed_profile_interpolation", "step")),
                    "profile": params.get(
                        "speed_profile",
                        [
                            {"t": 0.0, "speed_mps": 0.0},
                            {"t": 5.0, "speed_mps": 12.0},
                            {"t": 15.0, "speed_mps": 4.0},
                        ],
                    ),
                }
            ],
        }
    ]
    storyboard["success_criteria"].setdefault("speed_profile_executed", True)
    return storyboard


def _compile_cut_in(template: Mapping[str, Any], scene_id: str) -> dict[str, Any]:
    params = dict(template.get("params", {}))
    lead_role = str(params.get("lead_role", "lead_vehicle"))
    start_time = float(params.get("lane_change_start_s", 4.0))
    trigger_gap = params.get("lane_change_trigger_gap_m")
    target_lane_offset = int(params.get("target_lane_offset", 1))
    lead_speed = float(params.get("lead_speed_mps", 10.0))
    lane_change_action = {
        "role": lead_role,
        "type": "lane_change",
        "controller": "lane_change",
        "target_lane_offset": target_lane_offset,
        "target_speed_mps": lead_speed,
        "duration_s": float(params.get("lane_change_duration_s", 3.0)),
        "lateral_shift_m": float(params.get("lane_change_lateral_shift_m", 3.6)),
        "easing": str(params.get("lane_change_easing", "cosine")),
        "max_yaw_hint_deg": float(params.get("lane_change_max_yaw_hint_deg", 5.0)),
        "lane_change_runtime_mode": "set_transform_interpolation",
        "physics_controlled_lane_change": False,
        "claim_grade_lane_change": False,
        "velocity_source": "carla_get_velocity",
        "trigger_frame": "ego_body" if trigger_gap is not None else None,
    }
    if "lane_change_direction" in params:
        lane_change_action["direction"] = str(params["lane_change_direction"])
    storyboard = _base_storyboard(template, scene_id)
    storyboard["storyboard"]["phases"] = [
        {
            "id": "adjacent_lane_prepare",
            "start": {"type": "simulation_time", "op": ">=", "value": 0.0},
            "actions": [
                {
                    "role": lead_role,
                    "type": "maintain_speed",
                    "controller": "speed_profile",
                    "target_speed_mps": lead_speed,
                }
            ],
        },
        {
            "id": "cut_in_lane_change",
            "start": (
                {
                    "type": "relative_longitudinal_distance",
                    "from_role": "ego",
                    "to_role": lead_role,
                    "frame": "ego_body",
                    "op": "<=",
                    "value_m": float(trigger_gap),
                }
                if trigger_gap is not None
                else {"type": "simulation_time", "op": ">=", "value": start_time}
            ),
            "actions": [lane_change_action],
        },
    ]
    storyboard["success_criteria"].setdefault("lane_change_completed", True)
    return storyboard


def _compile_cut_out(template: Mapping[str, Any], scene_id: str) -> dict[str, Any]:
    params = dict(template.get("params", {}))
    lead_role = str(params.get("lead_role", "lead_vehicle"))
    start_time = float(params.get("lane_change_start_s", 5.0))
    target_lane_offset = int(params.get("target_lane_offset", -1))
    storyboard = _base_storyboard(template, scene_id)
    storyboard["storyboard"]["phases"] = [
        {
            "id": "lead_ahead_prepare",
            "start": {"type": "simulation_time", "op": ">=", "value": 0.0},
            "actions": [
                {
                    "role": lead_role,
                    "type": "gap_control",
                    "controller": "gap_controller",
                    "target_gap_m": float(params.get("target_gap_m", 25.0)),
                }
            ],
        },
        {
            "id": "cut_out_lane_change",
            "start": {"type": "simulation_time", "op": ">=", "value": start_time},
            "actions": [
                {
                    "role": lead_role,
                    "type": "lane_change",
                    "controller": "lane_change",
                    "target_lane_offset": target_lane_offset,
                    "duration_s": float(params.get("lane_change_duration_s", 3.0)),
                    "lateral_shift_m": float(params.get("lane_change_lateral_shift_m", 3.6)),
                    "easing": str(params.get("lane_change_easing", "cosine")),
                    "lane_change_runtime_mode": "set_transform_interpolation",
                    "physics_controlled_lane_change": False,
                    "claim_grade_lane_change": False,
                    "velocity_source": "carla_get_velocity",
                }
            ],
        },
    ]
    storyboard["success_criteria"].setdefault("lead_vehicle_exits_ego_lane", True)
    return storyboard


def _stable_hash(data: Mapping[str, Any]) -> str:
    blob = json.dumps(data, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
