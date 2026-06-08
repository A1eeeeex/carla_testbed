from __future__ import annotations

import operator
from typing import Any, Mapping, Set

from carla_testbed.scenario_player.actor_registry import ScenarioActorRegistry

_OPS = {
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    ">=": operator.ge,
    ">": operator.gt,
}


def evaluate_trigger(
    trigger: Mapping[str, Any] | None,
    *,
    sim_time_sec: float,
    world_frame: int,
    actors: ScenarioActorRegistry,
    completed_phases: Set[str] | None = None,
) -> bool:
    if not trigger:
        return True
    trigger = _normalize_trigger(trigger)
    trigger_type = trigger.get("type")
    completed_phases = completed_phases or set()
    if trigger_type == "all":
        return all(
            evaluate_trigger(
                child,
                sim_time_sec=sim_time_sec,
                world_frame=world_frame,
                actors=actors,
                completed_phases=completed_phases,
            )
            for child in trigger.get("conditions", [])
        )
    if trigger_type == "any":
        return any(
            evaluate_trigger(
                child,
                sim_time_sec=sim_time_sec,
                world_frame=world_frame,
                actors=actors,
                completed_phases=completed_phases,
            )
            for child in trigger.get("conditions", [])
        )
    if trigger_type == "simulation_time":
        op, value = _op_value(
            trigger,
            default_op=">=",
            aliases={"gte_s": ">=", "lte_s": "<=", "gt_s": ">", "lt_s": "<"},
        )
        return _compare(sim_time_sec, op, value)
    if trigger_type == "world_frame":
        op, value = _op_value(
            trigger,
            default_op=">=",
            aliases={"gte": ">=", "lte": "<=", "gt": ">", "lt": "<"},
        )
        return _compare(world_frame, op, value)
    if trigger_type == "phase_completed":
        return str(trigger.get("phase")) in completed_phases
    if trigger_type == "relative_distance":
        from_role = str(trigger.get("from_role", trigger.get("from", "ego")))
        to_role = str(trigger.get("to_role", trigger.get("to", "lead_vehicle")))
        distance = actors.distance(from_role, to_role)
        op, value = _op_value(
            trigger,
            default_op="<=",
            aliases={"lte_m": "<=", "gte_m": ">=", "lt_m": "<", "gt_m": ">"},
            value_keys=("value_m", "value"),
        )
        return False if distance is None else _compare(distance, op, value)
    if trigger_type == "actor_speed":
        actor = actors.get(str(trigger.get("role", trigger.get("actor"))))
        op, value = _op_value(
            trigger,
            default_op="<=",
            aliases={"lte_mps": "<=", "gte_mps": ">=", "lt_mps": "<", "gt_mps": ">"},
            value_keys=("value_mps", "value"),
        )
        return False if actor is None or actor.speed_mps is None else _compare(actor.speed_mps, op, value)
    if trigger_type in {"actor_route_s", "ego_route_s", "route_s"}:
        role = "ego" if trigger_type == "ego_route_s" else str(trigger.get("role", trigger.get("actor", "ego")))
        actor = actors.get(role)
        op, value = _op_value(
            trigger,
            default_op=">=",
            aliases={"gte_m": ">=", "lte_m": "<=", "gt_m": ">", "lt_m": "<"},
            value_keys=("value_m", "value"),
        )
        return False if actor is None or actor.route_s is None else _compare(actor.route_s, op, value)
    return False


def _normalize_trigger(trigger: Mapping[str, Any]) -> Mapping[str, Any]:
    if "all" in trigger:
        return {"type": "all", "conditions": trigger.get("all")}
    if "any" in trigger:
        return {"type": "any", "conditions": trigger.get("any")}
    return trigger


def _op_value(
    trigger: Mapping[str, Any],
    *,
    default_op: str,
    aliases: Mapping[str, str],
    value_keys: tuple[str, ...] = ("value",),
) -> tuple[str, Any]:
    for key, op in aliases.items():
        if key in trigger:
            return op, trigger.get(key)
    for key in value_keys:
        if key in trigger:
            return str(trigger.get("op", default_op)), trigger.get(key)
    return str(trigger.get("op", default_op)), trigger.get("value")


def _compare(actual: float, op_name: Any, expected: Any) -> bool:
    op = _OPS.get(str(op_name), operator.ge)
    try:
        return bool(op(float(actual), float(expected)))
    except (TypeError, ValueError):
        return False
