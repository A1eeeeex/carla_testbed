from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.actions import action_controller_name, action_target_speed_mps
from carla_testbed.scenario_player.actor_registry import ScenarioActorState


class JsonlWriter:
    def __init__(self, path: str | Path | None) -> None:
        self.path = Path(path).expanduser() if path else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, payload: Mapping[str, Any]) -> None:
        if self.path is None:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")


class ScenarioActorTraceWriter(JsonlWriter):
    pass


class PhaseEventWriter(JsonlWriter):
    pass


def build_actor_trace_row(
    *,
    scene_id: str,
    sim_time_sec: float,
    world_frame: int,
    phase: str | None,
    actor: ScenarioActorState,
    action: Mapping[str, Any] | None,
    distance_to_ego_m: float | None,
    longitudinal_to_ego_m: float | None = None,
    lateral_to_ego_m: float | None = None,
    ttc_s: float | None = None,
    lane_change_progress: float | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "scenario_actor_trace.v1",
        "sim_time_sec": sim_time_sec,
        "world_frame": world_frame,
        "scene_id": scene_id,
        "phase": phase,
        "actor_role": actor.role,
        "actor_id": actor.actor_id,
        "control_source": "fixed_scene_player",
        "controller": action_controller_name(action),
        "action_type": action.get("type") if action else None,
        "target_speed_mps": action_target_speed_mps(action, sim_time_sec=sim_time_sec),
        "actual_speed_mps": actor.speed_mps,
        "route_s": actor.route_s,
        "current_lane_id": actor.lane_id,
        "lane_id": actor.lane_id,
        "target_lane_id": _target_lane_id(action),
        "lane_change_progress": lane_change_progress,
        "x": actor.x,
        "y": actor.y,
        "z": actor.z,
        "yaw": actor.yaw_rad,
        "yaw_rad": actor.yaw_rad,
        "distance_to_ego_m": distance_to_ego_m,
        "longitudinal_to_ego_m": longitudinal_to_ego_m,
        "lateral_to_ego_m": lateral_to_ego_m,
        "ttc_s": ttc_s,
        "applied_control": dict(actor.applied_control) if actor.applied_control else None,
    }


def build_phase_event(
    *,
    scene_id: str,
    phase: str,
    event: str,
    sim_time_sec: float,
    world_frame: int,
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "scenario_phase_event.v1",
        "scene_id": scene_id,
        "phase": phase,
        "event": event,
        "sim_time_sec": sim_time_sec,
        "world_frame": world_frame,
        "reason": reason,
    }


def _target_lane_id(action: Mapping[str, Any] | None) -> str | None:
    if not action:
        return None
    for key in ("target_lane_id", "target_lane", "target"):
        if action.get(key) not in {None, ""}:
            return str(action[key])
    if action.get("target_lane_offset") is not None:
        return f"offset:{action['target_lane_offset']}"
    return None
