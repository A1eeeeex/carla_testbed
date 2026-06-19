from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass
class ScenarioActorState:
    role: str
    actor_id: str | int | None = None
    x: float | None = None
    y: float | None = None
    z: float | None = None
    yaw_rad: float | None = None
    speed_mps: float | None = None
    length_m: float | None = None
    width_m: float | None = None
    height_m: float | None = None
    bbox_extent_x_m: float | None = None
    bbox_extent_y_m: float | None = None
    bbox_extent_z_m: float | None = None
    route_s: float | None = None
    lane_id: str | None = None
    trajectory_progress_m: float | None = None
    route_progress_gap_m: float | None = None
    route_progress_gap_source: str | None = None
    applied_control: Mapping[str, Any] | None = None

    @classmethod
    def from_mapping(cls, role: str, data: Mapping[str, Any]) -> "ScenarioActorState":
        return cls(
            role=role,
            actor_id=data.get("actor_id", data.get("id")),
            x=_optional_float(data.get("x")),
            y=_optional_float(data.get("y")),
            z=_optional_float(data.get("z")),
            yaw_rad=_optional_float(data.get("yaw_rad", data.get("yaw"))),
            speed_mps=_optional_float(data.get("speed_mps", data.get("actual_speed_mps"))),
            length_m=_optional_float(data.get("length_m", data.get("actor_length_m"))),
            width_m=_optional_float(data.get("width_m", data.get("actor_width_m"))),
            height_m=_optional_float(data.get("height_m", data.get("actor_height_m"))),
            bbox_extent_x_m=_optional_float(data.get("bbox_extent_x_m")),
            bbox_extent_y_m=_optional_float(data.get("bbox_extent_y_m")),
            bbox_extent_z_m=_optional_float(data.get("bbox_extent_z_m")),
            route_s=_optional_float(data.get("route_s")),
            lane_id=_optional_text(data.get("lane_id")),
            trajectory_progress_m=_optional_float(data.get("trajectory_progress_m")),
            route_progress_gap_m=_optional_float(data.get("route_progress_gap_m")),
            route_progress_gap_source=_optional_text(data.get("route_progress_gap_source")),
            applied_control=data.get("applied_control") if isinstance(data.get("applied_control"), Mapping) else None,
        )


class ScenarioActorRegistry:
    def __init__(self) -> None:
        self._states: dict[str, ScenarioActorState] = {}

    def update(self, states: Mapping[str, ScenarioActorState | Mapping[str, Any]]) -> None:
        for role, state in states.items():
            self._states[role] = (
                state
                if isinstance(state, ScenarioActorState)
                else ScenarioActorState.from_mapping(role, state)
            )

    def get(self, role: str) -> ScenarioActorState | None:
        return self._states.get(role)

    def roles(self) -> list[str]:
        return sorted(self._states)

    def distance(self, role_a: str, role_b: str) -> float | None:
        a = self.get(role_a)
        b = self.get(role_b)
        if a is None or b is None or a.x is None or a.y is None or b.x is None or b.y is None:
            return None
        return math.hypot(a.x - b.x, a.y - b.y)

    def trigger_distance(self, from_role: str, to_role: str) -> float | None:
        target = self.get(to_role)
        if from_role == "ego" and target is not None and target.route_progress_gap_m is not None:
            return target.route_progress_gap_m
        return self.distance(from_role, to_role)

    def relative_longitudinal_lateral(self, from_role: str, to_role: str) -> tuple[float, float] | None:
        source = self.get(from_role)
        target = self.get(to_role)
        if (
            source is None
            or target is None
            or source.x is None
            or source.y is None
            or source.yaw_rad is None
            or target.x is None
            or target.y is None
        ):
            return None
        dx = target.x - source.x
        dy = target.y - source.y
        longitudinal = dx * math.cos(source.yaw_rad) + dy * math.sin(source.yaw_rad)
        lateral = -dx * math.sin(source.yaw_rad) + dy * math.cos(source.yaw_rad)
        return longitudinal, lateral


def _optional_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)
