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
    route_s: float | None = None
    lane_id: str | None = None
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
            route_s=_optional_float(data.get("route_s")),
            lane_id=_optional_text(data.get("lane_id")),
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
