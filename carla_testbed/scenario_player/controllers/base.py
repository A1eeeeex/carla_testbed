from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass
class ControllerInput:
    sim_time_sec: float
    world_frame: int
    role: str
    actor_state: Mapping[str, Any]
    action: Mapping[str, Any]


@dataclass
class ControllerOutput:
    role: str
    controller: str
    target_speed_mps: float | None = None
    target_lane_offset: int | None = None
    target_gap_m: float | None = None
    debug: dict[str, Any] = field(default_factory=dict)


class FixedSceneController:
    controller_name = "base"

    def step(self, control_input: ControllerInput) -> ControllerOutput:
        return ControllerOutput(role=control_input.role, controller=self.controller_name)
