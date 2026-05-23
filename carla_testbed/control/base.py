from __future__ import annotations

from typing import Protocol

from carla_testbed.contracts import ControlCommand, EgoState, FrameStamp, SceneTruth


class Controller(Protocol):
    name: str

    def reset(self) -> None:
        ...

    def step(self, frame: FrameStamp, ego: EgoState, scene: SceneTruth) -> ControlCommand:
        ...
