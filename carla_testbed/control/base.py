from __future__ import annotations

from typing import Protocol

import carla

from carla_testbed.schemas import ControlCommand


class Controller(Protocol):
    name: str

    def reset(self):
        ...

    def step(
        self,
        t: float,
        dt: float,
        world: carla.World,
        carla_map: carla.Map,
        ego: carla.Vehicle,
        front: carla.Vehicle,
    ) -> ControlCommand:
        ...
