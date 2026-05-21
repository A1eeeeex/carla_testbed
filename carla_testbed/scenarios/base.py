from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

import carla


class Scenario(Protocol):
    def build(self, world: carla.World, carla_map: carla.Map, bp_lib: carla.BlueprintLibrary):
        ...

    def reset(self):
        ...

    def destroy(self):
        ...


@dataclass
class ActorRefs:
    ego: carla.Vehicle
    front: Optional[carla.Vehicle]
