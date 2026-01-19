from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import carla


@dataclass
class WorldHandle:
    world: carla.World
    original_settings: Optional[carla.WorldSettings] = None

    def spawn_actor(self, *args, **kwargs):
        return self.world.spawn_actor(*args, **kwargs)

    def destroy_actors(self, actors: Iterable[carla.Actor]):
        for a in actors:
            try:
                a.destroy()
            except Exception:
                pass

    def apply_control(self, vehicle: carla.Vehicle, control: carla.VehicleControl):
        vehicle.apply_control(control)
