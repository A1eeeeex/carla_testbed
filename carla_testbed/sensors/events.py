from __future__ import annotations

from typing import List

import carla


class CollisionEventSource:
    def __init__(self, world: carla.World, parent: carla.Actor):
        self.world = world
        self.parent = parent
        self.sensor = None
        self.events: List[carla.CollisionEvent] = []

    def start(self):
        bp = self.world.get_blueprint_library().find("sensor.other.collision")
        self.sensor = self.world.spawn_actor(bp, carla.Transform(), attach_to=self.parent)
        self.sensor.listen(self._on_event)

    def _on_event(self, event: carla.CollisionEvent):
        self.events.append(event)

    def fetch_and_clear(self) -> List[carla.CollisionEvent]:
        evs = list(self.events)
        self.events.clear()
        return evs

    def stop(self):
        if self.sensor is None:
            return
        try:
            self.sensor.stop()
        except Exception:
            pass
        try:
            self.sensor.destroy()
        except Exception:
            pass
        self.sensor = None


class LaneInvasionEventSource:
    def __init__(self, world: carla.World, parent: carla.Actor):
        self.world = world
        self.parent = parent
        self.sensor = None
        self.events: List[carla.LaneInvasionEvent] = []

    def start(self):
        bp = self.world.get_blueprint_library().find("sensor.other.lane_invasion")
        self.sensor = self.world.spawn_actor(bp, carla.Transform(), attach_to=self.parent)
        self.sensor.listen(self._on_event)

    def _on_event(self, event: carla.LaneInvasionEvent):
        self.events.append(event)

    def fetch_and_clear(self) -> List[carla.LaneInvasionEvent]:
        evs = list(self.events)
        self.events.clear()
        return evs

    def stop(self):
        if self.sensor is None:
            return
        try:
            self.sensor.stop()
        except Exception:
            pass
        try:
            self.sensor.destroy()
        except Exception:
            pass
        self.sensor = None
