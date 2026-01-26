from __future__ import annotations

from typing import List

import carla


class CollisionEventSource:
    def __init__(self, world: carla.World, parent: carla.Actor):
        self.world = world
        self.parent = parent
        self.sensor = None
        self.events: List[carla.CollisionEvent] = []

    def start(self, enable_ros: bool = False, name: str = "collision"):
        bp = self.world.get_blueprint_library().find("sensor.other.collision")
        if enable_ros:
            for attr in ["ros_name", "role_name"]:
                try:
                    bp.set_attribute(attr, name)
                except Exception:
                    pass
        self.sensor = self.world.spawn_actor(bp, carla.Transform(), attach_to=self.parent)
        if enable_ros:
            try:
                self.sensor.enable_for_ros()
            except Exception:
                pass
        if enable_ros:
            try:
                tr = self.sensor.get_transform()
                print(
                    f"[SensorRig] spawn {name} enable_ros={enable_ros} "
                    f"transform=({tr.location.x:.2f},{tr.location.y:.2f},{tr.location.z:.2f}) "
                    f"rpy=({tr.rotation.roll:.2f},{tr.rotation.pitch:.2f},{tr.rotation.yaw:.2f})"
                )
            except Exception:
                pass
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

    def start(self, enable_ros: bool = False, name: str = "lane_invasion"):
        bp = self.world.get_blueprint_library().find("sensor.other.lane_invasion")
        if enable_ros:
            for attr in ["ros_name", "role_name"]:
                try:
                    bp.set_attribute(attr, name)
                except Exception:
                    pass
        self.sensor = self.world.spawn_actor(bp, carla.Transform(), attach_to=self.parent)
        if enable_ros:
            try:
                self.sensor.enable_for_ros()
            except Exception:
                pass
        if enable_ros:
            try:
                tr = self.sensor.get_transform()
                print(
                    f"[SensorRig] spawn {name} enable_ros={enable_ros} "
                    f"transform=({tr.location.x:.2f},{tr.location.y:.2f},{tr.location.z:.2f}) "
                    f"rpy=({tr.rotation.roll:.2f},{tr.rotation.pitch:.2f},{tr.rotation.yaw:.2f})"
                )
            except Exception:
                pass
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
