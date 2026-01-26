from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple, Union

import carla


class Ros2NativePublisher:
    """
    Lightweight helper that enables CARLA's native ROS2 publishing on already-spawned actors.
    No rclpy dependency is required; the CARLA server handles publishing once enable_for_ros() is called.
    """

    def __init__(
        self,
        world: carla.World,
        traffic_manager: Optional[carla.TrafficManager],
        ego_vehicle: Optional[carla.Vehicle],
        rig_spec: Union[Iterable, Dict, object],
        ego_id: str = "hero",
        invert_tf: bool = True,
    ):
        self.world = world
        self.traffic_manager = traffic_manager
        self.ego_vehicle = ego_vehicle
        self.rig_spec = rig_spec
        self.ego_id = ego_id or "hero"
        self.invert_tf = invert_tf
        self._tm_sync_original: Optional[bool] = None
        self._enabled_ids: List[str] = []

    def _collect_sensor_ids(self) -> List[str]:
        ids: List[str] = []
        if hasattr(self.rig_spec, "entries"):
            for entry in getattr(self.rig_spec, "entries", []):
                spec = entry.get("spec")
                if spec and getattr(spec, "enabled", True):
                    ids.append(getattr(spec, "sensor_id", None))
        elif isinstance(self.rig_spec, dict):
            for sensor in self.rig_spec.get("sensors", []):
                if sensor.get("enabled", True):
                    ids.append(sensor.get("id"))
        else:
            for spec in self.rig_spec or []:
                if getattr(spec, "enabled", True):
                    ids.append(getattr(spec, "sensor_id", None) or getattr(spec, "id", None))
        return [sid for sid in ids if sid]

    def _iter_sensor_actors(self) -> List[Tuple[str, carla.Actor]]:
        ids = set(self._collect_sensor_ids())
        if hasattr(self.rig_spec, "entries"):
            out = []
            for entry in getattr(self.rig_spec, "entries", []):
                spec = entry.get("spec")
                actor = entry.get("actor")
                sid = getattr(spec, "sensor_id", None) if spec else None
                if actor is not None:
                    if ids and sid not in ids:
                        continue
                    out.append((sid or str(actor.id), actor))
            return out

        actors = []
        try:
            candidates = self.world.get_actors().filter("sensor.*")
        except Exception:
            candidates = []
        for actor in candidates:
            attrs = getattr(actor, "attributes", {}) or {}
            role = attrs.get("role_name") or attrs.get("ros_name")
            if ids and role not in ids:
                continue
            parent = getattr(actor, "parent", None)
            if parent is not None and self.ego_vehicle is not None:
                try:
                    if parent.id != self.ego_vehicle.id:
                        continue
                except Exception:
                    pass
            actors.append((role or str(actor.id), actor))
        return actors

    def setup_publishers(self) -> int:
        print("[ROS2 native] ensure CARLA server started with --ros2; server will publish /carla/<ego>/<sensor>/...")
        if self.traffic_manager is not None:
            try:
                self._tm_sync_original = self.traffic_manager.get_synchronous_mode()
            except Exception:
                self._tm_sync_original = None
            try:
                self.traffic_manager.set_synchronous_mode(True)
            except Exception as exc:
                print(f"[WARN] failed to set traffic manager synchronous mode: {exc}")

        enabled = 0
        for sid, actor in self._iter_sensor_actors():
            try:
                actor.enable_for_ros()
                enabled += 1
            except Exception as exc:
                print(f"[WARN] enable_for_ros failed for {sid}: {exc}")
            try:
                tr = actor.get_transform()
                print(
                    f"[ROS2 native] {sid} -> "
                    f"loc({tr.location.x:.2f},{tr.location.y:.2f},{tr.location.z:.2f}) "
                    f"rpy({tr.rotation.roll:.2f},{tr.rotation.pitch:.2f},{tr.rotation.yaw:.2f})"
                )
            except Exception:
                pass
        try:
            ego_attrs = getattr(self.ego_vehicle, "attributes", {}) or {}
            ego_role = ego_attrs.get("role_name", self.ego_id)
            print(f"[ROS2 native] ego role_name={ego_role}, ros_name={ego_attrs.get('ros_name', ego_role)}")
        except Exception:
            pass

        self._enabled_ids = self._collect_sensor_ids()
        return enabled

    def teardown(self):
        if self.traffic_manager is not None and self._tm_sync_original is not None:
            try:
                self.traffic_manager.set_synchronous_mode(self._tm_sync_original)
            except Exception as exc:
                print(f"[WARN] failed to restore traffic manager sync mode: {exc}")
