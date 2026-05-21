from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any, Dict, Optional, Sequence

import carla

from carla_testbed.sim import spawn_with_retry
from .base import ActorRefs, Scenario


@dataclass
class CalibrationOnlyConfig:
    spawn_idx: int = 120
    strict_spawn: bool = True
    ego_id: str = "hero"
    vehicle_blueprint_id: str = ""
    vehicle_blueprint_patterns: Sequence[str] = (
        "vehicle.lincoln.mkz_2020",
        "vehicle.lincoln.mkz_2017",
        "vehicle.lincoln.mkz*",
        "vehicle.tesla.model3",
    )
    force_green_traffic_lights: bool = False
    freeze_traffic_lights: bool = True
    ego_offset_x_m: float = 0.0
    ego_offset_y_m: float = 0.0
    ego_offset_z_m: float = 0.0
    ego_yaw_offset_deg: float = 0.0


class CalibrationOnlyScenario(Scenario):
    """Single-ego, fixed-spawn scene for open-loop actuator calibration."""

    def __init__(self, cfg: CalibrationOnlyConfig):
        self.cfg = cfg
        self.actors: Optional[ActorRefs] = None
        self._selected_meta: Dict[str, Any] = {}

    def metadata(self) -> Dict[str, Any]:
        return dict(self._selected_meta)

    def _clear_dynamic_actors(self, world: carla.World) -> None:
        removed = 0
        for actor in world.get_actors():
            type_id = getattr(actor, "type_id", "") or ""
            if not (
                type_id.startswith("vehicle.")
                or type_id.startswith("walker.")
                or type_id.startswith("controller.ai.walker")
            ):
                continue
            try:
                actor.destroy()
                removed += 1
            except Exception:
                continue
        if removed:
            self._selected_meta["cleared_dynamic_actors"] = int(removed)

    def _select_blueprint(self, bp_lib: carla.BlueprintLibrary):
        explicit_id = str(self.cfg.vehicle_blueprint_id or "").strip()
        if explicit_id:
            cands = bp_lib.filter(explicit_id)
            if cands:
                return cands[0]
        for pattern in self.cfg.vehicle_blueprint_patterns:
            cands = bp_lib.filter(str(pattern))
            if cands:
                return cands[0]
        raise RuntimeError("No suitable vehicle blueprint found for calibration-only scene")

    def _apply_traffic_light_policy(self, world: carla.World) -> None:
        if not self.cfg.force_green_traffic_lights:
            return
        changed = 0
        for actor in world.get_actors().filter("traffic.traffic_light*"):
            try:
                if hasattr(actor, "set_state"):
                    actor.set_state(carla.TrafficLightState.Green)
                if self.cfg.freeze_traffic_lights and hasattr(actor, "freeze"):
                    actor.freeze(True)
                changed += 1
            except Exception:
                continue
        self._selected_meta["traffic_lights_overridden"] = int(changed)

    @staticmethod
    def _offset_transform(
        base_tf: carla.Transform,
        *,
        offset_x_m: float,
        offset_y_m: float,
        offset_z_m: float,
        yaw_offset_deg: float,
    ) -> carla.Transform:
        yaw_rad = math.radians(float(base_tf.rotation.yaw))
        hx = math.cos(yaw_rad)
        hy = math.sin(yaw_rad)
        dx = (float(offset_x_m) * hx) - (float(offset_y_m) * hy)
        dy = (float(offset_x_m) * hy) + (float(offset_y_m) * hx)
        return carla.Transform(
            carla.Location(
                x=float(base_tf.location.x) + dx,
                y=float(base_tf.location.y) + dy,
                z=float(base_tf.location.z) + float(offset_z_m),
            ),
            carla.Rotation(
                pitch=float(base_tf.rotation.pitch),
                yaw=float(base_tf.rotation.yaw) + float(yaw_offset_deg),
                roll=float(base_tf.rotation.roll),
            ),
        )

    @staticmethod
    def _stable_actor_transform(
        actor: carla.Actor,
        *,
        max_attempts: int = 12,
        fallback_transform: Optional[carla.Transform] = None,
    ) -> carla.Transform:
        best = actor.get_transform()
        world = None
        sync_mode = False
        try:
            world = actor.get_world()
            sync_mode = bool(world.get_settings().synchronous_mode)
        except Exception:
            world = None
            sync_mode = False
        for _ in range(max(1, int(max_attempts))):
            tr = actor.get_transform()
            loc = tr.location
            if (abs(float(loc.x)) + abs(float(loc.y))) > 1e-3:
                return tr
            best = tr
            try:
                if sync_mode and world is not None:
                    world.tick()
                else:
                    time.sleep(0.03)
            except Exception:
                time.sleep(0.03)
        if fallback_transform is not None:
            loc = fallback_transform.location
            if (abs(float(loc.x)) + abs(float(loc.y))) > 1e-3:
                return fallback_transform
        return best

    def build(self, world: carla.World, carla_map: carla.Map, bp_lib: carla.BlueprintLibrary):
        self._clear_dynamic_actors(world)
        self._apply_traffic_light_policy(world)
        spawns = list(carla_map.get_spawn_points())
        if not spawns:
            raise RuntimeError("No spawn points available for calibration-only scene")
        requested_idx = int(self.cfg.spawn_idx)
        if requested_idx < 0 or requested_idx >= len(spawns):
            raise RuntimeError(
                f"Calibration spawn_idx out of range: requested={requested_idx} valid=0..{len(spawns)-1}"
            )
        veh_bp = self._select_blueprint(bp_lib)
        for key in ["role_name", "ros_name"]:
            try:
                veh_bp.set_attribute(key, self.cfg.ego_id)
            except Exception:
                pass
        if self.cfg.strict_spawn:
            try:
                ego = world.spawn_actor(veh_bp, spawns[requested_idx])
                used_idx = requested_idx
            except Exception as exc:
                raise RuntimeError(
                    f"Calibration strict spawn failed at idx={requested_idx}: {exc}"
                ) from exc
        else:
            ego, used_idx = spawn_with_retry(world, veh_bp, spawns, preferred_idx=requested_idx)
            if ego is None:
                raise RuntimeError("Calibration fallback spawn failed")
        base_tf = self._stable_actor_transform(
            ego,
            fallback_transform=spawns[used_idx],
        )
        target_tf = self._offset_transform(
            base_tf,
            offset_x_m=self.cfg.ego_offset_x_m,
            offset_y_m=self.cfg.ego_offset_y_m,
            offset_z_m=self.cfg.ego_offset_z_m,
            yaw_offset_deg=self.cfg.ego_yaw_offset_deg,
        )
        ego.set_transform(target_tf)
        ego.set_simulate_physics(True)
        stable_tf = self._stable_actor_transform(
            ego,
            fallback_transform=target_tf,
        )
        self._selected_meta = {
            "scene_type": "calibration_only_straight",
            "requested_spawn_idx": int(requested_idx),
            "used_spawn_idx": int(used_idx),
            "strict_spawn": bool(self.cfg.strict_spawn),
            "spawn": {
                "x": float(stable_tf.location.x),
                "y": float(stable_tf.location.y),
                "z": float(stable_tf.location.z),
                "yaw_deg": float(stable_tf.rotation.yaw),
            },
            "vehicle_blueprint_id": str(getattr(veh_bp, "id", "") or ""),
            "front_gap_m": None,
            "lead_profile": {"mode": "none"},
        }
        self.actors = ActorRefs(ego=ego, front=None)
        return self.actors

    def reset(self):
        pass

    def destroy(self):
        if self.actors and self.actors.ego is not None:
            try:
                self.actors.ego.destroy()
            except Exception:
                pass
        self.actors = None
