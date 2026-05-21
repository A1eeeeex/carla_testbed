from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional, Sequence

import carla

from carla_testbed.sim import spawn_with_retry
from .base import ActorRefs, Scenario


@dataclass
class FollowStopConfig:
    front_idx: int = 210
    ego_idx: int = 208
    stop_brake: float = 1.0
    ego_id: str = "hero"
    front_id: str = "front"
    vehicle_blueprint_id: str = ""
    vehicle_blueprint_patterns: Sequence[str] = (
        "vehicle.lincoln.mkz_2020",
        "vehicle.lincoln.mkz_2017",
        "vehicle.lincoln.mkz*",
        "vehicle.tesla.model3",
    )
    auto_align_front_spawn: bool = True
    front_min_ahead_m: float = 20.0
    front_max_ahead_m: float = 80.0
    front_max_lateral_m: float = 3.0
    front_max_heading_diff_deg: float = 25.0
    force_green_traffic_lights: bool = False
    freeze_traffic_lights: bool = True
    ego_offset_x_m: float = 0.0
    ego_offset_y_m: float = 0.0
    ego_offset_z_m: float = 0.0
    ego_yaw_offset_deg: float = 0.0
    front_offset_x_m: float = 0.0
    front_offset_y_m: float = 0.0
    front_offset_z_m: float = 0.0
    front_yaw_offset_deg: float = 0.0


class FollowStopScenario(Scenario):
    """Minimal follow-stop scene: spawn ego + front; front is held with brake."""

    def __init__(self, cfg: FollowStopConfig):
        self.cfg = cfg
        self.actors: Optional[ActorRefs] = None

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
            print(f"[followstop] cleared {removed} pre-existing dynamic actors before spawning scene")

    @staticmethod
    def _wrap_deg(delta_deg: float) -> float:
        v = (delta_deg + 180.0) % 360.0 - 180.0
        return v

    def _pick_front_spawn_idx(self, spawns: list[carla.Transform]) -> int:
        if not spawns:
            return self.cfg.front_idx
        ego_idx = self.cfg.ego_idx
        ego_tf = spawns[ego_idx]
        yaw = math.radians(float(ego_tf.rotation.yaw))
        hx = math.cos(yaw)
        hy = math.sin(yaw)
        best_idx = self.cfg.front_idx
        best_score = float("inf")
        for idx, tf in enumerate(spawns):
            if idx == ego_idx:
                continue
            dx = float(tf.location.x - ego_tf.location.x)
            dy = float(tf.location.y - ego_tf.location.y)
            lon = (dx * hx) + (dy * hy)
            lat = (-dx * hy) + (dy * hx)
            if lon < self.cfg.front_min_ahead_m or lon > self.cfg.front_max_ahead_m:
                continue
            if abs(lat) > self.cfg.front_max_lateral_m:
                continue
            yaw_diff = abs(self._wrap_deg(float(tf.rotation.yaw - ego_tf.rotation.yaw)))
            if yaw_diff > self.cfg.front_max_heading_diff_deg:
                continue
            # Prefer the nearest valid front spawn to keep follow-stop behavior stable.
            score = lon + (abs(lat) * 5.0) + yaw_diff
            if score < best_score:
                best_score = score
                best_idx = idx
        return best_idx

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
        print(
            "[followstop] traffic lights override: "
            f"force_green={self.cfg.force_green_traffic_lights} "
            f"freeze={self.cfg.freeze_traffic_lights} affected={changed}"
        )

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
        lx = float(offset_x_m)
        ly = float(offset_y_m)
        dx = (lx * hx) - (ly * hy)
        dy = (lx * hy) + (ly * hx)
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

    def _maybe_apply_actor_offset(
        self,
        actor: carla.Vehicle,
        *,
        label: str,
        offset_x_m: float,
        offset_y_m: float,
        offset_z_m: float,
        yaw_offset_deg: float,
    ) -> None:
        if (
            abs(float(offset_x_m)) <= 1e-6
            and abs(float(offset_y_m)) <= 1e-6
            and abs(float(offset_z_m)) <= 1e-6
            and abs(float(yaw_offset_deg)) <= 1e-6
        ):
            return
        try:
            base_tf = actor.get_transform()
            target_tf = self._offset_transform(
                base_tf,
                offset_x_m=float(offset_x_m),
                offset_y_m=float(offset_y_m),
                offset_z_m=float(offset_z_m),
                yaw_offset_deg=float(yaw_offset_deg),
            )
            actor.set_transform(target_tf)
            print(
                "[followstop] applied %s pose offset: dx=%.3f dy=%.3f dz=%.3f dyaw=%.3f"
                % (label, offset_x_m, offset_y_m, offset_z_m, yaw_offset_deg)
            )
        except Exception as exc:
            print(f"[followstop][warn] failed to apply {label} pose offset: {exc}")

    def build(self, world: carla.World, carla_map: carla.Map, bp_lib: carla.BlueprintLibrary):
        self._clear_dynamic_actors(world)
        self._apply_traffic_light_policy(world)
        spawns = carla_map.get_spawn_points()
        veh_bp = None
        explicit_id = str(self.cfg.vehicle_blueprint_id or "").strip()
        if explicit_id:
            cands = bp_lib.filter(explicit_id)
            if cands:
                veh_bp = cands[0]
                print(f"[followstop] selected explicit vehicle blueprint: {veh_bp.id}")
        if veh_bp is None:
            for pattern in self.cfg.vehicle_blueprint_patterns:
                cands = bp_lib.filter(str(pattern))
                if cands:
                    veh_bp = cands[0]
                    print(f"[followstop] selected vehicle blueprint: {veh_bp.id}")
                    break
        if veh_bp is None:
            raise RuntimeError("No suitable vehicle blueprint found (MKZ/Model3)")

        def _safe_idx(idx: int) -> int:
            if not spawns:
                return 0
            if idx < 0 or idx >= len(spawns):
                print(f"[WARN] preferred spawn idx {idx} out of range (0-{len(spawns)-1}), clamping.")
                return max(0, min(idx, len(spawns) - 1))
            return idx

        self.cfg.front_idx = _safe_idx(self.cfg.front_idx)
        self.cfg.ego_idx = _safe_idx(self.cfg.ego_idx)
        if self.cfg.auto_align_front_spawn:
            aligned_front_idx = self._pick_front_spawn_idx(spawns)
            if aligned_front_idx != self.cfg.front_idx:
                print(
                    f"[followstop] auto-aligned front spawn: requested={self.cfg.front_idx} -> aligned={aligned_front_idx}"
                )
            self.cfg.front_idx = aligned_front_idx

        try:
            veh_bp.set_attribute("role_name", self.cfg.front_id)
        except Exception:
            pass
        try:
            veh_bp.set_attribute("ros_name", self.cfg.front_id)
        except Exception:
            pass

        requested_front_idx = self.cfg.front_idx
        front, front_idx = spawn_with_retry(world, veh_bp, spawns, preferred_idx=requested_front_idx)
        if front is None:
            raise RuntimeError("Failed to spawn front vehicle")
        if front_idx != requested_front_idx:
            print(
                f"[followstop][warn] front spawn fallback: requested={requested_front_idx} used={front_idx}"
            )
        self._maybe_apply_actor_offset(
            front,
            label="front",
            offset_x_m=self.cfg.front_offset_x_m,
            offset_y_m=self.cfg.front_offset_y_m,
            offset_z_m=self.cfg.front_offset_z_m,
            yaw_offset_deg=self.cfg.front_yaw_offset_deg,
        )
        front.set_simulate_physics(True)
        front.apply_control(carla.VehicleControl(throttle=0.0, brake=self.cfg.stop_brake, hand_brake=True))

        try:
            veh_bp.set_attribute("role_name", self.cfg.ego_id)
        except Exception:
            pass
        try:
            veh_bp.set_attribute("ros_name", self.cfg.ego_id)
        except Exception:
            pass

        requested_ego_idx = self.cfg.ego_idx
        ego, ego_idx = spawn_with_retry(world, veh_bp, spawns, preferred_idx=requested_ego_idx)
        if ego is None:
            front.destroy()
            raise RuntimeError("Failed to spawn ego vehicle")
        if ego_idx != requested_ego_idx:
            print(
                f"[followstop][warn] ego spawn fallback: requested={requested_ego_idx} used={ego_idx}"
            )
        self._maybe_apply_actor_offset(
            ego,
            label="ego",
            offset_x_m=self.cfg.ego_offset_x_m,
            offset_y_m=self.cfg.ego_offset_y_m,
            offset_z_m=self.cfg.ego_offset_z_m,
            yaw_offset_deg=self.cfg.ego_yaw_offset_deg,
        )
        ego.set_simulate_physics(True)
        # Re-apply once after actor spawn to reduce race with map controller updates.
        self._apply_traffic_light_policy(world)

        self.cfg.front_idx = front_idx
        self.cfg.ego_idx = ego_idx
        self.actors = ActorRefs(ego=ego, front=front)
        return self.actors

    def reset(self):
        # placeholder for future use (traffic reset, etc.)
        pass

    def destroy(self):
        if self.actors:
            for a in [self.actors.ego, self.actors.front]:
                try:
                    a.destroy()
                except Exception:
                    pass
            self.actors = None
