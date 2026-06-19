from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional, Sequence

import carla

from carla_testbed.sim import spawn_with_retry
from .base import ActorRefs, Scenario
from .followstop_geometry import (
    relative_front_alignment,
    select_aligned_front_spawn_index,
    select_waypoint_ahead_transform,
    wrap_deg,
)


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
    require_aligned_front_spawn: bool = False
    front_min_ahead_m: float = 20.0
    front_max_ahead_m: float = 80.0
    front_target_ahead_m: float | None = None
    front_max_lateral_m: float = 3.0
    front_max_heading_diff_deg: float = 25.0
    force_green_traffic_lights: bool = False
    freeze_traffic_lights: bool = True
    ego_offset_x_m: float = 0.0
    ego_offset_y_m: float = 0.0
    ego_offset_z_m: float = 0.0
    ego_yaw_offset_deg: float = 0.0
    ego_spawn_s_offset_m: float = 0.0
    front_offset_x_m: float = 0.0
    front_offset_y_m: float = 0.0
    front_offset_z_m: float = 0.0
    front_yaw_offset_deg: float = 0.0
    front_placement_mode: str = "spawn_index"
    front_waypoint_ahead_m: float | None = None
    ego_initial_brake: float = 0.0
    ego_initial_hand_brake: bool = False


class FollowStopScenario(Scenario):
    """Minimal follow-stop scene: spawn ego + front; front is held with brake."""

    def __init__(self, cfg: FollowStopConfig):
        self.cfg = cfg
        self.actors: Optional[ActorRefs] = None
        self._initial_metadata: dict | None = None

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
        return wrap_deg(delta_deg)

    def _select_front_spawn(self, spawns: list[carla.Transform]):
        return select_aligned_front_spawn_index(
            spawns,
            ego_idx=self.cfg.ego_idx,
            requested_front_idx=self.cfg.front_idx,
            min_ahead_m=float(self.cfg.front_min_ahead_m),
            max_ahead_m=float(self.cfg.front_max_ahead_m),
            max_lateral_m=float(self.cfg.front_max_lateral_m),
            max_heading_diff_deg=float(self.cfg.front_max_heading_diff_deg),
            target_ahead_m=self.cfg.front_target_ahead_m,
        )

    def _validate_or_select_front_spawn(self, spawns: list[carla.Transform]) -> None:
        selection = self._select_front_spawn(spawns)
        if self.cfg.auto_align_front_spawn and selection.found:
            if selection.index != self.cfg.front_idx:
                print(
                    f"[followstop] auto-aligned front spawn: requested={self.cfg.front_idx} -> aligned={selection.index}"
                )
            self.cfg.front_idx = selection.index
            return

        alignment = selection.requested_alignment
        if self.cfg.require_aligned_front_spawn and (alignment is None or not alignment.aligned):
            reasons = list(alignment.reasons) if alignment else ["front_alignment_unavailable"]
            raise RuntimeError(
                "Follow-stop front spawn is not aligned with ego: "
                f"ego_idx={self.cfg.ego_idx} front_idx={self.cfg.front_idx} "
                f"reasons={reasons} "
                f"relative={{'longitudinal_m': {getattr(alignment, 'longitudinal_m', None)}, "
                f"'lateral_m': {getattr(alignment, 'lateral_m', None)}, "
                f"'heading_diff_deg': {getattr(alignment, 'heading_diff_deg', None)}}}"
            )

        if self.cfg.auto_align_front_spawn and not selection.found:
            print(
                "[followstop][warn] no aligned front spawn found; "
                f"keeping requested={self.cfg.front_idx} candidates={selection.candidate_count}"
            )

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

    @classmethod
    def _offset_spawn_points(
        cls,
        spawns: list[carla.Transform],
        *,
        offset_x_m: float,
        offset_y_m: float,
        offset_z_m: float,
        yaw_offset_deg: float,
    ) -> list[carla.Transform]:
        if (
            abs(float(offset_x_m)) <= 1e-6
            and abs(float(offset_y_m)) <= 1e-6
            and abs(float(offset_z_m)) <= 1e-6
            and abs(float(yaw_offset_deg)) <= 1e-6
        ):
            return spawns
        return [
            cls._offset_transform(
                tf,
                offset_x_m=offset_x_m,
                offset_y_m=offset_y_m,
                offset_z_m=offset_z_m,
                yaw_offset_deg=yaw_offset_deg,
            )
            for tf in spawns
        ]

    def _ego_spawn_points(
        self,
        carla_map: carla.Map,
        spawns: list[carla.Transform],
    ) -> list[carla.Transform]:
        s_offset_m = float(self.cfg.ego_spawn_s_offset_m or 0.0)
        if s_offset_m < -1e-6:
            raise RuntimeError(
                "Follow-stop ego_spawn_s_offset_m currently supports non-negative "
                f"route-ahead offsets only: {s_offset_m}"
            )
        if s_offset_m > 1e-6:
            base_tf = spawns[self.cfg.ego_idx]
            shifted_tf = self._waypoint_ahead_transform(
                carla_map,
                base_tf,
                s_offset_m,
                label="ego_spawn_s_offset",
            )
            return [
                self._offset_transform(
                    tf,
                    offset_x_m=0.0,
                    offset_y_m=0.0,
                    offset_z_m=0.0,
                    yaw_offset_deg=0.0,
                )
                if idx != self.cfg.ego_idx
                else self._offset_transform(
                    shifted_tf,
                    offset_x_m=float(self.cfg.ego_offset_x_m),
                    offset_y_m=float(self.cfg.ego_offset_y_m),
                    offset_z_m=float(self.cfg.ego_offset_z_m),
                    yaw_offset_deg=float(self.cfg.ego_yaw_offset_deg),
                )
                for idx, tf in enumerate(spawns)
            ]
        return self._offset_spawn_points(
            spawns,
            offset_x_m=float(self.cfg.ego_offset_x_m),
            offset_y_m=float(self.cfg.ego_offset_y_m),
            offset_z_m=float(self.cfg.ego_offset_z_m),
            yaw_offset_deg=float(self.cfg.ego_yaw_offset_deg),
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
        front_placement_mode = str(self.cfg.front_placement_mode or "spawn_index").strip()
        if front_placement_mode == "spawn_index":
            self._validate_or_select_front_spawn(spawns)
        elif front_placement_mode != "waypoint_ahead":
            raise RuntimeError(f"Unsupported follow-stop front_placement_mode={front_placement_mode!r}")

        requested_ego_idx = self.cfg.ego_idx
        ego_spawns = self._ego_spawn_points(carla_map, spawns)
        ego_spawn_for_front = ego_spawns[requested_ego_idx]

        try:
            veh_bp.set_attribute("role_name", self.cfg.front_id)
        except Exception:
            pass
        try:
            veh_bp.set_attribute("ros_name", self.cfg.front_id)
        except Exception:
            pass

        requested_front_idx = self.cfg.front_idx
        if front_placement_mode == "waypoint_ahead":
            front_spawns = [
                self._front_waypoint_ahead_transform(
                    carla_map,
                    ego_spawn_for_front,
                    float(self.cfg.front_waypoint_ahead_m or self.cfg.front_target_ahead_m or 300.0),
                )
            ]
            requested_front_idx = 0
        else:
            front_spawns = self._offset_spawn_points(
                spawns,
                offset_x_m=float(self.cfg.front_offset_x_m),
                offset_y_m=float(self.cfg.front_offset_y_m),
                offset_z_m=float(self.cfg.front_offset_z_m),
                yaw_offset_deg=float(self.cfg.front_yaw_offset_deg),
            )
        front, front_idx = spawn_with_retry(world, veh_bp, front_spawns, preferred_idx=requested_front_idx)
        if front is None:
            raise RuntimeError("Failed to spawn front vehicle")
        if front_idx != requested_front_idx:
            print(
                f"[followstop][warn] front spawn fallback: requested={requested_front_idx} used={front_idx}"
            )
        if front_placement_mode == "waypoint_ahead":
            print(
                "[followstop] placed front by waypoint_ahead: ego_idx=%s ahead_m=%.3f"
                % (
                    self.cfg.ego_idx,
                    float(self.cfg.front_waypoint_ahead_m or self.cfg.front_target_ahead_m or 300.0),
                )
            )
        elif front_spawns is not spawns:
            print(
                "[followstop] applied front pre-spawn offset: dx=%.3f dy=%.3f dz=%.3f dyaw=%.3f"
                % (
                    self.cfg.front_offset_x_m,
                    self.cfg.front_offset_y_m,
                    self.cfg.front_offset_z_m,
                    self.cfg.front_yaw_offset_deg,
                )
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

        ego, ego_idx = spawn_with_retry(
            world,
            veh_bp,
            ego_spawns,
            preferred_idx=requested_ego_idx,
            max_tries=1 if abs(float(self.cfg.ego_spawn_s_offset_m or 0.0)) > 1e-6 else 5,
        )
        if ego is None:
            front.destroy()
            raise RuntimeError("Failed to spawn ego vehicle")
        if ego_idx != requested_ego_idx:
            print(
                f"[followstop][warn] ego spawn fallback: requested={requested_ego_idx} used={ego_idx}"
            )
        if ego_spawns is not spawns:
            print(
                "[followstop] applied ego pre-spawn offset: s=%.3f dx=%.3f dy=%.3f dz=%.3f dyaw=%.3f"
                % (
                    self.cfg.ego_spawn_s_offset_m,
                    self.cfg.ego_offset_x_m,
                    self.cfg.ego_offset_y_m,
                    self.cfg.ego_offset_z_m,
                    self.cfg.ego_yaw_offset_deg,
                )
            )
        ego.set_simulate_physics(True)
        if float(self.cfg.ego_initial_brake) > 0.0 or bool(self.cfg.ego_initial_hand_brake):
            ego.apply_control(
                carla.VehicleControl(
                    throttle=0.0,
                    brake=max(0.0, min(1.0, float(self.cfg.ego_initial_brake))),
                    steer=0.0,
                    hand_brake=bool(self.cfg.ego_initial_hand_brake),
                )
            )
            print(
                "[followstop] applied ego initial hold: brake=%.3f hand_brake=%s"
                % (float(self.cfg.ego_initial_brake), bool(self.cfg.ego_initial_hand_brake))
            )
        # Re-apply once after actor spawn to reduce race with map controller updates.
        self._apply_traffic_light_policy(world)

        self.cfg.front_idx = front_idx if front_placement_mode == "spawn_index" else self.cfg.front_idx
        self.cfg.ego_idx = ego_idx
        self.actors = ActorRefs(ego=ego, front=front)
        return self.actors

    def reset(self):
        # placeholder for future use (traffic reset, etc.)
        pass

    def metadata(self):
        if self._initial_metadata is not None:
            return dict(self._initial_metadata)

        self._initial_metadata = self._build_metadata()
        return dict(self._initial_metadata)

    def _build_metadata(self):
        payload = {
            "scenario_driver": "carla_followstop",
            "front_idx": int(self.cfg.front_idx),
            "ego_idx": int(self.cfg.ego_idx),
            "auto_align_front_spawn": bool(self.cfg.auto_align_front_spawn),
            "require_aligned_front_spawn": bool(self.cfg.require_aligned_front_spawn),
            "front_alignment_thresholds": {
                "min_ahead_m": float(self.cfg.front_min_ahead_m),
                "max_ahead_m": float(self.cfg.front_max_ahead_m),
                "target_ahead_m": self.cfg.front_target_ahead_m,
                "max_lateral_m": float(self.cfg.front_max_lateral_m),
                "max_heading_diff_deg": float(self.cfg.front_max_heading_diff_deg),
            },
            "front_placement_mode": str(self.cfg.front_placement_mode or "spawn_index"),
            "front_waypoint_ahead_m": self.cfg.front_waypoint_ahead_m,
            "ego_spawn_s_offset_m": float(self.cfg.ego_spawn_s_offset_m),
            "ego_initial_hold": {
                "brake": float(self.cfg.ego_initial_brake),
                "hand_brake": bool(self.cfg.ego_initial_hand_brake),
            },
        }
        if not self.actors:
            return payload

        def _tf(actor: carla.Actor):
            try:
                tr = actor.get_transform()
                return {
                    "x": float(tr.location.x),
                    "y": float(tr.location.y),
                    "z": float(tr.location.z),
                    "yaw_deg": float(tr.rotation.yaw),
                }
            except Exception:
                return None

        ego_tf = _tf(self.actors.ego)
        front_tf = _tf(self.actors.front)
        if ego_tf is not None:
            payload["spawn"] = ego_tf
        if front_tf is not None:
            payload["front_spawn"] = front_tf
        if ego_tf is not None and front_tf is not None:
            class _Location:
                def __init__(self, data):
                    self.x = data.get("x")
                    self.y = data.get("y")

            class _Rotation:
                def __init__(self, data):
                    self.yaw = data.get("yaw_deg")

            class _Transform:
                def __init__(self, data):
                    self.location = _Location(data)
                    self.rotation = _Rotation(data)

            alignment = relative_front_alignment(
                _Transform(ego_tf),
                _Transform(front_tf),
                min_ahead_m=float(self.cfg.front_min_ahead_m),
                max_ahead_m=float(self.cfg.front_max_ahead_m),
                max_lateral_m=float(self.cfg.front_max_lateral_m),
                max_heading_diff_deg=float(self.cfg.front_max_heading_diff_deg),
            )
            payload["front_alignment"] = {
                "aligned": alignment.aligned,
                "reasons": list(alignment.reasons),
                "longitudinal_m": alignment.longitudinal_m,
                "lateral_m": alignment.lateral_m,
                "euclidean_m": alignment.euclidean_m,
                "heading_diff_deg": alignment.heading_diff_deg,
            }
        return payload

    def _front_waypoint_ahead_transform(
        self,
        carla_map: carla.Map,
        ego_tf: carla.Transform,
        ahead_m: float,
    ) -> carla.Transform:
        selected_tf = self._waypoint_ahead_transform(
            carla_map,
            ego_tf,
            ahead_m,
            label="front_waypoint_ahead",
        )
        return self._offset_transform(
            selected_tf,
            offset_x_m=float(self.cfg.front_offset_x_m),
            offset_y_m=float(self.cfg.front_offset_y_m),
            offset_z_m=float(self.cfg.front_offset_z_m),
            yaw_offset_deg=float(self.cfg.front_yaw_offset_deg),
        )

    def _waypoint_ahead_transform(
        self,
        carla_map: carla.Map,
        base_tf: carla.Transform,
        ahead_m: float,
        *,
        label: str,
    ) -> carla.Transform:
        try:
            start_wp = carla_map.get_waypoint(
                base_tf.location,
                project_to_road=True,
                lane_type=carla.LaneType.Driving,
            )
        except TypeError:
            start_wp = carla_map.get_waypoint(base_tf.location)
        selection = select_waypoint_ahead_transform(start_wp, float(ahead_m))
        if not selection.found or selection.selected_transform is None:
            raise RuntimeError(
                f"Failed to resolve {label} by waypoint_ahead: "
                f"ahead_m={ahead_m} reason={selection.reason}"
            )
        selected_tf = selection.selected_transform
        start_tf = getattr(start_wp, "transform", None)
        z_lift = 0.0
        try:
            # Waypoint locations can sit on the road surface, while CARLA spawn
            # points are lifted enough for vehicle collision boxes. Reuse the
            # base spawn lift so RoadRunner maps do not reject the actor.
            z_lift = float(base_tf.location.z) - float(start_tf.location.z)
        except Exception:
            z_lift = 0.0
        if abs(z_lift) > 1e-6:
            selected_tf = carla.Transform(
                carla.Location(
                    x=float(selected_tf.location.x),
                    y=float(selected_tf.location.y),
                    z=float(selected_tf.location.z) + z_lift,
                ),
                carla.Rotation(
                    pitch=float(selected_tf.rotation.pitch),
                    yaw=float(selected_tf.rotation.yaw),
                    roll=float(selected_tf.rotation.roll),
                ),
            )
        return selected_tf

    def destroy(self):
        if self.actors:
            for a in [self.actors.ego, self.actors.front]:
                try:
                    a.destroy()
                except Exception:
                    pass
            self.actors = None
