from __future__ import annotations

from dataclasses import dataclass, field
import math
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import carla

from .base import ActorRefs, Scenario


def _wrap_deg(angle_deg: float) -> float:
    return ((angle_deg + 180.0) % 360.0) - 180.0


def _lane_transform(tf: carla.Transform, *, z_offset: float = 0.3) -> carla.Transform:
    return carla.Transform(
        carla.Location(
            x=float(tf.location.x),
            y=float(tf.location.y),
            z=float(tf.location.z) + float(z_offset),
        ),
        carla.Rotation(
            pitch=float(tf.rotation.pitch),
            yaw=float(tf.rotation.yaw),
            roll=float(tf.rotation.roll),
        ),
    )


@dataclass
class SemanticLeadProfile:
    mode: str = "static_stop"
    static_brake: float = 1.0
    hand_brake: bool = True
    cruise_speed_mps: float = 6.0
    hold_before_move_sec: float = 2.0
    cruise_hold_sec: float = 3.0
    stop_hold_sec: float = 2.0
    throttle_cap: float = 0.55
    brake_cap: float = 0.65


@dataclass
class ApolloSemanticSuiteConfig:
    scene_type: str = "lateral_straight_track"
    role_ego: str = "hero"
    role_front: str = "front"
    vehicle_blueprint_id: str = ""
    vehicle_blueprint_patterns: Sequence[str] = (
        "vehicle.lincoln.mkz_2020",
        "vehicle.lincoln.mkz_2017",
        "vehicle.lincoln.mkz*",
        "vehicle.tesla.model3",
    )
    waypoint_spacing_m: float = 3.0
    preview_distance_m: float = 40.0
    curve_min_yaw_delta_deg: float = 12.0
    straight_max_yaw_delta_deg: float = 4.0
    ego_front_gap_m: float = 38.0
    far_front_gap_m: float = 120.0
    scene_length_ahead_m: float = 120.0
    allow_junction: bool = False
    preferred_road_ids: Sequence[int] = ()
    preferred_section_ids: Sequence[int] = ()
    preferred_lane_ids: Sequence[int] = ()
    min_start_y: Optional[float] = None
    max_start_y: Optional[float] = None
    candidate_sorted_index: Optional[int] = None
    ego_offset_x_m: float = 0.0
    ego_offset_y_m: float = 0.0
    ego_offset_z_m: float = 0.0
    ego_yaw_offset_deg: float = 0.0
    front_offset_x_m: float = 0.0
    front_offset_y_m: float = 0.0
    front_offset_z_m: float = 0.0
    front_yaw_offset_deg: float = 0.0
    lead_profile: SemanticLeadProfile = field(default_factory=SemanticLeadProfile)


class ApolloSemanticSuiteScenario(Scenario):
    """Dedicated scenario set for Apollo↔CARLA semantic capture."""

    def __init__(self, cfg: ApolloSemanticSuiteConfig):
        self.cfg = cfg
        self.actors: Optional[ActorRefs] = None
        self._front_profile_started_ts: Optional[float] = None
        self._selected_meta: Dict[str, Any] = {}
        self._candidate_filter_meta: Dict[str, Any] = {}

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
        raise RuntimeError("No suitable vehicle blueprint found for Apollo semantic suite")

    def _scene_gap_m(self) -> float:
        if self.cfg.scene_type in {"longitudinal_launch_cruise", "lateral_straight_track", "lateral_left_curve", "lateral_right_curve"}:
            return float(self.cfg.far_front_gap_m)
        return float(self.cfg.ego_front_gap_m)

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

    @staticmethod
    def _pose_dict(tf: carla.Transform) -> Dict[str, float]:
        return {
            "x": float(tf.location.x),
            "y": float(tf.location.y),
            "z": float(tf.location.z),
            "yaw_deg": float(tf.rotation.yaw),
        }

    def _all_waypoints(self, carla_map: carla.Map) -> List[carla.Waypoint]:
        points = list(carla_map.generate_waypoints(max(1.0, float(self.cfg.waypoint_spacing_m))))
        unique: List[carla.Waypoint] = []
        seen = set()
        for wp in points:
            loc = wp.transform.location
            key = (round(float(loc.x), 1), round(float(loc.y), 1), int(getattr(wp, "road_id", 0)), int(getattr(wp, "lane_id", 0)))
            if key in seen:
                continue
            seen.add(key)
            unique.append(wp)
        return unique

    def _advance_waypoint(self, wp: carla.Waypoint, distance_m: float) -> Optional[carla.Waypoint]:
        current = wp
        remain = max(0.0, float(distance_m))
        step = max(1.0, float(self.cfg.waypoint_spacing_m))
        while remain > 1e-6:
            next_wps = current.next(min(step, remain))
            if not next_wps:
                return None
            current = next_wps[0]
            remain -= min(step, remain)
        return current

    def _classify_waypoint(self, wp: carla.Waypoint) -> Optional[Tuple[str, Dict[str, Any]]]:
        if (not self.cfg.allow_junction) and bool(getattr(wp, "is_junction", False)):
            return None
        ahead = self._advance_waypoint(wp, float(self.cfg.preview_distance_m))
        front = self._advance_waypoint(wp, self._scene_gap_m())
        length_check = self._advance_waypoint(wp, float(self.cfg.scene_length_ahead_m))
        if ahead is None or front is None or length_check is None:
            return None
        yaw_delta = _wrap_deg(float(ahead.transform.rotation.yaw) - float(wp.transform.rotation.yaw))
        info = {
            "yaw_delta_deg": float(yaw_delta),
            "road_id": int(getattr(wp, "road_id", 0)),
            "lane_id": int(getattr(wp, "lane_id", 0)),
            "start_x": float(wp.transform.location.x),
            "start_y": float(wp.transform.location.y),
        }
        if self.cfg.scene_type in {"lateral_straight_track", "longitudinal_launch_cruise", "longitudinal_follow_decel", "longitudinal_stop_and_go"}:
            if abs(float(yaw_delta)) <= float(self.cfg.straight_max_yaw_delta_deg):
                return "straight", info
            return None
        if self.cfg.scene_type == "lateral_left_curve" and float(yaw_delta) >= float(self.cfg.curve_min_yaw_delta_deg):
            return "left_curve", info
        if self.cfg.scene_type == "lateral_right_curve" and float(yaw_delta) <= -float(self.cfg.curve_min_yaw_delta_deg):
            return "right_curve", info
        return None

    def _candidate_waypoints(self, carla_map: carla.Map) -> List[Tuple[carla.Waypoint, Dict[str, Any]]]:
        out: List[Tuple[carla.Waypoint, Dict[str, Any]]] = []
        for wp in self._all_waypoints(carla_map):
            classified = self._classify_waypoint(wp)
            if classified is None:
                continue
            _, info = classified
            out.append((wp, info))
        if self.cfg.scene_type == "lateral_left_curve":
            out.sort(key=lambda item: (-abs(float(item[1]["yaw_delta_deg"])), float(item[1]["start_x"])))
        elif self.cfg.scene_type == "lateral_right_curve":
            out.sort(key=lambda item: (-abs(float(item[1]["yaw_delta_deg"])), float(item[1]["start_x"])))
        else:
            out.sort(key=lambda item: (abs(float(item[1]["yaw_delta_deg"])), float(item[1]["start_x"])))
        for sorted_index, (_, info) in enumerate(out):
            info["sorted_index"] = int(sorted_index)
        return self._apply_candidate_preferences(out)

    @staticmethod
    def _normalize_int_sequence(values: Sequence[int]) -> Tuple[int, ...]:
        out: List[int] = []
        seen = set()
        for raw in values or ():
            try:
                value = int(raw)
            except Exception:
                continue
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return tuple(out)

    def _apply_candidate_preferences(
        self,
        candidates: List[Tuple[carla.Waypoint, Dict[str, Any]]],
    ) -> List[Tuple[carla.Waypoint, Dict[str, Any]]]:
        preferred_road_ids = self._normalize_int_sequence(self.cfg.preferred_road_ids)
        preferred_section_ids = self._normalize_int_sequence(self.cfg.preferred_section_ids)
        preferred_lane_ids = self._normalize_int_sequence(self.cfg.preferred_lane_ids)
        min_start_y = self.cfg.min_start_y
        max_start_y = self.cfg.max_start_y
        candidate_sorted_index = self.cfg.candidate_sorted_index
        filters = {
            "preferred_road_ids": list(preferred_road_ids),
            "preferred_section_ids": list(preferred_section_ids),
            "preferred_lane_ids": list(preferred_lane_ids),
            "min_start_y": None if min_start_y is None else float(min_start_y),
            "max_start_y": None if max_start_y is None else float(max_start_y),
            "candidate_sorted_index": None if candidate_sorted_index is None else int(candidate_sorted_index),
        }
        filtered: List[Tuple[carla.Waypoint, Dict[str, Any]]] = []
        filter_active = bool(
            preferred_road_ids
            or preferred_section_ids
            or preferred_lane_ids
            or min_start_y is not None
            or max_start_y is not None
            or candidate_sorted_index is not None
        )
        for wp, info in candidates:
            road_id = int(info.get("road_id", 0) or 0)
            section_id = int(getattr(wp, "section_id", info.get("section_id", 0)) or 0)
            lane_id = int(info.get("lane_id", 0) or 0)
            start_y = float(info.get("start_y", 0.0) or 0.0)
            sorted_index = int(info.get("sorted_index", -1) or -1)
            if preferred_road_ids and road_id not in preferred_road_ids:
                continue
            if preferred_section_ids and section_id not in preferred_section_ids:
                continue
            if preferred_lane_ids and lane_id not in preferred_lane_ids:
                continue
            if min_start_y is not None and start_y < float(min_start_y):
                continue
            if max_start_y is not None and start_y > float(max_start_y):
                continue
            if candidate_sorted_index is not None and sorted_index != int(candidate_sorted_index):
                continue
            filtered.append((wp, info))
        self._candidate_filter_meta = {
            "candidate_pool_total": int(len(candidates)),
            "candidate_pool_after_filter": int(len(filtered)),
            "candidate_filter": filters,
        }
        return filtered if filter_active else candidates

    def _spawn_actor(
        self,
        world: carla.World,
        blueprint: carla.ActorBlueprint,
        transform: carla.Transform,
        *,
        role_name: str,
    ) -> Optional[carla.Vehicle]:
        try:
            blueprint.set_attribute("role_name", role_name)
        except Exception:
            pass
        try:
            blueprint.set_attribute("ros_name", role_name)
        except Exception:
            pass
        actor = None
        try:
            actor = world.try_spawn_actor(blueprint, transform)
        except AttributeError:
            try:
                actor = world.spawn_actor(blueprint, transform)
            except Exception:
                actor = None
        except Exception:
            actor = None
        return actor if isinstance(actor, carla.Vehicle) else None

    def _spawn_pair_from_waypoint(
        self,
        world: carla.World,
        blueprint: carla.ActorBlueprint,
        wp: carla.Waypoint,
        info: Dict[str, Any],
    ) -> Optional[ActorRefs]:
        gap_m = self._scene_gap_m()
        front_wp = self._advance_waypoint(wp, gap_m)
        goal_wp = self._advance_waypoint(wp, float(self.cfg.scene_length_ahead_m))
        if front_wp is None or goal_wp is None:
            return None
        ego_spawn_tf = _lane_transform(wp.transform)
        front_spawn_tf = _lane_transform(front_wp.transform)
        ego_target_tf = self._offset_transform(
            ego_spawn_tf,
            offset_x_m=self.cfg.ego_offset_x_m,
            offset_y_m=self.cfg.ego_offset_y_m,
            offset_z_m=self.cfg.ego_offset_z_m,
            yaw_offset_deg=self.cfg.ego_yaw_offset_deg,
        )
        front_target_tf = self._offset_transform(
            front_spawn_tf,
            offset_x_m=self.cfg.front_offset_x_m,
            offset_y_m=self.cfg.front_offset_y_m,
            offset_z_m=self.cfg.front_offset_z_m,
            yaw_offset_deg=self.cfg.front_yaw_offset_deg,
        )
        ego = self._spawn_actor(world, blueprint, ego_spawn_tf, role_name=self.cfg.role_ego)
        if ego is None:
            return None
        front = self._spawn_actor(world, blueprint, front_spawn_tf, role_name=self.cfg.role_front)
        if front is None:
            try:
                ego.destroy()
            except Exception:
                pass
            return None
        ego.set_transform(ego_target_tf)
        front.set_transform(front_target_tf)
        ego.set_simulate_physics(True)
        front.set_simulate_physics(True)
        ego_stable_tf = self._stable_actor_transform(ego, fallback_transform=ego_target_tf)
        front_stable_tf = self._stable_actor_transform(front, fallback_transform=front_target_tf)
        goal_tf = _lane_transform(goal_wp.transform)
        self._selected_meta = {
            "scene_type": self.cfg.scene_type,
            "candidate": dict(info),
            "candidate_pool_total": int(self._candidate_filter_meta.get("candidate_pool_total", 0) or 0),
            "candidate_pool_after_filter": int(
                self._candidate_filter_meta.get("candidate_pool_after_filter", 0) or 0
            ),
            "candidate_filter": dict(self._candidate_filter_meta.get("candidate_filter", {}) or {}),
            "spawn": self._pose_dict(ego_stable_tf),
            "front_spawn": self._pose_dict(front_stable_tf),
            "spawn_lane": {
                "road_id": int(getattr(wp, "road_id", 0)),
                "section_id": int(getattr(wp, "section_id", 0)),
                "lane_id": int(getattr(wp, "lane_id", 0)),
            },
            "front_lane": {
                "road_id": int(getattr(front_wp, "road_id", 0)),
                "section_id": int(getattr(front_wp, "section_id", 0)),
                "lane_id": int(getattr(front_wp, "lane_id", 0)),
            },
            "goal": self._pose_dict(goal_tf),
            "front_gap_m": float(gap_m),
            "lead_profile": {
                "mode": self.cfg.lead_profile.mode,
                "cruise_speed_mps": self.cfg.lead_profile.cruise_speed_mps,
                "hold_before_move_sec": self.cfg.lead_profile.hold_before_move_sec,
                "cruise_hold_sec": self.cfg.lead_profile.cruise_hold_sec,
                "stop_hold_sec": self.cfg.lead_profile.stop_hold_sec,
            },
            "scenario_goal_raw_carla": {
                "x": float(goal_tf.location.x),
                "y": float(goal_tf.location.y),
                "z": float(goal_tf.location.z),
            },
        }
        return ActorRefs(ego=ego, front=front)

    def _scene_lead_profile(self) -> SemanticLeadProfile:
        profile = self.cfg.lead_profile
        if self.cfg.scene_type in {"lateral_straight_track", "lateral_left_curve", "lateral_right_curve", "longitudinal_follow_decel"}:
            return SemanticLeadProfile(
                mode="static_stop",
                static_brake=profile.static_brake,
                hand_brake=profile.hand_brake,
            )
        if self.cfg.scene_type == "longitudinal_launch_cruise":
            return SemanticLeadProfile(
                mode="static_stop",
                static_brake=profile.static_brake,
                hand_brake=True,
            )
        if self.cfg.scene_type == "longitudinal_stop_and_go":
            return SemanticLeadProfile(
                mode="stop_and_go",
                static_brake=profile.static_brake,
                hand_brake=False,
                cruise_speed_mps=max(4.0, float(profile.cruise_speed_mps)),
                hold_before_move_sec=max(1.0, float(profile.hold_before_move_sec)),
                cruise_hold_sec=max(2.0, float(profile.cruise_hold_sec)),
                stop_hold_sec=max(1.0, float(profile.stop_hold_sec)),
                throttle_cap=float(profile.throttle_cap),
                brake_cap=float(profile.brake_cap),
            )
        return profile

    def build(self, world: carla.World, carla_map: carla.Map, bp_lib: carla.BlueprintLibrary):
        self._clear_dynamic_actors(world)
        blueprint = self._select_blueprint(bp_lib)
        candidates = self._candidate_waypoints(carla_map)
        if not candidates:
            raise RuntimeError(
                f"No candidate waypoints for semantic scene type={self.cfg.scene_type} "
                f"filters={self._candidate_filter_meta.get('candidate_filter', {})}"
            )
        for wp, info in candidates:
            actors = self._spawn_pair_from_waypoint(world, blueprint, wp, info)
            if actors is not None:
                self.actors = actors
                self._front_profile_started_ts = None
                self._apply_static_front()
                return actors
        raise RuntimeError(f"Failed to spawn semantic scene actors for scene type={self.cfg.scene_type}")

    def _front_speed_mps(self) -> float:
        if self.actors is None:
            return 0.0
        vel = self.actors.front.get_velocity()
        return math.sqrt(float(vel.x) ** 2 + float(vel.y) ** 2 + float(vel.z) ** 2)

    def _apply_static_front(self) -> None:
        if self.actors is None:
            return
        profile = self._scene_lead_profile()
        if profile.mode != "static_stop":
            return
        self.actors.front.apply_control(
            carla.VehicleControl(throttle=0.0, brake=float(profile.static_brake), hand_brake=bool(profile.hand_brake))
        )

    def _control_front_speed(self, target_speed_mps: float) -> None:
        if self.actors is None:
            return
        profile = self._scene_lead_profile()
        speed = self._front_speed_mps()
        err = float(target_speed_mps) - float(speed)
        if err > 0.35:
            throttle = min(float(profile.throttle_cap), 0.18 + (0.08 * err))
            brake = 0.0
        elif err < -0.35:
            throttle = 0.0
            brake = min(float(profile.brake_cap), 0.10 + (0.10 * abs(err)))
        else:
            throttle = 0.05 if target_speed_mps > 0.3 else 0.0
            brake = 0.0 if target_speed_mps > 0.3 else 0.12
        self.actors.front.apply_control(carla.VehicleControl(throttle=throttle, brake=brake, steer=0.0))

    def on_sim_tick(self, *, frame_id: int, timestamp: float, step: int) -> None:
        if self.actors is None:
            return
        profile = self._scene_lead_profile()
        if profile.mode == "static_stop":
            self._apply_static_front()
            return
        if self._front_profile_started_ts is None:
            self._front_profile_started_ts = float(timestamp)
        elapsed = float(timestamp) - float(self._front_profile_started_ts)
        if profile.mode == "stop_and_go":
            if elapsed <= float(profile.hold_before_move_sec):
                target_speed = 0.0
                phase = "hold_before_move"
            elif elapsed <= float(profile.hold_before_move_sec + profile.cruise_hold_sec):
                target_speed = float(profile.cruise_speed_mps)
                phase = "cruise"
            elif elapsed <= float(profile.hold_before_move_sec + profile.cruise_hold_sec + profile.stop_hold_sec):
                span = max(float(profile.stop_hold_sec), 1e-3)
                remain = max(0.0, 1.0 - ((elapsed - profile.hold_before_move_sec - profile.cruise_hold_sec) / span))
                target_speed = float(profile.cruise_speed_mps) * remain
                phase = "decel_to_stop"
            else:
                target_speed = 0.0
                phase = "final_stop"
            self._selected_meta["lead_phase"] = phase
            self._selected_meta["lead_elapsed_sec"] = float(elapsed)
            self._control_front_speed(target_speed)

    def reset(self):
        self._front_profile_started_ts = None

    def destroy(self):
        if self.actors:
            for actor in [self.actors.ego, self.actors.front]:
                try:
                    actor.destroy()
                except Exception:
                    pass
            self.actors = None
