from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import random
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import carla
except ModuleNotFoundError:
    carla = SimpleNamespace(
        TrafficLightState=SimpleNamespace(Red="Red", Yellow="Yellow", Green="Green"),
    )

from .base import ActorRefs, Scenario


def _wrap_deg(delta_deg: float) -> float:
    return (delta_deg + 180.0) % 360.0 - 180.0


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class Town01RouteHealthConfig:
    random_seed: int = 1
    ego_id: str = "hero"
    vehicle_blueprint_id: str = ""
    vehicle_blueprint_patterns: Sequence[str] = (
        "vehicle.lincoln.mkz_2020",
        "vehicle.lincoln.mkz_2017",
        "vehicle.lincoln.mkz*",
        "vehicle.tesla.model3",
    )
    strict_spawn: bool = False
    preferred_spawn_idx: int = -1
    force_green_traffic_lights: bool = False
    freeze_traffic_lights: bool = True
    traffic_light_control_mode: str = ""
    traffic_light_initial_state: str = ""
    traffic_light_release_state: str = ""
    traffic_light_release_after_s: float = 0.0
    traffic_light_target_actor_ids: Sequence[int] = ()
    route_step_m: float = 5.0
    spawn_min_forward_length_m: float = 160.0
    spawn_min_backward_length_m: float = 25.0
    spawn_reject_junction: bool = True
    spawn_junction_window_m: float = 12.0
    spawn_min_successor_count: int = 1
    goal_min_route_length_m: float = 220.0
    goal_max_route_length_m: float = 360.0
    goal_min_end_margin_m: float = 25.0
    goal_reject_junction: bool = True
    max_spawn_candidates: int = 80
    max_goal_attempts: int = 6
    route_id: str = ""
    route_corpus_path: str = ""
    ego_offset_x_m: float = 0.0
    ego_offset_y_m: float = 0.0
    ego_offset_z_m: float = 0.0
    ego_yaw_offset_deg: float = 0.0


class Town01RouteHealthScenario(Scenario):
    """Single-ego random-start cruise scene for Apollo route health validation."""

    def __init__(self, cfg: Town01RouteHealthConfig):
        self.cfg = cfg
        self.actors: Optional[ActorRefs] = None
        self._selected_meta: Dict[str, Any] = {}
        self._traffic_light_policy_meta: Dict[str, Any] = {}
        self._traffic_light_world: Optional[carla.World] = None
        self._traffic_light_started_ts: Optional[float] = None
        self._traffic_light_released: bool = False

    def metadata(self) -> Dict[str, Any]:
        payload = dict(self._selected_meta)
        if self._traffic_light_policy_meta:
            payload["traffic_light_control"] = dict(self._traffic_light_policy_meta)
        return payload

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
        raise RuntimeError("No suitable vehicle blueprint found for town01 route health scene")

    @staticmethod
    def _traffic_light_state_name(state: str) -> str:
        return str(state or "").strip().upper()

    @staticmethod
    def _traffic_light_state_from_name(state: str):
        normalized = Town01RouteHealthScenario._traffic_light_state_name(state)
        traffic_light_state = getattr(carla, "TrafficLightState", None)
        if traffic_light_state is None:
            traffic_light_state = SimpleNamespace(Red="Red", Yellow="Yellow", Green="Green")
            try:
                setattr(carla, "TrafficLightState", traffic_light_state)
            except Exception:
                pass
        mapping = {
            "RED": getattr(traffic_light_state, "Red", None),
            "YELLOW": getattr(traffic_light_state, "Yellow", None),
            "GREEN": getattr(traffic_light_state, "Green", None),
        }
        return mapping.get(normalized)

    def _target_traffic_light_ids(self) -> set[int]:
        ids: set[int] = set()
        for item in self.cfg.traffic_light_target_actor_ids or ():
            try:
                ids.add(int(item))
            except Exception:
                continue
        return ids

    def _iter_target_traffic_lights(self, world: carla.World):
        target_ids = self._target_traffic_light_ids()
        for actor in world.get_actors().filter("traffic.traffic_light*"):
            if target_ids and int(getattr(actor, "id", -1) or -1) not in target_ids:
                continue
            yield actor

    def _apply_traffic_light_state(
        self,
        world: carla.World,
        *,
        state_name: str,
        phase: str,
        frame_id: Optional[int] = None,
        timestamp: Optional[float] = None,
    ) -> int:
        state = self._traffic_light_state_from_name(state_name)
        normalized_state = self._traffic_light_state_name(state_name)
        if state is None:
            warnings = list(self._traffic_light_policy_meta.get("warnings") or [])
            warnings.append(f"unsupported_traffic_light_state:{normalized_state or '<empty>'}")
            self._traffic_light_policy_meta["warnings"] = warnings
            return 0
        changed = 0
        actor_ids: list[int] = []
        for actor in self._iter_target_traffic_lights(world):
            try:
                if hasattr(actor, "set_state"):
                    actor.set_state(state)
                if self.cfg.freeze_traffic_lights and hasattr(actor, "freeze"):
                    actor.freeze(True)
                changed += 1
                try:
                    actor_ids.append(int(getattr(actor, "id", 0) or 0))
                except Exception:
                    pass
            except Exception as exc:
                warnings = list(self._traffic_light_policy_meta.get("warnings") or [])
                warnings.append(f"traffic_light_apply_failed:{type(exc).__name__}:{exc}")
                self._traffic_light_policy_meta["warnings"] = warnings
                continue
        event = {
            "phase": phase,
            "state": normalized_state,
            "affected_count": int(changed),
            "frame_id": frame_id,
            "timestamp": timestamp,
        }
        events = list(self._traffic_light_policy_meta.get("events") or [])
        events.append(event)
        self._traffic_light_policy_meta.update(
            {
                "current_state": normalized_state,
                "current_phase": phase,
                "last_affected_count": int(changed),
                "last_actor_ids": actor_ids,
                "events": events,
            }
        )
        return changed

    def _deterministic_traffic_light_enabled(self) -> bool:
        mode = str(self.cfg.traffic_light_control_mode or "").strip().lower()
        return mode in {"deterministic_gt_control", "deterministic_all_lights"}

    def _apply_traffic_light_policy(self, world: carla.World) -> None:
        self._traffic_light_world = world
        if self._deterministic_traffic_light_enabled():
            initial_state = self._traffic_light_state_name(self.cfg.traffic_light_initial_state or "RED")
            self._traffic_light_policy_meta = {
                "mode": str(self.cfg.traffic_light_control_mode or "").strip(),
                "stimulus_mode": "deterministic_gt_control",
                "scope": "all_traffic_lights"
                if not self._target_traffic_light_ids()
                else "target_actor_ids",
                "target_actor_ids": sorted(self._target_traffic_light_ids()),
                "initial_state": initial_state,
                "release_state": self._traffic_light_state_name(self.cfg.traffic_light_release_state),
                "release_after_s": float(self.cfg.traffic_light_release_after_s or 0.0),
                "freeze": bool(self.cfg.freeze_traffic_lights),
                "warnings": [],
                "events": [],
            }
            changed = self._apply_traffic_light_state(world, state_name=initial_state, phase="initial")
            self._traffic_light_policy_meta["initial_affected_count"] = int(changed)
            self._traffic_light_policy_meta["claim_boundary"] = (
                "CARLA traffic-light actors are controlled deterministically for truth-input "
                "stimulus; behavior claims still require Apollo HDMap signal/stop-line contract "
                "and traffic-light behavior artifacts."
            )
            return
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
        self._traffic_light_policy_meta = {
            "mode": "force_green_legacy",
            "stimulus_mode": "force_green",
            "scope": "all_traffic_lights",
            "initial_state": "GREEN",
            "freeze": bool(self.cfg.freeze_traffic_lights),
            "initial_affected_count": int(changed),
        }
        self._selected_meta["traffic_lights_overridden"] = int(changed)

    def on_sim_tick(self, *, frame_id: int, timestamp: float, step: int) -> None:
        if not self._deterministic_traffic_light_enabled():
            return
        if self._traffic_light_world is None:
            return
        if self._traffic_light_started_ts is None:
            self._traffic_light_started_ts = float(timestamp)
            return
        if self._traffic_light_released:
            return
        release_state = self._traffic_light_state_name(self.cfg.traffic_light_release_state)
        release_after_s = float(self.cfg.traffic_light_release_after_s or 0.0)
        if not release_state or release_after_s <= 0.0:
            return
        elapsed = float(timestamp) - float(self._traffic_light_started_ts)
        self._traffic_light_policy_meta["elapsed_s"] = float(elapsed)
        if elapsed < release_after_s:
            return
        self._apply_traffic_light_state(
            self._traffic_light_world,
            state_name=release_state,
            phase="release",
            frame_id=int(frame_id),
            timestamp=float(timestamp),
        )
        self._traffic_light_policy_meta["release_frame_id"] = int(frame_id)
        self._traffic_light_policy_meta["release_timestamp"] = float(timestamp)
        self._traffic_light_released = True

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

    @staticmethod
    def _lane_dict(wp: carla.Waypoint) -> Dict[str, int]:
        return {
            "road_id": int(wp.road_id),
            "section_id": int(wp.section_id),
            "lane_id": int(wp.lane_id),
        }

    @staticmethod
    def _route_trace_points(trace: Sequence[carla.Waypoint], *, end_index: int) -> List[Dict[str, Any]]:
        points: List[Dict[str, Any]] = []
        total_s = 0.0
        previous_loc = None
        for index, wp in enumerate(list(trace)[: max(0, int(end_index)) + 1]):
            loc = wp.transform.location
            if previous_loc is not None:
                dx = float(loc.x - previous_loc.x)
                dy = float(loc.y - previous_loc.y)
                dz = float(loc.z - previous_loc.z)
                total_s += math.sqrt((dx * dx) + (dy * dy) + (dz * dz))
            tags: List[str] = []
            if bool(getattr(wp, "is_junction", False)):
                tags.append("junction")
            points.append(
                {
                    "index": int(index),
                    "x": float(loc.x),
                    "y": float(loc.y),
                    "z": float(loc.z),
                    "s": float(total_s),
                    "heading": math.radians(float(wp.transform.rotation.yaw)),
                    "lane_id": f"{int(wp.road_id)}:{int(wp.section_id)}:{int(wp.lane_id)}",
                    "tags": tags,
                }
            )
            previous_loc = loc
        return points

    @staticmethod
    def _route_trace_length_from_points(points: Sequence[Mapping[str, Any]]) -> tuple[Optional[float], str]:
        rows = [row for row in points if isinstance(row, Mapping)]
        if len(rows) < 2:
            return None, "route_trace_missing_or_too_short"
        first_s = _safe_float(rows[0].get("s"))
        last_s = _safe_float(rows[-1].get("s"))
        if first_s is not None and last_s is not None:
            return abs(float(last_s) - float(first_s)), "route_trace_s_span"
        total = 0.0
        used_segments = 0
        prev_x = _safe_float(rows[0].get("x"))
        prev_y = _safe_float(rows[0].get("y"))
        prev_z = _safe_float(rows[0].get("z")) or 0.0
        for row in rows[1:]:
            x = _safe_float(row.get("x"))
            y = _safe_float(row.get("y"))
            z = _safe_float(row.get("z")) or 0.0
            if prev_x is not None and prev_y is not None and x is not None and y is not None:
                dx = float(x) - float(prev_x)
                dy = float(y) - float(prev_y)
                dz = float(z) - float(prev_z)
                total += math.sqrt((dx * dx) + (dy * dy) + (dz * dz))
                used_segments += 1
            prev_x = x
            prev_y = y
            prev_z = z
        if used_segments:
            return float(total), "route_trace_xyz_polyline"
        return None, "route_trace_length_unavailable"

    @staticmethod
    def route_id_for(spawn_idx: int, goal_trace_index: int) -> str:
        return f"town01_rh_spawn{int(spawn_idx):03d}_goal{int(goal_trace_index):03d}"

    def _waypoint_trace(
        self,
        start_wp: carla.Waypoint,
        *,
        direction: str,
        max_length_m: float,
        step_m: float,
    ) -> Tuple[List[carla.Waypoint], float]:
        trace: List[carla.Waypoint] = [start_wp]
        total = 0.0
        current = start_wp
        while total < max_length_m:
            try:
                next_candidates = current.next(step_m) if direction == "forward" else current.previous(step_m)
            except Exception:
                break
            candidates = [wp for wp in next_candidates if int(getattr(wp, "lane_type", 0)) == int(carla.LaneType.Driving)]
            if not candidates:
                break
            current_yaw = float(current.transform.rotation.yaw)
            current_road = int(current.road_id)
            current_lane = int(current.lane_id)

            def _score(wp: carla.Waypoint) -> float:
                yaw_diff = abs(_wrap_deg(float(wp.transform.rotation.yaw) - current_yaw))
                road_penalty = 0.0 if int(wp.road_id) == current_road else 10.0
                lane_penalty = 0.0 if int(wp.lane_id) == current_lane else 5.0
                junction_penalty = 20.0 if bool(wp.is_junction) else 0.0
                return yaw_diff + road_penalty + lane_penalty + junction_penalty

            nxt = min(candidates, key=_score)
            if nxt.id == current.id:
                break
            dx = float(nxt.transform.location.x - current.transform.location.x)
            dy = float(nxt.transform.location.y - current.transform.location.y)
            dz = float(nxt.transform.location.z - current.transform.location.z)
            seg_len = math.sqrt((dx * dx) + (dy * dy) + (dz * dz))
            if seg_len <= 1e-3:
                break
            total += seg_len
            trace.append(nxt)
            current = nxt
        return trace, float(total)

    def _has_junction_within(self, wp: carla.Waypoint, distance_m: float, *, direction: str) -> bool:
        if bool(wp.is_junction):
            return True
        trace, _ = self._waypoint_trace(
            wp,
            direction=direction,
            max_length_m=max(0.0, float(distance_m)),
            step_m=max(2.0, float(self.cfg.route_step_m)),
        )
        return any(bool(item.is_junction) for item in trace[1:])

    def _evaluate_spawn_candidate(
        self,
        idx: int,
        tf: carla.Transform,
        carla_map: carla.Map,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        try:
            wp = carla_map.get_waypoint(tf.location, project_to_road=True, lane_type=carla.LaneType.Driving)
        except Exception:
            return None, "waypoint_lookup_failed"
        if wp is None:
            return None, "waypoint_missing"
        if self.cfg.spawn_reject_junction and (
            bool(wp.is_junction)
            or self._has_junction_within(wp, self.cfg.spawn_junction_window_m, direction="forward")
            or self._has_junction_within(wp, self.cfg.spawn_junction_window_m, direction="backward")
        ):
            return None, "spawn_near_junction"
        forward_trace, forward_length = self._waypoint_trace(
            wp,
            direction="forward",
            max_length_m=max(
                self.cfg.spawn_min_forward_length_m,
                self.cfg.goal_max_route_length_m + self.cfg.goal_min_end_margin_m + 20.0,
            ),
            step_m=self.cfg.route_step_m,
        )
        backward_trace, backward_length = self._waypoint_trace(
            wp,
            direction="backward",
            max_length_m=self.cfg.spawn_min_backward_length_m,
            step_m=self.cfg.route_step_m,
        )
        if forward_length < float(self.cfg.spawn_min_forward_length_m):
            return None, "spawn_forward_length_too_short"
        if backward_length < float(self.cfg.spawn_min_backward_length_m):
            return None, "spawn_backward_length_too_short"
        if len(forward_trace) - 1 < int(self.cfg.spawn_min_successor_count):
            return None, "spawn_successor_count_too_low"
        return (
            {
                "spawn_idx": int(idx),
                "waypoint": wp,
                "transform": tf,
                "lane_id": int(wp.lane_id),
                "road_id": int(wp.road_id),
                "section_id": int(wp.section_id),
                "forward_trace": forward_trace,
                "forward_length_m": float(forward_length),
                "backward_length_m": float(backward_length),
                "lane_change": str(getattr(wp, "lane_change", "")),
            },
            "",
        )

    def _build_spawn_pool(
        self,
        spawns: Sequence[carla.Transform],
        carla_map: carla.Map,
        *,
        max_candidates: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        limit = int(max_candidates if max_candidates is not None else self.cfg.max_spawn_candidates)
        for idx, tf in enumerate(spawns):
            candidate, _ = self._evaluate_spawn_candidate(idx, tf, carla_map)
            if candidate is None:
                continue
            candidates.append(candidate)
            if len(candidates) >= limit:
                break
        return candidates

    def _collect_goal_candidates_from_spawn(
        self,
        spawn_item: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        wp = spawn_item["waypoint"]
        forward_trace = list(spawn_item.get("forward_trace") or [])
        forward_length = float(spawn_item.get("forward_length_m") or 0.0)
        if not forward_trace:
            forward_trace, forward_length = self._waypoint_trace(
                wp,
                direction="forward",
                max_length_m=max(
                    self.cfg.goal_max_route_length_m + self.cfg.goal_min_end_margin_m + 20.0,
                    self.cfg.goal_min_route_length_m + 20.0,
                ),
                step_m=self.cfg.route_step_m,
            )
        attempts: List[Dict[str, Any]] = []
        goals: List[Dict[str, Any]] = []
        for idx in range(1, len(forward_trace)):
            cand_wp = forward_trace[idx]
            dx = float(cand_wp.transform.location.x - wp.transform.location.x)
            dy = float(cand_wp.transform.location.y - wp.transform.location.y)
            dz = float(cand_wp.transform.location.z - wp.transform.location.z)
            route_length = math.sqrt((dx * dx) + (dy * dy) + (dz * dz))
            remain_end_margin = max(0.0, float(forward_length) - float(route_length))
            record = {
                "candidate_trace_index": int(idx),
                "candidate_lane_id": int(cand_wp.lane_id),
                "candidate_road_id": int(cand_wp.road_id),
                "candidate_route_length_m": float(route_length),
                "candidate_remain_end_margin_m": float(remain_end_margin),
                "candidate_is_junction": bool(cand_wp.is_junction),
            }
            reject_reason = ""
            if route_length < float(self.cfg.goal_min_route_length_m):
                reject_reason = "route_length_too_short"
            elif route_length > float(self.cfg.goal_max_route_length_m):
                reject_reason = "route_length_too_long"
            elif remain_end_margin < float(self.cfg.goal_min_end_margin_m):
                reject_reason = "goal_too_close_to_route_end"
            elif self.cfg.goal_reject_junction and bool(cand_wp.is_junction):
                reject_reason = "goal_in_junction"
            record["accepted"] = not bool(reject_reason)
            record["reject_reason"] = reject_reason
            attempts.append(record)
            if reject_reason:
                continue
            road_ids = [int(item.road_id) for item in forward_trace[: idx + 1]]
            lane_ids = [int(item.lane_id) for item in forward_trace[: idx + 1]]
            goals.append(
                {
                    "goal_waypoint": cand_wp,
                    "goal_transform": cand_wp.transform,
                    "route_length_m": float(route_length),
                    "forward_available_length_m": float(forward_length),
                    "remain_end_margin_m": float(remain_end_margin),
                    "goal_trace_index": int(idx),
                    "road_transition_count": max(0, len(set(road_ids)) - 1),
                    "lane_transition_count": max(0, len(set(lane_ids)) - 1),
                }
            )
        return goals, attempts

    def _pick_goal_from_spawn(
        self,
        spawn_item: Dict[str, Any],
        rng: random.Random,
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        goals, attempts = self._collect_goal_candidates_from_spawn(spawn_item)
        if not goals:
            return None, attempts
        rng.shuffle(goals)
        selected = goals[0]
        enriched_attempts: List[Dict[str, Any]] = []
        for order, item in enumerate(attempts, start=1):
            enriched = dict(item)
            enriched["attempt_index"] = int(order)
            enriched_attempts.append(enriched)
        return dict(selected), enriched_attempts

    def build_route_corpus(self, carla_map: carla.Map) -> Dict[str, Any]:
        from carla_testbed.utils.town01_route_health import _sync_capability_recommended_uses

        spawns = list(carla_map.get_spawn_points())
        routes: List[Dict[str, Any]] = []
        spawn_rejections: Dict[str, int] = {}
        goal_rejections: Dict[str, int] = {}
        coverage: Dict[str, int] = {}
        recommended_regression: List[str] = []
        recommended_demo: List[str] = []
        recommended_calibration: List[str] = []
        recommended_lateral_smoke: List[Tuple[Tuple[float, int, int, str], str]] = []

        for idx, tf in enumerate(spawns):
            spawn_item, reject_reason = self._evaluate_spawn_candidate(idx, tf, carla_map)
            if spawn_item is None:
                spawn_rejections[reject_reason] = int(spawn_rejections.get(reject_reason, 0)) + 1
                continue
            goals, attempts = self._collect_goal_candidates_from_spawn(spawn_item)
            for attempt in attempts:
                reason = str(attempt.get("reject_reason") or "")
                if reason:
                    goal_rejections[reason] = int(goal_rejections.get(reason, 0)) + 1
            for goal in goals:
                route_id = self.route_id_for(spawn_item["spawn_idx"], goal["goal_trace_index"])
                route_length = float(goal["route_length_m"])
                recommended_uses: List[str] = []
                if 220.0 <= route_length <= 320.0:
                    recommended_uses.append("mainline_regression")
                if 250.0 <= route_length <= 340.0:
                    recommended_uses.append("demo_candidate")
                if 220.0 <= route_length <= 280.0:
                    recommended_uses.append("calibration_compare_candidate")
                if (
                    int(goal.get("road_transition_count") or 0) == 0
                    and int(goal.get("lane_transition_count") or 0) == 0
                    and 220.0 <= route_length <= 250.0
                ):
                    recommended_uses.append("lateral_smoke_candidate")
                if "mainline_regression" in recommended_uses:
                    recommended_regression.append(route_id)
                if "demo_candidate" in recommended_uses:
                    recommended_demo.append(route_id)
                if "calibration_compare_candidate" in recommended_uses:
                    recommended_calibration.append(route_id)
                if "lateral_smoke_candidate" in recommended_uses:
                    recommended_lateral_smoke.append(
                        (
                            (
                                abs(route_length - 230.0),
                                abs(int(spawn_item["lane_id"])),
                                int(spawn_item["road_id"]),
                                route_id,
                            ),
                            route_id,
                        )
                    )
                coverage_key = (
                    f"road{int(spawn_item['road_id'])}_lane{int(spawn_item['lane_id'])}_"
                    f"{'220_260' if route_length < 260.0 else '260_320' if route_length < 320.0 else '320_plus'}"
                )
                coverage[coverage_key] = int(coverage.get(coverage_key, 0)) + 1
                goal_wp = goal["goal_waypoint"]
                signed_heading_delta_deg = (
                    (float(goal["goal_transform"].rotation.yaw) - float(spawn_item["transform"].rotation.yaw) + 180.0)
                    % 360.0
                ) - 180.0
                abs_heading_delta_deg = abs(float(signed_heading_delta_deg))
                route_record = {
                    "route_id": route_id,
                    "spawn_idx": int(spawn_item["spawn_idx"]),
                    "goal_trace_index": int(goal["goal_trace_index"]),
                    "spawn_pose": self._pose_dict(spawn_item["transform"]),
                    "goal_pose": self._pose_dict(goal["goal_transform"]),
                    "spawn_lane": self._lane_dict(spawn_item["waypoint"]),
                    "goal_lane": self._lane_dict(goal_wp),
                    "route_length_m": route_length,
                    "forward_available_length_m": float(goal["forward_available_length_m"]),
                    "remaining_margin_m": float(goal["remain_end_margin_m"]),
                    "road_transition_count": int(goal.get("road_transition_count") or 0),
                    "lane_transition_count": int(goal.get("lane_transition_count") or 0),
                    "goal_heading_delta_deg": float(signed_heading_delta_deg),
                    "goal_abs_heading_delta_deg": float(abs_heading_delta_deg),
                    "health_tags": ["unreviewed"],
                    "recommended_uses": [],
                }
                route_record["recommended_uses"] = _sync_capability_recommended_uses(
                    route_record,
                    recommended_uses,
                )
                routes.append(
                    route_record
                )

        routes.sort(key=lambda item: str(item["route_id"]))
        return {
            "map": "Town01",
            "scene_type": "town01_route_health_random_spawn_cruise",
            "generated_at_unix_sec": time.time(),
            "generation_policy": {
                "spawn_min_forward_length_m": float(self.cfg.spawn_min_forward_length_m),
                "spawn_min_backward_length_m": float(self.cfg.spawn_min_backward_length_m),
                "spawn_junction_window_m": float(self.cfg.spawn_junction_window_m),
                "goal_min_route_length_m": float(self.cfg.goal_min_route_length_m),
                "goal_max_route_length_m": float(self.cfg.goal_max_route_length_m),
                "goal_min_end_margin_m": float(self.cfg.goal_min_end_margin_m),
                "route_step_m": float(self.cfg.route_step_m),
            },
            "route_count": int(len(routes)),
            "routes": routes,
            "excluded_routes": {
                "spawn_rejections": spawn_rejections,
                "goal_rejections": goal_rejections,
            },
            "recommended_subsets": {
                "mainline_regression": recommended_regression[:8],
                "demo_candidate": recommended_demo[:6],
                "calibration_compare_candidate": recommended_calibration[:6],
                "lateral_smoke_candidate": [
                    route_id for _score, route_id in sorted(recommended_lateral_smoke)[:8]
                ],
            },
            "coverage": coverage,
        }

    def _load_route_record_from_corpus(self) -> Optional[Dict[str, Any]]:
        route_id = str(self.cfg.route_id or "").strip()
        corpus_path = str(self.cfg.route_corpus_path or "").strip()
        if not route_id or not corpus_path:
            return None
        path = Path(corpus_path).expanduser().resolve()
        if not path.exists():
            raise RuntimeError(f"Town01 route corpus not found: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Failed to parse Town01 route corpus {path}: {exc}") from exc
        routes = payload.get("routes") or []
        if not isinstance(routes, list):
            raise RuntimeError(f"Invalid Town01 route corpus format: {path}")
        for item in routes:
            if isinstance(item, dict) and str(item.get("route_id") or "") == route_id:
                return dict(item)
        raise RuntimeError(f"Town01 route id not found in corpus: {route_id}")

    def build(self, world: carla.World, carla_map: carla.Map, bp_lib: carla.BlueprintLibrary):
        self._clear_dynamic_actors(world)
        self._apply_traffic_light_policy(world)
        spawns = list(carla_map.get_spawn_points())
        if not spawns:
            raise RuntimeError("No spawn points available for town01 route health scene")

        pool = self._build_spawn_pool(spawns, carla_map)
        if not pool:
            raise RuntimeError("No valid spawn candidates available for town01 route health scene")

        selected_route_record = self._load_route_record_from_corpus()
        selected_spawn: Optional[Dict[str, Any]] = None
        selected_goal: Optional[Dict[str, Any]] = None
        goal_attempt_records: List[Dict[str, Any]] = []
        spawn_attempt_records: List[Dict[str, Any]] = []

        if selected_route_record is not None:
            requested_spawn_idx = int(selected_route_record.get("spawn_idx", -1))
            selected_spawn = next(
                (item for item in pool if int(item["spawn_idx"]) == requested_spawn_idx),
                None,
            )
            if selected_spawn is None:
                if not (0 <= requested_spawn_idx < len(spawns)):
                    raise RuntimeError(
                        f"Town01 route corpus route spawn index out of range for current map: {requested_spawn_idx}"
                    )
                selected_spawn, reject_reason = self._evaluate_spawn_candidate(
                    requested_spawn_idx,
                    spawns[requested_spawn_idx],
                    carla_map,
                )
                if selected_spawn is None:
                    raise RuntimeError(
                        "Town01 route corpus route spawn rejected by current scene filters: "
                        f"{requested_spawn_idx} ({reject_reason or 'unknown'})"
                    )
            goal_pose = selected_route_record.get("goal_pose") or {}
            goal_tf = carla.Transform(
                carla.Location(
                    x=float(goal_pose.get("x", 0.0)),
                    y=float(goal_pose.get("y", 0.0)),
                    z=float(goal_pose.get("z", 0.0)),
                ),
                carla.Rotation(yaw=float(goal_pose.get("yaw_deg", 0.0))),
            )
            goal_wp = carla_map.get_waypoint(goal_tf.location, project_to_road=True, lane_type=carla.LaneType.Driving)
            if goal_wp is None:
                raise RuntimeError(f"Town01 route corpus goal waypoint unavailable for route {self.cfg.route_id}")
            selected_goal = {
                "goal_waypoint": goal_wp,
                "goal_transform": goal_tf,
                "route_length_m": float(selected_route_record.get("route_length_m") or 0.0),
                "forward_available_length_m": float(selected_route_record.get("forward_available_length_m") or 0.0),
                "remain_end_margin_m": float(selected_route_record.get("remaining_margin_m") or 0.0),
                "goal_trace_index": int(selected_route_record.get("goal_trace_index") or 0),
                "road_transition_count": int(selected_route_record.get("road_transition_count") or 0),
                "lane_transition_count": int(selected_route_record.get("lane_transition_count") or 0),
            }
            spawn_attempt_records.append(
                {
                    "spawn_attempt_index": 1,
                    "spawn_idx": int(selected_spawn["spawn_idx"]),
                    "spawn_lane_id": int(selected_spawn["lane_id"]),
                    "spawn_road_id": int(selected_spawn["road_id"]),
                    "forward_length_m": float(selected_spawn["forward_length_m"]),
                    "backward_length_m": float(selected_spawn["backward_length_m"]),
                    "goal_found": True,
                    "route_id": str(selected_route_record.get("route_id") or ""),
                    "route_selected_from_corpus": True,
                    "spawn_selected_outside_sampling_pool": bool(
                        not any(int(item["spawn_idx"]) == requested_spawn_idx for item in pool)
                    ),
                }
            )
        else:
            rng = random.Random(int(self.cfg.random_seed))
            if 0 <= int(self.cfg.preferred_spawn_idx) < len(spawns):
                preferred = [item for item in pool if int(item["spawn_idx"]) == int(self.cfg.preferred_spawn_idx)]
                ordered_pool = preferred + [item for item in pool if item not in preferred]
            else:
                ordered_pool = list(pool)
                rng.shuffle(ordered_pool)

            for spawn_order, item in enumerate(ordered_pool, start=1):
                goal, attempts = self._pick_goal_from_spawn(item, rng)
                spawn_attempt_records.append(
                    {
                        "spawn_attempt_index": int(spawn_order),
                        "spawn_idx": int(item["spawn_idx"]),
                        "spawn_lane_id": int(item["lane_id"]),
                        "spawn_road_id": int(item["road_id"]),
                        "forward_length_m": float(item["forward_length_m"]),
                        "backward_length_m": float(item["backward_length_m"]),
                        "goal_found": bool(goal),
                    }
                )
                for rec in attempts:
                    enriched = dict(rec)
                    enriched["spawn_idx"] = int(item["spawn_idx"])
                    goal_attempt_records.append(enriched)
                if goal is not None:
                    selected_spawn = item
                    selected_goal = goal
                    break

        if selected_spawn is None or selected_goal is None:
            raise RuntimeError("Failed to sample a valid spawn/goal pair for town01 route health scene")

        veh_bp = self._select_blueprint(bp_lib)
        for key in ["role_name", "ros_name"]:
            try:
                veh_bp.set_attribute(key, self.cfg.ego_id)
            except Exception:
                pass

        requested_idx = int(selected_spawn["spawn_idx"])
        if self.cfg.strict_spawn:
            try:
                ego = world.spawn_actor(veh_bp, spawns[requested_idx])
                used_idx = requested_idx
            except Exception as exc:
                raise RuntimeError(
                    f"Town01 route health strict spawn failed at idx={requested_idx}: {exc}"
                ) from exc
        else:
            from carla_testbed.sim import spawn_with_retry

            ego, used_idx = spawn_with_retry(world, veh_bp, spawns, preferred_idx=requested_idx)
            if ego is None:
                raise RuntimeError("Town01 route health spawn_with_retry failed")

        base_tf = self._stable_actor_transform(ego, fallback_transform=spawns[used_idx])
        target_tf = self._offset_transform(
            base_tf,
            offset_x_m=self.cfg.ego_offset_x_m,
            offset_y_m=self.cfg.ego_offset_y_m,
            offset_z_m=self.cfg.ego_offset_z_m,
            yaw_offset_deg=self.cfg.ego_yaw_offset_deg,
        )
        ego.set_transform(target_tf)
        ego.set_simulate_physics(True)
        stable_tf = self._stable_actor_transform(ego, fallback_transform=target_tf)

        goal_tf: carla.Transform = selected_goal["goal_transform"]
        goal_wp: carla.Waypoint = selected_goal["goal_waypoint"]
        route_id = (
            str((selected_route_record or {}).get("route_id") or "")
            or self.route_id_for(int(selected_spawn["spawn_idx"]), int(selected_goal["goal_trace_index"]))
        )
        route_trace = self._route_trace_points(
            list(selected_spawn.get("forward_trace") or []),
            end_index=int(selected_goal["goal_trace_index"]),
        )
        route_trace_length_m, route_trace_length_source = self._route_trace_length_from_points(route_trace)
        legacy_route_length_m = float(selected_goal["route_length_m"])
        claim_route_length_m = float(route_trace_length_m) if route_trace_length_m is not None else legacy_route_length_m
        self._selected_meta = {
            "scene_type": "town01_route_health_random_spawn_cruise",
            "route_id": route_id,
            "map": "Town01",
            "route_selected_from_corpus": bool(selected_route_record is not None),
            "route_corpus_path": str(self.cfg.route_corpus_path or ""),
            "random_seed": int(self.cfg.random_seed),
            "spawn_pool_size": int(len(pool)),
            "spawn_sampling_policy": {
                "reject_junction": bool(self.cfg.spawn_reject_junction),
                "junction_window_m": float(self.cfg.spawn_junction_window_m),
                "min_forward_length_m": float(self.cfg.spawn_min_forward_length_m),
                "min_backward_length_m": float(self.cfg.spawn_min_backward_length_m),
                "min_successor_count": int(self.cfg.spawn_min_successor_count),
            },
            "goal_generation_policy": {
                "min_route_length_m": float(self.cfg.goal_min_route_length_m),
                "max_route_length_m": float(self.cfg.goal_max_route_length_m),
                "min_end_margin_m": float(self.cfg.goal_min_end_margin_m),
                "reject_junction": bool(self.cfg.goal_reject_junction),
                "route_step_m": float(self.cfg.route_step_m),
            },
            "spawn_attempts": spawn_attempt_records,
            "goal_selection_attempts": goal_attempt_records,
            "requested_spawn_idx": int(requested_idx),
            "used_spawn_idx": int(used_idx),
            "strict_spawn": bool(self.cfg.strict_spawn),
            "spawn": self._pose_dict(stable_tf),
            "spawn_lane": {
                "road_id": int(selected_spawn["road_id"]),
                "section_id": int(selected_spawn["section_id"]),
                "lane_id": int(selected_spawn["lane_id"]),
            },
            "goal": self._pose_dict(goal_tf),
            "goal_lane": {
                "road_id": int(goal_wp.road_id),
                "section_id": int(goal_wp.section_id),
                "lane_id": int(goal_wp.lane_id),
            },
            "route_length_m": legacy_route_length_m,
            "route_length_m_role": "legacy_selection_straight_line_distance",
            "route_length_m_claim_grade": False,
            "claim_route_length_m": claim_route_length_m,
            "claim_route_length_source": (
                route_trace_length_source
                if route_trace_length_m is not None
                else "legacy_selection_route_length_fallback"
            ),
            "route_trace_source": "town01_forward_waypoint_trace",
            "route_trace_length_m": route_trace_length_m,
            "route_trace_length_source": route_trace_length_source,
            "route_trace_point_count": int(len(route_trace)),
            "route_trace": route_trace,
            "forward_available_length_m": float(selected_goal["forward_available_length_m"]),
            "remain_length_after_goal_m": float(selected_goal["remain_end_margin_m"]),
            "goal_trace_index": int(selected_goal["goal_trace_index"]),
            "road_transition_count": int(selected_goal.get("road_transition_count") or 0),
            "lane_transition_count": int(selected_goal.get("lane_transition_count") or 0),
            "health_tags": list((selected_route_record or {}).get("health_tags") or ["unreviewed"]),
            "recommended_uses": list((selected_route_record or {}).get("recommended_uses") or []),
            "vehicle_blueprint_id": str(getattr(veh_bp, "id", "") or ""),
            "front_gap_m": None,
            "lead_profile": {"mode": "none"},
            "scenario_goal_raw_carla": {
                "x": float(goal_tf.location.x),
                "y": float(goal_tf.location.y),
                "z": float(goal_tf.location.z),
            },
        }
        self.actors = ActorRefs(ego=ego, front=None)
        return self.actors

    def reset(self):
        self._traffic_light_started_ts = None
        self._traffic_light_released = False

    def destroy(self):
        if self.actors and self.actors.ego is not None:
            try:
                self.actors.ego.destroy()
            except Exception:
                pass
        self.actors = None
        self._traffic_light_world = None
