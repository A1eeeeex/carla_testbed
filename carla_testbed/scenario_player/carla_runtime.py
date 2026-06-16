from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.control.applicator import ControlApplyResult, apply_control_to_vehicle
from carla_testbed.scenario_player.actions import speed_profile_target_mps
from carla_testbed.scenario_player.actor_registry import ScenarioActorState
from carla_testbed.scenario_player.player import FixedSceneFrameContext, FixedScenePlayer
from carla_testbed.scenario_player.schema import validate_fixed_scene_storyboard


@dataclass
class CarlaFixedSceneRuntimeState:
    scene_id: str
    actor_roles: dict[str, int | str | None] = field(default_factory=dict)
    spawn_feasibility: dict[str, dict[str, Any]] = field(default_factory=dict)
    lane_change_runtime: dict[str, dict[str, Any]] = field(default_factory=dict)
    spawned_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    artifact_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "fixed_scene_runtime_state.v1",
            "scene_id": self.scene_id,
            "actor_roles": dict(self.actor_roles),
            "spawn_feasibility": dict(self.spawn_feasibility),
            "lane_change_runtime": dict(self.lane_change_runtime),
            "spawned_count": int(self.spawned_count),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "artifact_paths": dict(self.artifact_paths),
        }


@dataclass
class _ControlCommand:
    throttle: float = 0.0
    brake: float = 0.0
    steer: float = 0.0
    reverse: bool = False
    hand_brake: bool = False
    manual_gear_shift: bool = False
    gear: int = 0
    source: str = "fixed_scene_player"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _SpawnSelection:
    transform: Any | None
    source: str


class CarlaFixedSceneRuntime:
    """CARLA-backed adapter for fixed-scene non-ego actors.

    The adapter keeps CARLA imports lazy by using duck-typed world/actor
    objects. Unit tests can pass fake CARLA objects; real runs can pass the
    actual CARLA world and ego vehicle.
    """

    def __init__(self) -> None:
        self.storyboard: dict[str, Any] | None = None
        self.player: FixedScenePlayer | None = None
        self.actors: dict[str, Any] = {}
        self.state: CarlaFixedSceneRuntimeState | None = None
        self._artifact_dir: Path | None = None
        self._lane_change_states: dict[str, dict[str, Any]] = {}

    def setup(self, context: Any, storyboard: Mapping[str, Any]) -> CarlaFixedSceneRuntimeState:
        resolved = dict(storyboard)
        validate_fixed_scene_storyboard(resolved)
        self.storyboard = resolved
        self._artifact_dir = _artifact_dir(context)
        paths = self._write_setup_artifacts(resolved)
        self.player = FixedScenePlayer(
            trace_path=paths.get("scenario_actor_trace"),
            phase_events_path=paths.get("scenario_phase_events"),
        )
        self.player.setup({}, resolved)
        state = CarlaFixedSceneRuntimeState(scene_id=str(resolved["scene_id"]), artifact_paths=paths)
        state.lane_change_runtime = _lane_change_runtime_metadata(resolved)
        world = _context_attr(context, "world")
        ego = _ego_actor(context)
        if world is None:
            state.errors.append("world_missing")
        if ego is None:
            state.errors.append("ego_actor_missing")
        if world is None or ego is None:
            self.state = state
            return state
        for role, role_cfg in dict(resolved.get("roles", {})).items():
            if role == "ego":
                continue
            actor = self._spawn_role_actor(world=world, ego=ego, role=role, role_cfg=role_cfg, state=state)
            if actor is None:
                continue
            self.actors[role] = actor
            state.actor_roles[role] = getattr(actor, "id", None)
        state.spawned_count = len(self.actors)
        self.state = state
        self._write_runtime_state()
        return state

    def tick(self, context: Any) -> dict[str, Any]:
        if self.storyboard is None or self.player is None:
            raise RuntimeError("CarlaFixedSceneRuntime.setup() must be called before tick()")
        ego = _ego_actor(context)
        world = _context_attr(context, "world", None)
        actors = {"ego": _actor_state("ego", ego, world=world)}
        for role, actor in self.actors.items():
            actors[role] = _actor_state(role, actor, world=world)
        frame = FixedSceneFrameContext(
            sim_time_sec=float(_context_attr(context, "sim_time_sec", _context_attr(context, "sim_time", 0.0)) or 0.0),
            world_frame=int(_context_attr(context, "world_frame", _context_attr(context, "frame", 0)) or 0),
            actors=actors,
        )
        result = self.player.tick(frame)
        applied: dict[str, dict[str, Any]] = {}
        for role, action in dict(result.get("active_actions") or {}).items():
            actor = self.actors.get(role)
            if actor is None:
                continue
            apply_result = self._apply_action(actor=actor, action=action, context=context)
            applied[role] = apply_result.to_dict()
        result["applied_controls"] = applied
        return result

    def teardown(self) -> list[str]:
        errors: list[str] = []
        for actor in reversed(list(self.actors.values())):
            try:
                actor.destroy()
            except Exception as exc:  # pragma: no cover - defensive runtime cleanup
                errors.append(f"{getattr(actor, 'id', 'unknown')}: {type(exc).__name__}: {exc}")
        self.actors.clear()
        self._lane_change_states.clear()
        if self.player is not None:
            self.player.teardown()
        if self.state is not None:
            self.state.errors.extend(errors)
            self._write_runtime_state()
        return errors

    def active_roles(self) -> list[str]:
        return sorted(self.actors)

    def _spawn_role_actor(
        self,
        *,
        world: Any,
        ego: Any,
        role: str,
        role_cfg: Mapping[str, Any],
        state: CarlaFixedSceneRuntimeState,
    ) -> Any | None:
        blueprint_id = str(role_cfg.get("blueprint") or "vehicle.lincoln.mkz_2020")
        blueprint = _find_blueprint(world, blueprint_id)
        if blueprint is None:
            state.errors.append(f"blueprint_missing:{blueprint_id}")
            return None
        _set_blueprint_role_name(blueprint, role)
        spawn_cfg = role_cfg.get("spawn") if isinstance(role_cfg.get("spawn"), Mapping) else {}
        selection = _select_spawn_transform_result(world, ego, spawn_cfg)
        if selection.transform is None:
            state.errors.append(f"spawn_transform_missing:{role}")
            return None
        actor = _try_spawn_actor(world, blueprint, selection.transform)
        if actor is None:
            state.errors.append(f"spawn_failed:{role}")
            return None
        _set_actor_initial_speed(actor, selection.transform, _initial_speed_from_role(role_cfg, spawn_cfg))
        _tick_world_if_available(world)
        feasibility = _spawn_feasibility(
            world=world,
            ego=ego,
            actor=actor,
            spawn_cfg=spawn_cfg,
            selection=selection,
        )
        state.spawn_feasibility[role] = feasibility
        for reason in feasibility.get("blocking_reasons") or []:
            state.errors.append(f"spawn_feasibility_failed:{role}:{reason}")
        if hasattr(actor, "set_autopilot"):
            try:
                actor.set_autopilot(False)
            except Exception as exc:  # pragma: no cover - CARLA runtime detail
                state.warnings.append(f"set_autopilot_false_failed:{role}:{type(exc).__name__}")
        return actor

    def _apply_action(self, *, actor: Any, action: Mapping[str, Any], context: Any) -> ControlApplyResult:
        action_type = str(action.get("type"))
        if action_type in {"brake_to_stop", "hold_stop"}:
            _set_actor_target_velocity(actor, 0.0)
            command = _ControlCommand(
                throttle=0.0,
                brake=1.0,
                steer=0.0,
                hand_brake=action_type == "hold_stop",
                metadata={"action_type": action_type, "controller": action.get("controller")},
            )
            return apply_control_to_vehicle(actor, command, stamp=_stamp(context))
        target_speed = _target_speed_from_action(action, context)
        if action_type in {"gap_control", "maintain_gap"}:
            target_speed = _gap_target_speed(actor=actor, action=action, context=context)
        if action_type == "lane_change":
            _apply_lane_change_transform(actor=actor, action=action, context=context, states=self._lane_change_states)
        if target_speed is not None:
            _set_actor_target_velocity(actor, target_speed)
        actual_speed = _actor_speed_mps(actor)
        throttle = 0.0
        brake = 0.0
        if target_speed is not None:
            error = target_speed - actual_speed
            if error >= 0.0:
                throttle = min(0.7, max(0.0, 0.12 * error))
            else:
                brake = min(1.0, max(0.0, -0.18 * error))
        command = _ControlCommand(
            throttle=throttle,
            brake=brake,
            steer=_steer_from_action(action),
            metadata={
                "action_type": action_type,
                "controller": action.get("controller"),
                "target_speed_mps": target_speed,
                "actual_speed_mps": actual_speed,
                "runtime_boundary": "fixed_scene_non_ego_actor_control",
            },
        )
        return apply_control_to_vehicle(actor, command, stamp=_stamp(context))

    def _write_setup_artifacts(self, storyboard: Mapping[str, Any]) -> dict[str, str]:
        if self._artifact_dir is None:
            return {}
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        storyboard_path = self._artifact_dir / "fixed_scene_resolved.json"
        trace_path = self._artifact_dir / "scenario_actor_trace.jsonl"
        events_path = self._artifact_dir / "scenario_phase_events.jsonl"
        storyboard_path.write_text(json.dumps(dict(storyboard), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        for path in (trace_path, events_path):
            if path.exists():
                path.unlink()
        return {
            "fixed_scene_resolved": str(storyboard_path),
            "scenario_actor_trace": str(trace_path),
            "scenario_phase_events": str(events_path),
        }

    def _write_runtime_state(self) -> None:
        if self._artifact_dir is None or self.state is None:
            return
        path = self._artifact_dir / "fixed_scene_runtime_state.json"
        path.write_text(json.dumps(self.state.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _artifact_dir(context: Any) -> Path | None:
    value = _context_attr(context, "artifact_dir", None) or _context_attr(context, "artifacts_dir", None)
    if value is None:
        run_dir = _context_attr(context, "run_dir", None)
        if run_dir is not None:
            value = Path(run_dir) / "artifacts"
    return Path(value).expanduser() if value is not None else None


def _lane_change_runtime_metadata(storyboard: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    phases = storyboard.get("storyboard", {}).get("phases", []) if isinstance(storyboard.get("storyboard"), Mapping) else []
    for phase in (phases if isinstance(phases, list) else []):
        if not isinstance(phase, Mapping):
            continue
        actions = phase.get("actions", [])
        for action in (actions if isinstance(actions, list) else []):
            if not isinstance(action, Mapping) or action.get("type") != "lane_change":
                continue
            role = str(action.get("role", "unknown"))
            result[role] = {
                "lane_change_runtime_mode": str(action.get("lane_change_runtime_mode") or "set_transform_interpolation"),
                "physics_controlled_lane_change": bool(action.get("physics_controlled_lane_change", False)),
                "claim_grade_lane_change": bool(action.get("claim_grade_lane_change", False)),
                "velocity_source": str(action.get("velocity_source") or "carla_get_velocity"),
                "interpretation_boundary": "diagnostic_scripted_lane_change_not_physics_controlled",
            }
    return result


def _context_attr(context: Any, name: str, default: Any = None) -> Any:
    if isinstance(context, Mapping):
        return context.get(name, default)
    return getattr(context, name, default)


def _ego_actor(context: Any) -> Any | None:
    return (
        _context_attr(context, "ego_actor", None)
        or _context_attr(context, "ego", None)
        or _context_attr(context, "vehicle", None)
    )


def _find_blueprint(world: Any, blueprint_id: str) -> Any | None:
    library = world.get_blueprint_library() if hasattr(world, "get_blueprint_library") else None
    if library is None:
        return None
    if hasattr(library, "find"):
        try:
            return library.find(blueprint_id)
        except Exception:
            pass
    matches = list(library.filter(blueprint_id)) if hasattr(library, "filter") else []
    if matches:
        return matches[0]
    fallback = list(library.filter("vehicle.*")) if hasattr(library, "filter") else []
    return fallback[0] if fallback else None


def _set_blueprint_role_name(blueprint: Any, role_name: str) -> None:
    try:
        if hasattr(blueprint, "has_attribute") and blueprint.has_attribute("role_name"):
            blueprint.set_attribute("role_name", role_name)
        elif hasattr(blueprint, "set_attribute"):
            blueprint.set_attribute("role_name", role_name)
    except Exception:
        pass


def _try_spawn_actor(world: Any, blueprint: Any, transform: Any) -> Any | None:
    if hasattr(world, "try_spawn_actor"):
        return world.try_spawn_actor(blueprint, transform)
    if hasattr(world, "spawn_actor"):
        return world.spawn_actor(blueprint, transform)
    return None


def _select_spawn_transform(world: Any, ego: Any, spawn_cfg: Mapping[str, Any]) -> Any | None:
    return _select_spawn_transform_result(world, ego, spawn_cfg).transform


def _select_spawn_transform_result(world: Any, ego: Any, spawn_cfg: Mapping[str, Any]) -> _SpawnSelection:
    ego_transform = _safe_call(ego, "get_transform")
    if ego_transform is None:
        ego_transform = getattr(ego, "transform", None)
    if ego_transform is None:
        return _SpawnSelection(transform=None, source="missing_ego_transform")
    s_offset_m = float(spawn_cfg.get("s_offset_m", spawn_cfg.get("distance_m", 0.0)) or 0.0)
    carla_map = world.get_map() if hasattr(world, "get_map") else None
    if carla_map is not None and hasattr(carla_map, "get_waypoint"):
        try:
            waypoint = carla_map.get_waypoint(getattr(ego_transform, "location"))
            candidates = list(waypoint.next(s_offset_m)) if waypoint is not None and hasattr(waypoint, "next") else []
            if candidates:
                transform = getattr(candidates[0], "transform", None)
                if transform is not None:
                    return _SpawnSelection(transform=_offset_transform(transform, spawn_cfg), source="carla_waypoint_next")
        except Exception:
            pass
    return _SpawnSelection(transform=_fallback_transform_ahead(ego_transform, spawn_cfg), source="fallback_transform_ahead")


def _initial_speed_from_role(role_cfg: Mapping[str, Any], spawn_cfg: Mapping[str, Any]) -> float | None:
    for source in (role_cfg, spawn_cfg):
        value = source.get("initial_speed_mps")
        if value is not None:
            return float(value)
    return None


def _set_actor_initial_speed(actor: Any, transform: Any, speed_mps: float | None) -> None:
    if speed_mps is None or speed_mps <= 0.0:
        return
    setter = getattr(actor, "set_target_velocity", None)
    if not callable(setter):
        return
    yaw = math.radians(_float_attr(getattr(transform, "rotation", None), "yaw", 0.0) or 0.0)
    vector = _make_vector(
        x=float(speed_mps) * math.cos(yaw),
        y=float(speed_mps) * math.sin(yaw),
        z=0.0,
    )
    try:
        setter(vector)
    except Exception:
        return


def _set_actor_target_velocity(actor: Any, speed_mps: float) -> None:
    transform = _safe_call(actor, "get_transform") or getattr(actor, "transform", None)
    setter = getattr(actor, "set_target_velocity", None)
    if not callable(setter) or transform is None:
        return
    yaw = math.radians(_float_attr(getattr(transform, "rotation", None), "yaw", 0.0) or 0.0)
    vector = _make_vector(
        x=float(speed_mps) * math.cos(yaw),
        y=float(speed_mps) * math.sin(yaw),
        z=0.0,
    )
    try:
        setter(vector)
    except Exception:
        return


def _spawn_feasibility(
    *,
    world: Any,
    ego: Any,
    actor: Any,
    spawn_cfg: Mapping[str, Any],
    selection: _SpawnSelection,
) -> dict[str, Any]:
    expected_s = float(spawn_cfg.get("s_offset_m", spawn_cfg.get("distance_m", 0.0)) or 0.0)
    feasibility_cfg = spawn_cfg.get("feasibility") if isinstance(spawn_cfg.get("feasibility"), Mapping) else {}
    enabled = bool(feasibility_cfg.get("enabled", expected_s != 0.0))
    tolerance_m = float(feasibility_cfg.get("s_offset_tolerance_m", 10.0) or 10.0)
    max_lateral_m = float(feasibility_cfg.get("max_lateral_m", 4.0) or 4.0)
    require_waypoint = bool(feasibility_cfg.get("require_waypoint", False))
    ego_tf = _safe_call(ego, "get_transform") or getattr(ego, "transform", None)
    actor_tf = _safe_call(actor, "get_transform") or getattr(actor, "transform", None)
    longitudinal = None
    lateral = None
    blocking: list[str] = []
    warnings: list[str] = []
    if ego_tf is not None and actor_tf is not None:
        longitudinal, lateral = _relative_longitudinal_lateral(
            getattr(ego_tf, "location", None),
            getattr(actor_tf, "location", None),
            _float_attr(getattr(ego_tf, "rotation", None), "yaw", 0.0) or 0.0,
        )
    else:
        warnings.append("spawn_feasibility_pose_missing")
    if enabled and longitudinal is not None:
        if longitudinal < 0.0:
            blocking.append("actor_spawned_behind_ego")
        if abs(longitudinal - expected_s) > tolerance_m:
            blocking.append("longitudinal_offset_out_of_tolerance")
    if enabled and lateral is not None and abs(lateral) > max_lateral_m:
        blocking.append("lateral_offset_out_of_tolerance")
    if require_waypoint and selection.source != "carla_waypoint_next":
        blocking.append("waypoint_spawn_required_but_fallback_used")
    lane_check = _same_lane_check(world, ego_tf, actor_tf, spawn_cfg)
    if lane_check.get("status") == "fail":
        blocking.append("same_lane_check_failed")
    elif lane_check.get("status") == "warn":
        warnings.extend(lane_check.get("warnings") or [])
    return {
        "enabled": enabled,
        "expected_s_offset_m": expected_s,
        "actual_longitudinal_offset_m": longitudinal,
        "actual_lateral_offset_m": lateral,
        "s_offset_tolerance_m": tolerance_m,
        "max_lateral_m": max_lateral_m,
        "spawn_source": selection.source,
        "require_waypoint": require_waypoint,
        "lane_check": lane_check,
        "status": "fail" if blocking else ("warn" if warnings else "pass"),
        "blocking_reasons": blocking,
        "warnings": warnings,
    }


def _relative_longitudinal_lateral(ego_location: Any, actor_location: Any, ego_yaw_deg: float) -> tuple[float, float]:
    dx = (_float_attr(actor_location, "x", 0.0) or 0.0) - (_float_attr(ego_location, "x", 0.0) or 0.0)
    dy = (_float_attr(actor_location, "y", 0.0) or 0.0) - (_float_attr(ego_location, "y", 0.0) or 0.0)
    yaw = math.radians(float(ego_yaw_deg))
    longitudinal = dx * math.cos(yaw) + dy * math.sin(yaw)
    lateral = -dx * math.sin(yaw) + dy * math.cos(yaw)
    return longitudinal, lateral


def _same_lane_check(world: Any, ego_tf: Any, actor_tf: Any, spawn_cfg: Mapping[str, Any]) -> dict[str, Any]:
    if spawn_cfg.get("lane") != "same":
        return {"status": "not_applicable"}
    carla_map = world.get_map() if hasattr(world, "get_map") else None
    if carla_map is None or ego_tf is None or actor_tf is None or not hasattr(carla_map, "get_waypoint"):
        return {"status": "warn", "warnings": ["same_lane_check_unavailable"]}
    try:
        ego_wp = carla_map.get_waypoint(getattr(ego_tf, "location"))
        actor_wp = carla_map.get_waypoint(getattr(actor_tf, "location"))
    except Exception:
        return {"status": "warn", "warnings": ["same_lane_check_failed_to_query_map"]}
    if ego_wp is None or actor_wp is None:
        return {"status": "warn", "warnings": ["same_lane_check_missing_waypoint"]}
    ego_lane = (getattr(ego_wp, "road_id", None), getattr(ego_wp, "section_id", None), getattr(ego_wp, "lane_id", None))
    actor_lane = (
        getattr(actor_wp, "road_id", None),
        getattr(actor_wp, "section_id", None),
        getattr(actor_wp, "lane_id", None),
    )
    return {"status": "pass" if ego_lane == actor_lane else "fail", "ego_lane": ego_lane, "actor_lane": actor_lane}


def _offset_transform(transform: Any, spawn_cfg: Mapping[str, Any]) -> Any:
    lateral = float(spawn_cfg.get("lateral_offset_m", 0.0) or 0.0)
    z_offset = float(spawn_cfg.get("z_offset_m", 0.35) or 0.35)
    if lateral == 0.0 and z_offset == 0.0:
        return transform
    return _fallback_transform_ahead(
        transform,
        {"s_offset_m": 0.0, "lateral_offset_m": lateral, "z_offset_m": z_offset},
    )


def _fallback_transform_ahead(base_transform: Any, spawn_cfg: Mapping[str, Any]) -> Any:
    distance = float(spawn_cfg.get("s_offset_m", spawn_cfg.get("distance_m", 0.0)) or 0.0)
    lateral = float(spawn_cfg.get("lateral_offset_m", 0.0) or 0.0)
    z_offset = float(spawn_cfg.get("z_offset_m", 0.5) or 0.5)
    location = getattr(base_transform, "location", None)
    rotation = getattr(base_transform, "rotation", None)
    yaw_deg = float(getattr(rotation, "yaw", 0.0) or 0.0)
    yaw = math.radians(yaw_deg)
    dx = distance * math.cos(yaw) - lateral * math.sin(yaw)
    dy = distance * math.sin(yaw) + lateral * math.cos(yaw)
    x = float(getattr(location, "x", 0.0) or 0.0) + dx
    y = float(getattr(location, "y", 0.0) or 0.0) + dy
    z = float(getattr(location, "z", 0.0) or 0.0) + z_offset
    carla_mod = _try_import_carla()
    if _has_carla_transform_types(carla_mod):
        return carla_mod.Transform(
            carla_mod.Location(x=x, y=y, z=z),
            carla_mod.Rotation(
                pitch=float(getattr(rotation, "pitch", 0.0) or 0.0),
                yaw=yaw_deg,
                roll=float(getattr(rotation, "roll", 0.0) or 0.0),
            ),
        )
    return _SimpleTransform(_SimpleLocation(x, y, z), _SimpleRotation(yaw=yaw_deg))


def _actor_state(role: str, actor: Any | None, *, world: Any | None = None) -> ScenarioActorState:
    if actor is None:
        return ScenarioActorState(role=role)
    transform = _safe_call(actor, "get_transform") or getattr(actor, "transform", None)
    velocity = _safe_call(actor, "get_velocity") or getattr(actor, "velocity", None)
    control = _safe_call(actor, "get_control")
    location = getattr(transform, "location", None)
    rotation = getattr(transform, "rotation", None)
    route_projection = _actor_route_projection(world, transform)
    dimensions = _actor_dimensions(actor)
    return ScenarioActorState(
        role=role,
        actor_id=getattr(actor, "id", None),
        x=_float_attr(location, "x"),
        y=_float_attr(location, "y"),
        z=_float_attr(location, "z"),
        yaw_rad=math.radians(_float_attr(rotation, "yaw", 0.0) or 0.0),
        speed_mps=_vector_norm(velocity),
        length_m=dimensions.get("length_m"),
        width_m=dimensions.get("width_m"),
        height_m=dimensions.get("height_m"),
        bbox_extent_x_m=dimensions.get("bbox_extent_x_m"),
        bbox_extent_y_m=dimensions.get("bbox_extent_y_m"),
        bbox_extent_z_m=dimensions.get("bbox_extent_z_m"),
        route_s=route_projection.get("route_s"),
        lane_id=route_projection.get("lane_id"),
        applied_control=_control_dict(control),
    )


def _actor_dimensions(actor: Any | None) -> dict[str, float | None]:
    bbox = getattr(actor, "bounding_box", None) if actor is not None else None
    extent = getattr(bbox, "extent", None)
    if extent is None:
        return {
            "length_m": None,
            "width_m": None,
            "height_m": None,
            "bbox_extent_x_m": None,
            "bbox_extent_y_m": None,
            "bbox_extent_z_m": None,
        }
    extent_x = _float_attr(extent, "x")
    extent_y = _float_attr(extent, "y")
    extent_z = _float_attr(extent, "z")
    return {
        "length_m": 2.0 * extent_x if extent_x is not None else None,
        "width_m": 2.0 * extent_y if extent_y is not None else None,
        "height_m": 2.0 * extent_z if extent_z is not None else None,
        "bbox_extent_x_m": extent_x,
        "bbox_extent_y_m": extent_y,
        "bbox_extent_z_m": extent_z,
    }


def _actor_route_projection(world: Any | None, transform: Any | None) -> dict[str, Any]:
    carla_map = world.get_map() if world is not None and hasattr(world, "get_map") else None
    if carla_map is None or transform is None or not hasattr(carla_map, "get_waypoint"):
        return {}
    try:
        waypoint = carla_map.get_waypoint(getattr(transform, "location"))
    except Exception:
        return {}
    if waypoint is None:
        return {}
    lane_id = getattr(waypoint, "lane_id", None)
    road_id = getattr(waypoint, "road_id", None)
    section_id = getattr(waypoint, "section_id", None)
    route_s = _optional_float(getattr(waypoint, "s", None))
    lane_parts = [road_id, section_id, lane_id]
    return {
        "route_s": route_s,
        "lane_id": ":".join(str(part) for part in lane_parts) if all(part is not None for part in lane_parts) else None,
    }


def _actor_speed_mps(actor: Any) -> float:
    return _vector_norm(_safe_call(actor, "get_velocity") or getattr(actor, "velocity", None)) or 0.0


def _target_speed_from_action(action: Mapping[str, Any], context: Any) -> float | None:
    if action.get("type") == "speed_profile":
        sim_time = float(_context_attr(context, "sim_time_sec", _context_attr(context, "sim_time", 0.0)) or 0.0)
        return speed_profile_target_mps(
            action.get("profile"),
            sim_time_sec=sim_time,
            interpolation=str(action.get("interpolation") or "step"),
        )
    for key in ("target_speed_mps", "speed_mps"):
        if action.get(key) is not None:
            return float(action[key])
    return None


def _gap_target_speed(*, actor: Any, action: Mapping[str, Any], context: Any) -> float | None:
    ego = _ego_actor(context)
    if ego is None:
        return _target_speed_from_action(action, context)
    ego_tf = _safe_call(ego, "get_transform") or getattr(ego, "transform", None)
    actor_tf = _safe_call(actor, "get_transform") or getattr(actor, "transform", None)
    if ego_tf is None or actor_tf is None:
        return _target_speed_from_action(action, context)
    gap = _distance_xy(getattr(ego_tf, "location", None), getattr(actor_tf, "location", None))
    target_gap = float(action.get("target_gap_m", action.get("gap_m", 20.0)) or 20.0)
    ego_speed = _actor_speed_mps(ego)
    target = ego_speed + max(-5.0, min(5.0, 0.35 * (gap - target_gap)))
    return max(0.0, target)


def _steer_from_action(action: Mapping[str, Any]) -> float:
    if action.get("type") != "lane_change":
        return 0.0
    direction = str(action.get("direction") or "").lower()
    if not direction and action.get("target_lane_offset") is not None:
        try:
            offset = int(action["target_lane_offset"])
            direction = "left" if offset < 0 else "right"
        except (TypeError, ValueError):
            direction = ""
    magnitude = float(action.get("steer_hint", 0.18) or 0.18)
    if direction in {"left", "adjacent_left"}:
        return -abs(magnitude)
    if direction in {"right", "adjacent_right"}:
        return abs(magnitude)
    return 0.0


def _apply_lane_change_transform(
    *,
    actor: Any,
    action: Mapping[str, Any],
    context: Any,
    states: dict[str, dict[str, Any]],
) -> None:
    setter = getattr(actor, "set_transform", None)
    if not callable(setter):
        return
    current = _safe_call(actor, "get_transform") or getattr(actor, "transform", None)
    if current is None or getattr(current, "location", None) is None or getattr(current, "rotation", None) is None:
        return
    sim_time = float(_context_attr(context, "sim_time_sec", _context_attr(context, "sim_time", 0.0)) or 0.0)
    key = f"{getattr(actor, 'id', 'unknown')}:lane_change:{action.get('target_lane_offset', '')}"
    state = states.get(key)
    if state is None:
        state = {
            "start_time_s": sim_time,
            "start_x": _float_attr(current.location, "x", 0.0) or 0.0,
            "start_y": _float_attr(current.location, "y", 0.0) or 0.0,
            "start_z": _float_attr(current.location, "z", 0.0) or 0.0,
            "start_yaw_deg": _float_attr(current.rotation, "yaw", 0.0) or 0.0,
            "start_pitch_deg": _float_attr(current.rotation, "pitch", 0.0) or 0.0,
            "start_roll_deg": _float_attr(current.rotation, "roll", 0.0) or 0.0,
        }
        states[key] = state
    duration = max(0.01, float(action.get("duration_s", 3.0) or 3.0))
    raw_progress = max(0.0, min(1.0, (sim_time - float(state["start_time_s"])) / duration))
    progress = _lane_change_eased_progress(raw_progress, str(action.get("easing") or "cosine"))
    shift_m = _lane_change_shift_m(action)
    yaw_rad = math.radians(float(state["start_yaw_deg"]))
    forward_x = math.cos(yaw_rad)
    forward_y = math.sin(yaw_rad)
    right_x = -math.sin(yaw_rad)
    right_y = math.cos(yaw_rad)
    current_x = _float_attr(current.location, "x", float(state["start_x"])) or float(state["start_x"])
    current_y = _float_attr(current.location, "y", float(state["start_y"])) or float(state["start_y"])
    dx = current_x - float(state["start_x"])
    dy = current_y - float(state["start_y"])
    longitudinal = dx * forward_x + dy * forward_y
    desired_lateral = shift_m * progress
    x = float(state["start_x"]) + longitudinal * forward_x + desired_lateral * right_x
    y = float(state["start_y"]) + longitudinal * forward_y + desired_lateral * right_y
    z = _float_attr(current.location, "z", float(state["start_z"])) or float(state["start_z"])
    yaw_hint = _lane_change_yaw_hint_deg(action, raw_progress)
    transform = _make_transform(
        x=x,
        y=y,
        z=z,
        pitch=float(state["start_pitch_deg"]),
        yaw=float(state["start_yaw_deg"]) + yaw_hint,
        roll=float(state["start_roll_deg"]),
    )
    try:
        setter(transform)
    except Exception:
        return


def _lane_change_shift_m(action: Mapping[str, Any]) -> float:
    lane_width = float(action.get("lane_width_m", 3.6) or 3.6)
    offset = int(action.get("target_lane_offset", 1) or 1)
    shift = float(action.get("lateral_shift_m", abs(offset) * lane_width) or abs(offset) * lane_width)
    direction = str(action.get("direction") or "").lower()
    if direction in {"left", "adjacent_left"}:
        return -abs(shift)
    if direction in {"right", "adjacent_right"}:
        return abs(shift)
    return shift if action.get("lateral_shift_m") is not None else (abs(shift) if offset >= 0 else -abs(shift))


def _lane_change_eased_progress(raw_progress: float, easing: str) -> float:
    progress = max(0.0, min(1.0, float(raw_progress)))
    if easing == "linear":
        return progress
    return 0.5 - 0.5 * math.cos(math.pi * progress)


def _lane_change_yaw_hint_deg(action: Mapping[str, Any], raw_progress: float) -> float:
    max_yaw = abs(float(action.get("max_yaw_hint_deg", 5.0) or 5.0))
    shift = _lane_change_shift_m(action)
    sign = 1.0 if shift >= 0.0 else -1.0
    return sign * max_yaw * math.sin(math.pi * max(0.0, min(1.0, raw_progress)))


def _make_transform(*, x: float, y: float, z: float, pitch: float, yaw: float, roll: float) -> Any:
    carla_mod = _try_import_carla()
    if _has_carla_transform_types(carla_mod):
        return carla_mod.Transform(
            carla_mod.Location(x=x, y=y, z=z),
            carla_mod.Rotation(pitch=pitch, yaw=yaw, roll=roll),
        )
    return _SimpleTransform(_SimpleLocation(x, y, z), _SimpleRotation(pitch=pitch, yaw=yaw, roll=roll))


def _stamp(context: Any) -> Any:
    return _SimpleStamp(
        frame_id=_context_attr(context, "world_frame", _context_attr(context, "frame", None)),
        sim_time_s=_context_attr(context, "sim_time_sec", _context_attr(context, "sim_time", None)),
    )


def _safe_call(obj: Any, method_name: str) -> Any:
    if obj is None:
        return None
    method = getattr(obj, method_name, None)
    if not callable(method):
        return None
    try:
        return method()
    except Exception:
        return None


def _vector_norm(vector: Any) -> float | None:
    if vector is None:
        return None
    x = _float_attr(vector, "x", 0.0) or 0.0
    y = _float_attr(vector, "y", 0.0) or 0.0
    z = _float_attr(vector, "z", 0.0) or 0.0
    return math.sqrt(x * x + y * y + z * z)


def _distance_xy(left: Any, right: Any) -> float:
    return math.hypot(
        (_float_attr(left, "x", 0.0) or 0.0) - (_float_attr(right, "x", 0.0) or 0.0),
        (_float_attr(left, "y", 0.0) or 0.0) - (_float_attr(right, "y", 0.0) or 0.0),
    )


def _float_attr(obj: Any, name: str, default: float | None = None) -> float | None:
    if obj is None:
        return default
    value = getattr(obj, name, default)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _control_dict(control: Any) -> dict[str, Any] | None:
    if control is None:
        return None
    return {
        "throttle": _float_attr(control, "throttle", 0.0),
        "brake": _float_attr(control, "brake", 0.0),
        "steer": _float_attr(control, "steer", 0.0),
        "hand_brake": bool(getattr(control, "hand_brake", False)),
    }


def _make_vector(*, x: float, y: float, z: float) -> Any:
    carla_mod = _try_import_carla()
    if carla_mod is not None and hasattr(carla_mod, "Vector3D"):
        return carla_mod.Vector3D(x=x, y=y, z=z)
    return _SimpleVector(x=x, y=y, z=z)


def _try_import_carla() -> Any | None:
    try:
        import carla  # type: ignore

        return carla
    except Exception:
        return None


def _has_carla_transform_types(carla_mod: Any | None) -> bool:
    return all(hasattr(carla_mod, name) for name in ("Transform", "Location", "Rotation"))


def _tick_world_if_available(world: Any) -> None:
    tick = getattr(world, "tick", None)
    if callable(tick):
        tick()


@dataclass
class _SimpleLocation:
    x: float
    y: float
    z: float


@dataclass
class _SimpleRotation:
    pitch: float = 0.0
    yaw: float = 0.0
    roll: float = 0.0


@dataclass
class _SimpleTransform:
    location: _SimpleLocation
    rotation: _SimpleRotation


@dataclass
class _SimpleVector:
    x: float
    y: float
    z: float


@dataclass
class _SimpleStamp:
    frame_id: int | None
    sim_time_s: float | None
