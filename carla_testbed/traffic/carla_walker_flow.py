from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any, Mapping

from .base import TrafficActorInfo, TrafficFlowState
from .walker_spawn_policy import location_to_dict, select_walker_spawn_locations


class CarlaWalkerFlow:
    name = "carla_walker_ai_controller"

    def __init__(self) -> None:
        self._walkers: list[Any] = []
        self._controllers: list[Any] = []
        self._state: TrafficFlowState | None = None
        self._events: list[dict[str, Any]] = []

    def setup(self, context: Any, config: dict[str, Any]) -> TrafficFlowState:
        flow_cfg = dict(config.get("traffic_flow") or config)
        enabled = bool(flow_cfg.get("enabled", False))
        seed = _int_or_none(flow_cfg.get("seed"))
        walker_cfg = flow_cfg.get("walkers") if isinstance(flow_cfg.get("walkers"), Mapping) else {}
        requested = int(walker_cfg.get("count", 0) or 0)
        if not enabled or not bool(walker_cfg.get("enabled", True)):
            self._state = TrafficFlowState(
                provider=self.name,
                enabled=False,
                seed=seed,
                requested_count=0,
                spawned_count=0,
            )
            return self._state

        world = getattr(context, "world", None)
        if world is None:
            self._state = TrafficFlowState(
                provider=self.name,
                enabled=True,
                seed=seed,
                requested_count=requested,
                spawned_count=0,
                errors=["carla_world_missing"],
            )
            return self._state

        rng = random.Random(seed)
        warnings: list[str] = []
        errors: list[str] = []
        _configure_pedestrian_world(world, walker_cfg, seed=seed, warnings=warnings)
        candidate_locations = _sample_nav_locations(context, world, walker_cfg, rng=rng)
        selected_locations, candidates = _select_walker_locations(
            context,
            candidate_locations,
            walker_cfg,
            seed=seed,
            count=requested,
        )
        walker_blueprints = _select_walker_blueprints(world, walker_cfg, rng=rng)
        controller_blueprint = _controller_blueprint(world)
        actors: list[TrafficActorInfo] = []
        controller_started_count = 0
        controller_count = 0

        if not walker_blueprints:
            errors.append("no_walker_blueprints_available")
        if controller_blueprint is None:
            errors.append("walker_ai_controller_blueprint_missing")

        for index, location in enumerate(selected_locations[:requested]):
            if not walker_blueprints or controller_blueprint is None:
                break
            role_name = f"{walker_cfg.get('role_name_prefix', 'background_walker')}_{index}"
            blueprint = walker_blueprints[index % len(walker_blueprints)]
            _set_blueprint_attr(blueprint, "role_name", role_name)
            speed_mps = _sample_speed(walker_cfg, rng)
            transform = _location_to_transform(location)
            walker = _try_spawn_actor(world, blueprint, transform)
            self._events.append(
                {"event": "walker_spawn_attempt", "role_name": role_name, "status": "ok" if walker else "failed"}
            )
            if walker is None:
                warnings.append(f"walker_spawn_failed:{role_name}")
                continue
            controller = _try_spawn_controller(world, controller_blueprint, walker)
            if controller is None:
                warnings.append(f"walker_controller_spawn_failed:{role_name}")
            else:
                controller_count += 1
                self._controllers.append(controller)
            self._walkers.append(walker)
            destination = _random_nav_location(world, rng=rng)
            controller_started = False
            if controller is not None:
                controller_started = _start_controller(controller, destination, speed_mps)
                controller_started_count += int(controller_started)
            _register_actor(context, walker, role_name=role_name)
            actor_info = TrafficActorInfo(
                actor_id=int(getattr(walker, "id", -1)),
                role_name=role_name,
                blueprint_id=str(getattr(blueprint, "id", getattr(walker, "type_id", "unknown"))),
                provider=self.name,
                control_source="carla_walker_ai_controller",
                spawn_transform={"location": location_to_dict(location)},
                behavior={
                    "controller_id": int(getattr(controller, "id", -1)) if controller is not None else None,
                    "controller_started": controller_started,
                    "max_speed_mps": speed_mps,
                    "destination": location_to_dict(destination) if destination is not None else None,
                    "initial_distance_to_ego_m": _distance_to_ego(context, location),
                },
            )
            actors.append(actor_info)
            self._events.append(
                {
                    "event": "walker_spawned",
                    "actor_id": actor_info.actor_id,
                    "controller_id": actor_info.behavior.get("controller_id"),
                    "role_name": role_name,
                }
            )
            if controller_started:
                self._events.append(
                    {
                        "event": "walker_controller_started",
                        "actor_id": actor_info.actor_id,
                        "controller_id": actor_info.behavior.get("controller_id"),
                        "role_name": role_name,
                    }
                )

        state = TrafficFlowState(
            provider=self.name,
            enabled=True,
            seed=seed,
            requested_count=requested,
            spawned_count=len(actors),
            actors=actors,
            warnings=warnings,
            errors=errors,
        )
        self._state = state
        _write_artifacts(
            context,
            state=state,
            flow_cfg=flow_cfg,
            candidates=candidates,
            events=self._events,
            controller_count=controller_count,
            controller_started_count=controller_started_count,
            provider=self.name,
        )
        return state

    def tick(self, context: Any) -> None:
        root = _artifact_root(context)
        if root is None or not self._walkers:
            return
        artifacts = root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        role_by_id = _role_by_actor_id(self._state)
        rows = [
            _walker_trace_row(
                walker,
                context=context,
                role_name=role_by_id.get(int(getattr(walker, "id", -1)), "background_walker_unknown"),
            )
            for walker in self._walkers
        ]
        _append_jsonl(artifacts / "walker_flow_trace.jsonl", rows)

    def teardown(self, context: Any) -> None:
        del context
        for controller in reversed(self._controllers):
            try:
                if hasattr(controller, "stop"):
                    controller.stop()
            except Exception:
                pass
            try:
                controller.destroy()
            except Exception:
                pass
        for walker in reversed(self._walkers):
            try:
                walker.destroy()
            except Exception:
                pass
        self._controllers.clear()
        self._walkers.clear()


def _configure_pedestrian_world(world: Any, walker_cfg: Mapping[str, Any], *, seed: int | None, warnings: list[str]) -> None:
    if seed is not None and hasattr(world, "set_pedestrians_seed"):
        world.set_pedestrians_seed(seed)
    elif seed is not None:
        warnings.append("world_set_pedestrians_seed_unavailable")
    cross_factor = float(walker_cfg.get("cross_factor", 0.0) or 0.0)
    if hasattr(world, "set_pedestrians_cross_factor"):
        world.set_pedestrians_cross_factor(cross_factor)
    elif cross_factor:
        warnings.append("world_set_pedestrians_cross_factor_unavailable")


def _sample_nav_locations(context: Any, world: Any, walker_cfg: Mapping[str, Any], *, rng: random.Random) -> list[Any]:
    del context
    requested = int(walker_cfg.get("count", 0) or 0)
    policy = walker_cfg.get("spawn_policy") if isinstance(walker_cfg.get("spawn_policy"), Mapping) else {}
    attempts = int(policy.get("max_spawn_attempts", max(40, requested * 10)) or max(40, requested * 10))
    locations: list[Any] = []
    seen: set[tuple[float, float, float]] = set()
    for _ in range(max(0, attempts)):
        location = _random_nav_location(world, rng=rng)
        if location is None:
            continue
        key = (
            round(float(getattr(location, "x", 0.0)), 2),
            round(float(getattr(location, "y", 0.0)), 2),
            round(float(getattr(location, "z", 0.0)), 2),
        )
        if key in seen:
            continue
        seen.add(key)
        locations.append(location)
    return locations


def _select_walker_locations(
    context: Any,
    candidate_locations: list[Any],
    walker_cfg: Mapping[str, Any],
    *,
    seed: int | None,
    count: int,
) -> tuple[list[Any], list[dict[str, Any]]]:
    ego_transform = getattr(context, "ego_transform", None)
    ego_location = getattr(ego_transform, "location", None) or getattr(context, "ego_location", None) or {"x": 0, "y": 0, "z": 0}
    route_points = list(getattr(context, "route_points", []) or [])
    existing = list(getattr(context, "existing_actor_locations", []) or [])
    policy = walker_cfg.get("spawn_policy") if isinstance(walker_cfg.get("spawn_policy"), Mapping) else {}
    return select_walker_spawn_locations(
        candidate_locations,
        ego_location=ego_location,
        route_points=route_points,
        existing_actor_locations=existing,
        policy=policy,
        seed=seed,
        count=count,
    )


def _select_walker_blueprints(world: Any, walker_cfg: Mapping[str, Any], *, rng: random.Random) -> list[Any]:
    library = world.get_blueprint_library()
    include = list(((walker_cfg.get("blueprints") or {}).get("include") or ["walker.pedestrian.*"]))
    exclude = [str(item) for item in ((walker_cfg.get("blueprints") or {}).get("exclude") or [])]
    blueprints: list[Any] = []
    for pattern in include:
        blueprints.extend(list(library.filter(str(pattern))) if hasattr(library, "filter") else [])
    result = [bp for bp in blueprints if not any(_matches(str(getattr(bp, "id", "")), pattern) for pattern in exclude)]
    rng.shuffle(result)
    return result


def _controller_blueprint(world: Any) -> Any | None:
    library = world.get_blueprint_library()
    matches = list(library.filter("controller.ai.walker")) if hasattr(library, "filter") else []
    return matches[0] if matches else None


def _try_spawn_actor(world: Any, blueprint: Any, transform: Any) -> Any | None:
    try:
        if hasattr(world, "try_spawn_actor"):
            return world.try_spawn_actor(blueprint, transform)
        if hasattr(world, "spawn_actor"):
            return world.spawn_actor(blueprint, transform)
    except Exception:
        return None
    return None


def _try_spawn_controller(world: Any, controller_blueprint: Any, walker: Any) -> Any | None:
    try:
        if hasattr(world, "spawn_actor"):
            return world.spawn_actor(controller_blueprint, _location_to_transform({"x": 0, "y": 0, "z": 0}), walker)
        if hasattr(world, "try_spawn_actor"):
            return world.try_spawn_actor(controller_blueprint, _location_to_transform({"x": 0, "y": 0, "z": 0}), walker)
    except Exception:
        return None
    return None


def _start_controller(controller: Any, destination: Any | None, speed_mps: float) -> bool:
    try:
        if hasattr(controller, "start"):
            controller.start()
        if destination is not None and hasattr(controller, "go_to_location"):
            controller.go_to_location(destination)
        if hasattr(controller, "set_max_speed"):
            controller.set_max_speed(speed_mps)
    except Exception:
        return False
    return True


def _random_nav_location(world: Any, *, rng: random.Random) -> Any | None:
    del rng
    if not hasattr(world, "get_random_location_from_navigation"):
        return None
    try:
        return world.get_random_location_from_navigation()
    except Exception:
        return None


def _location_to_transform(location: Any) -> Any:
    if hasattr(location, "location"):
        return location
    try:
        carla_module = __import__("carla")

        return carla_module.Transform(
            location if not isinstance(location, Mapping) else carla_module.Location(**location),
            carla_module.Rotation(),
        )
    except Exception:
        return location


def _sample_speed(walker_cfg: Mapping[str, Any], rng: random.Random) -> float:
    speed = walker_cfg.get("max_speed_mps")
    speed_range = walker_cfg.get("max_speed_mps_range")
    if isinstance(speed_range, list) and len(speed_range) == 2:
        return float(rng.uniform(float(speed_range[0]), float(speed_range[1])))
    return float(speed or 1.4)


def _set_blueprint_attr(blueprint: Any, name: str, value: str) -> None:
    try:
        if hasattr(blueprint, "has_attribute") and blueprint.has_attribute(name):
            blueprint.set_attribute(name, value)
        elif hasattr(blueprint, "set_attribute"):
            blueprint.set_attribute(name, value)
    except Exception:
        pass


def _register_actor(context: Any, actor: Any, *, role_name: str) -> None:
    registry = getattr(context, "actor_registry", None)
    if registry is None:
        return
    if hasattr(registry, "register"):
        try:
            registry.register(actor, role=role_name, control_source="carla_walker_ai_controller")
        except TypeError:
            registry.register(actor)


def _write_artifacts(
    context: Any,
    *,
    state: TrafficFlowState,
    flow_cfg: Mapping[str, Any],
    candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    controller_count: int,
    controller_started_count: int,
    provider: str,
) -> None:
    root = _artifact_root(context)
    if root is None:
        return
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    walker_cfg = flow_cfg.get("walkers") if isinstance(flow_cfg.get("walkers"), Mapping) else {}
    manifest = _walker_manifest(
        state=state,
        provider=provider,
        flow_cfg=flow_cfg,
        walker_cfg=walker_cfg,
        controller_count=controller_count,
        controller_started_count=controller_started_count,
    )
    (artifacts / "traffic_flow_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_jsonl(artifacts / "walker_spawn_candidates.jsonl", candidates)
    _append_jsonl(artifacts / "traffic_flow_events.jsonl", [{**event, "wall_time_sec": time.time()} for event in events])


def _walker_manifest(
    *,
    state: TrafficFlowState,
    provider: str,
    flow_cfg: Mapping[str, Any],
    walker_cfg: Mapping[str, Any],
    controller_count: int,
    controller_started_count: int,
) -> dict[str, Any]:
    actors = [_actor_info_to_dict(actor) for actor in state.actors]
    return {
        "schema_version": "traffic_flow_manifest.v1",
        "provider": provider,
        "enabled": state.enabled,
        "seed": state.seed,
        "background_walker_control_source": "carla_walker_ai_controller",
        "requested_vehicle_count": 0,
        "spawned_vehicle_count": 0,
        "requested_walker_count": state.requested_count,
        "spawned_walker_count": state.spawned_count,
        "controller_count": controller_count,
        "controller_started_count": controller_started_count,
        "world_pedestrians_seed": state.seed,
        "walker_cross_factor": float(walker_cfg.get("cross_factor", 0.0) or 0.0),
        "destroy_on_teardown": bool(flow_cfg.get("lifecycle", {}).get("destroy_on_teardown", True)),
        "walkers": actors,
        "actors": actors,
        "warnings": list(state.warnings),
        "errors": list(state.errors),
    }


def _actor_info_to_dict(actor: TrafficActorInfo) -> dict[str, Any]:
    payload = dict(actor.__dict__)
    payload.setdefault("control_source", actor.control_source or actor.provider)
    return payload


def _role_by_actor_id(state: TrafficFlowState | None) -> dict[int, str]:
    if state is None:
        return {}
    return {int(actor.actor_id): actor.role_name for actor in state.actors}


def _walker_trace_row(walker: Any, *, context: Any, role_name: str) -> dict[str, Any]:
    location = _actor_location(walker)
    velocity = _actor_velocity(walker)
    speed = _vector_norm(velocity)
    return {
        "schema_version": "walker_flow_trace.v1",
        "event_type": "walker_state",
        "sim_time_sec": _context_sim_time(context),
        "world_frame": _context_world_frame(context),
        "wall_time_sec": time.time(),
        "actor_id": int(getattr(walker, "id", -1)),
        "role_name": role_name,
        "x": _coord(location, "x"),
        "y": _coord(location, "y"),
        "z": _coord(location, "z"),
        "velocity_x": _coord(velocity, "x"),
        "velocity_y": _coord(velocity, "y"),
        "velocity_z": _coord(velocity, "z"),
        "speed_mps": speed,
        "velocity_source": "carla_actor_velocity" if velocity is not None else "missing",
        "destination_retarget_count": 0,
    }


def _actor_location(actor: Any) -> Any | None:
    try:
        if hasattr(actor, "get_transform"):
            transform = actor.get_transform()
            return getattr(transform, "location", None)
        return getattr(actor, "location", None)
    except Exception:
        return None


def _actor_velocity(actor: Any) -> Any | None:
    try:
        if hasattr(actor, "get_velocity"):
            return actor.get_velocity()
        return getattr(actor, "velocity", None)
    except Exception:
        return None


def _vector_norm(vector: Any | None) -> float | None:
    if vector is None:
        return None
    x = _coord(vector, "x")
    y = _coord(vector, "y")
    z = _coord(vector, "z")
    if x is None or y is None or z is None:
        return None
    return float((x * x + y * y + z * z) ** 0.5)


def _coord(obj: Any | None, name: str) -> float | None:
    if obj is None:
        return None
    try:
        if isinstance(obj, Mapping):
            value = obj.get(name)
        else:
            value = getattr(obj, name)
        return float(value)
    except (TypeError, ValueError, AttributeError):
        return None


def _context_sim_time(context: Any) -> float | None:
    for name in ("sim_time_sec", "sim_time", "timestamp_sec", "timestamp"):
        value = getattr(context, name, None)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _context_world_frame(context: Any) -> int | None:
    for name in ("world_frame", "frame", "frame_id"):
        value = getattr(context, name, None)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    return None


def _artifact_root(context: Any) -> Path | None:
    for name in ("run_dir", "artifact_root", "output_dir"):
        value = getattr(context, name, None)
        if value:
            return Path(value).expanduser()
    return None


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    text = "\n".join(json.dumps(row, sort_keys=True) for row in rows)
    if not text:
        return
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text + "\n")


def _distance_to_ego(context: Any, location: Any) -> float | None:
    ego_transform = getattr(context, "ego_transform", None)
    ego_location = getattr(ego_transform, "location", None) or getattr(context, "ego_location", None)
    if ego_location is None:
        return None
    try:
        return (
            (float(getattr(location, "x", 0.0)) - float(getattr(ego_location, "x", 0.0))) ** 2
            + (float(getattr(location, "y", 0.0)) - float(getattr(ego_location, "y", 0.0))) ** 2
        ) ** 0.5
    except Exception:
        return None


def _matches(text: str, pattern: str) -> bool:
    if pattern == "*":
        return True
    if pattern.startswith("*") and pattern.endswith("*"):
        return pattern.strip("*") in text
    if pattern.startswith("*"):
        return text.endswith(pattern[1:])
    if pattern.endswith("*"):
        return text.startswith(pattern[:-1])
    return text == pattern


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
