from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any, Mapping

from .base import TrafficActorInfo, TrafficFlowState
from .spawn_policy import select_spawn_points_near_route, transform_to_dict


class CarlaTrafficManagerFlow:
    name = "carla_traffic_manager"

    def __init__(self) -> None:
        self._actors: list[Any] = []
        self._state: TrafficFlowState | None = None

    def setup(self, context: Any, config: dict[str, Any]) -> TrafficFlowState:
        flow_cfg = dict(config.get("traffic_flow") or config)
        enabled = bool(flow_cfg.get("enabled", False))
        seed = _int_or_none(flow_cfg.get("seed"))
        vehicle_cfg = flow_cfg.get("vehicles") if isinstance(flow_cfg.get("vehicles"), Mapping) else {}
        requested = int(vehicle_cfg.get("count", 0) or 0)
        if not enabled:
            self._state = TrafficFlowState(
                provider=self.name,
                enabled=False,
                seed=seed,
                requested_count=0,
                spawned_count=0,
            )
            return self._state

        client = getattr(context, "carla_client", None)
        world = getattr(context, "world", None)
        if client is None or world is None:
            self._state = TrafficFlowState(
                provider=self.name,
                enabled=True,
                seed=seed,
                requested_count=requested,
                spawned_count=0,
                errors=["carla_client_or_world_missing"],
            )
            return self._state

        rng = random.Random(seed)
        tm_cfg = flow_cfg.get("traffic_manager") if isinstance(flow_cfg.get("traffic_manager"), Mapping) else {}
        tm_port = int(tm_cfg.get("port", 8000) or 8000)
        traffic_manager = client.get_trafficmanager(tm_port)
        world_sync = bool(getattr(world.get_settings(), "synchronous_mode", False))
        tm_sync_effective = _configure_traffic_manager(traffic_manager, tm_cfg, world_sync=world_sync, seed=seed)
        selected_spawns, candidates = _select_spawns(context, world, vehicle_cfg, flow_cfg, seed=seed, count=requested)
        blueprints = _select_blueprints(world, vehicle_cfg, rng=rng)

        actors: list[TrafficActorInfo] = []
        events: list[dict[str, Any]] = []
        warnings: list[str] = []
        errors: list[str] = []
        for index, spawn_transform in enumerate(selected_spawns[:requested]):
            if not blueprints:
                errors.append("no_vehicle_blueprints_available")
                break
            blueprint = blueprints[index % len(blueprints)]
            role_name = f"{vehicle_cfg.get('role_name_prefix', 'background_vehicle')}_{index}"
            _set_blueprint_role_name(blueprint, role_name)
            actor = world.try_spawn_actor(blueprint, spawn_transform)
            events.append({"event": "spawn_attempt", "role_name": role_name, "status": "ok" if actor else "failed"})
            if actor is None:
                warnings.append(f"spawn_failed:{role_name}")
                continue
            actor.set_autopilot(True, tm_port)
            behavior = _sample_behavior(flow_cfg.get("behavior") or vehicle_cfg.get("behavior") or {}, rng)
            _apply_behavior(traffic_manager, actor, behavior, vehicle_lights=bool(tm_cfg.get("vehicle_lights", True)))
            _register_actor(context, actor, role_name=role_name)
            self._actors.append(actor)
            actors.append(
                TrafficActorInfo(
                    actor_id=int(getattr(actor, "id", -1)),
                    role_name=role_name,
                    blueprint_id=str(getattr(blueprint, "id", getattr(actor, "type_id", "unknown"))),
                    provider=self.name,
                    control_source="carla_traffic_manager",
                    tm_port=tm_port,
                    spawn_point_index=_spawn_index_for_transform(candidates, spawn_transform),
                    spawn_transform=transform_to_dict(spawn_transform),
                    behavior=behavior,
                )
            )
            events.append(
                {
                    "event": "spawned",
                    "actor_id": int(getattr(actor, "id", -1)),
                    "role_name": role_name,
                }
            )
            events.append(
                {
                    "event": "autopilot_enabled",
                    "actor_id": int(getattr(actor, "id", -1)),
                    "role_name": role_name,
                    "tm_port": tm_port,
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
            tm_port=tm_port,
            world_sync=world_sync,
            tm_sync_effective=tm_sync_effective,
            candidates=candidates,
            events=events,
        )
        return state

    def tick(self, context: Any) -> None:
        del context

    def teardown(self, context: Any) -> None:
        del context
        for actor in reversed(self._actors):
            try:
                actor.destroy()
            except Exception:
                pass
        self._actors.clear()


def _configure_traffic_manager(
    traffic_manager: Any,
    tm_cfg: Mapping[str, Any],
    *,
    world_sync: bool,
    seed: int | None,
) -> bool:
    requested = tm_cfg.get("synchronous_mode", "follow_world")
    effective = world_sync if requested == "follow_world" else bool(requested)
    if hasattr(traffic_manager, "set_synchronous_mode"):
        traffic_manager.set_synchronous_mode(effective)
    if bool(tm_cfg.get("deterministic", True)) and seed is not None and hasattr(traffic_manager, "set_random_device_seed"):
        traffic_manager.set_random_device_seed(seed)
    if hasattr(traffic_manager, "set_global_distance_to_leading_vehicle"):
        traffic_manager.set_global_distance_to_leading_vehicle(
            float(tm_cfg.get("global_distance_to_leading_vehicle_m", 8.0) or 8.0)
        )
    elif hasattr(traffic_manager, "global_distance_to_leading_vehicle"):
        traffic_manager.global_distance_to_leading_vehicle(
            float(tm_cfg.get("global_distance_to_leading_vehicle_m", 8.0) or 8.0)
        )
    if hasattr(traffic_manager, "global_percentage_speed_difference"):
        traffic_manager.global_percentage_speed_difference(
            float(tm_cfg.get("global_percentage_speed_difference", 0.0) or 0.0)
        )
    return bool(effective)


def _select_spawns(
    context: Any,
    world: Any,
    vehicle_cfg: Mapping[str, Any],
    flow_cfg: Mapping[str, Any],
    *,
    seed: int | None,
    count: int,
) -> tuple[list[Any], list[dict[str, Any]]]:
    spawn_points = list(world.get_map().get_spawn_points())
    ego_transform = getattr(context, "ego_transform", None)
    ego_location = getattr(ego_transform, "location", None) or getattr(context, "ego_location", None) or {"x": 0, "y": 0, "z": 0}
    route_points = list(getattr(context, "route_points", []) or [])
    existing = list(getattr(context, "existing_actor_locations", []) or [])
    spawn_policy = vehicle_cfg.get("spawn_policy") if isinstance(vehicle_cfg.get("spawn_policy"), Mapping) else {}
    waypoint_lookup = None
    if hasattr(world.get_map(), "get_waypoint"):
        waypoint_lookup = world.get_map().get_waypoint
    return select_spawn_points_near_route(
        spawn_points,
        ego_location=ego_location,
        route_points=route_points,
        existing_actor_locations=existing,
        policy=spawn_policy,
        seed=seed,
        count=count,
        waypoint_lookup=waypoint_lookup,
    )


def _select_blueprints(world: Any, vehicle_cfg: Mapping[str, Any], *, rng: random.Random) -> list[Any]:
    library = world.get_blueprint_library()
    include = list(((vehicle_cfg.get("blueprints") or {}).get("include") or ["vehicle.*"]))
    exclude = [str(item) for item in ((vehicle_cfg.get("blueprints") or {}).get("exclude") or [])]
    blueprints: list[Any] = []
    for pattern in include:
        matches = list(library.filter(str(pattern))) if hasattr(library, "filter") else []
        blueprints.extend(matches)
    result = [bp for bp in blueprints if not any(_matches(str(getattr(bp, "id", "")), pattern) for pattern in exclude)]
    rng.shuffle(result)
    return result


def _set_blueprint_role_name(blueprint: Any, role_name: str) -> None:
    try:
        if hasattr(blueprint, "has_attribute") and blueprint.has_attribute("role_name"):
            blueprint.set_attribute("role_name", role_name)
        elif hasattr(blueprint, "set_attribute"):
            blueprint.set_attribute("role_name", role_name)
    except Exception:
        pass


def _sample_behavior(behavior_cfg: Mapping[str, Any], rng: random.Random) -> dict[str, Any]:
    def ranged(name: str, default: float) -> float:
        range_value = behavior_cfg.get(f"{name}_range")
        if isinstance(range_value, list) and len(range_value) == 2:
            return float(rng.uniform(float(range_value[0]), float(range_value[1])))
        return float(behavior_cfg.get(name, default) or default)

    return {
        "percentage_speed_difference": ranged("percentage_speed_difference", 0.0),
        "distance_to_leading_vehicle_m": ranged("distance_to_leading_vehicle_m", 8.0),
        "auto_lane_change": bool(behavior_cfg.get("auto_lane_change", True)),
        "random_left_lanechange_percentage": float(behavior_cfg.get("random_left_lanechange_percentage", 0.0) or 0.0),
        "random_right_lanechange_percentage": float(behavior_cfg.get("random_right_lanechange_percentage", 0.0) or 0.0),
        "ignore_lights_percentage": float(behavior_cfg.get("ignore_lights_percentage", 0.0) or 0.0),
        "ignore_signs_percentage": float(behavior_cfg.get("ignore_signs_percentage", 0.0) or 0.0),
        "ignore_vehicles_percentage": float(behavior_cfg.get("ignore_vehicles_percentage", 0.0) or 0.0),
        "ignore_walkers_percentage": float(behavior_cfg.get("ignore_walkers_percentage", 0.0) or 0.0),
    }


def _apply_behavior(traffic_manager: Any, actor: Any, behavior: Mapping[str, Any], *, vehicle_lights: bool) -> None:
    calls = {
        "vehicle_percentage_speed_difference": behavior.get("percentage_speed_difference"),
        "distance_to_leading_vehicle": behavior.get("distance_to_leading_vehicle_m"),
        "auto_lane_change": behavior.get("auto_lane_change"),
        "random_left_lanechange_percentage": behavior.get("random_left_lanechange_percentage"),
        "random_right_lanechange_percentage": behavior.get("random_right_lanechange_percentage"),
        "ignore_lights_percentage": behavior.get("ignore_lights_percentage"),
        "ignore_signs_percentage": behavior.get("ignore_signs_percentage"),
        "ignore_vehicles_percentage": behavior.get("ignore_vehicles_percentage"),
        "ignore_walkers_percentage": behavior.get("ignore_walkers_percentage"),
    }
    for method_name, value in calls.items():
        method = getattr(traffic_manager, method_name, None)
        if callable(method):
            method(actor, value)
    if vehicle_lights and hasattr(traffic_manager, "update_vehicle_lights"):
        traffic_manager.update_vehicle_lights(actor, True)


def _register_actor(context: Any, actor: Any, *, role_name: str) -> None:
    registry = getattr(context, "actor_registry", None)
    if registry is None:
        return
    if hasattr(registry, "register"):
        try:
            registry.register(actor, role=role_name, control_source="carla_traffic_manager")
        except TypeError:
            registry.register(actor)


def _write_artifacts(
    context: Any,
    *,
    state: TrafficFlowState,
    flow_cfg: Mapping[str, Any],
    tm_port: int,
    world_sync: bool,
    tm_sync_effective: bool,
    candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> None:
    root = _artifact_root(context)
    if root is None:
        return
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": "traffic_flow_manifest.v1",
        "provider": state.provider,
        "enabled": state.enabled,
        "seed": state.seed,
        "tm_port": tm_port,
        "world_synchronous_mode": world_sync,
        "tm_synchronous_mode_requested": flow_cfg.get("traffic_manager", {}).get("synchronous_mode", "follow_world"),
        "tm_synchronous_mode_effective": tm_sync_effective,
        "requested_vehicle_count": state.requested_count,
        "spawned_vehicle_count": state.spawned_count,
        "destroy_on_teardown": bool(flow_cfg.get("lifecycle", {}).get("destroy_on_teardown", True)),
        "actors": [_actor_info_to_dict(actor) for actor in state.actors],
        "warnings": list(state.warnings),
        "errors": list(state.errors),
    }
    (artifacts / "traffic_flow_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_jsonl(artifacts / "traffic_spawn_candidates.jsonl", candidates)
    now = time.time()
    _write_jsonl(artifacts / "traffic_flow_events.jsonl", [{**event, "wall_time_sec": now} for event in events])


def _actor_info_to_dict(actor: TrafficActorInfo) -> dict[str, Any]:
    payload = dict(actor.__dict__)
    payload.setdefault("control_source", actor.control_source or actor.provider)
    return payload


def _artifact_root(context: Any) -> Path | None:
    for name in ("run_dir", "artifact_root", "output_dir"):
        value = getattr(context, name, None)
        if value:
            return Path(value).expanduser()
    return None


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""), encoding="utf-8")


def _spawn_index_for_transform(candidates: list[dict[str, Any]], spawn_transform: Any) -> int | None:
    target = transform_to_dict(spawn_transform)
    for candidate in candidates:
        if not candidate.get("selected"):
            continue
        if candidate.get("spawn_transform") == target:
            return int(candidate.get("spawn_point_index"))
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
