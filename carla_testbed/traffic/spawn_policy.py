from __future__ import annotations

import math
import random
from typing import Any, Callable, Mapping, Sequence

WaypointLookup = Callable[[Any], Any]


def select_spawn_points_near_route(
    spawn_points: Sequence[Any],
    *,
    ego_location: Any,
    route_points: Sequence[Any] = (),
    existing_actor_locations: Sequence[Any] = (),
    policy: Mapping[str, Any] | None = None,
    seed: int | None = None,
    count: int = 1,
    waypoint_lookup: WaypointLookup | None = None,
) -> tuple[list[Any], list[dict[str, Any]]]:
    policy = policy or {}
    min_distance = float(policy.get("min_distance_from_ego_m", 0.0) or 0.0)
    max_distance = float(policy.get("max_distance_from_ego_m", 1.0e9) or 1.0e9)
    avoid_existing = float(policy.get("avoid_existing_actor_radius_m", 0.0) or 0.0)
    avoid_junctions = bool(policy.get("avoid_junctions", False))
    route_near_threshold = float(policy.get("route_near_threshold_m", 30.0) or 30.0)
    same_road_preferred = bool(policy.get("same_road_preferred", False))
    rng = random.Random(seed)

    candidates: list[dict[str, Any]] = []
    for index, spawn_point in enumerate(spawn_points):
        location = _location(spawn_point)
        ego_distance = _distance_xy(location, ego_location)
        waypoint = waypoint_lookup(location) if waypoint_lookup else None
        is_junction = bool(getattr(waypoint, "is_junction", False)) if waypoint is not None else False
        road_id = getattr(waypoint, "road_id", None) if waypoint is not None else None
        lane_id = getattr(waypoint, "lane_id", None) if waypoint is not None else None
        distance_to_route = _distance_to_route(location, route_points)
        reject_reason = None
        if ego_distance < min_distance:
            reject_reason = "too_close_to_ego"
        elif ego_distance > max_distance:
            reject_reason = "too_far_from_ego"
        elif avoid_junctions and is_junction:
            reject_reason = "junction"
        elif avoid_existing > 0.0 and any(
            _distance_xy(location, existing) < avoid_existing for existing in existing_actor_locations
        ):
            reject_reason = "too_close_to_existing_actor"
        elif same_road_preferred and route_points and distance_to_route > route_near_threshold:
            reject_reason = "too_far_from_route"
        candidates.append(
            {
                "spawn_point_index": index,
                "spawn_transform": transform_to_dict(spawn_point),
                "distance_to_ego_m": ego_distance,
                "distance_to_route_m": distance_to_route,
                "road_id": road_id,
                "lane_id": lane_id,
                "is_junction": is_junction,
                "selected": False,
                "reject_reason": reject_reason,
                "_spawn_point": spawn_point,
            }
        )

    accepted = [item for item in candidates if item["reject_reason"] is None]
    rng.shuffle(accepted)
    if same_road_preferred and route_points:
        accepted.sort(key=lambda item: (item["distance_to_route_m"], item["distance_to_ego_m"]))
    selected_indices = {id(item) for item in accepted[: max(0, int(count))]}
    selected: list[Any] = []
    public_candidates: list[dict[str, Any]] = []
    for item in candidates:
        public = {key: value for key, value in item.items() if key != "_spawn_point"}
        if id(item) in selected_indices:
            public["selected"] = True
            selected.append(item["_spawn_point"])
        public_candidates.append(public)
    return selected, public_candidates


def transform_to_dict(transform: Any) -> dict[str, float | None]:
    location = _location(transform)
    rotation = getattr(transform, "rotation", None)
    if isinstance(transform, Mapping):
        rotation = transform.get("rotation", transform)
    return {
        "x": _number(_get_attr(location, "x")),
        "y": _number(_get_attr(location, "y")),
        "z": _number(_get_attr(location, "z")),
        "yaw": _number(_get_attr(rotation, "yaw")) if rotation is not None else None,
    }


def _location(value: Any) -> Any:
    if isinstance(value, Mapping):
        return value.get("location", value)
    return getattr(value, "location", value)


def _distance_xy(left: Any, right: Any) -> float:
    return math.hypot(
        float(_get_attr(left, "x") or 0.0) - float(_get_attr(right, "x") or 0.0),
        float(_get_attr(left, "y") or 0.0) - float(_get_attr(right, "y") or 0.0),
    )


def _distance_to_route(location: Any, route_points: Sequence[Any]) -> float:
    if not route_points:
        return 0.0
    return min(_distance_xy(location, point) for point in route_points)


def _get_attr(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None
