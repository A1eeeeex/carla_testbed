from __future__ import annotations

import math
import random
from typing import Any, Mapping, Sequence


def select_walker_spawn_locations(
    candidate_locations: Sequence[Any],
    *,
    ego_location: Any,
    route_points: Sequence[Any] = (),
    existing_actor_locations: Sequence[Any] = (),
    policy: Mapping[str, Any] | None = None,
    seed: int | None = None,
    count: int = 1,
) -> tuple[list[Any], list[dict[str, Any]]]:
    policy = policy or {}
    min_distance = float(policy.get("min_distance_from_ego_m", 8.0) or 8.0)
    max_distance = float(policy.get("max_distance_from_ego_m", 120.0) or 120.0)
    avoid_existing = float(policy.get("avoid_existing_actor_radius_m", 2.0) or 2.0)
    route_near_threshold = float(policy.get("route_near_threshold_m", 60.0) or 60.0)
    route_near_preferred = bool(policy.get("route_near_preferred", True))
    rng = random.Random(seed)

    candidates: list[dict[str, Any]] = []
    for index, location in enumerate(candidate_locations):
        ego_distance = _distance_xy(location, ego_location)
        distance_to_route = _distance_to_route(location, route_points)
        reject_reason = None
        if ego_distance < min_distance:
            reject_reason = "too_close_to_ego"
        elif ego_distance > max_distance:
            reject_reason = "too_far_from_ego"
        elif route_points and route_near_preferred and distance_to_route > route_near_threshold:
            reject_reason = "too_far_from_route"
        elif avoid_existing > 0.0 and any(
            _distance_xy(location, existing) < avoid_existing for existing in existing_actor_locations
        ):
            reject_reason = "too_close_to_existing_actor"
        candidates.append(
            {
                "candidate_index": index,
                "location": location_to_dict(location),
                "distance_to_ego_m": ego_distance,
                "distance_to_route_m": distance_to_route,
                "selected": False,
                "reject_reason": reject_reason,
                "_location": location,
            }
        )

    accepted = [item for item in candidates if item["reject_reason"] is None]
    rng.shuffle(accepted)
    if route_near_preferred and route_points:
        accepted.sort(key=lambda item: (item["distance_to_route_m"], item["distance_to_ego_m"]))
    selected_ids = {id(item) for item in accepted[: max(0, int(count))]}
    selected: list[Any] = []
    public_candidates: list[dict[str, Any]] = []
    for item in candidates:
        public = {key: value for key, value in item.items() if key != "_location"}
        if id(item) in selected_ids:
            public["selected"] = True
            selected.append(item["_location"])
        public_candidates.append(public)
    return selected, public_candidates


def location_to_dict(location: Any) -> dict[str, float | None]:
    return {
        "x": _number(_get_attr(location, "x")),
        "y": _number(_get_attr(location, "y")),
        "z": _number(_get_attr(location, "z")),
    }


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
