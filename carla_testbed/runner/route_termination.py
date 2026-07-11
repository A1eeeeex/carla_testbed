from __future__ import annotations

import math
from typing import Any, Mapping


NOMINAL_ROUTE_PROFILES = {"lane_keep", "lane_keep_straight", "lane_keep_curve", "curve_lane_follow"}


def evaluate_nominal_route_completion(
    scenario_metadata: Mapping[str, Any] | None,
    *,
    ego_x: Any,
    ego_y: Any,
    route_s: Any,
) -> dict[str, Any]:
    """Evaluate the explicit end condition for no-lead nominal route cases."""
    metadata = scenario_metadata if isinstance(scenario_metadata, Mapping) else {}
    profile = str(metadata.get("capability_profile") or "").strip().lower()
    lead_profile = metadata.get("lead_profile")
    lead_mode = (
        str(lead_profile.get("mode") or "").strip().lower()
        if isinstance(lead_profile, Mapping)
        else ""
    )
    has_front_actor = bool(metadata.get("front_actor_id"))
    eligible = profile in NOMINAL_ROUTE_PROFILES and not has_front_actor and lead_mode in {"", "none"}

    goal = metadata.get("goal")
    goal_x = _finite_number(goal.get("x")) if isinstance(goal, Mapping) else None
    goal_y = _finite_number(goal.get("y")) if isinstance(goal, Mapping) else None
    progress_m = _finite_number(route_s)
    route_length_m = _finite_number(
        metadata.get("claim_route_length_m") or metadata.get("route_length_m")
    )
    x = _finite_number(ego_x)
    y = _finite_number(ego_y)
    tolerance_m = _finite_number(metadata.get("route_goal_tolerance_m")) or 2.0

    missing_fields: list[str] = []
    for field, value in (
        ("goal.x", goal_x),
        ("goal.y", goal_y),
        ("route_s", progress_m),
        ("route_length_m", route_length_m),
        ("ego_x", x),
        ("ego_y", y),
    ):
        if value is None:
            missing_fields.append(field)

    goal_distance_m = None
    completion_ratio = None
    if x is not None and y is not None and goal_x is not None and goal_y is not None:
        goal_distance_m = math.hypot(x - goal_x, y - goal_y)
    if progress_m is not None and route_length_m is not None and route_length_m > 0.0:
        completion_ratio = progress_m / route_length_m

    reached = bool(
        eligible
        and not missing_fields
        and completion_ratio is not None
        and completion_ratio >= 0.95
        and goal_distance_m is not None
        and goal_distance_m <= tolerance_m
    )
    return {
        "eligible": eligible,
        "reached": reached,
        "capability_profile": profile or None,
        "route_completion_ratio": completion_ratio,
        "route_s": progress_m,
        "route_length_m": route_length_m,
        "goal_distance_m": goal_distance_m,
        "goal_tolerance_m": tolerance_m,
        "missing_fields": missing_fields,
    }


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
