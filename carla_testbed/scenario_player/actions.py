from __future__ import annotations

from typing import Any, Mapping


def action_target_speed_mps(action: Mapping[str, Any] | None, *, sim_time_sec: float | None = None) -> float | None:
    if not action:
        return None
    if action.get("type") == "speed_profile":
        return speed_profile_target_mps(
            action.get("profile"),
            sim_time_sec=sim_time_sec,
            interpolation=str(action.get("interpolation") or "step"),
        )
    for key in ("target_speed_mps", "speed_mps", "hold_speed_mps"):
        value = action.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    if action.get("type") in {"brake_to_stop", "hold_stop"}:
        return 0.0
    return None


def speed_profile_target_mps(
    profile: Any,
    *,
    sim_time_sec: float | None,
    interpolation: str = "step",
) -> float | None:
    if not isinstance(profile, list) or not profile:
        return None
    points = _valid_profile_points(profile)
    if not points:
        return None
    if sim_time_sec is None:
        return points[0][1]
    sim_time = float(sim_time_sec)
    if sim_time <= points[0][0]:
        return points[0][1]
    for (t0, v0), (t1, v1) in zip(points, points[1:]):
        if sim_time <= t1:
            if interpolation == "linear" and t1 > t0:
                ratio = max(0.0, min(1.0, (sim_time - t0) / (t1 - t0)))
                return v0 + (v1 - v0) * ratio
            return v0
    return points[-1][1]


def _valid_profile_points(profile: list[Any]) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for point in profile:
        if not isinstance(point, Mapping) or point.get("speed_mps") is None:
            continue
        try:
            points.append((float(point.get("t", 0.0) or 0.0), float(point["speed_mps"])))
        except (TypeError, ValueError):
            continue
    return sorted(points, key=lambda item: item[0])


def action_controller_name(action: Mapping[str, Any] | None) -> str | None:
    if not action:
        return None
    return str(action.get("controller") or action.get("type"))


def action_for_role(actions: list[Mapping[str, Any]], role: str) -> Mapping[str, Any] | None:
    for action in actions:
        if str(action.get("role")) == role:
            return action
    return None
