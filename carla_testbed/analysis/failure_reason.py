from __future__ import annotations

from typing import Any, Mapping

FAILURE_REASONS = [
    "artifact_missing",
    "planning_missing",
    "control_missing",
    "no_control",
    "stuck",
    "off_route",
    "high_lateral_error",
    "heading_divergence",
    "collision",
    "lane_invasion",
    "timeout",
    "bridge_drop",
    "insufficient_data",
    "success",
]


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_failure(run_metrics: Mapping[str, Any], artifacts: Mapping[str, Any] | None = None) -> str:
    artifacts = artifacts or {}
    if artifacts.get("artifact_complete") is False or artifacts.get("route_health_missing"):
        return "artifact_missing"

    exit_reason = str(run_metrics.get("exit_reason") or run_metrics.get("status") or "").lower()
    if "timeout" in exit_reason:
        return "timeout"
    if run_metrics.get("bridge_drop") is True:
        return "bridge_drop"

    if _num(run_metrics.get("collision_count")) and _num(run_metrics.get("collision_count")) > 0:
        return "collision"
    if _num(run_metrics.get("lane_invasion_count")) and _num(run_metrics.get("lane_invasion_count")) > 0:
        return "lane_invasion"

    planning_hz = _num(run_metrics.get("planning_hz"))
    control_hz = _num(run_metrics.get("carla_applied_control_hz"))
    apollo_control_hz = _num(run_metrics.get("apollo_control_hz"))
    if planning_hz == 0:
        return "planning_missing"
    if control_hz == 0 and apollo_control_hz == 0:
        return "control_missing"
    if control_hz == 0:
        return "no_control"

    stuck_duration_s = _num(run_metrics.get("stuck_duration_s"))
    if stuck_duration_s is not None and stuck_duration_s >= 5.0:
        route_completion = _num(run_metrics.get("route_completion"))
        max_speed_mps = _num(run_metrics.get("max_speed_mps"))
        if not (
            route_completion is not None
            and route_completion >= 0.05
            and max_speed_mps is not None
            and max_speed_mps >= 1.0
        ):
            return "stuck"
    off_route_duration_s = _num(run_metrics.get("off_route_duration_s"))
    if off_route_duration_s is not None and off_route_duration_s >= 2.0:
        return "off_route"
    lateral_p95 = _num(run_metrics.get("lateral_error_p95_m"))
    lateral_max = _num(run_metrics.get("lateral_error_max_m"))
    if (lateral_p95 is not None and lateral_p95 > 1.5) or (lateral_max is not None and lateral_max > 2.5):
        return "high_lateral_error"
    heading_p95 = _num(run_metrics.get("heading_error_p95_rad"))
    heading_max = _num(run_metrics.get("heading_error_max_rad"))
    if (heading_p95 is not None and heading_p95 > 0.5) or (heading_max is not None and heading_max > 0.8):
        return "heading_divergence"

    required = ["route_completion", "lateral_error_p95_m", "heading_error_p95_rad", "planning_hz", "carla_applied_control_hz"]
    if any(run_metrics.get(key) is None for key in required):
        return "insufficient_data"
    return "success"
