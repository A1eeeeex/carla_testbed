from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from carla_testbed.routes.geometry import heading_error, nearest_route_index, project_onto_route
from carla_testbed.routes.schema import RouteDefinition, RoutePoint

ROUTE_CURVE_FIELDS_SCHEMA_VERSION = "route_curve_p0.v1"

ROUTE_CURVE_P0_FIELDS = [
    "sim_time",
    "frame_id",
    "route_id",
    "route_index",
    "nearest_route_index",
    "route_s",
    "route_x",
    "route_y",
    "route_z",
    "route_heading",
    "route_curvature",
    "ego_x",
    "ego_y",
    "ego_z",
    "ego_heading",
    "ego_speed",
    "ego_yaw_rate",
    "cross_track_error",
    "heading_error",
    "curvature_at_nearest",
    "curve_segment_id",
    "is_curve_segment",
    "is_junction_segment",
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "throttle_raw",
    "throttle_mapped",
    "throttle_applied",
    "brake_raw",
    "brake_mapped",
    "brake_applied",
    "lateral_guard_applied",
    "trajectory_contract_guard_applied",
]

ROUTE_CURVE_P1_FIELDS = [
    "apollo_matched_point_index",
    "apollo_matched_point_distance",
    "apollo_target_point_index",
    "apollo_target_point_distance",
    "apollo_trajectory_heading",
    "apollo_trajectory_curvature",
]

ROUTE_CURVE_P2_FIELDS = [
    "localization_timestamp",
    "localization_header_timestamp_sec",
    "localization_measurement_time",
    "localization_sequence_num",
    "localization_module_name",
    "localization_frame_id",
    "localization_carla_frame_id",
    "localization_time_base",
    "localization_heading",
    "localization_orientation_yaw",
    "orientation_heading_diff_rad",
    "heading_source",
    "orientation_convention",
    "chassis_timestamp",
    "chassis_speed_mps",
    "planning_timestamp",
    "control_timestamp",
    "control_latency_ms",
    "planning_message_age_ms",
    "localization_message_age_ms",
    "control_message_age_ms",
    "localization_speed_mps",
    "velocity_norm_vs_chassis_speed_mps",
    "heading_fd_yaw_rate",
    "yaw_rate_vs_heading_fd_rad_s",
    "localization_angular_velocity_unit",
    "linear_acceleration_available",
    "linear_acceleration_vrf_available",
    "angular_velocity_vrf_available",
    "acceleration_source",
    "calibration_profile_id",
    "actuator_mapping_mode",
    "steer_scale",
]


def _get(source: Any, *names: str) -> Any:
    if source is None:
        return None
    for name in names:
        if isinstance(source, Mapping):
            if name in source:
                return source.get(name)
        elif hasattr(source, name):
            return getattr(source, name)
    return None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _bool_or_none(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _point_by_route_index(route: RouteDefinition, index: int | None) -> RoutePoint | None:
    if index is None or not route.points:
        return None
    for point in route.points:
        if int(point.index) == int(index):
            return point
    if 0 <= int(index) < len(route.points):
        return route.points[int(index)]
    return None


def _curve_segment_id(route: RouteDefinition, route_index: int | None, *, curvature_abs_threshold: float) -> int | None:
    if route_index is None:
        return None
    current_segment_id = -1
    active = False
    for point in route.points:
        curvature = _float_or_none(point.curvature)
        is_curve = curvature is not None and abs(curvature) >= float(curvature_abs_threshold)
        if is_curve and not active:
            active = True
            current_segment_id += 1
        if not is_curve:
            active = False
        if int(point.index) == int(route_index):
            return current_segment_id if is_curve else None
    return None


def _is_junction_point(point: RoutePoint | None) -> bool | None:
    if point is None:
        return None
    tags = {str(tag).strip().lower() for tag in point.tags}
    if tags:
        return bool(tags.intersection({"junction", "intersection", "turn"}))
    lane_id = str(point.lane_id or "").lower()
    if lane_id:
        return "junction" in lane_id or "intersection" in lane_id
    return None


def _heading_from_pose(ego_pose: Any) -> float | None:
    return _float_or_none(_get(ego_pose, "heading", "yaw", "ego_heading"))


def _control_value(source: Any, *names: str) -> float | None:
    return _float_or_none(_get(source, *names))


def _semantic_value(source: Any, *names: str) -> float | None:
    """Read Apollo/bridge debug semantics without requiring full route geometry."""
    return _float_or_none(_get(source, *names))


def ensure_route_curve_p0_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of ``row`` where every P0 diagnostic field exists."""
    payload = dict(row)
    for field in ROUTE_CURVE_P0_FIELDS:
        payload.setdefault(field, None)
    return payload


def build_route_curve_row(
    *,
    sim_time: float | None = None,
    frame_id: int | None = None,
    route_id: str | None = None,
    route: RouteDefinition | None = None,
    route_index: int | None = None,
    ego_pose: Mapping[str, Any] | Any | None = None,
    ego_speed: float | None = None,
    ego_yaw_rate: float | None = None,
    apollo_control: Mapping[str, Any] | Any | None = None,
    raw_control: Mapping[str, Any] | Any | None = None,
    mapped_control: Mapping[str, Any] | Any | None = None,
    applied_control: Mapping[str, Any] | Any | None = None,
    guard_flags: Mapping[str, Any] | Any | None = None,
    is_junction_segment: bool | None = None,
    curvature_abs_threshold: float = 0.03,
) -> dict[str, Any]:
    """Build one route/curve diagnostics row.

    Missing route, ego, Apollo, mapping, or guard context degrades to ``None``
    per field instead of dropping columns. P1/P2 details intentionally remain
    out of this helper for now.
    """
    ego_x = _float_or_none(_get(ego_pose, "x", "ego_x"))
    ego_y = _float_or_none(_get(ego_pose, "y", "ego_y"))
    ego_z = _float_or_none(_get(ego_pose, "z", "ego_z"))
    ego_heading = _heading_from_pose(ego_pose)

    nearest_index: int | None = None
    projection = None
    route_point: RoutePoint | None = None
    if route is not None and ego_x is not None and ego_y is not None:
        nearest_index = nearest_route_index(route.points, ego_x, ego_y)
        projection = project_onto_route(route.points, ego_x, ego_y, extend_ends=True)
    if route_index is None:
        route_index = projection.nearest_index if projection is not None else nearest_index
    if route is not None:
        route_point = _point_by_route_index(route, route_index)
    if is_junction_segment is None:
        is_junction_segment = _is_junction_point(route_point)

    route_heading = None if route_point is None else route_point.heading
    if route_heading is None and projection is not None:
        route_heading = projection.heading
    semantic_curvature = _semantic_value(
        apollo_control,
        "route_curvature",
        "curvature_at_nearest",
        "kappa",
        "target_point_kappa",
        "apollo_trajectory_curvature",
    )
    route_curvature = projection.curvature if projection is not None else (None if route_point is None else route_point.curvature)
    if route_curvature is None:
        route_curvature = semantic_curvature
    segment_id = None
    if route is not None and route_index is not None:
        segment_id = _curve_segment_id(route, route_index, curvature_abs_threshold=curvature_abs_threshold)
    semantic_lateral_error = _semantic_value(
        apollo_control,
        "cross_track_error",
        "lateral_error",
        "e_y",
        "simple_lat_lateral_error",
    )
    semantic_heading_error = _semantic_value(
        apollo_control,
        "heading_error",
        "e_psi",
        "simple_lat_heading_error",
    )
    computed_cross_track_error = None if projection is None else projection.cross_track_error
    computed_heading_error = None if ego_heading is None else heading_error(ego_heading, route_heading)

    row = {
        "sim_time": None if sim_time is None else float(sim_time),
        "frame_id": None if frame_id is None else int(frame_id),
        "route_id": route_id or (route.route_id if route is not None else None) or _get(apollo_control, "route_id"),
        "route_index": route_index,
        "nearest_route_index": nearest_index,
        "route_s": projection.s if projection is not None else (None if route_point is None else route_point.s),
        "route_x": projection.x if projection is not None else (None if route_point is None else route_point.x),
        "route_y": projection.y if projection is not None else (None if route_point is None else route_point.y),
        "route_z": projection.z if projection is not None else (None if route_point is None else route_point.z),
        "route_heading": route_heading,
        "route_curvature": route_curvature,
        "ego_x": ego_x,
        "ego_y": ego_y,
        "ego_z": ego_z,
        "ego_heading": ego_heading,
        "ego_speed": None if ego_speed is None else float(ego_speed),
        "ego_yaw_rate": None if ego_yaw_rate is None else float(ego_yaw_rate),
        "cross_track_error": computed_cross_track_error
        if computed_cross_track_error is not None
        else semantic_lateral_error,
        "heading_error": computed_heading_error if computed_heading_error is not None else semantic_heading_error,
        "curvature_at_nearest": route_curvature,
        "curve_segment_id": segment_id,
        "is_curve_segment": None if route_curvature is None else bool(abs(float(route_curvature)) >= curvature_abs_threshold),
        "is_junction_segment": is_junction_segment,
        "apollo_steer_raw": _control_value(
            apollo_control,
            "apollo_steer_raw",
            "raw_steer",
            "steering_target",
            "steer",
        ),
        "bridge_steer_mapped": _control_value(mapped_control, "bridge_steer_mapped", "mapped_steer", "steer"),
        "carla_steer_applied": _control_value(applied_control, "carla_steer_applied", "applied_steer", "steer"),
        "throttle_raw": _control_value(raw_control, "throttle_raw", "throttle"),
        "throttle_mapped": _control_value(mapped_control, "throttle_mapped", "mapped_throttle", "throttle"),
        "throttle_applied": _control_value(applied_control, "throttle_applied", "applied_throttle", "throttle"),
        "brake_raw": _control_value(raw_control, "brake_raw", "brake"),
        "brake_mapped": _control_value(mapped_control, "brake_mapped", "mapped_brake", "brake"),
        "brake_applied": _control_value(applied_control, "brake_applied", "applied_brake", "brake"),
        "lateral_guard_applied": _bool_or_none(
            _get(guard_flags, "lateral_guard_applied", "sustained_lateral_guard_applied")
        ),
        "trajectory_contract_guard_applied": _bool_or_none(
            _get(
                guard_flags,
                "trajectory_contract_guard_applied",
                "trajectory_contract_lateral_guard_applied",
            )
        ),
    }
    return ensure_route_curve_p0_fields(row)
