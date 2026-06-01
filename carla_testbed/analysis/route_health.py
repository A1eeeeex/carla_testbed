from __future__ import annotations

import math
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence

from carla_testbed.routes.geometry import compute_spacing, heading_error, nearest_route_index
from carla_testbed.routes.schema import RouteDefinition, RouteHealthReport


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (len(cleaned) - 1) * float(q)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return cleaned[lower]
    weight = rank - lower
    return cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight


def _stats(values: Sequence[float]) -> dict[str, float | None]:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not cleaned:
        return {"mean": None, "p95": None, "max": None}
    return {"mean": mean(cleaned), "p95": _percentile(cleaned, 0.95), "max": max(cleaned)}


def _series(rows: Sequence[Mapping[str, Any]], names: Sequence[str], *, absolute: bool = False) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _scalar(row, *names)
        if value is not None:
            values.append(abs(value) if absolute else value)
    return values


def _scalar(row: Mapping[str, Any], *names: str) -> float | None:
    for name in names:
        if name not in row:
            continue
        value = row.get(name)
        if value is None or value == "":
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _route_length(route: RouteDefinition) -> float:
    if not route.points:
        return 0.0
    last = route.points[-1]
    if last.s is not None:
        return float(last.s)
    return sum(compute_spacing(route.points))


def _curve_direction(values: Sequence[float]) -> str:
    cleaned = [float(value) for value in values if math.isfinite(float(value))]
    if not cleaned:
        return "unknown"
    has_left = any(value > 0.0 for value in cleaned)
    has_right = any(value < 0.0 for value in cleaned)
    if has_left and has_right:
        return "mixed"
    if has_left:
        return "left"
    if has_right:
        return "right"
    return "unknown"


def detect_curve_segments(route: RouteDefinition, *, curvature_abs_threshold: float = 0.03) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    active_start: int | None = None
    segment_id = 0

    def close_segment(end_index: int) -> None:
        nonlocal active_start, segment_id
        if active_start is None:
            return
        points = route.points[active_start : end_index + 1]
        curvatures = [
            float(point.curvature)
            for point in points
            if point.curvature is not None and math.isfinite(float(point.curvature))
        ]
        start_s = route.points[active_start].s
        end_s = route.points[end_index].s
        length_m = None if start_s is None or end_s is None else max(0.0, float(end_s) - float(start_s))
        warning = ""
        if len(points) < 2:
            warning = "short_segment"
        segments.append(
            {
                "curve_segment_id": segment_id,
                "start_index": route.points[active_start].index,
                "end_index": route.points[end_index].index,
                "start_s": start_s,
                "end_s": end_s,
                "length_m": length_m,
                "mean_abs_curvature": mean(abs(value) for value in curvatures) if curvatures else None,
                "max_abs_curvature": max((abs(value) for value in curvatures), default=None),
                "direction": _curve_direction(curvatures),
                "warning": warning,
            }
        )
        segment_id += 1
        active_start = None

    for index, point in enumerate(route.points):
        curvature = point.curvature
        is_curve = curvature is not None and abs(float(curvature)) >= float(curvature_abs_threshold)
        if is_curve and active_start is None:
            active_start = index
        elif not is_curve and active_start is not None:
            close_segment(index - 1)
    if active_start is not None:
        close_segment(len(route.points) - 1)
    return segments


def _spawn_alignment(route: RouteDefinition, missing_fields: list[str]) -> dict[str, Any]:
    result = {
        "distance_m": None,
        "heading_error_rad": None,
        "direction_consistent": None,
    }
    if not route.spawn_pose:
        missing_fields.append("spawn_pose")
        return result
    if not route.points:
        return result
    try:
        spawn_x = float(route.spawn_pose["x"])
        spawn_y = float(route.spawn_pose["y"])
    except Exception:
        missing_fields.append("spawn_pose.x_y")
        return result
    first = route.points[0]
    result["distance_m"] = math.hypot(spawn_x - float(first.x), spawn_y - float(first.y))
    spawn_heading = route.spawn_pose.get("yaw", route.spawn_pose.get("heading"))
    if spawn_heading is None and route.spawn_pose.get("yaw_deg") is not None:
        spawn_heading = math.radians(float(route.spawn_pose["yaw_deg"]))
    if spawn_heading is None and route.spawn_pose.get("heading_deg") is not None:
        spawn_heading = math.radians(float(route.spawn_pose["heading_deg"]))
    if spawn_heading is None:
        missing_fields.append("spawn_pose.yaw")
        return result
    result["heading_error_rad"] = heading_error(float(spawn_heading), first.heading)
    result["direction_consistent"] = (
        None if result["heading_error_rad"] is None else abs(float(result["heading_error_rad"])) <= math.pi / 2.0
    )
    return result


def _route_direction_consistency(route: RouteDefinition) -> dict[str, Any]:
    warnings: list[str] = []
    for index in range(1, len(route.points)):
        prev_s = route.points[index - 1].s
        current_s = route.points[index].s
        if prev_s is not None and current_s is not None and float(current_s) < float(prev_s):
            warnings.append(f"s decreases at index {route.points[index].index}")
    for index, spacing in enumerate(compute_spacing(route.points), start=1):
        if spacing <= 1e-6:
            warnings.append(f"zero spacing before index {route.points[index].index}")
    return {"status": "ok" if not warnings else "warning", "warnings": warnings}


def _geometry_summary(
    route: RouteDefinition,
    missing_fields: list[str],
    *,
    curvature_abs_threshold: float,
) -> dict[str, Any]:
    spacing = compute_spacing(route.points)
    heading_jumps: list[tuple[int, float]] = []
    for index in range(1, len(route.points)):
        prev = route.points[index - 1].heading
        current = route.points[index].heading
        if prev is None or current is None:
            continue
        jump = abs(heading_error(float(current), float(prev)) or 0.0)
        heading_jumps.append((index, jump))
    curvature_values = [
        abs(float(point.curvature))
        for point in route.points
        if point.curvature is not None and math.isfinite(float(point.curvature))
    ]
    curvature_p95 = _percentile(curvature_values, 0.95)
    spike_threshold = max(float(curvature_p95 or 0.0) * 2.0, 1e-6)
    curvature_spikes = [
        point.index
        for point in route.points
        if point.curvature is not None and abs(float(point.curvature)) > spike_threshold
    ]
    curve_segments = detect_curve_segments(route, curvature_abs_threshold=curvature_abs_threshold)
    return {
        "point_count": len(route.points),
        "length_m": _route_length(route),
        "spacing": {
            "mean_m": mean(spacing) if spacing else None,
            "p95_m": _percentile(spacing, 0.95),
            "max_m": max(spacing) if spacing else None,
        },
        "heading": {
            "max_jump_rad": max((jump for _, jump in heading_jumps), default=None),
            "jump_locations": [index for index, jump in heading_jumps if jump > 0.5],
        },
        "curvature": {
            "mean_abs": mean(curvature_values) if curvature_values else None,
            "p95_abs": curvature_p95,
            "max_abs": max(curvature_values) if curvature_values else None,
            "spike_locations": curvature_spikes,
        },
        "curve_segments_count": len(curve_segments),
        "curve_segments": curve_segments,
        "spawn_alignment": _spawn_alignment(route, missing_fields),
        "route_direction_consistency": _route_direction_consistency(route),
    }


def _curvature_bucket(route: RouteDefinition, row: Mapping[str, Any]) -> str:
    row_curvature = _scalar(row, "route_curvature", "curvature_at_nearest")
    if row_curvature is not None:
        return _curvature_bucket_from_value(row_curvature)
    x = _scalar(row, "x", "ego_x", "position_x")
    y = _scalar(row, "y", "ego_y", "position_y")
    if x is None or y is None:
        return "unknown"
    index = nearest_route_index(route.points, x, y)
    if index is None:
        return "unknown"
    curvature = route.points[index].curvature
    if curvature is None:
        return "unknown"
    return _curvature_bucket_from_value(float(curvature))


def _curvature_bucket_from_value(curvature: float) -> str:
    abs_curvature = abs(float(curvature))
    if abs_curvature < 1e-4:
        return "straight"
    if abs_curvature < 0.02:
        return "gentle_curve"
    return "sharp_curve"


def _metric_by_bucket(route: RouteDefinition, rows: Sequence[Mapping[str, Any]], field_names: Sequence[str]) -> dict[str, Any]:
    buckets: dict[str, list[float]] = {}
    for row in rows:
        value = _scalar(row, *field_names)
        if value is None:
            continue
        bucket = _curvature_bucket(route, row)
        buckets.setdefault(bucket, []).append(abs(value))
    return {bucket: _stats(values) for bucket, values in sorted(buckets.items())}


def _derived_yaw_rate_series(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    values: list[float] = []
    prev_time: float | None = None
    prev_heading: float | None = None
    for row in rows:
        current_time = _scalar(row, "sim_time", "time_s", "t", "timestamp", "wall_time_s")
        current_heading = _scalar(row, "ego_heading", "ego_yaw", "yaw_rad")
        if current_time is None or current_heading is None:
            continue
        if prev_time is not None and prev_heading is not None:
            dt = current_time - prev_time
            if dt > 1e-9:
                delta = heading_error(current_heading, prev_heading)
                if delta is not None:
                    values.append(delta / dt)
        prev_time = current_time
        prev_heading = current_heading
    return values


def _run_metrics(
    route: RouteDefinition,
    rows: Sequence[Mapping[str, Any]] | None,
    missing_fields: list[str],
    missing_inputs: list[str],
) -> dict[str, Any]:
    keys = {
        "lateral_error": ("lateral_error", "lateral_error_m", "cross_track_error", "cross_track_error_m", "e_y", "dbg_e_y"),
        "heading_error": ("heading_error", "heading_error_rad", "heading_error_m", "e_psi", "dbg_e_psi"),
        "ego_speed": ("ego_speed", "ego_speed_mps", "speed", "speed_mps"),
        "ego_yaw_rate": ("ego_yaw_rate", "ego_yaw_rate_rad_s", "yaw_rate", "yaw_rate_rad_s"),
        "throttle_raw": ("throttle_raw", "apollo_throttle_raw", "raw_throttle"),
        "throttle_mapped": ("throttle_mapped", "bridge_throttle_mapped", "mapped_throttle"),
        "throttle_applied": ("throttle_applied", "carla_throttle_applied", "applied_throttle"),
        "brake_raw": ("brake_raw", "apollo_brake_raw", "raw_brake"),
        "brake_mapped": ("brake_mapped", "bridge_brake_mapped", "mapped_brake"),
        "brake_applied": ("brake_applied", "carla_brake_applied", "applied_brake"),
    }
    empty = {
        "lateral_error_mean_m": None,
        "lateral_error_p95_m": None,
        "lateral_error_max_m": None,
        "heading_error_mean_rad": None,
        "heading_error_p95_rad": None,
        "heading_error_max_rad": None,
        "ego_speed_mean_mps": None,
        "ego_speed_p95_mps": None,
        "ego_speed_max_mps": None,
        "stopped_ratio": None,
        "ego_yaw_rate_abs_p95_rad_s": None,
        "ego_yaw_rate_source": None,
        "throttle_raw_p95": None,
        "throttle_mapped_p95": None,
        "throttle_applied_p95": None,
        "brake_raw_p95": None,
        "brake_mapped_p95": None,
        "brake_applied_p95": None,
        "brake_throttle_conflict_frames": None,
        "lateral_error_by_curvature_bucket": {},
        "heading_error_by_curvature_bucket": {},
    }
    if rows is None:
        missing_inputs.append("timeseries")
        return empty
    lateral_values = _series(rows, keys["lateral_error"], absolute=True)
    heading_values = _series(rows, keys["heading_error"], absolute=True)
    ego_speed_values = _series(rows, keys["ego_speed"], absolute=True)
    yaw_rate_values = _series(rows, keys["ego_yaw_rate"], absolute=True)
    yaw_rate_source = "direct" if yaw_rate_values else None
    if not yaw_rate_values:
        yaw_rate_values = [abs(value) for value in _derived_yaw_rate_series(rows)]
        yaw_rate_source = "derived_from_ego_heading" if yaw_rate_values else None
    throttle_raw = _series(rows, keys["throttle_raw"])
    throttle_mapped = _series(rows, keys["throttle_mapped"])
    throttle_applied = _series(rows, keys["throttle_applied"])
    brake_raw = _series(rows, keys["brake_raw"])
    brake_mapped = _series(rows, keys["brake_mapped"])
    brake_applied = _series(rows, keys["brake_applied"])
    if not lateral_values:
        missing_fields.append("lateral_error")
    if not heading_values:
        missing_fields.append("heading_error")
    for key in (
        "ego_speed",
        "throttle_raw",
        "throttle_mapped",
        "throttle_applied",
        "brake_raw",
        "brake_mapped",
        "brake_applied",
    ):
        if not _series(rows, keys[key]):
            missing_fields.append(key)
    if not yaw_rate_values:
        missing_fields.append("ego_yaw_rate")
    lateral_stats = _stats(lateral_values)
    heading_stats = _stats(heading_values)
    speed_stats = _stats(ego_speed_values)
    throttle_raw_stats = _stats(throttle_raw)
    throttle_mapped_stats = _stats(throttle_mapped)
    throttle_applied_stats = _stats(throttle_applied)
    brake_raw_stats = _stats(brake_raw)
    brake_mapped_stats = _stats(brake_mapped)
    brake_applied_stats = _stats(brake_applied)
    conflict_frames = 0
    conflict_available = False
    for row in rows:
        throttle = _scalar(row, *keys["throttle_applied"])
        brake = _scalar(row, *keys["brake_applied"])
        if throttle is None or brake is None:
            continue
        conflict_available = True
        if throttle > 0.05 and brake > 0.05:
            conflict_frames += 1
    return {
        "lateral_error_mean_m": lateral_stats["mean"],
        "lateral_error_p95_m": lateral_stats["p95"],
        "lateral_error_max_m": lateral_stats["max"],
        "heading_error_mean_rad": heading_stats["mean"],
        "heading_error_p95_rad": heading_stats["p95"],
        "heading_error_max_rad": heading_stats["max"],
        "ego_speed_mean_mps": speed_stats["mean"],
        "ego_speed_p95_mps": speed_stats["p95"],
        "ego_speed_max_mps": speed_stats["max"],
        "stopped_ratio": (
            None
            if not ego_speed_values
            else sum(1 for speed in ego_speed_values if abs(speed) < 0.2) / float(len(ego_speed_values))
        ),
        "ego_yaw_rate_abs_p95_rad_s": _percentile(yaw_rate_values, 0.95),
        "ego_yaw_rate_source": yaw_rate_source,
        "throttle_raw_p95": throttle_raw_stats["p95"],
        "throttle_mapped_p95": throttle_mapped_stats["p95"],
        "throttle_applied_p95": throttle_applied_stats["p95"],
        "brake_raw_p95": brake_raw_stats["p95"],
        "brake_mapped_p95": brake_mapped_stats["p95"],
        "brake_applied_p95": brake_applied_stats["p95"],
        "brake_throttle_conflict_frames": conflict_frames if conflict_available else None,
        "lateral_error_by_curvature_bucket": _metric_by_bucket(route, rows, keys["lateral_error"]),
        "heading_error_by_curvature_bucket": _metric_by_bucket(route, rows, keys["heading_error"]),
    }


def _apollo_semantics(
    rows: Sequence[Mapping[str, Any]] | None,
    missing_fields: list[str],
    missing_inputs: list[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "matched_point_anomaly_locations": [],
        "target_point_anomaly_locations": [],
        "first_high_steer": None,
    }
    if rows is None:
        if "timeseries" not in missing_inputs:
            missing_inputs.append("timeseries")
        return result
    matched_names = ("matched_point_too_large", "matched_point_anomaly", "apollo_matched_point_anomaly")
    target_names = ("target_point_anomaly", "target_point_kappa_spike", "apollo_target_point_anomaly")
    steer_names = ("apollo_raw_steer", "apollo_steer_raw", "raw_steer", "steering_target", "cmd_steer")
    matched_seen = False
    target_seen = False
    steer_seen = False
    for position, row in enumerate(rows):
        seq = int(_scalar(row, "seq", "frame", "frame_id") or position)
        if any(name in row for name in matched_names):
            matched_seen = True
        if any(_boolish(row.get(name)) for name in matched_names):
            result["matched_point_anomaly_locations"].append(seq)
        if any(name in row for name in target_names):
            target_seen = True
        if any(_boolish(row.get(name)) for name in target_names):
            result["target_point_anomaly_locations"].append(seq)
        steer = _scalar(row, *steer_names)
        if steer is not None:
            steer_seen = True
            if result["first_high_steer"] is None and abs(steer) >= 0.95:
                result["first_high_steer"] = {"seq": seq, "value": steer}
    if not matched_seen:
        missing_fields.append("matched_point")
    if not target_seen:
        missing_fields.append("target_point")
    if not steer_seen:
        missing_fields.append("apollo_raw_steer")
    return result


def _control_semantics(rows: Sequence[Mapping[str, Any]] | None, missing_inputs: list[str]) -> dict[str, Any]:
    result = {
        "raw_mapped_applied_steer_available": False,
        "guard_apply_counts": {
            "lateral_guard": 0,
            "trajectory_contract_lateral_guard": 0,
            "low_speed_steer_guard": 0,
        },
    }
    if rows is None:
        if "timeseries" not in missing_inputs:
            missing_inputs.append("timeseries")
        return result
    raw_names = ("raw_steer", "apollo_raw_steer", "apollo_steer_raw", "steering_target")
    mapped_names = ("mapped_steer", "bridge_steer_mapped", "cmd_steer", "clamped_steer")
    applied_names = ("applied_steer", "carla_steer_applied", "steer")
    for row in rows:
        if any(name in row for name in raw_names) and any(name in row for name in mapped_names) and any(
            name in row for name in applied_names
        ):
            result["raw_mapped_applied_steer_available"] = True
        if _boolish(row.get("lateral_guard_applied")) or _boolish(row.get("sustained_lateral_guard_applied")):
            result["guard_apply_counts"]["lateral_guard"] += 1
        if _boolish(row.get("trajectory_contract_lateral_guard_applied")) or _boolish(
            row.get("trajectory_contract_guard_applied")
        ):
            result["guard_apply_counts"]["trajectory_contract_lateral_guard"] += 1
        if _boolish(row.get("low_speed_steer_guard_applied")) or _boolish(row.get("dbg_lat_guard")):
            result["guard_apply_counts"]["low_speed_steer_guard"] += 1
    return result


def _verdict(warnings: Sequence[str], missing_inputs: Sequence[str]) -> dict[str, Any]:
    if missing_inputs:
        return {"status": "diagnostic_incomplete", "reason": "missing required analysis inputs"}
    if warnings:
        return {"status": "diagnostic_with_warnings", "reason": "analysis completed with warnings"}
    return {"status": "diagnostic_ready", "reason": "route geometry and available run metrics analyzed"}


def analyze_route_health(
    route: RouteDefinition,
    timeseries_rows: Iterable[Mapping[str, Any]] | None = None,
    *,
    curvature_abs_threshold: float = 0.03,
) -> dict[str, Any]:
    rows = list(timeseries_rows) if timeseries_rows is not None else None
    missing_fields: list[str] = []
    missing_inputs: list[str] = []
    warnings: list[str] = []
    if not route.points:
        warnings.append("route has no points")
    report = RouteHealthReport(
        route_id=route.route_id,
        map_name=route.map_name,
        source=route.source,
        route_geometry=_geometry_summary(
            route,
            missing_fields,
            curvature_abs_threshold=curvature_abs_threshold,
        ),
        run_metrics=_run_metrics(route, rows, missing_fields, missing_inputs),
        apollo_semantics=_apollo_semantics(rows, missing_fields, missing_inputs),
        control_semantics=_control_semantics(rows, missing_inputs),
        missing_fields=sorted(set(missing_fields)),
        missing_inputs=sorted(set(missing_inputs)),
        warnings=warnings,
        verdict={},
    )
    payload = report.to_dict()
    payload["verdict"] = _verdict(payload["warnings"], payload["missing_inputs"])
    return payload
