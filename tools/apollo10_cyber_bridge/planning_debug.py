from __future__ import annotations

import math
from typing import Any, Mapping, Sequence


def build_trajectory_shape_debug(points: Any, *, max_sample_points: int = 8) -> dict[str, Any]:
    """Build small JSON-safe trajectory shape diagnostics without depending on protobuf classes."""
    seq = _as_sequence(points)
    samples = _sample_points(seq, max_sample_points=max_sample_points)
    kappas = [_num(_nested(point, ("path_point", "kappa"))) for point in seq]
    kappas = [value for value in kappas if value is not None]
    thetas = [_num(_nested(point, ("path_point", "theta"))) for point in seq]
    thetas = [value for value in thetas if value is not None]
    xy_steps = _xy_steps(seq)
    theta_deltas = [_wrap_to_pi(thetas[index] - thetas[index - 1]) for index in range(1, len(thetas))]
    first_segment_heading = _first_segment_heading(seq)
    first_theta = _num(_nested(seq[0], ("path_point", "theta"))) if seq else None
    return {
        "trajectory_sample_points": samples,
        "trajectory_kappa": _series_stats(kappas),
        "trajectory_theta_delta_abs": _series_stats([abs(value) for value in theta_deltas]),
        "trajectory_xy_step_m": _series_stats(xy_steps),
        "trajectory_kappa_spike_count_abs_ge_0_05": sum(1 for value in kappas if abs(value) >= 0.05),
        "trajectory_kappa_spike_count_abs_ge_0_10": sum(1 for value in kappas if abs(value) >= 0.10),
        "trajectory_first_segment_heading": first_segment_heading,
        "trajectory_first_theta_minus_first_segment_heading_rad": (
            _wrap_to_pi(float(first_theta) - float(first_segment_heading))
            if first_theta is not None and first_segment_heading is not None
            else None
        ),
    }


def build_planning_debug_presence(msg: Any) -> dict[str, Any]:
    """Describe which Apollo Planning debug fields were actually present.

    This is intentionally protobuf-agnostic so tests can use light fake message
    objects. The bridge writes this alongside the existing parsed route/reference
    counters to distinguish "Apollo did not populate debug fields" from "bridge
    parsed the wrong path".
    """

    debug = _nested(msg, ("debug",))
    planning_data = _nested(msg, ("debug", "planning_data"))
    reference_line, reference_line_path = _first_nested_with_path(
        msg,
        (
            ("debug", "planning_data", "reference_line"),
            ("debug", "reference_line"),
        ),
    )
    routing, routing_path = _first_nested_with_path(
        msg,
        (
            ("debug", "planning_data", "routing"),
            ("routing",),
        ),
    )
    reference_lines = _as_sequence(reference_line)
    reference_line_lengths = []
    for item in reference_lines:
        length = _num(_nested(item, ("length",)))
        if length is not None:
            reference_line_lengths.append(length)
    routing_roads = _as_sequence(_nested(routing, ("road",))) if routing is not None else []
    routing_passage_count = 0
    routing_segment_count = 0
    for road in routing_roads:
        passages = _as_sequence(_nested(road, ("passage",)))
        routing_passage_count += len(passages)
        for passage in passages:
            routing_segment_count += len(_as_sequence(_nested(passage, ("segment",))))
    return {
        "planning_debug_debug_present": debug is not None,
        "planning_debug_planning_data_present": planning_data is not None,
        "planning_debug_reference_line_path": reference_line_path,
        "planning_debug_reference_line_field_present": reference_line is not None,
        "planning_debug_reference_line_count": len(reference_lines),
        "planning_debug_reference_line_lengths": reference_line_lengths,
        "planning_debug_routing_path": routing_path,
        "planning_debug_routing_field_present": routing is not None,
        "planning_debug_routing_road_count": len(routing_roads),
        "planning_debug_routing_passage_count": routing_passage_count,
        "planning_debug_routing_segment_count": routing_segment_count,
        "planning_debug_diagnosis": _planning_debug_diagnosis(
            debug_present=debug is not None,
            planning_data_present=planning_data is not None,
            reference_line_field_present=reference_line is not None,
            reference_line_count=len(reference_lines),
            routing_field_present=routing is not None,
            routing_segment_count=routing_segment_count,
        ),
    }


def _sample_points(points: Sequence[Any], *, max_sample_points: int) -> list[dict[str, Any]]:
    if not points or max_sample_points <= 0:
        return []
    candidate_indices = [0, 1, 2, 5, 10, len(points) - 1]
    indices: list[int] = []
    for index in candidate_indices:
        if 0 <= index < len(points) and index not in indices:
            indices.append(index)
    if len(indices) > max_sample_points:
        indices = indices[:max_sample_points]
    samples = []
    for index in indices:
        point = points[index]
        samples.append(
            {
                "index": index,
                "x": _num(_nested(point, ("path_point", "x"))),
                "y": _num(_nested(point, ("path_point", "y"))),
                "theta": _num(_nested(point, ("path_point", "theta"))),
                "kappa": _num(_nested(point, ("path_point", "kappa"))),
                "v": _num(_nested(point, ("v",))),
                "relative_time": _num(_nested(point, ("relative_time",))),
            }
        )
    return samples


def _xy_steps(points: Sequence[Any]) -> list[float]:
    out: list[float] = []
    previous: tuple[float, float] | None = None
    for point in points:
        x = _num(_nested(point, ("path_point", "x")))
        y = _num(_nested(point, ("path_point", "y")))
        if x is None or y is None:
            previous = None
            continue
        if previous is not None:
            out.append(math.hypot(x - previous[0], y - previous[1]))
        previous = (x, y)
    return out


def _first_segment_heading(points: Sequence[Any]) -> float | None:
    first: tuple[float, float] | None = None
    for point in points:
        x = _num(_nested(point, ("path_point", "x")))
        y = _num(_nested(point, ("path_point", "y")))
        if x is None or y is None:
            continue
        if first is None:
            first = (x, y)
            continue
        dx = x - first[0]
        dy = y - first[1]
        if abs(dx) > 1e-9 or abs(dy) > 1e-9:
            return math.atan2(dy, dx)
    return None


def _series_stats(values: Sequence[float]) -> dict[str, Any]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    if not finite:
        return {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None}
    abs_values = sorted(abs(value) for value in finite)
    return {
        "count": len(finite),
        "min": min(finite),
        "max": max(finite),
        "max_abs": max(abs_values),
        "p95_abs": abs_values[int(0.95 * (len(abs_values) - 1))],
    }


def _as_sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    try:
        return list(value)
    except TypeError:
        return []


def _nested(value: Any, path: Sequence[str]) -> Any:
    current = value
    for item in path:
        if isinstance(current, Mapping):
            current = current.get(item)
        else:
            current = getattr(current, item, None)
        if current is None:
            return None
    return current


def _first_nested_with_path(value: Any, paths: Sequence[Sequence[str]]) -> tuple[Any, str | None]:
    for path in paths:
        marker = object()
        found = _nested_with_default(value, path, marker)
        if found is not marker:
            return found, ".".join(path)
    return None, None


def _nested_with_default(value: Any, path: Sequence[str], default: Any) -> Any:
    current = value
    for item in path:
        if isinstance(current, Mapping):
            if item not in current:
                return default
            current = current.get(item)
        else:
            if not hasattr(current, item):
                return default
            current = getattr(current, item)
        if current is None:
            return default
    return current


def _num(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _wrap_to_pi(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def _planning_debug_diagnosis(
    *,
    debug_present: bool,
    planning_data_present: bool,
    reference_line_field_present: bool,
    reference_line_count: int,
    routing_field_present: bool,
    routing_segment_count: int,
) -> str:
    if not debug_present:
        return "debug_field_missing"
    if not planning_data_present:
        return "planning_data_missing"
    if reference_line_count > 0 and routing_segment_count > 0:
        return "reference_line_and_routing_present"
    if reference_line_count > 0:
        return "reference_line_present_routing_missing_or_empty"
    if routing_segment_count > 0:
        return "routing_present_reference_line_empty"
    if reference_line_field_present and routing_field_present:
        return "reference_line_and_routing_empty"
    if reference_line_field_present:
        return "reference_line_empty_routing_missing"
    if routing_field_present:
        return "reference_line_missing_routing_empty"
    return "reference_line_and_routing_missing"
