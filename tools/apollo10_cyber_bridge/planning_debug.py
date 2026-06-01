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
