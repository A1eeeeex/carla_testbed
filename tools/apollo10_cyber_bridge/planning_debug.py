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
    field_inventory = _planning_debug_field_inventory(
        msg,
        debug=debug,
        planning_data=planning_data,
    )
    path_candidate_summary = _planning_debug_path_candidate_summary(
        planning_data,
        field_inventory=field_inventory,
    )
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
        "planning_debug_field_inventory": field_inventory,
        "planning_debug_path_candidate_summary": path_candidate_summary,
        "planning_debug_diagnosis": _planning_debug_diagnosis(
            debug_present=debug is not None,
            planning_data_present=planning_data is not None,
            reference_line_field_present=reference_line is not None,
            reference_line_count=len(reference_lines),
            routing_field_present=routing is not None,
            routing_segment_count=routing_segment_count,
        ),
    }


def _planning_debug_path_candidate_summary(
    planning_data: Any,
    *,
    field_inventory: Mapping[str, Any],
    max_candidates: int = 8,
    max_items_per_candidate: int = 2,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    candidate_paths = field_inventory.get("reference_line_candidate_paths")
    if not isinstance(candidate_paths, Sequence) or isinstance(
        candidate_paths, (str, bytes, bytearray)
    ):
        candidate_paths = []
    for candidate in candidate_paths:
        if not isinstance(candidate, Mapping):
            continue
        path = str(candidate.get("path") or "")
        if not path.startswith("debug.planning_data."):
            continue
        field_name = path.rsplit(".", 1)[-1]
        field_value = _nested(planning_data, (field_name,))
        items = _as_sequence(field_value)
        if not items:
            candidates.append(
                {
                    "path": path,
                    "item_count": 0,
                    "field_name_match": bool(candidate.get("field_name_match")),
                    "item_summaries": [],
                }
            )
            continue
        candidates.append(
            {
                "path": path,
                "item_count": len(items),
                "field_name_match": bool(candidate.get("field_name_match")),
                "item_summaries": [
                    _planning_debug_path_item_summary(item, index=index)
                    for index, item in enumerate(items[:max_items_per_candidate])
                ],
            }
        )
        if len(candidates) >= max_candidates:
            break
    nonempty = [item for item in candidates if int(item.get("item_count") or 0) > 0]
    path_like_nonempty = [
        item
        for item in nonempty
        if "path" in str(item.get("path") or "").lower()
        or "trajectory" in str(item.get("path") or "").lower()
    ]
    return {
        "available": bool(nonempty),
        "candidate_count": len(candidates),
        "nonempty_candidate_count": len(nonempty),
        "path_like_nonempty_candidate_count": len(path_like_nonempty),
        "candidates": candidates,
        "claim_boundary": (
            "Planning debug path candidate summaries are diagnostic only. They "
            "describe raw Planning debug structure and do not prove reference-line "
            "correctness or behavior success."
        ),
    }


def _planning_debug_path_item_summary(item: Any, *, index: int) -> dict[str, Any]:
    field_names = _field_names(item)
    sequence_counts = _field_sequence_counts(item, field_names)
    scalar_fields = {}
    for name in field_names:
        if len(scalar_fields) >= 12:
            break
        value = _nested(item, (name,))
        if isinstance(value, (str, int, float, bool)) or value is None:
            scalar_fields[name] = value
    point_sequences = []
    for name, count in sequence_counts.items():
        if count <= 0:
            continue
        seq = _as_sequence(_nested(item, (name,)))
        point_count, first_point = _point_like_sequence_summary(seq)
        if point_count > 0:
            point_sequences.append(
                {
                    "field": name,
                    "sequence_count": count,
                    "point_like_count": point_count,
                    "first_point": first_point,
                    "sample_points": _sample_point_like_sequence(seq, max_sample_points=8),
                }
            )
    return {
        "index": index,
        "field_names": field_names,
        "scalar_fields": scalar_fields,
        "sequence_counts": sequence_counts,
        "point_sequence_candidates": point_sequences,
    }


def _point_like_sequence_summary(items: Sequence[Any]) -> tuple[int, dict[str, Any] | None]:
    count = 0
    first: dict[str, Any] | None = None
    for item in items:
        point = _point_like_summary(item)
        if point is None:
            continue
        count += 1
        if first is None:
            first = point
    return count, first


def _sample_point_like_sequence(items: Sequence[Any], *, max_sample_points: int) -> list[dict[str, Any]]:
    points = []
    for index, item in enumerate(items):
        point = _point_like_summary(item)
        if point is None:
            continue
        payload = {"index": index, **point}
        points.append(payload)
    if max_sample_points <= 0 or len(points) <= max_sample_points:
        return points
    last = points[-1]
    keep = max(1, max_sample_points - 1)
    if keep == 1:
        return [points[0], last]
    stride = max(1, (len(points) - 1) // keep)
    sampled = points[0:-1:stride][:keep]
    if sampled[-1].get("index") != last.get("index"):
        sampled.append(last)
    return sampled[:max_sample_points]


def _point_like_summary(item: Any) -> dict[str, Any] | None:
    candidates = (
        (),
        ("path_point",),
        ("point",),
        ("map_path_point",),
    )
    for prefix in candidates:
        x = _num(_nested(item, (*prefix, "x")))
        y = _num(_nested(item, (*prefix, "y")))
        if x is None or y is None:
            continue
        return {
            "x": x,
            "y": y,
            "theta": _num(_nested(item, (*prefix, "theta"))),
            "kappa": _num(_nested(item, (*prefix, "kappa"))),
        }
    return None


def _planning_debug_field_inventory(
    msg: Any,
    *,
    debug: Any,
    planning_data: Any,
) -> dict[str, Any]:
    """Return a compact, JSON-safe field inventory for Planning debug diagnosis.

    The online bridge should not need Apollo protobuf descriptors at import time,
    but real protobuf messages expose enough runtime metadata to distinguish
    "the known reference_line field is empty" from "there may be another
    reference/path-like field we are not inspecting yet".
    """

    planning_data_fields = _field_names(planning_data)
    repeated_counts = _field_sequence_counts(planning_data, planning_data_fields)
    candidate_paths = []
    for name in planning_data_fields:
        lowered = name.lower()
        count = repeated_counts.get(name)
        if (
            "reference" in lowered
            or "line" in lowered
            or "path" in lowered
            or "trajectory" in lowered
        ):
            candidate_paths.append(
                {
                    "path": f"debug.planning_data.{name}",
                    "repeated_count": count,
                    "field_name_match": True,
                }
            )
    for name, count in repeated_counts.items():
        if count <= 0:
            continue
        path = f"debug.planning_data.{name}"
        if not any(item.get("path") == path for item in candidate_paths):
            candidate_paths.append(
                {
                    "path": path,
                    "repeated_count": count,
                    "field_name_match": False,
                }
            )
    return {
        "top_level_fields": _field_names(msg),
        "debug_fields": _field_names(debug),
        "planning_data_fields": planning_data_fields,
        "planning_data_present_fields": _present_field_names(planning_data),
        "planning_data_repeated_field_counts": repeated_counts,
        "reference_line_candidate_paths": candidate_paths[:20],
        "claim_boundary": (
            "Field inventory is diagnostic only. It helps decide whether the "
            "bridge is reading the right Planning debug field, but it does not "
            "prove reference-line correctness or behavior success."
        ),
    }


def _field_names(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return sorted(str(key) for key in value.keys())
    descriptor = getattr(value, "DESCRIPTOR", None)
    fields = getattr(descriptor, "fields", None)
    if fields is not None:
        out = []
        for field in fields:
            name = getattr(field, "name", None)
            if name:
                out.append(str(name))
        return sorted(out)
    if hasattr(value, "__dict__"):
        return sorted(str(key) for key in vars(value).keys() if not str(key).startswith("_"))
    return []


def _present_field_names(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return sorted(str(key) for key, item in value.items() if item is not None)
    list_fields = getattr(value, "ListFields", None)
    if callable(list_fields):
        out = []
        try:
            for field, _item in list_fields():
                name = getattr(field, "name", None)
                if name:
                    out.append(str(name))
        except Exception:
            return []
        return sorted(out)
    if hasattr(value, "__dict__"):
        return sorted(
            str(key)
            for key, item in vars(value).items()
            if not str(key).startswith("_") and item is not None
        )
    return []


def _field_sequence_counts(value: Any, field_names: Sequence[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for name in field_names:
        item = _nested(value, (name,))
        if item is None or isinstance(item, (str, bytes, bytearray, Mapping)):
            continue
        try:
            count = len(item)
        except TypeError:
            continue
        except Exception:
            continue
        out[str(name)] = int(count)
    return out


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
