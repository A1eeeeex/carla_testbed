from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from carla_testbed.routes.geometry import compute_cumulative_s, compute_curvature, compute_headings
from carla_testbed.routes.schema import RouteDefinition, RoutePoint


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _point_from_metadata(index: int, payload: Mapping[str, Any]) -> RoutePoint | None:
    x = _float_or_none(payload.get("x"))
    y = _float_or_none(payload.get("y"))
    if x is None or y is None:
        return None
    tags = payload.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    return RoutePoint(
        index=int(payload.get("index", index)),
        x=x,
        y=y,
        z=_float_or_none(payload.get("z")) or 0.0,
        s=_float_or_none(payload.get("s")),
        heading=_float_or_none(payload.get("heading")),
        curvature=_float_or_none(payload.get("curvature")),
        lane_id=str(payload.get("lane_id")) if payload.get("lane_id") is not None else None,
        tags=[str(tag) for tag in tags],
    )


def _fill_missing_geometry(points: list[RoutePoint]) -> None:
    cumulative_s = compute_cumulative_s(points)
    headings = compute_headings(points)
    curvatures = compute_curvature(points)
    for index, point in enumerate(points):
        if point.s is None and index < len(cumulative_s):
            point.s = cumulative_s[index]
        if point.heading is None and index < len(headings):
            point.heading = headings[index]
        if point.curvature is None and index < len(curvatures):
            point.curvature = curvatures[index]


def route_definition_from_metadata(metadata: Mapping[str, Any] | None) -> RouteDefinition | None:
    """Build a route definition from scenario metadata when available.

    The scenario emits plain JSON-compatible points, not CARLA objects, so the
    recorder can compute route-relative P0 fields without depending on CARLA.
    """
    if not isinstance(metadata, Mapping):
        return None
    raw_points = metadata.get("route_trace") or metadata.get("route_points") or metadata.get("points")
    if not isinstance(raw_points, Sequence) or isinstance(raw_points, (str, bytes)):
        return None
    points: list[RoutePoint] = []
    for index, raw_point in enumerate(raw_points):
        if not isinstance(raw_point, Mapping):
            continue
        point = _point_from_metadata(index, raw_point)
        if point is not None:
            points.append(point)
    if not points:
        return None
    _fill_missing_geometry(points)
    return RouteDefinition(
        route_id=str(metadata.get("route_id") or "unknown_route"),
        map_name=str(metadata.get("map") or metadata.get("map_name") or "Town01"),
        source=str(metadata.get("route_trace_source") or "scenario_metadata.route_trace"),
        points=points,
        spawn_pose=metadata.get("spawn") if isinstance(metadata.get("spawn"), Mapping) else None,
        goal_pose=metadata.get("goal") if isinstance(metadata.get("goal"), Mapping) else None,
        metadata={
            "route_length_m": metadata.get("route_length_m"),
            "route_selected_from_corpus": metadata.get("route_selected_from_corpus"),
        },
    )
