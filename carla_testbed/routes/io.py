from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .geometry import compute_cumulative_s, compute_curvature, compute_headings
from .schema import RouteDefinition, RoutePoint


def _point_from_dict(index: int, payload: dict[str, Any]) -> RoutePoint:
    tags = payload.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    return RoutePoint(
        index=int(payload.get("index", index)),
        x=float(payload["x"]),
        y=float(payload["y"]),
        z=float(payload.get("z", 0.0)),
        s=float(payload["s"]) if payload.get("s") is not None else None,
        heading=float(payload["heading"]) if payload.get("heading") is not None else None,
        curvature=float(payload["curvature"]) if payload.get("curvature") is not None else None,
        lane_id=str(payload["lane_id"]) if payload.get("lane_id") is not None else None,
        tags=[str(item) for item in tags],
    )


def _fill_geometry_defaults(points: list[RoutePoint]) -> None:
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


def load_route_json(path: str | Path) -> RouteDefinition:
    route_path = Path(path).expanduser()
    payload = json.loads(route_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"route JSON must contain an object: {route_path}")
    points_payload = payload.get("points")
    if not isinstance(points_payload, list) or not points_payload:
        raise ValueError(f"route JSON must contain non-empty points list: {route_path}")
    points = [_point_from_dict(index, item) for index, item in enumerate(points_payload)]
    _fill_geometry_defaults(points)
    route_id = str(payload.get("route_id") or route_path.stem)
    map_name = str(payload.get("map") or payload.get("map_name") or "")
    if not map_name:
        raise ValueError(f"route JSON missing map/map_name: {route_path}")
    return RouteDefinition(
        route_id=route_id,
        map_name=map_name,
        source=str(payload.get("source") or str(route_path)),
        points=points,
        spawn_pose=payload.get("spawn_pose"),
        goal_pose=payload.get("goal_pose"),
        metadata=dict(payload.get("metadata") or {}),
    )
