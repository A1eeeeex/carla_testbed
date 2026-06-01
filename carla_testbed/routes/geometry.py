from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

from .schema import RoutePoint


@dataclass(frozen=True)
class RouteProjection:
    nearest_index: int
    segment_start_index: int | None
    segment_end_index: int | None
    t: float | None
    x: float
    y: float
    z: float
    s: float | None
    heading: float | None
    curvature: float | None
    cross_track_error: float | None
    distance_m: float


def normalize_angle(angle_rad: float) -> float:
    """Normalize an angle to [-pi, pi)."""
    value = math.fmod(float(angle_rad) + math.pi, 2.0 * math.pi)
    if value < 0.0:
        value += 2.0 * math.pi
    return value - math.pi


def _xy(point: RoutePoint) -> tuple[float, float]:
    return float(point.x), float(point.y)


def _dist(a: RoutePoint, b: RoutePoint) -> float:
    ax, ay = _xy(a)
    bx, by = _xy(b)
    return math.hypot(bx - ax, by - ay)


def compute_spacing(points: Sequence[RoutePoint]) -> list[float]:
    return [_dist(points[index - 1], points[index]) for index in range(1, len(points))]


def compute_cumulative_s(points: Sequence[RoutePoint]) -> list[float]:
    values = [0.0]
    total = 0.0
    for spacing in compute_spacing(points):
        total += spacing
        values.append(total)
    return values[: len(points)]


def compute_headings(points: Sequence[RoutePoint]) -> list[float | None]:
    if not points:
        return []
    if len(points) == 1:
        return [None]
    headings: list[float | None] = []
    for index, point in enumerate(points):
        if index < len(points) - 1:
            next_point = points[index + 1]
            dx = float(next_point.x) - float(point.x)
            dy = float(next_point.y) - float(point.y)
        else:
            prev_point = points[index - 1]
            dx = float(point.x) - float(prev_point.x)
            dy = float(point.y) - float(prev_point.y)
        headings.append(math.atan2(dy, dx) if dx or dy else None)
    return headings


def compute_curvature(points: Sequence[RoutePoint]) -> list[float | None]:
    """Estimate curvature at each point from heading delta over arc length."""
    if not points:
        return []
    if len(points) < 3:
        return [None for _ in points]
    headings = compute_headings(points)
    curvatures: list[float | None] = [None]
    for index in range(1, len(points) - 1):
        prev_heading = headings[index - 1]
        next_heading = headings[index]
        ds = (_dist(points[index - 1], points[index]) + _dist(points[index], points[index + 1])) / 2.0
        if prev_heading is None or next_heading is None or ds <= 1e-9:
            curvatures.append(None)
        else:
            curvatures.append(normalize_angle(float(next_heading) - float(prev_heading)) / ds)
    curvatures.append(None)
    return curvatures


def nearest_route_index(points: Sequence[RoutePoint], x: float, y: float) -> int | None:
    if not points:
        return None
    best_index = 0
    best_distance = float("inf")
    for index, point in enumerate(points):
        distance = math.hypot(float(x) - float(point.x), float(y) - float(point.y))
        if distance < best_distance:
            best_distance = distance
            best_index = index
    return best_index


def project_onto_route(
    points: Sequence[RoutePoint],
    x: float,
    y: float,
    *,
    extend_ends: bool = False,
) -> RouteProjection | None:
    if not points:
        return None
    nearest = nearest_route_index(points, x, y)
    if nearest is None:
        return None
    if len(points) == 1:
        point = points[0]
        distance = math.hypot(float(x) - float(point.x), float(y) - float(point.y))
        return RouteProjection(
            nearest_index=int(point.index),
            segment_start_index=None,
            segment_end_index=None,
            t=None,
            x=float(point.x),
            y=float(point.y),
            z=float(point.z),
            s=point.s,
            heading=point.heading,
            curvature=point.curvature,
            cross_track_error=distance,
            distance_m=distance,
        )

    best: RouteProjection | None = None
    cumulative_s = compute_cumulative_s(points)
    for index in range(len(points) - 1):
        start = points[index]
        end = points[index + 1]
        sx, sy = _xy(start)
        ex, ey = _xy(end)
        dx = ex - sx
        dy = ey - sy
        segment_len_sq = dx * dx + dy * dy
        if segment_len_sq <= 1e-12:
            continue
        raw_t = ((float(x) - sx) * dx + (float(y) - sy) * dy) / segment_len_sq
        lower = float("-inf") if extend_ends and index == 0 else 0.0
        upper = float("inf") if extend_ends and index == len(points) - 2 else 1.0
        t = min(max(raw_t, lower), upper)
        px = sx + t * dx
        py = sy + t * dy
        pz = float(start.z) + t * (float(end.z) - float(start.z))
        distance = math.hypot(float(x) - px, float(y) - py)
        heading = math.atan2(dy, dx)
        cte = -math.sin(heading) * (float(x) - px) + math.cos(heading) * (float(y) - py)
        start_s = start.s if start.s is not None else cumulative_s[index]
        end_s = end.s if end.s is not None else cumulative_s[index + 1]
        route_s = None if start_s is None or end_s is None else float(start_s) + t * (float(end_s) - float(start_s))
        curvature = _interpolate_optional(start.curvature, end.curvature, t)
        nearest_index_value = int(start.index if abs(t) <= abs(1.0 - t) else end.index)
        projection = RouteProjection(
            nearest_index=nearest_index_value,
            segment_start_index=int(start.index),
            segment_end_index=int(end.index),
            t=t,
            x=px,
            y=py,
            z=pz,
            s=route_s,
            heading=heading,
            curvature=curvature,
            cross_track_error=cte,
            distance_m=distance,
        )
        if best is None or projection.distance_m < best.distance_m:
            best = projection
    if best is not None:
        return best
    point = points[nearest]
    distance = math.hypot(float(x) - float(point.x), float(y) - float(point.y))
    return RouteProjection(
        nearest_index=int(point.index),
        segment_start_index=None,
        segment_end_index=None,
        t=None,
        x=float(point.x),
        y=float(point.y),
        z=float(point.z),
        s=point.s,
        heading=point.heading,
        curvature=point.curvature,
        cross_track_error=distance,
        distance_m=distance,
    )


def cross_track_error(points: Sequence[RoutePoint], x: float, y: float) -> float | None:
    projection = project_onto_route(points, x, y)
    if projection is not None:
        return projection.cross_track_error
    index = nearest_route_index(points, x, y)
    if index is None:
        return None
    route_heading = points[index].heading
    if route_heading is None:
        headings = compute_headings(points)
        route_heading = headings[index] if index < len(headings) else None
    if route_heading is None:
        return math.hypot(float(x) - float(points[index].x), float(y) - float(points[index].y))
    dx = float(x) - float(points[index].x)
    dy = float(y) - float(points[index].y)
    # Positive means the ego point is left of the route tangent.
    return -math.sin(float(route_heading)) * dx + math.cos(float(route_heading)) * dy


def heading_error(ego_heading: float, route_heading: float | None) -> float | None:
    if route_heading is None:
        return None
    return normalize_angle(float(ego_heading) - float(route_heading))


def _interpolate_optional(a: float | None, b: float | None, t: float) -> float | None:
    if a is None and b is None:
        return None
    if a is None:
        return float(b) if b is not None else None
    if b is None:
        return float(a)
    return float(a) + float(t) * (float(b) - float(a))
