"""Route definitions and geometry helpers for offline analysis."""

from .geometry import (
    compute_cumulative_s,
    compute_curvature,
    compute_headings,
    compute_spacing,
    cross_track_error,
    heading_error,
    nearest_route_index,
    normalize_angle,
    project_onto_route,
)
from .io import load_route_json
from .schema import RouteDefinition, RouteHealthReport, RoutePoint

__all__ = [
    "RouteDefinition",
    "RouteHealthReport",
    "RoutePoint",
    "compute_cumulative_s",
    "compute_curvature",
    "compute_headings",
    "compute_spacing",
    "cross_track_error",
    "heading_error",
    "load_route_json",
    "nearest_route_index",
    "normalize_angle",
    "project_onto_route",
]
