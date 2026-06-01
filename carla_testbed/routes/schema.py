from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RoutePoint:
    index: int
    x: float
    y: float
    z: float = 0.0
    s: float | None = None
    heading: float | None = None
    curvature: float | None = None
    lane_id: str | None = None
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RouteDefinition:
    route_id: str
    map_name: str
    source: str
    points: list[RoutePoint]
    spawn_pose: dict[str, Any] | None = None
    goal_pose: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "map_name": self.map_name,
            "source": self.source,
            "points": [point.to_dict() for point in self.points],
            "spawn_pose": self.spawn_pose,
            "goal_pose": self.goal_pose,
            "metadata": dict(self.metadata),
        }


@dataclass
class RouteHealthReport:
    route_id: str
    map_name: str
    source: str
    route_geometry: dict[str, Any]
    run_metrics: dict[str, Any]
    apollo_semantics: dict[str, Any]
    control_semantics: dict[str, Any]
    missing_fields: list[str] = field(default_factory=list)
    missing_inputs: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    verdict: dict[str, Any] = field(default_factory=dict)
    schema_version: str = "route_health.v1"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "route_id": self.route_id,
            "map_name": self.map_name,
            "source": self.source,
            "route_geometry": self.route_geometry,
            "run_metrics": self.run_metrics,
            "apollo_semantics": self.apollo_semantics,
            "control_semantics": self.control_semantics,
            "missing_fields": list(self.missing_fields),
            "missing_inputs": list(self.missing_inputs),
            "warnings": list(self.warnings),
            "verdict": dict(self.verdict),
        }
