from __future__ import annotations

from pathlib import Path

from carla_testbed.routes.io import load_route_json


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_load_route_json_fills_geometry_defaults() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")

    assert route.route_id == "simple_straight"
    assert route.map_name == "TownTest"
    assert route.source == "fixture"
    assert len(route.points) == 4
    assert route.points[-1].s == 30.0
    assert route.points[0].heading == 0.0
    assert route.points[1].curvature == 0.0
    assert route.spawn_pose and route.spawn_pose["x"] == 0.0
    assert route.goal_pose and route.goal_pose["x"] == 30.0


def test_load_curve_route_has_nonzero_curvature() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_curve.json")

    curvatures = [abs(point.curvature or 0.0) for point in route.points]

    assert route.route_id == "simple_curve"
    assert max(curvatures) > 0.01
