from __future__ import annotations

import pytest

from carla_testbed.scenarios.town01_route_health import Town01RouteHealthScenario


def test_route_trace_length_prefers_s_span_for_claim_metadata() -> None:
    length, source = Town01RouteHealthScenario._route_trace_length_from_points(
        [
            {"x": 0.0, "y": 0.0, "z": 0.0, "s": 10.0},
            {"x": 3.0, "y": 4.0, "z": 0.0, "s": 398.4},
        ]
    )

    assert length == pytest.approx(388.4)
    assert source == "route_trace_s_span"


def test_route_trace_length_falls_back_to_polyline_when_s_missing() -> None:
    length, source = Town01RouteHealthScenario._route_trace_length_from_points(
        [
            {"x": 0.0, "y": 0.0, "z": 0.0},
            {"x": 3.0, "y": 4.0, "z": 0.0},
            {"x": 3.0, "y": 4.0, "z": 12.0},
        ]
    )

    assert length == pytest.approx(17.0)
    assert source == "route_trace_xyz_polyline"
