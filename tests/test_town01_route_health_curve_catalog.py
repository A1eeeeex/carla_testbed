from __future__ import annotations

from types import SimpleNamespace

from carla_testbed.scenarios.town01_route_health import Town01RouteHealthScenario
from carla_testbed.utils.town01_route_health import (
    _is_curve_lane_follow_candidate,
    _is_terminal_short_turn_curve,
)


def _route(*, post_transition_length_m: float) -> dict:
    return {
        "route_id": "route",
        "route_length_m": 240.0,
        "road_transition_count": 1,
        "lane_transition_count": 1,
        "spawn_pose": {"yaw_deg": 0.0},
        "goal_pose": {"yaw_deg": 45.0},
        "post_last_lane_transition_length_m": post_transition_length_m,
        "post_last_road_transition_length_m": post_transition_length_m,
    }


def test_terminal_short_turn_is_not_clean_curve_lane_follow_candidate() -> None:
    route = _route(post_transition_length_m=5.0)

    assert _is_terminal_short_turn_curve(route) is True
    assert _is_curve_lane_follow_candidate(route) is False


def test_curve_lane_follow_candidate_requires_post_turn_lookahead() -> None:
    route = _route(post_transition_length_m=40.0)

    assert _is_terminal_short_turn_curve(route) is False
    assert _is_curve_lane_follow_candidate(route) is True


def _waypoint(x: float, y: float, road_id: int, lane_id: int):
    return SimpleNamespace(
        road_id=road_id,
        section_id=0,
        lane_id=lane_id,
        transform=SimpleNamespace(
            location=SimpleNamespace(x=x, y=y, z=0.0),
        ),
    )


def test_route_transition_metrics_record_post_turn_length() -> None:
    trace = [
        _waypoint(0.0, 0.0, 1, 1),
        _waypoint(0.0, 10.0, 1, 1),
        _waypoint(0.0, 20.0, 1, 1),
        _waypoint(5.0, 25.0, 2, -1),
        _waypoint(10.0, 30.0, 2, -1),
    ]

    metrics = Town01RouteHealthScenario._route_transition_metrics(trace, end_index=len(trace) - 1)

    assert metrics["road_transition_index_first"] == 3
    assert metrics["lane_transition_index_first"] == 3
    assert round(metrics["post_last_lane_transition_length_m"], 3) == 7.071
    assert metrics["terminal_short_turn_like"] is True
