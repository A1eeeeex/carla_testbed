from __future__ import annotations

import math
from types import SimpleNamespace

from tools.apollo10_cyber_bridge.planning_debug import build_trajectory_shape_debug


def _point(x: float, y: float, theta: float, kappa: float, v: float, t: float) -> SimpleNamespace:
    return SimpleNamespace(
        path_point=SimpleNamespace(x=x, y=y, theta=theta, kappa=kappa),
        v=v,
        relative_time=t,
    )


def test_build_trajectory_shape_debug_from_fake_points() -> None:
    points = [
        _point(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        _point(1.0, 0.0, 0.0, 0.12, 1.0, 0.1),
        _point(2.0, 0.2, 0.1, -0.20, 2.0, 0.2),
        _point(3.0, 0.4, 0.2, 0.01, 3.0, 0.3),
    ]

    debug = build_trajectory_shape_debug(points)

    assert debug["trajectory_kappa"]["count"] == 4
    assert debug["trajectory_kappa"]["max_abs"] == 0.2
    assert debug["trajectory_kappa_spike_count_abs_ge_0_05"] == 2
    assert debug["trajectory_kappa_spike_count_abs_ge_0_10"] == 2
    assert debug["trajectory_xy_step_m"]["count"] == 3
    assert debug["trajectory_first_segment_heading"] == 0.0
    assert debug["trajectory_first_theta_minus_first_segment_heading_rad"] == 0.0
    assert debug["trajectory_sample_points"][0]["index"] == 0
    assert debug["trajectory_sample_points"][-1]["index"] == 3


def test_build_trajectory_shape_debug_handles_dict_points_and_heading_wrap() -> None:
    points = [
        {"path_point": {"x": 0.0, "y": 0.0, "theta": math.pi - 0.01, "kappa": 0.0}, "v": 1.0},
        {"path_point": {"x": -1.0, "y": 0.0, "theta": -math.pi + 0.01, "kappa": 0.0}, "v": 1.0},
    ]

    debug = build_trajectory_shape_debug(points)

    assert debug["trajectory_theta_delta_abs"]["max_abs"] < 0.03
    assert debug["trajectory_first_segment_heading"] == math.pi
    assert abs(debug["trajectory_first_theta_minus_first_segment_heading_rad"] + 0.01) < 1e-9


def test_build_trajectory_shape_debug_empty_points() -> None:
    debug = build_trajectory_shape_debug(None)

    assert debug["trajectory_sample_points"] == []
    assert debug["trajectory_kappa"]["count"] == 0
    assert debug["trajectory_first_segment_heading"] is None
