from __future__ import annotations

import math

from carla_testbed.analysis.gap import ActorKinematics2D, bumper_to_bumper_gap


def test_bumper_gap_same_lane_same_heading() -> None:
    ego = ActorKinematics2D(x=0.0, y=0.0, yaw_rad=0.0, speed_mps=10.0, length_m=4.0, width_m=2.0)
    target = ActorKinematics2D(x=20.0, y=0.0, yaw_rad=0.0, speed_mps=8.0, length_m=4.0, width_m=2.0)

    result = bumper_to_bumper_gap(ego, target)

    assert result["gap_method"] == "bumper_to_bumper_longitudinal_projection"
    assert result["gap_m"] == 16.0
    assert result["relative_speed_mps"] == -2.0


def test_bumper_gap_opposite_heading_uses_target_extent_in_ego_frame() -> None:
    ego = ActorKinematics2D(x=0.0, y=0.0, yaw_rad=0.0, length_m=4.0, width_m=2.0)
    target = ActorKinematics2D(x=20.0, y=0.0, yaw_rad=math.pi, length_m=4.0, width_m=2.0)

    result = bumper_to_bumper_gap(ego, target)

    assert result["gap_m"] == 16.0


def test_bumper_gap_lateral_offset_preserves_longitudinal_gap() -> None:
    ego = ActorKinematics2D(x=0.0, y=0.0, yaw_rad=0.0, length_m=4.0, width_m=2.0)
    target = ActorKinematics2D(x=20.0, y=3.0, yaw_rad=0.0, length_m=4.0, width_m=2.0)

    result = bumper_to_bumper_gap(ego, target)

    assert result["gap_m"] == 16.0
    assert result["lateral_offset_m"] == 3.0


def test_bumper_gap_missing_lengths_is_degraded_fallback() -> None:
    ego = ActorKinematics2D(x=0.0, y=0.0, yaw_rad=0.0)
    target = ActorKinematics2D(x=20.0, y=0.0, yaw_rad=0.0)

    result = bumper_to_bumper_gap(ego, target)

    assert result["status"] == "warn"
    assert result["gap_method"] == "center_distance_fallback"
    assert result["degraded"] is True
