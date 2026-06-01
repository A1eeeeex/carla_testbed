from __future__ import annotations

from types import SimpleNamespace

from carla_testbed.scenarios.followstop_geometry import (
    relative_front_alignment,
    select_aligned_front_spawn_index,
    select_waypoint_ahead_transform,
)


def _tf(x: float, y: float, yaw: float = 0.0):
    return SimpleNamespace(
        location=SimpleNamespace(x=x, y=y, z=0.0),
        rotation=SimpleNamespace(yaw=yaw, pitch=0.0, roll=0.0),
    )


def test_selects_nearest_same_lane_front_spawn():
    spawns = [
        _tf(0.0, 0.0, 0.0),
        _tf(100.0, 8.0, 0.0),
        _tf(50.0, 0.5, 0.0),
        _tf(-20.0, 0.0, 0.0),
    ]

    selection = select_aligned_front_spawn_index(
        spawns,
        ego_idx=0,
        requested_front_idx=1,
        min_ahead_m=20.0,
        max_ahead_m=320.0,
        max_lateral_m=4.0,
        max_heading_diff_deg=35.0,
    )

    assert selection.found is True
    assert selection.index == 2
    assert selection.candidate_count == 1
    assert selection.requested_alignment is not None
    assert "front_lateral_misaligned" in selection.requested_alignment.reasons


def test_selects_spawn_closest_to_target_distance_when_configured():
    spawns = [
        _tf(0.0, 0.0, 0.0),
        _tf(40.0, 0.0, 0.0),
        _tf(220.0, 0.0, 0.0),
        _tf(180.0, 0.0, 0.0),
    ]

    selection = select_aligned_front_spawn_index(
        spawns,
        ego_idx=0,
        requested_front_idx=1,
        min_ahead_m=20.0,
        max_ahead_m=320.0,
        max_lateral_m=4.0,
        max_heading_diff_deg=35.0,
        target_ahead_m=300.0,
    )

    assert selection.found is True
    assert selection.index == 2
    assert selection.selected_alignment is not None
    assert selection.selected_alignment.longitudinal_m == 220.0


def test_requested_front_alignment_flags_not_ahead_and_heading_mismatch():
    alignment = relative_front_alignment(
        _tf(0.0, 0.0, 0.0),
        _tf(0.0, 10.0, 90.0),
        min_ahead_m=20.0,
        max_ahead_m=320.0,
        max_lateral_m=4.0,
        max_heading_diff_deg=35.0,
    )

    assert alignment.aligned is False
    assert "front_not_ahead_of_ego" in alignment.reasons
    assert "front_lateral_misaligned" in alignment.reasons
    assert "front_heading_misaligned" in alignment.reasons


def test_far_front_is_not_accepted_as_followstop_spawn():
    alignment = relative_front_alignment(
        _tf(0.0, 0.0, 0.0),
        _tf(500.0, 0.0, 0.0),
        min_ahead_m=20.0,
        max_ahead_m=320.0,
        max_lateral_m=4.0,
        max_heading_diff_deg=35.0,
    )

    assert alignment.aligned is False
    assert alignment.longitudinal_m == 500.0
    assert "front_too_far_ahead" in alignment.reasons


class _FakeWaypoint:
    def __init__(self, transform, candidates=None):
        self.transform = transform
        self._candidates = list(candidates or [])

    def next(self, distance_m):
        assert distance_m == 300.0
        return self._candidates


def test_select_waypoint_ahead_prefers_heading_continuity():
    curved_branch = _FakeWaypoint(_tf(300.0, 20.0, 35.0))
    same_lane = _FakeWaypoint(_tf(300.0, 0.0, 2.0))
    start = _FakeWaypoint(_tf(0.0, 0.0, 0.0), [curved_branch, same_lane])

    selection = select_waypoint_ahead_transform(start, 300.0)

    assert selection.found is True
    assert selection.candidate_count == 2
    assert selection.selected_transform is same_lane.transform
    assert selection.selected_heading_diff_deg == 2.0


def test_select_waypoint_ahead_reports_missing_candidates():
    start = _FakeWaypoint(_tf(0.0, 0.0, 0.0), [])

    selection = select_waypoint_ahead_transform(start, 300.0)

    assert selection.found is False
    assert selection.selected_transform is None
    assert selection.reason == "no_waypoint_ahead"
