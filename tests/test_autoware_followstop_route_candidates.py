from __future__ import annotations

from carla_testbed.autoware.followstop_route_candidates import (
    build_candidate_report,
    scan_followstop_route_candidates,
)


class Loc:
    def __init__(self, x: float, y: float, z: float = 0.0):
        self.x = x
        self.y = y
        self.z = z


class Rot:
    def __init__(self, yaw: float):
        self.yaw = yaw


class Tf:
    def __init__(self, x: float, y: float, yaw: float = 0.0):
        self.location = Loc(x, y, 0.0)
        self.rotation = Rot(yaw)


class FakeWaypoint:
    def __init__(self, x: float, y: float = 0.0, yaw: float = 0.0, *, max_s: float = 500.0):
        self.transform = Tf(x, y, yaw)
        self._s = x
        self._max_s = max_s

    def next(self, distance: float):
        next_s = self._s + float(distance)
        if next_s > self._max_s:
            return []
        return [FakeWaypoint(next_s, 0.0, 0.0, max_s=self._max_s)]


class FakeMap:
    def __init__(self, max_s: float = 500.0):
        self.max_s = max_s

    def get_waypoint(self, location, project_to_road=True):
        return FakeWaypoint(float(location.x), 0.0, 0.0, max_s=self.max_s)


def test_scan_followstop_route_candidates_finds_300m_front_and_goal_beyond():
    spawns = [
        Tf(0.0, 0.0, 0.0),
        Tf(300.0, 0.3, 0.0),
        Tf(120.0, 20.0, 90.0),
    ]

    candidates = scan_followstop_route_candidates(
        FakeMap(max_s=420.0),
        spawns,
        front_target_ahead_m=300.0,
        goal_beyond_front_m=80.0,
    )

    assert len(candidates) == 1
    assert candidates[0].ego_idx == 0
    assert candidates[0].front_idx == 1
    assert candidates[0].front_relative.longitudinal_m == 300.0
    assert abs(candidates[0].front_relative.lateral_m) < 1.0
    assert candidates[0].goal_pose.x == 380.0


def test_scan_followstop_route_candidates_requires_goal_continuation():
    spawns = [Tf(0.0, 0.0, 0.0), Tf(300.0, 0.0, 0.0)]

    candidates = scan_followstop_route_candidates(
        FakeMap(max_s=320.0),
        spawns,
        front_target_ahead_m=300.0,
        goal_beyond_front_m=80.0,
    )
    report = build_candidate_report(
        map_name="Town01",
        candidates=candidates,
        front_target_ahead_m=300.0,
        goal_beyond_front_m=80.0,
    )

    assert candidates == []
    assert report["status"] == "insufficient_data"
    assert "no_candidate_with_goal_beyond_front" in report["warnings"]
