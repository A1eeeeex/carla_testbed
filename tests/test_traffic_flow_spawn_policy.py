from __future__ import annotations

from dataclasses import dataclass

from carla_testbed.traffic.spawn_policy import select_spawn_points_near_route


@dataclass
class _Location:
    x: float
    y: float
    z: float = 0.0


@dataclass
class _Rotation:
    yaw: float = 0.0


@dataclass
class _Transform:
    location: _Location
    rotation: _Rotation


@dataclass
class _Waypoint:
    road_id: int
    lane_id: int
    is_junction: bool = False


def _spawns() -> list[_Transform]:
    return [
        _Transform(_Location(10, 0), _Rotation()),
        _Transform(_Location(40, 0), _Rotation()),
        _Transform(_Location(50, 0), _Rotation()),
        _Transform(_Location(60, 0), _Rotation()),
    ]


def test_seed_reproducible_spawn_selection() -> None:
    policy = {"min_distance_from_ego_m": 20.0, "max_distance_from_ego_m": 100.0}

    first, _ = select_spawn_points_near_route(
        _spawns(),
        ego_location=_Location(0, 0),
        policy=policy,
        seed=42,
        count=2,
    )
    second, _ = select_spawn_points_near_route(
        _spawns(),
        ego_location=_Location(0, 0),
        policy=policy,
        seed=42,
        count=2,
    )
    third, _ = select_spawn_points_near_route(
        _spawns(),
        ego_location=_Location(0, 0),
        policy=policy,
        seed=7,
        count=2,
    )

    assert [item.location.x for item in first] == [item.location.x for item in second]
    assert [item.location.x for item in first] != [item.location.x for item in third]


def test_spawn_policy_filters_ego_distance() -> None:
    selected, candidates = select_spawn_points_near_route(
        _spawns(),
        ego_location=_Location(0, 0),
        policy={"min_distance_from_ego_m": 35.0, "max_distance_from_ego_m": 45.0},
        seed=1,
        count=3,
    )

    assert [item.location.x for item in selected] == [40]
    assert candidates[0]["reject_reason"] == "too_close_to_ego"
    assert candidates[2]["reject_reason"] == "too_far_from_ego"


def test_spawn_policy_filters_junctions() -> None:
    def waypoint_lookup(location: _Location) -> _Waypoint:
        return _Waypoint(road_id=1, lane_id=-1, is_junction=location.x == 40)

    selected, candidates = select_spawn_points_near_route(
        _spawns(),
        ego_location=_Location(0, 0),
        policy={"min_distance_from_ego_m": 20.0, "max_distance_from_ego_m": 100.0, "avoid_junctions": True},
        seed=1,
        count=3,
        waypoint_lookup=waypoint_lookup,
    )

    assert all(item.location.x != 40 for item in selected)
    assert candidates[1]["reject_reason"] == "junction"
