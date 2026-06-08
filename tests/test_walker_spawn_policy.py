from __future__ import annotations

from dataclasses import dataclass

from carla_testbed.traffic.walker_spawn_policy import select_walker_spawn_locations


@dataclass(frozen=True)
class _Location:
    x: float
    y: float
    z: float = 0.0


def test_walker_spawn_policy_selects_near_route_not_ego() -> None:
    selected, candidates = select_walker_spawn_locations(
        [_Location(2, 0), _Location(30, 2), _Location(120, 0), _Location(35, 3)],
        ego_location=_Location(0, 0),
        route_points=[_Location(30, 0), _Location(40, 0)],
        policy={
            "min_distance_from_ego_m": 10.0,
            "max_distance_from_ego_m": 80.0,
            "route_near_preferred": True,
            "route_near_threshold_m": 10.0,
        },
        seed=3,
        count=2,
    )

    assert len(selected) == 2
    assert {tuple((item["location"]["x"], item["location"]["y"])) for item in candidates if item["selected"]} == {
        (30.0, 2.0),
        (35.0, 3.0),
    }
    rejected = {item["reject_reason"] for item in candidates if item["reject_reason"]}
    assert "too_close_to_ego" in rejected
    assert "too_far_from_ego" in rejected


def test_walker_spawn_policy_avoids_existing_actors() -> None:
    selected, candidates = select_walker_spawn_locations(
        [_Location(30, 0), _Location(40, 0)],
        ego_location=_Location(0, 0),
        existing_actor_locations=[_Location(30.5, 0)],
        policy={"min_distance_from_ego_m": 5.0, "avoid_existing_actor_radius_m": 2.0},
        seed=1,
        count=2,
    )

    assert len(selected) == 1
    assert candidates[0]["reject_reason"] == "too_close_to_existing_actor"
