from __future__ import annotations

import carla_testbed.scenarios.town01_route_health as route_health_module
from carla_testbed.scenarios import Town01RouteHealthConfig, Town01RouteHealthScenario


class _FakeTrafficLight:
    def __init__(self, actor_id: int):
        self.id = actor_id
        self.state = None
        self.freeze_calls: list[bool] = []

    def set_state(self, state) -> None:
        self.state = state

    def freeze(self, frozen: bool) -> None:
        self.freeze_calls.append(bool(frozen))


class _FakeActorList(list):
    def filter(self, _pattern: str):
        return list(self)


class _FakeWorld:
    def __init__(self, actors):
        self._actors = _FakeActorList(actors)

    def get_actors(self):
        return self._actors


def test_deterministic_traffic_light_policy_sets_initial_and_release_state() -> None:
    light = _FakeTrafficLight(101)
    world = _FakeWorld([light])
    scenario = Town01RouteHealthScenario(
        Town01RouteHealthConfig(
            traffic_light_control_mode="deterministic_gt_control",
            traffic_light_initial_state="RED",
            traffic_light_release_state="GREEN",
            traffic_light_release_after_s=1.0,
            freeze_traffic_lights=True,
        )
    )

    scenario._apply_traffic_light_policy(world)
    scenario.on_sim_tick(frame_id=1, timestamp=10.0, step=0)
    scenario.on_sim_tick(frame_id=2, timestamp=11.1, step=22)

    assert light.state == route_health_module.carla.TrafficLightState.Green
    assert light.freeze_calls == [True, True]
    meta = scenario.metadata()["traffic_light_control"]
    assert meta["stimulus_mode"] == "deterministic_gt_control"
    assert meta["initial_state"] == "RED"
    assert meta["release_state"] == "GREEN"
    assert meta["release_frame_id"] == 2
    assert [event["phase"] for event in meta["events"]] == ["initial", "release"]


def test_deterministic_traffic_light_policy_can_target_actor_ids() -> None:
    target = _FakeTrafficLight(101)
    other = _FakeTrafficLight(202)
    world = _FakeWorld([target, other])
    scenario = Town01RouteHealthScenario(
        Town01RouteHealthConfig(
            traffic_light_control_mode="deterministic_gt_control",
            traffic_light_initial_state="YELLOW",
            traffic_light_target_actor_ids=(101,),
        )
    )

    scenario._apply_traffic_light_policy(world)

    assert target.state == route_health_module.carla.TrafficLightState.Yellow
    assert other.state is None
    meta = scenario.metadata()["traffic_light_control"]
    assert meta["scope"] == "target_actor_ids"
    assert meta["target_actor_ids"] == [101]


def test_default_traffic_light_policy_does_not_change_lights() -> None:
    light = _FakeTrafficLight(101)
    scenario = Town01RouteHealthScenario(Town01RouteHealthConfig())

    scenario._apply_traffic_light_policy(_FakeWorld([light]))

    assert light.state is None
    assert "traffic_light_control" not in scenario.metadata()
