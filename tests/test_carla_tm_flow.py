from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from carla_testbed.traffic.carla_tm_flow import CarlaTrafficManagerFlow


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


class _Blueprint:
    id = "vehicle.tesla.model3"

    def __init__(self) -> None:
        self.attrs = {}

    def has_attribute(self, name: str) -> bool:
        return name == "role_name"

    def set_attribute(self, name: str, value: str) -> None:
        self.attrs[name] = value


class _BlueprintLibrary:
    def filter(self, pattern: str) -> list[_Blueprint]:
        del pattern
        return [_Blueprint()]


class _Actor:
    _next_id = 100

    def __init__(self, blueprint: _Blueprint) -> None:
        self.id = _Actor._next_id
        _Actor._next_id += 1
        self.type_id = blueprint.id
        self.autopilot = None
        self.destroyed = False

    def set_autopilot(self, enabled: bool, port: int) -> None:
        self.autopilot = (enabled, port)

    def destroy(self) -> None:
        self.destroyed = True


class _Map:
    def get_spawn_points(self) -> list[_Transform]:
        return [
            _Transform(_Location(40, 0), _Rotation(0)),
            _Transform(_Location(50, 0), _Rotation(0)),
        ]

    def get_waypoint(self, location: _Location) -> object:
        del location
        return type("Waypoint", (), {"road_id": 1, "lane_id": -1, "is_junction": False})()


class _World:
    def __init__(self) -> None:
        self.actors: list[_Actor] = []

    def get_settings(self) -> object:
        return type("Settings", (), {"synchronous_mode": True})()

    def get_map(self) -> _Map:
        return _Map()

    def get_blueprint_library(self) -> _BlueprintLibrary:
        return _BlueprintLibrary()

    def try_spawn_actor(self, blueprint: _Blueprint, transform: _Transform) -> _Actor:
        del transform
        actor = _Actor(blueprint)
        self.actors.append(actor)
        return actor


class _TrafficManager:
    def __init__(self) -> None:
        self.sync = None
        self.seed = None
        self.calls: list[tuple[str, object]] = []

    def set_synchronous_mode(self, value: bool) -> None:
        self.sync = value

    def set_random_device_seed(self, value: int) -> None:
        self.seed = value

    def global_distance_to_leading_vehicle(self, value: float) -> None:
        self.calls.append(("global_distance", value))

    def global_percentage_speed_difference(self, value: float) -> None:
        self.calls.append(("global_speed", value))

    def vehicle_percentage_speed_difference(self, actor: _Actor, value: float) -> None:
        self.calls.append(("vehicle_speed", actor.id, value))

    def distance_to_leading_vehicle(self, actor: _Actor, value: float) -> None:
        self.calls.append(("distance", actor.id, value))

    def auto_lane_change(self, actor: _Actor, value: bool) -> None:
        self.calls.append(("auto_lane_change", actor.id, value))

    def update_vehicle_lights(self, actor: _Actor, value: bool) -> None:
        self.calls.append(("lights", actor.id, value))


class _Client:
    def __init__(self) -> None:
        self.tm = _TrafficManager()

    def get_trafficmanager(self, port: int) -> _TrafficManager:
        assert port == 8000
        return self.tm


def test_carla_tm_flow_setup_uses_tm_and_writes_artifacts(tmp_path: Path) -> None:
    client = _Client()
    world = _World()
    context = type(
        "Context",
        (),
        {
            "carla_client": client,
            "world": world,
            "run_dir": tmp_path,
            "ego_location": _Location(0, 0),
            "route_points": [_Location(40, 0), _Location(50, 0)],
        },
    )()
    config = {
        "traffic_flow": {
            "enabled": True,
            "provider": "carla_traffic_manager",
            "seed": 42,
            "traffic_manager": {"port": 8000, "synchronous_mode": "follow_world", "deterministic": True},
            "vehicles": {
                "count": 2,
                "spawn_policy": {"min_distance_from_ego_m": 35.0, "max_distance_from_ego_m": 80.0},
                "blueprints": {"include": ["vehicle.tesla.model3"], "exclude": []},
                "behavior": {"auto_lane_change": True},
            },
        }
    }

    state = CarlaTrafficManagerFlow().setup(context, config)

    assert state.spawned_count == 2
    assert client.tm.sync is True
    assert client.tm.seed == 42
    assert all(actor.autopilot == (True, 8000) for actor in world.actors)
    manifest = json.loads((tmp_path / "artifacts/traffic_flow_manifest.json").read_text(encoding="utf-8"))
    assert manifest["provider"] == "carla_traffic_manager"
    assert manifest["requested_vehicle_count"] == 2
    assert manifest["actors"][0]["control_source"] == "carla_traffic_manager"
    assert (tmp_path / "artifacts/traffic_flow_events.jsonl").is_file()
    assert (tmp_path / "artifacts/traffic_spawn_candidates.jsonl").is_file()
    events = [
        json.loads(line)
        for line in (tmp_path / "artifacts/traffic_flow_events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert "spawned" in {event["event"] for event in events}


def test_carla_tm_flow_accepts_carla_0916_global_distance_name(tmp_path: Path) -> None:
    class _Carla0916TrafficManager(_TrafficManager):
        def global_distance_to_leading_vehicle(self, value: float) -> None:  # type: ignore[override]
            raise AssertionError("CARLA 0.9.16 uses set_global_distance_to_leading_vehicle")

        def set_global_distance_to_leading_vehicle(self, value: float) -> None:
            self.calls.append(("set_global_distance", value))

    class _Carla0916Client(_Client):
        def __init__(self) -> None:
            self.tm = _Carla0916TrafficManager()

    client = _Carla0916Client()
    world = _World()
    context = type(
        "Context",
        (),
        {
            "carla_client": client,
            "world": world,
            "run_dir": tmp_path,
            "ego_location": _Location(0, 0),
            "route_points": [_Location(40, 0), _Location(50, 0)],
        },
    )()
    config = {
        "traffic_flow": {
            "enabled": True,
            "provider": "carla_traffic_manager",
            "seed": 42,
            "traffic_manager": {
                "port": 8000,
                "synchronous_mode": "follow_world",
                "deterministic": True,
                "global_distance_to_leading_vehicle_m": 9.0,
            },
            "vehicles": {
                "count": 1,
                "spawn_policy": {"min_distance_from_ego_m": 35.0, "max_distance_from_ego_m": 80.0},
                "blueprints": {"include": ["vehicle.tesla.model3"], "exclude": []},
            },
        }
    }

    state = CarlaTrafficManagerFlow().setup(context, config)

    assert state.spawned_count == 1
    assert ("set_global_distance", 9.0) in client.tm.calls
