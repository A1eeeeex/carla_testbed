from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path


def _install_fake_carla(monkeypatch) -> None:
    class Location:
        def __init__(self, x=0.0, y=0.0, z=0.0) -> None:
            self.x = float(x)
            self.y = float(y)
            self.z = float(z)

    class Rotation:
        def __init__(self, yaw=0.0, pitch=0.0, roll=0.0) -> None:
            self.yaw = float(yaw)
            self.pitch = float(pitch)
            self.roll = float(roll)

    class Transform:
        def __init__(self, location=None, rotation=None) -> None:
            self.location = location or Location()
            self.rotation = rotation or Rotation()

    fake_carla = types.SimpleNamespace(
        Actor=object,
        Vehicle=object,
        Location=Location,
        Rotation=Rotation,
        Transform=Transform,
    )
    monkeypatch.setitem(sys.modules, "carla", fake_carla)


class _FakeMap:
    def get_spawn_points(self):
        return []


class _FakeWorld:
    def get_map(self):
        return _FakeMap()

    def get_settings(self):
        return types.SimpleNamespace(synchronous_mode=False)


class _FakeActor:
    def __init__(self, transform) -> None:
        self._transform = transform
        self._world = _FakeWorld()

    def get_transform(self):
        return self._transform

    def get_world(self):
        return self._world


class _FakeScenario:
    def __init__(self, metadata) -> None:
        self._metadata = metadata

    def metadata(self):
        return dict(self._metadata)


def test_town01_route_health_scenario_goal_is_written_in_apollo_map_frame(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _install_fake_carla(monkeypatch)
    run_followstop = importlib.import_module("examples.run_followstop")
    carla = sys.modules["carla"]

    ego = _FakeActor(
        carla.Transform(
            carla.Location(x=2.0, y=208.0, z=0.3),
            carla.Rotation(yaw=0.0),
        )
    )
    scenario = _FakeScenario(
        {
            "scenario_driver": "town01_route_health",
            "scenario_goal_raw_carla": {"x": 154.0, "y": 37.0, "z": 0.0},
            "route_length_m": 229.18,
            "route_length_m_role": "legacy_selection_straight_line_distance",
            "route_length_m_claim_grade": False,
            "claim_route_length_m": 388.41,
            "claim_route_length_source": "route_trace_s_span",
            "route_trace_length_m": 388.41,
            "route_trace_length_source": "route_trace_s_span",
            "route_trace_point_count": 45,
            "spawn_lane": "lane_start",
            "goal_lane": "lane_goal",
        }
    )
    effective_cfg = {
        "scenario": {"driver": "town01_route_health"},
        "algo": {
            "stack": "apollo",
            "apollo": {
                "routing": {"goal_mode": "scenario_xy"},
                "carla_to_apollo": {"tx": 0.0, "ty": 0.0, "tz": 0.0, "yaw_deg": 0.0},
            },
        },
    }
    args = types.SimpleNamespace(
        ros_invert_tf=True,
        ego_idx=-1,
        front_idx=-1,
        scenario_goal_x=None,
        scenario_goal_y=None,
        scenario_goal_z=None,
        scenario_goal_ahead_m=None,
    )

    out_path = run_followstop._write_apollo_scenario_goal(
        effective_cfg,
        tmp_path,
        ego,
        front=None,
        args=args,
        scenario=scenario,
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["frame"] == "apollo_map"
    assert payload["source"] == "scenario_metadata_apollo_map_xy"
    assert payload["source_raw_frame"] == "carla_raw"
    assert payload["goal"] == {"x": 154.0, "y": -37.0, "z": 0.0}
    assert payload["goal_raw_carla"] == {"x": 154.0, "y": 37.0, "z": 0.0}
    assert payload["start_at_write_time"]["x"] == 2.0
    assert payload["start_at_write_time"]["y"] == -208.0

    route_meta = payload["route_health_metadata"]
    assert route_meta["route_length_m"] == 229.18
    assert route_meta["route_length_m_role"] == "legacy_selection_straight_line_distance"
    assert route_meta["route_length_m_claim_grade"] is False
    assert route_meta["claim_route_length_m"] == 388.41
    assert route_meta["route_trace_length_m"] == 388.41
