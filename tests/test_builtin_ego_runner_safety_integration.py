from __future__ import annotations

import csv
import json
from types import SimpleNamespace

from carla_testbed.scenario_player import builtin_ego_runner as runner


def test_builtin_runner_writes_safety_event_summary_timeseries_and_trace(monkeypatch, tmp_path) -> None:
    world = _FakeWorld()

    monkeypatch.setattr(runner, "_import_carla", lambda: _FakeCarla(world))
    monkeypatch.setattr(runner, "load_fixed_scene_template", lambda path: {"schema_version": "fixed_scene_template.v1"})
    monkeypatch.setattr(
        runner,
        "compile_fixed_scene_template",
        lambda template: {
            "scene_id": "fake_follow_stop",
            "scenario_class": "follow_stop_static",
            "roles": {"ego": {"spawn_ref": "carla_spawn_index_0"}},
            "params": {"duration_s": 0.05, "ego_target_speed_mps": 1.0},
        },
    )
    monkeypatch.setattr(runner, "CarlaFixedSceneRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(runner, "_spawn_ego", lambda world, spawn_index, **kwargs: world.ego)
    monkeypatch.setattr(runner, "apply_control_to_vehicle", lambda ego, command, stamp: _FakeApplyResult())
    monkeypatch.setattr(runner, "analyze_fixed_scene_contract_run_dir", lambda run_dir: {"status": "pass"})
    monkeypatch.setattr(runner, "analyze_scenario_actor_contract_run_dir", lambda run_dir: {"status": "pass"})
    monkeypatch.setattr(
        runner,
        "write_fixed_scene_contract_report",
        lambda report, out_dir: {"json": str(out_dir / "fixed_scene_contract_report.json")},
    )
    monkeypatch.setattr(
        runner,
        "write_scenario_actor_contract_report",
        lambda report, out_dir: {"json": str(out_dir / "scenario_actor_contract_report.json")},
    )
    monkeypatch.setattr(
        runner,
        "run_phase1_postprocess",
        lambda run_root: {
            "v_t_gap_status": "not_applicable",
            "phase1_status": "success",
            "phase1_failure_reason": None,
            "artifact_completeness_status": "pass",
        },
    )

    run_dir = tmp_path / "run"
    result = runner.run_builtin_ego_fixed_scene(
        template_path=tmp_path / "scenario.yaml",
        run_dir=run_dir,
        town="Town01",
        duration_s=0.05,
        fixed_dt_s=0.05,
    )

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["collision_count"] == 1
    assert summary["lane_invasion_count"] == 1
    assert summary["collision_sensor_available"] is True
    assert summary["lane_invasion_sensor_available"] is True
    assert summary["safety_event_trace_path"].endswith("artifacts/safety_event_trace.jsonl")
    assert result["safety_event_trace"].endswith("artifacts/safety_event_trace.jsonl")

    rows = list(csv.DictReader((run_dir / "timeseries.csv").open(encoding="utf-8", newline="")))
    assert rows
    assert rows[0]["collision_count"] == "1"
    assert rows[0]["lane_invasion_count"] == "1"

    event_rows = [
        json.loads(line)
        for line in (run_dir / "artifacts" / "safety_event_trace.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [row["event_type"] for row in event_rows] == ["collision", "lane_invasion"]
    assert {row["frame"] for row in event_rows} == {2}
    assert all(sensor.stopped for sensor in world.sensors)
    assert all(sensor.destroyed for sensor in world.sensors)


class _FakeCarla:
    def __init__(self, world):
        self._world = world

    def Client(self, host, port):  # noqa: N802 - mirrors CARLA API
        return _FakeClient(self._world)


class _FakeClient:
    def __init__(self, world):
        self._world = world

    def set_timeout(self, timeout_s):
        self.timeout_s = timeout_s

    def load_world(self, town):
        self.town = town
        return self._world


class _FakeRuntime:
    def __init__(self) -> None:
        self.actors = {}

    def setup(self, context, storyboard):
        return SimpleNamespace(errors=[])

    def tick(self, context):
        return None

    def teardown(self):
        return []


class _FakeApplyResult:
    def to_dict(self):
        return {"applied": True}


class _FakeWorld:
    def __init__(self) -> None:
        self.ego = _FakeActor()
        self.settings = SimpleNamespace(synchronous_mode=False, fixed_delta_seconds=None)
        self.map = _FakeMap()
        self.blueprints = _SafetyBlueprintLibrary()
        self.sensors: list[_SafetySensor] = []
        self.frame = 0

    def get_settings(self):
        return SimpleNamespace(
            synchronous_mode=self.settings.synchronous_mode,
            fixed_delta_seconds=self.settings.fixed_delta_seconds,
        )

    def apply_settings(self, settings):
        self.settings = settings

    def get_map(self):
        return self.map

    def get_blueprint_library(self):
        return self.blueprints

    def spawn_actor(self, blueprint, transform, attach_to=None):
        sensor = _SafetySensor(blueprint.id)
        self.sensors.append(sensor)
        return sensor

    def tick(self):
        self.frame += 1
        # Frame 1 represents the ego-spawn settling tick. The runner should not
        # attach safety sensors until after this tick, so these callbacks would
        # be ignored even if CARLA produced spawn-time transients.
        if self.frame in {1, 2}:
            self._trigger(
                "sensor.other.collision",
                _Event(frame=self.frame, timestamp=0.05 * self.frame, other_actor=_OtherActor()),
            )
            self._trigger(
                "sensor.other.lane_invasion",
                _Event(frame=self.frame, timestamp=0.05 * self.frame, crossed_lane_markings=[_LaneMarking("Solid")]),
            )
        return self.frame

    def _trigger(self, blueprint_id, event):
        for sensor in self.sensors:
            if sensor.blueprint_id == blueprint_id:
                sensor.trigger(event)


class _FakeMap:
    def get_waypoint(self, location):
        return SimpleNamespace(transform=_FakeTransform())


class _FakeActor:
    def __init__(self) -> None:
        self.bounding_box = SimpleNamespace(extent=SimpleNamespace(x=2.0, y=0.9, z=0.8))
        self.destroyed = False

    def get_transform(self):
        return _FakeTransform()

    def get_velocity(self):
        return SimpleNamespace(x=0.0, y=0.0, z=0.0)

    def destroy(self):
        self.destroyed = True


class _FakeTransform:
    def __init__(self) -> None:
        self.location = SimpleNamespace(x=0.0, y=0.0, z=0.0)
        self.rotation = SimpleNamespace(yaw=0.0)


class _SafetyBlueprintLibrary:
    def find(self, blueprint_id):
        return _SafetyBlueprint(blueprint_id)


class _SafetyBlueprint:
    def __init__(self, blueprint_id):
        self.id = blueprint_id


class _SafetySensor:
    def __init__(self, blueprint_id):
        self.blueprint_id = blueprint_id
        self.callback = None
        self.stopped = False
        self.destroyed = False

    def listen(self, callback):
        self.callback = callback

    def trigger(self, event):
        if self.callback is not None:
            self.callback(event)

    def stop(self):
        self.stopped = True

    def destroy(self):
        self.destroyed = True


class _Event:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _OtherActor:
    id = 77
    type_id = "vehicle.test"


class _LaneMarking:
    def __init__(self, marking_type):
        self.type = marking_type
