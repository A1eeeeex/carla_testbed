from __future__ import annotations

import json
from types import SimpleNamespace

from carla_testbed.scenario_player import builtin_ego_runner as runner


def test_ego_spawn_s_offset_reader_accepts_nested_spawn_block() -> None:
    storyboard = {
        "roles": {
            "ego": {
                "spawn_ref": "carla_spawn_index_0",
                "spawn": {"s_offset_m": 2.0},
            }
        }
    }

    assert runner._ego_spawn_s_offset_m(storyboard) == 2.0


def test_ego_spawn_transform_applies_configured_route_offset() -> None:
    base = _Transform(x=0.0, y=0.0, z=0.7, yaw=0.0)
    shifted = _Transform(x=2.0, y=0.0, yaw=0.0)
    world = _SpawnOffsetWorld(base, shifted)

    transform = runner._ego_spawn_transform(world, spawn_index=0, spawn_s_offset_m=2.0)

    assert transform.location.x == 2.0
    assert transform.location.y == 0.0
    assert transform.location.z == 0.7
    assert world.map.requested_offset_m == 2.0


def test_builtin_runner_manifest_records_scenario_spawn_offset(monkeypatch, tmp_path) -> None:
    world = _RunnerWorld()

    monkeypatch.setattr(runner, "_import_carla", lambda: _FakeCarla(world))
    monkeypatch.setattr(runner, "load_fixed_scene_template", lambda path: {"schema_version": "fixed_scene_template.v1"})
    monkeypatch.setattr(
        runner,
        "compile_fixed_scene_template",
        lambda template: {
            "scene_id": "fake_spawn_offset",
            "scenario_class": "follow_stop_static",
            "roles": {
                "ego": {
                    "spawn_ref": "carla_spawn_index_0",
                    "spawn": {"s_offset_m": 2.0},
                }
            },
            "params": {"duration_s": 0.05, "ego_target_speed_mps": 1.0},
        },
    )
    monkeypatch.setattr(runner, "CarlaFixedSceneRuntime", lambda: _FakeRuntime())
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
    runner.run_builtin_ego_fixed_scene(
        template_path=tmp_path / "scenario.yaml",
        run_dir=run_dir,
        town="Town01",
        duration_s=0.05,
        fixed_dt_s=0.05,
    )

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert manifest["ego_spawn_s_offset_m"] == 2.0
    assert manifest["ego_spawn_source"] == "carla_spawn_waypoint_offset"
    assert summary["ego_spawn_s_offset_m"] == 2.0
    assert world.spawn_transform.location.x == 2.0


class _Location:
    def __init__(self, *, x: float, y: float, z: float = 0.0) -> None:
        self.x = x
        self.y = y
        self.z = z


class _Rotation:
    def __init__(self, *, yaw: float) -> None:
        self.yaw = yaw


class _Transform:
    def __init__(self, *, x: float, y: float, yaw: float, z: float = 0.0) -> None:
        self.location = _Location(x=x, y=y, z=z)
        self.rotation = _Rotation(yaw=yaw)


class _Waypoint:
    def __init__(self, transform: _Transform, next_waypoints: list["_Waypoint"]) -> None:
        self.transform = transform
        self._next_waypoints = next_waypoints

    def next(self, distance_m: float):
        return list(self._next_waypoints)


class _SpawnOffsetWorld:
    def __init__(self, base: _Transform, shifted: _Transform) -> None:
        self.map = _SpawnOffsetMap(base, shifted)

    def get_map(self):
        return self.map


class _SpawnOffsetMap:
    def __init__(self, base: _Transform, shifted: _Transform) -> None:
        self.base = base
        self.shifted = shifted
        self.requested_offset_m = None

    def get_spawn_points(self):
        return [self.base]

    def get_waypoint(self, location, project_to_road=True):
        return _SpawnOffsetWaypoint(self.base, self.shifted, self)


class _SpawnOffsetWaypoint:
    def __init__(self, base: _Transform, shifted: _Transform, parent: _SpawnOffsetMap) -> None:
        self.transform = base
        self._shifted = shifted
        self._parent = parent

    def next(self, distance_m: float):
        self._parent.requested_offset_m = float(distance_m)
        return [_Waypoint(self._shifted, [])]


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


class _RunnerWorld:
    def __init__(self) -> None:
        self.base = _Transform(x=0.0, y=0.0, yaw=0.0)
        self.shifted = _Transform(x=2.0, y=0.0, yaw=0.0)
        self.map = _SpawnOffsetMap(self.base, self.shifted)
        self.blueprints = _BlueprintLibrary()
        self.settings = SimpleNamespace(synchronous_mode=False, fixed_delta_seconds=None)
        self.spawn_transform = None
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

    def try_spawn_actor(self, blueprint, transform):
        self.spawn_transform = transform
        return _Actor(transform)

    def spawn_actor(self, blueprint, transform, attach_to=None):
        return _Sensor() if attach_to is not None else _Actor(transform)

    def tick(self):
        self.frame += 1
        return self.frame


class _BlueprintLibrary:
    def find(self, blueprint_id):
        return SimpleNamespace(id=blueprint_id)


class _Actor:
    def __init__(self, transform):
        self._transform = transform
        self.bounding_box = SimpleNamespace(extent=SimpleNamespace(x=2.0, y=0.9, z=0.8))

    def get_transform(self):
        return self._transform

    def get_velocity(self):
        return SimpleNamespace(x=0.0, y=0.0, z=0.0)

    def destroy(self):
        return None


class _Sensor:
    def listen(self, callback):
        self.callback = callback

    def stop(self):
        return None

    def destroy(self):
        return None
