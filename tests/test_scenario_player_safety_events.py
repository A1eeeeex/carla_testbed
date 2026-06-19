from __future__ import annotations

import json

from carla_testbed.scenario_player.safety_events import SafetyEventTracker, empty_safety_snapshot


def test_safety_event_tracker_records_collision_and_lane_invasion(tmp_path) -> None:
    world = _SafetyWorld()
    ego = object()

    tracker = SafetyEventTracker(world=world, ego=ego, artifact_dir=tmp_path)
    world.trigger("sensor.other.collision", _Event(frame=10, timestamp=1.0, other_actor=_OtherActor()))
    world.trigger(
        "sensor.other.lane_invasion",
        _Event(frame=11, timestamp=1.1, crossed_lane_markings=[_LaneMarking("Solid")]),
    )

    snapshot = tracker.snapshot()
    assert snapshot["collision_count"] == 1
    assert snapshot["lane_invasion_count"] == 1
    assert snapshot["collision_sensor_available"] is True
    assert snapshot["lane_invasion_sensor_available"] is True
    assert snapshot["warnings"] == []

    rows = [json.loads(line) for line in (tmp_path / "safety_event_trace.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["event_type"] for row in rows] == ["collision", "lane_invasion"]
    assert rows[0]["schema_version"] == "builtin_safety_event.v1"
    assert rows[0]["other_actor_id"] == 77
    assert rows[0]["other_actor_type_id"] == "vehicle.test"
    assert rows[1]["crossed_lane_marking_count"] == 1
    assert rows[1]["crossed_lane_marking_types"] == ["Solid"]

    assert tracker.destroy() == []
    assert all(sensor.stopped for sensor in world.sensors)
    assert all(sensor.destroyed for sensor in world.sensors)


def test_safety_event_tracker_degrades_when_blueprints_are_missing(tmp_path) -> None:
    tracker = SafetyEventTracker(world=_NoBlueprintWorld(), ego=object(), artifact_dir=tmp_path)

    snapshot = tracker.snapshot()

    assert snapshot["collision_count"] == 0
    assert snapshot["lane_invasion_count"] == 0
    assert snapshot["collision_sensor_available"] is False
    assert snapshot["lane_invasion_sensor_available"] is False
    assert "collision_sensor_blueprint_missing:sensor.other.collision" in snapshot["warnings"]
    assert "lane_invasion_sensor_blueprint_missing:sensor.other.lane_invasion" in snapshot["warnings"]
    assert tracker.destroy() == []


def test_safety_event_tracker_creates_empty_trace_without_events(tmp_path) -> None:
    tracker = SafetyEventTracker(world=_SafetyWorld(), ego=object(), artifact_dir=tmp_path)

    trace_path = tmp_path / "safety_event_trace.jsonl"

    assert trace_path.is_file()
    assert trace_path.read_text(encoding="utf-8") == ""
    assert tracker.snapshot()["collision_count"] == 0
    assert tracker.snapshot()["lane_invasion_count"] == 0


def test_empty_safety_snapshot_marks_surface_unavailable() -> None:
    snapshot = empty_safety_snapshot()

    assert snapshot["collision_count"] == 0
    assert snapshot["lane_invasion_count"] == 0
    assert snapshot["collision_sensor_available"] is False
    assert snapshot["lane_invasion_sensor_available"] is False
    assert snapshot["warnings"] == ["safety_event_tracker_not_initialized"]


class _SafetyWorld:
    def __init__(self) -> None:
        self.blueprints = _SafetyBlueprintLibrary()
        self.sensors: list[_SafetySensor] = []

    def get_blueprint_library(self):
        return self.blueprints

    def spawn_actor(self, blueprint, transform, attach_to=None):
        sensor = _SafetySensor(blueprint.id)
        self.sensors.append(sensor)
        return sensor

    def trigger(self, blueprint_id, event):
        for sensor in self.sensors:
            if sensor.blueprint_id == blueprint_id:
                sensor.trigger(event)


class _NoBlueprintWorld:
    def get_blueprint_library(self):
        raise RuntimeError("no blueprint library")


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
