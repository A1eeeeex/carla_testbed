from __future__ import annotations

import json
import math

from carla_testbed.contracts import (
    ChassisState,
    EgoState,
    FrameStamp,
    ObstacleTruth,
    Pose3D,
    Quaternion,
    SceneTruth,
    Vector3D,
)
from carla_testbed.control.simple_acc_route_follower import (
    SimpleAccRouteFollowerConfig,
    SimpleAccRouteFollowerController,
)
from carla_testbed.scenario_player.builtin_ego_runner import _ego_spawn_s_offset_m, _ego_spawn_transform
from carla_testbed.scenario_player.builtin_ego_runner import _route_lookahead_heading_error
from carla_testbed.scenario_player.builtin_ego_runner import _prepare_route_plan, _route_plan_metadata
from carla_testbed.scenario_player.builtin_ego_runner import _SafetyEventTracker


def test_builtin_ego_controller_tracks_target_speed_without_lead() -> None:
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=19.44))
    frame = FrameStamp(frame_id=1, sim_time_s=0.05)
    ego = _ego(frame, speed_mps=5.0)

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego))

    assert command.source == "carla_testbed_builtin_controller"
    assert command.throttle > 0.0
    assert command.brake == 0.0
    assert command.metadata["target_speed_mps"] == 19.44
    assert command.metadata["lead_gap_m"] is None


def test_builtin_ego_controller_brakes_for_close_lead_vehicle() -> None:
    controller = SimpleAccRouteFollowerController(
        SimpleAccRouteFollowerConfig(target_speed_mps=19.44, target_gap_m=20.0, time_headway_s=0.0)
    )
    frame = FrameStamp(frame_id=2, sim_time_s=0.1)
    ego = _ego(frame, speed_mps=19.0)
    lead = _lead("lead", x=12.0, speed_mps=11.0)

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego, obstacles=(lead,)))

    assert command.throttle == 0.0
    assert command.brake > 0.0
    assert command.metadata["lead_gap_m"] == 12.0
    assert command.metadata["lead_speed_mps"] == 11.0
    assert command.metadata["lead_selection_mode"] == "ego_frame_lateral_gate"
    assert command.metadata["effective_target_speed_mps"] < command.metadata["target_speed_mps"]


def test_builtin_ego_controller_tracks_explicit_target_role_after_lateral_gate_loss() -> None:
    controller = SimpleAccRouteFollowerController(
        SimpleAccRouteFollowerConfig(target_speed_mps=19.44, target_gap_m=20.0, time_headway_s=0.0)
    )
    frame = FrameStamp(frame_id=5, sim_time_s=0.25)
    ego = _ego(frame, speed_mps=16.0)
    lead = _lead("lead", x=2.0, y=10.0, speed_mps=4.0)

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego, obstacles=(lead,)))

    assert command.throttle == 0.0
    assert command.brake > 0.0
    assert command.metadata["lead_obstacle_id"] == "lead"
    assert command.metadata["lead_selection_mode"] == "explicit_target_role_distance_fallback"
    assert command.metadata["lead_gap_m"] < 20.0


def test_builtin_ego_controller_uses_explicit_target_route_progress_gap() -> None:
    controller = SimpleAccRouteFollowerController(
        SimpleAccRouteFollowerConfig(target_speed_mps=22.22, target_gap_m=20.0, time_headway_s=0.0)
    )
    frame = FrameStamp(frame_id=7, sim_time_s=0.35)
    ego = _ego(frame, speed_mps=20.0)
    lead = _lead(
        "lead",
        x=250.0,
        y=200.0,
        speed_mps=4.0,
        metadata={
            "role": "lead_vehicle",
            "route_progress_gap_m": 12.0,
            "route_progress_gap_source": "trajectory_progress_initial_center_gap",
        },
    )

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego, obstacles=(lead,)))

    assert command.throttle == 0.0
    assert command.brake > 0.0
    assert command.metadata["lead_obstacle_id"] == "lead"
    assert command.metadata["lead_gap_m"] == 12.0
    assert command.metadata["lead_selection_mode"] == "explicit_target_role_route_progress_gap"


def test_builtin_ego_controller_does_not_use_unlabeled_lateral_obstacle_as_target() -> None:
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=19.44))
    frame = FrameStamp(frame_id=6, sim_time_s=0.3)
    ego = _ego(frame, speed_mps=5.0)
    obstacle = _lead("other", x=10.0, y=20.0, speed_mps=4.0, role="background_vehicle")

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego, obstacles=(obstacle,)))

    assert command.metadata["lead_obstacle_id"] is None
    assert command.metadata["lead_selection_mode"] is None
    assert command.throttle > 0.0
    assert command.brake == 0.0


def test_builtin_ego_controller_forces_brake_without_throttle_at_emergency_gap() -> None:
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=19.44))
    frame = FrameStamp(frame_id=3, sim_time_s=0.15)
    ego = _ego(frame, speed_mps=2.0)
    lead = _lead("lead", x=4.0, speed_mps=0.0)

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego, obstacles=(lead,)))

    assert command.throttle == 0.0
    assert command.brake >= 0.8


def test_builtin_ego_controller_uses_route_error_for_steer() -> None:
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=5.0))
    frame = FrameStamp(frame_id=4, sim_time_s=0.2)
    ego = _ego(frame, speed_mps=5.0, metadata={"heading_error_rad": 0.2, "cross_track_error_m": 1.0})

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego))

    assert command.steer < 0.0


def test_builtin_ego_controller_prefers_lookahead_heading_error_for_steer() -> None:
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=5.0))
    frame = FrameStamp(frame_id=5, sim_time_s=0.25)
    ego = _ego(
        frame,
        speed_mps=5.0,
        metadata={
            "heading_error_rad": 0.2,
            "route_lookahead_heading_error_rad": -0.2,
            "cross_track_error_m": 0.0,
        },
    )

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego))

    assert command.steer > 0.0
    assert command.metadata["steer_heading_error_rad"] == -0.2
    assert command.metadata["steer_cross_track_error_m"] == 0.0


def test_builtin_ego_route_lookahead_heading_error_uses_waypoint_ahead() -> None:
    transform = _Transform(x=0.0, y=0.0, yaw=0.0)
    waypoint = _Waypoint(_Transform(x=0.0, y=0.0, yaw=0.0), [_Waypoint(_Transform(x=8.0, y=8.0, yaw=45.0), [])])

    error = _route_lookahead_heading_error(transform, waypoint, lookahead_m=8.0)

    assert error is not None
    assert math.isclose(error, -math.pi / 4.0, rel_tol=1e-6)


def test_builtin_ego_route_plan_metadata_uses_global_route_lookahead() -> None:
    waypoints = [
        _Waypoint(_Transform(x=0.0, y=0.0, yaw=0.0), []),
        _Waypoint(_Transform(x=8.0, y=0.0, yaw=0.0), []),
        _Waypoint(_Transform(x=8.0, y=8.0, yaw=90.0), []),
    ]
    route_plan = _prepare_route_plan(waypoints)

    metadata = _route_plan_metadata(route_plan, _Transform(x=7.8, y=0.0, yaw=0.0))

    assert metadata["route_plan_available"] is True
    assert metadata["route_plan_source"] == "carla_global_route_planner"
    assert metadata["route_lookahead_heading_error_rad"] < 0.0
    assert metadata["lane_id"] is None


def test_builtin_ego_spawn_transform_applies_configured_route_offset() -> None:
    base = _Transform(x=0.0, y=0.0, yaw=0.0)
    shifted = _Transform(x=2.0, y=0.0, yaw=0.0)
    world = _SpawnOffsetWorld(base, shifted)

    transform = _ego_spawn_transform(world, spawn_index=0, spawn_s_offset_m=2.0)

    assert transform.location.x == 2.0
    assert transform.location.y == 0.0
    assert transform.location.z > shifted.location.z
    assert world.map.requested_offset_m == 2.0


def test_builtin_ego_spawn_offset_reader_accepts_nested_spawn_block() -> None:
    storyboard = {
        "roles": {
            "ego": {
                "spawn_ref": "carla_spawn_index_0",
                "spawn": {"s_offset_m": 2.0},
            }
        }
    }

    assert _ego_spawn_s_offset_m(storyboard) == 2.0


def test_builtin_safety_event_tracker_records_collision_and_lane_invasion(tmp_path) -> None:
    world = _SafetyWorld()
    ego = object()

    tracker = _SafetyEventTracker(world=world, ego=ego, artifact_dir=tmp_path)
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
    rows = [json.loads(line) for line in (tmp_path / "safety_event_trace.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["event_type"] for row in rows] == ["collision", "lane_invasion"]
    assert rows[0]["other_actor_id"] == 77
    assert rows[1]["crossed_lane_marking_types"] == ["Solid"]

    assert tracker.destroy() == []
    assert all(sensor.destroyed for sensor in world.sensors)


def _ego(frame: FrameStamp, *, speed_mps: float, metadata: dict | None = None) -> EgoState:
    return EgoState(
        stamp=frame,
        pose=Pose3D(position=Vector3D(), orientation=_yaw_quaternion(0.0)),
        linear_velocity=Vector3D(x=speed_mps),
        chassis=ChassisState(speed_mps=speed_mps),
        metadata=metadata or {},
    )


def _lead(
    obstacle_id: str,
    *,
    x: float,
    speed_mps: float,
    y: float = 0.0,
    role: str = "lead_vehicle",
    metadata: dict | None = None,
) -> ObstacleTruth:
    resolved_metadata = {"role": role}
    if metadata:
        resolved_metadata.update(metadata)
    return ObstacleTruth(
        obstacle_id=obstacle_id,
        obstacle_type="vehicle",
        pose=Pose3D(position=Vector3D(x=x, y=y), orientation=_yaw_quaternion(0.0)),
        linear_velocity=Vector3D(x=speed_mps),
        metadata=resolved_metadata,
    )


def _yaw_quaternion(yaw: float) -> Quaternion:
    return Quaternion(z=math.sin(yaw / 2.0), w=math.cos(yaw / 2.0))


class _Location:
    def __init__(self, *, x: float, y: float, z: float = 0.0) -> None:
        self.x = x
        self.y = y
        self.z = z


class _Rotation:
    def __init__(self, *, yaw: float) -> None:
        self.yaw = yaw


class _Transform:
    def __init__(self, *, x: float, y: float, yaw: float) -> None:
        self.location = _Location(x=x, y=y)
        self.rotation = _Rotation(yaw=yaw)


class _Waypoint:
    def __init__(self, transform: _Transform, next_waypoints: list["_Waypoint"]) -> None:
        self.transform = transform
        self._next_waypoints = next_waypoints

    def next(self, distance_m: float) -> list["_Waypoint"]:
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
