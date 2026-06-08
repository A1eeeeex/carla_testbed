from __future__ import annotations

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
    assert command.metadata["effective_target_speed_mps"] < command.metadata["target_speed_mps"]


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


def _ego(frame: FrameStamp, *, speed_mps: float, metadata: dict | None = None) -> EgoState:
    return EgoState(
        stamp=frame,
        pose=Pose3D(position=Vector3D(), orientation=_yaw_quaternion(0.0)),
        linear_velocity=Vector3D(x=speed_mps),
        chassis=ChassisState(speed_mps=speed_mps),
        metadata=metadata or {},
    )


def _lead(obstacle_id: str, *, x: float, speed_mps: float) -> ObstacleTruth:
    return ObstacleTruth(
        obstacle_id=obstacle_id,
        obstacle_type="vehicle",
        pose=Pose3D(position=Vector3D(x=x), orientation=_yaw_quaternion(0.0)),
        linear_velocity=Vector3D(x=speed_mps),
        metadata={"role": "lead_vehicle"},
    )


def _yaw_quaternion(yaw: float) -> Quaternion:
    return Quaternion(z=math.sin(yaw / 2.0), w=math.cos(yaw / 2.0))
