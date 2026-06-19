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


def test_builtin_controller_uses_explicit_target_route_progress_gap() -> None:
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


def test_builtin_controller_uses_explicit_target_distance_fallback_only_ahead() -> None:
    controller = SimpleAccRouteFollowerController(
        SimpleAccRouteFollowerConfig(target_speed_mps=19.44, target_gap_m=20.0, time_headway_s=0.0)
    )
    frame = FrameStamp(frame_id=5, sim_time_s=0.25)
    ego = _ego(frame, speed_mps=16.0)
    lateral_lead = _lead("lead", x=2.0, y=10.0, speed_mps=4.0)
    behind_target = _lead("behind", x=-2.0, y=1.0, speed_mps=4.0)

    command = controller.step(
        frame,
        ego,
        SceneTruth(stamp=frame, ego=ego, obstacles=(behind_target, lateral_lead)),
    )

    assert command.throttle == 0.0
    assert command.brake > 0.0
    assert command.metadata["lead_obstacle_id"] == "lead"
    assert command.metadata["lead_selection_mode"] == "explicit_target_role_distance_fallback"
    assert math.isclose(command.metadata["lead_gap_m"], math.hypot(2.0, 10.0))


def test_builtin_controller_ignores_unlabeled_lateral_obstacle() -> None:
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=19.44))
    frame = FrameStamp(frame_id=6, sim_time_s=0.3)
    ego = _ego(frame, speed_mps=5.0)
    obstacle = _lead("other", x=10.0, y=20.0, speed_mps=4.0, role="background_vehicle")

    command = controller.step(frame, ego, SceneTruth(stamp=frame, ego=ego, obstacles=(obstacle,)))

    assert command.metadata["lead_obstacle_id"] is None
    assert command.metadata["lead_selection_mode"] is None
    assert command.throttle > 0.0
    assert command.brake == 0.0


def test_builtin_controller_prefers_lookahead_heading_error_for_steering() -> None:
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


def _ego(frame: FrameStamp, *, speed_mps: float, metadata: dict | None = None) -> EgoState:
    return EgoState(
        stamp=frame,
        pose=Pose3D(position=Vector3D(), orientation=_yaw_quaternion(0.0)),
        linear_velocity=Vector3D(x=speed_mps),
        chassis=ChassisState(speed_mps=speed_mps),
        metadata=metadata or {"heading_error_rad": 0.0, "cross_track_error_m": 0.0},
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
