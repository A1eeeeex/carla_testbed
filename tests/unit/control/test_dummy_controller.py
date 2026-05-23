from __future__ import annotations

import pytest

from carla_testbed.contracts import ControlCommand, EgoState, FrameStamp, SceneTruth
from carla_testbed.control.dummy import DummyController, ManualSequenceController


def _frame_scene() -> tuple[FrameStamp, EgoState, SceneTruth]:
    frame = FrameStamp(frame_id=3, sim_time_s=0.15)
    ego = EgoState(stamp=frame)
    return frame, ego, SceneTruth(stamp=frame, ego=ego)


def test_dummy_controller_defaults_to_safe_hold() -> None:
    frame, ego, scene = _frame_scene()
    controller = DummyController()

    command = controller.step(frame, ego, scene)

    command.validate()
    assert command.throttle == 0.0
    assert command.brake == 1.0
    assert command.steer == 0.0
    assert command.source == "dummy_controller"


def test_dummy_controller_clamps_configured_output() -> None:
    frame, ego, scene = _frame_scene()
    controller = DummyController(throttle=2.0, brake=-1.0, steer=-2.0)

    command = controller.step(frame, ego, scene)

    command.validate()
    assert command.throttle == 1.0
    assert command.brake == 0.0
    assert command.steer == -1.0


def test_manual_sequence_controller_replays_then_holds_final_command() -> None:
    frame, ego, scene = _frame_scene()
    controller = ManualSequenceController(
        [
            ControlCommand(throttle=0.1, brake=0.0, steer=0.0, source="seq0"),
            ControlCommand(throttle=0.0, brake=0.8, steer=0.1, source="seq1"),
        ]
    )

    first = controller.step(frame, ego, scene)
    second = controller.step(frame, ego, scene)
    third = controller.step(frame, ego, scene)

    assert first.source == "seq0"
    assert second.source == "seq1"
    assert third.source == "seq1"
    assert third.brake == 0.8


def test_manual_sequence_controller_requires_commands() -> None:
    with pytest.raises(ValueError, match="at least one"):
        ManualSequenceController([])
