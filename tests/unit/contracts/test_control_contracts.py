from __future__ import annotations

import pytest

from carla_testbed.contracts import AppliedControl, ControlCommand, FrameStamp


def test_control_command_validate_accepts_valid_values() -> None:
    cmd = ControlCommand(
        throttle=0.4,
        brake=0.0,
        steer=-0.25,
        source="unit_test",
        stamp=FrameStamp(frame_id=1, sim_time_s=0.05),
        metadata={"debug": "allowed"},
    )

    cmd.validate()
    payload = cmd.to_dict()

    assert payload["throttle"] == 0.4
    assert payload["source"] == "unit_test"
    assert payload["stamp"]["frame_id"] == 1
    assert payload["metadata"] == {"debug": "allowed"}


def test_control_command_validation_rejects_out_of_range_values() -> None:
    with pytest.raises(ValueError, match="throttle"):
        ControlCommand(throttle=1.1).validate()
    with pytest.raises(ValueError, match="brake"):
        ControlCommand(brake=-0.1).validate()
    with pytest.raises(ValueError, match="steer"):
        ControlCommand(steer=1.5).validate()


def test_control_command_clamped_returns_valid_copy() -> None:
    cmd = ControlCommand(throttle=2.0, brake=-1.0, steer=-2.0, source="raw")
    clamped = cmd.clamped()

    clamped.validate()
    assert clamped.throttle == 1.0
    assert clamped.brake == 0.0
    assert clamped.steer == -1.0
    assert clamped.source == "raw"


def test_applied_control_validation_and_serialization() -> None:
    applied = AppliedControl(
        command=ControlCommand(throttle=0.2, brake=0.0, steer=0.1),
        applied_throttle=0.19,
        applied_brake=0.0,
        applied_steer=0.08,
    )

    applied.validate()

    assert applied.to_dict()["applied_steer"] == 0.08
