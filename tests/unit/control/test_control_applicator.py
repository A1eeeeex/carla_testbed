from __future__ import annotations

from types import SimpleNamespace

import pytest

from carla_testbed.contracts import ControlCommand, FrameStamp
from carla_testbed.control.applicator import (
    ControlApplicator,
    ControlLimits,
    apply_control_to_vehicle,
    clamp_command,
)


class FakeVehicle:
    def __init__(self, actor_id: int = 42):
        self.id = actor_id
        self.applied = None

    def apply_control(self, control):
        self.applied = control

    def get_control(self):
        return self.applied


class FailingVehicle(FakeVehicle):
    def apply_control(self, control):
        raise RuntimeError("apply failed")


class SnapshotFailingVehicle(FakeVehicle):
    def get_control(self):
        raise RuntimeError("snapshot failed")


def test_clamps_control_and_zeroes_throttle_when_braking():
    cmd = ControlCommand(throttle=2.0, brake=0.2, steer=3.0, reverse=True, hand_brake=True)

    result = apply_control_to_vehicle(FakeVehicle(), cmd, stamp=FrameStamp(frame_id=7, sim_time_s=0.35))

    assert result.applied_ok is True
    assert result.actor_id == 42
    assert result.frame_id == 7
    assert result.sim_time_s == 0.35
    assert result.requested_command["throttle"] == 2.0
    assert result.clamped_command["throttle"] == 0.0
    assert result.clamped_command["brake"] == 0.2
    assert result.clamped_command["steer"] == 1.0
    assert result.clamped_command["reverse"] is True
    assert result.clamped_command["hand_brake"] is True
    assert result.applied_control["throttle"] == 0.0
    assert result.applied_control["brake"] == pytest.approx(0.2)
    assert result.applied_control["steer"] == 1.0


def test_brake_threshold_is_strictly_greater_than_limit():
    cmd_at_threshold = ControlCommand(throttle=0.8, brake=0.05, steer=0.0)
    cmd_over_threshold = ControlCommand(throttle=0.8, brake=0.051, steer=0.0)

    assert clamp_command(cmd_at_threshold)["throttle"] == 0.8
    assert clamp_command(cmd_over_threshold)["throttle"] == 0.0


def test_custom_limits_and_applicator_wrapper():
    vehicle = FakeVehicle(actor_id=101)
    applicator = ControlApplicator(limits=ControlLimits(max_steer=0.5))

    result = applicator.apply(vehicle, ControlCommand(throttle=0.4, brake=0.0, steer=0.8))

    assert result.applied_ok is True
    assert result.clamped_command["steer"] == 0.5
    assert vehicle.applied.steer == 0.5


def test_legacy_like_command_fields_are_preserved():
    legacy_cmd = SimpleNamespace(
        throttle=0.3,
        brake=0.0,
        steer=-0.2,
        reverse=False,
        hand_brake=False,
        manual_gear_shift=True,
        gear=1,
        meta={"name": "legacy_follow_stop", "last_debug": {"gap": 5.0}},
    )

    result = apply_control_to_vehicle(FakeVehicle(), legacy_cmd)

    assert result.applied_ok is True
    assert result.clamped_command["source"] == "legacy_follow_stop"
    assert result.clamped_command["manual_gear_shift"] is True
    assert result.clamped_command["gear"] == 1
    assert result.clamped_command["metadata"]["last_debug"] == {"gap": 5.0}


def test_apply_error_is_reported_without_raising():
    cmd = ControlCommand(throttle=0.2, brake=0.0, steer=0.0)

    result = apply_control_to_vehicle(FailingVehicle(), cmd)

    assert result.applied_ok is False
    assert result.error == "apply failed"
    assert result.metadata["error_type"] == "RuntimeError"
    assert result.applied_control == {}


def test_snapshot_error_does_not_mark_apply_failed():
    cmd = ControlCommand(throttle=0.2, brake=0.0, steer=-0.1)

    result = apply_control_to_vehicle(SnapshotFailingVehicle(), cmd)

    assert result.applied_ok is True
    assert result.error is None
    assert result.applied_control["throttle"] == pytest.approx(0.2)
    assert result.applied_control["steer"] == pytest.approx(-0.1)
    assert "snapshot failed" in result.metadata["snapshot_error"]
