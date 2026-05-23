from __future__ import annotations

import inspect

import pytest

from carla_testbed.contracts import EgoState, FrameStamp, SceneTruth
from carla_testbed.control.base import Controller
from carla_testbed.control.dummy import DummyController


def test_controller_base_does_not_import_carla() -> None:
    source = inspect.getsource(__import__("carla_testbed.control.base", fromlist=[""]))

    assert "import carla" not in source
    assert "rclpy" not in source
    assert "cyber" not in source
    assert "apollo" not in source


def test_contract_controller_protocol_accepts_dummy_controller() -> None:
    controller: Controller = DummyController(throttle=0.2, brake=0.0)
    frame = FrameStamp(frame_id=1, sim_time_s=0.05)
    ego = EgoState(stamp=frame)
    scene = SceneTruth(stamp=frame, ego=ego)

    command = controller.step(frame, ego, scene)

    command.validate()
    assert command.throttle == 0.2
    assert command.brake == 0.0
    assert command.stamp == frame


def test_legacy_adapter_can_be_constructed_without_carla_server() -> None:
    pytest.importorskip("carla")

    from carla_testbed.control import CarlaLegacyControllerAdapter, LegacyControllerConfig

    adapter = CarlaLegacyControllerAdapter(LegacyControllerConfig(lateral_mode="dummy"))

    assert adapter.name
    assert hasattr(adapter, "step")
