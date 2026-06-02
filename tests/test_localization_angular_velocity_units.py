from __future__ import annotations

import importlib
import math
import sys
import types

import pytest

from tools.apollo10_cyber_bridge.ros_shims import OdometryShim


class _Vec:
    def __init__(self, x: float, y: float, z: float) -> None:
        self.x = x
        self.y = y
        self.z = z


def _install_fake_carla() -> None:
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)


def test_ros2_gt_converts_actor_angular_velocity_deg_per_s_to_odom_rad_per_s() -> None:
    _install_fake_carla()
    module = importlib.import_module("carla_testbed.ros2.gt_publisher")

    wx, wy, wz = module._carla_angular_velocity_deg_to_ros_rad(_Vec(0.0, 0.0, 90.0))

    assert wx == pytest.approx(0.0)
    assert wy == pytest.approx(0.0)
    assert wz == pytest.approx(math.pi / 2.0)


def test_carla_direct_converts_actor_angular_velocity_deg_per_s_to_odom_rad_per_s() -> None:
    _install_fake_carla()
    module = importlib.import_module("tools.apollo10_cyber_bridge.carla_direct_transport")

    wx, wy, wz = module._carla_angular_velocity_deg_to_ros_rad(_Vec(0.0, 0.0, 90.0))

    assert wx == pytest.approx(0.0)
    assert wy == pytest.approx(0.0)
    assert wz == pytest.approx(math.pi / 2.0)


def test_bridge_treats_odom_angular_velocity_as_rad_per_s_without_second_conversion() -> None:
    _install_fake_protobuf()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    odom = OdometryShim()
    odom.twist.twist.angular.z = math.pi / 2.0

    wx, wy, wz = bridge._odom_angular_velocity_rad_per_s(odom)

    assert wx == pytest.approx(0.0)
    assert wy == pytest.approx(0.0)
    assert wz == pytest.approx(math.pi / 2.0)


def test_bridge_localization_time_prefers_odom_sim_time() -> None:
    _install_fake_protobuf()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    odom = OdometryShim()
    odom.header.stamp.sec = 12
    odom.header.stamp.nanosec = 345_000_000

    timestamp_sec, time_base = adapter._localization_time_from_odom(
        odom,
        fallback_wall_time_sec=999.0,
    )

    assert timestamp_sec == pytest.approx(12.345)
    assert time_base == "sim_time"


def test_bridge_localization_time_fallback_is_explicit_when_stamp_missing() -> None:
    _install_fake_protobuf()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    odom = OdometryShim()
    odom.header.stamp = None

    timestamp_sec, time_base = adapter._localization_time_from_odom(
        odom,
        fallback_wall_time_sec=999.0,
    )

    assert timestamp_sec == pytest.approx(999.0)
    assert time_base == "cyber_time_fallback"
