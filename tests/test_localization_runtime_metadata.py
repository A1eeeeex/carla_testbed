from __future__ import annotations

import importlib
import math
import sys
import types

import pytest

from tools.apollo10_cyber_bridge.ros_shims import OdometryShim


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)


def _install_fake_carla() -> None:
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))


def test_runtime_localization_timestamp_prefers_odom_sim_time() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    odom = OdometryShim()
    odom.header.stamp.sec = 42
    odom.header.stamp.nanosec = 500_000_000

    timestamp_sec, time_base = adapter._localization_time_from_odom(
        odom,
        fallback_wall_time_sec=1234.0,
    )

    assert timestamp_sec == pytest.approx(42.5)
    assert time_base == "sim_time"


def test_runtime_rfu_quaternion_decodes_forward_heading() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    qx, qy, qz, qw = bridge._rfu_to_enu_quat_from_heading(0.0)
    assert bridge._decode_rfu_to_enu_heading_from_quat(qx, qy, qz, qw) == pytest.approx(0.0)

    qx, qy, qz, qw = bridge._rfu_to_enu_quat_from_heading(math.pi / 2.0)
    assert bridge._decode_rfu_to_enu_heading_from_quat(qx, qy, qz, qw) == pytest.approx(math.pi / 2.0)


def test_runtime_localization_acceleration_uses_velocity_finite_difference() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(None, 10.0, 1.0, 2.0, 0.0)
    assert (ax, ay, az) == pytest.approx((0.0, 0.0, 0.0))
    assert source == "initial_sample_missing_previous_velocity"

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(
        (10.0, 1.0, 2.0, 0.0),
        10.1,
        1.5,
        2.2,
        0.0,
    )
    assert (ax, ay, az) == pytest.approx((5.0, 2.0, 0.0))
    assert source == "finite_difference"

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(
        (10.1, 1.5, 2.2, 0.0),
        10.1,
        1.5,
        2.2,
        0.0,
    )
    assert (ax, ay, az) == pytest.approx((0.0, 0.0, 0.0))
    assert source == "stale_timestamp_republish"


def test_runtime_localization_acceleration_filter_limits_finite_difference_spikes() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(
        (10.0, 0.0, 0.0, 0.0),
        10.05,
        0.0,
        0.8,
        0.0,
        previous_acceleration=(0.0, 0.0, 0.0),
        smoothing_alpha=0.35,
        max_abs_mps2=4.0,
        max_delta_mps2=1.0,
    )

    assert (ax, ay, az) == pytest.approx((0.0, 1.0, 0.0))
    assert source == "finite_difference_filtered"
