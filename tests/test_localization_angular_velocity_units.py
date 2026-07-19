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


def test_ros2_gt_ego_state_preserves_wheel_angle_units_and_sim_stamp(monkeypatch) -> None:
    _install_fake_carla()
    module = importlib.import_module("carla_testbed.ros2.gt_publisher")
    wheel_location = types.SimpleNamespace(FL_Wheel="fl", FR_Wheel="fr")
    monkeypatch.setattr(module.carla, "VehicleWheelLocation", wheel_location, raising=False)

    vec = lambda x=0.0, y=0.0, z=0.0: types.SimpleNamespace(x=x, y=y, z=z)
    transform = types.SimpleNamespace(
        location=vec(1.0, 2.0, 0.5),
        rotation=types.SimpleNamespace(roll=0.0, pitch=0.0, yaw=15.0),
        get_forward_vector=lambda: vec(1.0, 0.0, 0.0),
        get_right_vector=lambda: vec(0.0, 1.0, 0.0),
    )
    ego = types.SimpleNamespace(
        id=7,
        type_id="vehicle.test",
        attributes={"role_name": "hero"},
        bounding_box=types.SimpleNamespace(
            extent=vec(2.4, 0.9, 0.75),
            location=vec(0.1, -0.05, 0.0),
        ),
        get_control=lambda: types.SimpleNamespace(
            throttle=0.2, brake=0.0, steer=0.9, reverse=False, hand_brake=False, gear=4
        ),
        get_transform=lambda: transform,
        get_velocity=lambda: vec(10.0, 0.0, 0.0),
        get_acceleration=lambda: vec(1.5, 0.25, 0.0),
        get_angular_velocity=lambda: vec(0.0, 0.0, 30.0),
        get_physics_control=lambda: types.SimpleNamespace(
            wheels=[types.SimpleNamespace(max_steer_angle=70.0)],
            mass=1900.0,
            drag_coefficient=0.3,
        ),
        get_wheel_steer_angle=lambda wheel: 7.0 if wheel == "fl" else 5.0,
    )
    publisher = module.GroundTruthRos2Publisher.__new__(module.GroundTruthRos2Publisher)
    publisher._ego_state_actor_id = None
    publisher._ego_max_steer_angle_deg = None
    publisher._ego_vehicle_characteristics = None
    publisher._ego_last_speed_sample = None
    publisher._stats = {}
    publisher.invert_tf = False
    publisher._warn_once = lambda *_args: None

    state = publisher._ego_state_payload(12.5, ego)

    assert state is not None
    assert state["stamp"] == pytest.approx(12.5)
    assert state["steer_feedback_source"] == "wheel_angle"
    assert state["steer_feedback_deg"] == pytest.approx(6.0)
    assert state["steer_feedback_pct"] == pytest.approx(6.0 / 70.0 * 100.0)
    assert state["yaw_rate_rps"] == pytest.approx(math.pi / 6.0)
    assert state["forward_accel_mps2"] == pytest.approx(1.5)
    assert state["lateral_accel_mps2"] == pytest.approx(0.25)
    assert state["vehicle_characteristics"]["length"] == pytest.approx(4.8)
    assert state["vehicle_characteristics"]["width"] == pytest.approx(1.8)
    assert state["vehicle_characteristics"]["front_edge_to_center"] == pytest.approx(2.5)
    assert state["vehicle_characteristics"]["source"] == "runner_tick_aligned_gt"


def test_ros2_gt_publishes_tick_aligned_objects_before_odom() -> None:
    _install_fake_carla()
    module = importlib.import_module("carla_testbed.ros2.gt_publisher")
    publisher = module.GroundTruthRos2Publisher.__new__(module.GroundTruthRos2Publisher)
    calls = []
    publisher.enabled = True
    publisher._stats = {}
    publisher.publish_odom = True
    publisher.publish_tf = False
    publisher._period_odom = 0.05
    publisher._period_tf = 0.05
    publisher._period_objects = 0.05
    publisher._period_markers = 0.05
    publisher._objects3d_pub = None
    publisher._objects_json_pub = object()
    publisher._markers_pub = None
    publisher._should_publish = lambda *_args: True
    publisher._filtered_objects = lambda _ego, actors: list(actors)
    publisher._publish_objects3d = lambda *_args: calls.append("objects3d")
    publisher._publish_objects_json = lambda *_args: calls.append("objects_json")
    publisher._publish_odom = lambda *_args: calls.append("odom")
    publisher._publish_dynamic_tf = lambda *_args: calls.append("tf")
    publisher._warn_once = lambda *_args: None
    publisher._node = object()
    publisher._rclpy = types.SimpleNamespace(spin_once=lambda *_args, **_kwargs: None)
    world = types.SimpleNamespace(
        get_snapshot=lambda: types.SimpleNamespace(
            timestamp=types.SimpleNamespace(elapsed_seconds=12.5)
        )
    )

    publisher.publish_tick(world, types.SimpleNamespace(id=1), [types.SimpleNamespace(id=2)])

    assert calls == ["objects3d", "objects_json", "odom"]


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
