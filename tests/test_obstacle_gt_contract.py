from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

from carla_testbed.analysis.obstacle_gt_contract import (
    OBSTACLE_GT_CONTRACT_SCHEMA_VERSION,
    analyze_obstacle_gt_contract_records,
)


def _load_bridge_module_with_runtime_shims():
    bridge_path = Path.cwd() / "tools" / "apollo10_cyber_bridge" / "bridge.py"
    module_name = "bridge_obstacle_contract_unit_test"

    fake_rclpy = types.ModuleType("rclpy")
    fake_google_mod = types.ModuleType("google")
    fake_google_protobuf_mod = types.ModuleType("google.protobuf")
    fake_empty_pb2_mod = types.ModuleType("google.protobuf.empty_pb2")
    fake_google_protobuf_mod.empty_pb2 = fake_empty_pb2_mod
    fake_node_mod = types.ModuleType("rclpy.node")
    fake_exec_mod = types.ModuleType("rclpy.executors")
    fake_qos_mod = types.ModuleType("rclpy.qos")
    fake_nav_msg_mod = types.ModuleType("nav_msgs.msg")
    fake_std_msg_mod = types.ModuleType("std_msgs.msg")
    fake_ackermann_msg_mod = types.ModuleType("ackermann_msgs.msg")
    fake_geom_msg_mod = types.ModuleType("geometry_msgs.msg")
    fake_vision_msg_mod = types.ModuleType("vision_msgs.msg")
    fake_viz_msg_mod = types.ModuleType("visualization_msgs.msg")
    fake_carla_mod = types.ModuleType("carla")

    fake_node_mod.Node = object
    fake_exec_mod.MultiThreadedExecutor = object
    fake_qos_mod.QoSHistoryPolicy = object
    fake_qos_mod.QoSProfile = object
    fake_qos_mod.QoSReliabilityPolicy = object
    fake_qos_mod.qos_profile_sensor_data = object()
    fake_nav_msg_mod.Odometry = object
    fake_std_msg_mod.String = object
    fake_std_msg_mod.Float32MultiArray = object
    fake_ackermann_msg_mod.AckermannDriveStamped = object
    fake_geom_msg_mod.Twist = object
    fake_vision_msg_mod.Detection3DArray = object
    fake_viz_msg_mod.MarkerArray = object
    fake_carla_mod.Client = object
    fake_carla_mod.VehicleControl = object
    fake_carla_mod.World = object
    fake_carla_mod.Vehicle = object
    fake_carla_mod.Actor = object
    fake_carla_mod.Location = object
    fake_carla_mod.Rotation = object
    fake_carla_mod.Transform = object

    spec = importlib.util.spec_from_file_location(module_name, bridge_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "rclpy": fake_rclpy,
            "google": fake_google_mod,
            "google.protobuf": fake_google_protobuf_mod,
            "google.protobuf.empty_pb2": fake_empty_pb2_mod,
            "rclpy.node": fake_node_mod,
            "rclpy.executors": fake_exec_mod,
            "rclpy.qos": fake_qos_mod,
            "nav_msgs.msg": fake_nav_msg_mod,
            "std_msgs.msg": fake_std_msg_mod,
            "ackermann_msgs.msg": fake_ackermann_msg_mod,
            "geometry_msgs.msg": fake_geom_msg_mod,
            "vision_msgs.msg": fake_vision_msg_mod,
            "visualization_msgs.msg": fake_viz_msg_mod,
            "carla": fake_carla_mod,
            module_name: module,
        },
    ):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


def _obstacle(**updates):
    payload = {
        "timestamp": 1.0,
        "ego_actor_id": "ego",
        "carla_actor_id": "front_1",
        "apollo_perception_id": "front_1",
        "is_ego": False,
        "frame_transform_checked": True,
        "theta_frame_checked": True,
        "position_frame_apollo_map": True,
        "length": 4.5,
        "width": 1.8,
        "height": 1.5,
        "velocity_source": "carla_actor_state",
        "velocity": {"x": 5.0, "y": 0.0, "z": 0.0},
        "dynamic": True,
        "tracking_time": 0.1,
    }
    payload.update(updates)
    return payload


def test_obstacle_gt_contract_valid_dynamic_actor_passes() -> None:
    report = analyze_obstacle_gt_contract_records(
        [_obstacle()],
        scenario_class="follow_stop",
    )

    assert report["schema_version"] == OBSTACLE_GT_CONTRACT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["object_count"] == 1


def test_ego_included_as_obstacle_fails() -> None:
    report = analyze_obstacle_gt_contract_records(
        [_obstacle(carla_actor_id="ego", apollo_perception_id="ego", is_ego=True)],
        scenario_class="follow_stop",
    )

    assert report["status"] == "fail"
    assert "ego_actor_included_as_obstacle" in report["errors"]


def test_dynamic_actor_velocity_zero_filled_fails_for_dynamic_scenario() -> None:
    report = analyze_obstacle_gt_contract_records(
        [
            _obstacle(
                velocity_source="Detection3DArray_missing_velocity",
                velocity={"x": 0.0, "y": 0.0, "z": 0.0},
                dynamic=True,
                actually_stationary=False,
            )
        ],
        scenario_class="follow_stop",
    )

    assert report["status"] == "fail"
    assert "velocity_source_missing_or_zero_filled" in report["warnings"]
    assert "dynamic_actor_velocity_zero_filled" in report["errors"]


def test_detection3d_velocity_missing_warns_when_dynamic_behavior_not_claimed() -> None:
    report = analyze_obstacle_gt_contract_records(
        [
            _obstacle(
                velocity_source="Detection3DArray_missing_velocity",
                velocity={"x": 0.0, "y": 0.0, "z": 0.0},
                dynamic=True,
            )
        ],
        scenario_class="lane_keep",
    )

    assert report["status"] == "warn"
    assert "velocity_source_missing_or_zero_filled" in report["warnings"]


def test_tracking_time_non_monotonic_fails() -> None:
    report = analyze_obstacle_gt_contract_records(
        [
            _obstacle(timestamp=1.0, tracking_time=1.0),
            _obstacle(timestamp=2.0, tracking_time=0.5),
        ],
        scenario_class="follow_stop",
    )

    assert report["status"] == "fail"
    assert "tracking_time_non_monotonic" in report["errors"]


def test_bridge_obstacle_contract_ego_id_is_safe_without_direct_vehicle() -> None:
    bridge = _load_bridge_module_with_runtime_shims()
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)

    assert adapter._ego_actor_id_for_obstacle_contract() is None

    class FakeVehicle:
        id = 42

    adapter.vehicle = FakeVehicle()
    assert adapter._ego_actor_id_for_obstacle_contract() == 42


def test_bridge_obstacle_contract_does_not_reference_undefined_front_speed_symbol() -> None:
    bridge_source = (Path.cwd() / "tools" / "apollo10_cyber_bridge" / "bridge.py").read_text(
        encoding="utf-8"
    )

    assert "_finite_or_none(front_obstacle_actor_speed_mps)" not in bridge_source
    assert "bool(front_obstacle_actor_speed_mps" not in bridge_source
