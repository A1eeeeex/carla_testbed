from __future__ import annotations

import importlib
import math
import sys
import types

import pytest

from carla_testbed.adapters.apollo.frame_transform import quaternion_forward_heading
from carla_testbed.adapters.apollo.messages import (
    build_localization_estimate_dict_from_map_state,
    write_localization_estimate_to_pb,
)


class _Header:
    timestamp_sec: float
    module_name: str
    sequence_num: int
    frame_id: str


class _Vector:
    def __init__(self) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = 0.0


class _Orientation:
    def __init__(self) -> None:
        self.qx = 0.0
        self.qy = 0.0
        self.qz = 0.0
        self.qw = 1.0


class _Pose:
    def __init__(self) -> None:
        self.position = _Vector()
        self.orientation = _Orientation()
        self.heading = 0.0
        self.linear_velocity = _Vector()
        self.linear_velocity_vrf = _Vector()
        self.angular_velocity = _Vector()
        self.angular_velocity_vrf = _Vector()
        self.linear_acceleration = _Vector()
        self.linear_acceleration_vrf = _Vector()


class _Localization:
    def __init__(self) -> None:
        self.header = _Header()
        self.pose = _Pose()
        self.measurement_time = 0.0


def test_canonical_localization_dict_writes_header_frame_and_measurement_time() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=12.5,
        sequence_num=7,
        position={"x": 1.0, "y": 2.0, "z": 0.0},
        heading=0.25,
        linear_velocity={"x": 3.0, "y": 0.0, "z": 0.0},
        angular_velocity={"x": 0.0, "y": 0.0, "z": 0.1},
        module_name="tb_apollo10_gt_bridge",
        frame_id="map",
        heading_source="odom_quaternion_yaw_after_frame_transform",
        vehicle_reference_confidence="verified",
        vehicle_reference_hard_gate_eligible=True,
    )
    loc = _Localization()

    write_localization_estimate_to_pb(loc, payload)

    assert loc.header.timestamp_sec == pytest.approx(12.5)
    assert loc.header.frame_id == "map"
    assert loc.measurement_time == pytest.approx(loc.header.timestamp_sec)
    assert loc.pose.heading == pytest.approx(0.25)


def test_canonical_rfu_quaternion_decodes_to_published_heading() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=1.0,
        sequence_num=1,
        position={"x": 0.0, "y": 0.0, "z": 0.0},
        heading=math.pi / 3.0,
    )
    orientation = payload["pose"]["orientation"]

    decoded = quaternion_forward_heading(orientation)

    assert decoded == pytest.approx(payload["pose"]["heading"], abs=1e-12)
    assert abs(payload["metadata"]["quaternion_heading_diff_rad"]) < 1e-12


def test_heading_source_metadata_is_truthful_for_odom_path() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=1.0,
        sequence_num=1,
        position={"x": 0.0, "y": 0.0, "z": 0.0},
        heading=0.0,
        heading_source="odom_quaternion_yaw_after_frame_transform",
    )

    assert payload["metadata"]["heading_source"] == "odom_quaternion_yaw_after_frame_transform"
    assert payload["metadata"]["orientation_convention"] == "RFU_to_ENU"


def test_assumed_vehicle_reference_is_serialized_as_non_claim_grade() -> None:
    payload = build_localization_estimate_dict_from_map_state(
        timestamp_sec=1.0,
        sequence_num=1,
        position={"x": 0.0, "y": 0.0, "z": 0.0},
        heading=0.0,
        vehicle_reference_confidence="assumed",
        vehicle_reference_hard_gate_eligible=False,
    )

    assert payload["metadata"]["vehicle_reference_confidence"] == "assumed"
    assert payload["metadata"]["vehicle_reference_hard_gate_eligible"] is False


def test_bridge_fill_header_writes_frame_id_when_supported() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.seq = 0
    header = _Header()

    sequence_num = adapter._fill_header(header, 2.0, "tb_apollo10_gt_bridge", frame_id="map")

    assert sequence_num == 1
    assert header.timestamp_sec == pytest.approx(2.0)
    assert header.module_name == "tb_apollo10_gt_bridge"
    assert header.sequence_num == 1
    assert header.frame_id == "map"


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)


def _install_fake_carla() -> None:
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))
