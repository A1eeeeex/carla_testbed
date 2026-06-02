from __future__ import annotations

import math
from pathlib import Path

import pytest

from carla_testbed.adapters.apollo.frame_transform import (
    ApolloFrameTransform,
    Vector3,
    quaternion_forward_heading,
    quaternion_rotate_vector,
)
from carla_testbed.adapters.apollo.localization_gt import (
    CarlaEgoKinematics,
    LocalizationGTPolicy,
    build_localization_estimate_dict,
)
from carla_testbed.adapters.apollo.vehicle_reference import VehicleReferenceConfig

LOCALIZATION_GT_MODULE = Path("carla_testbed/adapters/apollo/localization_gt.py")


def test_timestamp_and_measurement_time_use_sim_time() -> None:
    msg = build_localization_estimate_dict(
        _ego(sim_time_s=12.5, wall_time_s=99.0),
        ApolloFrameTransform(),
        _vehicle_reference(),
        sequence_num=42,
        policy=LocalizationGTPolicy(frame_id="map"),
    )

    assert msg["header"]["timestamp_sec"] == pytest.approx(12.5)
    assert msg["header"]["module_name"] == "carla_testbed_localization_gt"
    assert msg["header"]["sequence_num"] == 42
    assert msg["header"]["frame_id"] == "map"
    assert msg["measurement_time"] == pytest.approx(12.5)
    assert msg["metadata"]["wall_time_s"] == pytest.approx(99.0)


def test_position_uses_vrp_offset_not_actor_origin() -> None:
    msg = build_localization_estimate_dict(
        _ego(actor_location=Vector3(10.0, 20.0, 0.0)),
        ApolloFrameTransform(tx=100.0, ty=200.0, tz=0.0, y_flip=True),
        _vehicle_reference(offset=Vector3(-1.35, 0.0, 0.0)),
        sequence_num=1,
    )

    assert msg["pose"]["position"]["x"] == pytest.approx(108.65)
    assert msg["pose"]["position"]["y"] == pytest.approx(180.0)
    assert msg["metadata"]["vrp_carla"] == {"x": 8.65, "y": 20.0, "z": 0.0}


def test_heading_comes_from_transformed_forward_vector() -> None:
    msg = build_localization_estimate_dict(
        _ego(actor_forward=Vector3(1.0, 0.0, 0.0)),
        ApolloFrameTransform(yaw_rad=math.pi / 2.0, y_flip=True),
        _vehicle_reference(),
        sequence_num=1,
    )

    assert msg["pose"]["heading"] == pytest.approx(math.pi / 2.0)


def test_linear_velocity_is_transformed_to_apollo_map_frame() -> None:
    msg = build_localization_estimate_dict(
        _ego(linear_velocity=Vector3(1.0, 2.0, 3.0)),
        ApolloFrameTransform(tx=100.0, ty=-50.0, tz=8.0, scale=2.0, y_flip=True),
        _vehicle_reference(),
        sequence_num=1,
    )

    assert msg["pose"]["linear_velocity"] == {"x": 2.0, "y": -4.0, "z": 6.0}
    assert msg["pose"]["linear_velocity_vrf"] == {"x": 2.0, "y": 1.0, "z": 3.0}


def test_vrf_axis_order_is_right_forward_up() -> None:
    msg = build_localization_estimate_dict(
        _ego(linear_velocity=Vector3(1.0, 2.0, 3.0)),
        ApolloFrameTransform(),
        _vehicle_reference(),
        sequence_num=1,
    )

    assert msg["pose"]["linear_velocity_vrf"] == {"x": 2.0, "y": 1.0, "z": 3.0}


def test_angular_velocity_deg_per_s_is_converted_to_rad_per_s() -> None:
    msg = build_localization_estimate_dict(
        _ego(angular_velocity=Vector3(0.0, 0.0, 90.0), angular_velocity_unit="deg_per_s"),
        ApolloFrameTransform(y_flip=True),
        _vehicle_reference(),
        sequence_num=1,
    )

    assert msg["pose"]["angular_velocity"]["z"] == pytest.approx(math.pi / 2.0)
    assert msg["pose"]["angular_velocity_vrf"]["z"] == pytest.approx(math.pi / 2.0)
    assert msg["metadata"]["angular_velocity_input_unit"] == "deg_per_s"


def test_unknown_angular_velocity_unit_warns_and_omits_angular_velocity() -> None:
    msg = build_localization_estimate_dict(
        _ego(angular_velocity=Vector3(0.0, 0.0, 1.0), angular_velocity_unit="unknown"),
        ApolloFrameTransform(),
        _vehicle_reference(),
        sequence_num=1,
    )

    assert "angular_velocity" not in msg["pose"]
    assert "angular_velocity_unit_unknown" in msg["metadata"]["warnings"]
    assert "angular_velocity" in msg["metadata"]["missing_fields"]


def test_orientation_quaternion_is_unit_norm() -> None:
    msg = build_localization_estimate_dict(
        _ego(actor_forward=Vector3(0.0, 1.0, 0.0)),
        ApolloFrameTransform(y_flip=True),
        _vehicle_reference(),
        sequence_num=1,
    )
    quat = msg["pose"]["orientation"]
    norm = math.sqrt(sum(float(quat[key]) ** 2 for key in ("x", "y", "z", "w")))

    assert norm == pytest.approx(1.0)


def test_orientation_is_rfu_to_enu_and_matches_heading() -> None:
    msg = build_localization_estimate_dict(
        _ego(actor_forward=Vector3(1.0, 0.0, 0.0), actor_right=Vector3(0.0, 1.0, 0.0)),
        ApolloFrameTransform(y_flip=True),
        _vehicle_reference(),
        sequence_num=1,
    )
    quat = msg["pose"]["orientation"]
    forward_world = quaternion_rotate_vector(quat, Vector3(0.0, 1.0, 0.0))

    assert forward_world.x == pytest.approx(1.0)
    assert forward_world.y == pytest.approx(0.0)
    assert quaternion_forward_heading(quat) == pytest.approx(msg["pose"]["heading"])
    assert msg["metadata"]["orientation_convention"] == "RFU_to_ENU"
    assert msg["metadata"]["quaternion_heading_diff_rad"] == pytest.approx(0.0)


def test_missing_acceleration_gracefully_degrades() -> None:
    msg = build_localization_estimate_dict(
        _ego(linear_acceleration=None),
        ApolloFrameTransform(),
        _vehicle_reference(),
        sequence_num=1,
    )

    assert "linear_acceleration" not in msg["pose"]
    assert "linear_acceleration_vrf" not in msg["pose"]
    assert "linear_acceleration" in msg["metadata"]["missing_fields"]
    assert "linear_acceleration_missing" in msg["metadata"]["warnings"]


def test_policy_mapping_sets_frame_metadata() -> None:
    msg = build_localization_estimate_dict(
        _ego(),
        ApolloFrameTransform(map_name="Town01"),
        _vehicle_reference(confidence="assumed"),
        sequence_num=1,
        policy={
            "frame_id": "apollo_map",
            "time_base": "sim_time",
            "frame_transform_id": "town01_manual_v0",
            "allow_assumed_vehicle_reference": False,
        },
    )

    assert msg["header"]["frame_id"] == "apollo_map"
    assert msg["metadata"]["frame_transform_id"] == "town01_manual_v0"
    assert msg["metadata"]["map_name"] == "Town01"
    assert msg["metadata"]["vehicle_reference_confidence"] == "assumed"
    assert msg["metadata"]["vehicle_reference_hard_gate_eligible"] is False
    assert msg["metadata"]["gt_localization"] is True


def test_localization_gt_module_has_no_runtime_imports() -> None:
    text = LOCALIZATION_GT_MODULE.read_text(encoding="utf-8").lower()

    assert "import carla" not in text
    assert "import cyber" not in text
    assert "pb2" not in text


def _ego(**overrides: object) -> CarlaEgoKinematics:
    payload = {
        "carla_frame_id": 123,
        "sim_time_s": 1.25,
        "wall_time_s": None,
        "actor_location": Vector3(0.0, 0.0, 0.0),
        "actor_forward": Vector3(1.0, 0.0, 0.0),
        "actor_right": Vector3(0.0, 1.0, 0.0),
        "actor_up": Vector3(0.0, 0.0, 1.0),
        "linear_velocity": Vector3(0.0, 0.0, 0.0),
        "linear_acceleration": Vector3(0.0, 0.0, 0.0),
        "angular_velocity": Vector3(0.0, 0.0, 0.0),
        "angular_velocity_unit": "rad_per_s",
    }
    payload.update(overrides)
    return CarlaEgoKinematics(**payload)


def _vehicle_reference(
    *,
    offset: Vector3 = Vector3(0.0, 0.0, 0.0),
    confidence: str = "verified",
) -> VehicleReferenceConfig:
    return VehicleReferenceConfig(
        vehicle_blueprint="vehicle.lincoln.mkz_2020",
        apollo_reference_point="rear_axle_center",
        carla_actor_origin_definition="measured_actor_origin",
        origin_to_vrp_carla=offset,
        source="unit_test",
        confidence=confidence,
    )
