from __future__ import annotations

import math
from pathlib import Path

import pytest

from carla_testbed.adapters.apollo.frame_transform import (
    ApolloFrameTransform,
    Vector3,
    carla_angular_vector_to_apollo,
    carla_forward_to_heading,
    carla_point_to_apollo,
    carla_vector_to_apollo,
    frame_transform_report,
    heading_to_rfu_to_enu_quaternion,
    heading_to_quaternion_enu,
    load_frame_transform,
    normalize_angle,
    quaternion_forward_heading,
    quaternion_rotate_vector,
)

FRAME_TRANSFORM_EXAMPLE = Path("configs/town01/apollo_frame_transform.example.yaml")
FRAME_TRANSFORM_MODULE = Path("carla_testbed/adapters/apollo/frame_transform.py")


def test_load_example_frame_transform() -> None:
    transform = load_frame_transform(FRAME_TRANSFORM_EXAMPLE)

    assert transform.map_name == "Town01"
    assert transform.source_frame == "carla_world"
    assert transform.target_frame == "apollo_map"
    assert transform.y_flip is True


def test_y_flip_true_flips_point_y_direction() -> None:
    transform = ApolloFrameTransform(y_flip=True)

    point = carla_point_to_apollo(Vector3(1.0, 2.0, 3.0), transform)

    assert point == Vector3(1.0, -2.0, 3.0)


def test_point_applies_scale_yaw_and_translation() -> None:
    transform = ApolloFrameTransform(tx=10.0, ty=20.0, tz=30.0, yaw_rad=math.pi / 2.0, scale=2.0)

    point = carla_point_to_apollo(Vector3(1.0, 0.0, 3.0), transform)

    assert point.x == pytest.approx(10.0)
    assert point.y == pytest.approx(22.0)
    assert point.z == pytest.approx(36.0)


def test_vector_is_not_translated() -> None:
    transform = ApolloFrameTransform(tx=100.0, ty=-50.0, tz=7.0, yaw_rad=0.0, scale=3.0, y_flip=True)

    vector = carla_vector_to_apollo(Vector3(1.0, 2.0, 3.0), transform)

    assert vector == Vector3(3.0, -6.0, 9.0)


def test_angular_vector_is_not_scaled() -> None:
    transform = ApolloFrameTransform(tx=100.0, ty=-50.0, tz=7.0, yaw_rad=0.0, scale=3.0, y_flip=True)

    vector = carla_angular_vector_to_apollo(Vector3(1.0, 2.0, 3.0), transform)

    assert vector == Vector3(1.0, -2.0, 3.0)


def test_carla_forward_x_heading_is_stable() -> None:
    transform = ApolloFrameTransform(yaw_rad=0.0, y_flip=True)

    heading = carla_forward_to_heading(Vector3(1.0, 0.0, 0.0), transform)

    assert heading == pytest.approx(0.0)


def test_heading_uses_transformed_forward_vector_not_carla_yaw_shortcut() -> None:
    transform = ApolloFrameTransform(yaw_rad=0.0, y_flip=True)

    right_in_carla = carla_forward_to_heading(Vector3(0.0, 1.0, 0.0), transform)

    assert right_in_carla == pytest.approx(-math.pi / 2.0)


def test_heading_includes_transform_yaw() -> None:
    transform = ApolloFrameTransform(yaw_rad=math.pi / 2.0, y_flip=True)

    heading = carla_forward_to_heading(Vector3(1.0, 0.0, 0.0), transform)

    assert heading == pytest.approx(math.pi / 2.0)


def test_zero_forward_vector_rejected() -> None:
    with pytest.raises(ValueError, match="non-zero horizontal magnitude"):
        carla_forward_to_heading(Vector3(0.0, 0.0, 1.0), ApolloFrameTransform())


def test_normalize_angle_wraparound() -> None:
    assert normalize_angle(3.0 * math.pi) == pytest.approx(math.pi)
    assert normalize_angle(-3.0 * math.pi) == pytest.approx(math.pi)
    assert normalize_angle(2.5 * math.pi) == pytest.approx(math.pi / 2.0)


def test_heading_to_quaternion_norm_is_unit() -> None:
    quat = heading_to_quaternion_enu(math.pi / 3.0, roll_rad=0.1, pitch_rad=-0.2)

    assert quat.norm() == pytest.approx(1.0)


def test_rfu_orientation_heading_zero_rotates_forward_axis_to_enu_east() -> None:
    quat = heading_to_rfu_to_enu_quaternion(0.0)
    forward_world = quaternion_rotate_vector(quat, Vector3(0.0, 1.0, 0.0))

    assert quat.norm() == pytest.approx(1.0)
    identity = heading_to_quaternion_enu(0.0)
    assert (
        abs(quat.x - identity.x)
        + abs(quat.y - identity.y)
        + abs(quat.z - identity.z)
        + abs(quat.w - identity.w)
    ) > 1e-6
    assert forward_world.x == pytest.approx(1.0)
    assert forward_world.y == pytest.approx(0.0)
    assert forward_world.z == pytest.approx(0.0)
    assert quaternion_forward_heading(quat) == pytest.approx(0.0)


def test_rfu_orientation_heading_north_rotates_forward_axis_to_enu_north() -> None:
    quat = heading_to_rfu_to_enu_quaternion(math.pi / 2.0)
    forward_world = quaternion_rotate_vector(quat, Vector3(0.0, 1.0, 0.0))

    assert quat.norm() == pytest.approx(1.0)
    assert forward_world.x == pytest.approx(0.0, abs=1e-12)
    assert forward_world.y == pytest.approx(1.0)
    assert forward_world.z == pytest.approx(0.0)
    assert quaternion_forward_heading(quat) == pytest.approx(math.pi / 2.0)


def test_frame_transform_report_documents_heading_source() -> None:
    report = frame_transform_report(ApolloFrameTransform(map_name="Town01"))

    assert report["schema_version"] == "apollo_frame_transform_report.v1"
    assert report["convention"]["heading_source"] == "transformed_forward_vector"


def test_frame_transform_module_has_no_runtime_imports() -> None:
    text = FRAME_TRANSFORM_MODULE.read_text(encoding="utf-8").lower()

    assert "import carla" not in text
    assert "import cyber" not in text
    assert "pb2" not in text
