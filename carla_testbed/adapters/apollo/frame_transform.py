from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(frozen=True)
class Vector3:
    x: float
    y: float
    z: float

    @classmethod
    def from_any(cls, value: Any) -> "Vector3":
        if isinstance(value, Vector3):
            return value
        if isinstance(value, Mapping):
            return cls(x=float(value["x"]), y=float(value["y"]), z=float(value["z"]))
        return cls(x=float(getattr(value, "x")), y=float(getattr(value, "y")), z=float(getattr(value, "z")))

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


@dataclass(frozen=True)
class Quaternion:
    x: float
    y: float
    z: float
    w: float

    def norm(self) -> float:
        return math.sqrt(self.x * self.x + self.y * self.y + self.z * self.z + self.w * self.w)

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


@dataclass(frozen=True)
class ApolloFrameTransform:
    tx: float = 0.0
    ty: float = 0.0
    tz: float = 0.0
    yaw_rad: float = 0.0
    scale: float = 1.0
    y_flip: bool = True
    map_name: str | None = None
    source_frame: str = "carla_world"
    target_frame: str = "apollo_map"

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ApolloFrameTransform":
        transform = payload.get("transform") if "transform" in payload else payload
        if not isinstance(transform, Mapping):
            raise ValueError("transform must be a mapping")
        return cls(
            tx=float(transform.get("tx", 0.0)),
            ty=float(transform.get("ty", 0.0)),
            tz=float(transform.get("tz", 0.0)),
            yaw_rad=float(transform.get("yaw_rad", 0.0)),
            scale=float(transform.get("scale", 1.0)),
            y_flip=bool(transform.get("y_flip", True)),
            map_name=_optional_str(payload.get("map_name")),
            source_frame=str(payload.get("source_frame") or "carla_world"),
            target_frame=str(payload.get("target_frame") or "apollo_map"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_frame_transform(path: str | Path) -> ApolloFrameTransform:
    payload = yaml.safe_load(Path(path).expanduser().read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"{path} must contain a YAML mapping")
    return ApolloFrameTransform.from_mapping(payload)


def carla_point_to_apollo(point: Any, transform: ApolloFrameTransform) -> Vector3:
    mapped = _map_carla_axes(Vector3.from_any(point), transform)
    rotated = _rotate_xy(mapped, transform.yaw_rad)
    return Vector3(
        x=rotated.x * transform.scale + transform.tx,
        y=rotated.y * transform.scale + transform.ty,
        z=rotated.z * transform.scale + transform.tz,
    )


def carla_vector_to_apollo(
    vector: Any,
    transform: ApolloFrameTransform,
    *,
    apply_scale: bool = True,
) -> Vector3:
    mapped = _map_carla_axes(Vector3.from_any(vector), transform)
    rotated = _rotate_xy(mapped, transform.yaw_rad)
    scale = transform.scale if apply_scale else 1.0
    return Vector3(
        x=rotated.x * scale,
        y=rotated.y * scale,
        z=rotated.z * scale,
    )


def carla_angular_vector_to_apollo(vector: Any, transform: ApolloFrameTransform) -> Vector3:
    """Convert an angular vector to Apollo map axes without applying distance scale."""

    return carla_vector_to_apollo(vector, transform, apply_scale=False)


def carla_forward_to_heading(forward_vector_carla: Any, transform: ApolloFrameTransform) -> float:
    forward = carla_vector_to_apollo(forward_vector_carla, transform)
    if abs(forward.x) < 1e-12 and abs(forward.y) < 1e-12:
        raise ValueError("forward_vector_carla must have non-zero horizontal magnitude after transform")
    return normalize_angle(math.atan2(forward.y, forward.x))


def heading_to_quaternion_enu(
    heading_rad: float,
    roll_rad: float = 0.0,
    pitch_rad: float = 0.0,
) -> Quaternion:
    """Convert ENU roll/pitch/heading into a unit quaternion in x/y/z/w order."""

    cy = math.cos(float(heading_rad) * 0.5)
    sy = math.sin(float(heading_rad) * 0.5)
    cp = math.cos(float(pitch_rad) * 0.5)
    sp = math.sin(float(pitch_rad) * 0.5)
    cr = math.cos(float(roll_rad) * 0.5)
    sr = math.sin(float(roll_rad) * 0.5)

    q = Quaternion(
        x=sr * cp * cy - cr * sp * sy,
        y=cr * sp * cy + sr * cp * sy,
        z=cr * cp * sy - sr * sp * cy,
        w=cr * cp * cy + sr * sp * sy,
    )
    norm = q.norm()
    if norm == 0.0:
        raise ValueError("quaternion norm is zero")
    return Quaternion(x=q.x / norm, y=q.y / norm, z=q.z / norm, w=q.w / norm)


def heading_to_rfu_to_enu_quaternion(heading_rad: float) -> Quaternion:
    """Return Apollo Pose.orientation for a planar RFU vehicle frame.

    Apollo pose orientation is the rotation from vehicle RFU
    (Right/Forward/Up) to world ENU. This differs from an ordinary ENU yaw
    quaternion whose local X axis points forward.
    """

    heading = float(heading_rad)
    right = Vector3(math.sin(heading), -math.cos(heading), 0.0)
    forward = Vector3(math.cos(heading), math.sin(heading), 0.0)
    up = Vector3(0.0, 0.0, 1.0)
    return rfu_basis_to_enu_quaternion(right, forward, up)


def rfu_basis_to_enu_quaternion(
    right_apollo: Any,
    forward_apollo: Any,
    up_apollo: Any,
) -> Quaternion:
    """Build a unit quaternion from RFU basis vectors expressed in ENU.

    The rotation matrix columns are the ENU images of local RFU axes:
    local X=Right, local Y=Forward, local Z=Up.
    """

    right = _normalize_vector(Vector3.from_any(right_apollo), "right_apollo")
    forward = _normalize_vector(Vector3.from_any(forward_apollo), "forward_apollo")
    up = _normalize_vector(Vector3.from_any(up_apollo), "up_apollo")
    return _quaternion_from_rotation_matrix(
        (
            (right.x, forward.x, up.x),
            (right.y, forward.y, up.y),
            (right.z, forward.z, up.z),
        )
    )


def quaternion_rotate_vector(quaternion: Any, vector: Any) -> Vector3:
    q = quaternion if isinstance(quaternion, Quaternion) else Quaternion(**dict(quaternion))
    v = Vector3.from_any(vector)
    # Unit quaternion vector rotation: v + 2*w*(q_vec x v) + 2*(q_vec x (q_vec x v)).
    qv = Vector3(q.x, q.y, q.z)
    t = _cross(qv, v)
    t = Vector3(2.0 * t.x, 2.0 * t.y, 2.0 * t.z)
    u = _cross(qv, t)
    return Vector3(
        x=v.x + q.w * t.x + u.x,
        y=v.y + q.w * t.y + u.y,
        z=v.z + q.w * t.z + u.z,
    )


def quaternion_forward_heading(quaternion: Any) -> float:
    forward = quaternion_rotate_vector(quaternion, Vector3(0.0, 1.0, 0.0))
    if abs(forward.x) < 1e-12 and abs(forward.y) < 1e-12:
        raise ValueError("quaternion forward axis has zero horizontal magnitude")
    return normalize_angle(math.atan2(forward.y, forward.x))


def normalize_angle(angle_rad: float) -> float:
    wrapped = (float(angle_rad) + math.pi) % (2.0 * math.pi) - math.pi
    if wrapped <= -math.pi:
        return math.pi
    return wrapped


def frame_transform_report(transform: ApolloFrameTransform) -> dict[str, Any]:
    return {
        "schema_version": "apollo_frame_transform_report.v1",
        "source_frame": transform.source_frame,
        "target_frame": transform.target_frame,
        "map_name": transform.map_name,
        "transform": transform.to_dict(),
        "convention": {
            "carla": "UE_left_handed_X_forward_Y_right_Z_up",
            "apollo": "ENU_East_North_Up",
            "heading_source": "transformed_forward_vector",
        },
    }


def _map_carla_axes(value: Vector3, transform: ApolloFrameTransform) -> Vector3:
    y = -value.y if transform.y_flip else value.y
    return Vector3(x=value.x, y=y, z=value.z)


def _rotate_xy(value: Vector3, yaw_rad: float) -> Vector3:
    c = math.cos(float(yaw_rad))
    s = math.sin(float(yaw_rad))
    return Vector3(
        x=value.x * c - value.y * s,
        y=value.x * s + value.y * c,
        z=value.z,
    )


def _normalize_vector(value: Vector3, name: str) -> Vector3:
    norm = math.sqrt(value.x * value.x + value.y * value.y + value.z * value.z)
    if norm <= 1e-12:
        raise ValueError(f"{name} must have non-zero magnitude")
    return Vector3(x=value.x / norm, y=value.y / norm, z=value.z / norm)


def _cross(lhs: Vector3, rhs: Vector3) -> Vector3:
    return Vector3(
        x=lhs.y * rhs.z - lhs.z * rhs.y,
        y=lhs.z * rhs.x - lhs.x * rhs.z,
        z=lhs.x * rhs.y - lhs.y * rhs.x,
    )


def _quaternion_from_rotation_matrix(matrix: tuple[tuple[float, float, float], ...]) -> Quaternion:
    m00, m01, m02 = matrix[0]
    m10, m11, m12 = matrix[1]
    m20, m21, m22 = matrix[2]
    trace = m00 + m11 + m22
    if trace > 0.0:
        s = math.sqrt(trace + 1.0) * 2.0
        w = 0.25 * s
        x = (m21 - m12) / s
        y = (m02 - m20) / s
        z = (m10 - m01) / s
    elif m00 > m11 and m00 > m22:
        s = math.sqrt(1.0 + m00 - m11 - m22) * 2.0
        w = (m21 - m12) / s
        x = 0.25 * s
        y = (m01 + m10) / s
        z = (m02 + m20) / s
    elif m11 > m22:
        s = math.sqrt(1.0 + m11 - m00 - m22) * 2.0
        w = (m02 - m20) / s
        x = (m01 + m10) / s
        y = 0.25 * s
        z = (m12 + m21) / s
    else:
        s = math.sqrt(1.0 + m22 - m00 - m11) * 2.0
        w = (m10 - m01) / s
        x = (m02 + m20) / s
        y = (m12 + m21) / s
        z = 0.25 * s
    q = Quaternion(x=x, y=y, z=z, w=w)
    norm = q.norm()
    if norm == 0.0:
        raise ValueError("quaternion norm is zero")
    return Quaternion(x=q.x / norm, y=q.y / norm, z=q.z / norm, w=q.w / norm)


def _optional_str(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)
