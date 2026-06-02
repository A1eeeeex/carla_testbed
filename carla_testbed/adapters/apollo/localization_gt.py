from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

from .frame_transform import (
    ApolloFrameTransform,
    Vector3,
    carla_angular_vector_to_apollo,
    carla_forward_to_heading,
    carla_vector_to_apollo,
    normalize_angle,
    quaternion_forward_heading,
    rfu_basis_to_enu_quaternion,
)
from .vehicle_reference import VehicleReferenceConfig, carla_vrp_to_apollo_position

LOCALIZATION_GT_SCHEMA_VERSION = "apollo_localization_gt.v1"
MODULE_NAME = "carla_testbed_localization_gt"
ANGULAR_VELOCITY_UNITS = {"deg_per_s", "rad_per_s", "unknown"}


@dataclass(frozen=True)
class CarlaEgoKinematics:
    carla_frame_id: int | str
    sim_time_s: float
    actor_location: Any
    actor_forward: Any
    actor_right: Any
    actor_up: Any
    linear_velocity: Any
    wall_time_s: float | None = None
    linear_acceleration: Any | None = None
    angular_velocity: Any | None = None
    angular_velocity_unit: str = "unknown"


@dataclass(frozen=True)
class LocalizationGTPolicy:
    frame_id: str = "map"
    time_base: str = "sim_time"
    module_name: str = MODULE_NAME
    frame_transform_id: str | None = None
    allow_assumed_vehicle_reference: bool = False

    @classmethod
    def from_any(cls, value: Any | None) -> "LocalizationGTPolicy":
        if value is None:
            return cls()
        if isinstance(value, LocalizationGTPolicy):
            return value
        if isinstance(value, Mapping):
            return cls(
                frame_id=str(value.get("frame_id") or "map"),
                time_base=str(value.get("time_base") or "sim_time"),
                module_name=str(value.get("module_name") or MODULE_NAME),
                frame_transform_id=_optional_str(value.get("frame_transform_id")),
                allow_assumed_vehicle_reference=bool(value.get("allow_assumed_vehicle_reference", False)),
            )
        raise TypeError("policy must be a LocalizationGTPolicy, mapping, or None")


def build_localization_estimate_dict(
    ego: CarlaEgoKinematics,
    frame_transform: ApolloFrameTransform,
    vehicle_reference: VehicleReferenceConfig,
    sequence_num: int,
    policy: LocalizationGTPolicy | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    policy = LocalizationGTPolicy.from_any(policy)
    warnings: list[str] = []
    missing_fields: list[str] = []

    _validate_ego(ego)
    basis = _basis_vectors(ego)
    vrp = carla_vrp_to_apollo_position(
        ego.actor_location,
        basis,
        vehicle_reference,
        frame_transform,
        allow_assumed=policy.allow_assumed_vehicle_reference,
    )
    heading = carla_forward_to_heading(ego.actor_forward, frame_transform)
    right_apollo = carla_vector_to_apollo(ego.actor_right, frame_transform, apply_scale=False)
    forward_apollo = carla_vector_to_apollo(ego.actor_forward, frame_transform, apply_scale=False)
    up_apollo = carla_vector_to_apollo(ego.actor_up, frame_transform, apply_scale=False)
    orientation = rfu_basis_to_enu_quaternion(right_apollo, forward_apollo, up_apollo)
    quaternion_heading = quaternion_forward_heading(orientation)
    quaternion_heading_diff = normalize_angle(quaternion_heading - heading)
    linear_velocity_map = carla_vector_to_apollo(ego.linear_velocity, frame_transform)

    pose: dict[str, Any] = {
        "position": vrp["vrp_apollo"],
        "orientation": orientation.to_dict(),
        "heading": heading,
        "linear_velocity": linear_velocity_map.to_dict(),
        "linear_velocity_vrf": _carla_vector_to_vrf(ego.linear_velocity, basis).to_dict(),
    }

    if ego.linear_acceleration is None:
        missing_fields.append("linear_acceleration")
        warnings.append("linear_acceleration_missing")
    else:
        linear_accel_map = carla_vector_to_apollo(ego.linear_acceleration, frame_transform)
        pose["linear_acceleration"] = linear_accel_map.to_dict()
        pose["linear_acceleration_vrf"] = _carla_vector_to_vrf(ego.linear_acceleration, basis).to_dict()

    angular_velocity = _angular_velocity_rad_per_s(ego, warnings, missing_fields)
    if angular_velocity is not None:
        pose["angular_velocity"] = carla_angular_vector_to_apollo(angular_velocity, frame_transform).to_dict()
        pose["angular_velocity_vrf"] = _carla_vector_to_vrf(angular_velocity, basis).to_dict()

    if vehicle_reference.confidence == "assumed":
        warnings.append("vehicle_reference_confidence_assumed")

    return {
        "schema_version": LOCALIZATION_GT_SCHEMA_VERSION,
        "header": {
            "timestamp_sec": float(ego.sim_time_s),
            "module_name": policy.module_name,
            "sequence_num": int(sequence_num),
            "frame_id": policy.frame_id,
        },
        "measurement_time": float(ego.sim_time_s),
        "pose": pose,
        "metadata": {
            "time_base": policy.time_base,
            "carla_frame_id": ego.carla_frame_id,
            "wall_time_s": ego.wall_time_s,
            "position_reference": vehicle_reference.apollo_reference_point,
            "frame_transform_id": policy.frame_transform_id,
            "map_name": frame_transform.map_name,
            "vehicle_reference_confidence": vehicle_reference.confidence,
            "vehicle_reference_hard_gate_eligible": vrp["hard_gate_eligible"],
            "angular_velocity_input_unit": ego.angular_velocity_unit,
            "angular_velocity_output_unit": "rad_per_s" if angular_velocity is not None else None,
            "orientation_convention": "RFU_to_ENU",
            "orientation_quaternion": orientation.to_dict(),
            "heading_from_forward_vector": heading,
            "quaternion_heading": quaternion_heading,
            "quaternion_heading_diff_rad": quaternion_heading_diff,
            "gt_localization": True,
            "warnings": warnings,
            "missing_fields": missing_fields,
            "source_frames": {
                "carla": frame_transform.source_frame,
                "apollo": frame_transform.target_frame,
            },
            "vrp_carla": vrp["vrp_carla"],
        },
    }


def _validate_ego(ego: CarlaEgoKinematics) -> None:
    if ego.angular_velocity_unit not in ANGULAR_VELOCITY_UNITS:
        raise ValueError(
            f"angular_velocity_unit must be one of {sorted(ANGULAR_VELOCITY_UNITS)}, "
            f"got {ego.angular_velocity_unit!r}"
        )
    if ego.sim_time_s is None:
        raise ValueError("sim_time_s is required")


def _basis_vectors(ego: CarlaEgoKinematics) -> dict[str, Vector3]:
    return {
        "forward": Vector3.from_any(ego.actor_forward),
        "right": Vector3.from_any(ego.actor_right),
        "up": Vector3.from_any(ego.actor_up),
    }


def _angular_velocity_rad_per_s(
    ego: CarlaEgoKinematics,
    warnings: list[str],
    missing_fields: list[str],
) -> Vector3 | None:
    if ego.angular_velocity is None:
        missing_fields.append("angular_velocity")
        warnings.append("angular_velocity_missing")
        return None
    if ego.angular_velocity_unit == "unknown":
        missing_fields.append("angular_velocity")
        warnings.append("angular_velocity_unit_unknown")
        return None

    angular_velocity = Vector3.from_any(ego.angular_velocity)
    if ego.angular_velocity_unit == "deg_per_s":
        return Vector3(
            x=math.radians(angular_velocity.x),
            y=math.radians(angular_velocity.y),
            z=math.radians(angular_velocity.z),
        )
    return angular_velocity


def _carla_vector_to_vrf(vector: Any, basis: Mapping[str, Vector3]) -> Vector3:
    value = Vector3.from_any(vector)
    forward = _normalize(basis["forward"], "actor_forward")
    right = _normalize(basis["right"], "actor_right")
    up = _normalize(basis["up"], "actor_up")
    return Vector3(
        x=_dot(value, right),
        y=_dot(value, forward),
        z=_dot(value, up),
    )


def _normalize(value: Vector3, name: str) -> Vector3:
    norm = math.sqrt(value.x * value.x + value.y * value.y + value.z * value.z)
    if norm <= 1e-12:
        raise ValueError(f"{name} must have non-zero magnitude")
    return Vector3(x=value.x / norm, y=value.y / norm, z=value.z / norm)


def _dot(lhs: Vector3, rhs: Vector3) -> float:
    return lhs.x * rhs.x + lhs.y * rhs.y + lhs.z * rhs.z


def _optional_str(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)
