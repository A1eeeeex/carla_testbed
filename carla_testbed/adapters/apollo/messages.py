from __future__ import annotations

from typing import Any, Mapping

from .frame_transform import (
    Vector3,
    heading_to_rfu_to_enu_quaternion,
    normalize_angle,
    quaternion_forward_heading,
)

DEFAULT_LOCALIZATION_MODULE = "tb_apollo10_gt_bridge"
DEFAULT_LOCALIZATION_FRAME_ID = "map"
ORIENTATION_CONVENTION = "RFU_to_ENU"


def fill_header(
    header: Any,
    *,
    timestamp_sec: float,
    module_name: str,
    sequence_num: int | None = None,
    frame_id: str = DEFAULT_LOCALIZATION_FRAME_ID,
) -> None:
    """Fill an Apollo-like header object without depending on Apollo pb2."""

    if header is None:
        return
    _safe_set(header, "timestamp_sec", float(timestamp_sec))
    _safe_set(header, "module_name", module_name)
    if sequence_num is not None:
        _safe_set(header, "sequence_num", int(sequence_num))
    _safe_set(header, "frame_id", frame_id)


def build_localization_estimate_dict_from_map_state(
    *,
    timestamp_sec: float,
    sequence_num: int | None,
    position: Any,
    heading: float,
    linear_velocity: Any | None = None,
    linear_velocity_vrf: Any | None = None,
    angular_velocity: Any | None = None,
    angular_velocity_vrf: Any | None = None,
    linear_acceleration: Any | None = None,
    linear_acceleration_vrf: Any | None = None,
    module_name: str = DEFAULT_LOCALIZATION_MODULE,
    frame_id: str = DEFAULT_LOCALIZATION_FRAME_ID,
    time_base: str = "sim_time",
    heading_source: str = "odom_quaternion_yaw_after_frame_transform",
    position_reference: str = "rear_axle_center",
    vehicle_reference_confidence: str | None = None,
    vehicle_reference_hard_gate_eligible: bool | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical LocalizationEstimate-like dict from Apollo-frame state.

    This is the shared semantic boundary between online bridge code and offline
    analyzers. Runtime code may still source heading from odometry yaw, but the
    published pose is kept internally consistent: RFU-to-ENU quaternion decodes
    back to ``pose.heading``.
    """

    heading_value = normalize_angle(float(heading))
    orientation = heading_to_rfu_to_enu_quaternion(heading_value)
    quaternion_heading = quaternion_forward_heading(orientation)
    quaternion_heading_diff = normalize_angle(quaternion_heading - heading_value)
    extra_metadata = dict(metadata or {})
    extra_metadata.update(
        {
            "time_base": time_base,
            "position_reference": position_reference,
            "vehicle_reference_confidence": vehicle_reference_confidence,
            "vehicle_reference_hard_gate_eligible": vehicle_reference_hard_gate_eligible,
            "orientation_convention": ORIENTATION_CONVENTION,
            "heading_source": heading_source,
            "quaternion_heading": quaternion_heading,
            "quaternion_heading_diff_rad": quaternion_heading_diff,
            "linear_velocity_frame": "apollo_map",
            "linear_acceleration_frame": "apollo_map",
            "angular_velocity_frame": "apollo_map",
            "vrf_axis_convention": "RFU",
            "angular_velocity_output_unit": "rad_per_s",
            "acceleration_source": extra_metadata.get("acceleration_source"),
        }
    )

    pose: dict[str, Any] = {
        "position": _vector_dict(position),
        "orientation": orientation.to_dict(),
        "heading": heading_value,
    }
    _set_optional_vector(pose, "linear_velocity", linear_velocity)
    _set_optional_vector(pose, "linear_velocity_vrf", linear_velocity_vrf)
    _set_optional_vector(pose, "angular_velocity", angular_velocity)
    _set_optional_vector(pose, "angular_velocity_vrf", angular_velocity_vrf)
    _set_optional_vector(pose, "linear_acceleration", linear_acceleration)
    _set_optional_vector(pose, "linear_acceleration_vrf", linear_acceleration_vrf)

    return {
        "schema_version": "apollo_localization_message.v1",
        "header": {
            "timestamp_sec": float(timestamp_sec),
            "module_name": module_name,
            "sequence_num": sequence_num,
            "frame_id": frame_id,
        },
        "measurement_time": float(timestamp_sec),
        "pose": pose,
        "metadata": extra_metadata,
    }


def write_localization_estimate_to_pb(localization_pb: Any, payload: Mapping[str, Any]) -> None:
    """Write a canonical localization dict into an Apollo-like pb object."""

    header = _mapping(payload.get("header"))
    fill_header(
        getattr(localization_pb, "header", None),
        timestamp_sec=float(header.get("timestamp_sec")),
        module_name=str(header.get("module_name") or DEFAULT_LOCALIZATION_MODULE),
        sequence_num=_optional_int(header.get("sequence_num")),
        frame_id=str(header.get("frame_id") or DEFAULT_LOCALIZATION_FRAME_ID),
    )
    _safe_set(localization_pb, "measurement_time", float(payload.get("measurement_time")))

    pose_payload = _mapping(payload.get("pose"))
    pose_pb = getattr(localization_pb, "pose", None)
    if pose_pb is None:
        return
    _write_vector(getattr(pose_pb, "position", None), pose_payload.get("position"))
    _write_orientation(getattr(pose_pb, "orientation", None), pose_payload.get("orientation"))
    _safe_set(pose_pb, "heading", float(pose_payload.get("heading")))
    for field in (
        "linear_velocity",
        "linear_velocity_vrf",
        "angular_velocity",
        "angular_velocity_vrf",
        "linear_acceleration",
        "linear_acceleration_vrf",
    ):
        if field in pose_payload:
            _write_vector(getattr(pose_pb, field, None), pose_payload.get(field))


def localization_debug_from_dict(payload: Mapping[str, Any]) -> dict[str, Any]:
    header = _mapping(payload.get("header"))
    pose = _mapping(payload.get("pose"))
    metadata = _mapping(payload.get("metadata"))
    orientation = _mapping(pose.get("orientation"))
    linear_velocity = _mapping(pose.get("linear_velocity"))
    angular_velocity = _mapping(pose.get("angular_velocity"))
    linear_accel = _mapping(pose.get("linear_acceleration"))
    linear_accel_vrf = _mapping(pose.get("linear_acceleration_vrf"))
    angular_vrf = _mapping(pose.get("angular_velocity_vrf"))
    heading = pose.get("heading")
    quaternion_heading = metadata.get("quaternion_heading")
    diff = metadata.get("quaternion_heading_diff_rad")
    return {
        "localization_header_timestamp_sec": header.get("timestamp_sec"),
        "localization_measurement_time": payload.get("measurement_time"),
        "measurement_time": payload.get("measurement_time"),
        "localization_sequence_num": header.get("sequence_num"),
        "localization_module_name": header.get("module_name"),
        "localization_frame_id": header.get("frame_id"),
        "localization_time_base": metadata.get("time_base"),
        "localization_heading": heading,
        "localization_orientation_yaw": heading,
        "localization_orientation_qx": _quat_component(orientation, "x", "qx"),
        "localization_orientation_qy": _quat_component(orientation, "y", "qy"),
        "localization_orientation_qz": _quat_component(orientation, "z", "qz"),
        "localization_orientation_qw": _quat_component(orientation, "w", "qw"),
        "decoded_orientation_heading": quaternion_heading,
        "decoded_orientation_heading_diff_rad": diff,
        "orientation_heading_diff_rad": diff,
        "heading_source": metadata.get("heading_source"),
        "orientation_convention": metadata.get("orientation_convention") or ORIENTATION_CONVENTION,
        "localization_velocity_frame": metadata.get("linear_velocity_frame"),
        "localization_acceleration_frame": metadata.get("linear_acceleration_frame"),
        "localization_angular_velocity_frame": metadata.get("angular_velocity_frame"),
        "localization_vrf_axis_convention": metadata.get("vrf_axis_convention"),
        "ego_yaw_rate_rad_s": angular_velocity.get("z"),
        "localization_yaw_rate_rad_s": angular_velocity.get("z"),
        "localization_angular_velocity_z_rad_s": angular_velocity.get("z"),
        "angular_velocity_z_rad_s": angular_velocity.get("z"),
        "localization_angular_velocity_unit": metadata.get("angular_velocity_output_unit"),
        "angular_velocity_source": metadata.get("angular_velocity_source"),
        "linear_acceleration_x": linear_accel.get("x"),
        "linear_acceleration_y": linear_accel.get("y"),
        "linear_acceleration_z": linear_accel.get("z"),
        "linear_acceleration_vrf_x": linear_accel_vrf.get("x"),
        "linear_acceleration_vrf_y": linear_accel_vrf.get("y"),
        "linear_acceleration_vrf_z": linear_accel_vrf.get("z"),
        "linear_acceleration_available": "linear_acceleration" in pose,
        "linear_acceleration_vrf_available": "linear_acceleration_vrf" in pose,
        "angular_velocity_vrf_available": "angular_velocity_vrf" in pose,
        "angular_velocity_vrf_z": angular_vrf.get("z"),
        "acceleration_source": metadata.get("acceleration_source"),
        "vehicle_reference_confidence": metadata.get("vehicle_reference_confidence"),
        "vehicle_reference_hard_gate_eligible": metadata.get("vehicle_reference_hard_gate_eligible"),
        "localization_uncertainty_policy": "not_modelled_gt_truth",
        "localization_msf_status_policy": "not_applicable_gt_truth",
        "localization_sensor_status_policy": "not_applicable_gt_truth",
    }


def _set_optional_vector(payload: dict[str, Any], key: str, value: Any | None) -> None:
    if value is not None:
        payload[key] = _vector_dict(value)


def _vector_dict(value: Any) -> dict[str, float]:
    return Vector3.from_any(value).to_dict()


def _write_vector(target: Any, value: Any) -> None:
    if target is None or value is None:
        return
    vector = _mapping(value)
    _safe_set(target, "x", float(vector.get("x", 0.0)))
    _safe_set(target, "y", float(vector.get("y", 0.0)))
    _safe_set(target, "z", float(vector.get("z", 0.0)))


def _write_orientation(target: Any, value: Any) -> None:
    if target is None or value is None:
        return
    orientation = _mapping(value)
    x = _quat_component(orientation, "x", "qx")
    y = _quat_component(orientation, "y", "qy")
    z = _quat_component(orientation, "z", "qz")
    w = _quat_component(orientation, "w", "qw")
    _safe_set(target, "qx", float(x))
    _safe_set(target, "qy", float(y))
    _safe_set(target, "qz", float(z))
    _safe_set(target, "qw", float(w))
    _safe_set(target, "x", float(x))
    _safe_set(target, "y", float(y))
    _safe_set(target, "z", float(z))
    _safe_set(target, "w", float(w))


def _quat_component(value: Mapping[str, Any], primary: str, alternate: str) -> Any:
    if primary in value:
        return value.get(primary)
    return value.get(alternate)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def _safe_set(obj: Any, attr: str, value: Any) -> None:
    if obj is None:
        return
    try:
        setattr(obj, attr, value)
    except Exception:
        return
