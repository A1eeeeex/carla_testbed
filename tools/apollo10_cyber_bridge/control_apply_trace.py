from __future__ import annotations

from typing import Any, Mapping


_LONGITUDINAL_DEBUG_FIELDS = {
    "debug_simple_lon_current_speed_mps": "debug_simple_lon_current_speed",
    "debug_simple_lon_speed_reference_mps": "debug_simple_lon_speed_reference",
    "debug_simple_lon_speed_error_mps": "debug_simple_lon_speed_error",
    "debug_simple_lon_current_acceleration_mps2": "debug_simple_lon_current_acceleration",
    "debug_simple_lon_acceleration_reference_mps2": "debug_simple_lon_acceleration_reference",
    "debug_simple_lon_acceleration_error_mps2": "debug_simple_lon_acceleration_error",
    "debug_simple_lon_acceleration_cmd_mps2": "debug_simple_lon_acceleration_cmd",
    "debug_simple_lon_acceleration_cmd_closeloop_mps2": "debug_simple_lon_acceleration_cmd_closeloop",
    "debug_simple_lon_acceleration_lookup_mps2": "debug_simple_lon_acceleration_lookup",
    "debug_simple_lon_throttle_cmd_pct": "debug_simple_lon_throttle_cmd",
    "debug_simple_lon_brake_cmd_pct": "debug_simple_lon_brake_cmd",
    "debug_simple_lon_path_remain_m": "debug_simple_lon_path_remain",
    "debug_simple_lon_station_error_m": "debug_simple_lon_station_error",
    "debug_simple_lon_preview_station_error_m": "debug_simple_lon_preview_station_error",
    "debug_simple_lon_current_matched_point_s_m": "debug_simple_lon_current_matched_point_s",
    "debug_simple_lon_current_matched_point_x": "debug_simple_lon_current_matched_point_x",
    "debug_simple_lon_current_matched_point_y": "debug_simple_lon_current_matched_point_y",
    "debug_simple_lon_current_matched_point_theta_rad": "debug_simple_lon_current_matched_point_theta",
    "debug_simple_lon_current_matched_point_kappa": "debug_simple_lon_current_matched_point_kappa",
    "debug_simple_lon_current_reference_point_s_m": "debug_simple_lon_current_reference_point_s",
    "debug_simple_lon_current_reference_point_x": "debug_simple_lon_current_reference_point_x",
    "debug_simple_lon_current_reference_point_y": "debug_simple_lon_current_reference_point_y",
    "debug_simple_lon_current_reference_point_theta_rad": "debug_simple_lon_current_reference_point_theta",
    "debug_simple_lon_current_reference_point_kappa": "debug_simple_lon_current_reference_point_kappa",
    "debug_simple_lon_preview_reference_point_s_m": "debug_simple_lon_preview_reference_point_s",
    "debug_simple_lon_preview_reference_point_x": "debug_simple_lon_preview_reference_point_x",
    "debug_simple_lon_preview_reference_point_y": "debug_simple_lon_preview_reference_point_y",
    "debug_simple_lon_preview_reference_point_theta_rad": "debug_simple_lon_preview_reference_point_theta",
    "debug_simple_lon_preview_reference_point_kappa": "debug_simple_lon_preview_reference_point_kappa",
}


def finite_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def first_finite(*values: Any) -> float | None:
    for value in values:
        parsed = finite_or_none(value)
        if parsed is not None:
            return parsed
    return None


def first_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def build_control_apply_trace_payload(
    row: Mapping[str, Any],
    measured: Mapping[str, Any],
    stats: Mapping[str, Any],
) -> dict[str, Any]:
    raw_payload = stats.get("last_control_raw", {}) or {}
    control_in = stats.get("last_control_in", {}) or {}
    control_out = stats.get("last_control_out", {}) or {}
    if not isinstance(raw_payload, Mapping):
        raw_payload = {}
    if not isinstance(control_in, Mapping):
        control_in = {}
    if not isinstance(control_out, Mapping):
        control_out = {}
    raw_msg = raw_payload.get("raw_control_msg_dump") or raw_payload.get("apollo_control_raw") or {}
    if not isinstance(raw_msg, Mapping):
        raw_msg = {}

    return {
        "schema_version": "control_apply_trace.v1",
        "event_type": "control_apply",
        "timestamp": finite_or_none(row.get("ts_sec")),
        "sim_time": finite_or_none(row.get("sim_time")),
        "world_frame": finite_or_none(row.get("frame_id")),
        "route_id": row.get("route_id"),
        "route_s": finite_or_none(row.get("route_s")),
        "actuator_mapping_mode": first_value(row.get("actuator_mapping_mode"), control_out.get("actuator_mapping_mode")),
        "calibration_profile_id": first_value(row.get("calibration_profile_id"), control_out.get("calibration_profile_id")),
        "steer_scale": first_finite(row.get("steer_scale"), control_out.get("steer_scale")),
        "steering_sign": first_finite(row.get("steering_sign"), control_out.get("steer_sign")),
        "control_latency_ms": first_finite(
            row.get("control_latency_ms"),
            control_out.get("control_latency_ms"),
            control_in.get("control_latency_ms"),
        ),
        "control_message_age_ms": first_finite(
            row.get("control_message_age_ms"),
            control_out.get("control_message_age_ms"),
            control_in.get("control_message_age_ms"),
        ),
        "planning_message_age_ms": first_finite(row.get("planning_message_age_ms"), control_out.get("planning_message_age_ms")),
        **build_longitudinal_debug_fields(row, control_in, raw_msg),
        "apollo_control": {
            "header_sequence_num": first_finite(
                row.get("control_header_sequence_num"),
                control_in.get("control_header_sequence_num"),
                raw_msg.get("control_header_sequence_num"),
            ),
            "header_timestamp_sec": first_finite(
                row.get("control_header_timestamp_sec"),
                control_in.get("control_header_timestamp_sec"),
                raw_msg.get("control_header_timestamp_sec"),
            ),
            "rx_timestamp": first_finite(
                row.get("control_rx_timestamp"),
                control_in.get("control_rx_timestamp"),
            ),
            "control_timestamp": first_finite(
                row.get("control_timestamp"),
                control_in.get("control_timestamp"),
                raw_payload.get("ts_sec"),
            ),
        },
        "apollo_raw": {
            "throttle": first_finite(row.get("throttle_raw"), control_in.get("throttle"), raw_msg.get("throttle")),
            "brake": first_finite(row.get("brake_raw"), control_in.get("brake"), raw_msg.get("brake")),
            "steer": first_finite(row.get("apollo_steer_raw"), control_in.get("steer")),
            "gear": first_value(row.get("apollo_gear"), control_in.get("gear"), raw_msg.get("gear_location")),
            "estop": bool(first_value(row.get("apollo_estop"), control_in.get("estop"), raw_msg.get("estop"), False)),
        },
        "bridge_mapped": {
            "throttle": first_finite(row.get("throttle_mapped"), control_out.get("throttle")),
            "brake": first_finite(row.get("brake_mapped"), control_out.get("brake")),
            "steer": first_finite(row.get("bridge_steer_mapped"), control_out.get("steer")),
            "mapped_throttle_cmd": first_finite(row.get("mapped_throttle_cmd"), control_out.get("mapped_throttle_cmd")),
            "mapped_brake_cmd": first_finite(row.get("mapped_brake_cmd"), control_out.get("mapped_brake_cmd")),
            "mapped_carla_steer_cmd": first_finite(
                row.get("mapped_carla_steer_cmd"),
                control_out.get("mapped_carla_steer_cmd"),
            ),
            "throttle_brake_mutual_exclusion_applied": bool(
                first_value(
                    row.get("throttle_brake_mutual_exclusion_applied"),
                    control_out.get("throttle_brake_mutual_exclusion_applied"),
                    False,
                )
            ),
            "throttle_brake_hysteresis_held": bool(
                first_value(
                    row.get("throttle_brake_hysteresis_held"),
                    control_out.get("throttle_brake_hysteresis_held"),
                    False,
                )
            ),
        },
        "carla_applied": {
            "throttle": first_finite(row.get("throttle_applied"), row.get("measured_throttle"), measured.get("throttle")),
            "brake": first_finite(row.get("brake_applied"), row.get("measured_brake"), measured.get("brake")),
            "steer": first_finite(row.get("carla_steer_applied"), row.get("measured_steer"), measured.get("steer")),
            "reverse": measured.get("reverse", False),
            "hand_brake": measured.get("hand_brake", False),
        },
        "vehicle_response": {
            "speed_mps": finite_or_none(row.get("speed_mps")),
            "yaw_rate_rad_s": finite_or_none(measured.get("yaw_rate_rps")),
            "forward_accel_mps2": finite_or_none(row.get("measured_forward_accel_mps2")),
            "pose_x": finite_or_none(measured.get("pose_x")),
            "pose_y": finite_or_none(measured.get("pose_y")),
            "pose_yaw_deg": finite_or_none(measured.get("pose_yaw_deg")),
        },
        "diagnostics": {
            "planning_lateral_contract_valid": row.get("planning_lateral_contract_valid"),
            "planning_lateral_contract_reason": row.get("planning_lateral_contract_reason"),
            "lateral_guard_applied": row.get("lateral_guard_applied"),
            "trajectory_contract_guard_applied": row.get("trajectory_contract_guard_applied"),
        },
    }


def build_longitudinal_debug_fields(
    row: Mapping[str, Any],
    control_in: Mapping[str, Any],
    raw_msg: Mapping[str, Any],
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for target, raw_key in _LONGITUDINAL_DEBUG_FIELDS.items():
        fields[target] = first_finite(row.get(target), control_in.get(target), raw_msg.get(raw_key))
    fields["debug_simple_lon_is_full_stop"] = bool(
        first_value(
            row.get("debug_simple_lon_is_full_stop"),
            control_in.get("debug_simple_lon_is_full_stop"),
            raw_msg.get("debug_simple_lon_is_full_stop"),
            False,
        )
    )
    return fields
