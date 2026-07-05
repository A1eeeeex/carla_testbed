from __future__ import annotations

from collections import Counter, deque
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


def radians_from_degrees(value: Any) -> float | None:
    parsed = finite_or_none(value)
    if parsed is None:
        return None
    return parsed * 3.141592653589793 / 180.0


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


class ControlCriticalWindowRecorder:
    """Small bounded recorder for raw/mapped/applied evidence around lateral events."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        pre_samples: int = 20,
        post_samples: int = 40,
        max_windows: int = 3,
        max_rows: int = 240,
        route_lateral_error_threshold_m: float = 0.5,
        simple_lat_lateral_error_threshold_m: float = 0.5,
        raw_steer_threshold: float = 0.85,
        matched_point_distance_threshold_m: float = 8.0,
        target_point_distance_threshold_m: float = 8.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.pre_samples = max(0, int(pre_samples or 0))
        self.post_samples = max(0, int(post_samples or 0))
        self.max_windows = max(0, int(max_windows or 0))
        self.max_rows = max(0, int(max_rows or 0))
        self.route_lateral_error_threshold_m = max(0.0, float(route_lateral_error_threshold_m or 0.0))
        self.simple_lat_lateral_error_threshold_m = max(
            0.0,
            float(simple_lat_lateral_error_threshold_m or 0.0),
        )
        self.raw_steer_threshold = max(0.0, float(raw_steer_threshold or 0.0))
        self.matched_point_distance_threshold_m = max(
            0.0,
            float(matched_point_distance_threshold_m or 0.0),
        )
        self.target_point_distance_threshold_m = max(
            0.0,
            float(target_point_distance_threshold_m or 0.0),
        )
        self._pre_buffer: deque[dict[str, Any]] = deque(maxlen=self.pre_samples)
        self._active_window_id: int | None = None
        self._post_remaining = 0
        self._windows_started = 0
        self._rows_written = 0
        self._trigger_counts: Counter[str] = Counter()
        self._dropped_rows = 0

    def observe(self, payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        payload_dict = dict(payload)
        if not self.enabled or self.max_windows <= 0 or self.max_rows <= 0:
            self._remember(payload_dict)
            return []

        reason, trigger_fields = self._trigger_reason(payload_dict)
        rows: list[dict[str, Any]] = []
        if reason and self._active_window_id is None and self._windows_started < self.max_windows:
            self._windows_started += 1
            self._active_window_id = self._windows_started
            self._post_remaining = self.post_samples
            self._trigger_counts[reason] += 1
            for item in list(self._pre_buffer):
                rows.append(self._decorate(item, phase="pre", trigger_reason=reason, trigger_fields=trigger_fields))
            rows.append(
                self._decorate(
                    payload_dict,
                    phase="anchor",
                    trigger_reason=reason,
                    trigger_fields=trigger_fields,
                )
            )
        elif self._active_window_id is not None:
            rows.append(
                self._decorate(
                    payload_dict,
                    phase="post",
                    trigger_reason=reason or "active_window",
                    trigger_fields=trigger_fields,
                )
            )
            self._post_remaining -= 1
            if self._post_remaining <= 0:
                self._active_window_id = None

        self._remember(payload_dict)
        return self._limit_rows(rows)

    def stats(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "pre_samples": self.pre_samples,
            "post_samples": self.post_samples,
            "max_windows": self.max_windows,
            "max_rows": self.max_rows,
            "windows_started": self._windows_started,
            "rows_written": self._rows_written,
            "dropped_rows": self._dropped_rows,
            "active": self._active_window_id is not None,
            "trigger_counts": dict(self._trigger_counts),
            "thresholds": {
                "route_lateral_error_threshold_m": self.route_lateral_error_threshold_m,
                "simple_lat_lateral_error_threshold_m": self.simple_lat_lateral_error_threshold_m,
                "raw_steer_threshold": self.raw_steer_threshold,
                "matched_point_distance_threshold_m": self.matched_point_distance_threshold_m,
                "target_point_distance_threshold_m": self.target_point_distance_threshold_m,
            },
        }

    def _remember(self, payload: dict[str, Any]) -> None:
        if self.pre_samples > 0:
            self._pre_buffer.append(dict(payload))

    def _limit_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        remaining = max(0, self.max_rows - self._rows_written)
        if remaining <= 0:
            self._dropped_rows += len(rows)
            return []
        kept = rows[:remaining]
        self._rows_written += len(kept)
        if len(kept) < len(rows):
            self._dropped_rows += len(rows) - len(kept)
        return kept

    def _decorate(
        self,
        payload: Mapping[str, Any],
        *,
        phase: str,
        trigger_reason: str,
        trigger_fields: Mapping[str, Any],
    ) -> dict[str, Any]:
        row = dict(payload)
        row["schema_version"] = "control_critical_window_trace.v1"
        row["event_type"] = "control_critical_window"
        row["critical_window_id"] = self._active_window_id
        row["critical_window_phase"] = phase
        row["critical_window_trigger_reason"] = trigger_reason
        row["critical_window_trigger_fields"] = dict(trigger_fields)
        return row

    def _trigger_reason(self, payload: Mapping[str, Any]) -> tuple[str | None, dict[str, Any]]:
        fields: dict[str, Any] = {}
        route_lateral = first_finite(payload.get("cross_track_error"), payload.get("e_y_m"), payload.get("lane_dist_m"))
        if (
            route_lateral is not None
            and self.route_lateral_error_threshold_m > 0.0
            and abs(route_lateral) >= self.route_lateral_error_threshold_m
        ):
            fields["route_lateral_error_m"] = route_lateral
            return "route_lateral_error_high", fields
        simple_lat = first_finite(
            payload.get("apollo_debug_simple_lat_lateral_error_m"),
            payload.get("debug_simple_lat_lateral_error"),
        )
        if (
            simple_lat is not None
            and self.simple_lat_lateral_error_threshold_m > 0.0
            and abs(simple_lat) >= self.simple_lat_lateral_error_threshold_m
        ):
            fields["simple_lat_lateral_error_m"] = simple_lat
            return "simple_lat_lateral_error_high", fields
        raw_steer = first_finite(payload.get("apollo_steer_raw"), payload.get("steering_target"))
        if (
            raw_steer is not None
            and self.raw_steer_threshold > 0.0
            and abs(raw_steer) >= self.raw_steer_threshold
        ):
            fields["apollo_steer_raw"] = raw_steer
            return "apollo_raw_steer_high", fields
        matched_distance = finite_or_none(payload.get("apollo_matched_point_distance"))
        if (
            matched_distance is not None
            and self.matched_point_distance_threshold_m > 0.0
            and matched_distance >= self.matched_point_distance_threshold_m
        ):
            fields["apollo_matched_point_distance"] = matched_distance
            return "matched_point_distance_high", fields
        target_distance = finite_or_none(payload.get("apollo_target_point_distance"))
        if (
            target_distance is not None
            and self.target_point_distance_threshold_m > 0.0
            and target_distance >= self.target_point_distance_threshold_m
        ):
            fields["apollo_target_point_distance"] = target_distance
            return "target_point_distance_high", fields
        lane_inside = _as_bool(payload.get("lane_inside"))
        if lane_inside is False:
            fields["lane_inside"] = False
            return "lane_inside_false", fields
        return None, fields


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
    gt_state = stats.get("last_gt_state_publish", {}) or {}
    if not isinstance(gt_state, Mapping):
        gt_state = {}

    payload = {
        "schema_version": "control_apply_trace.v1",
        "event_type": "control_apply",
        "timestamp": first_finite(
            row.get("ts_sec"),
            control_out.get("control_timestamp"),
            control_in.get("control_timestamp"),
            raw_payload.get("ts_sec"),
        ),
        "sim_time": first_finite(
            row.get("sim_time"),
            gt_state.get("sim_time_sec"),
            control_out.get("gt_state_sim_time_sec"),
            control_in.get("gt_state_sim_time_sec"),
        ),
        "world_frame": first_finite(
            row.get("frame_id"),
            gt_state.get("world_frame"),
            control_out.get("gt_state_world_frame"),
            control_in.get("gt_state_world_frame"),
        ),
        "route_id": row.get("route_id"),
        "route_s": finite_or_none(row.get("route_s")),
        "map_x": finite_or_none(row.get("map_x")),
        "map_y": finite_or_none(row.get("map_y")),
        "map_yaw_deg": finite_or_none(row.get("map_yaw_deg")),
        "in_bounds": row.get("in_bounds"),
        "lane_inside": row.get("lane_inside"),
        "lane_dist_m": finite_or_none(row.get("lane_dist_m")),
        "e_y_m": finite_or_none(row.get("e_y_m")),
        "e_psi_deg": finite_or_none(row.get("e_psi_deg")),
        "cross_track_error": first_finite(row.get("cross_track_error"), row.get("e_y_m")),
        "heading_error": first_finite(row.get("heading_error"), radians_from_degrees(row.get("e_psi_deg"))),
        "apollo_debug_simple_lat_lateral_error_m": first_finite(
            row.get("apollo_debug_simple_lat_lateral_error_m"),
            row.get("debug_simple_lat_lateral_error"),
        ),
        "apollo_debug_simple_lat_heading_error_rad": first_finite(
            row.get("apollo_debug_simple_lat_heading_error_rad"),
            row.get("debug_simple_lat_heading_error"),
        ),
        "apollo_debug_simple_lat_lateral_error_rate_mps": first_finite(
            row.get("apollo_debug_simple_lat_lateral_error_rate_mps"),
            row.get("debug_simple_lat_lateral_error_rate"),
        ),
        "apollo_debug_simple_lat_heading_error_rate_radps": first_finite(
            row.get("apollo_debug_simple_lat_heading_error_rate_radps"),
            row.get("debug_simple_lat_heading_error_rate"),
        ),
        "apollo_target_point_kappa": first_finite(
            row.get("apollo_target_point_kappa"),
            row.get("apollo_debug_simple_lat_target_point_kappa"),
            row.get("debug_simple_lat_current_target_point_kappa"),
        ),
        "apollo_matched_point_distance": finite_or_none(row.get("apollo_matched_point_distance")),
        "apollo_target_point_distance": finite_or_none(row.get("apollo_target_point_distance")),
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
        "gt_state": {
            "sim_time_sec": first_finite(
                gt_state.get("sim_time_sec"),
                control_out.get("gt_state_sim_time_sec"),
                control_in.get("gt_state_sim_time_sec"),
            ),
            "world_frame": first_finite(
                gt_state.get("world_frame"),
                control_out.get("gt_state_world_frame"),
                control_in.get("gt_state_world_frame"),
            ),
            "publish_wall_time_sec": first_finite(gt_state.get("publish_wall_time_sec")),
            "sample_reason": first_value(
                gt_state.get("sample_reason"),
                control_out.get("gt_state_sample_reason"),
                control_in.get("gt_state_sample_reason"),
            ),
            "fresh_sample": bool(first_value(gt_state.get("fresh_sample"), False)),
            "localization_sequence_num": first_finite(gt_state.get("localization_sequence_num")),
            "chassis_sequence_num": first_finite(gt_state.get("chassis_sequence_num")),
            "age_wall_ms": first_finite(
                control_out.get("gt_state_age_wall_ms"),
                control_in.get("gt_state_age_wall_ms"),
            ),
            "age_sim_ms": first_finite(
                control_out.get("gt_state_age_sim_ms"),
                control_in.get("gt_state_age_sim_ms"),
            ),
            "same_world_frame_control_cycle_streak": first_finite(
                control_out.get("same_gt_world_frame_control_cycle_streak"),
                control_in.get("same_gt_world_frame_control_cycle_streak"),
            ),
            "chassis_speed_mps": first_finite(row.get("chassis_speed_mps")),
            "driving_mode": first_value(row.get("driving_mode"), row.get("driving_mode_value")),
            "gear_location": first_value(row.get("gear_location"), row.get("gear_location_value")),
            "chassis_error_code": first_value(
                row.get("chassis_error_code"),
                row.get("chassis_error_code_value"),
            ),
            "feedback_source": first_value(row.get("chassis_feedback_source")),
            "feedback_available": first_value(row.get("chassis_control_feedback_available")),
            "throttle_percentage": first_finite(row.get("chassis_throttle_percentage")),
            "brake_percentage": first_finite(row.get("chassis_brake_percentage")),
            "steering_percentage": first_finite(row.get("chassis_steering_percentage")),
            "steering_percentage_frame": first_value(row.get("chassis_steering_percentage_frame")),
            "carla_steering_percentage": first_finite(row.get("chassis_carla_steering_percentage")),
            "steering_feedback_sign": first_finite(row.get("chassis_steering_feedback_sign")),
            "steering_percentage_cmd": first_finite(row.get("chassis_steering_percentage_cmd")),
            "carla_steering_percentage_cmd": first_finite(
                row.get("chassis_carla_steering_percentage_cmd")
            ),
            "measured_steer_pct": first_finite(row.get("measured_steer_pct")),
            "measured_steer_deg": first_finite(row.get("measured_steer_deg")),
        },
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
            # Prefer the publish-loop row snapshot over live stats. The control
            # callback can update stats while artifacts are being written, so
            # mixing row fields with stats can create false mapped/applied
            # mismatches in high-rate runs.
            "throttle": first_finite(
                row.get("commanded_throttle"),
                row.get("throttle_applied"),
                row.get("throttle_mapped"),
                control_out.get("throttle"),
            ),
            "brake": first_finite(
                row.get("commanded_brake"),
                row.get("brake_applied"),
                row.get("brake_mapped"),
                control_out.get("brake"),
            ),
            "steer": first_finite(
                row.get("commanded_steer"),
                row.get("carla_steer_applied"),
                row.get("bridge_steer_mapped"),
                control_out.get("steer"),
            ),
            "steer_pre_policy": first_finite(
                row.get("commanded_steer_pre_lateral_guards"),
                row.get("commanded_steer_pre_clamp"),
                control_out.get("steer_before_lateral_guards"),
                control_out.get("steer_pre_clamp"),
            ),
            "mapped_throttle_cmd": first_finite(
                row.get("commanded_throttle"),
                row.get("throttle_applied"),
                row.get("mapped_throttle_cmd"),
                control_out.get("mapped_throttle_cmd"),
                control_out.get("throttle"),
            ),
            "mapped_brake_cmd": first_finite(
                row.get("commanded_brake"),
                row.get("brake_applied"),
                row.get("mapped_brake_cmd"),
                control_out.get("mapped_brake_cmd"),
                control_out.get("brake"),
            ),
            "mapped_carla_steer_cmd": first_finite(
                row.get("commanded_steer"),
                row.get("carla_steer_applied"),
                row.get("mapped_carla_steer_cmd"),
                control_out.get("steer"),
                control_out.get("mapped_carla_steer_cmd"),
            ),
            "base_mapped_carla_steer_cmd": first_finite(
                row.get("base_mapped_carla_steer_cmd"),
                control_out.get("base_mapped_carla_steer_cmd"),
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
            "low_speed_steer_guard_applied": row.get("low_speed_steer_guard_applied"),
            "low_speed_sustained_guard_applied": row.get("low_speed_sustained_guard_applied"),
            "low_speed_sustained_guard_active": row.get("low_speed_sustained_guard_active"),
            "trajectory_contract_guard_applied": row.get("trajectory_contract_guard_applied"),
        },
    }
    _populate_flat_control_fields(payload)
    return payload


def _populate_flat_control_fields(payload: dict[str, Any]) -> None:
    """Keep control traces readable by analyzers that expect flat time-series fields."""
    apollo_raw = payload.get("apollo_raw") if isinstance(payload.get("apollo_raw"), Mapping) else {}
    bridge_mapped = payload.get("bridge_mapped") if isinstance(payload.get("bridge_mapped"), Mapping) else {}
    carla_applied = payload.get("carla_applied") if isinstance(payload.get("carla_applied"), Mapping) else {}
    apollo_control = payload.get("apollo_control") if isinstance(payload.get("apollo_control"), Mapping) else {}
    vehicle_response = payload.get("vehicle_response") if isinstance(payload.get("vehicle_response"), Mapping) else {}

    flat_values = {
        "control_header_sequence_num": apollo_control.get("header_sequence_num"),
        "control_header_timestamp_sec": apollo_control.get("header_timestamp_sec"),
        "control_rx_timestamp": apollo_control.get("rx_timestamp"),
        "control_timestamp": apollo_control.get("control_timestamp"),
        "throttle_raw": apollo_raw.get("throttle"),
        "brake_raw": apollo_raw.get("brake"),
        "apollo_steer_raw": apollo_raw.get("steer"),
        "apollo_gear": apollo_raw.get("gear"),
        "apollo_estop": apollo_raw.get("estop"),
        "throttle_mapped": bridge_mapped.get("mapped_throttle_cmd", bridge_mapped.get("throttle")),
        "brake_mapped": bridge_mapped.get("mapped_brake_cmd", bridge_mapped.get("brake")),
        "bridge_steer_mapped": bridge_mapped.get("mapped_carla_steer_cmd", bridge_mapped.get("steer")),
        "bridge_steer_pre_policy": bridge_mapped.get("steer_pre_policy"),
        "bridge_steer_base_mapped": bridge_mapped.get("base_mapped_carla_steer_cmd"),
        "throttle_applied": carla_applied.get("throttle"),
        "brake_applied": carla_applied.get("brake"),
        "carla_steer_applied": carla_applied.get("steer"),
        "ego_speed": vehicle_response.get("speed_mps"),
        "ego_yaw_rate": vehicle_response.get("yaw_rate_rad_s"),
    }
    for key, value in flat_values.items():
        if value is not None and payload.get(key) is None:
            payload[key] = value


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
