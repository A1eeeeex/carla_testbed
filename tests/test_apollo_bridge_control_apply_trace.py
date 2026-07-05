from __future__ import annotations

import importlib
import sys
import types

import pytest

from tools.apollo10_cyber_bridge.control_apply_trace import (
    ControlCriticalWindowRecorder,
    build_control_apply_trace_payload,
)


class _Header:
    timestamp_sec: float
    module_name: str
    sequence_num: int
    frame_id: str


class _Chassis:
    COMPLETE_AUTO_DRIVE = 1
    GEAR_DRIVE = 3
    NO_ERROR = 0

    def __init__(self) -> None:
        self.header = _Header()
        self.engine_started = False
        self.speed_mps = 0.0
        self.throttle_percentage = 0.0
        self.brake_percentage = 0.0
        self.steering_percentage = 0.0
        self.throttle_percentage_cmd = 0.0
        self.brake_percentage_cmd = 0.0
        self.steering_percentage_cmd = 0.0
        self.parking_brake = False
        self.driving_mode = 0
        self.gear_location = 0
        self.error_code = 0


class _ChassisPb2:
    Chassis = _Chassis


def _install_fake_runtime_modules() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault(
        "google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2")
    )
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))


def test_bridge_chassis_steering_feedback_uses_apollo_sign_and_preserves_carla_sign() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.seq = 0
    adapter.chassis_pb2 = _ChassisPb2
    adapter.steer_sign = -1.0
    adapter.stats = {
        "last_control_out": {
            "steer": -0.50,
            "steer_sign": -1.0,
            "steering_normalized_for_mapping": 0.50,
            "throttle": 0.0,
            "brake": 0.0,
        }
    }

    chassis = adapter._odom_to_chassis(
        3.0,
        (1.0, 0.0, 0.0),
        {
            "available": True,
            "source": "wheel_angle",
            "steer": -0.50,
            "steer_feedback_pct": -50.0,
            "throttle": 0.0,
            "brake": 0.0,
        },
    )

    assert chassis.steering_percentage == pytest.approx(50.0)
    assert chassis.steering_percentage_cmd == pytest.approx(50.0)
    feedback = adapter.stats["last_control_feedback"]
    assert feedback["measured"]["steer_pct"] == pytest.approx(50.0)
    assert feedback["measured"]["carla_steer_pct"] == pytest.approx(-50.0)
    assert feedback["measured"]["steering_percentage_frame"] == "apollo_control"
    assert feedback["measured"]["steer_feedback_sign"] == pytest.approx(-1.0)
    assert feedback["chassis"]["steering_percentage"] == pytest.approx(50.0)
    assert feedback["chassis"]["carla_steering_percentage"] == pytest.approx(-50.0)
    assert feedback["chassis"]["carla_steering_percentage_cmd"] == pytest.approx(-50.0)


def test_control_apply_trace_falls_back_to_latest_control_cache() -> None:
    stats = {
        "last_control_raw": {
            "ts_sec": 12.5,
            "raw_control_msg_dump": {
                "control_header_timestamp_sec": 12.25,
                "control_header_sequence_num": 42,
                "throttle": 42.0,
                "brake": 0.0,
                "debug_simple_lon_current_speed": 2.5,
                "debug_simple_lon_speed_reference": 6.0,
                "debug_simple_lon_speed_error": 3.5,
                "debug_simple_lon_acceleration_cmd": 1.2,
                "debug_simple_lon_throttle_cmd": 42.0,
                "debug_simple_lon_brake_cmd": 0.0,
                "debug_simple_lon_path_remain": 18.0,
                "debug_simple_lon_station_error": 0.8,
                "debug_simple_lon_current_matched_point_s": 7.0,
                "debug_simple_lon_current_matched_point_x": 1.0,
                "debug_simple_lon_current_matched_point_y": 2.0,
                "debug_simple_lon_current_matched_point_theta": 0.3,
                "debug_simple_lon_current_matched_point_kappa": 0.01,
                "debug_simple_lon_current_reference_point_s": 8.0,
                "debug_simple_lon_current_reference_point_x": 1.5,
                "debug_simple_lon_current_reference_point_y": 2.5,
                "debug_simple_lon_current_reference_point_theta": 0.4,
                "debug_simple_lon_current_reference_point_kappa": 0.02,
                "debug_simple_lon_preview_reference_point_s": 9.0,
                "debug_simple_lon_preview_reference_point_x": 2.0,
                "debug_simple_lon_preview_reference_point_y": 3.0,
                "debug_simple_lon_preview_reference_point_theta": 0.5,
                "debug_simple_lon_preview_reference_point_kappa": 0.03,
                "gear_location": 1,
                "estop": False,
            }
        },
        "last_control_in": {
            "throttle": 0.42,
            "brake": 0.0,
            "steer": 0.1,
            "gear": "1",
            "control_header_timestamp_sec": 12.25,
            "control_header_sequence_num": 42,
            "control_rx_timestamp": 12.3,
            "control_timestamp": 12.31,
            "control_latency_ms": 3.0,
            "control_message_age_ms": 4.0,
            "gt_state_age_wall_ms": 25.0,
            "gt_state_age_sim_ms": 0.0,
            "same_gt_world_frame_control_cycle_streak": 3,
        },
        "last_control_out": {
            "actuator_mapping_mode": "legacy",
            "steer_scale": 0.25,
            "steer_sign": 1.0,
            "throttle": 0.42,
            "brake": 0.0,
            "steer": 0.025,
            "base_mapped_carla_steer_cmd": 0.1,
            "mapped_throttle_cmd": 0.42,
            "mapped_brake_cmd": 0.0,
            "mapped_carla_steer_cmd": 0.025,
            "low_speed_sustained_guard_applied": True,
            "low_speed_sustained_guard_active": True,
            "planning_message_age_ms": 5.0,
            "throttle_brake_mutual_exclusion_applied": False,
            "throttle_brake_hysteresis_held": False,
        },
        "last_gt_state_publish": {
            "sim_time_sec": 1.0,
            "world_frame": 123,
            "publish_wall_time_sec": 12.285,
            "sample_reason": "fresh_sample",
            "fresh_sample": True,
            "localization_sequence_num": 7,
            "chassis_sequence_num": 7,
        },
    }

    payload = build_control_apply_trace_payload(
        {
            "ts_sec": 10.0,
            "sim_time": 1.0,
            "frame_id": 123,
            "throttle_raw": None,
            "brake_raw": None,
            "apollo_steer_raw": None,
            "throttle_mapped": None,
            "brake_mapped": None,
            "bridge_steer_mapped": None,
            "chassis_speed_mps": 2.0,
            "driving_mode": "COMPLETE_AUTO_DRIVE",
            "gear_location": "GEAR_DRIVE",
            "chassis_error_code": "NO_ERROR",
            "chassis_feedback_source": "wheel_angle",
            "chassis_control_feedback_available": True,
            "chassis_throttle_percentage": 40.0,
            "chassis_brake_percentage": 0.0,
            "chassis_steering_percentage": -12.0,
            "chassis_steering_percentage_frame": "apollo_control",
            "chassis_carla_steering_percentage": -12.0,
            "chassis_steering_feedback_sign": 1.0,
            "chassis_steering_percentage_cmd": 10.0,
            "chassis_carla_steering_percentage_cmd": 10.0,
            "measured_steer_pct": -12.0,
            "measured_steer_deg": -8.4,
        },
        {"throttle": 0.4, "brake": 0.0, "steer": 0.02, "speed_mps": 2.0, "reverse": False, "hand_brake": False},
        stats,
    )

    assert payload["apollo_raw"]["throttle"] == 0.42
    assert payload["apollo_raw"]["brake"] == 0.0
    assert payload["apollo_raw"]["steer"] == 0.1
    assert payload["throttle_raw"] == 0.42
    assert payload["brake_raw"] == 0.0
    assert payload["apollo_steer_raw"] == 0.1
    assert payload["bridge_mapped"]["throttle"] == 0.42
    assert payload["bridge_mapped"]["brake"] == 0.0
    assert payload["bridge_mapped"]["steer"] == 0.025
    assert payload["bridge_mapped"]["base_mapped_carla_steer_cmd"] == 0.1
    assert payload["bridge_mapped"]["mapped_carla_steer_cmd"] == 0.025
    assert payload["throttle_mapped"] == 0.42
    assert payload["brake_mapped"] == 0.0
    assert payload["bridge_steer_mapped"] == 0.025
    assert payload["bridge_steer_base_mapped"] == 0.1
    assert payload["diagnostics"]["low_speed_sustained_guard_applied"] is None
    assert payload["carla_applied"]["throttle"] == 0.4
    assert payload["carla_applied"]["brake"] == 0.0
    assert payload["carla_applied"]["steer"] == 0.02
    assert payload["throttle_applied"] == 0.4
    assert payload["brake_applied"] == 0.0
    assert payload["carla_steer_applied"] == 0.02
    assert payload["apollo_control"]["header_sequence_num"] == 42.0
    assert payload["apollo_control"]["header_timestamp_sec"] == 12.25
    assert payload["apollo_control"]["rx_timestamp"] == 12.3
    assert payload["apollo_control"]["control_timestamp"] == 12.31
    assert payload["debug_simple_lon_current_speed_mps"] == 2.5
    assert payload["debug_simple_lon_speed_reference_mps"] == 6.0
    assert payload["debug_simple_lon_speed_error_mps"] == 3.5
    assert payload["debug_simple_lon_acceleration_cmd_mps2"] == 1.2
    assert payload["debug_simple_lon_throttle_cmd_pct"] == 42.0
    assert payload["debug_simple_lon_brake_cmd_pct"] == 0.0
    assert payload["debug_simple_lon_path_remain_m"] == 18.0
    assert payload["debug_simple_lon_station_error_m"] == 0.8
    assert payload["debug_simple_lon_current_matched_point_s_m"] == 7.0
    assert payload["debug_simple_lon_current_matched_point_x"] == 1.0
    assert payload["debug_simple_lon_current_reference_point_s_m"] == 8.0
    assert payload["debug_simple_lon_current_reference_point_theta_rad"] == 0.4
    assert payload["debug_simple_lon_preview_reference_point_s_m"] == 9.0
    assert payload["debug_simple_lon_preview_reference_point_kappa"] == 0.03
    assert payload["control_latency_ms"] == 3.0
    assert payload["control_message_age_ms"] == 4.0
    assert payload["planning_message_age_ms"] == 5.0
    assert payload["actuator_mapping_mode"] == "legacy"
    assert payload["steer_scale"] == 0.25
    assert payload["steering_sign"] == 1.0
    assert payload["gt_state"]["sim_time_sec"] == 1.0
    assert payload["gt_state"]["world_frame"] == 123.0
    assert payload["gt_state"]["sample_reason"] == "fresh_sample"
    assert payload["gt_state"]["fresh_sample"] is True
    assert payload["gt_state"]["age_wall_ms"] == 25.0
    assert payload["gt_state"]["age_sim_ms"] == 0.0
    assert payload["gt_state"]["same_world_frame_control_cycle_streak"] == 3.0
    assert payload["gt_state"]["chassis_speed_mps"] == 2.0
    assert payload["gt_state"]["driving_mode"] == "COMPLETE_AUTO_DRIVE"
    assert payload["gt_state"]["gear_location"] == "GEAR_DRIVE"
    assert payload["gt_state"]["chassis_error_code"] == "NO_ERROR"
    assert payload["gt_state"]["feedback_source"] == "wheel_angle"
    assert payload["gt_state"]["feedback_available"] is True
    assert payload["gt_state"]["throttle_percentage"] == 40.0
    assert payload["gt_state"]["brake_percentage"] == 0.0
    assert payload["gt_state"]["steering_percentage"] == -12.0
    assert payload["gt_state"]["steering_percentage_frame"] == "apollo_control"
    assert payload["gt_state"]["carla_steering_percentage"] == -12.0
    assert payload["gt_state"]["steering_feedback_sign"] == 1.0
    assert payload["gt_state"]["steering_percentage_cmd"] == 10.0
    assert payload["gt_state"]["carla_steering_percentage_cmd"] == 10.0
    assert payload["gt_state"]["measured_steer_pct"] == -12.0
    assert payload["gt_state"]["measured_steer_deg"] == -8.4


def test_control_apply_trace_prefers_row_snapshot_over_live_control_cache() -> None:
    stats = {
        "last_control_raw": {"raw_control_msg_dump": {"throttle": 0.2, "brake": 0.0}},
        "last_control_in": {"steer": 1.0},
        # Simulate a newer control callback landing while the publish-loop row
        # for the previous command is being serialized.
        "last_control_out": {
            "throttle": 0.3,
            "brake": 0.0,
            "steer": -1.0,
            "steer_before_lateral_guards": -1.0,
            "mapped_throttle_cmd": 0.3,
            "mapped_brake_cmd": 0.0,
            "mapped_carla_steer_cmd": -1.0,
        },
    }

    payload = build_control_apply_trace_payload(
        {
            "ts_sec": 20.0,
            "sim_time": 20.0,
            "apollo_steer_raw": -0.2,
            "commanded_steer_pre_lateral_guards": 0.2,
            "commanded_steer": 0.31,
            "base_mapped_carla_steer_cmd": 0.2,
            "mapped_carla_steer_cmd": 0.31,
            "steer_scale": 1.0,
            "steering_sign": -1.0,
            "carla_steer_applied": 0.31,
            "low_speed_sustained_guard_applied": True,
            "low_speed_sustained_guard_active": True,
            "commanded_throttle": 0.5,
            "throttle_applied": 0.5,
            "commanded_brake": 0.0,
            "brake_applied": 0.0,
        },
        {"throttle": 0.5, "brake": 0.0, "steer": 0.31, "reverse": False, "hand_brake": False},
        stats,
    )

    assert payload["bridge_mapped"]["steer_pre_policy"] == 0.2
    assert payload["bridge_mapped"]["steer"] == 0.31
    assert payload["bridge_mapped"]["mapped_carla_steer_cmd"] == 0.31
    assert payload["bridge_mapped"]["base_mapped_carla_steer_cmd"] == 0.2
    assert payload["bridge_steer_pre_policy"] == 0.2
    assert payload["bridge_steer_mapped"] == 0.31
    assert payload["bridge_steer_base_mapped"] == 0.2
    assert payload["carla_steer_applied"] == 0.31
    assert payload["steer_scale"] == 1.0
    assert payload["steering_sign"] == -1.0
    assert payload["diagnostics"]["low_speed_sustained_guard_applied"] is True
    assert payload["diagnostics"]["low_speed_sustained_guard_active"] is True


def test_control_apply_trace_payload_carries_lateral_semantics_fields() -> None:
    payload = build_control_apply_trace_payload(
        {
            "ts_sec": 30.0,
            "sim_time": 30.0,
            "map_x": 12.0,
            "map_y": -4.0,
            "map_yaw_deg": 91.0,
            "lane_inside": False,
            "lane_dist_m": 0.52,
            "e_y_m": -0.52,
            "e_psi_deg": 5.0,
            "apollo_debug_simple_lat_lateral_error_m": 0.61,
            "apollo_debug_simple_lat_heading_error_rad": 0.08,
            "apollo_debug_simple_lat_lateral_error_rate_mps": 0.2,
            "apollo_debug_simple_lat_heading_error_rate_radps": 0.1,
            "apollo_debug_simple_lat_target_point_kappa": 0.03,
            "apollo_matched_point_distance": 1.5,
            "apollo_target_point_distance": 9.5,
            "apollo_steer_raw": 0.9,
            "commanded_steer": 0.2,
            "carla_steer_applied": 0.2,
        },
        {"throttle": 0.0, "brake": 0.0, "steer": 0.2, "yaw_rate_rps": 0.04},
        {},
    )

    assert payload["map_x"] == 12.0
    assert payload["map_y"] == -4.0
    assert payload["lane_inside"] is False
    assert payload["cross_track_error"] == pytest.approx(-0.52)
    assert payload["heading_error"] == pytest.approx(0.087266, rel=1e-3)
    assert payload["apollo_debug_simple_lat_lateral_error_m"] == pytest.approx(0.61)
    assert payload["apollo_target_point_kappa"] == pytest.approx(0.03)
    assert payload["apollo_matched_point_distance"] == pytest.approx(1.5)
    assert payload["apollo_target_point_distance"] == pytest.approx(9.5)
    assert payload["ego_yaw_rate"] == pytest.approx(0.04)


def test_control_critical_window_recorder_flushes_pre_anchor_and_post_rows() -> None:
    recorder = ControlCriticalWindowRecorder(
        pre_samples=2,
        post_samples=2,
        max_windows=1,
        max_rows=10,
        route_lateral_error_threshold_m=0.5,
        simple_lat_lateral_error_threshold_m=0.5,
        raw_steer_threshold=0.85,
    )

    assert recorder.observe({"timestamp": 1.0, "cross_track_error": 0.1}) == []
    assert recorder.observe({"timestamp": 2.0, "cross_track_error": 0.2}) == []

    rows = recorder.observe({"timestamp": 3.0, "cross_track_error": 0.55})
    assert [row["critical_window_phase"] for row in rows] == ["pre", "pre", "anchor"]
    assert rows[-1]["critical_window_trigger_reason"] == "route_lateral_error_high"
    assert rows[-1]["critical_window_trigger_fields"]["route_lateral_error_m"] == pytest.approx(0.55)

    post_rows = recorder.observe({"timestamp": 4.0, "cross_track_error": 0.4})
    assert [row["critical_window_phase"] for row in post_rows] == ["post"]
    assert post_rows[0]["critical_window_trigger_reason"] == "active_window"
    stats = recorder.stats()
    assert stats["windows_started"] == 1
    assert stats["rows_written"] == 4
    assert stats["trigger_counts"] == {"route_lateral_error_high": 1}


def test_control_critical_window_recorder_is_bounded() -> None:
    recorder = ControlCriticalWindowRecorder(
        pre_samples=5,
        post_samples=5,
        max_windows=2,
        max_rows=3,
        raw_steer_threshold=0.5,
    )

    recorder.observe({"timestamp": 1.0, "apollo_steer_raw": 0.1})
    rows = recorder.observe({"timestamp": 2.0, "apollo_steer_raw": 0.9})

    assert len(rows) == 2
    rows.extend(recorder.observe({"timestamp": 3.0, "apollo_steer_raw": 0.1}))
    assert len(rows) == 3
    assert recorder.observe({"timestamp": 4.0, "apollo_steer_raw": 0.1}) == []
    stats = recorder.stats()
    assert stats["rows_written"] == 3
    assert stats["dropped_rows"] >= 1
