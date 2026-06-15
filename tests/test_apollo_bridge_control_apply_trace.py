from __future__ import annotations

from tools.apollo10_cyber_bridge.control_apply_trace import build_control_apply_trace_payload


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
            "mapped_throttle_cmd": 0.42,
            "mapped_brake_cmd": 0.0,
            "mapped_carla_steer_cmd": 0.025,
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
    assert payload["bridge_mapped"]["mapped_carla_steer_cmd"] == 0.025
    assert payload["throttle_mapped"] == 0.42
    assert payload["brake_mapped"] == 0.0
    assert payload["bridge_steer_mapped"] == 0.025
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
    assert payload["gt_state"]["sim_time_sec"] == 1.0
    assert payload["gt_state"]["world_frame"] == 123.0
    assert payload["gt_state"]["sample_reason"] == "fresh_sample"
    assert payload["gt_state"]["fresh_sample"] is True
    assert payload["gt_state"]["age_wall_ms"] == 25.0
    assert payload["gt_state"]["age_sim_ms"] == 0.0
    assert payload["gt_state"]["same_world_frame_control_cycle_streak"] == 3.0
