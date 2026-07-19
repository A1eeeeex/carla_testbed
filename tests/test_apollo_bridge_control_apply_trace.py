from __future__ import annotations

import importlib
import json
import sys
import threading
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


def test_bridge_physical_steering_feedback_uses_apollo_front_wheel_angle_domain() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.seq = 0
    adapter.chassis_pb2 = _ChassisPb2
    adapter.steer_sign = -1.0
    adapter.actuator_mapping_mode = "physical"
    adapter.physical_map_steering = True
    adapter.physical_map_steering_feedback = True
    adapter.physical_apollo_max_steer_angle_deg = 30.0
    adapter.stats = {
        "last_control_out": {
            "steer": -0.25,
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
            "steer": -0.25,
            "steer_feedback_pct": -25.0,
            "steer_feedback_deg": -15.0,
            "throttle": 0.0,
            "brake": 0.0,
        },
    )

    assert chassis.steering_percentage == pytest.approx(50.0)
    feedback = adapter.stats["last_control_feedback"]
    assert feedback["measured"]["steer_pct"] == pytest.approx(50.0)
    assert feedback["measured"]["carla_steer_pct"] == pytest.approx(-25.0)
    assert (
        feedback["measured"]["steering_feedback_normalization"]
        == "apollo_front_wheel_angle_percent"
    )
    assert feedback["chassis"]["steering_feedback_apollo_max_steer_angle_deg"] == 30.0


def test_startup_control_publish_gate_latches_after_first_valid_planning() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.require_valid_planning_before_first_publish = True
    adapter._valid_planning_publish_gate_open = False
    adapter.stats = {}

    assert not adapter._allow_control_publish_after_startup(
        planning_valid=False,
        planning_reason="zero_trajectory_points",
        timestamp_sec=1.0,
    )
    gate = adapter.stats["control_startup_publish_gate"]
    assert gate["skip_count"] == 1
    assert gate["last_skip_reason"] == "zero_trajectory_points"
    assert not gate["open"]

    assert adapter._allow_control_publish_after_startup(
        planning_valid=True,
        planning_reason="",
        timestamp_sec=1.1,
    )
    assert gate["open"]
    assert gate["first_open_timestamp_sec"] == pytest.approx(1.1)

    assert adapter._allow_control_publish_after_startup(
        planning_valid=False,
        planning_reason="trajectory_all_points_expired",
        timestamp_sec=1.2,
    )
    assert gate["skip_count"] == 1
    assert gate["open"]


def test_startup_control_publish_gate_disabled_does_not_suppress() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.require_valid_planning_before_first_publish = False
    adapter._valid_planning_publish_gate_open = True
    adapter.stats = {}

    assert adapter._allow_control_publish_after_startup(
        planning_valid=False,
        planning_reason="no_recent_planning_message",
        timestamp_sec=1.0,
    )
    assert adapter.stats["control_startup_publish_gate"]["skip_count"] == 0


def test_startup_control_publish_gate_waits_for_fixed_scene_handover(tmp_path) -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.require_valid_planning_before_first_publish = True
    adapter._valid_planning_publish_gate_open = False
    adapter.require_fixed_scene_handover_before_publish = True
    adapter.fixed_scene_handover_path = tmp_path / "fixed_scene_ego_handover.json"
    adapter.stats = {}

    assert not adapter._allow_control_publish_after_startup(
        planning_valid=True,
        planning_reason="",
        timestamp_sec=1.0,
    )
    gate = adapter.stats["control_startup_publish_gate"]
    assert gate["planning_open"] is False
    assert gate["fixed_scene_handover_open"] is False
    assert gate["last_skip_reason"] == "fixed_scene_handover_not_ready"

    adapter.fixed_scene_handover_path.write_text(
        json.dumps({"status": "ready"}), encoding="utf-8"
    )
    assert not adapter._allow_control_publish_after_startup(
        planning_valid=False,
        planning_reason="trajectory_all_points_expired",
        timestamp_sec=1.1,
    )
    assert gate["open"] is False
    assert gate["fixed_scene_handover_open"] is True
    assert gate["last_skip_reason"] == "trajectory_all_points_expired"

    assert adapter._allow_control_publish_after_startup(
        planning_valid=True,
        planning_reason="",
        timestamp_sec=1.2,
    )
    assert gate["open"] is True
    assert gate["first_open_timestamp_sec"] == pytest.approx(1.2)


def test_first_fixed_scene_control_publish_writes_immediate_handover_ack(
    tmp_path,
) -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.require_fixed_scene_handover_before_publish = True
    adapter.fixed_scene_handover_path = tmp_path / "fixed_scene_ego_handover.json"
    adapter.fixed_scene_control_handover_ack_path = (
        tmp_path / "fixed_scene_control_handover_ack.json"
    )
    adapter._fixed_scene_control_handover_ack_written = False
    adapter.stats = {"control_tx_count": 1}
    adapter.fixed_scene_handover_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "world_frame": 120,
                "world_sim_time_s": 6.0,
            }
        ),
        encoding="utf-8",
    )

    adapter._write_fixed_scene_control_handover_ack(
        control_timestamp_sec=6.05,
        control_sequence_num=42,
        planning_sequence_num=18,
        planning_trajectory_type="NORMAL",
        planning_exact_match=True,
    )

    ack = json.loads(adapter.fixed_scene_control_handover_ack_path.read_text())
    assert ack["status"] == "published"
    assert ack["control_tx_count"] == 1
    assert ack["control_timestamp_sec"] == pytest.approx(6.05)
    assert ack["control_header_sequence_num"] == 42
    assert ack["planning_sequence_num"] == 18
    assert ack["planning_trajectory_type"] == "NORMAL"
    assert ack["planning_exact_match"] is True
    assert ack["handover_world_frame"] == 120
    assert ack["handover_world_sim_time_s"] == pytest.approx(6.0)

    adapter.stats["control_tx_count"] = 2
    adapter._write_fixed_scene_control_handover_ack(
        control_timestamp_sec=6.10,
        control_sequence_num=43,
        planning_sequence_num=19,
        planning_trajectory_type="NORMAL",
        planning_exact_match=True,
    )
    unchanged = json.loads(adapter.fixed_scene_control_handover_ack_path.read_text())
    assert unchanged["control_tx_count"] == 1
    assert unchanged["control_header_sequence_num"] == 42


def test_startup_planning_publish_validity_requires_exact_nonfallback_sequence() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.require_exact_planning_match_before_first_publish = True
    adapter.require_nonfallback_planning_before_first_publish = True
    normal = {
        "planning_header_sequence_num": 10,
        "trajectory_type": "NORMAL",
    }

    missing_exact = adapter._startup_planning_publish_validity(
        planning_valid=True,
        planning_reason="",
        control_input_sequence_num=11,
        exact_planning=None,
        effective_planning=normal,
    )
    assert missing_exact["valid"] is False
    assert missing_exact["reason"] == "control_input_planning_sequence_not_observed"

    fallback = adapter._startup_planning_publish_validity(
        planning_valid=True,
        planning_reason="",
        control_input_sequence_num=11,
        exact_planning={"trajectory_type": "SPEED_FALLBACK"},
        effective_planning=normal,
    )
    assert fallback["valid"] is False
    assert fallback["reason"] == "fallback_or_unknown_planning_trajectory"

    ready = adapter._startup_planning_publish_validity(
        planning_valid=True,
        planning_reason="",
        control_input_sequence_num=12,
        exact_planning={"trajectory_type": "NORMAL"},
        effective_planning=normal,
    )
    assert ready == {
        "valid": True,
        "reason": "",
        "sequence_num": 12,
        "trajectory_type": "NORMAL",
        "exact_match": True,
    }


def test_startup_exact_planning_retry_replays_only_matching_fresh_sequence() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter._startup_pending_control_lock = threading.Lock()
    adapter._startup_pending_controls = {}
    adapter.stats = {"control_startup_publish_gate": {"open": False}}
    adapter._command_now_sec = lambda: 10.2
    calls = []

    class FakeControl:
        def __init__(self) -> None:
            self.value = 0

        def CopyFrom(self, other) -> None:
            self.value = other.value

    cmd = FakeControl()
    cmd.value = 7
    adapter._on_control_cmd = lambda replayed, **kwargs: calls.append(
        (replayed.value, kwargs)
    )
    adapter._queue_control_for_exact_planning_retry(
        cmd,
        planning_sequence_num=42,
        control_rx_ts=10.0,
    )
    cmd.value = 99

    adapter._retry_control_for_observed_planning(41)
    assert calls == []
    assert adapter.stats["control_startup_publish_gate"]["pending_control_sequence_nums"] == [42]

    adapter._retry_control_for_observed_planning(42)
    assert calls == [
        (
            7,
            {
                "_startup_exact_retry": True,
                "_original_control_rx_ts": 10.0,
            },
        )
    ]
    gate = adapter.stats["control_startup_publish_gate"]
    assert gate["exact_match_retry_count"] == 1
    assert gate["last_exact_match_retry_sequence_num"] == 42
    assert gate["last_exact_match_retry_age_sec"] == pytest.approx(0.2)
    assert gate["last_exact_match_retry_status"] == "re_evaluating"


def test_startup_exact_planning_retry_drops_expired_command() -> None:
    _install_fake_runtime_modules()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter._startup_pending_control_lock = threading.Lock()
    adapter._startup_pending_controls = {}
    adapter.stats = {"control_startup_publish_gate": {"open": False}}
    adapter._command_now_sec = lambda: 20.0
    adapter._on_control_cmd = lambda *_args, **_kwargs: pytest.fail(
        "expired Control must not be replayed"
    )
    adapter._queue_control_for_exact_planning_retry(
        object(),
        planning_sequence_num=43,
        control_rx_ts=19.0,
    )

    adapter._retry_control_for_observed_planning(43)

    gate = adapter.stats["control_startup_publish_gate"]
    assert gate["pending_control_expired_count"] == 1
    assert gate["last_exact_match_retry_status"] == "expired"
    assert gate.get("exact_match_retry_count", 0) == 0


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
            "actuator_mapping_speed_mps": 7.3,
            "decel_actuation_mapping_source": "physical_inverse_decel_high_clamp",
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
    assert payload["bridge_mapped"]["actuator_mapping_speed_mps"] == 7.3
    assert payload["bridge_mapped"]["decel_actuation_mapping_source"] == (
        "physical_inverse_decel_high_clamp"
    )
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
