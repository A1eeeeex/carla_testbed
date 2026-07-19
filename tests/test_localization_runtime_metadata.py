from __future__ import annotations

import importlib
import json
import math
import sys
import types
from pathlib import Path

import pytest

from tools.apollo10_cyber_bridge.ros_shims import OdometryShim


def _install_fake_protobuf() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    protobuf = sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
    empty_pb2 = sys.modules.setdefault("google.protobuf.empty_pb2", types.ModuleType("google.protobuf.empty_pb2"))
    setattr(google, "protobuf", protobuf)
    setattr(protobuf, "empty_pb2", empty_pb2)


def _install_fake_carla() -> None:
    sys.modules.setdefault("carla", types.SimpleNamespace(Vehicle=object, Actor=object))


def test_runtime_localization_timestamp_prefers_odom_sim_time() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    odom = OdometryShim()
    odom.header.stamp.sec = 42
    odom.header.stamp.nanosec = 500_000_000

    timestamp_sec, time_base = adapter._localization_time_from_odom(
        odom,
        fallback_wall_time_sec=1234.0,
    )

    assert timestamp_sec == pytest.approx(42.5)
    assert time_base == "sim_time"


def test_runtime_localization_timestamp_can_force_cyber_time_for_diagnostic_ab() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.localization_time_source = "cyber_time"
    odom = OdometryShim()
    odom.header.stamp.sec = 42
    odom.header.stamp.nanosec = 500_000_000

    timestamp_sec, time_base = adapter._localization_time_from_odom(
        odom,
        fallback_wall_time_sec=1234.0,
    )

    assert timestamp_sec == pytest.approx(1234.0)
    assert time_base == "cyber_time_diagnostic"


def test_runtime_localization_timestamp_forced_sim_time_fallback_is_explicit() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.localization_time_source = "sim_time"
    odom = OdometryShim()
    odom.header.stamp = None

    timestamp_sec, time_base = adapter._localization_time_from_odom(
        odom,
        fallback_wall_time_sec=1234.0,
    )

    assert timestamp_sec == pytest.approx(1234.0)
    assert time_base == "sim_time_missing_cyber_time_fallback"


def test_runtime_obstacle_timestamp_can_align_to_cyber_without_changing_localization() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.obstacle_time_source = "cyber_time"
    odom = OdometryShim()
    odom.header.stamp.sec = 42
    odom.header.stamp.nanosec = 500_000_000

    timestamp_sec, time_base = adapter._obstacle_time_from_odom(
        odom,
        localization_header_time_sec=42.5,
        fallback_cyber_time_sec=1234.0,
    )

    assert timestamp_sec == pytest.approx(1234.0)
    assert time_base == "cyber_time_prediction_alignment"


def test_runtime_obstacle_timestamp_defaults_to_localization_header_time() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    odom = OdometryShim()

    timestamp_sec, time_base = adapter._obstacle_time_from_odom(
        odom,
        localization_header_time_sec=42.5,
        fallback_cyber_time_sec=1234.0,
    )

    assert timestamp_sec == pytest.approx(42.5)
    assert time_base == "localization_time"


def test_cyber_time_cannot_be_promoted_to_claim_grade_by_claim_profile() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    enabled, reason = bridge._resolve_claim_grade_enabled(
        {"enabled": False},
        claim_profile_enabled=True,
        localization_time_source="cyber_time",
    )

    assert enabled is False
    assert reason == "cyber_time_diagnostic_not_claim_grade"


def test_explicit_claim_grade_false_wins_over_materialization_profile() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    enabled, reason = bridge._resolve_claim_grade_enabled(
        {"enabled": False},
        claim_profile_enabled=True,
        localization_time_source="sim_time",
    )

    assert enabled is False
    assert reason is None


def test_runtime_rfu_quaternion_decodes_forward_heading() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    qx, qy, qz, qw = bridge._rfu_to_enu_quat_from_heading(0.0)
    assert bridge._decode_rfu_to_enu_heading_from_quat(qx, qy, qz, qw) == pytest.approx(0.0)

    qx, qy, qz, qw = bridge._rfu_to_enu_quat_from_heading(math.pi / 2.0)
    assert bridge._decode_rfu_to_enu_heading_from_quat(qx, qy, qz, qw) == pytest.approx(math.pi / 2.0)


def test_runtime_localization_acceleration_uses_velocity_finite_difference() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(None, 10.0, 1.0, 2.0, 0.0)
    assert (ax, ay, az) == pytest.approx((0.0, 0.0, 0.0))
    assert source == "initial_sample_missing_previous_velocity"

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(
        (10.0, 1.0, 2.0, 0.0),
        10.1,
        1.5,
        2.2,
        0.0,
    )
    assert (ax, ay, az) == pytest.approx((5.0, 2.0, 0.0))
    assert source == "finite_difference"

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(
        (10.1, 1.5, 2.2, 0.0),
        10.1,
        1.5,
        2.2,
        0.0,
    )
    assert (ax, ay, az) == pytest.approx((0.0, 0.0, 0.0))
    assert source == "stale_timestamp_republish"


def test_runtime_localization_acceleration_filter_limits_finite_difference_spikes() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    ax, ay, az, source = bridge._localization_acceleration_from_velocity(
        (10.0, 0.0, 0.0, 0.0),
        10.05,
        0.0,
        0.8,
        0.0,
        previous_acceleration=(0.0, 0.0, 0.0),
        smoothing_alpha=0.35,
        max_abs_mps2=4.0,
        max_delta_mps2=1.0,
    )

    assert (ax, ay, az) == pytest.approx((0.0, 1.0, 0.0))
    assert source == "finite_difference_filtered"


def test_carla_feedback_acceleration_is_converted_from_body_to_apollo_map() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    ax, ay, az = bridge._carla_feedback_acceleration_to_map(
        math.pi / 2.0,
        2.0,
        lateral_accel_mps2=3.0,
    )

    assert (ax, ay, az) == pytest.approx((3.0, 2.0, 0.0))


def test_runtime_localization_acceleration_keeps_replan_speed_nonnegative() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    acceleration, limited, minimum = (
        bridge._limit_forward_acceleration_for_nonnegative_speed_prediction(
            0.001,
            -0.7,
            0.1,
        )
    )

    assert acceleration == pytest.approx(-0.01)
    assert limited is True
    assert minimum == pytest.approx(-0.01)
    assert 0.001 + acceleration * 0.1 == pytest.approx(0.0)


def test_runtime_localization_acceleration_preserves_normal_deceleration() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    acceleration, limited, minimum = (
        bridge._limit_forward_acceleration_for_nonnegative_speed_prediction(
            3.5,
            -0.8,
            0.1,
        )
    )

    assert acceleration == pytest.approx(-0.8)
    assert limited is False
    assert minimum == pytest.approx(-35.0)


def test_runtime_localization_acceleration_constraint_can_be_disabled() -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")

    acceleration, limited, minimum = (
        bridge._limit_forward_acceleration_for_nonnegative_speed_prediction(
            0.001,
            -0.7,
            0.0,
        )
    )

    assert acceleration == pytest.approx(-0.7)
    assert limited is False
    assert minimum is None


def test_authored_initial_state_transition_zeros_only_the_marked_speed_jump(
    tmp_path: Path,
) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    marker_path = tmp_path / "fixed_scene_gate_initial_state_materialization.json"
    marker_path.write_text(
        json.dumps(
            {
                "status": "pass",
                "start_gate": "apollo_planning_ready",
                "official_scenario_timer_started": False,
                "world_frame": 100,
                "world_sim_time_s": 10.0,
                "ego_initial_state_materialization": {
                    "status": "pass",
                    "enabled": True,
                    "expected_ego_initial_speed_mps": 19.44,
                },
            }
        ),
        encoding="utf-8",
    )
    adapter = bridge.ApolloGtBridge.__new__(bridge.ApolloGtBridge)
    adapter.artifacts_dir = tmp_path
    adapter.stats = {
        "localization_authored_initial_state_transition": {"applied_count": 0}
    }
    adapter.localization_authored_initial_state_transition_mode = "marker_zero_once"
    adapter.localization_authored_initial_state_transition_marker_path = marker_path
    adapter.localization_authored_initial_state_transition_required_status = "pass"
    adapter.localization_authored_initial_state_transition_max_world_frame_delta = 2
    adapter.localization_authored_initial_state_transition_max_sim_time_delta_s = 0.11
    adapter.localization_authored_initial_state_transition_speed_tolerance_mps = 0.5
    adapter._localization_authored_initial_state_transition_consumed = False

    observation = adapter._consume_authored_initial_state_acceleration_transition(
        direct_world_frame=101,
        localization_speed_mps=18.99,
        header_ts_sec=11.2,
        raw_acceleration=(379.7, 0.0, 0.0),
        raw_acceleration_source="carla_feedback_physical",
    )

    assert observation is not None
    assert observation["apply"] is True
    assert observation["world_frame_delta"] == 1
    assert observation["speed_delta_mps"] == pytest.approx(0.45)
    assert adapter.stats["localization_authored_initial_state_transition"]["applied_count"] == 1
    artifact = json.loads(
        (tmp_path / "localization_authored_initial_state_transition.json").read_text()
    )
    assert artifact["raw_acceleration_mps2"]["x"] == pytest.approx(379.7)
    assert artifact["claim_boundary"] == (
        "setup_state_discontinuity_only_not_acceleration_filter_or_control_tuning"
    )

    assert (
        adapter._consume_authored_initial_state_acceleration_transition(
            direct_world_frame=102,
            localization_speed_mps=19.1,
            header_ts_sec=11.25,
            raw_acceleration=(0.2, 0.0, 0.0),
            raw_acceleration_source="carla_feedback_physical",
        )
        is None
    )


def test_authored_initial_state_transition_rejects_unmarked_or_late_samples(
    tmp_path: Path,
) -> None:
    _install_fake_protobuf()
    _install_fake_carla()
    bridge = importlib.import_module("tools.apollo10_cyber_bridge.bridge")
    marker_path = tmp_path / "fixed_scene_gate_initial_state_materialization.json"
    marker_path.write_text(
        json.dumps(
            {
                "status": "pass",
                "start_gate": "apollo_planning_ready",
                "official_scenario_timer_started": False,
                "world_frame": 100,
                "world_sim_time_s": 10.0,
                "ego_initial_state_materialization": {
                    "status": "pass",
                    "enabled": True,
                    "expected_ego_initial_speed_mps": 19.44,
                },
            }
        ),
        encoding="utf-8",
    )

    same_frame = bridge._authored_initial_state_transition_marker_status(
        marker_path,
        direct_world_frame=100,
        localization_speed_mps=19.44,
    )
    late = bridge._authored_initial_state_transition_marker_status(
        marker_path,
        direct_world_frame=103,
        localization_speed_mps=19.44,
    )
    wrong_speed = bridge._authored_initial_state_transition_marker_status(
        marker_path,
        direct_world_frame=101,
        localization_speed_mps=15.0,
    )
    sim_time_fallback = bridge._authored_initial_state_transition_marker_status(
        marker_path,
        direct_world_frame=None,
        localization_timestamp_sec=10.05,
        localization_speed_mps=19.44,
    )

    assert same_frame["apply"] is False
    assert same_frame["reason"] == "waiting_for_post_materialization_frame"
    assert late["apply"] is False
    assert late["reason"] == "post_materialization_frame_window_expired"
    assert wrong_speed["apply"] is False
    assert wrong_speed["reason"] == "waiting_for_authored_initial_speed"
    assert sim_time_fallback["apply"] is True
    assert sim_time_fallback["boundary_source"] == "sim_time"
    assert sim_time_fallback["sim_time_delta_s"] == pytest.approx(0.05)
