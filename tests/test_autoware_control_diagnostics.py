import json
from pathlib import Path

from carla_testbed.analysis.autoware_control_diagnostics import (
    analyze_autoware_control_run,
    write_autoware_control_diagnostics,
)


def _longitudinal_diagnostic_row(
    *,
    target_velocity: float,
    nearest_velocity: float,
    control_state: int = 0,
    stop_distance: float = 10.0,
    smooth_stop_mode: float = 0.0,
) -> dict:
    data = [0.0] * 37
    data[1] = nearest_velocity
    data[2] = target_velocity
    data[4] = nearest_velocity
    data[13] = float(control_state)
    data[28] = stop_distance
    data[36] = smooth_stop_mode
    return {"stamp": {"sec": 1, "nanosec": 0}, "data": data}


def test_autoware_control_diagnostics_flags_low_control_target():
    report = analyze_autoware_control_run("tests/fixtures/autoware/low_control_run")

    assert report["schema_version"] == "autoware_control_diagnostics.v1"
    assert report["verdict"]["status"] == "warn"
    assert "control_target_low_despite_planning_speed" in report["verdict"]["failure_reasons"]
    assert "operation_mode_not_autonomous_available" in report["verdict"]["failure_reasons"]
    assert "operation_mode_kinematics_timeout" in report["verdict"]["failure_reasons"]
    assert "operation_mode_control_cmd_timeout" in report["verdict"]["failure_reasons"]
    assert "operation_mode_change_rejected" in report["verdict"]["failure_reasons"]
    assert "mixed_time_domain_between_kinematics_and_control" in report["verdict"]["failure_reasons"]
    assert report["relayed_planning"]["velocity_max_mps"]["max"] == 4.17
    assert report["gated_control"]["target_velocity_mps"]["max"] == 0.31
    assert report["launch_log_signals"]["kinematics_timeout_count"] == 1
    assert report["launch_log_signals"]["control_cmd_timeout_count"] == 1
    assert report["launch_log_signals"]["operation_mode_change_rejected_count"] == 1
    assert report["time_domains"]["kinematic_state"]["stamp_domain"] == "sim_time"
    assert report["time_domains"]["trajectory_follower_control"]["stamp_domain"] == "wall_time"
    assert "nearest_trajectory_sample_velocity_low" in report["verdict"]["failure_reasons"]
    assert report["trajectory_ego_alignment"]["nearest_sample_velocity_mps"]["max"] == 0.25
    assert report["trajectory_ego_alignment"]["nearest_sample_index"]["last"] == 30.0
    assert (
        report["trajectory_ego_alignment"]["near_field_velocity_profile"]["0_0p5m"][
            "max_velocity_mps"
        ]["max"]
        == 0.25
    )
    assert (
        report["trajectory_ego_alignment"]["near_field_velocity_profile"]["1_2m"][
            "max_velocity_mps"
        ]["max"]
        == 1.3
    )
    assert (
        report["trajectory_ego_alignment"]["first_sample_ge_velocity_threshold"]["0p5"][
            "distance_m"
        ]["last"]
        == 1.0
    )
    assert (
        report["trajectory_ego_alignment"]["first_sample_ge_velocity_threshold"]["2"][
            "distance_m"
        ]["last"]
        is None
    )
    assert report["bridge_control"]["apply_log_rows"] == 3
    assert report["bridge_control"]["source_counts"] == {"pending": 3}
    assert report["bridge_control"]["watchdog_rows"] == 0
    assert report["bridge_control"]["watchdog_after_first_control_count"] == 0
    assert report["bridge_control"]["speed_sources"] == ["ros_velocity_status"]
    assert report["bridge_control"]["throttle_brake_switch_count"] == 2
    assert report["bridge_control"]["current_speed_mps"]["max"] == 0.8
    assert report["vehicle_speed_response"]["positive_command_count"] == 2
    assert report["vehicle_speed_response"]["commanded_positive_acceleration_mps2"]["mean"] == 0.49
    assert (
        report["vehicle_speed_response"]["actual_acceleration_when_positive_command_mps2"]["mean"]
        == 0.1
    )
    assert "actuation_accel_underresponse_candidate" in report["verdict"]["failure_reasons"]
    assert "system_operation_mode_state" in report["missing_inputs"]


def test_autoware_control_diagnostics_flags_behavior_planning_crash(tmp_path):
    run = tmp_path / "run"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "AUTOWARE_PRE_ENGAGE_READINESS_FAILED",
                "fail_reason": "AUTOWARE_PRE_ENGAGE_READINESS_FAILED",
            }
        )
    )
    (artifacts / "autoware_launch.stdout").write_text(
        "\n".join(
            [
                "[component_container_mt-26]   what():  failed to add guard condition to wait set: guard condition implementation is invalid",
                "[ERROR] [component_container_mt-26]: process has died [pid 885, exit code -6, cmd '/opt/ros/humble/lib/rclcpp_components/component_container_mt --ros-args -r __node:=behavior_planning_container']",
            ]
        )
    )

    report = analyze_autoware_control_run(run)

    assert "behavior_planning_container_crash" in report["verdict"]["failure_reasons"]
    assert "ros2_guard_condition_failure" in report["verdict"]["failure_reasons"]
    assert report["launch_log_signals"]["behavior_planning_container_crash_count"] == 1
    assert report["launch_log_signals"]["guard_condition_failure_count"] == 1


def test_autoware_control_diagnostics_writes_json_and_markdown(tmp_path):
    report = analyze_autoware_control_run("tests/fixtures/autoware/low_control_run")
    write_autoware_control_diagnostics(report, tmp_path)

    json_path = tmp_path / "autoware_control_diagnostics.json"
    md_path = tmp_path / "autoware_control_diagnostics.md"
    assert json_path.exists()
    assert md_path.exists()
    parsed = json.loads(json_path.read_text())
    assert parsed["verdict"]["status"] == "warn"
    md = md_path.read_text()
    assert "planning_velocity_max_mps" in md
    assert "requested_planning_common_max_mps" in md
    assert "current_max_velocity_topic_max_mps" in md
    assert "velocity_limit_topic_message_count" in md
    assert "kinematics_timeout_count" in md
    assert "kinematic_stamp_domain" in md
    assert "nearest_trajectory_sample_velocity_max_mps" in md
    assert "first_high_velocity_sample_distance_max_m" in md
    assert "first_velocity_ge_0p5_distance_last_m" in md
    assert "near_field_0_0p5m_max_velocity_max_mps" in md
    assert "near_field_1_2m_max_velocity_max_mps" in md
    assert "near_field_2_5m_max_velocity_max_mps" in md
    assert "commanded_positive_acceleration_mean_mps2" in md
    assert "actual_acceleration_when_positive_command_mean_mps2" in md
    assert "actual_over_commanded_positive_acceleration_ratio" in md
    assert "bridge_throttle_brake_switch_count" in md
    assert "bridge_current_minus_target_speed_max_mps" in md
    assert "bridge_throttle_while_overspeed_gt_threshold_rows" in md
    assert "bridge_watchdog_rows" in md
    assert "bridge_watchdog_after_first_control_count" in md
    assert "applied_throttle_brake_switch_count" in md
    assert "applied_throttle_max" in md
    assert "applied_brake_max" in md
    assert "debug_resampled_reference_message_count" in md
    assert "debug_resampled_reference_velocity_max_mps" in md
    assert "debug_resampled_reference_nearest_velocity_max_mps" in md
    assert "debug_resampled_reference_first_high_distance_last_m" in md
    assert "debug_resampled_reference_near_field_1_2m_max_velocity_max_mps" in md
    assert "debug_predicted_frenet_message_count" in md
    assert "debug_predicted_frenet_velocity_max_mps" in md
    assert "debug_predicted_frenet_nearest_velocity_max_mps" in md
    assert "debug_predicted_frenet_first_high_distance_last_m" in md
    assert "debug_predicted_frenet_near_field_1_2m_max_velocity_max_mps" in md
    assert "lateral_predicted_trajectory_message_count" in md
    assert "lateral_predicted_trajectory_velocity_max_mps" in md
    assert "lateral_predicted_trajectory_nearest_velocity_max_mps" in md
    assert "lateral_predicted_trajectory_first_high_distance_last_m" in md
    assert "lateral_predicted_trajectory_near_field_1_2m_max_velocity_max_mps" in md
    assert "longitudinal_diagnostic_message_count" in md
    assert "longitudinal_diag_target_velocity_max_mps" in md
    assert "longitudinal_diag_nearest_velocity_max_mps" in md
    assert "longitudinal_diag_control_state_counts" in md
    assert "longitudinal_diag_stop_distance_last_m" in md
    assert "longitudinal_diag_smooth_stop_mode_last" in md


def test_autoware_control_diagnostics_classifies_startup_only_control_timeout(tmp_path):
    run_dir = tmp_path / "startup_timeout_only"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 1.0}))

    control_row = {
        "stamp": {"sec": 20, "nanosec": 0},
        "_node_stamp": 10.0,
        "longitudinal": {"velocity": 1.0, "acceleration": 0.2},
        "lateral": {"steering_tire_angle": 0.0},
    }
    trajectory_row = {
        "stamp": {"sec": 20, "nanosec": 0},
        "trajectory_point_count": 3,
        "trajectory_velocity_mps": {"min": 0.0, "max": 1.2, "mean": 0.8, "first": 0.5, "last": 0.0},
        "first_nonzero_velocity_index": 0,
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 1.0}
        ],
    }
    operation_row = {
        "stamp": {"sec": 20, "nanosec": 0},
        "_node_stamp": 10.0,
        "mode": 2,
        "is_autoware_control_enabled": True,
        "is_autonomous_mode_available": True,
    }
    kinematic_row = {
        "stamp": {"sec": 20, "nanosec": 0},
        "pose": {"x": 0.0, "y": 0.0, "z": 0.0},
    }
    for name, row in {
        "ros2_topic__planning__scenario_planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "ros2_topic__control__trajectory_follower__controller_node_exe__debug__resampled_reference_trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__controller_node_exe__debug__predicted_trajectory_in_frenet_coordinate.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__lateral__predicted_trajectory.jsonl": trajectory_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__control__vehicle_cmd_gate__operation_mode.jsonl": operation_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")
    (artifacts / "ros2_topic__diagnostics.jsonl").write_text(
        json.dumps(
            {
                "stamp": {"sec": 20, "nanosec": 0},
                "status": [
                    {
                        "name": "/autoware/control/topic_rate_check/control_command",
                        "level": 0,
                        "message": "OK",
                    }
                ],
            }
        )
        + "\n"
    )
    (artifacts / "autoware_launch.stdout").write_text(
        "[component_container_mt-33] [WARN] [2.0] "
        "[control.autoware_operation_mode_transition_manager]: "
        "Subscribed control_cmd is timed out.\n"
        "[logging_node-15] [WARN] [3.0] [logging_diag_graph]: "
        "The target mode is not available for the following reasons:\n"
        "[logging_node-15] - /autoware/modes/autonomous ERROR\n"
        "[logging_node-15]     - /autoware/control ERROR\n"
        "[logging_node-15]         - /autoware/control/topic_rate_check/control_command STALE\n"
        "[logging_node-15] [INFO] [4.0] [logging_diag_graph]: The target mode is available now.\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert report["launch_log_signals"]["control_cmd_timeout_count"] == 1
    assert report["launch_log_signals"]["control_cmd_timeout_count_before_first_control_count"] == 1
    assert report["launch_log_signals"]["control_cmd_timeout_count_after_first_control_count"] == 0
    assert report["launch_log_signals"]["gear_command_missing_count"] == 0
    assert report["diagnostic_graph_log"]["not_available_block_count"] == 1
    assert report["diagnostic_graph_log"]["not_available_after_first_control_count"] == 0
    assert report["diagnostic_graph_log"]["available_now_count"] == 1
    assert report["diagnostics"]["status_detail_count"] == 1
    assert report["diagnostics"]["counts_by_level"]["ok"] == 1
    assert report["controller_debug_topics"]["resampled_reference_trajectory"]["message_count"] == 1
    assert (
        report["controller_debug_alignment"]["resampled_reference_trajectory"][
            "nearest_sample_velocity_mps"
        ]["max"]
        == 1.0
    )
    assert (
        report["controller_debug_topics"]["predicted_trajectory_in_frenet_coordinate"][
            "message_count"
        ]
        == 1
    )
    assert report["controller_debug_topics"]["lateral_predicted_trajectory"]["message_count"] == 1
    assert "operation_mode_control_cmd_timeout" not in report["verdict"]["failure_reasons"]
    assert "operation_mode_autonomous_availability_unstable" not in report["verdict"]["failure_reasons"]
    assert any("only before the first captured control command" in w for w in report["warnings"])


def test_autoware_control_diagnostics_reports_bridge_watchdog(tmp_path):
    run_dir = tmp_path / "watchdog_bridge"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 0.0}))
    (artifacts / "autoware_carla_control_bridge.log").write_text(
        "[INFO] [1.0] [carla_control_bridge]: apply frame=10 source=pending "
        "target_speed=0.250 accel=0.500 current_speed=0.000 speed_error=0.250 "
        "speed_source=ros_velocity_status longitudinal_mode=speed_feedback throttle=0.500 brake=0.000\n"
        "[INFO] [2.0] [carla_control_bridge]: apply frame=11 source=watchdog "
        "target_speed=null accel=null current_speed=null speed_error=null "
        "speed_source=null longitudinal_mode=speed_feedback throttle=0.000 brake=1.000\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert report["bridge_control"]["source_counts"] == {"pending": 1, "watchdog": 1}
    assert report["bridge_control"]["watchdog_rows"] == 1
    assert report["bridge_control"]["watchdog_before_first_control_count"] == 1
    assert report["bridge_control"]["watchdog_after_first_control_count"] == 0
    assert any("only before the first captured control command" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_flags_applied_control_oscillation(tmp_path):
    run_dir = tmp_path / "applied_control_oscillation"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 6.0}))
    (run_dir / "timeseries.csv").write_text(
        "frame_id,sim_time,throttle_applied,brake_applied,carla_steer_applied\n"
        + "\n".join(
            f"{idx},{idx * 0.05},{0.8 if idx % 2 == 0 else 0.0},{0.0 if idx % 2 == 0 else 0.6},0.0"
            for idx in range(14)
        )
        + "\n"
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 2,
        "trajectory_velocity_mps": {"min": 2.0, "max": 4.0, "mean": 3.0, "first": 2.0, "last": 4.0},
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 2.0},
            {"index": 1, "pose": {"x": 1.0, "y": 0.0}, "longitudinal_velocity_mps": 4.0},
        ],
    }
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 2.0, "acceleration": 0.2},
        "lateral": {"steering_tire_angle": 0.0},
    }
    kinematic_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "pose": {"x": 0.0, "y": 0.0},
        "twist": {"linear": {"x": 1.0, "y": 0.0, "z": 0.0}},
    }
    for name, row in {
        "ros2_topic__planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["applied_control"]["throttle_brake_switch_count"] == 13
    assert report["applied_control"]["throttle"]["max"] == 0.8
    assert report["applied_control"]["brake"]["max"] == 0.6
    assert "applied_actuation_oscillation_candidate" in report["verdict"]["failure_reasons"]
    assert any("applied control alternates" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_flags_bridge_throttle_above_target(tmp_path):
    run_dir = tmp_path / "bridge_throttle_above_target"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 6.5}))
    bridge_lines = []
    for frame in range(12):
        bridge_lines.append(
            "[INFO] [10.0] [carla_control_bridge]: "
            f"apply frame={frame} source=pending speed_source=ros_velocity_status "
            "longitudinal_mode=speed_feedback target_speed=1.000 accel=0.800 "
            "current_speed=4.500 throttle=0.650 brake=0.000"
        )
    (artifacts / "autoware_carla_control_bridge.log").write_text("\n".join(bridge_lines) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["bridge_control"]["current_minus_target_speed_mps"]["max"] == 3.5
    assert report["bridge_control"]["overspeed_gt_threshold_rows"] == 12
    assert report["bridge_control"]["throttle_while_overspeed_gt_threshold_rows"] == 12
    assert report["bridge_control"]["positive_accel_while_overspeed_gt_threshold_rows"] == 12
    assert "bridge_throttle_while_above_target_candidate" in report["verdict"]["failure_reasons"]
    assert any("well above Autoware target speed" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_does_not_parse_config_throttle_as_output(tmp_path):
    run_dir = tmp_path / "bridge_config_throttle_token"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 3.0}))
    (artifacts / "autoware_carla_control_bridge.log").write_text(
        "[INFO] [10.0] [carla_control_bridge]: "
        "apply frame=1 source=pending target_speed=1.000 accel=0.800 "
        "current_speed=4.500 speed_error=-3.500 "
        "positive_accel_min_throttle=0.650 throttle=0.000 brake=0.500\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert report["bridge_control"]["throttle"]["max"] == 0.0
    assert report["bridge_control"]["brake"]["max"] == 0.5
    assert report["bridge_control"]["overspeed_gt_threshold_rows"] == 1
    assert report["bridge_control"]["throttle_while_overspeed_gt_threshold_rows"] == 0
    assert "bridge_throttle_while_above_target_candidate" not in report["verdict"]["failure_reasons"]


def test_autoware_control_diagnostics_flags_planning_ceiling_below_acceptance_speed(tmp_path):
    run_dir = tmp_path / "planning_velocity_ceiling"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "max_speed_mps": 4.8,
                "acceptance": {
                    "checks": {
                        "ego_motion": {
                            "threshold": 5.0,
                        }
                    }
                },
            }
        )
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 3,
        "trajectory_velocity_mps": {
            "min": 0.25,
            "max": 4.17,
            "mean": 2.0,
            "first": 0.25,
            "last": 0.0,
        },
        "first_nonzero_velocity_index": 0,
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 1.0, "y": 0.0}, "longitudinal_velocity_mps": 4.17},
        ],
    }
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 4.0, "acceleration": 0.2},
        "lateral": {"steering_tire_angle": 0.0},
    }
    for name, row in {
        "ros2_topic__planning__scenario_planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["verdict"]["expected_motion_speed_mps"] == 5.0
    assert (
        "planning_velocity_ceiling_below_acceptance_speed"
        in report["verdict"]["failure_reasons"]
    )
    assert any("trajectory never reaches" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_flags_planning_to_control_speed_compression(tmp_path):
    run_dir = tmp_path / "planning_to_control_speed_compression"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "max_speed_mps": 4.9,
                "acceptance": {
                    "checks": {
                        "ego_motion": {
                            "threshold": 5.0,
                        }
                    }
                },
            }
        )
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 4,
        "trajectory_velocity_mps": {
            "min": 0.25,
            "max": 8.0,
            "mean": 4.0,
            "first": 0.25,
            "last": 0.0,
        },
        "first_nonzero_velocity_index": 0,
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 2.0, "y": 0.0}, "longitudinal_velocity_mps": 2.7},
            {"index": 2, "pose": {"x": 10.0, "y": 0.0}, "longitudinal_velocity_mps": 8.0},
        ],
    }
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 2.7, "acceleration": 0.8},
        "lateral": {"steering_tire_angle": 0.0},
    }
    kinematic_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "pose": {"x": 2.0, "y": 0.0},
        "twist": {"linear": {"x": 2.0, "y": 0.0, "z": 0.0}},
    }
    for name, row in {
        "ros2_topic__planning__scenario_planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["verdict"]["planning_control_speed_gap_mps"] == 5.3
    assert report["verdict"]["control_target_expected_speed_margin_mps"] == 2.3
    assert "control_target_below_expected_motion_speed" in report["verdict"]["failure_reasons"]
    assert "planning_to_control_velocity_compression" in report["verdict"]["failure_reasons"]
    assert "planning_velocity_ceiling_below_acceptance_speed" not in report["verdict"]["failure_reasons"]
    assert any("planning-to-control" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_reports_requested_speed_ceiling_and_velocity_limits(tmp_path):
    run_dir = tmp_path / "planning_requested_speed_ceiling"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 13.5}))
    (artifacts / "autoware_planning_common_override.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "status": "applied",
                "original_max_vel_mps": 4.17,
                "requested_max_vel_mps": 22.22,
            }
        )
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 3,
        "trajectory_velocity_mps": {
            "min": 0.25,
            "max": 13.888889,
            "mean": 8.0,
            "first": 0.25,
            "last": 13.888889,
        },
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 1.0, "y": 0.0}, "longitudinal_velocity_mps": 13.888889},
        ],
    }
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 13.5, "acceleration": 0.2},
        "lateral": {"steering_tire_angle": 0.0},
    }
    for name, row in {
        "ros2_topic__planning__scenario_planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl": trajectory_row,
        "ros2_topic__planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")
    (artifacts / "ros2_topic__planning__scenario_planning__current_max_velocity_probe.jsonl").write_text(
        json.dumps({"stamp": {"sec": 1, "nanosec": 0}, "max_velocity": 13.888889}) + "\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert report["verdict"]["requested_planning_common_max_mps"] == 22.22
    assert round(report["verdict"]["requested_planning_common_speed_gap_mps"], 6) == round(
        22.22 - 13.888889, 6
    )
    assert "planning_velocity_below_requested_common_max" in report["verdict"]["failure_reasons"]
    assert (
        "planning_velocity_matches_lanelet_default_urban_speed_candidate"
        in report["verdict"]["failure_reasons"]
    )
    assert "velocity_limit_topics_missing" not in report["verdict"]["failure_reasons"]
    assert report["velocity_limits"]["current_max_velocity"]["message_count"] == 1
    assert report["velocity_limits"]["current_max_velocity"]["max_velocity_mps"]["max"] == 13.888889


def test_autoware_control_diagnostics_flags_missing_velocity_limit_topics(tmp_path):
    run_dir = tmp_path / "planning_requested_speed_ceiling_without_limit_topics"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 13.5}))
    (artifacts / "autoware_planning_common_override.json").write_text(
        json.dumps({"enabled": True, "status": "applied", "requested_max_vel_mps": 22.22})
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 1,
        "trajectory_velocity_mps": {
            "min": 13.888889,
            "max": 13.888889,
            "mean": 13.888889,
            "first": 13.888889,
            "last": 13.888889,
        },
    }
    for name in (
        "ros2_topic__planning__scenario_planning__trajectory.jsonl",
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl",
        "ros2_topic__planning__trajectory.jsonl",
    ):
        (artifacts / name).write_text(json.dumps(trajectory_row) + "\n")
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 13.5, "acceleration": 0.0},
    }
    (artifacts / "autoware_control.jsonl").write_text(json.dumps(control_row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert "planning_velocity_below_requested_common_max" in report["verdict"]["failure_reasons"]
    assert "velocity_limit_topics_missing" in report["verdict"]["failure_reasons"]
    assert (
        "planning_velocity_matches_lanelet_default_urban_speed_candidate"
        in report["verdict"]["failure_reasons"]
    )


def test_autoware_control_diagnostics_reads_new_common_override_schema(tmp_path):
    run_dir = tmp_path / "planning_requested_speed_ceiling_new_schema"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 13.5}))
    (artifacts / "autoware_planning_common_override.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "status": "applied",
                "original_values": {"max_vel": 4.17},
                "requested_values": {
                    "max_vel": 22.22,
                    "normal.max_acc": 2.5,
                    "normal.min_acc": -3.0,
                },
            }
        )
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 1,
        "trajectory_velocity_mps": {
            "min": 13.888889,
            "max": 13.888889,
            "mean": 13.888889,
            "first": 13.888889,
            "last": 13.888889,
        },
    }
    for name in (
        "ros2_topic__planning__scenario_planning__trajectory.jsonl",
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl",
        "ros2_topic__planning__trajectory.jsonl",
    ):
        (artifacts / name).write_text(json.dumps(trajectory_row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["verdict"]["requested_planning_common_max_mps"] == 22.22
    assert "planning_velocity_below_requested_common_max" in report["verdict"]["failure_reasons"]


def test_autoware_control_diagnostics_flags_planning_below_current_velocity_limit(tmp_path):
    run_dir = tmp_path / "planning_below_current_velocity_limit"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 12.0}))
    (artifacts / "autoware_planning_common_override.json").write_text(
        json.dumps({"enabled": True, "status": "applied", "requested_max_vel_mps": 22.22})
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 1,
        "trajectory_velocity_mps": {
            "min": 13.888889,
            "max": 13.888889,
            "mean": 13.888889,
            "first": 13.888889,
            "last": 13.888889,
        },
    }
    for name in (
        "ros2_topic__planning__scenario_planning__trajectory.jsonl",
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl",
        "ros2_topic__planning__trajectory.jsonl",
    ):
        (artifacts / name).write_text(json.dumps(trajectory_row) + "\n")
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 11.0, "acceleration": 0.0},
    }
    (artifacts / "autoware_control.jsonl").write_text(json.dumps(control_row) + "\n")
    (artifacts / "ros2_topic__planning__scenario_planning__current_max_velocity_probe.jsonl").write_text(
        json.dumps({"stamp": {"sec": 1, "nanosec": 0}, "max_velocity": 22.22}) + "\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert "planning_velocity_below_current_velocity_limit" in report["verdict"]["failure_reasons"]
    assert report["verdict"]["current_velocity_limit_max_mps"] == 22.22
    assert round(report["verdict"]["planning_current_velocity_limit_gap_mps"], 6) == round(
        22.22 - 13.888889, 6
    )


def test_autoware_control_diagnostics_flags_low_target_vs_controller_reference(tmp_path):
    run_dir = tmp_path / "controller_debug_fast_reference"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 0.1}))
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 0.25, "acceleration": 0.4},
        "lateral": {"steering_tire_angle": 0.0},
    }
    kinematic_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "pose": {"x": 0.0, "y": 0.0},
        "twist": {"linear": {"x": 0.0, "y": 0.0, "z": 0.0}},
    }
    fast_reference_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 2,
        "trajectory_velocity_mps": {"min": 0.25, "max": 2.5, "mean": 1.3, "first": 0.25, "last": 2.5},
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 2.0, "y": 0.0}, "longitudinal_velocity_mps": 2.5},
        ],
    }
    for name, row in {
        "ros2_topic__planning__trajectory.jsonl": fast_reference_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
        "ros2_topic__control__trajectory_follower__controller_node_exe__debug__resampled_reference_trajectory.jsonl": fast_reference_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert (
        "control_target_low_despite_controller_reference_speed"
        in report["verdict"]["failure_reasons"]
    )
    assert (
        report["controller_debug_topics"]["resampled_reference_trajectory"]["trajectory"][
            "velocity_max_mps"
        ]["max"]
        == 2.5
    )
    assert (
        "controller_nearest_reference_velocity_low"
        in report["verdict"]["failure_reasons"]
    )
    assert (
        report["controller_debug_alignment"]["resampled_reference_trajectory"][
            "nearest_sample_velocity_mps"
        ]["max"]
        == 0.25
    )
    assert (
        report["controller_debug_alignment"]["resampled_reference_trajectory"][
            "near_field_velocity_profile"
        ]["2_5m"]["max_velocity_mps"]["max"]
        == 2.5
    )


def test_autoware_control_diagnostics_flags_low_target_vs_controller_nearest_reference(tmp_path):
    run_dir = tmp_path / "controller_debug_fast_nearest_reference"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 0.1}))
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 0.25, "acceleration": 0.4},
        "lateral": {"steering_tire_angle": 0.0},
    }
    kinematic_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "pose": {"x": 2.0, "y": 0.0},
        "twist": {"linear": {"x": 0.0, "y": 0.0, "z": 0.0}},
    }
    fast_reference_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 2,
        "trajectory_velocity_mps": {
            "min": 0.25,
            "max": 2.5,
            "mean": 1.3,
            "first": 0.25,
            "last": 2.5,
        },
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 2.0, "y": 0.0}, "longitudinal_velocity_mps": 2.5},
        ],
    }
    for name, row in {
        "ros2_topic__planning__trajectory.jsonl": fast_reference_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
        "ros2_topic__control__trajectory_follower__controller_node_exe__debug__resampled_reference_trajectory.jsonl": fast_reference_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert (
        "control_target_low_despite_controller_nearest_reference_speed"
        in report["verdict"]["failure_reasons"]
    )
    assert (
        "controller_nearest_reference_velocity_low"
        not in report["verdict"]["failure_reasons"]
    )
    assert (
        report["controller_debug_alignment"]["resampled_reference_trajectory"][
            "nearest_sample_velocity_mps"
        ]["max"]
        == 2.5
    )
    assert (
        report["controller_debug_alignment"]["resampled_reference_trajectory"][
            "near_field_velocity_profile"
        ]["0_0p5m"]["max_velocity_mps"]["max"]
        == 2.5
    )


def test_autoware_control_diagnostics_parses_longitudinal_pid_diagnostic(tmp_path):
    run_dir = tmp_path / "longitudinal_diagnostic_low_target"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 0.1}))
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 0.25, "acceleration": 0.4},
        "lateral": {"steering_tire_angle": 0.0},
    }
    planning_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 2,
        "trajectory_velocity_mps": {
            "min": 0.25,
            "max": 4.0,
            "mean": 2.0,
            "first": 0.25,
            "last": 4.0,
        },
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 2.0, "y": 0.0}, "longitudinal_velocity_mps": 4.0},
        ],
    }
    for name, row in {
        "ros2_topic__planning__trajectory.jsonl": planning_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__control__trajectory_follower__longitudinal__diagnostic.jsonl": (
            _longitudinal_diagnostic_row(
                target_velocity=0.25,
                nearest_velocity=0.25,
                control_state=1,
                stop_distance=0.4,
                smooth_stop_mode=2.0,
            )
        ),
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["longitudinal_diagnostic"]["message_count"] == 1
    assert (
        report["longitudinal_diagnostic"]["fields"]["target_velocity_mps"]["max"]
        == 0.25
    )
    assert (
        report["longitudinal_diagnostic"]["fields"]["nearest_velocity_mps"]["max"]
        == 0.25
    )
    assert report["longitudinal_diagnostic"]["control_state_counts"] == {"STOPPING": 1}
    assert "longitudinal_pid_target_velocity_low" in report["verdict"]["failure_reasons"]
    assert "longitudinal_pid_nearest_velocity_low" in report["verdict"]["failure_reasons"]
    assert "longitudinal_pid_not_drive_state" in report["verdict"]["failure_reasons"]


def test_autoware_control_diagnostics_flags_kinematic_twist_axis_mismatch(tmp_path):
    run_dir = tmp_path / "kinematic_twist_axis_mismatch"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 4.0}))
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 2.0, "acceleration": 0.5},
        "lateral": {"steering_tire_angle": 0.0},
    }
    planning_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 2,
        "trajectory_velocity_mps": {
            "min": 0.25,
            "max": 5.0,
            "mean": 2.5,
            "first": 0.25,
            "last": 5.0,
        },
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 0.25},
            {"index": 1, "pose": {"x": 2.0, "y": 0.0}, "longitudinal_velocity_mps": 5.0},
        ],
    }
    kinematic_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "pose": {"x": 0.0, "y": 0.0},
        "twist": {"linear": {"x": 0.2, "y": 4.0, "z": 0.0}},
    }
    for name, row in {
        "ros2_topic__planning__trajectory.jsonl": planning_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert (
        report["kinematic_twist_contract"]["forward_component_ratio_at_max_speed"]
        < 0.1
    )
    assert "kinematic_twist_forward_component_low" in report["verdict"]["failure_reasons"]
    assert (
        "kinematic_twist_velocity_axis_swapped_candidate"
        in report["verdict"]["failure_reasons"]
    )


def test_autoware_control_diagnostics_flags_runtime_bridge_watchdog(tmp_path):
    run_dir = tmp_path / "runtime_watchdog_bridge"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 0.0}))
    (artifacts / "ros2_topic__control__trajectory_follower__control_cmd.jsonl").write_text(
        json.dumps(
            {
                "_node_stamp": 10.0,
                "stamp": {"sec": 1, "nanosec": 0},
                "longitudinal": {"velocity": 1.0, "acceleration": 0.0},
                "lateral": {"steering_tire_angle": 0.0},
            }
        )
        + "\n"
    )
    (artifacts / "autoware_carla_control_bridge.log").write_text(
        "[INFO] [9.0] [carla_control_bridge]: apply frame=10 source=watchdog "
        "target_speed=null accel=null current_speed=null speed_error=null "
        "speed_source=null longitudinal_mode=speed_feedback throttle=0.000 brake=1.000\n"
        "[INFO] [11.0] [carla_control_bridge]: apply frame=11 source=watchdog "
        "target_speed=null accel=null current_speed=null speed_error=null "
        "speed_source=null longitudinal_mode=speed_feedback throttle=0.000 brake=1.000\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert report["bridge_control"]["watchdog_before_first_control_count"] == 1
    assert report["bridge_control"]["watchdog_after_first_control_count"] == 1
    assert any("after first captured control" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_does_not_fail_availability_when_engaged(tmp_path):
    run_dir = tmp_path / "availability_engaged"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(json.dumps({"max_speed_mps": 1.0}))
    operation_rows = [
        {
            "stamp": {"sec": 1, "nanosec": 0},
            "mode": 2,
            "is_autoware_control_enabled": True,
            "is_autonomous_mode_available": True,
        },
        {
            "stamp": {"sec": 2, "nanosec": 0},
            "mode": 2,
            "is_autoware_control_enabled": True,
            "is_autonomous_mode_available": False,
        },
    ]
    (artifacts / "ros2_topic__control__vehicle_cmd_gate__operation_mode.jsonl").write_text(
        "\n".join(json.dumps(row) for row in operation_rows) + "\n"
    )

    report = analyze_autoware_control_run(run_dir)

    assert "operation_mode_autonomous_availability_unstable" not in report["verdict"]["failure_reasons"]
    assert any("control-enabled mode" in warning for warning in report["warnings"])


def test_autoware_control_diagnostics_graceful_missing_inputs(tmp_path):
    run_dir = tmp_path / "missing"
    (run_dir / "artifacts").mkdir(parents=True)
    (run_dir / "summary.json").write_text("{}")

    report = analyze_autoware_control_run(run_dir)

    assert report["verdict"]["status"] == "insufficient_data"
    assert "planning_trajectory" in report["missing_inputs"]
    assert "gated_control" in report["missing_inputs"]


def test_autoware_control_diagnostics_flags_lane_invasion_summary(tmp_path):
    run_dir = tmp_path / "lane_invasion"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "LANE_INVASION",
                "lane_invasion_count": 1,
                "collision_count": 0,
                "max_speed_mps": 7.0,
            }
        )
    )
    trajectory_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "trajectory_point_count": 2,
        "trajectory_velocity_mps": {"min": 2.0, "max": 4.0, "mean": 3.0, "first": 2.0, "last": 4.0},
        "trajectory_points_sample": [
            {"index": 0, "pose": {"x": 0.0, "y": 0.0}, "longitudinal_velocity_mps": 2.0},
            {"index": 1, "pose": {"x": 1.0, "y": 0.0}, "longitudinal_velocity_mps": 4.0},
        ],
    }
    control_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "longitudinal": {"velocity": 2.0, "acceleration": 0.2},
        "lateral": {"steering_tire_angle": 0.0},
    }
    kinematic_row = {
        "stamp": {"sec": 1, "nanosec": 0},
        "pose": {"x": 0.0, "y": 0.0},
        "twist": {"linear": {"x": 1.0, "y": 0.0, "z": 0.0}},
    }
    for name, row in {
        "ros2_topic__planning__trajectory.jsonl": trajectory_row,
        "ros2_topic__control__trajectory_follower__control_cmd.jsonl": control_row,
        "autoware_control.jsonl": control_row,
        "ros2_topic__localization__kinematic_state.jsonl": kinematic_row,
    }.items():
        (artifacts / name).write_text(json.dumps(row) + "\n")

    report = analyze_autoware_control_run(run_dir)

    assert report["summary"]["exit_reason"] == "LANE_INVASION"
    assert "lane_invasion" in report["verdict"]["failure_reasons"]
    assert any("lane invasion" in warning for warning in report["warnings"])
