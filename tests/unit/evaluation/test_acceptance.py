from __future__ import annotations

from carla_testbed.evaluation import (
    applied_control_health_from_timeseries,
    bridge_target_speed_failures_from_check,
    bridge_target_speed_health_from_log,
    control_health_failures_from_check,
    safety_failures_from_summary,
)


def test_safety_exit_reason_forces_acceptance_failure():
    failures = safety_failures_from_summary(
        {
            "success": True,
            "exit_reason": "LANE_INVASION",
            "lane_invasion_count": 1,
            "collision_count": 0,
        }
    )

    assert [failure.code for failure in failures] == ["LANE_INVASION"]
    assert failures[0].detail["source"] == "exit_reason"
    assert failures[0].detail["scope"] == "safety"


def test_safety_counter_forces_acceptance_failure_when_exit_reason_is_success():
    failures = safety_failures_from_summary(
        {
            "success": True,
            "exit_reason": "success",
            "metrics": {
                "lane_invasion_count": 2,
                "collision_count": 0,
            },
        }
    )

    assert [failure.code for failure in failures] == ["LANE_INVASION"]
    assert failures[0].detail["source"] == "lane_invasion_count"


def test_collision_and_lane_invasion_are_reported_independently():
    failures = safety_failures_from_summary(
        {
            "exit_reason": "max_steps_reached",
            "collision_count": 1,
            "lane_invasion_count": 3,
        }
    )

    assert [failure.code for failure in failures] == ["COLLISION", "LANE_INVASION"]


def test_no_safety_signal_has_no_failure():
    assert safety_failures_from_summary({"exit_reason": "max_steps_reached"}) == []


def test_control_health_flags_applied_throttle_brake_oscillation(tmp_path):
    timeseries = tmp_path / "timeseries.csv"
    timeseries.write_text(
        "frame_id,throttle_applied,brake_applied\n"
        + "\n".join(
            f"{index},{0.8 if index % 2 == 0 else 0.0},{0.0 if index % 2 == 0 else 0.7}"
            for index in range(12)
        )
        + "\n"
    )

    check = applied_control_health_from_timeseries(
        timeseries,
        max_throttle_brake_switch_count=10,
    )
    failures = control_health_failures_from_check(
        check,
        enabled=True,
        fail_on_missing=True,
    )

    assert check["throttle_brake_switch_count"] == 11
    assert [failure.code for failure in failures] == ["CONTROL_HEALTH_APPLIED_OSCILLATION"]
    assert failures[0].detail["scope"] == "control_health"


def test_control_health_missing_timeseries_can_fail_when_required(tmp_path):
    check = applied_control_health_from_timeseries(
        tmp_path / "missing.csv",
        max_throttle_brake_switch_count=10,
    )

    failures = control_health_failures_from_check(
        check,
        enabled=True,
        fail_on_missing=True,
    )

    assert [failure.code for failure in failures] == ["CONTROL_HEALTH_MISSING_TIMESERIES"]


def test_bridge_target_speed_health_flags_throttle_above_target(tmp_path):
    log_path = tmp_path / "autoware_carla_control_bridge.log"
    log_path.write_text(
        "\n".join(
            "[INFO] [10.0] [carla_control_bridge]: "
            f"apply frame={index} source=pending target_speed=1.000 accel=0.800 "
            "current_speed=4.500 throttle=0.650 brake=0.000"
            for index in range(12)
        )
        + "\n"
    )

    check = bridge_target_speed_health_from_log(
        log_path,
        overspeed_threshold_mps=2.0,
        max_throttle_while_overspeed_rows=9,
    )
    failures = bridge_target_speed_failures_from_check(
        check,
        enabled=True,
        fail_on_missing=True,
    )

    assert check["max_current_minus_target_speed_mps"] == 3.5
    assert check["overspeed_rows"] == 12
    assert check["throttle_while_overspeed_rows"] == 12
    assert [failure.code for failure in failures] == ["CONTROL_HEALTH_BRIDGE_OVERSPEED_THROTTLE"]
    assert failures[0].detail["source"] == "bridge_log"


def test_bridge_target_speed_health_ignores_config_throttle_token(tmp_path):
    log_path = tmp_path / "autoware_carla_control_bridge.log"
    log_path.write_text(
        "[INFO] [10.0] [carla_control_bridge]: "
        "apply frame=1 source=pending target_speed=1.000 accel=0.800 "
        "current_speed=4.500 positive_accel_min_throttle=0.650 "
        "throttle=0.000 brake=0.500\n"
    )

    check = bridge_target_speed_health_from_log(
        log_path,
        overspeed_threshold_mps=2.0,
        max_throttle_while_overspeed_rows=0,
    )
    failures = bridge_target_speed_failures_from_check(
        check,
        enabled=True,
        fail_on_missing=True,
    )

    assert check["overspeed_rows"] == 1
    assert check["throttle_while_overspeed_rows"] == 0
    assert failures == []
