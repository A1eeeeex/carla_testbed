from __future__ import annotations

import pytest

from carla_testbed.calibration.steering_response import (
    analyze_steering_step_response,
    analyze_speed_dependent_steering_authority,
    interpolate_steering_curve,
    summarize_steering_step_trials,
)


def _step_rows(*, sign: float = 1.0) -> list[dict[str, float]]:
    rows = [
        {"elapsed_sec": -0.10, "measured_steer_deg": 0.0},
        {"elapsed_sec": -0.05, "measured_steer_deg": 0.0},
        {"elapsed_sec": 0.00, "measured_steer_deg": 0.0},
        {"elapsed_sec": 0.05, "measured_steer_deg": 0.0},
        {"elapsed_sec": 0.10, "measured_steer_deg": sign * 1.0},
        {"elapsed_sec": 0.15, "measured_steer_deg": sign * 5.0},
        {"elapsed_sec": 0.20, "measured_steer_deg": sign * 9.0},
        {"elapsed_sec": 0.25, "measured_steer_deg": sign * 10.0},
        {"elapsed_sec": 0.30, "measured_steer_deg": sign * 10.0},
        {"elapsed_sec": 0.35, "measured_steer_deg": sign * 10.0},
    ]
    return rows


@pytest.mark.parametrize("sign", [1.0, -1.0])
def test_analyze_steering_step_response_is_direction_independent(sign: float) -> None:
    report = analyze_steering_step_response(_step_rows(sign=sign))

    assert report["status"] == "pass"
    assert report["dead_time_sec"] == pytest.approx(0.10)
    assert report["time_to_10_percent_sec"] == pytest.approx(0.10)
    assert report["time_to_50_percent_sec"] == pytest.approx(0.15)
    assert report["time_to_90_percent_sec"] == pytest.approx(0.20)
    assert report["rise_time_10_90_sec"] == pytest.approx(0.10)
    assert report["settling_time_sec"] == pytest.approx(0.25)
    assert report["response_amplitude"] == pytest.approx(sign * 10.0)


def test_analyze_steering_step_response_requires_baseline_and_response() -> None:
    report = analyze_steering_step_response(
        [{"elapsed_sec": 0.0, "measured_steer_deg": 1.0}]
    )

    assert report["status"] == "insufficient_data"
    assert report["missing_fields"] == ["baseline_samples", "response_samples"]


def test_trial_summary_is_diagnostic_only_and_bidirectional() -> None:
    trials = []
    for index, angle in enumerate((-6.0, -3.0, 3.0, 6.0)):
        trials.append(
            {
                "trial_id": f"trial_{index}",
                "target_front_wheel_angle_deg": angle,
                "speed_gate": {"status": "pass"},
                "response": analyze_steering_step_response(
                    _step_rows(sign=-1.0 if angle < 0.0 else 1.0)
                ),
            }
        )

    report = summarize_steering_step_trials(trials)

    assert report["status"] == "pass"
    assert report["directions_observed"] == [-1, 1]
    assert report["claim_boundary"]["eligible_for_phase1_status"] is False
    assert report["claim_boundary"]["eligible_for_apollo_capability_claim"] is False
    assert report["apollo_latency_param_candidate"]["promotion_allowed"] is False
    assert report["apollo_latency_param_candidate"]["dead_time"] == pytest.approx(0.10)
    assert report["front_wheel_target_tracking_ratio_median"] == pytest.approx(2.5)
    assert report["front_wheel_target_tracking_error_deg_median"] == pytest.approx(5.5)


def test_trial_summary_rejects_stationary_operating_point() -> None:
    trials = [
        {
            "trial_id": "stationary",
            "target_front_wheel_angle_deg": 3.0,
            "speed_gate": {"status": "fail"},
            "response": analyze_steering_step_response(_step_rows()),
        }
    ]

    report = summarize_steering_step_trials(trials)

    assert report["status"] == "insufficient_data"
    assert report["valid_trial_count"] == 0
    assert report["rejected_speed_trial_count"] == 1


def test_interpolate_steering_curve_clamps_and_interpolates() -> None:
    curve = [
        {"speed_axis_value": 0.0, "max_steer_scale": 1.0},
        {"speed_axis_value": 20.0, "max_steer_scale": 0.9},
        {"speed_axis_value": 60.0, "max_steer_scale": 0.8},
    ]

    assert interpolate_steering_curve(curve, speed_axis_value=-1.0) == pytest.approx(1.0)
    assert interpolate_steering_curve(curve, speed_axis_value=10.0) == pytest.approx(0.95)
    assert interpolate_steering_curve(curve, speed_axis_value=80.0) == pytest.approx(0.8)


def test_speed_dependent_authority_identifies_kph_curve_axis_without_promotion() -> None:
    curve = [
        {"speed_axis_value": 0.0, "max_steer_scale": 1.0},
        {"speed_axis_value": 20.0, "max_steer_scale": 0.9},
        {"speed_axis_value": 60.0, "max_steer_scale": 0.8},
        {"speed_axis_value": 120.0, "max_steer_scale": 0.7},
    ]
    max_angle = 70.0
    command = 0.1
    trials = []
    for speed_mps in (5.0, 10.0, 15.0, 19.0):
        expected_fraction = interpolate_steering_curve(
            curve,
            speed_axis_value=speed_mps * 3.6,
        )
        assert expected_fraction is not None
        for direction in (-1.0, 1.0):
            trials.append(
                {
                    "target_speed_mps": speed_mps,
                    "speed_at_step_mps": speed_mps,
                    "carla_steer_cmd": direction * command,
                    "speed_gate": {"status": "pass"},
                    "response": {
                        "status": "pass",
                        "response_amplitude": direction
                        * command
                        * max_angle
                        * expected_fraction,
                    },
                }
            )

    report = analyze_speed_dependent_steering_authority(
        trials,
        steering_curve=curve,
        max_steer_angle_deg=max_angle,
    )

    assert report["status"] == "pass"
    assert report["distinct_target_speed_count"] == 4
    assert report["speed_axis_unit_identification"]["status"] == "resolved"
    assert report["speed_axis_unit_identification"]["best_fit_unit"] == "kph"
    assert report["speed_axis_unit_identification"]["best_fit_absolute_error_median"] == pytest.approx(0.0)
    assert report["claim_boundary"]["promotion_allowed"] is False
