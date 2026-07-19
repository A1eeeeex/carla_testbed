from __future__ import annotations

from types import SimpleNamespace

import pytest

from tools import validate_apollo_carla_actuator_tracking as tracking
from tools.apollo10_cyber_bridge.actuator_mapping import ActuatorCalibration


def test_parse_axes_is_ordered_deduplicated_and_strict() -> None:
    assert tracking._parse_axes("brake,steering,brake") == ("brake", "steering")
    with pytest.raises(ValueError, match="unsupported validation axes"):
        tracking._parse_axes("brake,unknown")
    with pytest.raises(ValueError, match="at least one"):
        tracking._parse_axes("")


def test_effective_brake_decel_uses_fixed_forward_speed_window() -> None:
    samples = [
        {"elapsed_sec": 0.1, "forward_speed_mps": 9.8, "forward_accel_mps2": -50.0},
        {"elapsed_sec": 0.2, "forward_speed_mps": 9.5, "forward_accel_mps2": -20.0},
        {"elapsed_sec": 0.4, "forward_speed_mps": 9.0, "forward_accel_mps2": 30.0},
        {"elapsed_sec": 0.8, "forward_speed_mps": 0.0, "forward_accel_mps2": -100.0},
    ]

    assert tracking._effective_brake_decel(
        samples,
        start_sec=0.2,
        end_sec=0.4,
    ) == pytest.approx(2.5)


def test_sample_window_delegates_to_probe_simulation_duration_sampler() -> None:
    class Probe:
        def __init__(self) -> None:
            self.call = None

        def hold_and_sample(self, **kwargs):
            self.call = kwargs
            return [{"elapsed_sec": 0.05}]

    probe = Probe()
    rows = tracking._sample_window(
        probe,
        throttle=0.0,
        brake=0.2,
        steer=0.0,
        sample_sec=1.2,
    )

    assert rows == [{"elapsed_sec": 0.05}]
    assert probe.call == {
        "throttle": 0.0,
        "brake": 0.2,
        "steer": 0.0,
        "settle_sec": 0.0,
        "sample_sec": 1.2,
    }


def test_validate_brake_records_unmapped_requests_and_all_speed_coverage() -> None:
    class Probe:
        def __init__(self) -> None:
            self.speed = 0.0

        def reset_to_reference_pose(self, *, settle_sec: float) -> None:
            self.speed = 0.0

        def accelerate_to_speed(self, *, target_speed_mps: float, throttle: float, timeout_sec: float):
            self.speed = target_speed_mps + 0.05
            return {
                "target_speed_mps": target_speed_mps,
                "final_speed_mps": self.speed,
                "reached": True,
            }

        def state(self):
            return {"speed_mps": self.speed}

        def hold_and_sample(self, **kwargs):
            del kwargs
            return [
                {"elapsed_sec": 0.2, "forward_speed_mps": 8.0},
                {"elapsed_sec": 0.4, "forward_speed_mps": 7.4},
            ]

    class Calibration:
        def brake_mapping_for_decel(self, target_decel_mps2: float, *, speed_mps: float):
            assert speed_mps > 0.0
            if target_decel_mps2 < 2.0:
                return {"cmd": None, "source": "decel_below_calibrated_brake_range"}
            return {"cmd": 0.1, "source": "physical_inverse_decel"}

    args = SimpleNamespace(
        validation_brake_entry_speeds="3.5,7.5",
        validation_target_decels="1.0,3.0",
        reset_settle_sec=0.8,
        brake_prep_throttle=0.45,
        longitudinal_prep_throttle=0.75,
        brake_prep_timeout_sec=8.0,
        longitudinal_prep_timeout_sec=18.0,
        validation_longitudinal_sample_sec=1.2,
        validation_brake_response_start_sec=0.2,
        validation_brake_response_end_sec=0.4,
        max_raw_series_per_axis=600,
    )

    result = tracking.validate_brake(Probe(), Calibration(), args)

    assert result["quality"]["sample_count"] == 2
    assert result["quality"]["skipped_count"] == 2
    assert result["quality"]["covered_entry_speed_count"] == 2
    assert result["quality"]["requested_entry_speed_count"] == 2
    assert result["quality"]["mean_abs_error_mps2"] == pytest.approx(0.0)
    assert all(
        item["response_metric"] == "forward_speed_delta_over_fixed_window"
        for item in result["measurements"]
    )
    assert {
        item["mapping_source"] for item in result["skipped_requests"]
    } == {"decel_below_calibrated_brake_range"}


def test_brake_only_cli_preserves_fixed_window_defaults() -> None:
    args = tracking.parse_args(
        [
            "--axes",
            "brake",
            "--skip-start-scene",
            "--spawn-vehicle",
            "--include-low-speed-brake-contract",
            "--low-speed-brake-contract-only",
        ]
    )

    assert tracking._parse_axes(args.axes) == ("brake",)
    assert args.validation_brake_response_start_sec == pytest.approx(0.2)
    assert args.validation_brake_response_end_sec == pytest.approx(0.4)
    assert args.include_low_speed_brake_contract is True
    assert args.low_speed_brake_contract_only is True
    assert args.validation_low_speed_stop_entry_speed_mps == pytest.approx(0.8)
    assert args.validation_low_speed_rolling_entry_speed_mps == pytest.approx(1.5)


def test_validate_low_speed_brake_contract_separates_hold_stop_and_rolling() -> None:
    class Probe:
        def __init__(self) -> None:
            self.speed = 0.0

        def reset_to_reference_pose(self, *, settle_sec: float) -> None:
            del settle_sec
            self.speed = 0.0

        def accelerate_to_speed(self, *, target_speed_mps: float, throttle: float, timeout_sec: float):
            del throttle, timeout_sec
            self.speed = float(target_speed_mps)
            return {
                "target_speed_mps": target_speed_mps,
                "final_speed_mps": self.speed,
                "reached": True,
            }

        def state(self):
            return {
                "forward_speed_mps": self.speed,
                "speed_mps": 0.17 if self.speed == 0.0 else self.speed,
            }

        def hold_and_sample(self, *, throttle: float, brake: float, steer: float, settle_sec: float, sample_sec: float):
            del throttle, steer, settle_sec, sample_sec
            if self.speed <= 0.1:
                return [
                    {"elapsed_sec": 0.05, "forward_speed_mps": 0.0, "speed_mps": 0.14},
                    {"elapsed_sec": 0.8, "forward_speed_mps": 0.0, "speed_mps": 0.0},
                ]
            if self.speed <= 1.0 and brake == pytest.approx(0.01):
                return [
                    {"elapsed_sec": 0.05, "forward_speed_mps": 0.4},
                    {"elapsed_sec": 0.1, "forward_speed_mps": 0.0},
                    {"elapsed_sec": 0.8, "forward_speed_mps": 0.0},
                ]
            return [
                {"elapsed_sec": 0.05, "forward_speed_mps": 1.48},
                {"elapsed_sec": 0.3, "forward_speed_mps": 1.20},
                {"elapsed_sec": 0.8, "forward_speed_mps": 0.90},
            ]

    class Calibration:
        def brake_mapping_for_decel(self, target_decel_mps2: float, *, speed_mps: float):
            assert target_decel_mps2 == pytest.approx(1.0)
            if speed_mps <= 0.1:
                return {"cmd": 0.0, "source": "physical_low_speed_hold"}
            if speed_mps <= 1.0:
                return {"cmd": 0.01, "source": "physical_low_speed_stop"}
            return {"cmd": 0.02, "source": "physical_low_speed_rolling_1.5"}

    args = SimpleNamespace(
        validation_low_speed_sample_sec=1.2,
        validation_low_speed_eval_sec=0.8,
        validation_low_speed_stop_speed_mps=0.05,
        validation_low_speed_hold_max_speed_mps=0.05,
        validation_low_speed_hold_target_decel_mps2=1.0,
        validation_low_speed_stop_entry_speed_mps=0.8,
        validation_low_speed_stop_target_decel_mps2=1.0,
        validation_low_speed_rolling_entry_speed_mps=1.5,
        validation_low_speed_rolling_target_decel_mps2=1.0,
        validation_low_speed_rolling_eval_sec=0.3,
        validation_low_speed_rolling_max_drop_error_mps=0.25,
        reset_settle_sec=0.8,
        brake_prep_throttle=0.45,
        longitudinal_prep_throttle=0.75,
        brake_prep_timeout_sec=8.0,
        longitudinal_prep_timeout_sec=18.0,
        max_raw_series_per_axis=600,
    )

    result = tracking.validate_low_speed_brake_contract(Probe(), Calibration(), args)

    assert result["quality"] == {
        "pass": True,
        "passed_state_count": 3,
        "required_state_count": 3,
    }
    by_state = {item["state"]: item for item in result["measurements"]}
    assert by_state["hold"]["mapped_brake_cmd"] == pytest.approx(0.0)
    assert by_state["hold"]["max_speed_mps"] == pytest.approx(0.0)
    assert by_state["stop"]["stop_time_sec"] == pytest.approx(0.1)
    assert by_state["rolling"]["measured_speed_drop_mps"] == pytest.approx(0.28)
    assert by_state["rolling"]["mapping_source"] == "physical_low_speed_rolling_1.5"


def test_brake_mapping_exposes_calibrated_range_and_high_clamp() -> None:
    calibration = ActuatorCalibration(
        {
            "brake": {
                "speed_bins": [
                    {
                        "speed_min_mps": 2.0,
                        "speed_max_mps": 5.0,
                        "quality": {"reliable": True},
                        "inverse": {
                            "target_decel_mps2_to_brake_cmd": [
                                {"target_decel_mps2": 2.5, "brake_cmd": 0.0},
                                {"target_decel_mps2": 3.0, "brake_cmd": 0.2},
                            ]
                        },
                    }
                ]
            }
        }
    )

    assert calibration.brake_decel_range(speed_mps=3.5) == pytest.approx((2.5, 3.0))
    assert calibration.brake_decel_range(speed_mps=1.8) is None
    below_speed_range = calibration.brake_mapping_for_decel(2.75, speed_mps=1.8)
    assert below_speed_range == {
        "cmd": None,
        "source": "speed_outside_calibrated_brake_bins",
        "calibrated_speed_min_mps": pytest.approx(2.0),
        "calibrated_speed_max_mps": pytest.approx(5.0),
    }
    in_range = calibration.brake_mapping_for_decel(2.75, speed_mps=3.5)
    assert in_range == {
        "cmd": pytest.approx(0.1),
        "source": "physical_inverse_decel",
        "calibrated_decel_mps2": pytest.approx(2.75),
    }
    high = calibration.brake_mapping_for_decel(3.5, speed_mps=3.5)
    assert high == {
        "cmd": pytest.approx(0.2),
        "source": "physical_inverse_decel_high_clamp",
        "calibrated_decel_mps2": pytest.approx(3.0),
    }


def test_brake_quality_rejects_single_large_tracking_error() -> None:
    threshold = {
        "metric": "mean_abs_error_mps2",
        "max": 1.0,
        "max_error_metric": "max_abs_error_mps2",
        "max_error_max": 1.0,
    }
    quality = {
        "sample_count": 16,
        "mean_abs_error_mps2": 0.48,
        "max_abs_error_mps2": 5.16,
        "covered_entry_speed_count": 5,
        "requested_entry_speed_count": 5,
    }

    assert tracking._axis_passes_quality("brake", quality, threshold) is False
