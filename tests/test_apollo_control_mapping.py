from __future__ import annotations

from pathlib import Path

from tools.apollo10_cyber_bridge.control_mapping import (
    ControlMappingConfig,
    apply_throttle_brake_mutual_exclusion,
    legacy_map_base_controls,
    physical_map_base_controls,
    select_steering_field,
    steering_normalization_mode,
)
from tools.apollo10_cyber_bridge.actuator_mapping import (
    ActuatorCalibration,
    load_actuator_calibration,
)


def _config(**overrides: object) -> ControlMappingConfig:
    values = {
        "throttle_scale": 1.0,
        "brake_scale": 1.0,
        "steer_scale": 0.25,
        "steer_sign": 1.0,
        "brake_deadzone": 0.05,
        "throttle_brake_mutual_exclusion_enabled": True,
        "throttle_brake_hysteresis_frames": 2,
        "throttle_brake_min_command": 0.05,
        "physical_allow_legacy_fallback": False,
        "physical_apollo_max_steer_angle_deg": 8.203,
        "physical_apollo_max_accel_mps2": 4.0,
        "physical_apollo_max_decel_mps2": 6.0,
        "physical_use_top_level_acceleration": True,
        "physical_use_lon_debug": True,
        "physical_steer_field_priority": ("steering_target",),
        "physical_acceleration_field_priority": ("acceleration",),
        "physical_map_steering": True,
        "physical_map_longitudinal": True,
        "physical_map_throttle": True,
        "physical_map_brake": True,
    }
    values.update(overrides)
    return ControlMappingConfig(**values)


def test_mutual_exclusion_resolves_overlap_after_mapping() -> None:
    result = apply_throttle_brake_mutual_exclusion(
        throttle=0.35,
        brake=0.70,
        previous_state="coast",
        hysteresis_counter=0,
        config=_config(),
    )

    assert result["throttle"] == 0.0
    assert result["brake"] == 0.70
    assert result["policy_applied"] is True
    assert result["policy_reason"] == "mutual_exclusion_overlap"
    assert result["policy_state"] == "brake"
    assert result["desired_state"] == "brake"


def test_legacy_mapping_records_steer_scale_and_sign_for_audit_trace() -> None:
    result = legacy_map_base_controls(
        raw_throttle=0.2,
        raw_brake=0.0,
        raw_steer=0.4,
        now_sec=1.0,
        config=_config(steer_scale=0.25, steer_sign=-1.0),
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert abs(result["steer"] + 0.1) < 1e-9
    assert result["steer_scale"] == 0.25
    assert result["steer_sign"] == -1.0
    assert result["steer_source"] == "legacy_scale"


def test_mutual_exclusion_can_be_disabled_for_diagnostic_replay() -> None:
    result = apply_throttle_brake_mutual_exclusion(
        throttle=0.35,
        brake=0.70,
        previous_state="coast",
        hysteresis_counter=0,
        config=_config(throttle_brake_mutual_exclusion_enabled=False),
    )

    assert result["throttle"] == 0.35
    assert result["brake"] == 0.70
    assert result["policy_applied"] is False
    assert result["policy_reason"] == "disabled"


def test_hysteresis_marks_and_suppresses_immediate_throttle_brake_flip() -> None:
    result = apply_throttle_brake_mutual_exclusion(
        throttle=0.0,
        brake=0.60,
        previous_state="throttle",
        hysteresis_counter=0,
        config=_config(throttle_brake_hysteresis_frames=2),
    )

    assert result["throttle"] == 0.0
    assert result["brake"] == 0.0
    assert result["policy_applied"] is True
    assert result["policy_reason"] == "hysteresis_hold"
    assert result["policy_state"] == "throttle"
    assert result["desired_state"] == "brake"
    assert result["hysteresis_counter"] == 1
    assert result["hysteresis_held"] is True


def test_angle_degree_normalization_treats_steering_target_as_degrees() -> None:
    field, value = select_steering_field(
        {"steering_target": 15.0},
        physical_mode=False,
        physical_steer_field_priority=("steering_target",),
        coerce_float=lambda value, _default, _field, _scope: float(value),
        percent_normalization_mode="angle_degree_at_select",
        max_steer_angle_deg=30.0,
    )

    assert field == "steering_target"
    assert value == 0.5
    assert (
        steering_normalization_mode(
            field,
            percent_normalization_mode="angle_degree_at_select",
        )
        == "angle_degree_at_select"
    )


def test_physical_mapping_applies_same_steering_sign_as_legacy() -> None:
    class Calibration:
        loaded = True

        def __init__(self) -> None:
            self.requested_angle = None

        def steering_cmd_for_angle(self, angle_deg: float) -> float:
            self.requested_angle = angle_deg
            return angle_deg / 100.0

        def throttle_mapping_for_accel(self, *_args: object, **_kwargs: object) -> dict[str, float]:
            return {"cmd": 0.0}

        def brake_mapping_for_decel(self, *_args: object, **_kwargs: object) -> dict[str, float]:
            return {"cmd": 0.0}

    calibration = Calibration()
    result = physical_map_base_controls(
        raw_fields={"acceleration": 0.0},
        raw_throttle=0.0,
        raw_brake=0.0,
        raw_steer=0.5,
        now_sec=1.0,
        config=_config(
            steer_sign=-1.0,
            physical_apollo_max_steer_angle_deg=30.0,
        ),
        calibration=calibration,
        latest_speed_mps=5.0,
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert calibration.requested_angle == -15.0
    assert result["target_front_wheel_angle_deg"] == -15.0
    assert result["steer"] == -0.15
    assert result["steering_speed_compensation_enabled"] is False
    assert result["steering_speed_compensation_applied"] is False


def test_physical_mapping_can_apply_calibrated_speed_tracking_compensation() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "steering": {
                "inverse": {
                    "target_front_wheel_angle_deg_to_carla_cmd": [
                        {
                            "target_front_wheel_angle_deg": 0.0,
                            "carla_steer_cmd": 0.0,
                        },
                        {
                            "target_front_wheel_angle_deg": 30.0,
                            "carla_steer_cmd": 0.3,
                        },
                    ]
                },
                "speed_compensation": {
                    "front_wheel_target_tracking_ratio_by_speed_mps": [
                        {"speed_mps": 5.0, "tracking_ratio": 1.0},
                        {"speed_mps": 15.0, "tracking_ratio": 0.8},
                    ]
                },
            },
        }
    )

    result = physical_map_base_controls(
        raw_fields={"acceleration": 0.0},
        raw_throttle=0.0,
        raw_brake=0.0,
        raw_steer=0.5,
        now_sec=1.0,
        config=_config(
            steer_sign=-1.0,
            physical_apollo_max_steer_angle_deg=30.0,
            physical_map_longitudinal=False,
            physical_map_throttle=False,
            physical_map_brake=False,
            physical_steering_speed_compensation_enabled=True,
        ),
        calibration=calibration,
        latest_speed_mps=15.0,
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert calibration.status()["steering_speed_compensation_pairs"] == 2
    assert calibration.steering_speed_tracking_ratio(10.0) == 0.9
    assert result["target_front_wheel_angle_deg"] == -15.0
    assert result["steering_speed_tracking_ratio"] == 0.8
    assert result["steering_calibration_query_angle_deg"] == -18.75
    assert result["mapped_carla_steer_cmd"] == -0.1875
    assert result["steering_speed_compensation_applied"] is True


def test_physical_steering_only_preserves_legacy_longitudinal_mapping() -> None:
    class Calibration:
        loaded = True

        def steering_cmd_for_angle(self, angle_deg: float) -> float:
            return angle_deg / 100.0

        def throttle_mapping_for_accel(self, *_args: object, **_kwargs: object) -> dict[str, float]:
            raise AssertionError("longitudinal calibration must not be used")

        def brake_mapping_for_decel(self, *_args: object, **_kwargs: object) -> dict[str, float]:
            raise AssertionError("longitudinal calibration must not be used")

    result = physical_map_base_controls(
        raw_fields={"acceleration": 2.0},
        raw_throttle=0.3,
        raw_brake=0.1,
        raw_steer=0.5,
        now_sec=1.0,
        config=_config(
            steer_sign=-1.0,
            physical_apollo_max_steer_angle_deg=30.0,
            physical_map_longitudinal=False,
            physical_map_throttle=False,
            physical_map_brake=False,
        ),
        calibration=Calibration(),
        latest_speed_mps=5.0,
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert result["steer"] == -0.15
    assert result["throttle"] == 0.3
    assert result["brake"] == 0.1
    assert result["steer_source"] == "physical_inverse_front_wheel_angle"
    assert result["throttle_source"] == "legacy_scale_component_disabled"
    assert result["brake_source"] == "legacy_scale_component_disabled"
    assert result["physical_map_steering"] is True
    assert result["physical_map_longitudinal"] is False


def test_signed_acceleration_calibration_maps_mild_decel_to_throttle() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "throttle": {
                "speed_bins": [
                    {
                        "speed_min_mps": 10.0,
                        "speed_max_mps": 15.0,
                        "measurements": [
                            {"throttle_cmd": 0.3, "forward_accel_mps2": -0.74},
                            {"throttle_cmd": 0.4, "forward_accel_mps2": -0.05},
                        ],
                    }
                ]
            },
        }
    )

    result = calibration.decel_actuation_mapping(0.8, speed_mps=12.0)

    assert result["source"] == "physical_inverse_signed_accel_low_clamp"
    assert result["throttle_cmd"] == 0.3
    assert result["brake_cmd"] == 0.0


def test_signed_acceleration_calibration_clamps_near_upper_edge_instead_of_coasting() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "throttle": {
                "speed_bins": [
                    {
                        "speed_min_mps": 10.0,
                        "speed_max_mps": 15.0,
                        "measurements": [
                            {"throttle_cmd": 0.3, "forward_accel_mps2": -0.74},
                            {"throttle_cmd": 0.4, "forward_accel_mps2": -0.055},
                        ],
                    }
                ]
            },
        }
    )

    result = calibration.decel_actuation_mapping(0.049, speed_mps=12.0)

    assert result["source"] == "physical_inverse_signed_accel_high_clamp"
    assert result["throttle_cmd"] == 0.4
    assert result["brake_cmd"] == 0.0


def test_low_speed_legacy_crawl_accel_calibration_covers_moderate_decel() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "throttle": {
                "low_speed": {
                    "crawl": [
                        {
                            "entry_speed_mps": 4.0,
                            "quality": {"reliable": True},
                            "measurements": [
                                {
                                    "throttle_cmd": 0.35,
                                    "delta_speed_mps": -2.88,
                                    "crawl_accel_mps2": -0.52,
                                    "entry_reached": True,
                                },
                                {
                                    "throttle_cmd": 0.40,
                                    "delta_speed_mps": -0.295,
                                    "crawl_accel_mps2": -0.05,
                                    "entry_reached": True,
                                },
                                {
                                    "throttle_cmd": 0.45,
                                    "delta_speed_mps": 0.237,
                                    "crawl_accel_mps2": 0.41,
                                    "entry_reached": True,
                                },
                            ],
                        }
                    ],
                }
            },
        }
    )

    result = calibration.decel_actuation_mapping(0.05, speed_mps=4.2)

    assert result["source"] == "physical_inverse_signed_accel_crawl_throttle"
    assert result["throttle_cmd"] == 0.4
    assert result["brake_cmd"] == 0.0
    assert calibration.status()["signed_throttle_low_speed_sections"] == 1


def test_low_speed_effective_accel_takes_precedence_over_legacy_crawl_accel() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "throttle": {
                "low_speed": {
                    "crawl": [
                        {
                            "entry_speed_mps": 4.0,
                            "quality": {"reliable": True},
                            "measurements": [
                                {
                                    "throttle_cmd": 0.35,
                                    "effective_accel_mps2": -0.30,
                                    "crawl_accel_mps2": 0.30,
                                    "entry_reached": True,
                                },
                                {
                                    "throttle_cmd": 0.45,
                                    "effective_accel_mps2": 0.30,
                                    "crawl_accel_mps2": 0.60,
                                    "entry_reached": True,
                                },
                            ],
                        }
                    ]
                }
            },
        }
    )

    result = calibration.throttle_mapping_for_signed_accel(0.0, speed_mps=4.0)

    assert result["cmd"] == 0.4
    assert result["calibration_response_source"] == "effective_accel_mps2"


def test_low_speed_positive_and_negative_accel_share_continuous_signed_table() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "throttle": {
                "low_speed": {
                    "crawl": [
                        {
                            "entry_speed_mps": 4.0,
                            "quality": {"reliable": True},
                            "measurements": [
                                {"throttle_cmd": 0.35, "crawl_accel_mps2": -0.30},
                                {"throttle_cmd": 0.40, "crawl_accel_mps2": -0.02},
                                {"throttle_cmd": 0.45, "crawl_accel_mps2": 0.40},
                            ],
                        }
                    ]
                }
            },
        }
    )

    decel = calibration.decel_actuation_mapping(0.05, speed_mps=4.0)
    accel = calibration.throttle_mapping_for_accel(0.05, speed_mps=4.0)

    assert decel["source"] == "physical_inverse_signed_accel_crawl_throttle"
    assert accel["source"] == "physical_inverse_signed_accel_crawl_throttle"
    assert abs(float(accel["cmd"]) - float(decel["throttle_cmd"])) < 0.02


def test_low_speed_brake_contract_separates_hold_stop_and_rolling_ranges() -> None:
    calibration = ActuatorCalibration(
        {
            "brake": {
                "low_speed": {
                    "hold": {
                        "probe": {"activation_max_speed_mps": 0.1},
                        "summary": {"hold_cmd": 0.0},
                        "quality": {"reliable": True},
                    },
                    "stop": {
                        "probe": {
                            "activation_max_speed_mps": 1.0,
                            "request_decel_threshold_mps2": 0.8,
                        },
                        "summary": {"stop_cmd": 0.01},
                        "quality": {"reliable": True},
                    },
                    "rolling": [
                        {
                            "entry_speed_mps": 1.5,
                            "speed_min_mps": 1.25,
                            "speed_max_mps": 1.75,
                            "probe": {"eval_sec": 0.3},
                            "quality": {"reliable": True},
                            "inverse": {
                                "target_speed_drop_mps_to_brake_cmd": [
                                    {"target_speed_drop_mps": 0.0, "brake_cmd": 0.0},
                                    {"target_speed_drop_mps": 0.6, "brake_cmd": 0.05},
                                ]
                            },
                        }
                    ],
                }
            }
        }
    )

    assert calibration.brake_mapping_for_decel(1.0, speed_mps=0.05) == {
        "cmd": 0.0,
        "source": "physical_low_speed_hold",
    }
    assert calibration.brake_mapping_for_decel(1.0, speed_mps=0.5) == {
        "cmd": 0.01,
        "source": "physical_low_speed_stop",
    }
    assert calibration.brake_mapping_for_decel(0.3, speed_mps=0.5) == {
        "cmd": None,
        "source": "missing_speed_bin",
    }
    rolling = calibration.brake_mapping_for_decel(1.0, speed_mps=1.5)
    assert rolling == {
        "cmd": 0.025,
        "source": "physical_low_speed_rolling_1.5",
    }
    assert calibration.brake_mapping_for_decel(1.0, speed_mps=1.8) == {
        "cmd": None,
        "source": "missing_speed_bin",
    }


def test_crawl_delta_requires_an_explicit_matching_window() -> None:
    calibration = ActuatorCalibration(
        {
            "schema_version": 1,
            "throttle": {
                "low_speed": {
                    "launch": {"probe": {"eval_sec": 0.8}},
                    "crawl": [
                        {
                            "entry_speed_mps": 4.0,
                            "quality": {"reliable": True},
                            "measurements": [
                                {"throttle_cmd": 0.35, "delta_speed_mps": -0.3},
                                {"throttle_cmd": 0.45, "delta_speed_mps": 0.3},
                            ],
                        }
                    ],
                }
            },
        }
    )

    result = calibration.throttle_mapping_for_signed_accel(0.0, speed_mps=4.0)

    assert result["cmd"] is None
    assert calibration.status()["signed_throttle_low_speed_sections"] == 0


def test_physical_brake_component_uses_throttle_for_calibrated_mild_decel() -> None:
    class Calibration:
        loaded = True

        def steering_cmd_for_angle(self, angle_deg: float) -> float:
            return angle_deg / 100.0

        def decel_actuation_mapping(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            return {
                "throttle_cmd": 0.3,
                "brake_cmd": 0.0,
                "source": "physical_inverse_signed_accel_throttle",
            }

    result = physical_map_base_controls(
        raw_fields={"acceleration": -0.8},
        raw_throttle=0.0,
        raw_brake=0.145,
        raw_steer=0.0,
        now_sec=1.0,
        config=_config(
            physical_map_longitudinal=False,
            physical_map_throttle=False,
            physical_map_brake=True,
        ),
        calibration=Calibration(),
        latest_speed_mps=12.0,
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert result["target_decel_mps2"] == 0.8
    assert result["throttle"] == 0.3
    assert result["brake"] == 0.0
    assert result["throttle_source"] == "physical_inverse_signed_accel_throttle"
    assert result["brake_source"] == "physical_decel_via_throttle"
    assert result["physical_map_throttle"] is False
    assert result["physical_map_brake"] is True


def test_v57_brake_candidate_records_mapping_speed_and_unavailable_reason() -> None:
    calibration = load_actuator_calibration(
        Path(
            "configs/calibration/vehicles/vehicle.lincoln.mkz_2020/"
            "steering_brake_v57_diagnostic_candidate.json"
        )
    )

    unavailable = physical_map_base_controls(
        raw_fields={"acceleration": -2.14},
        raw_throttle=0.0,
        raw_brake=0.159,
        raw_steer=0.0,
        now_sec=1.0,
        config=_config(
            physical_map_longitudinal=True,
            physical_map_throttle=False,
            physical_map_brake=True,
        ),
        calibration=calibration,
        latest_speed_mps=3.35,
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert unavailable["brake"] == 0.0
    assert unavailable["brake_source"] == "physical_inverse_unavailable_no_fallback"
    assert unavailable["physical_fallback_reason"] == (
        "decel_inverse_missing_no_fallback"
    )
    assert unavailable["actuator_mapping_speed_mps"] == 3.35
    assert unavailable["decel_actuation_mapping_source"] == (
        "decel_below_calibrated_brake_range"
    )

    high_clamp = physical_map_base_controls(
        raw_fields={"acceleration": -4.1},
        raw_throttle=0.0,
        raw_brake=0.29,
        raw_steer=0.0,
        now_sec=1.0,
        config=_config(
            physical_map_longitudinal=True,
            physical_map_throttle=False,
            physical_map_brake=True,
        ),
        calibration=calibration,
        latest_speed_mps=7.3,
        apply_zero_hold=lambda throttle, _brake, _now: throttle,
    )

    assert high_clamp["brake"] == 0.2
    assert high_clamp["actuator_mapping_speed_mps"] == 7.3
    assert high_clamp["decel_actuation_mapping_source"] == (
        "physical_inverse_decel_high_clamp"
    )
