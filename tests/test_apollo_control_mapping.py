from __future__ import annotations

from tools.apollo10_cyber_bridge.control_mapping import (
    ControlMappingConfig,
    apply_throttle_brake_mutual_exclusion,
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
