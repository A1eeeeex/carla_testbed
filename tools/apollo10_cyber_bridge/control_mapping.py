from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, Sequence, Tuple

try:
    from actuator_mapping import ActuatorCalibration
except Exception:
    from tools.apollo10_cyber_bridge.actuator_mapping import ActuatorCalibration  # type: ignore


FloatCoercer = Callable[[Any, float, str, str], float]
ZeroHoldApplier = Callable[[float, float, float], float]

STEERING_PERCENT_FIELDS = {"steering_target", "steering_percentage"}
STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT = "single_percent_at_select"
STEERING_NORMALIZATION_LEGACY_DOUBLE_PERCENT = "legacy_double_percent"
STEERING_NORMALIZATION_FIELD_VALUE_CLAMP_ONLY = "field_value_clamp_only"
STEERING_PERCENT_NORMALIZATION_MODES = {
    STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT,
    STEERING_NORMALIZATION_LEGACY_DOUBLE_PERCENT,
}


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _safe_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return float(default)
        return out
    except Exception:
        return float(default)


@dataclass(frozen=True)
class ControlMappingConfig:
    throttle_scale: float
    brake_scale: float
    steer_scale: float
    steer_sign: float
    brake_deadzone: float
    throttle_brake_mutual_exclusion_enabled: bool
    throttle_brake_hysteresis_frames: int
    throttle_brake_min_command: float
    physical_allow_legacy_fallback: bool
    physical_apollo_max_steer_angle_deg: float
    physical_apollo_max_accel_mps2: float
    physical_apollo_max_decel_mps2: float
    physical_use_top_level_acceleration: bool
    physical_use_lon_debug: bool
    physical_steer_field_priority: Tuple[str, ...]
    physical_acceleration_field_priority: Tuple[str, ...]


def select_steering_field(
    raw_fields: Dict[str, Any],
    *,
    physical_mode: bool,
    physical_steer_field_priority: Sequence[str],
    coerce_float: FloatCoercer,
    percent_normalization_mode: str = STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT,
) -> Tuple[str, float]:
    percent_mode = (
        percent_normalization_mode
        if percent_normalization_mode in STEERING_PERCENT_NORMALIZATION_MODES
        else STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT
    )
    field_order = (
        tuple(physical_steer_field_priority)
        if physical_mode
        else ("steering_target", "steering_percentage", "steering", "steering_rate")
    )
    for key in field_order:
        if key in raw_fields:
            raw_value = coerce_float(raw_fields.get(key), 0.0, key, "apollo.control")
            if key in STEERING_PERCENT_FIELDS:
                if percent_mode == STEERING_NORMALIZATION_LEGACY_DOUBLE_PERCENT:
                    return key, raw_value / 10000.0
                return key, raw_value / 100.0
            return key, raw_value
    return "missing", 0.0


def steering_normalization_mode(
    selected_field: str,
    *,
    percent_normalization_mode: str = STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT,
) -> str:
    if selected_field in STEERING_PERCENT_FIELDS:
        if percent_normalization_mode in STEERING_PERCENT_NORMALIZATION_MODES:
            return percent_normalization_mode
        return STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT
    return STEERING_NORMALIZATION_FIELD_VALUE_CLAMP_ONLY


def normalize_steering_command(steer_value: float, *, physical_mode: bool) -> float:
    # select_steering_field() applies the configured percent-normalization
    # policy for Apollo percentage fields.  This helper is intentionally only a
    # final safety clamp so the policy is visible in trace artifacts.
    raw_steer = float(steer_value)
    return _clamp(raw_steer, -1.0, 1.0)


def legacy_map_base_controls(
    *,
    raw_throttle: float,
    raw_brake: float,
    raw_steer: float,
    now_sec: float,
    config: ControlMappingConfig,
    apply_zero_hold: ZeroHoldApplier,
) -> Dict[str, Any]:
    throttle = _clamp(raw_throttle * config.throttle_scale, 0.0, 1.0)
    brake_before_deadzone = _clamp(raw_brake * config.brake_scale, 0.0, 1.0)
    brake = 0.0 if brake_before_deadzone < config.brake_deadzone else brake_before_deadzone
    throttle = apply_zero_hold(throttle, brake, now_sec)
    steer_pre = raw_steer * config.steer_sign * config.steer_scale
    steer = _clamp(steer_pre, -1.0, 1.0)
    return {
        "mode": "legacy",
        "throttle": throttle,
        "brake": brake,
        "steer": steer,
        "steer_pre_clamp": steer_pre,
        "steer_clamped": abs(steer_pre) > 1.0,
        "steer_sign": config.steer_sign,
        "throttle_source": "legacy_scale",
        "brake_source": "legacy_scale",
        "steer_source": "legacy_scale",
        "throttle_before_deadzone": throttle,
        "brake_before_deadzone": brake_before_deadzone,
        "brake_after_deadzone": brake,
        "target_front_wheel_angle_deg": None,
        "target_accel_mps2": None,
        "target_decel_mps2": None,
        "mapped_throttle_cmd": throttle,
        "mapped_brake_cmd": brake,
        "mapped_carla_steer_cmd": steer,
        "physical_fallback_reason": "",
        "target_accel_source": "",
        "target_decel_source": "",
    }


def apply_throttle_brake_mutual_exclusion(
    *,
    throttle: float,
    brake: float,
    previous_state: str,
    hysteresis_counter: int,
    config: ControlMappingConfig,
) -> Dict[str, Any]:
    """Resolve mapped throttle/brake overlap without hiding raw commands.

    This is a bridge-output safety policy, not an Apollo-command smoother. The
    caller must still record raw and pre-policy mapped commands.
    """

    throttle_in = _clamp(float(throttle), 0.0, 1.0)
    brake_in = _clamp(float(brake), 0.0, 1.0)
    if not config.throttle_brake_mutual_exclusion_enabled:
        return {
            "throttle": throttle_in,
            "brake": brake_in,
            "policy_applied": False,
            "policy_reason": "disabled",
            "policy_state": previous_state or "coast",
            "hysteresis_counter": int(hysteresis_counter),
        }

    threshold = max(0.0, float(config.throttle_brake_min_command))
    throttle_active = throttle_in > threshold
    brake_active = brake_in > threshold
    desired_state = "coast"
    if throttle_active and brake_active:
        desired_state = "brake" if brake_in >= throttle_in else "throttle"
    elif brake_active:
        desired_state = "brake"
    elif throttle_active:
        desired_state = "throttle"

    previous = previous_state if previous_state in {"throttle", "brake", "coast"} else "coast"
    counter = max(0, int(hysteresis_counter))
    hysteresis_frames = max(0, int(config.throttle_brake_hysteresis_frames))
    selected_state = desired_state
    hysteresis_held = False
    if (
        hysteresis_frames > 0
        and desired_state in {"throttle", "brake"}
        and previous in {"throttle", "brake"}
        and desired_state != previous
        and counter < hysteresis_frames
    ):
        selected_state = previous
        counter += 1
        hysteresis_held = True
    elif desired_state == previous:
        counter = 0
    else:
        counter = 0

    throttle_out = throttle_in
    brake_out = brake_in
    policy_applied = False
    if selected_state == "throttle" and brake_out > 0.0:
        brake_out = 0.0
        policy_applied = True
    elif selected_state == "brake" and throttle_out > 0.0:
        throttle_out = 0.0
        policy_applied = True
    elif selected_state == "coast" and (throttle_out > 0.0 or brake_out > 0.0):
        throttle_out = 0.0
        brake_out = 0.0
        policy_applied = True

    reason = "none"
    if throttle_active and brake_active:
        reason = "mutual_exclusion_overlap"
    elif hysteresis_held:
        reason = "hysteresis_hold"
    return {
        "throttle": throttle_out,
        "brake": brake_out,
        "policy_applied": policy_applied,
        "policy_reason": reason,
        "policy_state": selected_state,
        "desired_state": desired_state,
        "hysteresis_counter": counter,
        "hysteresis_held": hysteresis_held,
    }


def derive_longitudinal_targets(
    *,
    raw_fields: Dict[str, Any],
    raw_throttle: float,
    raw_brake: float,
    config: ControlMappingConfig,
) -> Dict[str, Any]:
    accel_candidates = {
        "acceleration": _safe_float(raw_fields.get("acceleration"), float("nan")),
        "debug_simple_lon_acceleration_cmd": _safe_float(
            raw_fields.get("debug_simple_lon_acceleration_cmd"), float("nan")
        ),
        "debug_simple_lon_acceleration_lookup": _safe_float(
            raw_fields.get("debug_simple_lon_acceleration_lookup"), float("nan")
        ),
    }
    target_accel = 0.0
    target_decel = 0.0
    accel_source = "percentage_scaled"
    decel_source = "percentage_scaled"
    selected_signed_field = ""
    for key in config.physical_acceleration_field_priority:
        value = accel_candidates.get(key, float("nan"))
        if key == "acceleration" and (not config.physical_use_top_level_acceleration):
            continue
        if key != "acceleration" and (not config.physical_use_lon_debug):
            continue
        if not math.isfinite(value):
            continue
        if value >= 0.0:
            target_accel = float(value)
            target_decel = 0.0
        else:
            target_accel = 0.0
            target_decel = abs(float(value))
        accel_source = "apollo.acceleration" if key == "acceleration" else key
        decel_source = accel_source
        selected_signed_field = key
        break
    if not selected_signed_field:
        target_accel = raw_throttle * config.physical_apollo_max_accel_mps2
        target_decel = raw_brake * config.physical_apollo_max_decel_mps2
    if raw_brake > raw_throttle and target_decel <= 1e-6:
        target_decel = raw_brake * config.physical_apollo_max_decel_mps2
        decel_source = "percentage_scaled"
    if raw_throttle >= raw_brake and target_accel <= 1e-6:
        target_accel = raw_throttle * config.physical_apollo_max_accel_mps2
        accel_source = "percentage_scaled"
    return {
        "target_accel_mps2": max(0.0, float(target_accel)),
        "target_decel_mps2": max(0.0, float(target_decel)),
        "target_accel_source": accel_source,
        "target_decel_source": decel_source,
        "selected_signed_acceleration_field": selected_signed_field or "",
        "top_level_acceleration_mps2": (
            accel_candidates["acceleration"] if math.isfinite(accel_candidates["acceleration"]) else None
        ),
        "debug_longitudinal_acceleration_cmd_mps2": (
            accel_candidates["debug_simple_lon_acceleration_cmd"]
            if math.isfinite(accel_candidates["debug_simple_lon_acceleration_cmd"])
            else None
        ),
        "debug_longitudinal_acceleration_lookup_mps2": (
            accel_candidates["debug_simple_lon_acceleration_lookup"]
            if math.isfinite(accel_candidates["debug_simple_lon_acceleration_lookup"])
            else None
        ),
        "debug_longitudinal_throttle_cmd_pct": _safe_float(
            raw_fields.get("debug_simple_lon_throttle_cmd"), float("nan")
        ),
        "debug_longitudinal_brake_cmd_pct": _safe_float(
            raw_fields.get("debug_simple_lon_brake_cmd"), float("nan")
        ),
    }


def physical_map_base_controls(
    *,
    raw_fields: Dict[str, Any],
    raw_throttle: float,
    raw_brake: float,
    raw_steer: float,
    now_sec: float,
    config: ControlMappingConfig,
    calibration: ActuatorCalibration,
    latest_speed_mps: float,
    apply_zero_hold: ZeroHoldApplier,
) -> Dict[str, Any]:
    legacy = legacy_map_base_controls(
        raw_throttle=raw_throttle,
        raw_brake=raw_brake,
        raw_steer=raw_steer,
        now_sec=now_sec,
        config=config,
        apply_zero_hold=apply_zero_hold,
    )
    if not calibration.loaded:
        legacy.update(
            {
                "mode": "physical",
                "throttle_source": "legacy_scale_fallback",
                "brake_source": "legacy_scale_fallback",
                "steer_source": "legacy_scale_fallback",
                "physical_fallback_reason": "calibration_missing",
            }
        )
        return legacy
    out = dict(legacy)
    out["mode"] = "physical"
    out["physical_fallback_reason"] = ""
    target_front_wheel_angle_deg = float(raw_steer) * float(config.physical_apollo_max_steer_angle_deg)
    mapped_steer = calibration.steering_cmd_for_angle(target_front_wheel_angle_deg)
    if mapped_steer is None and config.physical_allow_legacy_fallback:
        out["steer_source"] = "legacy_scale_fallback"
        out["physical_fallback_reason"] = "steering_inverse_missing"
    elif mapped_steer is not None:
        out["steer"] = _clamp(float(mapped_steer), -1.0, 1.0)
        out["steer_pre_clamp"] = float(mapped_steer)
        out["steer_clamped"] = abs(float(mapped_steer)) > 1.0
        out["steer_source"] = "physical_inverse_front_wheel_angle"
    targets = derive_longitudinal_targets(
        raw_fields=raw_fields,
        raw_throttle=raw_throttle,
        raw_brake=raw_brake,
        config=config,
    )
    speed_mps = max(0.0, float(latest_speed_mps))
    target_accel = float(targets["target_accel_mps2"])
    target_decel = float(targets["target_decel_mps2"])
    throttle_mapping = calibration.throttle_mapping_for_accel(
        target_accel,
        speed_mps=speed_mps,
        target_accel_max_mps2=config.physical_apollo_max_accel_mps2,
    )
    brake_mapping = calibration.brake_mapping_for_decel(target_decel, speed_mps=speed_mps)
    mapped_throttle = throttle_mapping.get("cmd")
    mapped_brake = brake_mapping.get("cmd")
    if mapped_throttle is None and config.physical_allow_legacy_fallback:
        out["throttle_source"] = "legacy_scale_fallback"
        if not out["physical_fallback_reason"]:
            out["physical_fallback_reason"] = "throttle_inverse_missing"
    elif mapped_throttle is not None:
        out["throttle"] = apply_zero_hold(_clamp(float(mapped_throttle), 0.0, 1.0), out["brake"], now_sec)
        out["throttle_source"] = str(throttle_mapping.get("source") or "physical_inverse_accel")
    if mapped_brake is None and config.physical_allow_legacy_fallback:
        out["brake_source"] = "legacy_scale_fallback"
        if not out["physical_fallback_reason"]:
            out["physical_fallback_reason"] = "brake_inverse_missing"
    elif mapped_brake is not None:
        out["brake"] = _clamp(float(mapped_brake), 0.0, 1.0)
        out["brake_before_deadzone"] = out["brake"]
        out["brake_after_deadzone"] = out["brake"]
        out["brake_source"] = str(brake_mapping.get("source") or "physical_inverse_decel")
    if out["brake"] > 0.0 and target_decel > target_accel:
        out["throttle"] = 0.0
    out.update(targets)
    out["target_front_wheel_angle_deg"] = target_front_wheel_angle_deg
    out["mapped_throttle_cmd"] = out["throttle"]
    out["mapped_brake_cmd"] = out["brake"]
    out["mapped_carla_steer_cmd"] = out["steer"]
    return out
