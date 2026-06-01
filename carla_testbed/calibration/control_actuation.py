from __future__ import annotations

import csv
import math
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from .profile import CalibrationProfile

CALIBRATION_TRIALS_SCHEMA_VERSION = "calibration_trials.v1"

CALIBRATION_TRIAL_FIELDS = [
    "trial_id",
    "command_type",
    "command_value",
    "start_time_s",
    "duration_s",
    "speed_initial_mps",
    "speed_final_mps",
    "accel_mean_mps2",
    "decel_mean_mps2",
    "yaw_rate_mean_rad_s",
    "yaw_rate_p95_rad_s",
    "latency_ms",
    "route_id",
    "backend",
    "notes",
]

COMMAND_TYPES = {"throttle", "brake", "steer"}


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(value for value in values if math.isfinite(value))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (len(cleaned) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return cleaned[lower]
    weight = rank - lower
    return cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight


def _rows_for(trials: Sequence[Mapping[str, Any]], command_type: str) -> list[Mapping[str, Any]]:
    return [row for row in trials if str(row.get("command_type") or "").strip() == command_type]


def _values(trials: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in trials:
        value = _num(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _latency_summary(trials: Sequence[Mapping[str, Any]]) -> tuple[float | None, float | None]:
    latencies = _values(trials, "latency_ms")
    return _percentile(latencies, 0.50), _percentile(latencies, 0.95)


def load_calibration_trials_csv(path: str | Path) -> list[dict[str, Any]]:
    trial_path = Path(path).expanduser()
    with trial_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, Any]] = []
        for raw in reader:
            row = {field: raw.get(field, "") for field in CALIBRATION_TRIAL_FIELDS}
            rows.append(row)
    return rows


def _fit_response(
    trials: Sequence[Mapping[str, Any]],
    *,
    command_field: str,
    response_field: str,
    absolute: bool = False,
) -> dict[str, Any] | None:
    pairs: list[tuple[float, float]] = []
    for row in trials:
        command = _num(row.get(command_field))
        response = _num(row.get(response_field))
        if command is None or response is None:
            continue
        if absolute:
            command = abs(command)
            response = abs(response)
        pairs.append((command, response))
    if not pairs:
        return None
    commands = [item[0] for item in pairs]
    responses = [item[1] for item in pairs]
    denom = sum(command * command for command in commands)
    gain = None if denom <= 0 else sum(command * response for command, response in pairs) / denom
    return {
        "sample_count": len(pairs),
        "mean_command_value": mean(commands),
        "mean_response": mean(responses),
        "gain_per_command": gain,
    }


def _steering_sign_verified(trials: Sequence[Mapping[str, Any]], expected_sign: int) -> bool | None:
    observations: list[bool] = []
    for row in trials:
        command = _num(row.get("command_value"))
        yaw = _num(row.get("yaw_rate_mean_rad_s"))
        if command is None or yaw is None or abs(command) < 1e-9 or abs(yaw) < 1e-9:
            continue
        observations.append(math.copysign(1.0, yaw) == math.copysign(float(expected_sign), command))
    if not observations:
        return None
    return all(observations)


def analyze_control_actuation_trials(
    trials: Sequence[Mapping[str, Any]],
    *,
    profile: CalibrationProfile,
) -> tuple[dict[str, Any], list[str], list[str]]:
    missing_fields: list[str] = []
    warnings: list[str] = []

    unknown_types = sorted(
        {
            str(row.get("command_type") or "")
            for row in trials
            if str(row.get("command_type") or "") and str(row.get("command_type") or "") not in COMMAND_TYPES
        }
    )
    if unknown_types:
        warnings.append(f"unknown command_type values ignored: {', '.join(unknown_types)}")

    throttle_trials = _rows_for(trials, "throttle")
    brake_trials = _rows_for(trials, "brake")
    steer_trials = _rows_for(trials, "steer")

    if not throttle_trials:
        missing_fields.append("throttle_trials")
    if not brake_trials:
        missing_fields.append("brake_trials")
    if not steer_trials:
        missing_fields.append("steer_trials")

    for command_type, command_trials in [
        ("throttle", throttle_trials),
        ("brake", brake_trials),
        ("steer", steer_trials),
    ]:
        if command_trials and not _values(command_trials, "latency_ms"):
            warnings.append(f"{command_type} latency_ms missing")

    throttle_p50, throttle_p95 = _latency_summary(throttle_trials)
    brake_p50, brake_p95 = _latency_summary(brake_trials)
    steer_p50, steer_p95 = _latency_summary(steer_trials)
    accel_fit = _fit_response(throttle_trials, command_field="command_value", response_field="accel_mean_mps2")
    decel_fit = _fit_response(brake_trials, command_field="command_value", response_field="decel_mean_mps2")
    yaw_rate_fit = _fit_response(
        steer_trials,
        command_field="command_value",
        response_field="yaw_rate_mean_rad_s",
        absolute=True,
    )
    steering_sign_verified = _steering_sign_verified(steer_trials, profile.control_mapping.steering_sign)
    steer_supported = bool(yaw_rate_fit and steering_sign_verified is not False)
    legacy_scale_supported = (
        profile.control_mapping.actuator_mapping_mode == "legacy"
        and abs(profile.control_mapping.steer_scale - 0.25) < 1e-9
        and steer_supported
    )

    results = {
        "throttle_response": {
            "latency_ms_p50": throttle_p50,
            "latency_ms_p95": throttle_p95,
            "accel_fit": accel_fit,
            "supported": bool(accel_fit),
        },
        "brake_response": {
            "latency_ms_p50": brake_p50,
            "latency_ms_p95": brake_p95,
            "decel_fit": decel_fit,
            "supported": bool(decel_fit),
        },
        "steer_response": {
            "steering_sign_verified": steering_sign_verified,
            "legacy_steer_scale_025_supported": legacy_scale_supported,
            "yaw_rate_fit": yaw_rate_fit,
            "latency_ms_p50": steer_p50,
            "latency_ms_p95": steer_p95,
            "supported": steer_supported,
        },
    }
    return results, sorted(set(missing_fields)), warnings
