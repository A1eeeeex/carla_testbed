from __future__ import annotations

import math
import statistics
from typing import Any, Mapping, Sequence


STEERING_RESPONSE_SCHEMA_VERSION = "carla_steering_response.v1"


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _median(values: Sequence[float]) -> float | None:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    return float(statistics.median(finite)) if finite else None


def _percentile(values: Sequence[float], q: float) -> float | None:
    finite = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not finite:
        return None
    if len(finite) == 1:
        return finite[0]
    rank = min(1.0, max(0.0, float(q))) * (len(finite) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return finite[lo]
    weight = rank - lo
    return finite[lo] * (1.0 - weight) + finite[hi] * weight


def _crossing_time(
    rows: Sequence[tuple[float, float]],
    *,
    threshold: float,
) -> float | None:
    for elapsed, progress in rows:
        if progress >= threshold:
            return float(elapsed)
    return None


def analyze_steering_step_response(
    rows: Sequence[Mapping[str, Any]],
    *,
    output_field: str = "measured_steer_deg",
    settle_band_ratio: float = 0.05,
    min_response_amplitude_deg: float = 0.10,
) -> dict[str, Any]:
    """Analyze one zero-to-command steering step.

    Rows before the step use negative ``elapsed_sec``; rows at/after the command
    use non-negative values.  Times are simulation seconds.  The result is a
    diagnostic actuator-response measurement, not a Phase 1 behavior verdict.
    """

    samples: list[tuple[float, float]] = []
    for row in rows:
        elapsed = _number(row.get("elapsed_sec"))
        output = _number(row.get(output_field))
        if elapsed is not None and output is not None:
            samples.append((elapsed, output))
    samples.sort(key=lambda item: item[0])
    baseline_values = [output for elapsed, output in samples if elapsed < 0.0]
    response_rows = [(elapsed, output) for elapsed, output in samples if elapsed >= 0.0]
    missing: list[str] = []
    if len(baseline_values) < 2:
        missing.append("baseline_samples")
    if len(response_rows) < 4:
        missing.append("response_samples")
    if missing:
        return {
            "schema_version": STEERING_RESPONSE_SCHEMA_VERSION,
            "status": "insufficient_data",
            "missing_fields": missing,
            "sample_count": len(samples),
        }

    baseline = _median(baseline_values)
    tail_count = max(3, int(math.ceil(len(response_rows) * 0.25)))
    steady = _median([output for _, output in response_rows[-tail_count:]])
    if baseline is None or steady is None:
        return {
            "schema_version": STEERING_RESPONSE_SCHEMA_VERSION,
            "status": "insufficient_data",
            "missing_fields": ["baseline_or_steady_response"],
            "sample_count": len(samples),
        }
    amplitude = steady - baseline
    if abs(amplitude) < float(min_response_amplitude_deg):
        return {
            "schema_version": STEERING_RESPONSE_SCHEMA_VERSION,
            "status": "insufficient_data",
            "missing_fields": ["measurable_response_amplitude"],
            "sample_count": len(samples),
            "baseline_output": baseline,
            "steady_output": steady,
            "response_amplitude": amplitude,
        }

    progress_rows = [
        (elapsed, (output - baseline) / amplitude) for elapsed, output in response_rows
    ]
    baseline_noise = _percentile([abs(value - baseline) for value in baseline_values], 0.95) or 0.0
    dead_progress = max(0.02, min(0.20, (3.0 * baseline_noise) / abs(amplitude)))
    dead_time = _crossing_time(progress_rows, threshold=dead_progress)
    t10 = _crossing_time(progress_rows, threshold=0.10)
    t50 = _crossing_time(progress_rows, threshold=0.50)
    t90 = _crossing_time(progress_rows, threshold=0.90)
    rise_time = None if t10 is None or t90 is None else max(0.0, t90 - t10)

    peak_elapsed, peak_progress = max(progress_rows, key=lambda item: item[1])
    overshoot_ratio = max(0.0, float(peak_progress) - 1.0)
    peak_time = float(peak_elapsed) if overshoot_ratio > 0.01 else t90
    settling_tolerance = max(
        abs(amplitude) * float(settle_band_ratio),
        3.0 * baseline_noise,
        0.02,
    )
    settling_time: float | None = None
    for index, (elapsed, output) in enumerate(response_rows):
        if t90 is not None and elapsed < t90:
            continue
        if all(abs(later_output - steady) <= settling_tolerance for _, later_output in response_rows[index:]):
            settling_time = float(elapsed)
            break

    intervals = [
        response_rows[index][0] - response_rows[index - 1][0]
        for index in range(1, len(response_rows))
        if response_rows[index][0] > response_rows[index - 1][0]
    ]
    sample_interval = _median(intervals)
    temporal_resolution_status = (
        "resolved"
        if rise_time is not None
        and sample_interval is not None
        and rise_time >= 0.5 * sample_interval
        else "interval_censored"
    )
    return {
        "schema_version": STEERING_RESPONSE_SCHEMA_VERSION,
        "status": "pass" if all(value is not None for value in (dead_time, rise_time, settling_time)) else "warn",
        "claim_boundary": "diagnostic_actuator_response_only",
        "sample_count": len(samples),
        "baseline_sample_count": len(baseline_values),
        "response_sample_count": len(response_rows),
        "sample_interval_sec_median": sample_interval,
        "crossing_time_semantics": "first_observed_sample_at_or_above_threshold",
        "temporal_resolution_status": temporal_resolution_status,
        "baseline_output": baseline,
        "baseline_noise_abs_p95": baseline_noise,
        "steady_output": steady,
        "response_amplitude": amplitude,
        "dead_time_threshold_ratio": dead_progress,
        "dead_time_sec": dead_time,
        "time_to_10_percent_sec": t10,
        "time_to_50_percent_sec": t50,
        "time_to_90_percent_sec": t90,
        "rise_time_10_90_sec": rise_time,
        "peak_time_sec": peak_time,
        "peak_response_ratio": float(peak_progress),
        "overshoot_ratio": overshoot_ratio,
        "settling_band_abs": settling_tolerance,
        "settling_time_sec": settling_time,
        "missing_fields": [],
    }


def summarize_steering_step_trials(trials: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    valid = [
        trial
        for trial in trials
        if isinstance(trial.get("response"), Mapping)
        and str((trial.get("response") or {}).get("status")) in {"pass", "warn"}
        and str((trial.get("speed_gate") or {}).get("status")) == "pass"
    ]

    def values(field: str) -> list[float]:
        result: list[float] = []
        for trial in valid:
            value = _number((trial.get("response") or {}).get(field))
            if value is not None:
                result.append(value)
        return result

    directions = {
        -1 if (_number(trial.get("target_front_wheel_angle_deg")) or 0.0) < 0.0 else 1
        for trial in valid
        if abs(_number(trial.get("target_front_wheel_angle_deg")) or 0.0) > 1e-9
    }
    dead_times = values("dead_time_sec")
    rise_times = values("rise_time_10_90_sec")
    peak_times = values("peak_time_sec")
    settling_times = values("settling_time_sec")
    sample_intervals = values("sample_interval_sec_median")
    target_tracking_ratios: list[float] = []
    target_tracking_errors_deg: list[float] = []
    for trial in valid:
        target = _number(trial.get("target_front_wheel_angle_deg"))
        amplitude = _number((trial.get("response") or {}).get("response_amplitude"))
        if target is None or amplitude is None or abs(target) <= 1e-9:
            continue
        target_tracking_ratios.append(abs(amplitude) / abs(target))
        target_tracking_errors_deg.append(abs(amplitude) - abs(target))
    temporally_resolved_count = sum(
        1
        for trial in valid
        if str((trial.get("response") or {}).get("temporal_resolution_status")) == "resolved"
    )
    sufficient = bool(
        len(valid) >= 4
        and directions == {-1, 1}
        and len(dead_times) >= 4
        and len(rise_times) >= 4
        and len(settling_times) >= 4
    )
    latency_parameters_resolved = sufficient and temporally_resolved_count == len(valid)
    rejected_speed_trial_count = sum(
        1 for trial in trials if str((trial.get("speed_gate") or {}).get("status")) != "pass"
    )
    return {
        "schema_version": STEERING_RESPONSE_SCHEMA_VERSION,
        "status": "pass" if sufficient else "insufficient_data",
        "claim_boundary": {
            "control_owner": "diagnostic_carla_direct",
            "apollo_control_in_loop": False,
            "eligible_for_phase1_status": False,
            "eligible_for_apollo_capability_claim": False,
        },
        "trial_count": len(trials),
        "valid_trial_count": len(valid),
        "rejected_speed_trial_count": rejected_speed_trial_count,
        "directions_observed": sorted(directions),
        "temporally_resolved_trial_count": temporally_resolved_count,
        "dead_time_sec_median": _median(dead_times),
        "dead_time_sec_p95": _percentile(dead_times, 0.95),
        "rise_time_10_90_sec_median": _median(rise_times),
        "rise_time_10_90_sec_p95": _percentile(rise_times, 0.95),
        "peak_time_sec_median": _median(peak_times),
        "settling_time_sec_median": _median(settling_times),
        "settling_time_sec_p95": _percentile(settling_times, 0.95),
        "front_wheel_target_tracking_ratio_median": _median(target_tracking_ratios),
        "front_wheel_target_tracking_ratio_p05": _percentile(target_tracking_ratios, 0.05),
        "front_wheel_target_tracking_ratio_p95": _percentile(target_tracking_ratios, 0.95),
        "front_wheel_target_tracking_error_deg_median": _median(target_tracking_errors_deg),
        "apollo_latency_param_candidate": {
            "status": (
                "diagnostic_only"
                if latency_parameters_resolved
                else "insufficient_temporal_resolution"
                if sufficient
                else "insufficient_data"
            ),
            "units": "seconds",
            "dead_time": _median(dead_times) if latency_parameters_resolved else None,
            "rise_time": _median(rise_times) if latency_parameters_resolved else None,
            "peak_time": _median(peak_times) if latency_parameters_resolved else None,
            "settling_time": _median(settling_times) if latency_parameters_resolved else None,
            "dead_time_upper_bound_sec": (
                _median(sample_intervals) if sufficient and not latency_parameters_resolved else None
            ),
            "rise_time_upper_bound_sec": (
                _median(sample_intervals) if sufficient and not latency_parameters_resolved else None
            ),
            "promotion_allowed": False,
            "required_next_evidence": [
                "repeatability_across_independent_probe_runs",
                "profile_scoped_apollo_closed_loop_ab",
                "town01_no_regression_before_any_promotion",
            ],
        },
    }


def interpolate_steering_curve(
    points: Sequence[Mapping[str, Any]],
    *,
    speed_axis_value: float,
) -> float | None:
    parsed: list[tuple[float, float]] = []
    for point in points:
        speed = _number(point.get("speed_axis_value"))
        scale = _number(point.get("max_steer_scale"))
        if speed is not None and scale is not None:
            parsed.append((speed, scale))
    parsed.sort(key=lambda item: item[0])
    if not parsed:
        return None
    target = float(speed_axis_value)
    if target <= parsed[0][0]:
        return parsed[0][1]
    if target >= parsed[-1][0]:
        return parsed[-1][1]
    for index in range(1, len(parsed)):
        right_x, right_y = parsed[index]
        left_x, left_y = parsed[index - 1]
        if target <= right_x:
            span = right_x - left_x
            if span <= 1e-12:
                return right_y
            fraction = (target - left_x) / span
            return left_y + fraction * (right_y - left_y)
    return parsed[-1][1]


def analyze_speed_dependent_steering_authority(
    trials: Sequence[Mapping[str, Any]],
    *,
    steering_curve: Sequence[Mapping[str, Any]],
    max_steer_angle_deg: float,
) -> dict[str, Any]:
    """Compare measured wheel authority with the vehicle physics steering curve.

    CARLA exposes the curve points without an axis-unit label in its Python
    API.  This diagnostic compares m/s, km/h, and mph interpretations against
    measured front-wheel response.  It never authorizes mapping promotion.
    """

    max_angle = abs(float(max_steer_angle_deg))
    observed: list[dict[str, float]] = []
    distinct_target_speeds: set[float] = set()
    for trial in trials:
        response = trial.get("response") or {}
        speed_gate = trial.get("speed_gate") or {}
        if not isinstance(response, Mapping) or not isinstance(speed_gate, Mapping):
            continue
        if str(response.get("status")) not in {"pass", "warn"}:
            continue
        if str(speed_gate.get("status")) != "pass":
            continue
        speed_mps = _number(trial.get("speed_at_step_mps"))
        target_speed_mps = _number(trial.get("target_speed_mps"))
        command = _number(trial.get("carla_steer_cmd"))
        amplitude = _number(response.get("response_amplitude"))
        if (
            speed_mps is None
            or target_speed_mps is None
            or command is None
            or amplitude is None
            or max_angle <= 1e-9
            or abs(command) <= 1e-9
        ):
            continue
        observed_fraction = abs(amplitude) / (abs(command) * max_angle)
        observed.append(
            {
                "target_speed_mps": target_speed_mps,
                "observed_speed_mps": speed_mps,
                "carla_steer_cmd": command,
                "response_amplitude_deg": amplitude,
                "observed_max_steer_fraction": observed_fraction,
            }
        )
        distinct_target_speeds.add(round(target_speed_mps, 6))

    speed_axis_candidates = {
        "mps": 1.0,
        "kph": 3.6,
        "mph": 2.2369362920544,
    }
    fits: list[dict[str, Any]] = []
    for unit, factor in speed_axis_candidates.items():
        errors: list[float] = []
        predicted_rows: list[dict[str, float]] = []
        for row in observed:
            axis_value = row["observed_speed_mps"] * factor
            predicted = interpolate_steering_curve(
                steering_curve,
                speed_axis_value=axis_value,
            )
            if predicted is None:
                continue
            error = row["observed_max_steer_fraction"] - predicted
            errors.append(abs(error))
            predicted_rows.append(
                {
                    "observed_speed_mps": row["observed_speed_mps"],
                    "speed_axis_value": axis_value,
                    "observed_max_steer_fraction": row["observed_max_steer_fraction"],
                    "predicted_max_steer_fraction": predicted,
                    "signed_error": error,
                }
            )
        fits.append(
            {
                "speed_axis_unit": unit,
                "speed_axis_factor_from_mps": factor,
                "sample_count": len(errors),
                "absolute_error_median": _median(errors),
                "absolute_error_p95": _percentile(errors, 0.95),
                "rows": predicted_rows,
            }
        )
    ranked = sorted(
        (fit for fit in fits if fit["absolute_error_median"] is not None),
        key=lambda fit: float(fit["absolute_error_median"]),
    )
    best = ranked[0] if ranked else None
    runner_up = ranked[1] if len(ranked) > 1 else None
    best_error = _number((best or {}).get("absolute_error_median"))
    runner_up_error = _number((runner_up or {}).get("absolute_error_median"))
    sufficient = len(observed) >= 6 and len(distinct_target_speeds) >= 3 and bool(steering_curve)
    unit_resolved = bool(
        sufficient
        and best_error is not None
        and best_error <= 0.03
        and runner_up_error is not None
        and runner_up_error - best_error >= 0.02
    )
    return {
        "schema_version": STEERING_RESPONSE_SCHEMA_VERSION,
        "status": "pass" if sufficient else "insufficient_data",
        "claim_boundary": {
            "control_owner": "diagnostic_carla_direct",
            "apollo_control_in_loop": False,
            "eligible_for_phase1_status": False,
            "eligible_for_apollo_capability_claim": False,
            "promotion_allowed": False,
        },
        "max_steer_angle_deg": max_angle,
        "steering_curve": [dict(point) for point in steering_curve],
        "valid_trial_count": len(observed),
        "distinct_target_speed_count": len(distinct_target_speeds),
        "observed_max_steer_fraction_median": _median(
            [row["observed_max_steer_fraction"] for row in observed]
        ),
        "speed_axis_unit_identification": {
            "status": "resolved" if unit_resolved else "diagnostic_only",
            "best_fit_unit": (best or {}).get("speed_axis_unit"),
            "best_fit_absolute_error_median": best_error,
            "runner_up_unit": (runner_up or {}).get("speed_axis_unit"),
            "runner_up_absolute_error_median": runner_up_error,
        },
        "candidate_fits": fits,
        "observed_trials": observed,
    }
