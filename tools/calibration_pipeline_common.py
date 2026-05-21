from __future__ import annotations

import csv
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_CAPTURE_POLICY: Dict[str, Any] = {
    "min_jsonl_rows": 20,
    "min_capture_samples": 20,
    "min_capture_duration_sec": 5.0,
    "min_nonzero_steer_samples": 5,
    "min_positive_accel_samples": 5,
    "min_decel_samples": 5,
    "max_dead_sequence_ratio": 0.85,
    "require_summary": True,
    "min_summary_size_bytes": 2,
    "timestamp_monotonic_required": True,
    "target_zero_epsilon_steer_deg": 0.05,
    "target_zero_epsilon_accel_mps2": 0.05,
    "target_zero_epsilon_decel_mps2": 0.10,
    "brake_constant_stddev_epsilon": 0.05,
    "apollo_max_steer_angle_deg": 8.203,
    "apollo_max_accel_mps2": 4.0,
    "apollo_max_decel_mps2": 6.0,
}


DEFAULT_COMPARISON_POLICY: Dict[str, Any] = {
    "replay_max_mean_abs_steer_error_deg": 1.50,
    "replay_max_mean_abs_accel_error_mps2": 0.80,
    "replay_max_mean_abs_decel_error_mps2": 1.00,
    "tracking_max_steering_mae_deg": 1.20,
    "tracking_max_throttle_mae_mps2": 0.80,
    "tracking_max_brake_mae_mps2": 1.00,
    "improvement_epsilon_deg": 0.05,
    "improvement_epsilon_mps2": 0.05,
    "saturation_ratio_tolerance": 0.02,
    "weird_output_ratio_tolerance": 0.01,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        out = float(value)
    except Exception:
        return float(default)
    if math.isnan(out) or math.isinf(out):
        return float(default)
    return float(out)


def _median(values: Iterable[float], default: float = 0.0) -> float:
    buf = [float(item) for item in values]
    if not buf:
        return float(default)
    return float(statistics.median(buf))


def _mean(values: Iterable[float], default: float = 0.0) -> float:
    buf = [float(item) for item in values]
    if not buf:
        return float(default)
    return float(sum(buf) / len(buf))


def _stddev(values: Iterable[float], default: float = 0.0) -> float:
    buf = [float(item) for item in values]
    if len(buf) <= 1:
        return float(default)
    return float(statistics.pstdev(buf))


def merge_policy(defaults: Dict[str, Any], overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = dict(defaults)
    for key, value in (overrides or {}).items():
        out[key] = value
    return out


def load_jsonl_records(path: Optional[Path]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    nonempty_lines = 0
    if path is None or (not path.exists()):
        return {
            "path": str(path) if path is not None else "",
            "exists": False,
            "rows": rows,
            "error_count": 0,
            "nonempty_lines": 0,
            "parse_success": False,
            "errors": [],
        }
    with path.open() as fp:
        for index, line in enumerate(fp, start=1):
            text = line.strip()
            if not text:
                continue
            nonempty_lines += 1
            try:
                payload = json.loads(text)
            except Exception as exc:
                errors.append(f"line {index}: {exc}")
                continue
            if isinstance(payload, dict):
                rows.append(payload)
            else:
                errors.append(f"line {index}: non-dict-json")
    return {
        "path": str(path),
        "exists": True,
        "rows": rows,
        "error_count": len(errors),
        "nonempty_lines": nonempty_lines,
        "parse_success": bool(rows) and (len(errors) == 0),
        "errors": errors[:20],
    }


def load_csv_records(path: Optional[Path]) -> List[Dict[str, Any]]:
    if path is None or (not path.exists()):
        return []
    with path.open(newline="") as fp:
        return [dict(row) for row in csv.DictReader(fp)]


def _raw_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = row.get("apollo_control_raw")
    return payload if isinstance(payload, dict) else {}


def _select_raw_steer_pct(raw_fields: Dict[str, Any]) -> float:
    for key in ("steering_target", "steering_percentage", "steering", "steering_rate"):
        if key in raw_fields:
            return _safe_float(raw_fields.get(key), 0.0)
    return 0.0


def _derive_fallback_longitudinal_targets(
    raw_fields: Dict[str, Any],
    *,
    apollo_max_accel_mps2: float,
    apollo_max_decel_mps2: float,
) -> Dict[str, Any]:
    raw_throttle = max(0.0, min(1.0, _safe_float(raw_fields.get("throttle"), 0.0) / 100.0))
    raw_brake = max(0.0, min(1.0, _safe_float(raw_fields.get("brake"), 0.0) / 100.0))
    signed_candidates = (
        "acceleration",
        "debug_simple_lon_acceleration_cmd",
        "debug_simple_lon_acceleration_lookup",
    )
    selected_signed = ""
    target_accel = 0.0
    target_decel = 0.0
    for key in signed_candidates:
        value = raw_fields.get(key)
        if value is None or value == "":
            continue
        signed = _safe_float(value, 0.0)
        selected_signed = key
        if signed >= 0.0:
            target_accel = float(signed)
            target_decel = 0.0
        else:
            target_accel = 0.0
            target_decel = abs(float(signed))
        break
    if not selected_signed:
        target_accel = raw_throttle * float(apollo_max_accel_mps2)
        target_decel = raw_brake * float(apollo_max_decel_mps2)
    if raw_brake > raw_throttle and target_decel <= 1e-6:
        target_decel = raw_brake * float(apollo_max_decel_mps2)
    if raw_throttle >= raw_brake and target_accel <= 1e-6:
        target_accel = raw_throttle * float(apollo_max_accel_mps2)
    return {
        "target_accel_mps2": max(0.0, float(target_accel)),
        "target_decel_mps2": max(0.0, float(target_decel)),
        "selected_signed_acceleration_field": selected_signed,
        "raw_throttle": raw_throttle,
        "raw_brake": raw_brake,
    }


def target_sequence_from_decode_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seq: List[Dict[str, Any]] = []
    for row in rows:
        ts_sec = _safe_float(row.get("ts_sec"), float("nan"))
        if not math.isfinite(ts_sec):
            continue
        seq.append(
            {
                "ts_sec": float(ts_sec),
                "selected_steering_field": str(row.get("selected_steering_field", "") or ""),
                "selected_signed_acceleration_field": str(
                    row.get("selected_signed_acceleration_field", "") or ""
                ),
                "target_front_wheel_angle_deg": _safe_float(
                    row.get("target_front_wheel_angle_deg"), 0.0
                ),
                "target_accel_mps2": max(0.0, _safe_float(row.get("target_accel_mps2"), 0.0)),
                "target_decel_mps2": max(0.0, _safe_float(row.get("target_decel_mps2"), 0.0)),
                "mapped_carla_steer_cmd": _safe_float(row.get("mapped_carla_steer_cmd"), 0.0),
                "mapped_throttle_cmd": _safe_float(row.get("mapped_throttle_cmd"), 0.0),
                "mapped_brake_cmd": _safe_float(row.get("mapped_brake_cmd"), 0.0),
                "physical_fallback_reason": str(row.get("physical_fallback_reason", "") or ""),
            }
        )
    return seq


def target_sequence_from_raw_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    policy: Dict[str, Any],
) -> List[Dict[str, Any]]:
    seq: List[Dict[str, Any]] = []
    apollo_max_steer_angle_deg = float(policy.get("apollo_max_steer_angle_deg", 8.203) or 8.203)
    apollo_max_accel_mps2 = float(policy.get("apollo_max_accel_mps2", 4.0) or 4.0)
    apollo_max_decel_mps2 = float(policy.get("apollo_max_decel_mps2", 6.0) or 6.0)
    for row in rows:
        ts_sec = _safe_float(row.get("ts_sec"), float("nan"))
        if not math.isfinite(ts_sec):
            continue
        raw_fields = _raw_payload(row)
        if not raw_fields:
            continue
        steer_pct = _select_raw_steer_pct(raw_fields)
        raw_steer = max(-1.0, min(1.0, float(steer_pct) / 100.0))
        lon_targets = _derive_fallback_longitudinal_targets(
            raw_fields,
            apollo_max_accel_mps2=apollo_max_accel_mps2,
            apollo_max_decel_mps2=apollo_max_decel_mps2,
        )
        seq.append(
            {
                "ts_sec": float(ts_sec),
                "selected_steering_field": str(row.get("selected_steering_field", "") or ""),
                "selected_signed_acceleration_field": str(
                    lon_targets.get("selected_signed_acceleration_field", "") or ""
                ),
                "target_front_wheel_angle_deg": raw_steer * apollo_max_steer_angle_deg,
                "target_accel_mps2": float(lon_targets["target_accel_mps2"]),
                "target_decel_mps2": float(lon_targets["target_decel_mps2"]),
                "mapped_carla_steer_cmd": raw_steer,
                "mapped_throttle_cmd": _safe_float(lon_targets.get("raw_throttle"), 0.0),
                "mapped_brake_cmd": _safe_float(lon_targets.get("raw_brake"), 0.0),
                "physical_fallback_reason": "",
            }
        )
    return seq


def evaluate_target_sequence(
    sequence: Sequence[Dict[str, Any]],
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    policy = merge_policy(DEFAULT_CAPTURE_POLICY, policy)
    if not sequence:
        return {
            "sample_count": 0,
            "duration_sec": 0.0,
            "timestamps_monotonic": False,
            "nonzero_steer_count": 0,
            "positive_accel_count": 0,
            "positive_decel_count": 0,
            "max_abs_steer_deg": 0.0,
            "max_target_accel_mps2": 0.0,
            "max_target_decel_mps2": 0.0,
            "zero_intent_ratio": 1.0,
            "dead_sequence_ratio": 1.0,
            "dead_sequence_detected": True,
            "steer_all_zero": True,
            "accel_all_zero": True,
            "brake_constant_locked": False,
            "mapped_steer_saturation_ratio": 0.0,
            "weird_output_ratio": 0.0,
            "physical_fallback_count": 0,
            "sequence_valid": False,
            "sequence_invalid_reasons": ["no_samples"],
        }
    steer_eps = float(policy.get("target_zero_epsilon_steer_deg", 0.05) or 0.05)
    accel_eps = float(policy.get("target_zero_epsilon_accel_mps2", 0.05) or 0.05)
    decel_eps = float(policy.get("target_zero_epsilon_decel_mps2", 0.10) or 0.10)
    brake_constant_eps = float(policy.get("brake_constant_stddev_epsilon", 0.05) or 0.05)
    timestamps = [_safe_float(item.get("ts_sec"), float("nan")) for item in sequence]
    finite_timestamps = [ts for ts in timestamps if math.isfinite(ts)]
    monotonic = all(
        finite_timestamps[idx] >= finite_timestamps[idx - 1]
        for idx in range(1, len(finite_timestamps))
    )
    duration_sec = (
        max(finite_timestamps) - min(finite_timestamps)
        if len(finite_timestamps) >= 2
        else 0.0
    )
    abs_steers = [abs(_safe_float(item.get("target_front_wheel_angle_deg"), 0.0)) for item in sequence]
    accels = [max(0.0, _safe_float(item.get("target_accel_mps2"), 0.0)) for item in sequence]
    decels = [max(0.0, _safe_float(item.get("target_decel_mps2"), 0.0)) for item in sequence]
    nonzero_steer_count = sum(1 for value in abs_steers if value > steer_eps)
    positive_accel_count = sum(1 for value in accels if value > accel_eps)
    positive_decel_count = sum(1 for value in decels if value > decel_eps)
    zero_intent_count = sum(
        1
        for steer, accel, decel in zip(abs_steers, accels, decels)
        if steer <= steer_eps and accel <= accel_eps and decel <= decel_eps
    )
    zero_intent_ratio = float(zero_intent_count / max(1, len(sequence)))
    brake_active_values = [value for value in decels if value > decel_eps]
    brake_active_ratio = float(len(brake_active_values)) / max(1, len(sequence))
    brake_constant_locked = bool(brake_active_values) and brake_active_ratio >= 0.80 and (
        _stddev(brake_active_values) <= brake_constant_eps
    )
    mapped_steers = [abs(_safe_float(item.get("mapped_carla_steer_cmd"), 0.0)) for item in sequence]
    mapped_throttles = [_safe_float(item.get("mapped_throttle_cmd"), 0.0) for item in sequence]
    mapped_brakes = [_safe_float(item.get("mapped_brake_cmd"), 0.0) for item in sequence]
    saturation_ratio = (
        sum(1 for value in mapped_steers if value >= 0.98) / max(1, len(mapped_steers))
    )
    weird_output_ratio = (
        sum(
            1
            for throttle, brake, steer, target_steer in zip(
                mapped_throttles,
                mapped_brakes,
                mapped_steers,
                abs_steers,
            )
            if (throttle > 0.10 and brake > 0.10) or (steer >= 0.98 and target_steer <= steer_eps)
        )
        / max(1, len(sequence))
    )
    physical_fallback_count = sum(
        1 for item in sequence if str(item.get("physical_fallback_reason", "") or "").strip()
    )
    dead_ratio = zero_intent_ratio
    no_excitation_detected = bool(
        nonzero_steer_count == 0 and positive_accel_count == 0 and positive_decel_count == 0
    )
    brake_lock_without_other_excitation = bool(
        brake_constant_locked and nonzero_steer_count == 0 and positive_accel_count == 0
    )
    dead_sequence_detected = bool(
        dead_ratio > float(policy.get("max_dead_sequence_ratio", 0.85) or 0.85)
        or no_excitation_detected
        or brake_lock_without_other_excitation
    )
    reasons: List[str] = []
    if int(len(sequence)) < int(policy.get("min_capture_samples", 20) or 20):
        reasons.append("insufficient_samples")
    if float(duration_sec) < float(policy.get("min_capture_duration_sec", 5.0) or 5.0):
        reasons.append("insufficient_duration")
    if bool(policy.get("timestamp_monotonic_required", True)) and (not monotonic):
        reasons.append("timestamp_not_monotonic")
    if no_excitation_detected:
        reasons.append("no_excitation_detected")
    if dead_sequence_detected:
        reasons.append("dead_sequence_detected")
    return {
        "sample_count": len(sequence),
        "duration_sec": float(duration_sec),
        "timestamps_monotonic": bool(monotonic),
        "nonzero_steer_count": int(nonzero_steer_count),
        "positive_accel_count": int(positive_accel_count),
        "positive_decel_count": int(positive_decel_count),
        "max_abs_steer_deg": float(max(abs_steers) if abs_steers else 0.0),
        "max_target_accel_mps2": float(max(accels) if accels else 0.0),
        "max_target_decel_mps2": float(max(decels) if decels else 0.0),
        "zero_intent_ratio": float(zero_intent_ratio),
        "dead_sequence_ratio": float(dead_ratio),
        "dead_sequence_detected": bool(dead_sequence_detected),
        "steer_all_zero": bool(nonzero_steer_count == 0),
        "accel_all_zero": bool(positive_accel_count == 0),
        "brake_constant_locked": bool(brake_constant_locked),
        "mapped_steer_saturation_ratio": float(saturation_ratio),
        "weird_output_ratio": float(weird_output_ratio),
        "physical_fallback_count": int(physical_fallback_count),
        "sequence_valid": not reasons,
        "sequence_invalid_reasons": reasons,
    }


def merge_sequence_coverages(items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    merged = {
        "sample_count": 0,
        "duration_sec": 0.0,
        "nonzero_steer_count": 0,
        "positive_accel_count": 0,
        "positive_decel_count": 0,
        "max_abs_steer_deg": 0.0,
        "max_target_accel_mps2": 0.0,
        "max_target_decel_mps2": 0.0,
        "dead_sequence_ratio": 0.0,
        "mapped_steer_saturation_ratio": 0.0,
        "weird_output_ratio": 0.0,
        "physical_fallback_count": 0,
    }
    if not items:
        return merged
    for item in items:
        merged["sample_count"] += int(item.get("sample_count", 0) or 0)
        merged["duration_sec"] += float(item.get("duration_sec", 0.0) or 0.0)
        merged["nonzero_steer_count"] += int(item.get("nonzero_steer_count", 0) or 0)
        merged["positive_accel_count"] += int(item.get("positive_accel_count", 0) or 0)
        merged["positive_decel_count"] += int(item.get("positive_decel_count", 0) or 0)
        merged["max_abs_steer_deg"] = max(
            float(merged["max_abs_steer_deg"]),
            _safe_float(item.get("max_abs_steer_deg"), 0.0),
        )
        merged["max_target_accel_mps2"] = max(
            float(merged["max_target_accel_mps2"]),
            _safe_float(item.get("max_target_accel_mps2"), 0.0),
        )
        merged["max_target_decel_mps2"] = max(
            float(merged["max_target_decel_mps2"]),
            _safe_float(item.get("max_target_decel_mps2"), 0.0),
        )
        merged["physical_fallback_count"] += int(item.get("physical_fallback_count", 0) or 0)
    merged["dead_sequence_ratio"] = _mean(
        (float(item.get("dead_sequence_ratio", 0.0) or 0.0) for item in items),
        default=0.0,
    )
    merged["mapped_steer_saturation_ratio"] = _mean(
        (float(item.get("mapped_steer_saturation_ratio", 0.0) or 0.0) for item in items),
        default=0.0,
    )
    merged["weird_output_ratio"] = _mean(
        (float(item.get("weird_output_ratio", 0.0) or 0.0) for item in items),
        default=0.0,
    )
    return merged


def coverage_sufficient(
    coverage: Dict[str, Any],
    policy: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, List[str]]:
    policy = merge_policy(DEFAULT_CAPTURE_POLICY, policy)
    reasons: List[str] = []
    if int(coverage.get("sample_count", 0) or 0) < int(policy.get("min_capture_samples", 20) or 20):
        reasons.append("aggregate_samples_below_threshold")
    if float(coverage.get("duration_sec", 0.0) or 0.0) < float(
        policy.get("min_capture_duration_sec", 5.0) or 5.0
    ):
        reasons.append("aggregate_duration_below_threshold")
    if int(coverage.get("nonzero_steer_count", 0) or 0) < int(
        policy.get("min_nonzero_steer_samples", 5) or 5
    ):
        reasons.append("aggregate_steer_coverage_below_threshold")
    if int(coverage.get("positive_accel_count", 0) or 0) < int(
        policy.get("min_positive_accel_samples", 5) or 5
    ):
        reasons.append("aggregate_positive_accel_coverage_below_threshold")
    if int(coverage.get("positive_decel_count", 0) or 0) < int(
        policy.get("min_decel_samples", 5) or 5
    ):
        reasons.append("aggregate_decel_coverage_below_threshold")
    if float(coverage.get("dead_sequence_ratio", 0.0) or 0.0) > float(
        policy.get("max_dead_sequence_ratio", 0.85) or 0.85
    ):
        reasons.append("aggregate_dead_sequence_ratio_too_high")
    return (not reasons), reasons


def evaluate_capture_run(
    *,
    capture_id: str,
    run_dir: Path,
    exit_code: int,
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    policy = merge_policy(DEFAULT_CAPTURE_POLICY, policy)
    artifacts_dir = run_dir / "artifacts"
    raw_info = load_jsonl_records(artifacts_dir / "apollo_control_raw.jsonl")
    decode_info = load_jsonl_records(artifacts_dir / "bridge_control_decode.jsonl")
    summary_path = run_dir / "summary.json"
    summary_exists = summary_path.exists() and summary_path.stat().st_size >= int(
        policy.get("min_summary_size_bytes", 2) or 2
    )
    metadata_exists = (artifacts_dir / "scenario_metadata.json").exists()
    primary_sequence = target_sequence_from_decode_rows(decode_info["rows"])
    sequence_source = "bridge_control_decode"
    if not primary_sequence:
        primary_sequence = target_sequence_from_raw_rows(raw_info["rows"], policy=policy)
        sequence_source = "apollo_control_raw"
    coverage = evaluate_target_sequence(primary_sequence, policy=policy)
    invalid_reasons: List[str] = []
    if not raw_info["exists"]:
        invalid_reasons.append("raw_control_missing")
    if int(raw_info.get("nonempty_lines", 0) or 0) < int(policy.get("min_jsonl_rows", 20) or 20):
        invalid_reasons.append("raw_control_rows_below_threshold")
    if (not decode_info["exists"]) and (not raw_info["exists"]):
        invalid_reasons.append("no_capture_artifacts")
    if bool(policy.get("require_summary", True)) and (not summary_exists):
        invalid_reasons.append("summary_missing")
    if int(exit_code) != 0:
        invalid_reasons.append(f"nonzero_exit_code:{int(exit_code)}")
    invalid_reasons.extend(str(item) for item in coverage.get("sequence_invalid_reasons", []) or [])
    deduped_reasons: List[str] = []
    seen = set()
    for reason in invalid_reasons:
        if reason in seen:
            continue
        seen.add(reason)
        deduped_reasons.append(reason)
    capture_valid = not deduped_reasons
    return {
        "capture_id": str(capture_id),
        "run_dir": str(run_dir),
        "artifacts_dir": str(artifacts_dir),
        "raw_path": str(artifacts_dir / "apollo_control_raw.jsonl"),
        "decode_path": str(artifacts_dir / "bridge_control_decode.jsonl"),
        "exit_code": int(exit_code),
        "parse_success": bool(raw_info.get("parse_success") or decode_info.get("parse_success")),
        "raw_exists": bool(raw_info["exists"]),
        "decode_exists": bool(decode_info["exists"]),
        "summary_exists": bool(summary_exists),
        "scenario_metadata_exists": bool(metadata_exists),
        "raw_nonempty_lines": int(raw_info.get("nonempty_lines", 0) or 0),
        "raw_parse_errors": int(raw_info.get("error_count", 0) or 0),
        "decode_nonempty_lines": int(decode_info.get("nonempty_lines", 0) or 0),
        "decode_parse_errors": int(decode_info.get("error_count", 0) or 0),
        "sequence_source": sequence_source,
        "sample_count": int(coverage.get("sample_count", 0) or 0),
        "duration_sec": float(coverage.get("duration_sec", 0.0) or 0.0),
        "timestamps_monotonic": bool(coverage.get("timestamps_monotonic", False)),
        "steer_coverage": {
            "nonzero_samples": int(coverage.get("nonzero_steer_count", 0) or 0),
            "max_abs_steer_deg": float(coverage.get("max_abs_steer_deg", 0.0) or 0.0),
        },
        "accel_coverage": {
            "positive_samples": int(coverage.get("positive_accel_count", 0) or 0),
            "max_target_accel_mps2": float(coverage.get("max_target_accel_mps2", 0.0) or 0.0),
        },
        "decel_coverage": {
            "positive_samples": int(coverage.get("positive_decel_count", 0) or 0),
            "max_target_decel_mps2": float(coverage.get("max_target_decel_mps2", 0.0) or 0.0),
        },
        "dead_sequence_ratio": float(coverage.get("dead_sequence_ratio", 1.0) or 1.0),
        "dead_sequence_detected": bool(coverage.get("dead_sequence_detected", True)),
        "mapped_steer_saturation_ratio": float(
            coverage.get("mapped_steer_saturation_ratio", 0.0) or 0.0
        ),
        "weird_output_ratio": float(coverage.get("weird_output_ratio", 0.0) or 0.0),
        "physical_fallback_count": int(coverage.get("physical_fallback_count", 0) or 0),
        "capture_valid": bool(capture_valid),
        "invalid_reason": "; ".join(deduped_reasons),
        "invalid_reasons": deduped_reasons,
        "coverage": coverage,
    }


def summarize_scene_startup_state(run_dir: Path) -> Dict[str, Any]:
    artifacts_dir = run_dir / "artifacts"
    startup_rows = load_jsonl_records(artifacts_dir / "startup_stage_timeline.jsonl").get("rows", []) or []
    world_ready_summary = load_json_dict(artifacts_dir / "carla_world_ready_summary.json")
    stages = [str(row.get("stage") or "").strip() for row in startup_rows if str(row.get("stage") or "").strip()]
    status = str(world_ready_summary.get("status") or "").strip()
    world_ready_reached = status == "world_ready"

    carla_wait_ready_ok = "carla_wait_ready_ok" in stages or status in {
        "carla_wait_ready_ok",
        "carla_get_world_retrying",
        "carla_get_world_failed",
        "carla_get_world_ok",
        "carla_load_world_start",
        "carla_load_world_retrying",
        "carla_load_world_failed",
        "world_ready",
    }
    carla_get_world_started = "carla_get_world_start" in stages
    carla_get_world_ok = "carla_get_world_ok" in stages or status in {
        "carla_get_world_ok",
        "carla_load_world_start",
        "carla_load_world_retrying",
        "carla_load_world_failed",
        "world_ready",
    }
    carla_load_world_started = "carla_load_world_start" in stages or status in {
        "carla_load_world_start",
        "carla_load_world_retrying",
        "carla_load_world_failed",
    }

    highest_stage = "SCENE_START"
    if carla_wait_ready_ok:
        highest_stage = "CARLA_WAIT_READY_OK"
    if carla_get_world_started:
        highest_stage = "CARLA_GET_WORLD_STARTED"
    if carla_get_world_ok:
        highest_stage = "CARLA_GET_WORLD_OK"
    if carla_load_world_started:
        highest_stage = "CARLA_LOAD_WORLD_STARTED"
    if world_ready_reached:
        highest_stage = "WORLD_READY"

    blocker_family = "startup_stage_missing"
    if carla_wait_ready_ok and not carla_get_world_ok:
        blocker_family = "scene_timeout_during_carla_get_world"
    elif carla_get_world_ok and not world_ready_reached:
        blocker_family = "scene_timeout_during_carla_load_world"
    elif world_ready_reached:
        blocker_family = "world_ready_or_later"
    elif status in {"carla_wait_ready_failed", "carla_launch_exception", "external_carla_missing"}:
        blocker_family = "scene_timeout_before_carla_ready"

    return {
        "startup_stage_last": stages[-1] if stages else "",
        "startup_stage_count": len(stages),
        "startup_highest_confirmed_stage": highest_stage,
        "startup_blocker_family": blocker_family,
        "carla_world_ready_status": status,
        "carla_world_ready_final_town": world_ready_summary.get("final_town"),
        "world_ready_reached": bool(world_ready_reached),
    }


def summarize_capture_collection(
    captures: Sequence[Dict[str, Any]],
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    policy = merge_policy(DEFAULT_CAPTURE_POLICY, policy)
    valid = [item for item in captures if bool(item.get("capture_valid"))]
    invalid = [item for item in captures if not bool(item.get("capture_valid"))]
    aggregate_coverage = merge_sequence_coverages([item.get("coverage", {}) or {} for item in valid])
    aggregate_ok, aggregate_reasons = coverage_sufficient(aggregate_coverage, policy=policy)
    decision = "suite_recoverable" if valid and aggregate_ok else "suite_failed"
    return {
        "generated_at_unix_sec": time.time(),
        "policy": dict(policy),
        "capture_count": len(captures),
        "valid_capture_count": len(valid),
        "invalid_capture_count": len(invalid),
        "captures": list(captures),
        "aggregate_valid_coverage": aggregate_coverage,
        "aggregate_coverage_ok": bool(aggregate_ok),
        "aggregate_coverage_reasons": aggregate_reasons,
        "suite_recoverable": bool(valid) and bool(aggregate_ok),
        "suite_failed": not (bool(valid) and bool(aggregate_ok)),
        "decision": decision,
        "recovery_summary": {
            "continue_on_partial_failure": bool(valid) and bool(aggregate_ok) and bool(invalid),
            "all_captures_invalid": len(valid) == 0,
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def render_capture_validity_markdown(summary: Dict[str, Any]) -> str:
    lines = [
        "# Calibration Capture Validity Summary",
        "",
        "## Aggregate",
        "",
        f"- capture_count: `{summary.get('capture_count', 0)}`",
        f"- valid_capture_count: `{summary.get('valid_capture_count', 0)}`",
        f"- invalid_capture_count: `{summary.get('invalid_capture_count', 0)}`",
        f"- suite_recoverable: `{summary.get('suite_recoverable', False)}`",
        f"- aggregate_coverage_ok: `{summary.get('aggregate_coverage_ok', False)}`",
        f"- aggregate reasons: `{', '.join(summary.get('aggregate_coverage_reasons', []) or []) or 'none'}`",
        "",
        "| capture_id | exit_code | parse_success | startup_stage | startup_blocker | steer_samples | accel_samples | decel_samples | dead_sequence | valid | invalid_reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in summary.get("captures", []) or []:
        lines.append(
            "| {capture_id} | {exit_code} | {parse_success} | {startup_stage} | {startup_blocker} | {steer} | {accel} | {decel} | {dead} | {valid} | {reason} |".format(
                capture_id=str(item.get("capture_id", "") or ""),
                exit_code=int(item.get("exit_code", 0) or 0),
                parse_success=bool(item.get("parse_success", False)),
                startup_stage=str(item.get("startup_highest_confirmed_stage", "") or ""),
                startup_blocker=str(item.get("startup_blocker_family", "") or ""),
                steer=int(((item.get("steer_coverage") or {}).get("nonzero_samples", 0) or 0)),
                accel=int(((item.get("accel_coverage") or {}).get("positive_samples", 0) or 0)),
                decel=int(((item.get("decel_coverage") or {}).get("positive_samples", 0) or 0)),
                dead=bool(item.get("dead_sequence_detected", False)),
                valid=bool(item.get("capture_valid", False)),
                reason=str(item.get("invalid_reason", "") or ""),
            )
        )
    return "\n".join(lines)


def render_minimum_coverage_policy_markdown(policy: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Calibration Minimum Coverage Policy",
            "",
            "A suite is considered minimally runnable only when the combined valid captures satisfy all of the following thresholds:",
            "",
            f"- min_nonzero_steer_samples: `{int(policy.get('min_nonzero_steer_samples', 0) or 0)}`",
            f"- min_positive_accel_samples: `{int(policy.get('min_positive_accel_samples', 0) or 0)}`",
            f"- min_decel_samples: `{int(policy.get('min_decel_samples', 0) or 0)}`",
            f"- min_capture_duration_sec: `{float(policy.get('min_capture_duration_sec', 0.0) or 0.0):.2f}`",
            f"- min_capture_samples: `{int(policy.get('min_capture_samples', 0) or 0)}`",
            f"- max_dead_sequence_ratio: `{float(policy.get('max_dead_sequence_ratio', 0.0) or 0.0):.2f}`",
            "",
            "Coverage dimensions:",
            "",
            "- lateral excitation: non-zero steering targets",
            "- forward acceleration excitation: positive target acceleration",
            "- braking/deceleration excitation: positive target deceleration",
        ]
    )


def render_recovery_policy_markdown(summary: Dict[str, Any]) -> str:
    policy = summary.get("policy", {}) or {}
    aggregate = summary.get("aggregate_valid_coverage", {}) or {}
    continue_on_partial = bool((summary.get("recovery_summary") or {}).get("continue_on_partial_failure"))
    all_invalid = bool((summary.get("recovery_summary") or {}).get("all_captures_invalid"))
    return "\n".join(
        [
            "# Calibration Suite Recovery Policy",
            "",
            "## Continue When",
            "",
            "- one or more captures exit non-zero or are marked invalid",
            "- at least one capture remains valid after unified validity checks",
            "- aggregate valid coverage still satisfies the minimum thresholds",
            "",
            "## Fail When",
            "",
            "- all relevant captures are invalid",
            "- or aggregate valid coverage is still below the minimum thresholds",
            "",
            "## Current Decision",
            "",
            f"- continue_on_partial_failure: `{continue_on_partial}`",
            f"- all_captures_invalid: `{all_invalid}`",
            f"- aggregate_coverage_ok: `{summary.get('aggregate_coverage_ok', False)}`",
            f"- aggregate reasons: `{', '.join(summary.get('aggregate_coverage_reasons', []) or []) or 'none'}`",
            "",
            "## Current Minimum Coverage",
            "",
            f"- nonzero_steer_count: `{int(aggregate.get('nonzero_steer_count', 0) or 0)}` / required `{int(policy.get('min_nonzero_steer_samples', 0) or 0)}`",
            f"- positive_accel_count: `{int(aggregate.get('positive_accel_count', 0) or 0)}` / required `{int(policy.get('min_positive_accel_samples', 0) or 0)}`",
            f"- positive_decel_count: `{int(aggregate.get('positive_decel_count', 0) or 0)}` / required `{int(policy.get('min_decel_samples', 0) or 0)}`",
            f"- sample_count: `{int(aggregate.get('sample_count', 0) or 0)}` / required `{int(policy.get('min_capture_samples', 0) or 0)}`",
            f"- duration_sec: `{float(aggregate.get('duration_sec', 0.0) or 0.0):.2f}` / required `{float(policy.get('min_capture_duration_sec', 0.0) or 0.0):.2f}`",
            f"- dead_sequence_ratio: `{float(aggregate.get('dead_sequence_ratio', 0.0) or 0.0):.3f}` / max `{float(policy.get('max_dead_sequence_ratio', 0.0) or 0.0):.3f}`",
        ]
    )


def load_json_dict(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or (not path.exists()):
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def analyze_replay_records(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    mapped_steers = [abs(_safe_float(row.get("mapped_carla_steer_cmd"), 0.0)) for row in records]
    weird_count = sum(
        1
        for row in records
        if (
            _safe_float(row.get("mapped_throttle_cmd"), 0.0) > 0.10
            and _safe_float(row.get("mapped_brake_cmd"), 0.0) > 0.10
        )
        or (
            abs(_safe_float(row.get("mapped_carla_steer_cmd"), 0.0)) >= 0.98
            and abs(_safe_float(row.get("target_front_wheel_angle_deg"), 0.0)) <= 0.05
        )
    )
    return {
        "steer_command_saturation_ratio": (
            sum(1 for value in mapped_steers if value >= 0.98) / max(1, len(mapped_steers))
        ),
        "weird_output_ratio": weird_count / max(1, len(records)),
    }


def extract_validation_metrics(
    *,
    replay_payload: Optional[Dict[str, Any]],
    tracking_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    replay_payload = replay_payload or {}
    tracking_payload = tracking_payload or {}
    replay_quality = replay_payload.get("quality", {}) or {}
    replay_stats = analyze_replay_records(replay_payload.get("records", []) or [])
    tracking_steering = ((tracking_payload.get("steering") or {}).get("quality", {}) or {})
    tracking_throttle = ((tracking_payload.get("throttle") or {}).get("quality", {}) or {})
    tracking_brake = ((tracking_payload.get("brake") or {}).get("quality", {}) or {})
    return {
        "replay.mean_abs_steer_error_deg": _safe_float(
            replay_quality.get("mean_abs_steer_error_deg"), 0.0
        ),
        "replay.mean_abs_accel_error_mps2": _safe_float(
            replay_quality.get("mean_abs_accel_error_mps2"), 0.0
        ),
        "replay.mean_abs_decel_error_mps2": _safe_float(
            replay_quality.get("mean_abs_decel_error_mps2"), 0.0
        ),
        "replay.pass": bool(replay_payload.get("pass", False)),
        "tracking.steering_mae_deg": _safe_float(tracking_steering.get("mean_abs_error_deg"), 0.0),
        "tracking.throttle_mae_mps2": _safe_float(tracking_throttle.get("mean_abs_error_mps2"), 0.0),
        "tracking.brake_mae_mps2": _safe_float(tracking_brake.get("mean_abs_error_mps2"), 0.0),
        "tracking.pass": bool(tracking_payload.get("pass", False)),
        "replay.steer_command_saturation_ratio": _safe_float(
            replay_stats.get("steer_command_saturation_ratio"), 0.0
        ),
        "replay.weird_output_ratio": _safe_float(replay_stats.get("weird_output_ratio"), 0.0),
    }


def evaluate_validation_pass(
    *,
    replay_payload: Optional[Dict[str, Any]],
    tracking_payload: Optional[Dict[str, Any]],
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    policy = merge_policy(DEFAULT_COMPARISON_POLICY, policy)
    metrics = extract_validation_metrics(replay_payload=replay_payload, tracking_payload=tracking_payload)
    replay_pass = bool(
        metrics["replay.mean_abs_steer_error_deg"] <= float(policy["replay_max_mean_abs_steer_error_deg"])
        and metrics["replay.mean_abs_accel_error_mps2"] <= float(policy["replay_max_mean_abs_accel_error_mps2"])
        and metrics["replay.mean_abs_decel_error_mps2"] <= float(policy["replay_max_mean_abs_decel_error_mps2"])
    )
    tracking_pass = bool(
        metrics["tracking.steering_mae_deg"] <= float(policy["tracking_max_steering_mae_deg"])
        and metrics["tracking.throttle_mae_mps2"] <= float(policy["tracking_max_throttle_mae_mps2"])
        and metrics["tracking.brake_mae_mps2"] <= float(policy["tracking_max_brake_mae_mps2"])
    )
    reasons: List[str] = []
    if not replay_pass:
        reasons.append("replay_validation_threshold_failed")
    if not tracking_pass:
        reasons.append("tracking_validation_threshold_failed")
    return {
        "metrics": metrics,
        "replay_pass": replay_pass,
        "tracking_pass": tracking_pass,
        "overall_pass": replay_pass and tracking_pass,
        "reasons": reasons,
    }


def build_comparison_rows(
    *,
    baseline_metrics: Dict[str, Any],
    candidate_metrics: Dict[str, Any],
    policy: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    policy = merge_policy(DEFAULT_COMPARISON_POLICY, policy)
    eps_deg = float(policy.get("improvement_epsilon_deg", 0.05) or 0.05)
    eps_mps2 = float(policy.get("improvement_epsilon_mps2", 0.05) or 0.05)
    rows: List[Dict[str, Any]] = []
    specs = [
        ("replay.mean_abs_steer_error_deg", "lower", eps_deg),
        ("replay.mean_abs_accel_error_mps2", "lower", eps_mps2),
        ("replay.mean_abs_decel_error_mps2", "lower", eps_mps2),
        ("tracking.steering_mae_deg", "lower", eps_deg),
        ("tracking.throttle_mae_mps2", "lower", eps_mps2),
        ("tracking.brake_mae_mps2", "lower", eps_mps2),
        ("replay.steer_command_saturation_ratio", "lower", float(policy.get("saturation_ratio_tolerance", 0.02) or 0.02)),
        ("replay.weird_output_ratio", "lower", float(policy.get("weird_output_ratio_tolerance", 0.01) or 0.01)),
    ]
    for metric_name, better_direction, epsilon in specs:
        baseline_value = baseline_metrics.get(metric_name)
        candidate_value = candidate_metrics.get(metric_name)
        if baseline_value is None or candidate_value is None:
            judgement = "unavailable"
            delta = None
        else:
            delta = float(candidate_value) - float(baseline_value)
            if abs(delta) <= float(epsilon):
                judgement = "unchanged"
            elif better_direction == "lower":
                judgement = "improved" if delta < 0.0 else "regressed"
            else:
                judgement = "improved" if delta > 0.0 else "regressed"
        rows.append(
            {
                "metric_name": metric_name,
                "baseline_value": baseline_value,
                "candidate_value": candidate_value,
                "delta": delta,
                "better_direction": better_direction,
                "judgement": judgement,
            }
        )
    for metric_name in ("replay.pass", "tracking.pass"):
        baseline_value = baseline_metrics.get(metric_name)
        candidate_value = candidate_metrics.get(metric_name)
        delta = None
        if baseline_value is None or candidate_value is None:
            judgement = "unavailable"
        elif bool(candidate_value) == bool(baseline_value):
            judgement = "unchanged"
        elif bool(candidate_value):
            judgement = "improved"
        else:
            judgement = "regressed"
        rows.append(
            {
                "metric_name": metric_name,
                "baseline_value": baseline_value,
                "candidate_value": candidate_value,
                "delta": delta,
                "better_direction": "higher",
                "judgement": judgement,
            }
        )
    return rows


def comparison_recommendation(
    *,
    baseline_pass: Optional[Dict[str, Any]],
    candidate_pass: Dict[str, Any],
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    improved = [row["metric_name"] for row in rows if row.get("judgement") == "improved"]
    regressed = [row["metric_name"] for row in rows if row.get("judgement") == "regressed"]
    if not bool(candidate_pass.get("overall_pass", False)):
        return {
            "status": "regressed",
            "recommended_for_default": False,
            "recommendation_reason": "candidate failed replay/tracking threshold gates",
            "improved_metrics": improved,
            "regressed_metrics": regressed,
        }
    if baseline_pass is None:
        return {
            "status": "improved",
            "recommended_for_default": True,
            "recommendation_reason": "no baseline calibration was available; candidate passed threshold gates",
            "improved_metrics": improved,
            "regressed_metrics": regressed,
        }
    critical_regressions = [
        row["metric_name"]
        for row in rows
        if row.get("judgement") == "regressed"
        and str(row.get("metric_name", "")).startswith(("replay.", "tracking."))
    ]
    if critical_regressions:
        return {
            "status": "regressed",
            "recommended_for_default": False,
            "recommendation_reason": "candidate regressed critical replay/tracking metrics",
            "improved_metrics": improved,
            "regressed_metrics": regressed,
        }
    if improved:
        return {
            "status": "improved",
            "recommended_for_default": True,
            "recommendation_reason": "candidate passed threshold gates and improved at least one tracked metric",
            "improved_metrics": improved,
            "regressed_metrics": regressed,
        }
    return {
        "status": "unchanged",
        "recommended_for_default": False,
        "recommendation_reason": "candidate passed threshold gates but did not show a meaningful improvement",
        "improved_metrics": improved,
        "regressed_metrics": regressed,
    }


def write_csv_rows(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_policy_config(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or (not path.exists()):
        return dict(DEFAULT_CAPTURE_POLICY)
    try:
        payload = json.loads(path.read_text()) if path.suffix.lower() == ".json" else None
    except Exception:
        payload = None
    if payload is None:
        try:
            import yaml  # type: ignore

            payload = yaml.safe_load(path.read_text()) or {}
        except Exception:
            payload = {}
    if not isinstance(payload, dict):
        payload = {}
    capture_policy = None
    for key in ("capture_validity", "capture_validity_policy"):
        if isinstance(payload.get(key), dict):
            capture_policy = dict(payload.get(key) or {})
            break
    if capture_policy is None:
        capture_policy = payload if isinstance(payload, dict) else {}
    if "min_jsonl_records" in capture_policy and "min_jsonl_rows" not in capture_policy:
        capture_policy["min_jsonl_rows"] = capture_policy["min_jsonl_records"]
    if "dead_sequence_min_run_samples" in capture_policy and "min_capture_samples" not in capture_policy:
        capture_policy["min_capture_samples"] = capture_policy["dead_sequence_min_run_samples"]
    return merge_policy(DEFAULT_CAPTURE_POLICY, capture_policy if isinstance(capture_policy, dict) else {})


def evaluate_capture_validity(
    *,
    capture_id: str,
    raw_path: Optional[Path],
    exit_code: int,
    policy: Optional[Dict[str, Any]] = None,
    summary_path: Optional[Path] = None,
    metadata_path: Optional[Path] = None,
    log_path: Optional[Path] = None,
) -> Dict[str, Any]:
    policy = merge_policy(DEFAULT_CAPTURE_POLICY, policy)
    run_dir = raw_path.parent.parent if raw_path is not None and raw_path.parent.name == "artifacts" else None
    if run_dir is None:
        if summary_path is not None:
            run_dir = summary_path.parent
        elif metadata_path is not None:
            run_dir = metadata_path.parent.parent
        else:
            run_dir = Path(".").resolve()
    detail = evaluate_capture_run(
        capture_id=capture_id,
        run_dir=run_dir,
        exit_code=exit_code,
        policy=policy,
    )
    if raw_path is not None and (not raw_path.exists()):
        detail["invalid_reasons"] = ["raw_control_missing", *list(detail.get("invalid_reasons", []))]
        detail["invalid_reason"] = "; ".join(detail["invalid_reasons"])
        detail["capture_valid"] = False
    if log_path is not None:
        detail["log_path"] = str(log_path)
    if summary_path is not None:
        detail["summary_path"] = str(summary_path)
    if metadata_path is not None:
        detail["metadata_path"] = str(metadata_path)
    detail.update(summarize_scene_startup_state(run_dir))
    return detail


def summarize_capture_validity(
    captures: Sequence[Dict[str, Any]],
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    summary = summarize_capture_collection(captures, policy=policy)
    summary["minimum_coverage_ok"] = bool(summary.get("aggregate_coverage_ok", False))
    summary["minimum_coverage_reasons"] = list(summary.get("aggregate_coverage_reasons", []) or [])
    summary["minimum_coverage"] = dict(summary.get("aggregate_valid_coverage", {}) or {})
    return summary


def coverage_meets_minimum(
    coverage: Dict[str, Any],
    policy: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, List[str]]:
    mapped = {
        "sample_count": int(
            coverage.get("sample_count", coverage.get("valid_capture_count", 0)) or 0
        ),
        "duration_sec": float(
            coverage.get("duration_sec", coverage.get("min_duration_sec", 0.0)) or 0.0
        ),
        "nonzero_steer_count": int(
            coverage.get("nonzero_steer_count", coverage.get("nonzero_steer_samples", 0)) or 0
        ),
        "positive_accel_count": int(
            coverage.get("positive_accel_count", coverage.get("positive_accel_samples", 0)) or 0
        ),
        "positive_decel_count": int(
            coverage.get("positive_decel_count", coverage.get("decel_samples", 0)) or 0
        ),
        "dead_sequence_ratio": float(
            coverage.get("dead_sequence_ratio", coverage.get("max_dead_sequence_ratio", 0.0)) or 0.0
        ),
    }
    return coverage_sufficient(mapped, policy=policy)


def write_capture_validity_artifacts(artifacts_dir: Path, summary: Dict[str, Any]) -> None:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    write_json(artifacts_dir / "calibration_capture_validity_summary.json", summary)
    (artifacts_dir / "calibration_capture_validity_summary.md").write_text(
        render_capture_validity_markdown(summary)
    )


def write_policy_artifacts(artifacts_dir: Path, summary: Dict[str, Any]) -> None:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    policy = summary.get("policy", {}) or {}
    (artifacts_dir / "calibration_suite_recovery_policy.md").write_text(
        render_recovery_policy_markdown(summary)
    )
    (artifacts_dir / "calibration_minimum_coverage_policy.md").write_text(
        render_minimum_coverage_policy_markdown(policy)
    )


def deep_update(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_update(out[key], value)
        else:
            out[key] = value
    return out


def load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    payload = yaml.safe_load(path.read_text()) or {}
    return payload if isinstance(payload, dict) else {}


def dump_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import yaml  # type: ignore

        path.write_text(yaml.safe_dump(payload, sort_keys=False))
    except Exception:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def compare_validation_payloads(
    *,
    baseline_replay: Optional[Dict[str, Any]],
    candidate_replay: Optional[Dict[str, Any]],
    baseline_tracking: Optional[Dict[str, Any]],
    candidate_tracking: Optional[Dict[str, Any]],
    baseline_pass: Optional[bool] = None,
    candidate_pass: Optional[bool] = None,
    policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    policy = merge_policy(DEFAULT_COMPARISON_POLICY, policy)
    candidate_gate = evaluate_validation_pass(
        replay_payload=candidate_replay,
        tracking_payload=candidate_tracking,
        policy=policy,
    )
    baseline_gate = None
    baseline_metrics: Dict[str, Any] = {}
    if baseline_replay or baseline_tracking:
        baseline_gate = evaluate_validation_pass(
            replay_payload=baseline_replay,
            tracking_payload=baseline_tracking,
            policy=policy,
        )
        baseline_metrics = dict(baseline_gate.get("metrics", {}) or {})
    candidate_metrics = dict(candidate_gate.get("metrics", {}) or {})
    if baseline_pass is not None:
        baseline_metrics["replay.pass"] = bool(baseline_pass)
        baseline_metrics["tracking.pass"] = bool(baseline_pass)
    if candidate_pass is not None:
        candidate_metrics["replay.pass"] = bool(candidate_pass)
        candidate_metrics["tracking.pass"] = bool(candidate_pass)
    rows = build_comparison_rows(
        baseline_metrics=baseline_metrics,
        candidate_metrics=candidate_metrics,
        policy=policy,
    )
    recommendation = comparison_recommendation(
        baseline_pass=baseline_gate,
        candidate_pass=candidate_gate,
        rows=rows,
    )
    return {
        "policy": policy,
        "rows": rows,
        "baseline_metrics": baseline_metrics,
        "candidate_metrics": candidate_metrics,
        **recommendation,
    }


def write_comparison_artifacts(root: Path, comparison: Dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    rows = list(comparison.get("rows", []) or [])
    write_csv_rows(root / "calibration_comparison_table.csv", rows)
    lines = [
        "# Calibration Comparison Report",
        "",
        f"- status: `{comparison.get('status', '')}`",
        f"- recommended_for_default: `{comparison.get('recommended_for_default', False)}`",
        f"- recommendation_reason: `{comparison.get('recommendation_reason', '')}`",
        "",
        "## Improved",
        "",
    ]
    improved = list(comparison.get("improved_metrics", []) or [])
    regressed = list(comparison.get("regressed_metrics", []) or [])
    if improved:
        lines.extend(f"- `{item}`" for item in improved)
    else:
        lines.append("- none")
    lines.extend(["", "## Regressed", ""])
    if regressed:
        lines.extend(f"- `{item}`" for item in regressed)
    else:
        lines.append("- none")
    lines.extend(["", "## Metrics", ""])
    for row in rows:
        lines.append(
            "- `{}` baseline=`{}` candidate=`{}` delta=`{}` judgement=`{}`".format(
                row.get("metric_name", ""),
                row.get("baseline_value", ""),
                row.get("candidate_value", ""),
                row.get("delta", ""),
                row.get("judgement", ""),
            )
        )
    (root / "calibration_comparison_report.md").write_text("\n".join(lines))


def calibration_metadata_payload(
    *,
    calibration_path: Path,
    source_run_id: str,
    source_scenario_config: str,
    source_capture_ids: Sequence[str],
    source_inference_version: str,
    validation_version: str,
    vehicle_profile: str,
    map_name: str,
    recommended_for_default: bool,
) -> Dict[str, Any]:
    return {
        "calibration_version": calibration_path.stem,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source_run_id": source_run_id,
        "source_scenario_config": source_scenario_config,
        "source_capture_ids": list(source_capture_ids),
        "source_inference_version": source_inference_version,
        "validation_version": validation_version,
        "vehicle_profile": vehicle_profile,
        "map_name": map_name,
        "recommended_for_default": bool(recommended_for_default),
    }


def versioning_policy_markdown(metadata: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Calibration Versioning Policy",
            "",
            "- each calibration result carries a sidecar metadata json",
            "- metadata records source run, scenario config, capture ids, inference version, validation version, vehicle profile, map, and rollout recommendation",
            "",
            "## Current Metadata",
            "",
        ]
        + [f"- `{key}`: `{value}`" for key, value in metadata.items()]
    )


def demo_ready_summary_markdown(summary: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Calibration Demo Ready Summary",
            "",
            f"- baseline calibration id: `{summary.get('baseline_calibration_id', '')}`",
            f"- candidate calibration id: `{summary.get('candidate_calibration_id', '')}`",
            f"- improved metrics: `{', '.join(summary.get('improved_metrics', []) or []) or 'none'}`",
            f"- regressed metrics: `{', '.join(summary.get('regressed_metrics', []) or []) or 'none'}`",
            f"- recommended calibration id: `{summary.get('recommended_calibration_id', '')}`",
            f"- recommended demo case: `{summary.get('recommended_demo_case', '')}`",
        ]
    )
