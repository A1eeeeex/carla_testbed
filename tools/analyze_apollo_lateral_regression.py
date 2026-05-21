#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_ARTIFACTS = REPO_ROOT / "artifacts"
POLICY: Dict[str, Any] = {
    "blocked_commanded_nonzero_ratio_max": 0.01,
    "blocked_force_zero_apply_ratio_min": 0.80,
    "candidate_raw_saturation_ratio_max": 0.01,
    "candidate_longest_saturation_sec_max": 1.0,
    "candidate_guard_trigger_ratio_max": 0.05,
    "candidate_measured_error_mean_deg_max": 6.0,
    "unhealthy_raw_saturation_ratio_min": 0.05,
    "unhealthy_longest_saturation_sec_min": 5.0,
    "unhealthy_guard_trigger_ratio_min": 0.25,
    "unhealthy_offlane_count_min": 20,
}
PLANNING_ERROR_KEYWORDS = [
    "path data is empty",
    "reference line",
    "Fail to aggregate planning trajectory",
    "planner failed to make a driving plan",
]
CONTROL_ERROR_KEYWORDS = [
    "planning has no trajectory point",
    "Failed to produce control command",
]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fp:
        return list(csv.DictReader(fp))


def _mean(values: Iterable[float]) -> Optional[float]:
    vals = [float(v) for v in values]
    if not vals:
        return None
    return statistics.fmean(vals)


def _median(values: Iterable[float]) -> Optional[float]:
    vals = [float(v) for v in values]
    if not vals:
        return None
    return statistics.median(vals)


def _rate(num: float, den: float) -> Optional[float]:
    if not den:
        return None
    return float(num) / float(den)


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _count_keywords(path: Path, keywords: List[str]) -> Dict[str, int]:
    counts = {key: 0 for key in keywords}
    if not path.exists():
        return counts
    text = path.read_text(encoding="utf-8", errors="ignore")
    for key in keywords:
        counts[key] = text.count(key)
    return counts


def _sequence_metrics(values: List[float], timestamps: List[float], threshold: float) -> Dict[str, Any]:
    count = len(values)
    abs_vals = [abs(v) for v in values]
    sat_flags = [abs(v) >= threshold for v in values]
    sat_count = sum(sat_flags)
    nonzero_count = sum(1 for v in abs_vals if v > 1e-6)
    left_sat = sum(1 for v in values if v <= -threshold)
    right_sat = sum(1 for v in values if v >= threshold)
    sign_flips = 0
    last_sign = 0
    for v in values:
        sign = -1 if v < -0.05 else 1 if v > 0.05 else 0
        if sign != 0 and last_sign != 0 and sign != last_sign:
            sign_flips += 1
        if sign != 0:
            last_sign = sign
    best = 0
    best_start = None
    cur = 0
    start = 0
    for i, flag in enumerate(sat_flags):
        if flag:
            if cur == 0:
                start = i
            cur += 1
            if cur > best:
                best = cur
                best_start = start
        else:
            cur = 0
    best_end = (best_start + best - 1) if best_start is not None else None
    duration = 0.0
    start_ts = None
    end_ts = None
    if best_start is not None and best_end is not None:
        start_ts = timestamps[best_start]
        end_ts = timestamps[best_end]
        duration = max(0.0, end_ts - start_ts)
    side_bias = "balanced"
    if left_sat > right_sat * 1.2:
        side_bias = "left"
    elif right_sat > left_sat * 1.2:
        side_bias = "right"
    total_duration = max(0.0, timestamps[-1] - timestamps[0]) if len(timestamps) >= 2 else None
    return {
        "count": count,
        "nonzero_count": nonzero_count,
        "nonzero_ratio": _rate(nonzero_count, count),
        "mean_abs": _mean(abs_vals),
        "max_abs": max(abs_vals) if abs_vals else None,
        "saturation_count": sat_count,
        "saturation_ratio": _rate(sat_count, count),
        "longest_saturation_frames": best,
        "longest_saturation_sec": duration,
        "longest_saturation_start_ts": start_ts,
        "longest_saturation_end_ts": end_ts,
        "saturation_side_bias": side_bias,
        "sign_flip_rate": _rate(sign_flips, total_duration) if total_duration else None,
    }


def _analyze_control_decode(path: Path) -> Dict[str, Any]:
    records = _load_jsonl(path)
    timestamps: List[float] = []
    raw_values: List[float] = []
    commanded_values: List[float] = []
    guard_reasons: Counter[str] = Counter()
    guard_apply_count = 0
    low_speed_guard_apply_count = 0
    low_speed_sustained_guard_apply_count = 0
    sustained_guard_apply_count = 0
    force_zero_apply_count = 0
    decode_nonzero_output_count = 0
    guard_limited_amount_total = 0.0
    for rec in records:
        ts = _safe_float(rec.get("ts_sec"))
        raw = _safe_float(rec.get("raw_steer"))
        out = _safe_float(rec.get("commanded_steer"))
        if ts is None or raw is None or out is None:
            continue
        timestamps.append(ts)
        raw_values.append(raw)
        commanded_values.append(out)
        if abs(out) > 1e-6:
            decode_nonzero_output_count += 1
        reason = str(rec.get("sustained_lateral_guard_trigger_reason", "") or "").strip()
        if _safe_bool(rec.get("low_speed_steer_guard_applied")):
            guard_apply_count += 1
            low_speed_guard_apply_count += 1
            guard_limited_amount_total += abs(
                (_safe_float(rec.get("steer_before_lateral_guards")) or 0.0)
                - (_safe_float(rec.get("steer_after_lateral_guards")) or out)
            )
            guard_reasons["low_speed_steer_guard"] += 1
        if _safe_bool(rec.get("low_speed_sustained_guard_applied")):
            guard_apply_count += 1
            low_speed_sustained_guard_apply_count += 1
            guard_limited_amount_total += abs(
                _safe_float(rec.get("low_speed_sustained_guard_limited_amount")) or 0.0
            )
            guard_reasons[
                str(rec.get("low_speed_sustained_guard_trigger_reason") or "").strip()
                or "low_speed_sustained_raw_saturation"
            ] += 1
        if _safe_bool(rec.get("sustained_lateral_guard_applied")):
            guard_apply_count += 1
            sustained_guard_apply_count += 1
            guard_limited_amount_total += abs(_safe_float(rec.get("sustained_lateral_guard_limited_amount")) or 0.0)
            guard_reasons[reason or "sustained_raw_saturation"] += 1
        if _safe_bool(rec.get("force_zero_steer_applied")):
            force_zero_apply_count += 1
    raw_metrics = _sequence_metrics(raw_values, timestamps, threshold=0.99)
    commanded_metrics = _sequence_metrics(commanded_values, timestamps, threshold=0.99)
    return {
        "sample_count": len(timestamps),
        "decode_nonzero_output_count": decode_nonzero_output_count,
        "force_zero_apply_count": force_zero_apply_count,
        "guard_apply_count": guard_apply_count,
        "low_speed_guard_apply_count": low_speed_guard_apply_count,
        "low_speed_sustained_guard_apply_count": low_speed_sustained_guard_apply_count,
        "sustained_guard_apply_count": sustained_guard_apply_count,
        "guard_reason_counts": dict(guard_reasons),
        "guard_limited_amount_total": guard_limited_amount_total,
        "raw": raw_metrics,
        "commanded": commanded_metrics,
    }


def _analyze_debug_timeseries(path: Path) -> Dict[str, Any]:
    rows = _read_csv(path)
    offlane_count = 0
    guard_any_count = 0
    measured_target_errors: List[float] = []
    measured_nonzero = 0
    measured_abs: List[float] = []
    oscillation_sign_flips = 0
    last_sign = 0
    context_patterns: Counter[str] = Counter()
    saturation_ts: List[Tuple[float, float]] = []
    force_zero_applied = 0
    low_speed_guard_applied = 0
    sustained_guard_applied = 0
    for row in rows:
        lane_inside = _safe_bool(row.get("lane_inside"))
        if not lane_inside:
            offlane_count += 1
        if any(
            _safe_bool(row.get(name))
            for name in (
                "straight_lane_zero_steer_applied",
                "low_speed_steer_guard_applied",
                "sustained_lateral_guard_applied",
                "force_zero_steer_applied",
            )
        ):
            guard_any_count += 1
        if _safe_bool(row.get("force_zero_steer_applied")):
            force_zero_applied += 1
        if _safe_bool(row.get("low_speed_steer_guard_applied")):
            low_speed_guard_applied += 1
        if _safe_bool(row.get("sustained_lateral_guard_applied")):
            sustained_guard_applied += 1
        measured_deg = _safe_float(row.get("measured_steer_deg"))
        if measured_deg is not None:
            measured_abs.append(abs(measured_deg))
            if abs(measured_deg) > 1e-3:
                measured_nonzero += 1
            sign = -1 if measured_deg < -1.0 else 1 if measured_deg > 1.0 else 0
            if sign != 0 and last_sign != 0 and sign != last_sign:
                oscillation_sign_flips += 1
            if sign != 0:
                last_sign = sign
        target_deg = _safe_float(row.get("target_front_wheel_angle_deg"))
        if target_deg is not None and measured_deg is not None:
            measured_target_errors.append(abs(target_deg - measured_deg))
        raw = _safe_float(row.get("apollo_desired_steer"))
        ts = _safe_float(row.get("ts_sec"))
        if raw is None or ts is None or abs(raw) < 0.99:
            continue
        saturation_ts.append((ts, raw))
        speed = _safe_float(row.get("speed_mps")) or 0.0
        e_y = abs(_safe_float(row.get("e_y_m")) or 0.0)
        e_psi = abs(_safe_float(row.get("e_psi_deg")) or 0.0)
        curvature = abs(_safe_float(row.get("target_curvature")) or 0.0)
        if speed < 1.0:
            context_patterns["low_speed_saturation"] += 1
        elif lane_inside and e_y <= 0.2 and e_psi <= 5.0 and curvature <= 0.002:
            context_patterns["straight_center_saturation"] += 1
        elif (not lane_inside) or e_y > 1.0 or e_psi > 20.0:
            context_patterns["offlane_or_large_error_saturation"] += 1
        else:
            context_patterns["other_saturation"] += 1
    duration = None
    if len(rows) >= 2:
        start = _safe_float(rows[0].get("ts_sec"))
        end = _safe_float(rows[-1].get("ts_sec"))
        if start is not None and end is not None:
            duration = max(0.0, end - start)
    return {
        "row_count": len(rows),
        "offlane_count": offlane_count,
        "guard_any_count": guard_any_count,
        "force_zero_applied_count": force_zero_applied,
        "low_speed_guard_applied_count": low_speed_guard_applied,
        "sustained_guard_applied_count": sustained_guard_applied,
        "measured_steer_deg_nonzero_count": measured_nonzero,
        "measured_steer_deg_abs_mean": _mean(measured_abs),
        "target_vs_measured_front_wheel_angle_error_mean": _mean(measured_target_errors),
        "target_vs_measured_front_wheel_angle_error_median": _median(measured_target_errors),
        "measured_sign_flip_rate": _rate(oscillation_sign_flips, duration) if duration else None,
        "saturation_context_patterns": dict(context_patterns),
    }


def _analyze_vehicle_response(path: Path) -> Dict[str, Any]:
    rows = _read_csv(path)
    yaw_rates: List[float] = []
    lateral_accels: List[float] = []
    for row in rows:
        yaw = _safe_float(row.get("yaw_rate_rps"))
        lat = _safe_float(row.get("lateral_accel_mps2"))
        if yaw is not None:
            yaw_rates.append(abs(math.degrees(yaw)))
        if lat is not None:
            lateral_accels.append(abs(lat))
    return {
        "row_count": len(rows),
        "vehicle_heading_change_rate_degps_mean": _mean(yaw_rates),
        "vehicle_lateral_accel_abs_mean": _mean(lateral_accels),
    }


def _detect_low_speed_creep(summary: Dict[str, Any]) -> float:
    checks = (summary.get("acceptance") or {}).get("checks") or {}
    creep = checks.get("scenario_low_speed_creep") or {}
    return float(creep.get("duration_sec", 0.0) or 0.0)


def _compute_stack_health(planning_nonzero_ratio: Optional[float], control_no_trajectory_count: int, invalid_goal_count: int) -> Tuple[bool, str]:
    if planning_nonzero_ratio is None or planning_nonzero_ratio < 0.80:
        return False, "planning_nonzero_ratio_low"
    if control_no_trajectory_count > 1000:
        return False, "control_no_trajectory_high"
    if invalid_goal_count > 0:
        return False, "invalid_goal_present"
    return True, ""


def _lateral_status(
    *,
    sample_count: int,
    summary_fail_reason: str,
    force_zero_apply_ratio: Optional[float],
    commanded_nonzero_ratio: Optional[float],
    raw_saturated_ratio: Optional[float],
    longest_saturation_sec: Optional[float],
    guard_trigger_ratio: Optional[float],
    offlane_count: int,
    measured_error_mean: Optional[float],
) -> str:
    if sample_count <= 0:
        return "lateral_unhealthy" if summary_fail_reason else "lateral_guarded_but_usable"
    if (
        (commanded_nonzero_ratio is not None and commanded_nonzero_ratio <= POLICY["blocked_commanded_nonzero_ratio_max"])
        or (force_zero_apply_ratio is not None and force_zero_apply_ratio >= POLICY["blocked_force_zero_apply_ratio_min"])
    ):
        return "lateral_blocked"
    if (
        (raw_saturated_ratio is not None and raw_saturated_ratio >= POLICY["unhealthy_raw_saturation_ratio_min"])
        or (longest_saturation_sec is not None and longest_saturation_sec >= POLICY["unhealthy_longest_saturation_sec_min"])
        or (guard_trigger_ratio is not None and guard_trigger_ratio >= POLICY["unhealthy_guard_trigger_ratio_min"])
        or offlane_count >= POLICY["unhealthy_offlane_count_min"]
    ):
        return "lateral_unhealthy"
    if (
        raw_saturated_ratio is not None
        and raw_saturated_ratio <= POLICY["candidate_raw_saturation_ratio_max"]
        and (longest_saturation_sec or 0.0) <= POLICY["candidate_longest_saturation_sec_max"]
        and (guard_trigger_ratio or 0.0) <= POLICY["candidate_guard_trigger_ratio_max"]
        and (measured_error_mean is None or measured_error_mean <= POLICY["candidate_measured_error_mean_deg_max"])
    ):
        return "lateral_candidate_for_mainline"
    return "lateral_guarded_but_usable"


@dataclass
class RunAnalysis:
    record: Dict[str, Any]
    row: Dict[str, Any]


def _analyze_run(run_dir: Path) -> RunAnalysis:
    summary = _load_json(run_dir / "summary.json")
    effective = Path(run_dir / "effective.yaml")
    effective_cfg = yaml.safe_load(effective.read_text(encoding="utf-8")) if effective.exists() else {}
    bridge_health = _load_json(run_dir / "artifacts" / "bridge_health_summary.json")
    planning_summary = _load_json(run_dir / "artifacts" / "planning_topic_debug_summary.json")
    control_decode = _analyze_control_decode(run_dir / "artifacts" / "bridge_control_decode.jsonl")
    debug_metrics = _analyze_debug_timeseries(run_dir / "artifacts" / "debug_timeseries.csv")
    vehicle_metrics = _analyze_vehicle_response(run_dir / "artifacts" / "carla_vehicle_response.csv")
    planning_errors = _count_keywords(run_dir / "artifacts" / "apollo_planning.INFO", PLANNING_ERROR_KEYWORDS)
    control_errors = _count_keywords(run_dir / "artifacts" / "apollo_control.INFO", CONTROL_ERROR_KEYWORDS)
    reroute_reason_counts = bridge_health.get("reroute_reason_counts", {}) or {}
    invalid_goal_count = int(bridge_health.get("invalid_goal_count", 0) or 0)
    planning_msgs = int(planning_summary.get("total_messages_received", 0) or 0)
    planning_nonzero = int(planning_summary.get("messages_with_nonzero_trajectory_points", 0) or 0)
    planning_nonzero_ratio = _rate(planning_nonzero, planning_msgs)
    low_speed_creep_duration = _detect_low_speed_creep(summary)
    control_no_trajectory_count = int(control_errors.get("planning has no trajectory point", 0) or 0)
    stack_healthy, stack_reason = _compute_stack_health(
        planning_nonzero_ratio,
        control_no_trajectory_count,
        invalid_goal_count,
    )
    sample_count = max(control_decode["sample_count"], debug_metrics["row_count"])
    force_zero_apply_ratio = _rate(control_decode["force_zero_apply_count"], sample_count)
    commanded_nonzero_ratio = control_decode["commanded"]["nonzero_ratio"]
    guard_trigger_ratio = _rate(control_decode["guard_apply_count"], sample_count)
    lateral_status = _lateral_status(
        sample_count=sample_count,
        summary_fail_reason=str(summary.get("fail_reason") or ""),
        force_zero_apply_ratio=force_zero_apply_ratio,
        commanded_nonzero_ratio=commanded_nonzero_ratio,
        raw_saturated_ratio=control_decode["raw"]["saturation_ratio"],
        longest_saturation_sec=control_decode["raw"]["longest_saturation_sec"],
        guard_trigger_ratio=guard_trigger_ratio,
        offlane_count=debug_metrics["offlane_count"],
        measured_error_mean=debug_metrics["target_vs_measured_front_wheel_angle_error_mean"],
    )
    final_outcome_label = (
        "scenario_success_and_stack_healthy"
        if summary.get("success") and stack_healthy
        else "scenario_success_but_stack_unhealthy"
        if summary.get("success")
        else "scenario_fail_but_stack_partially_healthy"
        if stack_healthy
        else "scenario_fail_and_stack_unhealthy"
    )
    top_pattern = ""
    if debug_metrics["saturation_context_patterns"]:
        top_pattern = max(
            debug_metrics["saturation_context_patterns"].items(), key=lambda item: item[1]
        )[0]
    long_route_skip_count = int(reroute_reason_counts.get("long_phase_invalid_goal_skip", 0) or 0) + int(
        reroute_reason_counts.get("long_phase_unstable_reference_line_skip", 0) or 0
    )
    row = {
        "run_dir": str(run_dir),
        "profile": summary.get("profile_name") or (effective_cfg.get("run") or {}).get("profile_name") or run_dir.name,
        "seed": (effective_cfg.get("run") or {}).get("seed"),
        "repeat_id": run_dir.name.rsplit("_repeat", 1)[-1] if "_repeat" in run_dir.name else "",
        "summary.success": summary.get("success"),
        "summary.fail_reason": summary.get("fail_reason"),
        "lateral_status_label": lateral_status,
        "raw_steer_saturated_ratio": control_decode["raw"]["saturation_ratio"],
        "longest_continuous_saturation_sec": control_decode["raw"]["longest_saturation_sec"],
        "commanded_steer_nonzero_ratio": commanded_nonzero_ratio,
        "commanded_steer_saturated_ratio": control_decode["commanded"]["saturation_ratio"],
        "measured_steer_abs_mean": debug_metrics["measured_steer_deg_abs_mean"],
        "guard_trigger_count": control_decode["guard_apply_count"],
        "guard_trigger_ratio": guard_trigger_ratio,
        "planning_nonzero_ratio": planning_nonzero_ratio,
        "invalid_goal_count": invalid_goal_count,
        "long_route_skip_count": long_route_skip_count,
        "low_speed_creep_duration": low_speed_creep_duration,
        "stack_healthy": stack_healthy,
        "final_outcome_label": final_outcome_label,
        "raw_steer_nonzero_count": control_decode["raw"]["nonzero_count"],
        "raw_steer_nonzero_ratio": control_decode["raw"]["nonzero_ratio"],
        "raw_steer_saturated_count": control_decode["raw"]["saturation_count"],
        "longest_continuous_saturation_frames": control_decode["raw"]["longest_saturation_frames"],
        "longest_saturation_start_ts": control_decode["raw"]["longest_saturation_start_ts"],
        "longest_saturation_end_ts": control_decode["raw"]["longest_saturation_end_ts"],
        "saturation_side_bias": control_decode["raw"]["saturation_side_bias"],
        "steering_sign_flip_rate": control_decode["raw"]["sign_flip_rate"],
        "decode_nonzero_output_count": control_decode["decode_nonzero_output_count"],
        "commanded_steer_nonzero_count": control_decode["commanded"]["nonzero_count"],
        "low_speed_guard_trigger_count": control_decode["low_speed_guard_apply_count"],
        "low_speed_sustained_guard_trigger_count": control_decode["low_speed_sustained_guard_apply_count"],
        "sustained_guard_trigger_count": control_decode["sustained_guard_apply_count"],
        "guard_trigger_reason_topk": json.dumps(control_decode["guard_reason_counts"], ensure_ascii=True),
        "guard_limited_steer_amount": control_decode["guard_limited_amount_total"],
        "measured_steer_deg_nonzero_count": debug_metrics["measured_steer_deg_nonzero_count"],
        "target_vs_measured_front_wheel_angle_error_mean": debug_metrics[
            "target_vs_measured_front_wheel_angle_error_mean"
        ],
        "target_vs_measured_front_wheel_angle_error_median": debug_metrics[
            "target_vs_measured_front_wheel_angle_error_median"
        ],
        "vehicle_heading_change_rate": vehicle_metrics["vehicle_heading_change_rate_degps_mean"],
        "lane_departure_or_offlane_count": debug_metrics["offlane_count"],
        "planning_has_no_trajectory_count": control_no_trajectory_count,
        "path_data_is_empty_count": planning_errors["path data is empty"],
        "reference_line_error_count": planning_errors["reference line"],
        "aggregate_fail_count": planning_errors["Fail to aggregate planning trajectory"],
        "planner_failed_count": planning_errors["planner failed to make a driving plan"],
        "most_common_saturation_trigger_mode": top_pattern,
        "control_decode_sample_count": sample_count,
    }
    return RunAnalysis(
        record={
            "actual_run_dir": str(run_dir),
            "summary": summary,
            "effective_cfg": effective_cfg,
            "bridge_health": bridge_health,
            "planning_summary": planning_summary,
            "control_decode": control_decode,
            "debug_metrics": debug_metrics,
            "vehicle_metrics": vehicle_metrics,
            "planning_errors": planning_errors,
            "control_errors": control_errors,
            "top_pattern": top_pattern,
        },
        row=row,
    )


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_text_dual(batch_artifacts: Path, name: str, content: str) -> None:
    batch_path = batch_artifacts / name
    repo_path = CANONICAL_ARTIFACTS / name
    batch_path.parent.mkdir(parents=True, exist_ok=True)
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    batch_path.write_text(content, encoding="utf-8")
    repo_path.write_text(content, encoding="utf-8")


def _write_json_dual(batch_artifacts: Path, name: str, payload: Dict[str, Any]) -> None:
    content = json.dumps(payload, indent=2, ensure_ascii=False)
    _write_text_dual(batch_artifacts, name, content + "\n")


def _write_yaml_dual(batch_artifacts: Path, name: str, payload: Dict[str, Any]) -> None:
    content = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    _write_text_dual(batch_artifacts, name, content)


def _discover_run_entries(batch_root: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    pattern = re.compile(r"(?P<profile>.+)_seed(?P<seed>\d+)_repeat(?P<repeat>\d+)$")
    for child in sorted(batch_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name in {"artifacts", "latest"} or child.name.startswith("."):
            continue
        if re.search(r"__\d+$", child.name):
            continue
        match = pattern.match(child.name)
        if not match:
            continue
        entries.append(
            {
                "profile": match.group("profile"),
                "seed": int(match.group("seed")),
                "repeat_id": int(match.group("repeat")),
                "run_dir": str(child.resolve()),
                "status": "discovered",
                "returncode": None,
            }
        )
    return entries


def _load_batch_manifest_or_discover(batch_root: Path) -> Dict[str, Any]:
    batch_artifacts = batch_root / "artifacts"
    manifest_path = batch_artifacts / "apollo_lateral_regression_manifest.json"
    manifest = _load_json(manifest_path)
    run_entries = manifest.get("runs", []) or []
    if not run_entries:
        run_entries = _discover_run_entries(batch_root)
        manifest = {
            "created_at": None,
            "batch_root": str(batch_root),
            "profiles_run": sorted({item["profile"] for item in run_entries}),
            "seeds": sorted({item["seed"] for item in run_entries}),
            "repeats": max((item["repeat_id"] for item in run_entries), default=0),
            "ticks": None,
            "acceptance_policy_version": "apollo_lateral_acceptance_policy_v1",
            "runs": run_entries,
            "artifact_paths": {},
            "comparison_report_paths": {},
            "manifest_source": "discovered_from_run_dirs",
        }
    else:
        manifest["manifest_source"] = "existing_manifest"
    return manifest


def _profile_aggregate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_profile: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_profile[str(row["profile"])].append(row)
    out: List[Dict[str, Any]] = []
    for profile, items in sorted(by_profile.items()):
        out.append(
            {
                "profile": profile,
                "run_count": len(items),
                "success_count": sum(1 for item in items if item["summary.success"]),
                "lateral_status_counts": dict(Counter(str(item["lateral_status_label"]) for item in items)),
                "raw_steer_saturated_ratio_mean": _mean(
                    item["raw_steer_saturated_ratio"] for item in items if item["raw_steer_saturated_ratio"] is not None
                ),
                "longest_continuous_saturation_sec_mean": _mean(
                    item["longest_continuous_saturation_sec"]
                    for item in items
                    if item["longest_continuous_saturation_sec"] is not None
                ),
                "commanded_steer_nonzero_ratio_mean": _mean(
                    item["commanded_steer_nonzero_ratio"]
                    for item in items
                    if item["commanded_steer_nonzero_ratio"] is not None
                ),
                "guard_trigger_ratio_mean": _mean(
                    item["guard_trigger_ratio"] for item in items if item["guard_trigger_ratio"] is not None
                ),
                "planning_nonzero_ratio_mean": _mean(
                    item["planning_nonzero_ratio"] for item in items if item["planning_nonzero_ratio"] is not None
                ),
                "invalid_goal_count_mean": _mean(
                    item["invalid_goal_count"] for item in items if item["invalid_goal_count"] is not None
                ),
                "low_speed_creep_duration_mean": _mean(
                    item["low_speed_creep_duration"]
                    for item in items
                    if item["low_speed_creep_duration"] is not None
                ),
                "dominant_saturation_trigger_mode": Counter(
                    str(item["most_common_saturation_trigger_mode"] or "unknown") for item in items
                ).most_common(1)[0][0],
                "control_decode_sample_count_mean": _mean(
                    item["control_decode_sample_count"]
                    for item in items
                    if item["control_decode_sample_count"] is not None
                ),
                "fail_reason_counts": dict(
                    Counter(str(item["summary.fail_reason"] or "none") for item in items)
                ),
            }
        )
    return out


def _row_quality(row: Dict[str, Any], batch_index: int) -> tuple:
    score = 0
    if row.get("summary.success") in (True, False):
        score += 1000
    if row.get("summary.fail_reason"):
        score += 500
    if row.get("control_decode_sample_count") is not None:
        score += int(float(row.get("control_decode_sample_count") or 0))
    if row.get("raw_steer_saturated_ratio") is not None:
        score += 50
    if row.get("commanded_steer_nonzero_ratio") is not None:
        score += 50
    return (score, batch_index)


def _build_metrics_definition() -> str:
    return """# Lateral Metrics Definition

## Apollo 原始横向信号健康度
- `total_frames`: `bridge_control_decode.jsonl` 中有效控制样本数。
- `raw_steer_nonzero_count / ratio`: `abs(raw_steer) > 1e-6` 的样本数与占比。
- `raw_steer_saturated_count / ratio`: `abs(raw_steer) >= 0.99` 的样本数与占比。
- `longest_continuous_saturation_frames / sec`: 原始 steer 连续饱和的最长段。
- `saturation_side_bias`: 饱和主要偏左 / 偏右 / 平衡。
- `steering_sign_flip_rate`: `|raw_steer| > 0.05` 时符号翻转频率。

## bridge 输出层横向行为
- `commanded_steer_nonzero_count / ratio`: 最终发给 CARLA 的 steer 非零样本数与占比。
- `commanded_steer_saturated_ratio`: 最终输出中 `abs(commanded_steer) >= 0.99` 的占比。
- `guard_trigger_count / ratio`: 透明 lateral guard 生效样本数与占比。
- `guard_trigger_reason_topk`: guard 触发原因频次。
- `guard_limited_steer_amount`: guard 累积削减的绝对 steer 量。
- `decode_nonzero_output_count`: bridge decode 后非零输出样本数。

## 车辆实际执行层
- `measured_steer_deg_nonzero_count`: 实测转角非零样本数。
- `measured_steer_deg_abs_mean`: 实测转角绝对值均值。
- `target_vs_measured_front_wheel_angle_error_mean / median`: 目标前轮角与实测角误差。
- `vehicle_heading_change_rate`: `vehicle_response.csv` 中绝对 yaw rate 的均值（deg/s）。
- `lateral oscillation indicators`: 用实测 steer 符号翻转率近似。

## 场景/轨迹层
- `lane_departure_or_offlane_count`: `debug_timeseries.csv` 中 `lane_inside=false` 的样本数。
- `most_common_saturation_trigger_mode`: 饱和样本上下文主类（低速 / 直道居中 / 大误差 / 其它）。
- `whether lateral behavior caused scenario failure`: 由 `summary.success=false` 且 `lateral_status_label=lateral_unhealthy` 近似判定。
- `whether lateral guard prevented failure`: 由 guard 触发且 profile 被判 `lateral_guarded_but_usable` 近似推断。

## 结合主链健康
- `planning_nonzero_ratio`: planning 非空消息占比。
- `count(\"planning has no trajectory point\")`: `apollo_control.INFO` 关键字计数。
- `invalid_goal_count`: bridge health summary 中的 invalid goal 次数。
- `long_route_skip_count`: `long_phase_invalid_goal_skip + long_phase_unstable_reference_line_skip`。
- `low_speed_creep_duration`: acceptance 中 `scenario_low_speed_creep` 的持续时间。
"""


def _build_success_policy() -> str:
    return f"""# Apollo Lateral Success Policy

当前横向正式验收不再只看 `summary.success`，而是额外给每个 run 打 `lateral_status_label`：

1. `lateral_blocked`
   - `commanded_steer_nonzero_ratio <= {POLICY['blocked_commanded_nonzero_ratio_max']}`
   - 或 `force_zero_steer_applied_ratio >= {POLICY['blocked_force_zero_apply_ratio_min']}`

2. `lateral_unhealthy`
   - `raw_steer_saturated_ratio >= {POLICY['unhealthy_raw_saturation_ratio_min']}`
   - 或 `longest_continuous_saturation_sec >= {POLICY['unhealthy_longest_saturation_sec_min']}`
   - 或 `guard_trigger_ratio >= {POLICY['unhealthy_guard_trigger_ratio_min']}`
   - 或 `lane_departure_or_offlane_count >= {POLICY['unhealthy_offlane_count_min']}`

3. `lateral_guarded_but_usable`
   - 未被 blocked
   - 未触发 unhealthy 阈值
   - 但 guard 仍有显著参与，适合作为继续迭代档

4. `lateral_candidate_for_mainline`
   - `raw_steer_saturated_ratio <= {POLICY['candidate_raw_saturation_ratio_max']}`
   - `longest_continuous_saturation_sec <= {POLICY['candidate_longest_saturation_sec_max']}`
   - `guard_trigger_ratio <= {POLICY['candidate_guard_trigger_ratio_max']}`
   - `target_vs_measured_front_wheel_angle_error_mean <= {POLICY['candidate_measured_error_mean_deg_max']}`（若可取）

说明：
- `summary.success` 仍保留“场景运行是否成功”的语义。
- `lateral_status_label` 负责回答“横向是否真的被验证、是否健康、是否只是被屏蔽”。
"""


def _render_report(
    batch_roots: List[Path],
    rows: List[Dict[str, Any]],
    aggregates: List[Dict[str, Any]],
    demo_summary: Dict[str, Any],
) -> str:
    status_counts = Counter(str(row["lateral_status_label"]) for row in rows)
    dominant_issue = aggregates[0]["dominant_saturation_trigger_mode"] if aggregates else "unknown"
    best_candidate = next(
        (item["profile"] for item in aggregates if "candidate_for_mainline" in json.dumps(item["lateral_status_counts"])),
        None,
    )
    guarded = next((item for item in aggregates if item["profile"] == "lateral_enabled_guarded"), None)
    raw = next((item for item in aggregates if item["profile"] == "lateral_enabled_raw"), None)
    lines = [
        "# Apollo Lateral Regression Report",
        "",
        f"- batch_roots: `{[str(item) for item in batch_roots]}`",
        f"- run_count: `{len(rows)}`",
        f"- status_counts: `{dict(status_counts)}`",
        "",
        "## Executive Summary",
        "",
        f"- 当前横向默认主线 **仍然是 blocked**：`current_relaxed` 仍通过 `force_zero_steer_output` 屏蔽最终 steer。",
        f"- 放开横向后，Apollo 原始横向是否健康需要看 `raw` profile；当前主导触发模式是 `{dominant_issue}`。",
        f"- `guarded` profile {'明显优于 raw' if guarded and raw and (guarded.get('raw_steer_saturated_ratio_mean') or 1.0) < (raw.get('raw_steer_saturated_ratio_mean') or 0.0) else '尚未明显优于 raw'}。",
        f"- 当前最适合作为继续迭代 lateral-enabled 主线候选的是 `{best_candidate or 'apollo_lateral_enabled_guarded（待人工复核）'}`。",
        "",
        "## Profile Aggregates",
        "",
    ]
    for item in aggregates:
        lines.append(
            "- `{profile}` runs=`{run_count}` success=`{success_count}` lateral_status_counts=`{lateral_status_counts}` raw_sat_mean=`{raw_steer_saturated_ratio_mean}` longest_sat_sec_mean=`{longest_continuous_saturation_sec_mean}` guard_ratio_mean=`{guard_trigger_ratio_mean}` control_decode_sample_mean=`{control_decode_sample_count_mean}` fail_reason_counts=`{fail_reason_counts}`".format(
                **item
            )
        )
    lines.extend(
        [
            "",
            "## Key Conclusions",
            "",
            "- `relaxed` 现在不能算横向健康主线，因为最终 steer 仍被压制，属于 `lateral_blocked`。",
            "- `lateral_enabled_raw` 用来回答“完全放开会发生什么”；如果它出现长时间单侧饱和，这说明问题主要在 Apollo 原始 lateral 输出或 route/reference-line 残余问题，而不是 bridge 清零本身。",
            "- `lateral_enabled_guarded` 的目标是不再清零，而通过透明限幅把极端连续饱和拉回可控区间；如果 `control_decode_sample_count` 仍为 0，则当前问题更偏向控制桥/车体状态准备，而不是 guard 逻辑已经被证明有效。",
            "- `lateral_enabled_strict` 不是另一套逻辑，只是在 guarded 主线上增加更高观测成本与更完整证据。",
            "",
            "## Demo Candidates",
            "",
            f"- before_demo: `{demo_summary.get('before_demo_run_id')}` window=`{demo_summary.get('before_demo_window_sec')}`",
            f"- guarded_demo: `{demo_summary.get('guarded_demo_run_id')}` window=`{demo_summary.get('guarded_demo_window_sec')}`",
            "",
            "## Next Smallest Fix",
            "",
            "- 如果 raw 仍长期单侧饱和，下一阶段最该修的是 Apollo 原始 lateral 输出触发模式，而不是重新回到 bridge 清零。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Apollo lateral regression batch")
    parser.add_argument("--batch-root", action="append", required=True, dest="batch_roots")
    parser.add_argument("--output-batch-root", default="")
    args = parser.parse_args()

    batch_roots = [Path(item).expanduser().resolve() for item in args.batch_roots]
    output_batch_root = (
        Path(args.output_batch_root).expanduser().resolve()
        if args.output_batch_root
        else batch_roots[0]
    )
    batch_artifacts = output_batch_root / "artifacts"
    batch_artifacts.mkdir(parents=True, exist_ok=True)
    manifests = [ _load_batch_manifest_or_discover(root) for root in batch_roots ]
    best_rows: Dict[tuple[str, int, int], Dict[str, Any]] = {}
    best_records: Dict[tuple[str, int, int], Dict[str, Any]] = {}
    best_quality: Dict[tuple[str, int, int], tuple] = {}
    for batch_index, manifest in enumerate(manifests):
        run_entries = manifest.get("runs", []) or []
        for item in run_entries:
            run_dir = Path(item["run_dir"])
            redirect_path = run_dir / "RUN_DIR_REDIRECT.txt"
            if redirect_path.exists():
                redirect_target = Path(redirect_path.read_text(encoding="utf-8").strip())
                if redirect_target.exists():
                    run_dir = redirect_target
            if not (run_dir / "summary.json").exists():
                continue
            analyzed = _analyze_run(run_dir)
            row = dict(analyzed.row)
            row["run_dir"] = str(run_dir)
            row["profile"] = str(item.get("profile") or row["profile"])
            row["seed"] = int(item.get("seed", row.get("seed") or 0) or 0)
            row["repeat_id"] = int(item.get("repeat_id", row.get("repeat_id") or 0) or 0)
            row["source_batch_root"] = str(Path(manifest.get("batch_root", "")).resolve())
            key = (str(row["profile"]), int(row["seed"]), int(row["repeat_id"]))
            quality = _row_quality(row, batch_index)
            if key not in best_quality or quality > best_quality[key]:
                best_quality[key] = quality
                best_rows[key] = row
                best_records[key] = analyzed.record

    rows = list(best_rows.values())
    run_records: Dict[str, Dict[str, Any]] = {
        str(best_rows[key]["run_dir"]): best_records[key] for key in best_rows
    }

    rows.sort(key=lambda item: (str(item["profile"]), int(item["seed"]), int(item["repeat_id"])))
    aggregates = _profile_aggregate(rows)

    comparison_csv = batch_artifacts / "apollo_lateral_regression_case_comparison.csv"
    _write_csv(comparison_csv, rows)
    (CANONICAL_ARTIFACTS / comparison_csv.name).write_text(comparison_csv.read_text(encoding="utf-8"), encoding="utf-8")

    policy_payload = {
        "version": "apollo_lateral_acceptance_policy_v1",
        **POLICY,
    }
    _write_text_dual(batch_artifacts, "lateral_metrics_definition.md", _build_metrics_definition())
    _write_text_dual(batch_artifacts, "apollo_lateral_success_policy.md", _build_success_policy())
    _write_yaml_dual(batch_artifacts, "apollo_lateral_acceptance_policy.yaml", policy_payload)

    before_demo = max(
        (row for row in rows if row["profile"] == "lateral_enabled_raw"),
        key=lambda item: (item["longest_continuous_saturation_sec"] or 0.0),
        default=None,
    )
    guarded_demo = min(
        (row for row in rows if row["profile"] == "lateral_enabled_guarded"),
        key=lambda item: (item["raw_steer_saturated_ratio"] or 1.0, item["guard_trigger_ratio"] or 1.0),
        default=None,
    )
    demo_summary = {
        "before_demo_profile": before_demo["profile"] if before_demo else None,
        "before_demo_run_id": before_demo["run_dir"] if before_demo else None,
        "before_demo_window_sec": {
            "start": before_demo.get("longest_saturation_start_ts"),
            "end": before_demo.get("longest_saturation_end_ts"),
        }
        if before_demo
        else None,
        "guarded_demo_profile": guarded_demo["profile"] if guarded_demo else None,
        "guarded_demo_run_id": guarded_demo["run_dir"] if guarded_demo else None,
        "guarded_demo_window_sec": {
            "start": guarded_demo.get("longest_saturation_start_ts"),
            "end": guarded_demo.get("longest_saturation_end_ts"),
        }
        if guarded_demo
        else None,
        "recommended_overlay_metrics": [
            "raw_steer",
            "commanded_steer",
            "measured_steer_deg",
            "sustained_lateral_guard_applied",
            "speed_mps",
            "e_y_m",
            "e_psi_deg",
        ],
    }
    _write_json_dual(batch_artifacts, "apollo_lateral_demo_candidate_summary.json", demo_summary)
    demo_md = [
        "# Apollo Lateral Demo Candidate Summary",
        "",
        f"- before_demo_run: `{demo_summary.get('before_demo_run_id')}`",
        f"- before_demo_window_sec: `{demo_summary.get('before_demo_window_sec')}`",
        f"- guarded_demo_run: `{demo_summary.get('guarded_demo_run_id')}`",
        f"- guarded_demo_window_sec: `{demo_summary.get('guarded_demo_window_sec')}`",
        f"- recommended_overlay_metrics: `{demo_summary.get('recommended_overlay_metrics')}`",
    ]
    _write_text_dual(batch_artifacts, "apollo_lateral_demo_candidate_summary.md", "\n".join(demo_md) + "\n")

    report = _render_report(batch_roots, rows, aggregates, demo_summary)
    _write_text_dual(batch_artifacts, "apollo_lateral_regression_report.md", report)

    merged_manifest = {
        "created_at": None,
        "batch_root": str(output_batch_root),
        "source_batch_roots": [str(item) for item in batch_roots],
        "profiles_run": sorted({str(row["profile"]) for row in rows}),
        "seeds": sorted({int(row["seed"]) for row in rows}),
        "repeats": max((int(row["repeat_id"]) for row in rows), default=0),
        "ticks": None,
        "acceptance_policy_version": "apollo_lateral_acceptance_policy_v1",
        "runs": rows,
    }
    merged_manifest["artifact_paths"] = {
        "case_comparison_csv": str(comparison_csv),
        "report_md": str(batch_artifacts / "apollo_lateral_regression_report.md"),
        "metrics_definition_md": str(batch_artifacts / "lateral_metrics_definition.md"),
        "success_policy_md": str(batch_artifacts / "apollo_lateral_success_policy.md"),
        "acceptance_policy_yaml": str(batch_artifacts / "apollo_lateral_acceptance_policy.yaml"),
    }
    merged_manifest["comparison_report_paths"] = {
        "apollo_lateral_regression_case_comparison.csv": str(batch_artifacts / "apollo_lateral_regression_case_comparison.csv"),
        "apollo_lateral_regression_report.md": str(batch_artifacts / "apollo_lateral_regression_report.md"),
    }
    manifest_path = batch_artifacts / "apollo_lateral_regression_manifest.json"
    manifest_path.write_text(json.dumps(merged_manifest, indent=2), encoding="utf-8")
    (CANONICAL_ARTIFACTS / manifest_path.name).write_text(manifest_path.read_text(encoding="utf-8"), encoding="utf-8")
    manifest_md = batch_artifacts / "apollo_lateral_regression_manifest.md"
    manifest_md.write_text(
        "\n".join(
            [
                "# Apollo Lateral Regression Manifest",
                "",
                f"- output_batch_root: `{output_batch_root}`",
                f"- source_batch_roots: `{[str(item) for item in batch_roots]}`",
                f"- profiles_run: `{merged_manifest['profiles_run']}`",
                f"- seeds: `{merged_manifest['seeds']}`",
                f"- repeats: `{merged_manifest['repeats']}`",
                f"- acceptance_policy_version: `{merged_manifest['acceptance_policy_version']}`",
            ]
            + [
                f"- `{row['profile']}` seed=`{row['seed']}` repeat=`{row['repeat_id']}` run_dir=`{row['run_dir']}`"
                for row in rows
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (CANONICAL_ARTIFACTS / manifest_md.name).write_text(manifest_md.read_text(encoding="utf-8"), encoding="utf-8")


if __name__ == "__main__":
    main()
