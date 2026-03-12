#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


GAP_FIELDS = [
    "terminal_stop_hold_front_gap_lon_m",
    "front_obstacle_gap_lon_m",
    "front_obstacle_gap_distance_m",
]


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        out = float(value)
        return out if math.isfinite(out) else None
    text = str(value).strip()
    if not text:
        return None
    try:
        out = float(text)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _load_csv(path: Path) -> Optional[List[Dict[str, str]]]:
    if not path.exists():
        return None
    try:
        with path.open("r", newline="") as fp:
            reader = csv.DictReader(fp)
            return [dict(row) for row in reader]
    except Exception:
        return None


def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    if p <= 0:
        return float(min(values))
    if p >= 100:
        return float(max(values))
    arr = sorted(values)
    if len(arr) == 1:
        return float(arr[0])
    idx = (len(arr) - 1) * (p / 100.0)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(arr[lo])
    frac = idx - lo
    return float(arr[lo] * (1.0 - frac) + arr[hi] * frac)


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(statistics.median(values))


def _std(values: List[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return float(statistics.pstdev(values))


def _choose_gap_field(rows: List[Dict[str, Any]], *, min_count: int = 1) -> Tuple[Optional[str], Dict[str, int]]:
    counts: Dict[str, int] = {field: 0 for field in GAP_FIELDS}
    for row in rows:
        for field in GAP_FIELDS:
            val = row.get(field)
            if val is None:
                continue
            if val > 0.0:
                counts[field] += 1
    best_field: Optional[str] = None
    best_count = -1
    best_priority = len(GAP_FIELDS) + 1
    for idx, field in enumerate(GAP_FIELDS):
        count = counts[field]
        if count > best_count or (count == best_count and idx < best_priority):
            best_field = field
            best_count = count
            best_priority = idx
    if best_count < min_count:
        return None, counts
    return best_field, counts


def _prepare_timeseries(rows: Optional[List[Dict[str, str]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not rows:
        return out
    for row in rows:
        t = _safe_float(row.get("t"))
        v = _safe_float(row.get("v_mps"))
        out.append(
            {
                "t": t,
                "v_mps": v,
                "throttle": _safe_float(row.get("throttle")),
                "brake": _safe_float(row.get("brake")),
                "steer": _safe_float(row.get("steer")),
            }
        )
    valid_t = [item["t"] for item in out if item.get("t") is not None]
    if valid_t:
        t0 = min(valid_t)
        for item in out:
            item["rel_t"] = None if item.get("t") is None else float(item["t"] - t0)
    else:
        for idx, item in enumerate(out):
            item["rel_t"] = float(idx)
    return out


def _prepare_debug(rows: Optional[List[Dict[str, str]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not rows:
        return out
    for row in rows:
        item: Dict[str, Any] = {
            "ts_sec": _safe_float(row.get("ts_sec")),
            "speed_mps": _safe_float(row.get("speed_mps")),
            "commanded_throttle": _safe_float(row.get("commanded_throttle")),
            "commanded_brake": _safe_float(row.get("commanded_brake")),
            "commanded_steer": _safe_float(row.get("commanded_steer")),
            "measured_available": _safe_bool(row.get("measured_available")),
            "measured_throttle": _safe_float(row.get("measured_throttle")),
            "measured_brake": _safe_float(row.get("measured_brake")),
            "measured_steer": _safe_float(row.get("measured_steer")),
            "terminal_stop_hold_active": _safe_bool(row.get("terminal_stop_hold_active")),
            "routing_established": _safe_bool(row.get("routing_established")),
            "control_rx_count": _safe_float(row.get("control_rx_count")),
            "control_tx_count": _safe_float(row.get("control_tx_count")),
        }
        for field in GAP_FIELDS:
            item[field] = _safe_float(row.get(field))
        out.append(item)

    valid_ts = [item["ts_sec"] for item in out if item.get("ts_sec") is not None]
    if valid_ts:
        t0 = min(valid_ts)
        for item in out:
            item["rel_t"] = None if item.get("ts_sec") is None else float(item["ts_sec"] - t0)
    else:
        for idx, item in enumerate(out):
            item["rel_t"] = float(idx)
    return out


def _launch_from_series(
    rows: List[Dict[str, Any]],
    *,
    speed_key: str,
    window_sec: float,
    speed_threshold: float,
) -> Dict[str, Any]:
    if not rows:
        return {
            "computed": False,
            "reason": "series_empty",
            "launch_success": None,
            "launch_time_rel_sec": None,
            "max_speed_in_window_mps": None,
        }
    max_speed = 0.0
    seen_speed = False
    for row in rows:
        rel_t = _safe_float(row.get("rel_t"))
        speed = _safe_float(row.get(speed_key))
        if rel_t is None or speed is None:
            continue
        if rel_t > window_sec:
            continue
        max_speed = max(max_speed, speed)
        seen_speed = True
        if speed > speed_threshold:
            return {
                "computed": True,
                "reason": "ok",
                "launch_success": True,
                "launch_time_rel_sec": rel_t,
                "max_speed_in_window_mps": max_speed,
            }
    if not seen_speed:
        return {
            "computed": False,
            "reason": "no_valid_speed_in_window",
            "launch_success": None,
            "launch_time_rel_sec": None,
            "max_speed_in_window_mps": None,
        }
    return {
        "computed": True,
        "reason": "below_threshold",
        "launch_success": False,
        "launch_time_rel_sec": None,
        "max_speed_in_window_mps": max_speed,
    }


def _compute_tail_static_seconds(rows: List[Dict[str, Any]], *, speed_threshold: float) -> float:
    if not rows:
        return 0.0
    with_ts = [row for row in rows if row.get("ts_sec") is not None and row.get("speed_mps") is not None]
    if len(with_ts) < 2:
        return 0.0
    total = 0.0
    prev_ts = with_ts[-1]["ts_sec"]
    for row in reversed(with_ts):
        ts = row["ts_sec"]
        speed = row["speed_mps"]
        if speed is None or ts is None:
            break
        if speed > speed_threshold:
            break
        total += max(0.0, float(prev_ts - ts))
        prev_ts = ts
    return float(total)


def _count_watchdog_from_logs(log_paths: Iterable[Path]) -> Tuple[Optional[int], str, List[str]]:
    existing = [path for path in log_paths if path.exists()]
    if not existing:
        return None, "not_available", []
    count = 0
    used: List[str] = []
    for path in existing:
        used.append(str(path))
        try:
            with path.open("r", errors="ignore") as fp:
                for line in fp:
                    if "source=watchdog" in line:
                        count += 1
        except Exception:
            continue
    return count, "best_effort", used


def evaluate_run(run_dir: Path) -> Dict[str, Any]:
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    summary = _load_json(run_dir / "summary.json") or {}
    timeseries_raw = _load_csv(run_dir / "timeseries.csv")
    debug_raw = _load_csv(artifacts_dir / "debug_timeseries.csv")
    bridge_health = _load_json(artifacts_dir / "bridge_health_summary.json") or {}
    bridge_stats = _load_json(artifacts_dir / "cyber_bridge_stats.json") or {}

    timeseries = _prepare_timeseries(timeseries_raw)
    debug_rows = _prepare_debug(debug_raw)

    notes: List[str] = []

    baseline_markers = [
        "publish_ros2_native: false",
        "publish_ros2_gt: true",
        "acc_only_mode: true",
        "longitudinal_only_pipeline: true",
        "traffic_light:",
        "policy: ignore",
        "watchdog_wait_for_first_msg: true",
    ]
    effective_yaml = run_dir / "effective.yaml"
    marker_match = 0
    if effective_yaml.exists():
        text = effective_yaml.read_text(errors="ignore")
        marker_match = sum(1 for marker in baseline_markers if marker in text)
    else:
        notes.append("effective.yaml missing; baseline marker check skipped")

    metrics: Dict[str, Any] = {}

    routing_request_count = _safe_float(bridge_stats.get("routing_request_count"))
    routing_response_count = _safe_float(bridge_stats.get("routing_response_count"))
    routing_success_count = _safe_float(bridge_stats.get("routing_success_count"))
    routing_source = "artifacts/cyber_bridge_stats.json"
    routing_status = "ok"
    routing_value: Optional[bool]
    if routing_success_count is not None:
        routing_value = routing_success_count >= 1.0
    else:
        if debug_rows:
            routing_value = any(bool(row.get("routing_established")) for row in debug_rows)
            routing_source = "artifacts/debug_timeseries.csv"
            routing_status = "best_effort"
        else:
            routing_value = None
            routing_status = "not_available"
            routing_source = "missing"
    metrics["routing_success"] = {
        "value": routing_value,
        "metric_status": routing_status,
        "source": routing_source,
        "rule": "routing_success_count >= 1 (fallback: any(routing_established==true))",
        "details": {
            "routing_request_count": routing_request_count,
            "routing_response_count": routing_response_count,
            "routing_success_count": routing_success_count,
        },
    }

    launch_window_sec = 15.0
    launch_speed_threshold = 0.5
    launch_ts = _launch_from_series(
        timeseries,
        speed_key="v_mps",
        window_sec=launch_window_sec,
        speed_threshold=launch_speed_threshold,
    )
    launch_source = "timeseries.csv"
    if not launch_ts.get("computed"):
        launch_debug = _launch_from_series(
            debug_rows,
            speed_key="speed_mps",
            window_sec=launch_window_sec,
            speed_threshold=launch_speed_threshold,
        )
        if launch_debug.get("computed"):
            launch_ts = launch_debug
            launch_source = "artifacts/debug_timeseries.csv"
    launch_status = "ok" if launch_ts.get("computed") else "not_available"
    metrics["launch_success"] = {
        "value": launch_ts.get("launch_success"),
        "metric_status": launch_status,
        "source": launch_source,
        "rule": "within first 15s, speed > 0.5 m/s",
        "thresholds": {
            "window_sec": launch_window_sec,
            "speed_threshold_mps": launch_speed_threshold,
        },
        "details": {
            "launch_time_rel_sec": launch_ts.get("launch_time_rel_sec"),
            "max_speed_in_window_mps": launch_ts.get("max_speed_in_window_mps"),
            "compute_reason": launch_ts.get("reason"),
        },
    }

    summary_max = _safe_float(summary.get("max_speed_mps"))
    ts_max = _median([])
    ts_vals = [row["v_mps"] for row in timeseries if row.get("v_mps") is not None]
    dbg_vals = [row["speed_mps"] for row in debug_rows if row.get("speed_mps") is not None]
    ts_max = max(ts_vals) if ts_vals else None
    dbg_max = max(dbg_vals) if dbg_vals else None

    max_speed_source = "summary.json.max_speed_mps"
    max_speed_mps = summary_max
    if max_speed_mps is None:
        if ts_max is not None:
            max_speed_mps = ts_max
            max_speed_source = "timeseries.csv.v_mps"
        elif dbg_max is not None:
            max_speed_mps = dbg_max
            max_speed_source = "artifacts/debug_timeseries.csv.speed_mps"
        else:
            max_speed_source = "missing"
    metrics["max_speed"] = {
        "value": {
            "max_speed_mps": max_speed_mps,
            "max_speed_kmh": (None if max_speed_mps is None else max_speed_mps * 3.6),
        },
        "metric_status": "ok" if max_speed_mps is not None else "not_available",
        "source": max_speed_source,
        "rule": "prefer summary.max_speed_mps, fallback to timeseries/debug peak",
        "details": {
            "summary_max_speed_mps": summary_max,
            "timeseries_max_speed_mps": ts_max,
            "debug_timeseries_max_speed_mps": dbg_max,
        },
    }

    launch_time_rel = _safe_float(metrics["launch_success"]["details"].get("launch_time_rel_sec"))
    run_duration = None
    rel_candidates = [row.get("rel_t") for row in debug_rows if row.get("rel_t") is not None]
    if rel_candidates:
        run_duration = max(rel_candidates)

    stable_gap_field, stable_gap_counts = _choose_gap_field(debug_rows, min_count=8)
    if stable_gap_field is None:
        stable_gap_field, stable_gap_counts = _choose_gap_field(debug_rows, min_count=1)

    stable_candidates: List[Dict[str, Any]] = []
    if stable_gap_field and run_duration is not None:
        stable_start = max(run_duration * 0.60, (launch_time_rel or 0.0) + 2.0)
        for row in debug_rows:
            rel_t = _safe_float(row.get("rel_t"))
            speed = _safe_float(row.get("speed_mps"))
            gap = _safe_float(row.get(stable_gap_field))
            if rel_t is None or gap is None or speed is None:
                continue
            if rel_t < stable_start:
                continue
            if speed > 2.0:
                continue
            if gap <= 0.0:
                continue
            stable_candidates.append(row)
        if len(stable_candidates) < 8:
            stable_candidates = []
            for row in debug_rows:
                rel_t = _safe_float(row.get("rel_t"))
                gap = _safe_float(row.get(stable_gap_field))
                if rel_t is None or gap is None:
                    continue
                if run_duration is not None and rel_t < run_duration * 0.60:
                    continue
                if gap <= 0.0:
                    continue
                stable_candidates.append(row)

    stable_gaps = [float(row[stable_gap_field]) for row in stable_candidates] if stable_gap_field else []
    stable_median = _median(stable_gaps)
    stable_std = _std(stable_gaps)
    q25 = _percentile(stable_gaps, 25.0)
    q75 = _percentile(stable_gaps, 75.0)
    stable_iqr = None if q25 is None or q75 is None else float(q75 - q25)
    stable_cv = None
    if stable_std is not None and stable_median is not None and abs(stable_median) > 1e-6:
        stable_cv = float(stable_std / abs(stable_median))

    stable_confidence = "low"
    if stable_median is not None and stable_iqr is not None and stable_cv is not None:
        if len(stable_gaps) >= 20 and stable_iqr <= 1.0 and stable_cv <= 0.12:
            stable_confidence = "high"
        elif len(stable_gaps) >= 10 and stable_iqr <= 2.0 and stable_cv <= 0.25:
            stable_confidence = "medium"

    if stable_median is None:
        stable_status = "not_available"
        stable_source = "missing"
        stable_reason = "no_valid_gap_samples"
    else:
        stable_status = "ok"
        stable_source = f"artifacts/debug_timeseries.csv:{stable_gap_field}"
        stable_reason = "ok"

    metrics["stable_follow_gap_m"] = {
        "value": stable_median,
        "metric_status": stable_status,
        "source": stable_source,
        "rule": "post-launch late-run low-speed valid-gap median",
        "details": {
            "gap_field": stable_gap_field,
            "sample_count": len(stable_gaps),
            "gap_iqr_m": stable_iqr,
            "gap_std_m": stable_std,
            "gap_cv": stable_cv,
            "confidence": stable_confidence,
            "reason": stable_reason,
            "gap_field_valid_counts": stable_gap_counts,
        },
    }

    stop_gap_field, stop_gap_counts = _choose_gap_field(debug_rows, min_count=3)
    if stop_gap_field is None:
        stop_gap_field, stop_gap_counts = _choose_gap_field(debug_rows, min_count=1)

    stop_candidates: List[Dict[str, Any]] = []
    if stop_gap_field and run_duration is not None:
        stop_start = max(0.0, run_duration - 10.0)
        for row in debug_rows:
            rel_t = _safe_float(row.get("rel_t"))
            speed = _safe_float(row.get("speed_mps"))
            gap = _safe_float(row.get(stop_gap_field))
            if rel_t is None or speed is None or gap is None:
                continue
            if rel_t < stop_start:
                continue
            if speed > 0.3:
                continue
            if gap <= 0.0:
                continue
            stop_candidates.append(row)
        if len(stop_candidates) < 3:
            stop_candidates = []
            for row in debug_rows:
                rel_t = _safe_float(row.get("rel_t"))
                speed = _safe_float(row.get("speed_mps"))
                gap = _safe_float(row.get(stop_gap_field))
                if rel_t is None or speed is None or gap is None:
                    continue
                if run_duration is not None and rel_t < max(0.0, run_duration - 15.0):
                    continue
                if speed > 0.5:
                    continue
                if gap <= 0.0:
                    continue
                stop_candidates.append(row)

    stop_gaps = [float(row[stop_gap_field]) for row in stop_candidates] if stop_gap_field else []
    stop_gap_val = _median(stop_gaps)
    stop_reason = "ok"
    stop_source = "missing"
    stop_status = "not_available"
    if stop_gap_val is not None:
        stop_status = "ok"
        stop_source = f"artifacts/debug_timeseries.csv:{stop_gap_field}"
    else:
        fallback_health_gap = _safe_float(bridge_health.get("front_obstacle_gap_lon_m"))
        if fallback_health_gap is not None and fallback_health_gap > 0.0:
            stop_gap_val = fallback_health_gap
            stop_status = "best_effort"
            stop_source = "artifacts/bridge_health_summary.json:front_obstacle_gap_lon_m"
            stop_reason = "fallback_health_summary"
        else:
            stop_reason = "no_tail_stationary_gap_samples"

    metrics["stop_gap_m"] = {
        "value": stop_gap_val,
        "metric_status": stop_status,
        "source": stop_source,
        "rule": "tail-window near-stop valid-gap median",
        "details": {
            "gap_field": stop_gap_field,
            "sample_count": len(stop_gaps),
            "reason": stop_reason,
            "gap_field_valid_counts": stop_gap_counts,
        },
    }

    cmd_active_threshold = 0.02
    response_threshold = 0.02
    speed_response_threshold = 0.3
    cmd_active = 0
    cmd_effective = 0
    for row in debug_rows:
        c_th = abs(_safe_float(row.get("commanded_throttle")) or 0.0)
        c_br = abs(_safe_float(row.get("commanded_brake")) or 0.0)
        c_st = abs(_safe_float(row.get("commanded_steer")) or 0.0)
        has_cmd = (c_th > cmd_active_threshold) or (c_br > cmd_active_threshold) or (c_st > cmd_active_threshold)
        if not has_cmd:
            continue
        cmd_active += 1
        measured_available = bool(row.get("measured_available"))
        m_th = abs(_safe_float(row.get("measured_throttle")) or 0.0)
        m_br = abs(_safe_float(row.get("measured_brake")) or 0.0)
        m_st = abs(_safe_float(row.get("measured_steer")) or 0.0)
        speed = abs(_safe_float(row.get("speed_mps")) or 0.0)
        has_response = False
        if measured_available and ((m_th > response_threshold) or (m_br > response_threshold) or (m_st > response_threshold)):
            has_response = True
        elif speed > speed_response_threshold:
            has_response = True
        if has_response:
            cmd_effective += 1

    if cmd_active > 0:
        effective_rate = float(cmd_effective / cmd_active)
        control_status = "ok"
        control_source = "artifacts/debug_timeseries.csv"
    else:
        effective_rate = None
        control_status = "not_available"
        control_source = "missing"

    metrics["control_command_effective_rate"] = {
        "value": effective_rate,
        "metric_status": control_status,
        "source": control_source,
        "rule": "effective = active_command and (measured_response or speed_response)",
        "thresholds": {
            "command_active_threshold": cmd_active_threshold,
            "response_threshold": response_threshold,
            "speed_response_threshold_mps": speed_response_threshold,
        },
        "details": {
            "active_command_samples": cmd_active,
            "effective_response_samples": cmd_effective,
        },
    }

    watchdog_count, watchdog_status, watchdog_logs = _count_watchdog_from_logs(
        [
            artifacts_dir / "cyber_control_bridge.err.log",
            artifacts_dir / "cyber_control_bridge.out.log",
        ]
    )
    metrics["watchdog_trigger_count"] = {
        "value": watchdog_count,
        "metric_status": watchdog_status,
        "source": watchdog_logs if watchdog_logs else "missing",
        "rule": "best-effort count of 'source=watchdog' in control-bridge logs",
        "details": {
            "watchdog_metric_status": watchdog_status,
            "note": "log is periodic (~1Hz), count indicates watchdog-active cycles, not per-tick exact triggers",
        },
    }

    tail_static_sec = _compute_tail_static_seconds(debug_rows, speed_threshold=0.3)
    fail_reason = summary.get("fail_reason")
    terminal_tail_rows = []
    if debug_rows:
        end_ts = max([row["ts_sec"] for row in debug_rows if row.get("ts_sec") is not None] or [0.0])
        for row in debug_rows:
            ts = _safe_float(row.get("ts_sec"))
            if ts is None:
                continue
            if ts < end_ts - 12.0:
                continue
            terminal_tail_rows.append(row)

    hold_flags = [bool(row.get("terminal_stop_hold_active")) for row in terminal_tail_rows if row.get("terminal_stop_hold_active") is not None]
    hold_ratio = (sum(1 for x in hold_flags if x) / len(hold_flags)) if hold_flags else 0.0

    distance_to_destination = _safe_float(bridge_health.get("distance_to_destination"))
    routing_goal_dist = _safe_float(bridge_health.get("routing_goal_dist_m"))
    far_from_goal = False
    far_source = "none"
    far_conf = "low"
    if distance_to_destination is not None:
        far_from_goal = distance_to_destination > 25.0
        far_source = "bridge_health_summary.distance_to_destination"
        far_conf = "high"
    elif routing_goal_dist is not None:
        far_from_goal = routing_goal_dist > 80.0
        far_source = "bridge_health_summary.routing_goal_dist_m"
        far_conf = "low"

    stop_gap_value = _safe_float(metrics["stop_gap_m"].get("value"))
    stop_gap_large_or_missing = stop_gap_value is None or stop_gap_value > 25.0

    abnormal = False
    abnormal_reason = "insufficient_evidence"
    if (fail_reason is None or str(fail_reason).strip() == "") and tail_static_sec >= 5.0:
        if far_from_goal and hold_ratio < 0.2 and stop_gap_large_or_missing:
            abnormal = True
            abnormal_reason = "long_tail_stop_far_from_goal_without_terminal_hold"
        else:
            abnormal_reason = "tail_stop_exists_but_not_meeting_far_goal_or_hold_conditions"
    else:
        if fail_reason:
            abnormal_reason = "run_has_fail_reason"
        elif tail_static_sec < 5.0:
            abnormal_reason = "tail_static_duration_short"

    abnormal_status = "ok" if far_source != "none" else "best_effort"
    metrics["abnormal_stop_before_goal"] = {
        "value": abnormal,
        "metric_status": abnormal_status,
        "source": {
            "debug_timeseries": (artifacts_dir / "debug_timeseries.csv").exists(),
            "bridge_health_summary": bool(bridge_health),
            "summary": bool(summary),
        },
        "rule": "tail static >=5s AND far_from_goal AND terminal_stop_hold_ratio<0.2 AND stop_gap_large_or_missing",
        "details": {
            "tail_static_sec": tail_static_sec,
            "terminal_stop_hold_ratio": hold_ratio,
            "stop_gap_m": stop_gap_value,
            "distance_to_destination_m": distance_to_destination,
            "routing_goal_dist_m": routing_goal_dist,
            "far_from_goal": far_from_goal,
            "far_from_goal_source": far_source,
            "far_from_goal_confidence": far_conf,
            "fail_reason": fail_reason,
            "reason": abnormal_reason,
        },
    }

    if not debug_rows:
        notes.append("debug_timeseries.csv missing or unreadable; several metrics downgraded")
    if not timeseries:
        notes.append("timeseries.csv missing or unreadable; launch/max-speed fallback path used")
    if not bridge_health:
        notes.append("bridge_health_summary.json missing; abnormal-stop goal-distance confidence reduced")
    if watchdog_status == "not_available":
        notes.append("watchdog metric not available: control-bridge logs not found")

    result = {
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_profile": {
            "expected_profile": "configs/io/examples/followstop_apollo_gt_baseline.yaml",
            "legacy_profile": "configs/io/examples/followstop_apollo_gt.yaml",
            "effective_yaml": str(effective_yaml) if effective_yaml.exists() else None,
            "marker_check": {
                "matched": marker_match,
                "total": len(baseline_markers),
                "ratio": (float(marker_match) / float(len(baseline_markers))) if baseline_markers else 0.0,
                "method": "text_marker_best_effort",
            },
        },
        "metrics": metrics,
        "notes": notes,
    }
    return result


def _render_markdown(result: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# GT Baseline Metrics")
    lines.append("")
    lines.append(f"- run_dir: `{result.get('run_dir', '')}`")
    lines.append(f"- generated_at_utc: `{result.get('generated_at_utc', '')}`")
    profile = result.get("baseline_profile", {}) or {}
    lines.append(f"- expected_profile: `{profile.get('expected_profile', '')}`")
    marker = (profile.get("marker_check", {}) or {})
    lines.append(
        "- baseline_marker_match: "
        f"{marker.get('matched', 0)}/{marker.get('total', 0)} (ratio={marker.get('ratio', 0.0):.2f})"
    )
    lines.append("")
    lines.append("| Metric | Value | Status | Source |")
    lines.append("|---|---:|---|---|")

    metrics = result.get("metrics", {}) or {}
    for name, payload in metrics.items():
        value = payload.get("value")
        if isinstance(value, dict):
            if "max_speed_mps" in value:
                value_text = (
                    f"{value.get('max_speed_mps')} m/s"
                    f" ({value.get('max_speed_kmh')} km/h)"
                )
            else:
                value_text = json.dumps(value, ensure_ascii=True)
        else:
            value_text = "null" if value is None else str(value)
        status = str(payload.get("metric_status", ""))
        src = payload.get("source")
        if isinstance(src, list):
            src_text = ", ".join(src)
        elif isinstance(src, dict):
            src_text = json.dumps(src, ensure_ascii=True)
        else:
            src_text = str(src)
        lines.append(f"| {name} | {value_text} | {status} | {src_text} |")

    notes = result.get("notes", []) or []
    if notes:
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        for note in notes:
            lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate GT follow-stop Apollo baseline metrics")
    parser.add_argument("--run-dir", required=True, help="Path to one run directory, e.g. runs/<run_name>")
    parser.add_argument(
        "--no-md",
        action="store_true",
        help="Only write gt_baseline_metrics.json and skip markdown output",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists() or not run_dir.is_dir():
        raise SystemExit(f"run directory not found: {run_dir}")

    result = evaluate_run(run_dir)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    out_json = artifacts_dir / "gt_baseline_metrics.json"
    out_json.write_text(json.dumps(result, indent=2, ensure_ascii=False))

    if not args.no_md:
        out_md = artifacts_dir / "gt_baseline_metrics.md"
        out_md.write_text(_render_markdown(result))

    print(f"[gt-baseline-eval] written: {out_json}")
    if not args.no_md:
        print(f"[gt-baseline-eval] written: {artifacts_dir / 'gt_baseline_metrics.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
