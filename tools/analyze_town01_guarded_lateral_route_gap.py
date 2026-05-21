#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_bool, safe_float, safe_int


def _load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _route_gap_rank(row: Dict[str, str]) -> tuple:
    label = str(row.get("route_health_label") or "")
    completion = safe_float(row.get("route_completion_ratio")) or 0.0
    distance_m = safe_float(row.get("route_distance_achieved_m")) or 0.0
    fallback_count = safe_int(row.get("path_fallback_count")) or 0
    label_rank = {
        "route_health_pass": 2,
        "route_health_candidate": 1,
    }.get(label, 0)
    return (label_rank, completion, distance_m, -fallback_count, str(row.get("comparison_label") or ""))


def _select_best_guarded_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    by_route: Dict[str, Dict[str, str]] = {}
    for row in rows:
        if safe_bool(row.get("enable_lateral")) is not True or safe_bool(row.get("enable_guard")) is not True:
            continue
        route_id = str(row.get("route_id") or "").strip()
        if not route_id:
            continue
        current = by_route.get(route_id)
        if current is None or _route_gap_rank(row) > _route_gap_rank(current):
            by_route[route_id] = row
    return [by_route[key] for key in sorted(by_route.keys())]


def _load_debug_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _nearest_debug_row(debug_rows: List[Dict[str, str]], timestamp: Optional[float]) -> Dict[str, Any]:
    if not debug_rows or timestamp is None:
        return {}
    best_row: Optional[Dict[str, str]] = None
    best_delta: Optional[float] = None
    for row in debug_rows:
        ts_sec = safe_float(row.get("ts_sec"))
        if ts_sec is None:
            continue
        delta = abs(ts_sec - float(timestamp))
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_row = row
    if not best_row:
        return {}
    return {
        "nearest_debug_ts_sec": safe_float(best_row.get("ts_sec")),
        "nearest_debug_delta_sec": best_delta,
        "nearest_debug_route_distance_achieved_m": safe_float(best_row.get("route_distance_achieved_m")),
        "nearest_debug_speed_mps": safe_float(best_row.get("speed_mps")),
        "nearest_debug_commanded_brake": safe_float(best_row.get("commanded_brake")),
        "nearest_debug_commanded_throttle": safe_float(best_row.get("commanded_throttle")),
        "nearest_debug_apollo_desired_steer": safe_float(best_row.get("apollo_desired_steer")),
        "nearest_debug_commanded_steer": safe_float(best_row.get("commanded_steer")),
    }


def _planning_warning_counts(planning_log_path: Path) -> Dict[str, int]:
    counts = {
        "path_decider_tunnel_failure_count": 0,
        "path_fallback_due_to_algorithm_failure_count": 0,
        "requested_s_beyond_reference_line_count": 0,
    }
    if not planning_log_path.exists():
        return counts
    for line in planning_log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        lowered = line.lower()
        if "failed to make decision based on tunnel" in lowered:
            counts["path_decider_tunnel_failure_count"] += 1
        if "path fallback due to algorithm failure" in lowered:
            counts["path_fallback_due_to_algorithm_failure_count"] += 1
        if "requested s:" in lowered and "reference line length" in lowered:
            counts["requested_s_beyond_reference_line_count"] += 1
    return counts


def _extract_lon_diff(text: Any) -> Optional[float]:
    match = re.search(r"lon_diff\s*=\s*([-+]?\d+(?:\.\d+)?)", str(text or ""))
    if not match:
        return None
    return safe_float(match.group(1))


def _classify(row: Dict[str, Any]) -> str:
    label = str(row.get("route_health_label") or "")
    completion = safe_float(row.get("route_completion_ratio")) or 0.0
    path_fallback_count = safe_int(row.get("path_fallback_count")) or 0
    persistent = bool(row.get("persistent_path_fallback_at_end"))
    if label == "route_health_pass":
        return "pass"
    if persistent:
        return "persistent_fallback"
    if completion >= 0.55:
        return "candidate"
    if path_fallback_count >= 200:
        return "recoverable_fallback_pressure"
    return "underperforming"


def _planning_phase_stats(planning_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    first_fallback_index: Optional[int] = None
    max_consecutive_fallback = 0
    current_consecutive_fallback = 0
    first_recovery_seq: Optional[int] = None
    first_recovery_timestamp: Optional[float] = None
    first_relapse_after_recovery_seq: Optional[int] = None
    first_relapse_prev_normal_row: Optional[Dict[str, Any]] = None
    first_relapse_row: Optional[Dict[str, Any]] = None
    path_fallback_count_after_first_recovery = 0
    max_consecutive_path_fallback_after_first_recovery = 0
    current_consecutive_path_fallback_after_first_recovery = 0
    normal_count_after_first_recovery = 0
    reentered_path_fallback_after_recovery = False
    first_relapse_cluster_length = 0
    final_normal_tail_length = 0
    running_final_normal_tail = 0
    last_normal_after_recovery_row: Optional[Dict[str, Any]] = None
    tracking_first_relapse_cluster = False
    for idx, row in enumerate(planning_rows):
        traj_type = str(row.get("trajectory_type") or "").strip().upper()
        seq = safe_int(row.get("planning_header_sequence_num"))
        ts = safe_float(row.get("timestamp"))
        if traj_type == "PATH_FALLBACK":
            if first_fallback_index is None:
                first_fallback_index = idx
            current_consecutive_fallback += 1
            max_consecutive_fallback = max(max_consecutive_fallback, current_consecutive_fallback)
            running_final_normal_tail = 0
            continue
        current_consecutive_fallback = 0
        if first_fallback_index is not None and first_recovery_seq is None and traj_type == "NORMAL":
            first_recovery_seq = seq
            first_recovery_timestamp = ts
            last_normal_after_recovery_row = row
        elif first_recovery_seq is not None:
            if traj_type == "PATH_FALLBACK":
                reentered_path_fallback_after_recovery = True
                path_fallback_count_after_first_recovery += 1
                current_consecutive_path_fallback_after_first_recovery += 1
                max_consecutive_path_fallback_after_first_recovery = max(
                    max_consecutive_path_fallback_after_first_recovery,
                    current_consecutive_path_fallback_after_first_recovery,
                )
                if first_relapse_after_recovery_seq is None:
                    first_relapse_after_recovery_seq = seq
                    first_relapse_row = row
                    first_relapse_prev_normal_row = last_normal_after_recovery_row
                    tracking_first_relapse_cluster = True
                if tracking_first_relapse_cluster:
                    first_relapse_cluster_length += 1
            else:
                current_consecutive_path_fallback_after_first_recovery = 0
                if tracking_first_relapse_cluster:
                    tracking_first_relapse_cluster = False
                if traj_type == "NORMAL":
                    normal_count_after_first_recovery += 1
                    last_normal_after_recovery_row = row
        if traj_type == "NORMAL":
            running_final_normal_tail += 1
        else:
            running_final_normal_tail = 0
    final_normal_tail_length = running_final_normal_tail
    first_relapse_prev_normal_total_path_length = safe_float(
        (first_relapse_prev_normal_row or {}).get("trajectory_total_path_length")
    )
    first_relapse_total_path_length = safe_float((first_relapse_row or {}).get("trajectory_total_path_length"))
    first_relapse_path_length_collapse_ratio = None
    if (
        first_relapse_prev_normal_total_path_length is not None
        and first_relapse_total_path_length is not None
        and first_relapse_prev_normal_total_path_length > 0.0
    ):
        first_relapse_path_length_collapse_ratio = (
            float(first_relapse_total_path_length) / float(first_relapse_prev_normal_total_path_length)
        )
    first_relapse_replan_reason = str((first_relapse_row or {}).get("replan_reason") or "").strip() or None
    return {
        "max_consecutive_path_fallback": max_consecutive_fallback,
        "first_recovery_seq": first_recovery_seq,
        "first_recovery_timestamp": first_recovery_timestamp,
        "first_relapse_after_recovery_seq": first_relapse_after_recovery_seq,
        "first_relapse_after_recovery_total_path_length": first_relapse_total_path_length,
        "first_relapse_after_recovery_first_point_v": safe_float((first_relapse_row or {}).get("first_trajectory_point_v")),
        "first_relapse_after_recovery_replan_reason": first_relapse_replan_reason,
        "first_relapse_after_recovery_lon_diff": _extract_lon_diff(first_relapse_replan_reason),
        "first_relapse_prev_normal_total_path_length": first_relapse_prev_normal_total_path_length,
        "first_relapse_prev_normal_first_point_v": safe_float(
            (first_relapse_prev_normal_row or {}).get("first_trajectory_point_v")
        ),
        "first_relapse_cluster_length": first_relapse_cluster_length,
        "first_relapse_path_length_collapse_ratio": first_relapse_path_length_collapse_ratio,
        "path_fallback_count_after_first_recovery": path_fallback_count_after_first_recovery,
        "max_consecutive_path_fallback_after_first_recovery": max_consecutive_path_fallback_after_first_recovery,
        "normal_count_after_first_recovery": normal_count_after_first_recovery,
        "reentered_path_fallback_after_recovery": reentered_path_fallback_after_recovery,
        "final_normal_tail_length": final_normal_tail_length,
    }


def _collect_row(row: Dict[str, str]) -> Dict[str, Any]:
    run_dir = Path(str(row.get("run_dir") or "")).expanduser().resolve()
    summary = load_json(run_dir / "summary.json")
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    planning_rows = load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")
    debug_rows = _load_debug_rows(run_dir / "artifacts" / "debug_timeseries.csv")

    first_fallback_ts = safe_float(planning_summary.get("first_path_fallback_timestamp"))
    first_fallback_debug = _nearest_debug_row(debug_rows, first_fallback_ts)
    final_distance = safe_float(summary.get("route_distance_achieved_m")) or 0.0
    first_fallback_distance = safe_float(first_fallback_debug.get("nearest_debug_route_distance_achieved_m"))
    distance_gain_after_first_fallback = None
    if first_fallback_distance is not None:
        distance_gain_after_first_fallback = float(final_distance) - float(first_fallback_distance)

    counts = planning_summary.get("trajectory_type_counts") or {}
    total_traj_rows = sum(int(v) for v in counts.values() if isinstance(v, int))
    fallback_ratio = None
    if total_traj_rows > 0:
        fallback_ratio = float(safe_int(planning_summary.get("path_fallback_count")) or 0) / float(total_traj_rows)

    collected: Dict[str, Any] = {
        "route_id": row.get("route_id"),
        "comparison_label": row.get("comparison_label"),
        "run_dir": str(run_dir),
        "route_health_label": row.get("route_health_label"),
        "route_completion_ratio": safe_float(summary.get("route_completion_ratio")),
        "route_distance_achieved_m": final_distance,
        "path_fallback_count": safe_int(planning_summary.get("path_fallback_count")),
        "fallback_ratio": fallback_ratio,
        "first_path_fallback_seq": safe_int(planning_summary.get("first_path_fallback_seq")),
        "first_path_fallback_timestamp": first_fallback_ts,
        "first_recovery_seq": safe_int(planning_summary.get("first_recovery_after_path_fallback_seq")),
        "first_recovery_timestamp": safe_float(planning_summary.get("first_recovery_after_path_fallback_timestamp")),
        "first_relapse_after_recovery_seq": safe_int(planning_summary.get("first_relapse_after_recovery_seq")),
        "first_relapse_after_recovery_lon_diff": safe_float(
            planning_summary.get("first_relapse_after_recovery_lon_diff")
        ),
        "first_relapse_cluster_length": safe_int(planning_summary.get("first_relapse_cluster_length")),
        "first_relapse_path_length_collapse_ratio": safe_float(
            planning_summary.get("first_relapse_path_length_collapse_ratio")
        ),
        "first_relapse_prev_normal_total_path_length": safe_float(
            planning_summary.get("first_relapse_prev_normal_total_path_length")
        ),
        "first_relapse_prev_normal_first_point_v": safe_float(
            planning_summary.get("first_relapse_prev_normal_first_point_v")
        ),
        "first_relapse_after_recovery_total_path_length": safe_float(
            planning_summary.get("first_relapse_after_recovery_total_path_length")
        ),
        "first_relapse_after_recovery_first_point_v": safe_float(
            planning_summary.get("first_relapse_after_recovery_first_point_v")
        ),
        "first_relapse_after_recovery_replan_reason": planning_summary.get("first_relapse_after_recovery_replan_reason"),
        "path_fallback_count_after_first_recovery": safe_int(
            planning_summary.get("path_fallback_count_after_first_recovery")
        ),
        "max_consecutive_path_fallback_after_first_recovery": safe_int(
            planning_summary.get("max_consecutive_path_fallback_after_first_recovery")
        ),
        "normal_count_after_first_recovery": safe_int(planning_summary.get("normal_count_after_first_recovery")),
        "reentered_path_fallback_after_recovery": safe_bool(
            planning_summary.get("reentered_path_fallback_after_recovery")
        ),
        "max_consecutive_path_fallback": safe_int(planning_summary.get("max_consecutive_path_fallback_length")),
        "final_path_fallback_suffix_start_seq": safe_int(planning_summary.get("final_path_fallback_suffix_start_seq")),
        "final_path_fallback_suffix_length": safe_int(planning_summary.get("final_path_fallback_suffix_length")),
        "final_normal_tail_length": safe_int(planning_summary.get("final_normal_tail_length")),
        "persistent_path_fallback_at_end": bool(planning_summary.get("persistent_path_fallback_at_end")),
        "recovered_after_path_fallback": bool(planning_summary.get("recovered_after_path_fallback")),
        "last_trajectory_type": planning_summary.get("last_trajectory_type"),
        "last_trajectory_total_path_length": safe_float(planning_summary.get("last_trajectory_total_path_length")),
        "last_trajectory_first_point_v": safe_float(planning_summary.get("last_trajectory_first_point_v")),
        "first_path_fallback_distance_m": first_fallback_distance,
        "distance_gain_after_first_fallback_m": distance_gain_after_first_fallback,
    }
    if planning_rows:
        phase_stats = _planning_phase_stats(planning_rows)
        for key, value in phase_stats.items():
            if collected.get(key) is None:
                collected[key] = value
    recovered_ts = safe_float(collected.get("first_recovery_timestamp"))
    recovered_debug = _nearest_debug_row(debug_rows, recovered_ts)
    collected["first_recovery_distance_m"] = safe_float(recovered_debug.get("nearest_debug_route_distance_achieved_m"))
    if collected["first_recovery_distance_m"] is not None:
        collected["distance_gain_after_first_recovery_m"] = final_distance - float(collected["first_recovery_distance_m"])
    else:
        collected["distance_gain_after_first_recovery_m"] = None
    collected.update(
        {
            "first_path_fallback_speed_mps": safe_float(first_fallback_debug.get("nearest_debug_speed_mps")),
            "first_path_fallback_commanded_brake": safe_float(first_fallback_debug.get("nearest_debug_commanded_brake")),
            "first_path_fallback_apollo_desired_steer": safe_float(
                first_fallback_debug.get("nearest_debug_apollo_desired_steer")
            ),
        }
    )
    collected.update(_planning_warning_counts(run_dir / "artifacts" / "apollo_planning.INFO"))
    collected["classification"] = _classify(collected)
    return collected


def _format_float(value: Any, digits: int = 3) -> str:
    number = safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def render_report(rows: List[Dict[str, Any]], title: str) -> str:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            {"pass": 0, "candidate": 1, "recoverable_fallback_pressure": 2, "persistent_fallback": 3}.get(
                str(row.get("classification") or ""),
                4,
            ),
            -(safe_float(row.get("route_completion_ratio")) or 0.0),
            str(row.get("route_id") or ""),
        ),
    )
    good_rows = [row for row in sorted_rows if str(row.get("classification")) in {"pass", "candidate"}]
    pressured_rows = [
        row for row in sorted_rows if str(row.get("classification")) in {"recoverable_fallback_pressure", "persistent_fallback"}
    ]
    lines = [
        f"# {title}",
        "",
        f"- analyzed_run_count: `{len(sorted_rows)}`",
        "",
        "## Key Conclusion",
        "",
    ]
    if good_rows:
        good_route_ids = ", ".join(f"`{row.get('route_id')}`" for row in good_rows)
        lines.append(f"- healthier guarded lateral routes: {good_route_ids}")
    if pressured_rows:
        pressured_route_ids = ", ".join(f"`{row.get('route_id')}`" for row in pressured_rows)
        lines.append(f"- pressured guarded lateral routes: {pressured_route_ids}")
    if good_rows and pressured_rows:
        avg_good_completion = sum(safe_float(row.get("route_completion_ratio")) or 0.0 for row in good_rows) / len(good_rows)
        avg_pressure_completion = sum(safe_float(row.get("route_completion_ratio")) or 0.0 for row in pressured_rows) / len(pressured_rows)
        avg_good_fallback = sum(safe_int(row.get("path_fallback_count")) or 0 for row in good_rows) / len(good_rows)
        avg_pressure_fallback = sum(safe_int(row.get("path_fallback_count")) or 0 for row in pressured_rows) / len(pressured_rows)
        avg_good_relapse_cluster = sum(safe_int(row.get("first_relapse_cluster_length")) or 0 for row in good_rows) / len(good_rows)
        avg_pressure_relapse_cluster = sum(
            safe_int(row.get("first_relapse_cluster_length")) or 0 for row in pressured_rows
        ) / len(pressured_rows)
        lines.extend(
            [
                f"- avg completion: healthier=`{avg_good_completion:.3f}`, pressured=`{avg_pressure_completion:.3f}`",
                f"- avg path_fallback_count: healthier=`{avg_good_fallback:.1f}`, pressured=`{avg_pressure_fallback:.1f}`",
                f"- avg first relapse cluster length: healthier=`{avg_good_relapse_cluster:.1f}`, pressured=`{avg_pressure_relapse_cluster:.1f}`",
                "- practical split: healthier routes either avoid PATH_FALLBACK entirely or relapse into short fallback clusters and then recover into a long NORMAL tail; pressured routes relapse into materially longer fallback windows and keep re-entering fallback afterward.",
            ]
        )
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| route_id | class | completion | distance_m | path_fallback_count | fallback_ratio | first_fallback_seq | max_consecutive_fallback | first_recovery_seq | final_normal_tail / relapse | first_relapse_window | last_traj_path_len_m | last_traj_first_v_mps | tunnel_failures | algorithm_failures |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in sorted_rows:
        final_normal_tail = safe_int(row.get("final_normal_tail_length")) or 0
        relapse_count = safe_int(row.get("path_fallback_count_after_first_recovery")) or 0
        relapse_streak = safe_int(row.get("max_consecutive_path_fallback_after_first_recovery")) or 0
        relapse_cluster = safe_int(row.get("first_relapse_cluster_length")) or 0
        relapse_collapse_ratio = safe_float(row.get("first_relapse_path_length_collapse_ratio"))
        relapse_lon_diff = safe_float(row.get("first_relapse_after_recovery_lon_diff"))
        first_relapse_window = (
            f"cluster={relapse_cluster}, collapse={_format_float(relapse_collapse_ratio)}, lon_diff={_format_float(relapse_lon_diff)}"
            if relapse_cluster or relapse_collapse_ratio is not None or relapse_lon_diff is not None
            else ""
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("route_id") or ""),
                    str(row.get("classification") or ""),
                    _format_float(row.get("route_completion_ratio")),
                    _format_float(row.get("route_distance_achieved_m")),
                    str(safe_int(row.get("path_fallback_count")) or 0),
                    _format_float(row.get("fallback_ratio")),
                    str(safe_int(row.get("first_path_fallback_seq")) or ""),
                    str(safe_int(row.get("max_consecutive_path_fallback")) or 0),
                    str(safe_int(row.get("first_recovery_seq")) or ""),
                    f"{final_normal_tail} / relapse={relapse_count}, post_max={relapse_streak}",
                    first_relapse_window,
                    _format_float(row.get("last_trajectory_total_path_length")),
                    _format_float(row.get("last_trajectory_first_point_v")),
                    str(safe_int(row.get("path_decider_tunnel_failure_count")) or 0),
                    str(safe_int(row.get("path_fallback_due_to_algorithm_failure_count")) or 0),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "- Keep first-wave guarded lateral smoke anchored on routes that either avoid PATH_FALLBACK entirely or recover with a substantial final NORMAL tail.",
            "- Continue Apollo planning-side root-cause work on routes whose fallback starts early and whose final NORMAL tail stays short.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze guarded lateral route-gap behavior for Town01 route-health runs.")
    parser.add_argument(
        "--comparison-csv",
        default="artifacts/town01_route_health_platform_comparison.csv",
    )
    parser.add_argument(
        "--csv",
        default="artifacts/town01_guarded_lateral_route_gap_analysis_20260326.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/town01_guarded_lateral_route_gap_analysis_20260326.md",
    )
    parser.add_argument(
        "--title",
        default="Town01 Guarded Lateral Route Gap Analysis 2026-03-26",
    )
    args = parser.parse_args()

    source_rows = _load_rows(Path(args.comparison_csv).expanduser().resolve())
    rows = [_collect_row(row) for row in _select_best_guarded_rows(source_rows)]

    csv_path = Path(args.csv).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "route_id",
        "comparison_label",
        "classification",
        "route_health_label",
        "route_completion_ratio",
        "route_distance_achieved_m",
        "path_fallback_count",
        "fallback_ratio",
        "first_path_fallback_seq",
        "max_consecutive_path_fallback",
        "first_recovery_seq",
        "first_relapse_after_recovery_seq",
        "first_relapse_after_recovery_lon_diff",
        "first_relapse_cluster_length",
        "first_relapse_path_length_collapse_ratio",
        "first_relapse_prev_normal_total_path_length",
        "first_relapse_prev_normal_first_point_v",
        "first_relapse_after_recovery_total_path_length",
        "first_relapse_after_recovery_first_point_v",
        "first_relapse_after_recovery_replan_reason",
        "path_fallback_count_after_first_recovery",
        "max_consecutive_path_fallback_after_first_recovery",
        "normal_count_after_first_recovery",
        "reentered_path_fallback_after_recovery",
        "final_normal_tail_length",
        "distance_gain_after_first_fallback_m",
        "distance_gain_after_first_recovery_m",
        "persistent_path_fallback_at_end",
        "last_trajectory_type",
        "path_decider_tunnel_failure_count",
        "path_fallback_due_to_algorithm_failure_count",
        "requested_s_beyond_reference_line_count",
        "run_dir",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    report_path.write_text(render_report(rows, args.title), encoding="utf-8")
    print(f"[town01-guarded-lateral-route-gap] rows={len(rows)} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
