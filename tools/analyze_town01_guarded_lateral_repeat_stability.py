#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_bool, safe_float, safe_int


PATTERNS = {
    "tunnel_failure_count": "failed to make decision based on tunnel",
    "algorithm_failure_count": "path fallback due to algorithm failure",
    "empty_path_count": "path is empty",
    "matched_point_replan_count": "the distance between matched point and actual position is too large",
    "current_time_replan_count": "replan for current time smaller than the previous trajectory's first time",
}

LABEL_ORDER = {
    "lateral_subset_post_pool_fix": 0,
    "lateral_subset_cleaned_regress": 1,
    "guarded_lateral_first_wave_smoke": 2,
    "guarded_lateral_first_wave_repeat": 3,
}


def _load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _sort_key(row: Dict[str, str]) -> tuple:
    label = str(row.get("comparison_label") or "")
    return (
        LABEL_ORDER.get(label, 99),
        -(safe_float(row.get("route_completion_ratio")) or 0.0),
        label,
        str(row.get("run_dir") or ""),
    )


def _window(rows: List[Dict[str, Any]], center_seq: Optional[int], *, before: int = 2, after: int = 4) -> List[Dict[str, Any]]:
    if center_seq is None:
        return []
    selected: List[Dict[str, Any]] = []
    for row in rows:
        seq = safe_int(row.get("planning_header_sequence_num"))
        if seq is None:
            continue
        if center_seq - before <= seq <= center_seq + after:
            selected.append(
                {
                    "seq": seq,
                    "trajectory_type": row.get("trajectory_type"),
                    "trajectory_point_count": safe_int(row.get("trajectory_point_count")),
                    "trajectory_total_path_length": safe_float(row.get("trajectory_total_path_length")),
                    "first_trajectory_point_v": safe_float(row.get("first_trajectory_point_v")),
                    "replan_reason": row.get("replan_reason"),
                    "lane_id_first": row.get("lane_id_first"),
                    "target_lane_id_first": row.get("target_lane_id_first"),
                }
            )
    return selected


def _pattern_counts(log_path: Path) -> Dict[str, int]:
    text = log_path.read_text(encoding="utf-8", errors="ignore").lower() if log_path.exists() else ""
    return {name: text.count(pattern) for name, pattern in PATTERNS.items()}


def _snapshot(row: Dict[str, str]) -> Dict[str, Any]:
    run_dir = Path(str(row.get("run_dir") or "")).expanduser().resolve()
    summary = load_json(run_dir / "summary.json")
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    planning_rows = load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")
    return {
        "route_id": row.get("route_id"),
        "comparison_label": row.get("comparison_label"),
        "run_dir": str(run_dir),
        "route_health_label": row.get("route_health_label"),
        "route_completion_ratio": safe_float(row.get("route_completion_ratio")),
        "route_distance_achieved_m": safe_float(row.get("route_distance_achieved_m")),
        "persistent_path_fallback_at_end": safe_bool(row.get("persistent_path_fallback_at_end")) is True,
        "first_path_fallback_seq": safe_int(planning_summary.get("first_path_fallback_seq")),
        "first_path_fallback_lon_diff": safe_float(planning_summary.get("first_path_fallback_lon_diff")),
        "first_path_fallback_trigger_seq": safe_int(planning_summary.get("first_path_fallback_trigger_seq")),
        "first_path_fallback_trigger_seq_gap": safe_int(planning_summary.get("first_path_fallback_trigger_seq_gap")),
        "first_path_fallback_trigger_reason_family": planning_summary.get("first_path_fallback_trigger_reason_family"),
        "first_path_fallback_trigger_reason_streak_length": safe_int(
            planning_summary.get("first_path_fallback_trigger_reason_streak_length")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_count": safe_int(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_normal_count")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_start": safe_int(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_start")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_end": safe_int(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_end")
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_min": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_rel_min_min")
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_max": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_rel_min_max")
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_min": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_first_point_v_min")
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_max": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_first_point_v_max")
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_min": safe_int(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_point_count_min")
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_max": safe_int(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_point_count_max")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_min": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_path_length_min")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_max": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_path_length_max")
        ),
        "first_path_fallback_bridge_prev_normal_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_seq")
        ),
        "first_path_fallback_bridge_prev_normal_point_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_point_count")
        ),
        "first_path_fallback_bridge_prev_normal_first_point_v": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_first_point_v")
        ),
        "first_path_fallback_bridge_prev_normal_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_total_path_length")
        ),
        "first_path_fallback_bridge_prev_normal_route_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_route_segment_count")
        ),
        "first_path_fallback_bridge_prev_normal_route_segment_total_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_route_segment_total_length")
        ),
        "first_path_fallback_bridge_prev_normal_lane_follow_map_status": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_lane_follow_map_status"
        ),
        "first_path_fallback_bridge_prev_normal_reference_line_provider_status": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_reference_line_provider_status"
        ),
        "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess"
        ),
        "first_path_fallback_bridge_prev_normal_path_end_like_condition": safe_bool(
            planning_summary.get("first_path_fallback_bridge_prev_normal_path_end_like_condition")
        ),
        "first_path_fallback_bridge_prev_normal_current_lane_id": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_current_lane_id"
        ),
        "first_path_fallback_bridge_prev_normal_last_reroute_timestamp": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_last_reroute_timestamp")
        ),
        "first_path_fallback_bridge_route_segment_count_delta_from_prev_normal": safe_int(
            planning_summary.get("first_path_fallback_bridge_route_segment_count_delta_from_prev_normal")
        ),
        "first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal": safe_float(
            planning_summary.get("first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal")
        ),
        "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal")
        ),
        "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready")
        ),
        "first_path_fallback_bridge_unknown_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_seq")
        ),
        "first_path_fallback_bridge_unknown_seq_gap_to_fallback": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_seq_gap_to_fallback")
        ),
        "first_path_fallback_bridge_unknown_route_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_route_segment_count")
        ),
        "first_path_fallback_bridge_unknown_route_segment_total_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_route_segment_total_length")
        ),
        "first_path_fallback_bridge_unknown_create_route_segments_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_create_route_segments_status"
        ),
        "first_path_fallback_bridge_unknown_lane_follow_map_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_lane_follow_map_status"
        ),
        "first_path_fallback_bridge_unknown_lane_follow_map_inconsistent": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_lane_follow_map_inconsistent")
        ),
        "first_path_fallback_bridge_unknown_reference_line_provider_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_reference_line_provider_status"
        ),
        "first_path_fallback_bridge_unknown_planning_empty_reason_guess": planning_summary.get(
            "first_path_fallback_bridge_unknown_planning_empty_reason_guess"
        ),
        "first_path_fallback_bridge_unknown_path_end_like_condition": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_path_end_like_condition")
        ),
        "first_path_fallback_bridge_unknown_current_lane_id": planning_summary.get(
            "first_path_fallback_bridge_unknown_current_lane_id"
        ),
        "first_path_fallback_bridge_unknown_last_reroute_timestamp": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_last_reroute_timestamp")
        ),
        "first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal")
        ),
        "first_path_fallback_bridge_empty_previous_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_seq")
        ),
        "first_path_fallback_bridge_empty_previous_seq_gap_from_unknown": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_seq_gap_from_unknown")
        ),
        "first_path_fallback_bridge_empty_previous_seq_gap_to_fallback": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_seq_gap_to_fallback")
        ),
        "first_path_fallback_bridge_empty_previous_point_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_point_count")
        ),
        "first_path_fallback_bridge_empty_previous_first_point_v": safe_float(
            planning_summary.get("first_path_fallback_bridge_empty_previous_first_point_v")
        ),
        "first_path_fallback_bridge_empty_previous_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_empty_previous_total_path_length")
        ),
        "first_path_fallback_bridge_empty_previous_replan_reason": planning_summary.get(
            "first_path_fallback_bridge_empty_previous_replan_reason"
        ),
        "first_path_fallback_trigger_lon_diff": safe_float(planning_summary.get("first_path_fallback_trigger_lon_diff")),
        "first_path_fallback_trigger_relative_time_min_sec": safe_float(
            planning_summary.get("first_path_fallback_trigger_relative_time_min_sec")
        ),
        "first_path_fallback_trigger_first_point_v": safe_float(
            planning_summary.get("first_path_fallback_trigger_first_point_v")
        ),
        "first_path_fallback_trigger_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_trigger_total_path_length")
        ),
        "first_path_fallback_prev_normal_relative_time_min_sec": safe_float(
            planning_summary.get("first_path_fallback_prev_normal_relative_time_min_sec")
        ),
        "first_path_fallback_cluster_length": safe_int(planning_summary.get("first_path_fallback_cluster_length")) or 0,
        "first_path_fallback_prev_normal_first_point_v": safe_float(
            planning_summary.get("first_path_fallback_prev_normal_first_point_v")
        ),
        "first_path_fallback_prev_normal_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_prev_normal_total_path_length")
        ),
        "first_path_fallback_first_point_v": safe_float(planning_summary.get("first_path_fallback_first_point_v")),
        "first_path_fallback_total_path_length": safe_float(planning_summary.get("first_path_fallback_total_path_length")),
        "first_path_fallback_replan_reason": planning_summary.get("first_path_fallback_replan_reason"),
        "first_path_fallback_trigger_replan_reason": planning_summary.get("first_path_fallback_trigger_replan_reason"),
        "first_recovery_after_path_fallback_seq": safe_int(planning_summary.get("first_recovery_after_path_fallback_seq")),
        "first_relapse_after_recovery_seq": safe_int(planning_summary.get("first_relapse_after_recovery_seq")),
        "first_relapse_cluster_length": safe_int(planning_summary.get("first_relapse_cluster_length")) or 0,
        "first_relapse_after_recovery_reason_family": planning_summary.get(
            "first_relapse_after_recovery_reason_family"
        ),
        "first_relapse_after_recovery_lon_diff": safe_float(planning_summary.get("first_relapse_after_recovery_lon_diff")),
        "first_relapse_after_recovery_replan_reason": planning_summary.get("first_relapse_after_recovery_replan_reason"),
        "path_fallback_count_after_first_recovery": safe_int(
            planning_summary.get("path_fallback_count_after_first_recovery")
        )
        or 0,
        "max_consecutive_path_fallback_after_first_recovery": safe_int(
            planning_summary.get("max_consecutive_path_fallback_after_first_recovery")
        )
        or 0,
        "final_normal_tail_length": safe_int(planning_summary.get("final_normal_tail_length")) or 0,
        "first_relapse_prev_normal_first_point_v": safe_float(
            planning_summary.get("first_relapse_prev_normal_first_point_v")
        ),
        "first_relapse_prev_normal_total_path_length": safe_float(
            planning_summary.get("first_relapse_prev_normal_total_path_length")
        ),
        "first_relapse_after_recovery_first_point_v": safe_float(
            planning_summary.get("first_relapse_after_recovery_first_point_v")
        ),
        "first_relapse_after_recovery_total_path_length": safe_float(
            planning_summary.get("first_relapse_after_recovery_total_path_length")
        ),
        "first_path_fallback_window": _window(
            planning_rows,
            safe_int(planning_summary.get("first_path_fallback_seq")),
        ),
        "first_relapse_window": _window(
            planning_rows,
            safe_int(planning_summary.get("first_relapse_after_recovery_seq")),
        ),
        "planning_log_counts": _pattern_counts(run_dir / "artifacts" / "apollo_planning.INFO"),
}


def _replan_reason_family(reason: Any) -> str:
    text = str(reason or "").strip().lower()
    if not text:
        return ""
    if "current time smaller than the previous trajectory's first time" in text:
        return "current_time_smaller"
    if "distance between matched point and actual position is too large" in text:
        return "matched_point_too_large"
    if "algorithm failure" in text:
        return "algorithm_failure"
    return "other"


def _fmt(value: Any, digits: int = 3) -> str:
    number = safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def _render_window_table(lines: List[str], title: str, rows: List[Dict[str, Any]]) -> None:
    lines.extend(
        [
            f"### {title}",
            "",
            "| seq | type | point_count | total_path_length | first_point_v | lane_id | target_lane_id | replan_reason |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("seq") or ""),
                    str(row.get("trajectory_type") or ""),
                    str(safe_int(row.get("trajectory_point_count")) or ""),
                    _fmt(row.get("trajectory_total_path_length")),
                    _fmt(row.get("first_trajectory_point_v")),
                    str(row.get("lane_id_first") or ""),
                    str(row.get("target_lane_id_first") or ""),
                    str(row.get("replan_reason") or ""),
                ]
            )
            + " |"
        )
    if not rows:
        lines.append("|  |  |  |  |  |  |  |  |")
    lines.append("")


def render_report(route_id: str, rows: List[Dict[str, Any]], title: str) -> str:
    lines = [
        f"# {title}",
        "",
        f"- route_id: `{route_id}`",
        f"- analyzed_run_count: `{len(rows)}`",
        "",
        "## Key Conclusion",
        "",
    ]
    if rows:
        best = max(rows, key=lambda row: safe_float(row.get("route_completion_ratio")) or 0.0)
        worst = max(
            rows,
            key=lambda row: (
                1 if bool(row.get("persistent_path_fallback_at_end")) else 0,
                safe_int(row.get("path_fallback_count_after_first_recovery")) or 0,
                -(safe_int(row.get("final_normal_tail_length")) or 0),
            ),
        )
        lines.append(
            f"- best guarded run is `{best.get('comparison_label')}` with completion=`{_fmt(best.get('route_completion_ratio'))}` "
            f"distance=`{_fmt(best.get('route_distance_achieved_m'))}`."
        )
        lines.append(
            f"- worst guarded run is `{worst.get('comparison_label')}` with persistent_end=`{worst.get('persistent_path_fallback_at_end')}` "
            f"post_recovery_fallbacks=`{worst.get('path_fallback_count_after_first_recovery')}` "
            f"final_normal_tail=`{worst.get('final_normal_tail_length')}`."
        )
        smoke = next((row for row in rows if str(row.get("comparison_label") or "") == "guarded_lateral_first_wave_smoke"), None)
        repeat = next((row for row in rows if str(row.get("comparison_label") or "") == "guarded_lateral_first_wave_repeat"), None)
        if smoke and repeat:
            lines.append(
                "- smoke/repeat first-fallback split: "
                f"smoke precursor_count=`{safe_int(smoke.get('first_path_fallback_precurrent_time_smaller_normal_count')) or 0}` "
                f"v0=`{_fmt(smoke.get('first_path_fallback_precurrent_time_smaller_first_point_v_min'))}..{_fmt(smoke.get('first_path_fallback_precurrent_time_smaller_first_point_v_max'))}` "
                f"points=`{safe_int(smoke.get('first_path_fallback_precurrent_time_smaller_point_count_min')) or ''}..{safe_int(smoke.get('first_path_fallback_precurrent_time_smaller_point_count_max')) or ''}` "
                f"path_len=`{_fmt(smoke.get('first_path_fallback_precurrent_time_smaller_path_length_min'))}..{_fmt(smoke.get('first_path_fallback_precurrent_time_smaller_path_length_max'))}`; "
                f"repeat precursor_count=`{safe_int(repeat.get('first_path_fallback_precurrent_time_smaller_normal_count')) or 0}` "
                f"v0=`{_fmt(repeat.get('first_path_fallback_precurrent_time_smaller_first_point_v_min'))}..{_fmt(repeat.get('first_path_fallback_precurrent_time_smaller_first_point_v_max'))}` "
                f"points=`{safe_int(repeat.get('first_path_fallback_precurrent_time_smaller_point_count_min')) or ''}..{safe_int(repeat.get('first_path_fallback_precurrent_time_smaller_point_count_max')) or ''}` "
                f"path_len=`{_fmt(repeat.get('first_path_fallback_precurrent_time_smaller_path_length_min'))}..{_fmt(repeat.get('first_path_fallback_precurrent_time_smaller_path_length_max'))}`."
            )
            lines.append(
                "- smoke/repeat bridge split: "
                f"smoke prev_normal_seq=`{safe_int(smoke.get('first_path_fallback_bridge_prev_normal_seq')) or ''}` "
                f"bridge_unknown_seq=`{safe_int(smoke.get('first_path_fallback_bridge_unknown_seq')) or ''}` "
                f"bridge_empty_previous_seq=`{safe_int(smoke.get('first_path_fallback_bridge_empty_previous_seq')) or ''}`; "
                f"repeat prev_normal_seq=`{safe_int(repeat.get('first_path_fallback_bridge_prev_normal_seq')) or ''}` "
                f"prev_segments=`{safe_int(repeat.get('first_path_fallback_bridge_prev_normal_route_segment_count')) or ''}` "
                f"prev_lane_follow=`{repeat.get('first_path_fallback_bridge_prev_normal_lane_follow_map_status') or ''}` "
                f"bridge_unknown_seq=`{safe_int(repeat.get('first_path_fallback_bridge_unknown_seq')) or ''}` "
                f"seg_delta=`{safe_int(repeat.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal')) or ''}` "
                f"create_segments=`{repeat.get('first_path_fallback_bridge_unknown_create_route_segments_status') or ''}` "
                f"lane_follow=`{repeat.get('first_path_fallback_bridge_unknown_lane_follow_map_status') or ''}` "
                f"inconsistent=`{repeat.get('first_path_fallback_bridge_unknown_lane_follow_map_inconsistent')}` "
                f"lane_drop=`{repeat.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')}` "
                f"failed_with_ready=`{repeat.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')}` "
                f"empty_reason=`{repeat.get('first_path_fallback_bridge_unknown_planning_empty_reason_guess') or ''}` "
                f"reroute_delta=`{_fmt(repeat.get('first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal'))}` "
                f"bridge_empty_previous_seq=`{safe_int(repeat.get('first_path_fallback_bridge_empty_previous_seq')) or ''}`."
            )
        if len(rows) >= 2:
            earliest_fallback = min(
                (safe_int(row.get("first_path_fallback_seq")) or 10**9, row.get("comparison_label"))
                for row in rows
                if row.get("first_path_fallback_seq") is not None
            )
            lines.append(
                f"- earliest first fallback appears in `{earliest_fallback[1]}` at `seq={earliest_fallback[0]}`; "
                "this is the strongest hint for repeat-instability on the same route."
            )
    lines.extend(
        [
            "",
            "## Run Summary",
            "",
            "| comparison_label | class | completion | distance_m | first_path_fallback_seq | bridge | first_fallback_window | first_recovery_seq | first_relapse_seq | relapse_window | persistent_end | post_recovery_fallbacks | final_normal_tail | planning_log_counts |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        bridge_window = (
            f"prev_normal={safe_int(row.get('first_path_fallback_bridge_prev_normal_seq')) or ''}/"
            f"{safe_int(row.get('first_path_fallback_bridge_prev_normal_route_segment_count')) or ''}seg, "
            f"unknown={safe_int(row.get('first_path_fallback_bridge_unknown_seq')) or ''}/"
            f"{safe_int(row.get('first_path_fallback_bridge_unknown_route_segment_count')) or ''}seg, "
            f"seg_delta={safe_int(row.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal')) or ''}, "
            f"create_segments={row.get('first_path_fallback_bridge_unknown_create_route_segments_status') or ''}, "
            f"empty_prev={safe_int(row.get('first_path_fallback_bridge_empty_previous_seq')) or ''}, "
            f"lane_follow={row.get('first_path_fallback_bridge_unknown_lane_follow_map_status') or ''}, "
            f"inconsistent={row.get('first_path_fallback_bridge_unknown_lane_follow_map_inconsistent')}, "
            f"lane_drop={row.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')}, "
            f"failed_ready={row.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')}, "
            f"reroute_delta={_fmt(row.get('first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal'))}, "
            f"path_end_like={row.get('first_path_fallback_bridge_unknown_path_end_like_condition')}"
            if row.get("first_path_fallback_bridge_unknown_seq") is not None
            else ""
        )
        first_fallback_window = (
            f"cluster={safe_int(row.get('first_path_fallback_cluster_length')) or 0}, "
            f"trigger_gap={safe_int(row.get('first_path_fallback_trigger_seq_gap')) or 0}, "
            f"trigger={row.get('first_path_fallback_trigger_reason_family') or _replan_reason_family(row.get('first_path_fallback_trigger_replan_reason'))}, "
            f"streak={safe_int(row.get('first_path_fallback_trigger_reason_streak_length')) or 0}, "
            f"precursor_count={safe_int(row.get('first_path_fallback_precurrent_time_smaller_normal_count')) or 0}, "
            f"precursor_relmax={_fmt(row.get('first_path_fallback_precurrent_time_smaller_rel_min_max'))}, "
            f"precursor_v0={_fmt(row.get('first_path_fallback_precurrent_time_smaller_first_point_v_min'))}..{_fmt(row.get('first_path_fallback_precurrent_time_smaller_first_point_v_max'))}, "
            f"precursor_points={safe_int(row.get('first_path_fallback_precurrent_time_smaller_point_count_min')) or ''}..{safe_int(row.get('first_path_fallback_precurrent_time_smaller_point_count_max')) or ''}, "
            f"precursor_path_len={_fmt(row.get('first_path_fallback_precurrent_time_smaller_path_length_min'))}..{_fmt(row.get('first_path_fallback_precurrent_time_smaller_path_length_max'))}, "
            f"prev_rtmin={_fmt(row.get('first_path_fallback_prev_normal_relative_time_min_sec'))}, "
            f"trigger_rtmin={_fmt(row.get('first_path_fallback_trigger_relative_time_min_sec'))}, "
            f"lon_diff={_fmt(row.get('first_path_fallback_trigger_lon_diff')) or _fmt(row.get('first_path_fallback_lon_diff'))}"
            if row.get("first_path_fallback_seq") is not None
            else ""
        )
        relapse_window = (
            f"cluster={safe_int(row.get('first_relapse_cluster_length')) or 0}, "
            f"trigger={row.get('first_relapse_after_recovery_reason_family') or _replan_reason_family(row.get('first_relapse_after_recovery_replan_reason'))}, "
            f"lon_diff={_fmt(row.get('first_relapse_after_recovery_lon_diff'))}"
            if row.get("first_relapse_after_recovery_seq") is not None
            else ""
        )
        counts = row.get("planning_log_counts") or {}
        counts_text = (
            f"tunnel={safe_int(counts.get('tunnel_failure_count')) or 0}, "
            f"alg={safe_int(counts.get('algorithm_failure_count')) or 0}, "
            f"empty={safe_int(counts.get('empty_path_count')) or 0}"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("comparison_label") or ""),
                    str(row.get("route_health_label") or ""),
                    _fmt(row.get("route_completion_ratio")),
                    _fmt(row.get("route_distance_achieved_m")),
                    str(safe_int(row.get("first_path_fallback_seq")) or ""),
                    bridge_window,
                    first_fallback_window,
                    str(safe_int(row.get("first_recovery_after_path_fallback_seq")) or ""),
                    str(safe_int(row.get("first_relapse_after_recovery_seq")) or ""),
                    relapse_window,
                    str(bool(row.get("persistent_path_fallback_at_end"))),
                    str(safe_int(row.get("path_fallback_count_after_first_recovery")) or 0),
                    str(safe_int(row.get("final_normal_tail_length")) or 0),
                    counts_text,
                ]
            )
            + " |"
        )

    for row in rows:
        lines.extend(
            [
                "",
                f"## {row.get('comparison_label')}",
                "",
                f"- run_dir: `{row.get('run_dir')}`",
                f"- first_path_fallback_replan_reason: `{row.get('first_path_fallback_replan_reason') or ''}`",
                f"- first_path_fallback_trigger_seq: `{row.get('first_path_fallback_trigger_seq')}`",
                f"- first_path_fallback_trigger_seq_gap: `{row.get('first_path_fallback_trigger_seq_gap')}`",
                f"- first_path_fallback_trigger_replan_reason: `{row.get('first_path_fallback_trigger_replan_reason') or ''}`",
                f"- first_path_fallback_trigger_reason_family: `{row.get('first_path_fallback_trigger_reason_family') or ''}`",
                f"- first_path_fallback_trigger_reason_streak_length: `{row.get('first_path_fallback_trigger_reason_streak_length')}`",
                f"- first_path_fallback_precurrent_time_smaller_normal_count: `{row.get('first_path_fallback_precurrent_time_smaller_normal_count')}`",
                f"- first_path_fallback_precurrent_time_smaller_normal_seq_start: `{row.get('first_path_fallback_precurrent_time_smaller_normal_seq_start')}`",
                f"- first_path_fallback_precurrent_time_smaller_normal_seq_end: `{row.get('first_path_fallback_precurrent_time_smaller_normal_seq_end')}`",
                f"- first_path_fallback_precurrent_time_smaller_rel_min_min: `{row.get('first_path_fallback_precurrent_time_smaller_rel_min_min')}`",
                f"- first_path_fallback_precurrent_time_smaller_rel_min_max: `{row.get('first_path_fallback_precurrent_time_smaller_rel_min_max')}`",
                f"- first_path_fallback_precurrent_time_smaller_first_point_v_min: `{row.get('first_path_fallback_precurrent_time_smaller_first_point_v_min')}`",
                f"- first_path_fallback_precurrent_time_smaller_first_point_v_max: `{row.get('first_path_fallback_precurrent_time_smaller_first_point_v_max')}`",
                f"- first_path_fallback_precurrent_time_smaller_point_count_min: `{row.get('first_path_fallback_precurrent_time_smaller_point_count_min')}`",
                f"- first_path_fallback_precurrent_time_smaller_point_count_max: `{row.get('first_path_fallback_precurrent_time_smaller_point_count_max')}`",
                f"- first_path_fallback_precurrent_time_smaller_path_length_min: `{row.get('first_path_fallback_precurrent_time_smaller_path_length_min')}`",
                f"- first_path_fallback_precurrent_time_smaller_path_length_max: `{row.get('first_path_fallback_precurrent_time_smaller_path_length_max')}`",
                f"- first_path_fallback_bridge_prev_normal_seq: `{row.get('first_path_fallback_bridge_prev_normal_seq')}`",
                f"- first_path_fallback_bridge_prev_normal_route_segment_count: `{row.get('first_path_fallback_bridge_prev_normal_route_segment_count')}`",
                f"- first_path_fallback_bridge_prev_normal_route_segment_total_length: `{row.get('first_path_fallback_bridge_prev_normal_route_segment_total_length')}`",
                f"- first_path_fallback_bridge_prev_normal_lane_follow_map_status: `{row.get('first_path_fallback_bridge_prev_normal_lane_follow_map_status') or ''}`",
                f"- first_path_fallback_bridge_prev_normal_reference_line_provider_status: `{row.get('first_path_fallback_bridge_prev_normal_reference_line_provider_status') or ''}`",
                f"- first_path_fallback_bridge_prev_normal_path_end_like_condition: `{row.get('first_path_fallback_bridge_prev_normal_path_end_like_condition')}`",
                f"- first_path_fallback_bridge_prev_normal_current_lane_id: `{row.get('first_path_fallback_bridge_prev_normal_current_lane_id') or ''}`",
                f"- first_path_fallback_bridge_prev_normal_last_reroute_timestamp: `{row.get('first_path_fallback_bridge_prev_normal_last_reroute_timestamp')}`",
                f"- first_path_fallback_bridge_route_segment_count_delta_from_prev_normal: `{row.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal')}`",
                f"- first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal: `{row.get('first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal')}`",
                f"- first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal: `{row.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')}`",
                f"- first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready: `{row.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')}`",
                f"- first_path_fallback_bridge_unknown_seq: `{row.get('first_path_fallback_bridge_unknown_seq')}`",
                f"- first_path_fallback_bridge_unknown_route_segment_count: `{row.get('first_path_fallback_bridge_unknown_route_segment_count')}`",
                f"- first_path_fallback_bridge_unknown_route_segment_total_length: `{row.get('first_path_fallback_bridge_unknown_route_segment_total_length')}`",
                f"- first_path_fallback_bridge_unknown_create_route_segments_status: `{row.get('first_path_fallback_bridge_unknown_create_route_segments_status') or ''}`",
                f"- first_path_fallback_bridge_unknown_lane_follow_map_status: `{row.get('first_path_fallback_bridge_unknown_lane_follow_map_status') or ''}`",
                f"- first_path_fallback_bridge_unknown_lane_follow_map_inconsistent: `{row.get('first_path_fallback_bridge_unknown_lane_follow_map_inconsistent')}`",
                f"- first_path_fallback_bridge_unknown_reference_line_provider_status: `{row.get('first_path_fallback_bridge_unknown_reference_line_provider_status') or ''}`",
                f"- first_path_fallback_bridge_unknown_planning_empty_reason_guess: `{row.get('first_path_fallback_bridge_unknown_planning_empty_reason_guess') or ''}`",
                f"- first_path_fallback_bridge_unknown_path_end_like_condition: `{row.get('first_path_fallback_bridge_unknown_path_end_like_condition')}`",
                f"- first_path_fallback_bridge_unknown_last_reroute_timestamp: `{row.get('first_path_fallback_bridge_unknown_last_reroute_timestamp')}`",
                f"- first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal: `{row.get('first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal')}`",
                f"- first_path_fallback_bridge_empty_previous_seq: `{row.get('first_path_fallback_bridge_empty_previous_seq')}`",
                f"- first_path_fallback_bridge_empty_previous_seq_gap_from_unknown: `{row.get('first_path_fallback_bridge_empty_previous_seq_gap_from_unknown')}`",
                f"- first_path_fallback_bridge_empty_previous_total_path_length: `{row.get('first_path_fallback_bridge_empty_previous_total_path_length')}`",
                f"- first_path_fallback_bridge_empty_previous_replan_reason: `{row.get('first_path_fallback_bridge_empty_previous_replan_reason') or ''}`",
                f"- first_path_fallback_prev_normal_relative_time_min_sec: `{row.get('first_path_fallback_prev_normal_relative_time_min_sec')}`",
                f"- first_path_fallback_trigger_relative_time_min_sec: `{row.get('first_path_fallback_trigger_relative_time_min_sec')}`",
                f"- first_path_fallback_trigger_first_point_v: `{row.get('first_path_fallback_trigger_first_point_v')}`",
                f"- first_path_fallback_trigger_total_path_length: `{row.get('first_path_fallback_trigger_total_path_length')}`",
                f"- first_path_fallback_prev_normal_first_point_v: `{row.get('first_path_fallback_prev_normal_first_point_v')}`",
                f"- first_path_fallback_prev_normal_total_path_length: `{row.get('first_path_fallback_prev_normal_total_path_length')}`",
                f"- first_path_fallback_first_point_v: `{row.get('first_path_fallback_first_point_v')}`",
                f"- first_path_fallback_total_path_length: `{row.get('first_path_fallback_total_path_length')}`",
                f"- first_relapse_after_recovery_replan_reason: `{row.get('first_relapse_after_recovery_replan_reason') or ''}`",
                f"- first_relapse_prev_normal_first_point_v: `{row.get('first_relapse_prev_normal_first_point_v')}`",
                f"- first_relapse_prev_normal_total_path_length: `{row.get('first_relapse_prev_normal_total_path_length')}`",
                f"- first_relapse_after_recovery_first_point_v: `{row.get('first_relapse_after_recovery_first_point_v')}`",
                f"- first_relapse_after_recovery_total_path_length: `{row.get('first_relapse_after_recovery_total_path_length')}`",
                "",
            ]
        )
        _render_window_table(lines, "First PATH_FALLBACK Window", list(row.get("first_path_fallback_window") or []))
        _render_window_table(lines, "First Relapse Window", list(row.get("first_relapse_window") or []))

    lines.extend(
        [
            "## Next Step",
            "",
            "- Compare the same-route relapse windows first, before broadening to more routes.",
            "- Prioritize explaining why the repeat run enters fallback earlier and ends with zero NORMAL tail.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare guarded-lateral repeat stability for the same Town01 route.")
    parser.add_argument("--comparison-csv", default="artifacts/town01_route_health_platform_comparison.csv")
    parser.add_argument("--route-id", default="town01_rh_spawn219_goal046")
    parser.add_argument(
        "--csv",
        default="artifacts/town01_guarded_lateral_repeat_stability_20260326.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/town01_guarded_lateral_repeat_stability_20260326.md",
    )
    parser.add_argument(
        "--title",
        default="Town01 Guarded Lateral Repeat Stability Analysis 2026-03-26",
    )
    args = parser.parse_args()

    rows = [
        row
        for row in _load_rows(Path(args.comparison_csv).expanduser().resolve())
        if str(row.get("route_id") or "").strip() == str(args.route_id).strip()
        and safe_bool(row.get("enable_lateral")) is True
        and safe_bool(row.get("enable_guard")) is True
    ]
    snapshots = [_snapshot(row) for row in sorted(rows, key=_sort_key)]

    csv_path = Path(args.csv).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "route_id",
        "comparison_label",
        "route_health_label",
        "route_completion_ratio",
        "route_distance_achieved_m",
        "first_path_fallback_seq",
        "first_path_fallback_lon_diff",
        "first_path_fallback_trigger_seq",
        "first_path_fallback_trigger_seq_gap",
        "first_path_fallback_trigger_reason_family",
        "first_path_fallback_trigger_reason_streak_length",
        "first_path_fallback_precurrent_time_smaller_normal_count",
        "first_path_fallback_precurrent_time_smaller_normal_seq_start",
        "first_path_fallback_precurrent_time_smaller_normal_seq_end",
        "first_path_fallback_precurrent_time_smaller_rel_min_min",
        "first_path_fallback_precurrent_time_smaller_rel_min_max",
        "first_path_fallback_precurrent_time_smaller_first_point_v_min",
        "first_path_fallback_precurrent_time_smaller_first_point_v_max",
        "first_path_fallback_precurrent_time_smaller_point_count_min",
        "first_path_fallback_precurrent_time_smaller_point_count_max",
        "first_path_fallback_precurrent_time_smaller_path_length_min",
        "first_path_fallback_precurrent_time_smaller_path_length_max",
        "first_path_fallback_bridge_prev_normal_seq",
        "first_path_fallback_bridge_prev_normal_point_count",
        "first_path_fallback_bridge_prev_normal_first_point_v",
        "first_path_fallback_bridge_prev_normal_total_path_length",
        "first_path_fallback_bridge_prev_normal_route_segment_count",
        "first_path_fallback_bridge_prev_normal_route_segment_total_length",
        "first_path_fallback_bridge_prev_normal_lane_follow_map_status",
        "first_path_fallback_bridge_prev_normal_reference_line_provider_status",
        "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess",
        "first_path_fallback_bridge_prev_normal_path_end_like_condition",
        "first_path_fallback_bridge_prev_normal_current_lane_id",
        "first_path_fallback_bridge_prev_normal_last_reroute_timestamp",
        "first_path_fallback_bridge_route_segment_count_delta_from_prev_normal",
        "first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal",
        "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal",
        "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready",
        "first_path_fallback_bridge_unknown_seq",
        "first_path_fallback_bridge_unknown_seq_gap_to_fallback",
        "first_path_fallback_bridge_unknown_route_segment_count",
        "first_path_fallback_bridge_unknown_route_segment_total_length",
        "first_path_fallback_bridge_unknown_create_route_segments_status",
        "first_path_fallback_bridge_unknown_lane_follow_map_status",
        "first_path_fallback_bridge_unknown_lane_follow_map_inconsistent",
        "first_path_fallback_bridge_unknown_reference_line_provider_status",
        "first_path_fallback_bridge_unknown_planning_empty_reason_guess",
        "first_path_fallback_bridge_unknown_path_end_like_condition",
        "first_path_fallback_bridge_unknown_last_reroute_timestamp",
        "first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal",
        "first_path_fallback_bridge_empty_previous_seq",
        "first_path_fallback_bridge_empty_previous_seq_gap_from_unknown",
        "first_path_fallback_bridge_empty_previous_seq_gap_to_fallback",
        "first_path_fallback_bridge_empty_previous_total_path_length",
        "first_path_fallback_trigger_lon_diff",
        "first_path_fallback_trigger_relative_time_min_sec",
        "first_path_fallback_trigger_first_point_v",
        "first_path_fallback_trigger_total_path_length",
        "first_path_fallback_prev_normal_relative_time_min_sec",
        "first_path_fallback_cluster_length",
        "first_recovery_after_path_fallback_seq",
        "first_relapse_after_recovery_seq",
        "first_relapse_cluster_length",
        "first_relapse_after_recovery_lon_diff",
        "path_fallback_count_after_first_recovery",
        "max_consecutive_path_fallback_after_first_recovery",
        "final_normal_tail_length",
        "persistent_path_fallback_at_end",
        "run_dir",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in snapshots:
            writer.writerow({name: row.get(name) for name in fieldnames})

    report_path.write_text(render_report(args.route_id, snapshots, args.title), encoding="utf-8")
    print(f"[town01-guarded-repeat-stability] route_id={args.route_id} rows={len(snapshots)} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
