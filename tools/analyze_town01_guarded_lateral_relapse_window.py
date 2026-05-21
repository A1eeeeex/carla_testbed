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

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_float, safe_int


def _load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _best_guarded_row(rows: List[Dict[str, str]], route_id: str) -> Optional[Dict[str, str]]:
    best: Optional[Dict[str, str]] = None
    best_key: Optional[tuple] = None
    for row in rows:
        if str(row.get("route_id") or "").strip() != route_id:
            continue
        if str(row.get("enable_lateral") or "").strip() != "True":
            continue
        if str(row.get("enable_guard") or "").strip() != "True":
            continue
        label = str(row.get("route_health_label") or "")
        label_rank = {"route_health_pass": 2, "route_health_candidate": 1}.get(label, 0)
        key = (
            label_rank,
            safe_float(row.get("route_completion_ratio")) or 0.0,
            safe_float(row.get("route_distance_achieved_m")) or 0.0,
            -(safe_int(row.get("path_fallback_count")) or 0),
            str(row.get("comparison_label") or ""),
        )
        if best_key is None or key > best_key:
            best = row
            best_key = key
    return best


def _extract_lon_diff(text: Any) -> Optional[float]:
    match = re.search(r"lon_diff\s*=\s*([-+]?\d+(?:\.\d+)?)", str(text or ""))
    if not match:
        return None
    return safe_float(match.group(1))


def _pattern_counts(log_path: Path) -> Dict[str, int]:
    text = log_path.read_text(encoding="utf-8", errors="ignore").lower() if log_path.exists() else ""
    return {
        "tunnel_failure_count": text.count("failed to make decision based on tunnel"),
        "algorithm_failure_count": text.count("path fallback due to algorithm failure"),
        "requested_s_count": text.count("requested s:"),
        "empty_path_count": text.count("path is empty"),
    }


def _window(rows: List[Dict[str, Any]], center_seq: int, *, before: int = 2, after: int = 4) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        seq = safe_int(row.get("planning_header_sequence_num"))
        if seq is None:
            continue
        if center_seq - before <= seq <= center_seq + after:
            out.append(
                {
                    "seq": seq,
                    "trajectory_type": row.get("trajectory_type"),
                    "trajectory_point_count": safe_int(row.get("trajectory_point_count")),
                    "trajectory_total_path_length": safe_float(row.get("trajectory_total_path_length")),
                    "first_trajectory_point_v": safe_float(row.get("first_trajectory_point_v")),
                    "reference_line_count": safe_int(row.get("reference_line_count")),
                    "reference_line_length_max": safe_float(row.get("reference_line_length_max")),
                    "lane_id_first": row.get("lane_id_first"),
                    "target_lane_id_first": row.get("target_lane_id_first"),
                    "replan_reason": row.get("replan_reason"),
                }
            )
    return out


def _collect_route_snapshot(row: Dict[str, str]) -> Dict[str, Any]:
    run_dir = Path(str(row.get("run_dir") or "")).expanduser().resolve()
    summary = load_json(run_dir / "summary.json")
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    planning_rows = load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")
    relapse_seq = safe_int(planning_summary.get("first_relapse_after_recovery_seq"))
    route_snapshot: Dict[str, Any] = {
        "route_id": row.get("route_id"),
        "comparison_label": row.get("comparison_label"),
        "run_dir": str(run_dir),
        "route_health_label": row.get("route_health_label"),
        "route_completion_ratio": safe_float(row.get("route_completion_ratio")),
        "route_distance_achieved_m": safe_float(row.get("route_distance_achieved_m")),
        "first_recovery_after_path_fallback_seq": safe_int(
            planning_summary.get("first_recovery_after_path_fallback_seq")
        ),
        "first_relapse_after_recovery_seq": relapse_seq,
        "first_relapse_after_recovery_lon_diff": safe_float(
            planning_summary.get("first_relapse_after_recovery_lon_diff")
        ),
        "first_relapse_cluster_length": safe_int(planning_summary.get("first_relapse_cluster_length")) or 0,
        "first_relapse_path_length_collapse_ratio": safe_float(
            planning_summary.get("first_relapse_path_length_collapse_ratio")
        ),
        "path_fallback_count_after_first_recovery": safe_int(
            planning_summary.get("path_fallback_count_after_first_recovery")
        )
        or 0,
        "max_consecutive_path_fallback_after_first_recovery": safe_int(
            planning_summary.get("max_consecutive_path_fallback_after_first_recovery")
        )
        or 0,
        "final_normal_tail_length": safe_int(planning_summary.get("final_normal_tail_length")) or 0,
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
        "first_relapse_after_recovery_replan_reason": planning_summary.get(
            "first_relapse_after_recovery_replan_reason"
        ),
    }
    route_snapshot.update(_pattern_counts(run_dir / "artifacts" / "apollo_planning.INFO"))
    route_snapshot["window_rows"] = _window(planning_rows, relapse_seq) if relapse_seq is not None else []
    if route_snapshot["first_relapse_after_recovery_lon_diff"] is None:
        route_snapshot["first_relapse_after_recovery_lon_diff"] = _extract_lon_diff(
            route_snapshot.get("first_relapse_after_recovery_replan_reason")
        )
    return route_snapshot


def _fmt(value: Any, digits: int = 3) -> str:
    number = safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def render_report(rows: List[Dict[str, Any]], title: str) -> str:
    lines = [
        f"# {title}",
        "",
        f"- analyzed_route_count: `{len(rows)}`",
        "",
        "## Key Conclusion",
        "",
    ]
    if rows:
        stable = [
            row
            for row in rows
            if (safe_int(row.get("path_fallback_count_after_first_recovery")) or 0) <= 10
            and (safe_int(row.get("final_normal_tail_length")) or 0) >= 500
        ]
        relapse_heavy = [
            row
            for row in rows
            if (safe_int(row.get("path_fallback_count_after_first_recovery")) or 0) >= 100
            and (safe_int(row.get("first_relapse_cluster_length")) or 0) >= 12
        ]
        if stable:
            lines.append(
                "- stable-recovery routes: "
                + ", ".join(f"`{row.get('route_id')}`" for row in stable)
            )
        if relapse_heavy:
            lines.append(
                "- relapse-heavy routes: "
                + ", ".join(f"`{row.get('route_id')}`" for row in relapse_heavy)
            )
        lines.append(
            "- practical split: stable-recovery routes either avoid relapse or relapse into a short fallback cluster and return to a long NORMAL tail; relapse-heavy routes re-enter fallback with longer first clusters and then accumulate large post-recovery fallback counts."
        )
    lines.extend(
        [
            "",
            "## Route Summary",
            "",
            "| route_id | class | completion | first_recovery_seq | first_relapse_seq | relapse_window | post_recovery_fallbacks | final_normal_tail | planning_log_counts |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        relapse_window = (
            f"cluster={safe_int(row.get('first_relapse_cluster_length')) or 0}, collapse={_fmt(row.get('first_relapse_path_length_collapse_ratio'))}, lon_diff={_fmt(row.get('first_relapse_after_recovery_lon_diff'))}"
            if row.get("first_relapse_after_recovery_seq") is not None
            else ""
        )
        counts_text = (
            f"tunnel={safe_int(row.get('tunnel_failure_count')) or 0}, "
            f"alg={safe_int(row.get('algorithm_failure_count')) or 0}, "
            f"empty={safe_int(row.get('empty_path_count')) or 0}"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("route_id") or ""),
                    str(row.get("route_health_label") or ""),
                    _fmt(row.get("route_completion_ratio")),
                    str(safe_int(row.get("first_recovery_after_path_fallback_seq")) or ""),
                    str(safe_int(row.get("first_relapse_after_recovery_seq")) or ""),
                    relapse_window,
                    f"{safe_int(row.get('path_fallback_count_after_first_recovery')) or 0} (post_max={safe_int(row.get('max_consecutive_path_fallback_after_first_recovery')) or 0})",
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
                f"## {row.get('route_id')}",
                "",
                f"- comparison_label: `{row.get('comparison_label')}`",
                f"- run_dir: `{row.get('run_dir')}`",
                f"- first_recovery_after_path_fallback_seq: `{row.get('first_recovery_after_path_fallback_seq')}`",
                f"- first_relapse_after_recovery_seq: `{row.get('first_relapse_after_recovery_seq')}`",
                f"- first_relapse_after_recovery_replan_reason: `{row.get('first_relapse_after_recovery_replan_reason') or ''}`",
                "",
                "### Relapse Window",
                "",
                "| seq | type | point_count | total_path_length | first_point_v | refline_count | refline_len_max | lane_id | target_lane_id | replan_reason |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for window_row in list(row.get("window_rows") or []):
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(window_row.get("seq") or ""),
                        str(window_row.get("trajectory_type") or ""),
                        str(safe_int(window_row.get("trajectory_point_count")) or ""),
                        _fmt(window_row.get("trajectory_total_path_length")),
                        _fmt(window_row.get("first_trajectory_point_v")),
                        str(safe_int(window_row.get("reference_line_count")) or ""),
                        _fmt(window_row.get("reference_line_length_max")),
                        str(window_row.get("lane_id_first") or ""),
                        str(window_row.get("target_lane_id_first") or ""),
                        str(window_row.get("replan_reason") or ""),
                    ]
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "- Continue Apollo planning-side relapse-window comparison around the first relapse sequence, especially `219:591` vs `213:616`.",
            "- Prioritize explanations for why the same matched-point replan trigger yields a short cluster on `219` but a relapse-heavy post-recovery phase on `213`.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze guarded-lateral relapse windows for selected Town01 routes.")
    parser.add_argument(
        "--comparison-csv",
        default="artifacts/town01_route_health_platform_comparison.csv",
    )
    parser.add_argument(
        "--route-id",
        action="append",
        dest="route_ids",
        default=[],
        help="Route id to include. Repeatable.",
    )
    parser.add_argument(
        "--csv",
        default="artifacts/town01_guarded_lateral_relapse_window_analysis_20260326.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/town01_guarded_lateral_relapse_window_analysis_20260326.md",
    )
    parser.add_argument(
        "--title",
        default="Town01 Guarded Lateral Relapse Window Analysis 2026-03-26",
    )
    args = parser.parse_args()

    route_ids = list(args.route_ids or ["town01_rh_spawn097_goal046", "town01_rh_spawn219_goal046", "town01_rh_spawn213_goal046", "town01_rh_spawn215_goal046"])
    source_rows = _load_rows(Path(args.comparison_csv).expanduser().resolve())
    selected: List[Dict[str, Any]] = []
    for route_id in route_ids:
        row = _best_guarded_row(source_rows, route_id)
        if row is None:
            continue
        selected.append(_collect_route_snapshot(row))

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
        "first_recovery_after_path_fallback_seq",
        "first_relapse_after_recovery_seq",
        "first_relapse_after_recovery_lon_diff",
        "first_relapse_cluster_length",
        "first_relapse_path_length_collapse_ratio",
        "path_fallback_count_after_first_recovery",
        "max_consecutive_path_fallback_after_first_recovery",
        "final_normal_tail_length",
        "tunnel_failure_count",
        "algorithm_failure_count",
        "requested_s_count",
        "empty_path_count",
        "run_dir",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in selected:
            writer.writerow({key: row.get(key) for key in fieldnames})

    report_path.write_text(render_report(selected, args.title), encoding="utf-8")
    print(f"[town01-guarded-lateral-relapse-window] rows={len(selected)} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
