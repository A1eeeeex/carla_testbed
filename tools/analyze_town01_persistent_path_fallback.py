#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_float, safe_int
from tools.run_town01_route_health import _discover_batch_runs, _resolve_run_dir


def _load_debug_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _nearest_debug_row(debug_rows: List[Dict[str, str]], timestamp: Optional[float]) -> Dict[str, Any]:
    if not debug_rows or timestamp is None:
        return {}
    best_row: Optional[Dict[str, str]] = None
    best_delta: Optional[float] = None
    for row in debug_rows:
        ts_sec = safe_float(row.get("ts_sec"))
        if ts_sec is None:
            continue
        delta = abs(float(ts_sec) - float(timestamp))
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_row = row
    if not best_row:
        return {}
    return {
        "nearest_debug_ts_sec": safe_float(best_row.get("ts_sec")),
        "nearest_debug_delta_sec": best_delta,
        "nearest_debug_speed_mps": safe_float(best_row.get("speed_mps")),
        "nearest_debug_route_distance_achieved_m": safe_float(best_row.get("route_distance_achieved_m")),
        "nearest_debug_commanded_brake": safe_float(best_row.get("commanded_brake")),
        "nearest_debug_commanded_throttle": safe_float(best_row.get("commanded_throttle")),
        "nearest_debug_apollo_desired_steer": safe_float(best_row.get("apollo_desired_steer")),
        "nearest_debug_commanded_steer": safe_float(best_row.get("commanded_steer")),
        "nearest_debug_planning_seq": safe_int(best_row.get("planning_lateral_latest_sequence_num")),
        "nearest_debug_planning_point_count": safe_int(best_row.get("planning_lateral_latest_point_count")),
        "nearest_debug_guard_applied": str(best_row.get("trajectory_contract_lateral_guard_applied") or "").lower() == "true",
        "nearest_debug_force_zero_steer_applied": str(best_row.get("force_zero_steer_applied") or "").lower() == "true",
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


def _collect_run_row(run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    route_id = str(summary.get("route_id") or "")
    comparison_label = str(summary.get("comparison_label") or "")
    debug_rows = _load_debug_rows(run_dir / "artifacts" / "debug_timeseries.csv")
    fallback_ts = safe_float(planning_summary.get("final_path_fallback_suffix_start_timestamp"))
    row: Dict[str, Any] = {
        "run_dir": str(run_dir),
        "comparison_label": comparison_label,
        "route_id": route_id,
        "route_completion_ratio": safe_float(summary.get("route_completion_ratio")),
        "route_distance_achieved_m": safe_float(summary.get("route_distance_achieved_m")),
        "final_goal_distance_m": safe_float(summary.get("final_goal_distance_m")),
        "path_fallback_count": safe_int(planning_summary.get("path_fallback_count")),
        "first_path_fallback_seq": safe_int(planning_summary.get("first_path_fallback_seq")),
        "final_path_fallback_suffix_start_seq": safe_int(planning_summary.get("final_path_fallback_suffix_start_seq")),
        "final_path_fallback_suffix_start_timestamp": fallback_ts,
        "final_path_fallback_suffix_start_point_count": safe_int(
            planning_summary.get("final_path_fallback_suffix_start_point_count")
        ),
        "final_path_fallback_suffix_start_first_point_v": safe_float(
            planning_summary.get("final_path_fallback_suffix_start_first_point_v")
        ),
        "final_path_fallback_suffix_start_total_path_length": safe_float(
            planning_summary.get("final_path_fallback_suffix_start_total_path_length")
        ),
        "final_path_fallback_suffix_length": safe_int(planning_summary.get("final_path_fallback_suffix_length")),
        "last_normal_before_final_path_fallback_seq": safe_int(
            planning_summary.get("last_normal_before_final_path_fallback_seq")
        ),
        "persistent_path_fallback_at_end": bool(planning_summary.get("persistent_path_fallback_at_end")),
        "recovered_after_path_fallback": bool(planning_summary.get("recovered_after_path_fallback")),
        "last_trajectory_type": planning_summary.get("last_trajectory_type"),
    }
    row.update(_nearest_debug_row(debug_rows, fallback_ts))
    row.update(_planning_warning_counts(run_dir / "artifacts" / "apollo_planning.INFO"))
    return row


def _render_report(rows: List[Dict[str, Any]], batch_roots: List[Path]) -> str:
    persistent_rows = [row for row in rows if bool(row.get("persistent_path_fallback_at_end"))]
    recovered_rows = [
        row
        for row in rows
        if bool(row.get("recovered_after_path_fallback")) and not bool(row.get("persistent_path_fallback_at_end"))
    ]
    lines = [
        "# Town01 Persistent Path Fallback Report",
        "",
        "## Scope",
        "",
        "- Batch roots:",
    ]
    for batch_root in batch_roots:
        lines.append(f"  - `{batch_root}`")
    lines.extend(
        [
            "",
            f"- analyzed_run_count: `{len(rows)}`",
            f"- persistent_path_fallback_run_count: `{len(persistent_rows)}`",
            f"- recovered_path_fallback_run_count: `{len(recovered_rows)}`",
            "",
            "## Key Findings",
            "",
        ]
    )
    if persistent_rows:
        worst = sorted(
            persistent_rows,
            key=lambda item: safe_float(item.get("route_completion_ratio")) or 0.0,
        )[0]
        lines.extend(
            [
                f"- Persistent fallback strongest example: `{worst.get('route_id')}` "
                f"(completion=`{worst.get('route_completion_ratio')}`, final_suffix_start_seq=`{worst.get('final_path_fallback_suffix_start_seq')}`, "
                f"final_suffix_length=`{worst.get('final_path_fallback_suffix_length')}`).",
                f"- At final suffix start, nearest debug row shows "
                f"`distance={worst.get('nearest_debug_route_distance_achieved_m')}`, "
                f"`speed={worst.get('nearest_debug_speed_mps')}`, "
                f"`brake={worst.get('nearest_debug_commanded_brake')}`, "
                f"`planning_seq={worst.get('nearest_debug_planning_seq')}`, "
                f"`planning_points={worst.get('nearest_debug_planning_point_count')}`.",
                f"- Apollo planning warning counts on that run: "
                f"`path_decider_tunnel_failure_count={worst.get('path_decider_tunnel_failure_count')}`, "
                f"`path_fallback_due_to_algorithm_failure_count={worst.get('path_fallback_due_to_algorithm_failure_count')}`, "
                f"`requested_s_beyond_reference_line_count={worst.get('requested_s_beyond_reference_line_count')}`.",
            ]
        )
    if recovered_rows:
        best = sorted(
            recovered_rows,
            key=lambda item: safe_float(item.get("route_completion_ratio")) or 0.0,
            reverse=True,
        )[0]
        lines.extend(
            [
                f"- Recoverable fallback strongest example: `{best.get('route_id')}` "
                f"(completion=`{best.get('route_completion_ratio')}`, last_trajectory_type=`{best.get('last_trajectory_type')}`).",
                f"- Its Apollo planning warning counts are "
                f"`path_decider_tunnel_failure_count={best.get('path_decider_tunnel_failure_count')}`, "
                f"`path_fallback_due_to_algorithm_failure_count={best.get('path_fallback_due_to_algorithm_failure_count')}`, "
                f"`requested_s_beyond_reference_line_count={best.get('requested_s_beyond_reference_line_count')}`.",
            ]
        )
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| route_id | comparison_label | completion | distance_m | persistent_end | final_suffix_start_seq | final_suffix_length | nearest_debug_distance_m | nearest_debug_speed_mps | nearest_debug_brake | tunnel_failures | path_fallback_failures | requested_s_beyond_refline |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in sorted(rows, key=lambda item: (str(item.get("route_id") or ""), str(item.get("comparison_label") or ""))):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("route_id") or ""),
                    str(row.get("comparison_label") or ""),
                    str(row.get("route_completion_ratio")),
                    str(row.get("route_distance_achieved_m")),
                    str(row.get("persistent_path_fallback_at_end")),
                    str(row.get("final_path_fallback_suffix_start_seq")),
                    str(row.get("final_path_fallback_suffix_length")),
                    str(row.get("nearest_debug_route_distance_achieved_m")),
                    str(row.get("nearest_debug_speed_mps")),
                    str(row.get("nearest_debug_commanded_brake")),
                    str(row.get("path_decider_tunnel_failure_count")),
                    str(row.get("path_fallback_due_to_algorithm_failure_count")),
                    str(row.get("requested_s_beyond_reference_line_count")),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "- Prioritize Apollo planning/root-cause work on routes whose final suffix stays in `PATH_FALLBACK` to the end.",
            "- Distinguish route-specific tunnel/path-decider failures from terminal route/reference-line geometry failures using the new final-suffix fields.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze persistent PATH_FALLBACK behavior in Town01 route-health batches.")
    parser.add_argument("--batch-root", action="append", required=True, help="Town01 batch root to analyze. Repeatable.")
    parser.add_argument(
        "--csv",
        default="artifacts/town01_persistent_path_fallback_analysis.csv",
        help="CSV output path.",
    )
    parser.add_argument(
        "--report",
        default="artifacts/town01_persistent_path_fallback_analysis.md",
        help="Markdown report output path.",
    )
    args = parser.parse_args()

    batch_roots = [Path(item).expanduser().resolve() for item in args.batch_root]
    rows: List[Dict[str, Any]] = []
    for batch_root in batch_roots:
        for discovered in _discover_batch_runs(batch_root):
            effective_dir = _resolve_run_dir(Path(str(discovered["effective_run_dir"])))
            summary_path = effective_dir / "summary.json"
            if not summary_path.exists():
                continue
            rows.append(_collect_run_row(effective_dir))

    csv_path = Path(args.csv).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "route_id",
        "comparison_label",
        "run_dir",
        "route_completion_ratio",
        "route_distance_achieved_m",
        "final_goal_distance_m",
        "path_fallback_count",
        "first_path_fallback_seq",
        "final_path_fallback_suffix_start_seq",
        "final_path_fallback_suffix_start_timestamp",
        "final_path_fallback_suffix_start_point_count",
        "final_path_fallback_suffix_start_first_point_v",
        "final_path_fallback_suffix_start_total_path_length",
        "final_path_fallback_suffix_length",
        "last_normal_before_final_path_fallback_seq",
        "persistent_path_fallback_at_end",
        "recovered_after_path_fallback",
        "last_trajectory_type",
        "nearest_debug_ts_sec",
        "nearest_debug_delta_sec",
        "nearest_debug_route_distance_achieved_m",
        "nearest_debug_speed_mps",
        "nearest_debug_commanded_brake",
        "nearest_debug_commanded_throttle",
        "nearest_debug_apollo_desired_steer",
        "nearest_debug_commanded_steer",
        "nearest_debug_planning_seq",
        "nearest_debug_planning_point_count",
        "nearest_debug_guard_applied",
        "nearest_debug_force_zero_steer_applied",
        "path_decider_tunnel_failure_count",
        "path_fallback_due_to_algorithm_failure_count",
        "requested_s_beyond_reference_line_count",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})

    report_path.write_text(_render_report(rows, batch_roots), encoding="utf-8")
    print(f"[town01-persistent-fallback] rows={len(rows)} csv={csv_path} report={report_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
