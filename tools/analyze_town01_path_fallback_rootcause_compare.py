#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_int


PATTERNS: Tuple[str, ...] = (
    "path is empty",
    "failed to make decisions for static obstacles",
    "failed to make decision based on tunnel",
    "path fallback due to algorithm failure",
    "requested s:",
)


def _load_planning_rows(run_dir: Path) -> List[Dict[str, Any]]:
    return load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")


def _trajectory_window(rows: List[Dict[str, Any]], center_seq: int, radius: int = 2) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for row in rows:
        seq = safe_int(row.get("planning_header_sequence_num"))
        if seq is None:
            continue
        if center_seq - radius <= seq <= center_seq + radius:
            selected.append(
                {
                    "seq": seq,
                    "trajectory_type": row.get("trajectory_type"),
                    "trajectory_point_count": safe_int(row.get("trajectory_point_count")),
                    "trajectory_total_path_length": row.get("trajectory_total_path_length"),
                    "reference_line_count": safe_int(row.get("reference_line_count")),
                    "routing_segment_count": safe_int(row.get("routing_segment_count")),
                    "replan_reason": row.get("replan_reason"),
                }
            )
    return selected


def _last_path_fallback_window(rows: List[Dict[str, Any]], *, radius: int = 2) -> Tuple[Optional[int], List[Dict[str, Any]]]:
    fallback_seqs = [
        safe_int(row.get("planning_header_sequence_num"))
        for row in rows
        if str(row.get("trajectory_type") or "") == "PATH_FALLBACK"
    ]
    fallback_seqs = [seq for seq in fallback_seqs if seq is not None]
    if not fallback_seqs:
        return None, []
    center_seq = int(fallback_seqs[-1])
    return center_seq, _trajectory_window(rows, center_seq, radius=radius)


def _log_matches(log_path: Path, *, max_groups: int = 5) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
    counts = {pattern: 0 for pattern in PATTERNS}
    groups: List[Dict[str, Any]] = []
    if not log_path.exists():
        return counts, groups
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for index, line in enumerate(lines):
        lowered = line.lower()
        matched = [pattern for pattern in PATTERNS if pattern in lowered]
        if not matched:
            continue
        for pattern in matched:
            counts[pattern] += 1
        if len(groups) >= max_groups:
            continue
        start = max(0, index - 2)
        end = min(len(lines), index + 3)
        groups.append(
            {
                "line_number": index + 1,
                "matched_patterns": matched,
                "context": [f"{lineno}:{lines[lineno - 1]}" for lineno in range(start + 1, end + 1)],
            }
        )
    return counts, groups


def _run_snapshot(run_dir: Path, *, label: str) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    planning_rows = _load_planning_rows(run_dir)
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    final_suffix_start_seq = safe_int(planning_summary.get("final_path_fallback_suffix_start_seq"))
    if final_suffix_start_seq is not None:
        transition_window = _trajectory_window(planning_rows, final_suffix_start_seq, radius=2)
    else:
        last_fallback_seq, transition_window = _last_path_fallback_window(planning_rows, radius=2)
        final_suffix_start_seq = last_fallback_seq
    log_counts, log_groups = _log_matches(run_dir / "artifacts" / "apollo_planning.INFO")
    return {
        "label": label,
        "run_dir": str(run_dir),
        "route_id": summary.get("route_id"),
        "comparison_label": summary.get("comparison_label"),
        "route_completion_ratio": summary.get("route_completion_ratio"),
        "route_distance_achieved_m": summary.get("route_distance_achieved_m"),
        "route_health_label": summary.get("route_health_label"),
        "persistent_path_fallback_at_end": planning_summary.get("persistent_path_fallback_at_end"),
        "recovered_after_path_fallback": planning_summary.get("recovered_after_path_fallback"),
        "final_path_fallback_suffix_start_seq": final_suffix_start_seq,
        "final_path_fallback_suffix_length": planning_summary.get("final_path_fallback_suffix_length"),
        "transition_window": transition_window,
        "log_counts": log_counts,
        "log_groups": log_groups,
    }


def _render_snapshot(snapshot: Dict[str, Any]) -> List[str]:
    lines = [
        f"## {snapshot['label']}",
        "",
        f"- route_id: `{snapshot.get('route_id')}`",
        f"- comparison_label: `{snapshot.get('comparison_label')}`",
        f"- route_completion_ratio: `{snapshot.get('route_completion_ratio')}`",
        f"- route_distance_achieved_m: `{snapshot.get('route_distance_achieved_m')}`",
        f"- route_health_label: `{snapshot.get('route_health_label')}`",
        f"- persistent_path_fallback_at_end: `{snapshot.get('persistent_path_fallback_at_end')}`",
        f"- recovered_after_path_fallback: `{snapshot.get('recovered_after_path_fallback')}`",
        f"- focus_seq: `{snapshot.get('final_path_fallback_suffix_start_seq')}`",
        f"- final_path_fallback_suffix_length: `{snapshot.get('final_path_fallback_suffix_length')}`",
        "",
        "### Transition Window",
        "",
        "| seq | type | point_count | total_path_length | refline_count | routing_segment_count | replan_reason |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in list(snapshot.get("transition_window") or []):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("seq")),
                    str(row.get("trajectory_type")),
                    str(row.get("trajectory_point_count")),
                    str(row.get("trajectory_total_path_length")),
                    str(row.get("reference_line_count")),
                    str(row.get("routing_segment_count")),
                    str(row.get("replan_reason")),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "### Planning Log Counts",
            "",
        ]
    )
    for pattern, count in snapshot.get("log_counts", {}).items():
        lines.append(f"- `{pattern}`: `{count}`")
    lines.extend(
        [
            "",
            "### Planning Log Context",
            "",
        ]
    )
    groups = list(snapshot.get("log_groups") or [])
    if not groups:
        lines.append("- no matching planning-log context found")
    else:
        for group in groups:
            lines.append(
                f"- line `{group.get('line_number')}` matched `{', '.join(group.get('matched_patterns') or [])}`"
            )
            for line in group.get("context") or []:
                lines.append(f"  - `{line}`")
    lines.append("")
    return lines


def _render_report(persistent_snapshot: Dict[str, Any], recoverable_snapshot: Dict[str, Any]) -> str:
    lines = [
        "# Town01 Path Fallback Root Cause Compare",
        "",
        "## Scope",
        "",
        f"- persistent_run_dir: `{persistent_snapshot['run_dir']}`",
        f"- recoverable_run_dir: `{recoverable_snapshot['run_dir']}`",
        "",
        "## Key Findings",
        "",
        f"- Persistent sample `{persistent_snapshot.get('route_id')}` enters its final fallback suffix at "
        f"`seq={persistent_snapshot.get('final_path_fallback_suffix_start_seq')}` and stays there "
        f"for `length={persistent_snapshot.get('final_path_fallback_suffix_length')}`.",
        f"- Recoverable sample `{recoverable_snapshot.get('route_id')}` reaches "
        f"`completion={recoverable_snapshot.get('route_completion_ratio')}` without any "
        f"`Path is empty / tunnel / algorithm failure` log chain.",
        f"- Current strongest hypothesis: the persistent route fails because PATH_DECIDER receives an empty path; "
        f"the recoverable route does not show this upstream planning failure.",
        "",
    ]
    lines.extend(_render_snapshot(persistent_snapshot))
    lines.extend(_render_snapshot(recoverable_snapshot))
    lines.extend(
        [
            "## Next Step",
            "",
            "- Compare upstream path-generation tasks just before the persistent route enters the final fallback suffix.",
            "- Prioritize Apollo planning investigation over guard/control work when `Path is empty` dominates the planning log.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare persistent vs recoverable Town01 PATH_FALLBACK root causes.")
    parser.add_argument("--persistent-run-dir", required=True)
    parser.add_argument("--recoverable-run-dir", required=True)
    parser.add_argument(
        "--report",
        default="artifacts/town01_path_fallback_rootcause_compare_20260326.md",
        help="Markdown report path.",
    )
    args = parser.parse_args()

    persistent_run_dir = Path(args.persistent_run_dir).expanduser().resolve()
    recoverable_run_dir = Path(args.recoverable_run_dir).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()

    persistent_snapshot = _run_snapshot(persistent_run_dir, label="Persistent PATH_FALLBACK Sample")
    recoverable_snapshot = _run_snapshot(recoverable_run_dir, label="Recoverable PATH_FALLBACK Sample")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        _render_report(persistent_snapshot, recoverable_snapshot),
        encoding="utf-8",
    )
    print(
        "[town01-path-fallback-compare] "
        f"persistent={persistent_snapshot.get('route_id')} "
        f"recoverable={recoverable_snapshot.get('route_id')} "
        f"report={report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
