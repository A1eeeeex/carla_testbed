#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import classify_replan_reason, load_json, safe_bool, safe_float, safe_int


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _fmt(value: Any, digits: int = 3) -> str:
    number = safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def _reason_family(reason: Any) -> str:
    text = str(reason or "").strip().lower()
    if not text:
        return ""
    if "replan for empty previous trajectory" in text:
        return "empty_previous_trajectory"
    return classify_replan_reason(reason)


def _first_path_fallback_index(rows: List[Dict[str, Any]]) -> Optional[int]:
    for idx, row in enumerate(rows):
        if str(row.get("trajectory_type") or "").strip() == "PATH_FALLBACK":
            return idx
    return None


def _longest_current_time_smaller_window(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    for row in rows:
        if (
            str(row.get("trajectory_type") or "").strip() == "NORMAL"
            and _reason_family(row.get("replan_reason")) == "current_time_smaller"
        ):
            current.append(row)
            if len(current) > len(best):
                best = list(current)
        else:
            current = []
    return best


def _window_rows(rows: List[Dict[str, Any]], start_idx: int, end_idx: int) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for idx in range(max(0, start_idx), min(len(rows), end_idx + 1)):
        row = rows[idx]
        selected.append(
            {
                "seq": safe_int(row.get("planning_header_sequence_num")),
                "trajectory_type": row.get("trajectory_type"),
                "trajectory_point_count": safe_int(row.get("trajectory_point_count")),
                "trajectory_total_path_length": safe_float(row.get("trajectory_total_path_length")),
                "trajectory_total_time": safe_float(row.get("trajectory_total_time")),
                "trajectory_relative_time_min_sec": safe_float(row.get("trajectory_relative_time_min_sec")),
                "first_trajectory_point_v": safe_float(row.get("first_trajectory_point_v")),
                "is_replan": bool(row.get("is_replan")),
                "replan_reason": row.get("replan_reason"),
                "replan_reason_family": _reason_family(row.get("replan_reason")),
                "routing_segment_count": safe_int(row.get("routing_segment_count")),
                "reference_line_count": safe_int(row.get("reference_line_count")),
            }
        )
    return selected


def _find_last_index(rows: List[Dict[str, Any]], start_idx: int, end_idx: int, predicate) -> Optional[int]:
    for idx in range(end_idx, start_idx - 1, -1):
        if predicate(rows[idx]):
            return idx
    return None


def _case_summary(label: str, run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    rows = _load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")

    fallback_idx = _first_path_fallback_index(rows)
    fallback_seq = safe_int(planning_summary.get("first_path_fallback_seq"))
    precursor_start_seq = safe_int(planning_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_start"))
    precursor_end_seq = safe_int(planning_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_end"))
    bridge_prev_normal_seq = safe_int(planning_summary.get("first_path_fallback_bridge_prev_normal_seq"))

    focus_kind = "first_path_fallback" if fallback_idx is not None else "longest_current_time_smaller"
    if fallback_idx is not None:
        focus_idx = fallback_idx
        if precursor_start_seq is not None:
            start_idx = max(
                0,
                next(
                    (
                        idx
                        for idx, row in enumerate(rows)
                        if safe_int(row.get("planning_header_sequence_num")) == precursor_start_seq
                    ),
                    fallback_idx,
                )
                - 2,
            )
        else:
            start_idx = max(0, fallback_idx - 6)
        if bridge_prev_normal_seq is not None:
            bridge_prev_normal_idx = next(
                (
                    idx
                    for idx, row in enumerate(rows)
                    if safe_int(row.get("planning_header_sequence_num")) == bridge_prev_normal_seq
                ),
                None,
            )
            if bridge_prev_normal_idx is not None:
                start_idx = min(start_idx, bridge_prev_normal_idx)
        end_idx = fallback_idx
    else:
        longest_window = _longest_current_time_smaller_window(rows)
        if not longest_window:
            raise SystemExit(f"No fallback or current_time_smaller NORMAL window found for {label}: {run_dir}")
        start_seq = safe_int(longest_window[0].get("planning_header_sequence_num"))
        end_seq = safe_int(longest_window[-1].get("planning_header_sequence_num"))
        focus_idx = next(
            idx for idx, row in enumerate(rows) if safe_int(row.get("planning_header_sequence_num")) == start_seq
        )
        start_idx = max(0, focus_idx - 4)
        end_idx = min(
            len(rows) - 1,
            next(
                idx for idx, row in enumerate(rows) if safe_int(row.get("planning_header_sequence_num")) == min(end_seq, start_seq + 3)
            ),
        )

    leadin_rows = _window_rows(rows, start_idx, end_idx)
    search_start_idx = start_idx
    last_unknown_idx = _find_last_index(
        rows,
        search_start_idx,
        focus_idx - 1,
        lambda row: str(row.get("trajectory_type") or "").strip() == "UNKNOWN"
        or safe_int(row.get("trajectory_point_count")) == 0,
    )
    last_empty_previous_idx = _find_last_index(
        rows,
        search_start_idx,
        focus_idx - 1,
        lambda row: _reason_family(row.get("replan_reason")) == "empty_previous_trajectory",
    )
    last_matched_point_idx = _find_last_index(
        rows,
        search_start_idx,
        focus_idx - 1,
        lambda row: _reason_family(row.get("replan_reason")) == "matched_point_too_large",
    )
    if (
        last_matched_point_idx is None
        and fallback_idx is not None
        and planning_summary.get("first_path_fallback_trigger_reason_family") == "matched_point_too_large"
    ):
        last_matched_point_idx = fallback_idx
    longest_current_time_smaller = _longest_current_time_smaller_window(rows)
    longest_current_time_smaller_start = (
        safe_int(longest_current_time_smaller[0].get("planning_header_sequence_num")) if longest_current_time_smaller else None
    )
    longest_current_time_smaller_end = (
        safe_int(longest_current_time_smaller[-1].get("planning_header_sequence_num")) if longest_current_time_smaller else None
    )

    if (
        last_unknown_idx is not None
        and last_empty_previous_idx is not None
        and last_empty_previous_idx > last_unknown_idx
    ):
        onset_mode = "unknown_zero_point -> empty_previous_trajectory -> current_time_smaller"
    elif fallback_idx is not None and planning_summary.get("first_path_fallback_trigger_reason_family") == "matched_point_too_large":
        onset_mode = "steady_normal -> matched_point_too_large"
    elif last_matched_point_idx is not None and fallback_idx is None:
        onset_mode = "matched_point_too_large -> long current_time_smaller stability"
    else:
        onset_mode = "other"

    return {
        "label": label,
        "run_dir": str(run_dir),
        "route_id": summary.get("route_id"),
        "route_completion_ratio": safe_float(summary.get("route_completion_ratio")),
        "focus_kind": focus_kind,
        "focus_seq": fallback_seq if fallback_idx is not None else longest_current_time_smaller_start,
        "first_path_fallback_seq": fallback_seq,
        "first_path_fallback_trigger_reason_family": planning_summary.get("first_path_fallback_trigger_reason_family"),
        "first_path_fallback_precurrent_time_smaller_normal_count": safe_int(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_normal_count")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_start": precursor_start_seq,
        "first_path_fallback_precurrent_time_smaller_normal_seq_end": precursor_end_seq,
        "first_path_fallback_precurrent_time_smaller_path_length_min": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_path_length_min")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_max": safe_float(
            planning_summary.get("first_path_fallback_precurrent_time_smaller_path_length_max")
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
        "first_path_fallback_bridge_prev_normal_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_seq")
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
        "first_path_fallback_bridge_unknown_create_route_segments_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_create_route_segments_status"
        ),
        "first_path_fallback_bridge_unknown_lane_follow_map_inconsistent": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_lane_follow_map_inconsistent")
        ),
        "first_path_fallback_bridge_unknown_last_reroute_timestamp": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_last_reroute_timestamp")
        ),
        "first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal")
        ),
        "last_unknown_zero_point_seq": safe_int(rows[last_unknown_idx].get("planning_header_sequence_num"))
        if last_unknown_idx is not None
        else None,
        "last_empty_previous_replan_seq": safe_int(rows[last_empty_previous_idx].get("planning_header_sequence_num"))
        if last_empty_previous_idx is not None
        else None,
        "last_matched_point_replan_seq": safe_int(rows[last_matched_point_idx].get("planning_header_sequence_num"))
        if last_matched_point_idx is not None
        else None,
        "longest_current_time_smaller_start_seq": longest_current_time_smaller_start,
        "longest_current_time_smaller_end_seq": longest_current_time_smaller_end,
        "longest_current_time_smaller_length": len(longest_current_time_smaller),
        "onset_mode": onset_mode,
        "leadin_rows": leadin_rows,
    }


def _render_window(lines: List[str], title: str, rows: List[Dict[str, Any]]) -> None:
    lines.extend(
        [
            f"### {title}",
            "",
            "| seq | type | point_count | path_len | total_time | rel_time_min | v0 | is_replan | reason_family | replan_reason | routing_segments | refline_count |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("seq") or ""),
                    str(row.get("trajectory_type") or ""),
                    str(row.get("trajectory_point_count") or ""),
                    _fmt(row.get("trajectory_total_path_length")),
                    _fmt(row.get("trajectory_total_time")),
                    _fmt(row.get("trajectory_relative_time_min_sec")),
                    _fmt(row.get("first_trajectory_point_v")),
                    "true" if row.get("is_replan") else "false",
                    str(row.get("replan_reason_family") or ""),
                    str(row.get("replan_reason") or ""),
                    str(row.get("routing_segment_count") or ""),
                    str(row.get("reference_line_count") or ""),
                ]
            )
            + " |"
        )
    lines.append("")


def render_report(cases: List[Dict[str, Any]]) -> str:
    by_label = {case["label"]: case for case in cases}
    repeat_219 = by_label.get("219_repeat")
    smoke_219 = by_label.get("219_smoke")
    repeat_097 = by_label.get("097_repeat")

    lines = [
        "# Town01 First-Fallback Lead-In Compare",
        "",
        "## Key Conclusion",
        "",
    ]
    if repeat_219 and smoke_219 and repeat_097:
        lines.append(
            "- `219_repeat` 的 onset mode 已经可以明确写成：`current_time_smaller NORMAL -> UNKNOWN/0-point -> empty previous trajectory -> short-horizon current_time_smaller NORMAL -> PATH_FALLBACK`。"
        )
        lines.append(
            f"- 关键桥接点就是 `219_repeat` 在 "
            f"`seq={repeat_219['first_path_fallback_bridge_prev_normal_seq']}` 仍是 "
            f"`{repeat_219['first_path_fallback_bridge_prev_normal_route_segment_count']} segment / "
            f"{_fmt(repeat_219['first_path_fallback_bridge_prev_normal_route_segment_total_length'])}m` 的 "
            f"`{repeat_219['first_path_fallback_bridge_prev_normal_lane_follow_map_status'] or ''}`，"
            f"随后 `seq={repeat_219['last_unknown_zero_point_seq']}` 跳到 `UNKNOWN/0-point`，"
            f"而且这时 `seg_delta={repeat_219['first_path_fallback_bridge_route_segment_count_delta_from_prev_normal']}`、"
            f"`lane_drop={repeat_219['first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal']}`、"
            f"`failed_with_ready={repeat_219['first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready']}`、"
            f"而且这时 `create_route_segments_status={repeat_219['first_path_fallback_bridge_unknown_create_route_segments_status'] or ''}`、"
            f"`lane_follow_map_inconsistent={repeat_219['first_path_fallback_bridge_unknown_lane_follow_map_inconsistent']}`、"
            f"`reroute_delta={_fmt(repeat_219['first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal'])}s`，"
            f"随后 `seq={repeat_219['last_empty_previous_replan_seq']}` 明确出现 `replan for empty previous trajectory.`，"
            f"再进入 `seq={repeat_219['first_path_fallback_precurrent_time_smaller_normal_seq_start']}..{repeat_219['first_path_fallback_precurrent_time_smaller_normal_seq_end']}` 的 "
            f"短-horizon precursor `path_len={_fmt(repeat_219['first_path_fallback_precurrent_time_smaller_path_length_min'])}..{_fmt(repeat_219['first_path_fallback_precurrent_time_smaller_path_length_max'])}`。"
        )
        lines.append(
            f"- `219_smoke` 没有这条桥：它在 `seq={smoke_219['last_matched_point_replan_seq']}` 前仍是 steady non-replan NORMAL，"
            f"然后在 `seq={smoke_219['first_path_fallback_seq']}` 直接由 `{smoke_219['first_path_fallback_trigger_reason_family']}` 触发 first `PATH_FALLBACK`。"
        )
        lines.append(
            f"- `097_repeat` 也没有 `UNKNOWN/empty previous trajectory` 桥接点；它在 `seq={repeat_097['last_matched_point_replan_seq']}` 发生一次 `matched_point_too_large`，"
            f"随后进入 `seq={repeat_097['longest_current_time_smaller_start_seq']}..{repeat_097['longest_current_time_smaller_end_seq']}` 的长 `current_time_smaller + NORMAL` 稳定窗口，"
            f"长度为 `{repeat_097['longest_current_time_smaller_length']}`。"
        )
        lines.append(
            "- 当前因此可以把第一剩余问题进一步收窄成：为什么 `219 repeat` 会更早掉进 `UNKNOWN/empty previous trajectory` 这条桥接链，"
            "而 `219 smoke` 与 `097 repeat` 都不会。"
        )

    lines.extend(
        [
            "",
            "## Case Summary",
            "",
            "| label | onset_mode | completion | focus_seq | bridge_prev_normal | last_unknown_zero_point_seq | last_empty_previous_replan_seq | precursor_seq | precursor_path_len | precursor_v0 | precursor_point_count |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for case in cases:
        precursor_seq = ""
        precursor_path = ""
        precursor_v0 = ""
        precursor_points = ""
        bridge_prev_normal = ""
        if case.get("first_path_fallback_precurrent_time_smaller_normal_seq_start") is not None:
            precursor_seq = (
                f"{case['first_path_fallback_precurrent_time_smaller_normal_seq_start']}.."
                f"{case['first_path_fallback_precurrent_time_smaller_normal_seq_end']}"
            )
            precursor_path = (
                f"{_fmt(case['first_path_fallback_precurrent_time_smaller_path_length_min'])}.."
                f"{_fmt(case['first_path_fallback_precurrent_time_smaller_path_length_max'])}"
            )
            precursor_v0 = (
                f"{_fmt(case['first_path_fallback_precurrent_time_smaller_first_point_v_min'])}.."
                f"{_fmt(case['first_path_fallback_precurrent_time_smaller_first_point_v_max'])}"
            )
            precursor_points = (
                f"{case['first_path_fallback_precurrent_time_smaller_point_count_min'] or ''}.."
                f"{case['first_path_fallback_precurrent_time_smaller_point_count_max'] or ''}"
            )
        if case.get("first_path_fallback_bridge_prev_normal_seq") is not None:
            bridge_prev_normal = (
                f"{case.get('first_path_fallback_bridge_prev_normal_seq') or ''}/"
                f"{case.get('first_path_fallback_bridge_prev_normal_route_segment_count') or ''}seg/"
                f"{_fmt(case.get('first_path_fallback_bridge_prev_normal_route_segment_total_length'))}m"
            )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(case.get("label") or ""),
                    str(case.get("onset_mode") or ""),
                    _fmt(case.get("route_completion_ratio")),
                    str(case.get("focus_seq") or ""),
                    bridge_prev_normal,
                    str(case.get("last_unknown_zero_point_seq") or ""),
                    str(case.get("last_empty_previous_replan_seq") or ""),
                    precursor_seq,
                    precursor_path,
                    precursor_v0,
                    precursor_points,
                ]
            )
            + " |"
        )
    lines.append("")

    for case in cases:
        lines.extend(
            [
                f"## {case['label']}",
                "",
                f"- route_id: `{case.get('route_id') or ''}`",
                f"- run_dir: `{case.get('run_dir') or ''}`",
                f"- onset_mode: `{case.get('onset_mode') or ''}`",
                f"- completion: `{_fmt(case.get('route_completion_ratio'))}`",
                f"- bridge_prev_normal_seq: `{case.get('first_path_fallback_bridge_prev_normal_seq') or ''}`",
                f"- bridge_prev_normal_route_segment_count: `{case.get('first_path_fallback_bridge_prev_normal_route_segment_count') or ''}`",
                f"- bridge_prev_normal_route_segment_total_length: `{_fmt(case.get('first_path_fallback_bridge_prev_normal_route_segment_total_length'))}`",
                f"- bridge_prev_normal_lane_follow_map_status: `{case.get('first_path_fallback_bridge_prev_normal_lane_follow_map_status') or ''}`",
                f"- bridge_prev_normal_last_reroute_timestamp: `{case.get('first_path_fallback_bridge_prev_normal_last_reroute_timestamp')}`",
                f"- bridge_route_segment_count_delta_from_prev_normal: `{case.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal')}`",
                f"- bridge_route_segment_total_length_delta_from_prev_normal: `{case.get('first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal')}`",
                f"- bridge_unknown_current_lane_dropped_from_prev_normal: `{case.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')}`",
                f"- bridge_unknown_reference_line_failed_with_route_segments_ready: `{case.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')}`",
                f"- last_matched_point_replan_seq: `{case.get('last_matched_point_replan_seq') or ''}`",
                f"- last_unknown_zero_point_seq: `{case.get('last_unknown_zero_point_seq') or ''}`",
                f"- bridge_unknown_create_route_segments_status: `{case.get('first_path_fallback_bridge_unknown_create_route_segments_status') or ''}`",
                f"- bridge_unknown_lane_follow_map_inconsistent: `{case.get('first_path_fallback_bridge_unknown_lane_follow_map_inconsistent')}`",
                f"- bridge_unknown_last_reroute_timestamp: `{case.get('first_path_fallback_bridge_unknown_last_reroute_timestamp')}`",
                f"- bridge_unknown_reroute_delta_sec_from_prev_normal: `{case.get('first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal')}`",
                f"- last_empty_previous_replan_seq: `{case.get('last_empty_previous_replan_seq') or ''}`",
                "",
            ]
        )
        _render_window(lines, "Lead-In Window", case["leadin_rows"])

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare Town01 first-fallback lead-in modes across runs.")
    parser.add_argument(
        "--case",
        action="append",
        required=True,
        help="Case in label=run_dir form. May be repeated.",
    )
    parser.add_argument("--report", required=True, help="Markdown report output path.")
    args = parser.parse_args()

    cases: List[Dict[str, Any]] = []
    for item in args.case:
        if "=" not in item:
            raise SystemExit(f"Invalid --case {item!r}; expected label=run_dir")
        label, raw_path = item.split("=", 1)
        cases.append(_case_summary(label.strip(), Path(raw_path).expanduser().resolve()))

    report_path = Path(args.report).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(cases), encoding="utf-8")
    print(f"[town01-first-fallback-leadin] cases={len(cases)} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
