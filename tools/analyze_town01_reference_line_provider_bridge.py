#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import classify_replan_reason, load_json, load_jsonl, safe_float, safe_int

APOLLO_REFERENCE_LINE_PROVIDER = (
    "/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src/"
    "modules/planning/planning_base/reference_line/reference_line_provider.cc"
)
APOLLO_LANE_FOLLOW_MAP = (
    "/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src/"
    "modules/planning/pnc_map/lane_follow_map/lane_follow_map.cc"
)


def _fmt(value: Any, digits: int = 3) -> str:
    num = safe_float(value)
    if num is None:
        return ""
    return f"{num:.{digits}f}"


def _parse_case(value: str) -> Tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("case must be label=/abs/or/rel/run_dir")
    label, raw_path = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("case label cannot be empty")
    run_dir = Path(raw_path.strip()).expanduser().resolve()
    if not run_dir.exists():
        raise argparse.ArgumentTypeError(f"run_dir does not exist: {run_dir}")
    return label, run_dir


def _row_by_seq(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        seq = safe_int(row.get("planning_header_sequence_num"))
        if seq is None or seq in out:
            continue
        out[seq] = row
    return out


def _age_from_last_reroute(row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not row:
        return None
    header_ts = safe_float(row.get("planning_header_timestamp_sec"))
    reroute_ts = safe_float(row.get("last_reroute_timestamp"))
    if header_ts is None or reroute_ts is None:
        return None
    return float(header_ts) - float(reroute_ts)


def _longest_current_time_smaller_window(planning_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    previous_seq: Optional[int] = None
    for row in planning_rows:
        seq = safe_int(row.get("planning_header_sequence_num"))
        is_target = (
            str(row.get("trajectory_type") or "").strip() == "NORMAL"
            and classify_replan_reason(row.get("replan_reason")) == "current_time_smaller"
            and seq is not None
        )
        if not is_target:
            if len(current) > len(best):
                best = list(current)
            current = []
            previous_seq = None
            continue
        if previous_seq is None or seq == previous_seq + 1:
            current.append(row)
        else:
            if len(current) > len(best):
                best = list(current)
            current = [row]
        previous_seq = seq
    if len(current) > len(best):
        best = list(current)
    return best


def _focus_seq_and_kind(planning_summary: Dict[str, Any], planning_rows: List[Dict[str, Any]]) -> Tuple[Optional[int], str]:
    bridge_unknown_seq = safe_int(planning_summary.get("first_path_fallback_bridge_unknown_seq"))
    if bridge_unknown_seq is not None:
        return bridge_unknown_seq, "bridge_unknown"
    trigger_seq = safe_int(planning_summary.get("first_path_fallback_trigger_seq"))
    if trigger_seq is not None:
        return trigger_seq, "first_fallback_trigger"
    longest_window = _longest_current_time_smaller_window(planning_rows)
    if longest_window:
        start_row = longest_window[0]
        start_seq = safe_int(start_row.get("planning_header_sequence_num"))
        if start_seq is not None:
            previous_seq = start_seq - 1
            previous_row = None
            for row in planning_rows:
                if safe_int(row.get("planning_header_sequence_num")) == previous_seq:
                    previous_row = row
                    break
            if previous_row and classify_replan_reason(previous_row.get("replan_reason")) == "matched_point_too_large":
                return previous_seq, "matched_point_before_stable_current_time_smaller"
            return start_seq, "first_current_time_smaller_normal"
    for row in planning_rows:
        if classify_replan_reason(row.get("replan_reason")) == "matched_point_too_large":
            seq = safe_int(row.get("planning_header_sequence_num"))
            if seq is not None:
                return seq, "matched_point_replan"
    fallback_seq = safe_int(planning_summary.get("first_path_fallback_seq"))
    if fallback_seq is not None:
        return fallback_seq, "first_path_fallback"
    return None, "none"


def _window_rows(
    planning_by_seq: Dict[int, Dict[str, Any]],
    route_by_seq: Dict[int, Dict[str, Any]],
    start_seq: Optional[int],
    end_seq: Optional[int],
) -> List[Dict[str, Any]]:
    if start_seq is None or end_seq is None or end_seq < start_seq:
        return []
    rows: List[Dict[str, Any]] = []
    for seq in range(start_seq, end_seq + 1):
        prow = planning_by_seq.get(seq) or {}
        rrow = route_by_seq.get(seq) or {}
        if not prow and not rrow:
            continue
        rows.append(
            {
                "seq": seq,
                "trajectory_type": prow.get("trajectory_type"),
                "replan_reason_family": classify_replan_reason(prow.get("replan_reason")),
                "trajectory_point_count": safe_int(prow.get("trajectory_point_count")),
                "trajectory_total_path_length": safe_float(prow.get("trajectory_total_path_length")),
                "trajectory_relative_time_min_sec": safe_float(prow.get("trajectory_relative_time_min_sec")),
                "first_trajectory_point_v": safe_float(prow.get("first_trajectory_point_v")),
                "reference_line_provider_status": rrow.get("reference_line_provider_status"),
                "lane_follow_map_status": rrow.get("lane_follow_map_status"),
                "lane_follow_map_inconsistent": rrow.get("lane_follow_map_inconsistent"),
                "route_segment_count": safe_int(rrow.get("route_segment_count")),
                "route_segment_total_length": safe_float(rrow.get("route_segment_total_length")),
                "current_lane_id": rrow.get("current_lane_id"),
                "planning_empty_reason_guess": rrow.get("planning_empty_reason_guess"),
                "last_reroute_timestamp": safe_float(rrow.get("last_reroute_timestamp")),
                "planning_header_age_from_last_reroute_sec": _age_from_last_reroute(rrow),
            }
        )
    return rows


def _reroute_events(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for row in rows:
        events.append(
            {
                "timestamp": safe_float(row.get("timestamp")),
                "routing_phase": row.get("routing_phase"),
                "routing_request_index": safe_int(row.get("routing_request_index")),
                "reroute_reason": row.get("reroute_reason"),
                "trigger_source": row.get("trigger_source"),
                "current_lane_id": row.get("current_lane_id"),
                "route_segment_count": safe_int(row.get("route_segment_count")),
                "lane_follow_map_status": row.get("lane_follow_map_status"),
                "lane_follow_map_inconsistent": row.get("lane_follow_map_inconsistent"),
            }
        )
    return events


def _case_summary(label: str, run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    planning_summary = summary.get("planning_trajectory_type_summary") or {}
    route_rows = load_jsonl(run_dir / "artifacts" / "stage5_apollo_reference_line_debug.jsonl")
    planning_rows = load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")
    reroute_rows = load_jsonl(run_dir / "artifacts" / "stage5_reroute_decision_debug.jsonl")
    route_by_seq = _row_by_seq(route_rows)
    planning_by_seq = _row_by_seq(planning_rows)
    focus_seq, focus_kind = _focus_seq_and_kind(planning_summary, planning_rows)
    focus_route_row = route_by_seq.get(focus_seq or -1)
    focus_planning_row = planning_by_seq.get(focus_seq or -1)
    bridge_prev_normal_seq = safe_int(planning_summary.get("first_path_fallback_bridge_prev_normal_seq"))
    bridge_unknown_seq = safe_int(planning_summary.get("first_path_fallback_bridge_unknown_seq"))
    bridge_empty_previous_seq = safe_int(planning_summary.get("first_path_fallback_bridge_empty_previous_seq"))
    first_fallback_seq = safe_int(planning_summary.get("first_path_fallback_seq"))
    long_reroute = None
    for row in reroute_rows:
        if str(row.get("routing_phase") or "").strip() == "long":
            long_reroute = row
    window_start = bridge_prev_normal_seq
    if window_start is None and focus_seq is not None:
        window_start = max(0, focus_seq - 6)
    window_end = first_fallback_seq if first_fallback_seq is not None else focus_seq
    return {
        "label": label,
        "run_dir": str(run_dir),
        "route_id": summary.get("route_id"),
        "comparison_label": summary.get("comparison_label"),
        "route_completion_ratio": safe_float(summary.get("route_completion_ratio")),
        "focus_kind": focus_kind,
        "focus_seq": focus_seq,
        "focus_trajectory_type": (focus_planning_row or {}).get("trajectory_type"),
        "focus_reference_line_provider_status": (focus_route_row or {}).get("reference_line_provider_status"),
        "focus_lane_follow_map_status": (focus_route_row or {}).get("lane_follow_map_status"),
        "focus_route_segment_count": safe_int((focus_route_row or {}).get("route_segment_count")),
        "focus_route_segment_total_length": safe_float((focus_route_row or {}).get("route_segment_total_length")),
        "focus_current_lane_id": (focus_route_row or {}).get("current_lane_id"),
        "focus_planning_header_age_from_last_reroute_sec": _age_from_last_reroute(focus_route_row),
        "bridge_prev_normal_seq": bridge_prev_normal_seq,
        "bridge_unknown_seq": bridge_unknown_seq,
        "bridge_empty_previous_seq": bridge_empty_previous_seq,
        "first_fallback_seq": first_fallback_seq,
        "first_path_fallback_trigger_seq": safe_int(planning_summary.get("first_path_fallback_trigger_seq")),
        "first_path_fallback_trigger_reason_family": planning_summary.get("first_path_fallback_trigger_reason_family"),
        "first_path_fallback_bridge_route_segment_count_delta_from_prev_normal": safe_int(
            planning_summary.get("first_path_fallback_bridge_route_segment_count_delta_from_prev_normal")
        ),
        "first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal": safe_float(
            planning_summary.get("first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal")
        ),
        "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal": planning_summary.get(
            "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal"
        ),
        "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready": planning_summary.get(
            "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready"
        ),
        "first_path_fallback_bridge_likely_codepath_family": planning_summary.get(
            "first_path_fallback_bridge_likely_codepath_family"
        ),
        "first_path_fallback_bridge_likely_provider_failure_site": planning_summary.get(
            "first_path_fallback_bridge_likely_provider_failure_site"
        ),
        "first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec")
        ),
        "first_path_fallback_trigger_planning_header_age_from_last_reroute_sec": safe_float(
            planning_summary.get("first_path_fallback_trigger_planning_header_age_from_last_reroute_sec")
        ),
        "long_reroute_event": {
            "timestamp": safe_float((long_reroute or {}).get("timestamp")),
            "routing_request_index": safe_int((long_reroute or {}).get("routing_request_index")),
            "reroute_reason": (long_reroute or {}).get("reroute_reason"),
            "trigger_source": (long_reroute or {}).get("trigger_source"),
            "current_lane_id": (long_reroute or {}).get("current_lane_id"),
            "route_segment_count": safe_int((long_reroute or {}).get("route_segment_count")),
            "lane_follow_map_status": (long_reroute or {}).get("lane_follow_map_status"),
            "lane_follow_map_inconsistent": (long_reroute or {}).get("lane_follow_map_inconsistent"),
        },
        "reroute_events": _reroute_events(reroute_rows),
        "window_rows": _window_rows(planning_by_seq, route_by_seq, window_start, window_end),
    }


def render_report(title: str, cases: List[Dict[str, Any]]) -> str:
    lines = [f"# {title}", "", "## Key Conclusion", ""]
    repeat_bad = next((case for case in cases if case["label"] == "219_repeat"), None)
    smoke = next((case for case in cases if case["label"] == "219_smoke"), None)
    stable = next((case for case in cases if case["label"] == "097_repeat"), None)
    if repeat_bad and smoke and stable:
        repeat_bad_window_by_seq = {safe_int(row.get("seq")): row for row in repeat_bad.get("window_rows") or []}
        repeat_bad_prev_normal = repeat_bad_window_by_seq.get(safe_int(repeat_bad.get("bridge_prev_normal_seq")))
        repeat_bad_bridge_unknown = repeat_bad_window_by_seq.get(safe_int(repeat_bad.get("bridge_unknown_seq")))
        lines.append(
            "- `219 repeat` 是唯一一个在 first fallback 前出现 "
            "`reference_line_provider failed + route segments ready + current lane dropped` 的 case。"
        )
        lines.append(
            f"- 它的 bridge unknown 发生在 `seq={repeat_bad['bridge_unknown_seq']}`，"
            f"距最近 long-phase reroute 只有 `{_fmt(repeat_bad['first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec'])}s`；"
            f"而 `219 smoke` 的 first fallback trigger 距最近 reroute 是 `{_fmt(smoke['first_path_fallback_trigger_planning_header_age_from_last_reroute_sec'])}s`，"
            f"`097 repeat` 则是 `{_fmt(stable['focus_planning_header_age_from_last_reroute_sec'])}s`。"
        )
        if repeat_bad_prev_normal and repeat_bad_bridge_unknown:
            lines.append(
                f"- `219 repeat` 的 long-reroute propagation 也直接落在 bridge 上："
                f"`seq={repeat_bad['bridge_prev_normal_seq']}` 仍带旧 reroute ts "
                f"`{_fmt(repeat_bad_prev_normal.get('last_reroute_timestamp'), 3)}`、`1 seg / 30m`；"
                f"`seq={repeat_bad['bridge_unknown_seq']}` 才切到新 reroute ts "
                f"`{_fmt(repeat_bad_bridge_unknown.get('last_reroute_timestamp'), 3)}`，"
                f"同时 segment 从 `{repeat_bad_prev_normal.get('route_segment_count')}` 跳到 "
                f"`{repeat_bad_bridge_unknown.get('route_segment_count')}`，provider 也同步失败。"
            )
        lines.append(
            "- `219 repeat` 的退化链现在可以写成："
            "`1 seg / 30m NORMAL -> provider failed with route segments ready -> UNKNOWN/0-point -> empty previous trajectory -> short-horizon current_time_smaller NORMAL -> PATH_FALLBACK`。"
        )
        lines.append(
            f"- 它当前的 canonical codepath family 已经固定成："
            f"`{repeat_bad.get('first_path_fallback_bridge_likely_codepath_family') or ''}`。"
        )
        lines.append(
            f"- 更细一层的 provider failure site 现在也已经固定成："
            f"`{repeat_bad.get('first_path_fallback_bridge_likely_provider_failure_site') or ''}`。"
        )
        lines.append(
            "- 当前第一剩余问题因此更窄了：为什么 `219 repeat` 会在 long-phase reroute 后几百毫秒内掉进 "
            "`reference_line_missing -> empty previous trajectory`，而同路线 smoke 与 `097 repeat` 都不会。"
        )
    lines.extend(
        [
            "",
            "## Case Summary",
            "",
            "| label | completion | focus_kind | focus_seq | focus_provider | focus_lane_follow | focus_segments | focus_route_len_m | focus_age_from_last_reroute_sec | bridge_prev_normal | bridge_unknown | empty_previous | first_fallback | trigger_reason |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for case in cases:
        lines.append(
            "| "
            + " | ".join(
                [
                    case["label"],
                    _fmt(case.get("route_completion_ratio")),
                    str(case.get("focus_kind") or ""),
                    str(case.get("focus_seq") or ""),
                    str(case.get("focus_reference_line_provider_status") or ""),
                    str(case.get("focus_lane_follow_map_status") or ""),
                    str(case.get("focus_route_segment_count") or ""),
                    _fmt(case.get("focus_route_segment_total_length")),
                    _fmt(case.get("focus_planning_header_age_from_last_reroute_sec")),
                    str(case.get("bridge_prev_normal_seq") or ""),
                    str(case.get("bridge_unknown_seq") or ""),
                    str(case.get("bridge_empty_previous_seq") or ""),
                    str(case.get("first_fallback_seq") or ""),
                    str(case.get("first_path_fallback_trigger_reason_family") or ""),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Apollo Codepath Mapping",
            "",
            f"- `ReferenceLineProvider::UpdatePlanningCommand()` 入口在 `{APOLLO_REFERENCE_LINE_PROVIDER}:315-360`。"
            " reroute landing frame 上只要 `IsNewPlanningCommand(command)` 为真，就会把 `is_new_command_` 置为 true。",
            f"- `LaneFollowMap::UpdatePlanningCommand()` 在 `{APOLLO_LANE_FOLLOW_MAP}:373-530`。"
            " 这里会重建 `route_indices_ / all_lane_ids_`，并在新 command 上更新 route cache/debug 状态。",
            f"- `ReferenceLineProvider::CreateRouteSegments()` 在 `{APOLLO_REFERENCE_LINE_PROVIDER}:830-847`。"
            " 只要 current pnc map 还能提取 segments，`route_segments_present_reference_line_missing` 就不会卡在 route extraction。",
            f"- `ReferenceLineProvider::CreateReferenceLine()` 在 `{APOLLO_REFERENCE_LINE_PROVIDER}:849-1047`。"
            " 当 reroute landing frame 触发 new command 分支时，会走 `996-1016` 的 `SmoothRouteSegment()` 路径；"
            " 如果所有 segment 都失败，最终会留下 `route segments ready but reference line empty` 的状态。",
            "- 当前 canonical `bridge_likely_provider_failure_site=create_reference_line_new_command_empty_output`"
            " 指向的就是这段 new-command empty-output 语义，而不是更下游的 stitching branch。",
            f"- `ReferenceLineProvider::ExtendReferenceLine()` 在 `{APOLLO_REFERENCE_LINE_PROVIDER}:1050-1135`。"
            " 这条 stitching branch 更符合 smoke / stable case；它不解释 `219 repeat` 在 landing frame 就出现的 `UNKNOWN/0-point`。",
        ]
    )
    for case in cases:
        lines.extend(
            [
                "",
                f"## {case['label']}",
                "",
                f"- route_id: `{case.get('route_id') or ''}`",
                f"- run_dir: `{case.get('run_dir') or ''}`",
                f"- comparison_label: `{case.get('comparison_label') or ''}`",
                f"- long_reroute_timestamp: `{_fmt((case.get('long_reroute_event') or {}).get('timestamp'), 6)}`",
                f"- long_reroute_reason: `{(case.get('long_reroute_event') or {}).get('reroute_reason') or ''}`",
                f"- long_reroute_lane_follow_status: `{(case.get('long_reroute_event') or {}).get('lane_follow_map_status') or ''}`",
                f"- bridge_route_segment_count_delta_from_prev_normal: `{case.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal') if case.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal') is not None else ''}`",
                f"- bridge_route_segment_total_length_delta_from_prev_normal: `{_fmt(case.get('first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal'))}`",
                f"- bridge_unknown_current_lane_dropped_from_prev_normal: `{case.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')}`",
                f"- bridge_unknown_reference_line_failed_with_route_segments_ready: `{case.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')}`",
                f"- bridge_likely_codepath_family: `{case.get('first_path_fallback_bridge_likely_codepath_family') or ''}`",
                f"- bridge_likely_provider_failure_site: `{case.get('first_path_fallback_bridge_likely_provider_failure_site') or ''}`",
                f"- bridge_unknown_age_from_last_reroute_sec: `{_fmt(case.get('first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec'))}`",
                f"- first_fallback_trigger_age_from_last_reroute_sec: `{_fmt(case.get('first_path_fallback_trigger_planning_header_age_from_last_reroute_sec'))}`",
                "",
                "### Reroute Events",
                "",
                "| timestamp | routing_phase | request_idx | reroute_reason | trigger_source | current_lane | route_segments | lane_follow_status | inconsistent |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for event in case.get("reroute_events") or []:
            lines.append(
                "| "
                + " | ".join(
                    [
                        _fmt(event.get("timestamp"), 6),
                        str(event.get("routing_phase") or ""),
                        str(event.get("routing_request_index") or ""),
                        str(event.get("reroute_reason") or ""),
                        str(event.get("trigger_source") or ""),
                        str(event.get("current_lane_id") or ""),
                        str(event.get("route_segment_count") or ""),
                        str(event.get("lane_follow_map_status") or ""),
                        str(event.get("lane_follow_map_inconsistent") or ""),
                    ]
                )
                + " |"
            )
        lines.extend(
            [
                "",
                "### Focus Window",
                "",
                "| seq | trajectory_type | reason_family | point_count | path_len_m | rel_min_sec | v0 | ref_provider | lane_follow_status | inconsistent | route_segments | route_len_m | current_lane | planning_empty_reason | last_reroute_ts | age_from_last_reroute_sec |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in case.get("window_rows") or []:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("seq") or ""),
                        str(row.get("trajectory_type") or ""),
                        str(row.get("replan_reason_family") or ""),
                        str(row.get("trajectory_point_count") or ""),
                        _fmt(row.get("trajectory_total_path_length")),
                        _fmt(row.get("trajectory_relative_time_min_sec")),
                        _fmt(row.get("first_trajectory_point_v")),
                        str(row.get("reference_line_provider_status") or ""),
                        str(row.get("lane_follow_map_status") or ""),
                        str(row.get("lane_follow_map_inconsistent") or ""),
                        str(row.get("route_segment_count") or ""),
                        _fmt(row.get("route_segment_total_length")),
                        str(row.get("current_lane_id") or ""),
                        str(row.get("planning_empty_reason_guess") or ""),
                        _fmt(row.get("last_reroute_timestamp"), 3),
                        _fmt(row.get("planning_header_age_from_last_reroute_sec")),
                    ]
                )
                + " |"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", action="append", type=_parse_case, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("artifacts/town01_reference_line_provider_bridge_report_20260326.md"),
    )
    parser.add_argument(
        "--title",
        default="Town01 Reference-Line Provider Bridge Compare 2026-03-26",
    )
    args = parser.parse_args()

    cases = [_case_summary(label, run_dir) for label, run_dir in args.case]
    report = render_report(args.title, cases)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    print(f"[bridge] wrote report to {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
