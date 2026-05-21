#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from analyze_stage3_planning_control_rootcause import (
    _load_jsonl,
    _parse_control_no_trajectory_log,
    _safe_float,
    _write_csv,
    _write_jsonl,
    analyze_run,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS = REPO_ROOT / "artifacts"
APOLLO_SRC_ROOT = Path(
    "/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src"
).resolve()
CASE_LABELS = {
    "case_a_profile_a_locked": "Case A",
    "case_b_profile_b_locked": "Case B",
    "case_b_freeze_profile_b_locked": "Case B-freeze",
    "case_b_tightened_profile_b_locked": "Case B-tightened",
    "case_b_reflinefix_profile_b_locked": "Case B-reflinefix",
}
CASE_ORDER = {name: idx for idx, name in enumerate(CASE_LABELS, start=1)}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _copy_text(name: str, text: str, batch_root: Path) -> None:
    _write_text(batch_root / "artifacts" / name, text)
    _write_text(ROOT_ARTIFACTS / name, text)


def _copy_csv(name: str, rows: List[Dict[str, Any]], batch_root: Path) -> None:
    _write_csv(batch_root / "artifacts" / name, rows)
    _write_csv(ROOT_ARTIFACTS / name, rows)


def _copy_jsonl(name: str, rows: Iterable[Dict[str, Any]], batch_root: Path) -> None:
    _write_jsonl(batch_root / "artifacts" / name, rows)
    _write_jsonl(ROOT_ARTIFACTS / name, rows)


def _discover_completed_run_dirs(batch_root: Path) -> List[Path]:
    discovered: Dict[str, Path] = {}
    for child in sorted(batch_root.iterdir()):
        if child.name == "artifacts" or child.is_symlink() or not child.is_dir():
            continue
        if (child / "RUN_DIR_REDIRECT.txt").exists():
            continue
        if not (child / "summary.json").exists():
            continue
        canonical = child.name.split("__")[0]
        current = discovered.get(canonical)
        if current is None or child.name > current.name:
            discovered[canonical] = child.resolve()
    return list(discovered.values())


def _nearest_prior(rows: List[Dict[str, Any]], ts: float, window_sec: float) -> Optional[Dict[str, Any]]:
    best = None
    best_dt = None
    for row in rows:
        row_ts = _safe_float(row.get("timestamp"))
        if row_ts is None or row_ts > ts:
            continue
        dt = ts - row_ts
        if dt > window_sec:
            continue
        if best is None or dt < float(best_dt):
            best = row
            best_dt = dt
    return best


def _nearest_future(rows: List[Dict[str, Any]], ts: float, window_sec: float) -> Optional[Dict[str, Any]]:
    best = None
    best_dt = None
    for row in rows:
        row_ts = _safe_float(row.get("timestamp"))
        if row_ts is None or row_ts < ts:
            continue
        dt = row_ts - ts
        if dt > window_sec:
            continue
        if best is None or dt < float(best_dt):
            best = row
            best_dt = dt
    return best


def _refline_static_analysis_md() -> str:
    rows = [
        {
            "function_name": "ReferenceLineProvider::GetReferenceLines",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/planning_base/reference_line/reference_line_provider.cc"),
            "responsibility": "Return current cached/history reference lines to planning.",
            "key_inputs": "reference_lines_, route_segments_, reference_line_history_",
            "key_outputs": "reference_lines list, route_segments list, bool ready",
            "cache_dependency": "reference_lines_ / route_segments_ / history queues",
            "possible_empty_reference_line_exit": "cached reference_lines_ empty and history empty -> return false with 'Reference line is NOT ready.'",
            "related_logs": "Reference line is NOT ready.; Failed to use reference line latest history",
            "suspected_failure_mode": "CreateReferenceLine produced no reference_lines so thread/cache remains empty or stale.",
        },
        {
            "function_name": "ReferenceLineProvider::CreateRouteSegments",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/planning_base/reference_line/reference_line_provider.cc"),
            "responsibility": "Ask current_pnc_map for route segments.",
            "key_inputs": "vehicle_state, current_pnc_map_",
            "key_outputs": "segments list",
            "cache_dependency": "depends on pnc_map internal route state",
            "possible_empty_reference_line_exit": "GetRouteSegments fails or returns empty",
            "related_logs": "Failed to extract segments from routing",
            "suspected_failure_mode": "Upstream pnc_map failure; not the dominant stage5/stage6 mode.",
        },
        {
            "function_name": "ReferenceLineProvider::CreateReferenceLine",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/planning_base/reference_line/reference_line_provider.cc"),
            "responsibility": "Convert route segments into final smoothed or stitched reference lines.",
            "key_inputs": "segments from CreateRouteSegments, vehicle_state, is_new_command_, cached route/reference lines",
            "key_outputs": "reference_lines list, pruned segments list",
            "cache_dependency": "stitching path depends on route_segments_ and reference_lines_ caches",
            "possible_empty_reference_line_exit": "SmoothRouteSegment/ExtendReferenceLine fails for every segment, segment gets erased, output list stays empty",
            "related_logs": "Failed to create reference line from route segments; Failed to extend reference line",
            "suspected_failure_mode": "Most likely direct path for route_segment_present_but_reference_line_missing.",
        },
        {
            "function_name": "ReferenceLineProvider::ExtendReferenceLine",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/planning_base/reference_line/reference_line_provider.cc"),
            "responsibility": "Stitch previous reference line with shifted future segments.",
            "key_inputs": "previous cached route/refline, current segments, vehicle_state",
            "key_outputs": "stitched reference line or fallback smoothing",
            "cache_dependency": "route_segments_, reference_lines_",
            "possible_empty_reference_line_exit": "prev segment not connected, projection fail, shift fail, stitch fail, smoothing fallback fail",
            "related_logs": "Failed to shift route segments forward; Failed to stitch reference line; Failed to stitch route segments",
            "suspected_failure_mode": "Cache/history desync can force repeated fallback and eventual empty output.",
        },
        {
            "function_name": "LaneFollowMap::UpdatePlanningCommand",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/pnc_map/lane_follow_map/lane_follow_map.cc"),
            "responsibility": "Replace routing command and rebuild route_indices_/all_lane_ids_.",
            "key_inputs": "PlanningCommand lane_follow_command",
            "key_outputs": "route_indices_, all_lane_ids_, routing_waypoint_index_",
            "cache_dependency": "route_segments_lane_ids_ should be coherent with new route",
            "possible_empty_reference_line_exit": "stale route_segments_lane_ids_ survives reroute and filters future nearest-lane lookup",
            "related_logs": "none by default; stage6 cache debug now records stale_cache_after_reroute",
            "suspected_failure_mode": "Concrete stale-cache bug point; old route_segments_lane_ids_ was not cleared on new command.",
        },
        {
            "function_name": "LaneFollowMap::GetNearestPointFromRouting",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/pnc_map/lane_follow_map/lane_follow_map.cc"),
            "responsibility": "Project ego onto valid route lanes.",
            "key_inputs": "all_lane_ids_, range_lane_ids_, route_segments_lane_ids_, vehicle_state",
            "key_outputs": "adc waypoint on route",
            "cache_dependency": "route_segments_lane_ids_ prefilters candidate lanes",
            "possible_empty_reference_line_exit": "valid lanes all filtered by stale route_segments_lane_ids_, then UpdateVehicleState/GetRouteSegments fail",
            "related_logs": "not in last frame route_segments; Failed to find nearest point",
            "suspected_failure_mode": "Key lane_follow_map/cache inconsistency gate.",
        },
        {
            "function_name": "LaneFollowMap::GetRouteSegments",
            "file_path": str(APOLLO_SRC_ROOT / "modules/planning/pnc_map/lane_follow_map/lane_follow_map.cc"),
            "responsibility": "Build drive passages and extend them into route segments.",
            "key_inputs": "updated adc waypoint, route_indices_, routing passages",
            "key_outputs": "route_segments list",
            "cache_dependency": "calls UpdateVehicleState then updates route_segments_lane_ids_",
            "possible_empty_reference_line_exit": "UpdateVehicleState fails before segment extraction",
            "related_logs": "Failed to update vehicle state in pnc_map.",
            "suspected_failure_mode": "Secondary; can be induced by stale cache after reroute.",
        },
    ]
    lines = [
        "# Stage6 ReferenceLineProvider Static Analysis",
        "",
        "| function_name | file_path | responsibility | key_inputs | key_outputs | cache_dependency | possible_empty_reference_line_exit | related_logs | suspected_failure_mode |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['function_name']} | {row['file_path']} | {row['responsibility']} | "
            f"{row['key_inputs']} | {row['key_outputs']} | {row['cache_dependency']} | "
            f"{row['possible_empty_reference_line_exit']} | {row['related_logs']} | {row['suspected_failure_mode']} |"
        )
    lines.extend(
        [
            "",
            "## Most Likely `route_segment_present_but_reference_line_missing` Path",
            "",
            "1. `LaneFollowMap::UpdatePlanningCommand()` accepts a new routing command while old `route_segments_lane_ids_` cache may still be present.",
            "2. `LaneFollowMap::GetNearestPointFromRouting()` uses that cache to pre-filter current candidate lanes.",
            "3. `ReferenceLineProvider::CreateRouteSegments()` still returns route segments in some cycles.",
            "4. `ReferenceLineProvider::CreateReferenceLine()` enters the smoothing/stitching loop.",
            "5. `SmoothRouteSegment()` or `ExtendReferenceLine()` fails for every candidate segment, so the loop erases all segments and leaves `reference_lines` empty.",
            "6. `GetReferenceLines()` later observes no ready reference line and falls back to history or returns not-ready.",
        ]
    )
    return "\n".join(lines) + "\n"


def _classify_provider_row(row: Dict[str, Any], reroute_rows: List[Dict[str, Any]], cache_rows: List[Dict[str, Any]], prev_row: Optional[Dict[str, Any]]) -> str:
    failure_reason = str(row.get("failure_exit_reason") or "")
    if bool(row.get("map_contract_invalid")):
        return "map_context_mismatch"
    if str(row.get("create_route_segments_result_status") or "") != "ok":
        return "create_route_segments_failed"
    ts = _safe_float(row.get("timestamp")) or 0.0
    prior_reroute = _nearest_prior(reroute_rows, ts, 1.0)
    if prior_reroute is not None:
        return "reroute_after_unstable_reference_line"
    prior_cache = _nearest_prior(cache_rows, ts, 0.3)
    if prior_cache is not None and not bool(prior_cache.get("cache_consistent_with_current_route", True)):
        return "cache_inconsistent"
    if failure_reason in {"cache_inconsistent", "lane_follow_map_inconsistent"}:
        return failure_reason
    if (
        int(row.get("route_segment_count_input", 0) or 0) > 0
        and int(row.get("reference_line_generated_count", 0) or 0) <= 0
    ):
        if prev_row and str(prev_row.get("current_lane_id") or "") and str(row.get("current_lane_id") or ""):
            if str(prev_row.get("current_lane_id")) != str(row.get("current_lane_id")):
                return "transient_lane_id_jump"
        return "route_segment_present_but_reference_line_missing"
    if failure_reason in {"reference_line_provider_internal_failure", "empty_output_after_generation"}:
        return "reference_line_provider_failed"
    return "unknown"


def _timeline_class(
    row: Dict[str, Any],
    reroute_rows: List[Dict[str, Any]],
    inconsistent_rows: List[Dict[str, Any]],
    cache_rows: List[Dict[str, Any]],
) -> str:
    ts = _safe_float(row.get("timestamp")) or 0.0
    if _nearest_prior(reroute_rows, ts, 0.6) is not None:
        return "happens_right_after_reroute"
    prior_cache = _nearest_prior(cache_rows, ts, 0.3)
    if prior_cache is not None and not bool(prior_cache.get("cache_consistent_with_current_route", True)):
        return "happens_with_cache_mismatch"
    if _nearest_prior(inconsistent_rows, ts, 0.3) is not None:
        return "happens_after_lane_follow_map_inconsistent"
    if _nearest_future(inconsistent_rows, ts, 0.3) is not None:
        return "happens_before_lane_follow_map_inconsistent"
    if (
        _nearest_prior(reroute_rows, ts, 0.3) is None
        and prior_cache is None
        and _nearest_prior(inconsistent_rows, ts, 0.3) is None
    ):
        return "happens_without_any_clear_precursor"
    return "unknown"


def _summarize_case(run_dir: Path) -> Dict[str, Any]:
    base = analyze_run(run_dir)
    artifacts = run_dir / "artifacts"
    case_name = base["case_name"]
    case_label = CASE_LABELS.get(case_name, case_name)

    provider_rows = _load_jsonl(artifacts / "stage6_reference_line_provider_debug.jsonl")
    cache_rows = _load_jsonl(artifacts / "stage6_lane_follow_map_cache_debug.jsonl")
    event_rows = _load_jsonl(artifacts / "stage6_route_segment_present_refline_missing_events.jsonl")
    reroute_rows = _load_jsonl(artifacts / "stage5_reroute_decision_debug.jsonl")
    if not reroute_rows:
        reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    control_miss_rows = _parse_control_no_trajectory_log(artifacts / "apollo_control.INFO")

    provider_missing_rows = [
        row for row in provider_rows if int(row.get("reference_line_generated_count", 0) or 0) <= 0
    ]
    route_gap_rows = [
        row
        for row in provider_rows
        if bool(row.get("route_segment_present_but_reference_line_missing"))
        or (
            int(row.get("route_segment_count_input", 0) or 0) > 0
            and int(row.get("reference_line_generated_count", 0) or 0) <= 0
        )
    ]
    inconsistent_rows = [
        row
        for row in provider_rows
        if str(row.get("failure_exit_reason") or "") in {"lane_follow_map_inconsistent", "cache_inconsistent"}
    ]

    rootcause_counts: Counter[str] = Counter()
    prev = None
    for row in route_gap_rows:
        rootcause_counts[_classify_provider_row(row, reroute_rows, cache_rows, prev)] += 1
        prev = row

    timeline_rows: List[Dict[str, Any]] = []
    for row in reroute_rows:
        timeline_rows.append(
            {
                "timestamp": row.get("timestamp"),
                "event_type": "reroute",
                "case": case_label,
                "detail": row.get("reroute_reason"),
                "timeline_class": "",
            }
        )
    for row in inconsistent_rows:
        timeline_rows.append(
            {
                "timestamp": row.get("timestamp"),
                "event_type": "lane_follow_map_inconsistent",
                "case": case_label,
                "detail": row.get("failure_exit_reason"),
                "timeline_class": "",
            }
        )
    for row in provider_missing_rows:
        timeline_rows.append(
            {
                "timestamp": row.get("timestamp"),
                "event_type": "reference_line_missing",
                "case": case_label,
                "detail": row.get("failure_exit_reason"),
                "timeline_class": "",
            }
        )

    enriched_event_rows: List[Dict[str, Any]] = []
    timeline_class_counts: Counter[str] = Counter()
    for row in event_rows:
        ts = _safe_float(row.get("timestamp")) or 0.0
        nearby_control = _nearest_prior(control_miss_rows, ts, 0.3) is not None
        enriched = dict(row)
        enriched["control_no_trajectory_nearby"] = nearby_control
        timeline_class = _timeline_class(enriched, reroute_rows, inconsistent_rows, cache_rows)
        enriched["timeline_class"] = timeline_class
        timeline_class_counts[timeline_class] += 1
        enriched_event_rows.append(enriched)
        timeline_rows.append(
            {
                "timestamp": row.get("timestamp"),
                "event_type": "route_segment_present_refline_missing",
                "case": case_label,
                "detail": row.get("failure_exit_reason"),
                "timeline_class": timeline_class,
            }
        )
    for row in control_miss_rows:
        timeline_rows.append(
            {
                "timestamp": row.get("timestamp"),
                "event_type": "control_no_trajectory",
                "case": case_label,
                "detail": "planning has no trajectory point",
                "timeline_class": "",
            }
        )
    timeline_rows.sort(key=lambda item: (_safe_float(item.get("timestamp")) or 0.0, str(item.get("event_type"))))

    result = {
        **base,
        "case_label": case_label,
        "case_sort": CASE_ORDER.get(case_name, 999),
        "route_segment_present_but_reference_line_missing_count": len(route_gap_rows),
        "reference_line_missing_count": len(provider_missing_rows),
        "lane_follow_map_inconsistent_count": len(inconsistent_rows),
        "create_route_segments_failed_count": sum(
            1 for row in provider_rows if str(row.get("create_route_segments_result_status") or "") != "ok"
        ),
        "cache_inconsistent_count": sum(
            1
            for row in cache_rows
            if not bool(row.get("cache_consistent_with_current_route", True))
            or str(row.get("inconsistency_reason") or "") == "stale_cache_after_reroute"
        ),
        "refline_rootcause_counts": dict(rootcause_counts),
        "refline_rootcause_top1": rootcause_counts.most_common(1)[0][0] if rootcause_counts else "no_failure",
        "timeline_class_counts": dict(timeline_class_counts),
        "timeline_class_top1": timeline_class_counts.most_common(1)[0][0] if timeline_class_counts else "no_event",
        "provider_rows": provider_rows,
        "cache_rows": cache_rows,
        "event_rows": enriched_event_rows,
        "timeline_rows": timeline_rows,
        "reroute_reason_counts": dict(
            Counter(str(row.get("reroute_reason") or "") for row in reroute_rows if str(row.get("reroute_reason") or ""))
        ),
    }
    _write_jsonl(artifacts / "stage6_route_segment_present_refline_missing_events.jsonl", enriched_event_rows)
    return result


def _case_comparison_rows(case_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in sorted(case_results, key=lambda row: row["case_sort"]):
        rows.append(
            {
                "case": item["case_label"],
                "route_segment_present_but_reference_line_missing_count": item[
                    "route_segment_present_but_reference_line_missing_count"
                ],
                "reference_line_missing_count": item["reference_line_missing_count"],
                "lane_follow_map_inconsistent_count": item["lane_follow_map_inconsistent_count"],
                "create_route_segments_failed_count": item["create_route_segments_failed_count"],
                "cache_inconsistent_count": item["cache_inconsistent_count"],
                "count(planning has no trajectory point)": item["count_planning_has_no_trajectory_point"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "stack_healthy": item["stack_healthy"],
                "final_outcome_label": item["final_outcome_label"],
                "refline_rootcause_top1": item["refline_rootcause_top1"],
                "timeline_class_top1": item["timeline_class_top1"],
            }
        )
    return rows


def _timeline_report_md(case_results: List[Dict[str, Any]]) -> str:
    lines = ["# Stage6 Reference Line Timeline Report", ""]
    for item in sorted(case_results, key=lambda row: row["case_sort"]):
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- route_segment_present_but_reference_line_missing_count: `{item['route_segment_present_but_reference_line_missing_count']}`",
                f"- timeline_class_counts: `{json.dumps(item['timeline_class_counts'], ensure_ascii=False, sort_keys=True)}`",
                "",
            ]
        )
    totals = Counter()
    for item in case_results:
        totals.update(item["timeline_class_counts"])
    lines.extend(
        [
            "## Answers",
            "",
            f"- 主因排序：`{json.dumps(dict(totals), ensure_ascii=False, sort_keys=True)}`",
            "- `happens_right_after_reroute` 高，说明 reroute 仍在给后续 inconsistent 提供触发条件。",
            "- `happens_with_cache_mismatch` 高，说明 lane_follow_map / route segment cache 脱节是更直接的内部原因。",
            "- 若大量事件落到 `happens_without_any_clear_precursor`，则需要继续向 `ReferenceLineProvider::ExtendReferenceLine` / `SmoothRouteSegment` 内部打点。",
        ]
    )
    return "\n".join(lines) + "\n"


def _final_report_md(case_results: List[Dict[str, Any]]) -> str:
    totals = Counter()
    timeline_totals = Counter()
    for item in case_results:
        totals.update(item["refline_rootcause_counts"])
        timeline_totals.update(item["timeline_class_counts"])
    case_b_tightened = next((item for item in case_results if item["case_name"] == "case_b_tightened_profile_b_locked"), None)
    case_b_reflinefix = next((item for item in case_results if item["case_name"] == "case_b_reflinefix_profile_b_locked"), None)
    lines = [
        "# Stage6 Reference Line Provider Rootcause Report",
        "",
        "## Executive Summary",
        "",
        f"- `route_segment_present_but_reference_line_missing` 主因排序：`{json.dumps(dict(totals), ensure_ascii=False, sort_keys=True)}`。",
        f"- 最可疑的具体函数/分支：`ReferenceLineProvider::CreateReferenceLine()` 里对 `SmoothRouteSegment()/ExtendReferenceLine()` 失败后的 erase 分支，以及 `LaneFollowMap::UpdatePlanningCommand()` 未及时处理旧 `route_segments_lane_ids_` 的缓存切换点。",
        f"- lane_follow_map 与 cache 是否一致：时间线上 `timeline_class` 排序为 `{json.dumps(dict(timeline_totals), ensure_ascii=False, sort_keys=True)}`，若 `happens_with_cache_mismatch` 居高，说明不一致确实存在。",
        f"- reroute 后 stale cache 是否主导问题：`{('yes' if timeline_totals.get('happens_right_after_reroute', 0) + timeline_totals.get('happens_with_cache_mismatch', 0) > 0 else 'unclear')}`。",
    ]
    if case_b_tightened and case_b_reflinefix:
        lines.extend(
            [
                f"- 最小修复对照：Case B-tightened 的 route-gap=`{case_b_tightened['route_segment_present_but_reference_line_missing_count']}` / no-trajectory=`{case_b_tightened['count_planning_has_no_trajectory_point']}`；"
                f"Case B-reflinefix 的 route-gap=`{case_b_reflinefix['route_segment_present_but_reference_line_missing_count']}` / no-trajectory=`{case_b_reflinefix['count_planning_has_no_trajectory_point']}`。",
                f"- 最小修复是否有正向效果：`{('yes' if case_b_reflinefix['route_segment_present_but_reference_line_missing_count'] <= case_b_tightened['route_segment_present_but_reference_line_missing_count'] and case_b_reflinefix['success'] else 'mixed_or_no')}`。",
            ]
        )
    lines.extend(
        [
            "",
            "## Per Case",
            "",
        ]
    )
    for item in sorted(case_results, key=lambda row: row["case_sort"]):
        lines.append(
            f"- {item['case_label']}: route-gap=`{item['route_segment_present_but_reference_line_missing_count']}`, "
            f"reference_line_missing=`{item['reference_line_missing_count']}`, "
            f"lane_follow_map_inconsistent=`{item['lane_follow_map_inconsistent_count']}`, "
            f"cache_inconsistent=`{item['cache_inconsistent_count']}`, "
            f"no_trajectory=`{item['count_planning_has_no_trajectory_point']}`, "
            f"top1=`{item['refline_rootcause_top1']}`, success=`{item['success']}`."
        )
    lines.extend(
        [
            "",
            "## Conclusions",
            "",
            "- 当前 bridge 已经足够收敛，后续不应再作为主战场。",
            "- 如果这一轮最小修复已经带来下降，下一阶段最应该继续打的最小点是 `LaneFollowMap::UpdatePlanningCommand()` 与 `GetNearestPointFromRouting()` 之间的 stale `route_segments_lane_ids_` 生命周期。",
            "- 如果 route-gap 仍主要落在 `reference_line_provider_failed`，下一刀应进入 `ReferenceLineProvider::CreateReferenceLine()` 的 `SmoothRouteSegment/ExtendReferenceLine` 失败原因，而不是继续扩 reroute 或 startup geometry。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    run_dirs = _discover_completed_run_dirs(batch_root)
    case_results = [_summarize_case(path) for path in run_dirs]
    case_results.sort(key=lambda row: row["case_sort"])

    timeline_rows: List[Dict[str, Any]] = []
    for item in case_results:
        timeline_rows.extend(item["timeline_rows"])

    _copy_text(
        "stage6_reference_line_provider_static_analysis.md",
        _refline_static_analysis_md(),
        batch_root,
    )
    _copy_csv("stage6_reference_line_timeline_alignment.csv", timeline_rows, batch_root)
    _copy_text("stage6_reference_line_timeline_report.md", _timeline_report_md(case_results), batch_root)
    _copy_csv("stage6_case_comparison.csv", _case_comparison_rows(case_results), batch_root)
    all_event_rows: List[Dict[str, Any]] = []
    for item in case_results:
        for row in item["event_rows"]:
            all_event_rows.append({"case": item["case_label"], **row})
    _copy_jsonl("stage6_route_segment_present_refline_missing_events.jsonl", all_event_rows, batch_root)
    _copy_text(
        "stage6_reference_line_provider_rootcause_report.md",
        _final_report_md(case_results),
        batch_root,
    )


if __name__ == "__main__":
    main()
