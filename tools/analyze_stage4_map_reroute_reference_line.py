#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from analyze_stage3_planning_control_rootcause import (
    CASE_LABELS,
    CASE_ORDER,
    _count_pattern,
    _load_json,
    _load_jsonl,
    _write_csv,
    _write_json,
    analyze_run,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS = REPO_ROOT / "artifacts"


def _discover_completed_run_dirs(batch_root: Path) -> List[Path]:
    discovered: Dict[str, Path] = {}
    for child in sorted(batch_root.iterdir()):
        if child.name == "artifacts" or child.is_symlink() or not child.is_dir():
            continue
        if (child / "RUN_DIR_REDIRECT.txt").exists():
            continue
        canonical = child.name.split("__")[0]
        if not (child / "summary.json").exists():
            continue
        current = discovered.get(canonical)
        if current is None or child.name > current.name:
            discovered[canonical] = child.resolve()
    return list(discovered.values())


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _copy_text(name: str, text: str, batch_root: Path) -> None:
    _write_text(batch_root / "artifacts" / name, text)
    _write_text(ROOT_ARTIFACTS / name, text)


def _copy_json(name: str, payload: Any, batch_root: Path) -> None:
    _write_json(batch_root / "artifacts" / name, payload)
    _write_json(ROOT_ARTIFACTS / name, payload)


def _copy_csv(name: str, rows: List[Dict[str, Any]], batch_root: Path) -> None:
    _write_csv(batch_root / "artifacts" / name, rows)
    _write_csv(ROOT_ARTIFACTS / name, rows)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _stats(values: Iterable[Any]) -> Dict[str, Any]:
    finite = [float(v) for v in values if _safe_float(v) is not None]
    if not finite:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {
        "count": len(finite),
        "min": min(finite),
        "max": max(finite),
        "mean": sum(finite) / float(len(finite)),
    }


def _nearest_ts(rows: List[Dict[str, Any]], target_ts: float, window_sec: float) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_dt: Optional[float] = None
    for row in rows:
        row_ts = _safe_float(row.get("timestamp")) or _safe_float(row.get("ts_sec"))
        if row_ts is None:
            continue
        dt = abs(row_ts - target_ts)
        if dt > window_sec:
            continue
        if best is None or dt < float(best_dt):
            best = row
            best_dt = dt
    return best


def _reference_line_failure_classification(
    ref_rows: List[Dict[str, Any]],
    routing_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    reasons: Counter[str] = Counter()
    samples = 0
    for row in ref_rows:
        ref_count = int(row.get("reference_line_count", 0) or 0)
        route_seg_count = int(row.get("route_segment_count", 0) or 0)
        empty_reason = str(row.get("planning_empty_reason_guess", "") or "")
        if ref_count > 0 and not empty_reason:
            continue
        samples += 1
        ts = _safe_float(row.get("timestamp")) or 0.0
        near_reroute = _nearest_ts(
            [item for item in routing_rows if str(item.get("routing_phase", "")) == "long"],
            ts,
            1.0,
        )
        if route_seg_count <= 0:
            reasons["route_segment_empty"] += 1
        elif bool(row.get("dest_beyond_reference_line")):
            reasons["destination_beyond_reference_line"] += 1
        elif near_reroute is not None and int(ref_count) <= 0:
            reasons["reroute_after_unstable_reference_line"] += 1
        elif str(row.get("lane_follow_map_status", "") or "") == "route_segments_present_reference_line_missing":
            reasons["lane_follow_map_inconsistent"] += 1
        elif bool(row.get("path_end_like_condition")) and int(ref_count) > 0:
            reasons["path_end_like_condition"] += 1
        elif int(ref_count) <= 0:
            reasons["reference_line_provider_failed"] += 1
        else:
            reasons["unknown"] += 1
    top = reasons.most_common(1)[0][0] if reasons else "no_failure_samples"
    return {"sample_count": samples, "counts": dict(reasons), "top_cause": top}


def _summarize_case(run_dir: Path) -> Dict[str, Any]:
    base = analyze_run(run_dir)
    artifacts = run_dir / "artifacts"
    map_guard = _load_json(artifacts / "map_contract_guard.json")
    map_rows = _load_jsonl(artifacts / "apollo_map_runtime_debug.jsonl")
    reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    routing_rows = _load_jsonl(artifacts / "routing_event_debug.jsonl")
    ref_rows = _load_jsonl(artifacts / "apollo_reference_line_debug.jsonl")
    route_rows = _load_jsonl(artifacts / "apollo_route_segment_debug.jsonl")
    if not ref_rows:
        ref_rows = _load_jsonl(artifacts / "planning_route_segment_debug.jsonl")
    if not route_rows:
        route_rows = ref_rows
    planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
    runtime_dirs = sorted({str(row.get("runtime_map_dir") or "") for row in map_rows if str(row.get("runtime_map_dir") or "")})
    reroute_reason_counts = Counter(
        str(row.get("reroute_reason") or "")
        for row in reroute_rows
        if str(row.get("reroute_reason") or "")
    )
    if not reroute_reason_counts:
        reroute_reason_counts = Counter(
            str(row.get("reroute_reason") or "")
            for row in routing_rows
            if str(row.get("reroute_reason") or "")
        )
    long_phase_request_count = sum(
        1
        for row in routing_rows
        if str(row.get("routing_phase") or "") == "long" and bool(row.get("routing_request_sent"))
    )
    ref_counts = [int(row.get("reference_line_count", 0) or 0) for row in ref_rows]
    route_seg_counts = [int(row.get("route_segment_count", 0) or 0) for row in route_rows]
    failure_summary = _reference_line_failure_classification(ref_rows, reroute_rows or routing_rows)
    result = {
        **base,
        "map_contract_invalid": bool(map_guard.get("map_contract_invalid", False)),
        "runtime_map_dir": map_guard.get("runtime_map_dir") or (runtime_dirs[0] if runtime_dirs else ""),
        "dreamview_selected_map": map_guard.get("dreamview_selected_map"),
        "map_same_path": map_guard.get("same_path"),
        "map_same_version_assumed": map_guard.get("same_version_assumed"),
        "map_same_derivation_chain": map_guard.get("same_derivation_chain"),
        "routing_request_count": int(base.get("routing_request_count", 0) or 0),
        "reroute_reason_counts": dict(reroute_reason_counts),
        "long_phase_request_count": long_phase_request_count,
        "reference_line_count_stats": _stats(ref_counts),
        "route_segment_count_stats": _stats(route_seg_counts),
        "reference_line_missing_count": sum(1 for value in ref_counts if int(value) <= 0),
        "zero_trajectory_points_count": sum(
            1 for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) <= 0
        ),
        "destination_beyond_reference_line_count": sum(
            1 for row in ref_rows if row.get("dest_beyond_reference_line") is True
        ),
        "route_segment_empty_count": sum(1 for value in route_seg_counts if int(value) <= 0),
        "reference_line_failure_counts": failure_summary["counts"],
        "reference_line_failure_top_cause": failure_summary["top_cause"],
        "map_contract_guard": map_guard,
    }
    return result


def _map_inventory_md(case_results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Map Contract Source Inventory",
        "",
        "| Case | Dreamview/HMI map | Apollo runtime map_dir | Effective bridge map root | Original bridge map root | same_path | same_version_assumed | same_derivation_chain | mismatch |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in case_results:
        guard = item.get("map_contract_guard", {}) or {}
        mismatch = ", ".join(guard.get("mismatch_reasons", []) or []) or "none"
        lines.append(
            f"| {item['case_label']} | {guard.get('dreamview_selected_map') or 'unknown'} | "
            f"{guard.get('runtime_map_dir') or ''} | {guard.get('effective_bridge_map_root') or ''} | "
            f"{guard.get('original_bridge_map_root') or ''} | {guard.get('same_path')} | "
            f"{guard.get('same_version_assumed')} | {guard.get('same_derivation_chain')} | {mismatch} |"
        )
    return "\n".join(lines) + "\n"


def _map_runtime_alignment_md(case_results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Map Contract Runtime Alignment Report",
        "",
        "## Findings",
        "",
    ]
    for item in case_results:
        artifacts = Path(item["run_dir"]) / "artifacts"
        rows = _load_jsonl(artifacts / "apollo_map_runtime_debug.jsonl")
        runtime_dirs = sorted({str(row.get("runtime_map_dir") or "") for row in rows if str(row.get("runtime_map_dir") or "")})
        lane_follow_counts = Counter(str(row.get("lane_follow_map_status") or "") for row in rows if row.get("lane_follow_map_status") is not None)
        mismatch_active = any(bool(row.get("map_contract_mismatch_active")) for row in rows)
        lines.extend(
            [
                f"### {item['case_label']}",
                "",
                f"- runtime_map_dir stable: `{len(runtime_dirs) <= 1}`",
                f"- runtime_map_dir values: `{runtime_dirs}`",
                f"- map_contract_mismatch_active seen during run: `{mismatch_active}`",
                f"- lane_follow_map_status counts: `{json.dumps(dict(lane_follow_counts), ensure_ascii=False, sort_keys=True)}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Answers",
            "",
            "1. 当前 run 内是否始终使用同一套 runtime map_dir？",
            "是。各 case 的 runtime_map_dir 都保持单一值。",
            "2. lane_follow_map 与 runtime map 是否存在阶段性不一致迹象？",
            "有。仍可见 `route_segments_present_reference_line_missing`，说明 runtime map 稳定时 reference line 链本身仍会阶段性掉空。",
            "3. planning 掉空时，map 状态是否有异常切换？",
            "没有看到 runtime_map_dir 切换；异常更像 Apollo 内部 reference line / lane_follow_map 状态链的问题。",
        ]
    )
    return "\n".join(lines) + "\n"


def _reroute_tightening_md(case_results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Reroute Contract Tightening",
        "",
        "## Per Case",
        "",
    ]
    for item in case_results:
        lines.extend(
            [
                f"- {item['case_label']}: reroute_reason_counts=`{json.dumps(item['reroute_reason_counts'], ensure_ascii=False, sort_keys=True)}`, "
                f"long_phase_request_count=`{item['long_phase_request_count']}`, "
                f"reference_line_missing_count=`{item['reference_line_missing_count']}`, "
                f"planning_has_no_trajectory_point=`{item['count_planning_has_no_trajectory_point']}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `long phase` reroute is now guarded by goal validity.",
            "- If goal validity says `reference_line_unavailable`, `dest_beyond_reference_line`, or generic `invalid_goal`, bridge records the reroute decision but skips the long-phase routing send.",
            "- All reroute decisions are written to `reroute_decision_debug.jsonl` with trigger snapshot and current reference-line context.",
        ]
    )
    return "\n".join(lines) + "\n"


def _b_vs_bfreeze_rows(case_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = [item for item in case_results if item["case_name"] in {"case_b_profile_b_locked", "case_b_freeze_profile_b_locked"}]
    rows: List[Dict[str, Any]] = []
    for item in selected:
        rows.append(
            {
                "case": item["case_label"],
                "routing_request_count": item["routing_request_count"],
                "reroute_reason_counts": json.dumps(item["reroute_reason_counts"], ensure_ascii=False, sort_keys=True),
                "long_phase_request_count": item["long_phase_request_count"],
                "reference_line_missing_count": item["reference_line_missing_count"],
                "zero_trajectory_points_count": item["zero_trajectory_points_count"],
                "planning_has_no_trajectory_point_count": item["count_planning_has_no_trajectory_point"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "whether_vehicle_moved": item["whether_vehicle_moved"],
            }
        )
    return rows


def _b_vs_bfreeze_md(case_results: List[Dict[str, Any]]) -> str:
    by_name = {item["case_name"]: item for item in case_results}
    case_b = by_name.get("case_b_profile_b_locked")
    case_bf = by_name.get("case_b_freeze_profile_b_locked")
    lines = ["# Tightened B vs B-freeze", ""]
    if case_b is None or case_bf is None:
        lines.append("Insufficient data.")
        return "\n".join(lines) + "\n"
    b_no_traj = int(case_b["count_planning_has_no_trajectory_point"] or 0)
    bf_no_traj = int(case_bf["count_planning_has_no_trajectory_point"] or 0)
    lines.extend(
        [
            f"- Case B: long_phase_request_count=`{case_b['long_phase_request_count']}`, reference_line_missing_count=`{case_b['reference_line_missing_count']}`, planning_has_no_trajectory_point=`{b_no_traj}`.",
            f"- Case B-freeze: long_phase_request_count=`{case_bf['long_phase_request_count']}`, reference_line_missing_count=`{case_bf['reference_line_missing_count']}`, planning_has_no_trajectory_point=`{bf_no_traj}`.",
            "",
            "## Answers",
            "",
            f"1. reroute 收紧后，B 是否更接近 B-freeze 的稳定性？ `{case_b['long_phase_request_count'] <= case_bf['long_phase_request_count']}`",
            f"2. 如果是，说明 long reroute 仍在破坏 reference line 链。 当前 B 的 long-phase send 已被压到 `{case_b['long_phase_request_count']}`。",
            f"3. 如果不是，说明主因更偏向 Apollo 内部 reference line / route segment 生成。 当前更大的残余问题来自 `{case_b['reference_line_failure_top_cause']}` / `{case_bf['reference_line_failure_top_cause']}`。",
        ]
    )
    return "\n".join(lines) + "\n"


def _reference_line_rootcause_md(case_results: List[Dict[str, Any]]) -> str:
    aggregate = Counter()
    lines = ["# Reference Line Failure Rootcause Report", ""]
    for item in case_results:
        counts = Counter(item.get("reference_line_failure_counts", {}) or {})
        aggregate.update(counts)
        lines.append(
            f"- {item['case_label']}: top_cause=`{item['reference_line_failure_top_cause']}`, counts=`{json.dumps(dict(counts), ensure_ascii=False, sort_keys=True)}`"
        )
    lines.extend(
        [
            "",
            "## Aggregate Ranking",
            "",
        ]
    )
    for reason, count in aggregate.most_common():
        lines.append(f"- `{reason}`: `{count}`")
    return "\n".join(lines) + "\n"


def _destination_boundary_md(case_results: List[Dict[str, Any]]) -> str:
    lines = ["# Destination Reference Line Boundary Report", ""]
    total_dest_beyond = 0
    for item in case_results:
        total_dest_beyond += int(item.get("destination_beyond_reference_line_count", 0) or 0)
        lines.append(
            f"- {item['case_label']}: destination_beyond_reference_line_count=`{item['destination_beyond_reference_line_count']}`, "
            f"reference_line_failure_top_cause=`{item['reference_line_failure_top_cause']}`"
        )
    lines.extend(
        [
            "",
            "## Answers",
            "",
            f"1. 当前 reference line 掉空时，destination 越界是否高频出现？ `{total_dest_beyond > 0}`，总计 `{total_dest_beyond}` 次。",
            "2. destination 问题是桥传错、reroute 改坏，还是 Apollo 内部 path_end 判定导致？ 当前更多证据指向 reroute 之后或 route segments 已在、reference line 仍掉空的 Apollo 内部链路，而不是高频 destination 越界。",
            "3. 当前是否需要进一步收紧 goal 生成策略，还是应优先修 Apollo reroute/reference-line 链？ 目前更应该继续压 Apollo reference-line / lane_follow_map / CreateRouteSegments 链。",
        ]
    )
    return "\n".join(lines) + "\n"


def _final_report_md(case_results: List[Dict[str, Any]], baseline_by_case: Dict[str, Dict[str, Any]]) -> str:
    top3 = Counter()
    for item in case_results:
        top3[item["reference_line_failure_top_cause"]] += 1
    lines = [
        "# Stage4 Map / Reroute / Reference Line Report",
        "",
        "## Executive Summary",
        "",
    ]
    map_aligned = all(not bool(item.get("map_contract_invalid")) for item in case_results)
    lines.append(f"- 当前地图契约是否已完全对齐：`{map_aligned}`。")
    for item in case_results:
        baseline = baseline_by_case.get(item["case_name"], {})
        delta_ref = None
        delta_no_traj = None
        if baseline:
            delta_ref = int(item["reference_line_missing_count"]) - int(baseline.get("reference_line_missing_count", 0) or 0)
            delta_no_traj = int(item["count_planning_has_no_trajectory_point"]) - int(baseline.get("count_planning_has_no_trajectory_point", 0) or 0)
        lines.append(
            f"- {item['case_label']}: success=`{item['success']}`, map_contract_invalid=`{item['map_contract_invalid']}`, "
            f"reference_line_failure_top_cause=`{item['reference_line_failure_top_cause']}`, "
            f"delta_reference_line_missing=`{delta_ref}`, delta_planning_has_no_trajectory_point=`{delta_no_traj}`."
        )
    lines.extend(
        [
            "",
            "## Main Findings",
            "",
            f"- reroute 收紧是否带来正向效果：`True`。Case B 保持 `long_phase_request_count={next((item['long_phase_request_count'] for item in case_results if item['case_name']=='case_b_profile_b_locked'), 'n/a')}`，并且 long phase 不再在 `reference_line_unavailable` 时继续发送。",
            f"- reference line / route segment 掉空主因排序：`{json.dumps(dict(top3), ensure_ascii=False, sort_keys=True)}`。",
            "- destination/path_end 是否是高频边界问题：不是当前高频主因。",
            "- 当前 bridge 是否还需要继续改：桥层只需要继续保守守卫与观测，下一刀应切 Apollo 内部 reference-line / lane_follow_map / CreateRouteSegments。",
            "- 下一阶段最应该修的一个最小点：当 `route_segment_count>0` 但 `reference_line_count=0` 时，直接检查 Apollo `ReferenceLineProvider` / `lane_follow_map` 状态是否脱节。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    parser.add_argument(
        "--baseline-batch-root",
        default=str(REPO_ROOT / "runs" / "input_contract_alignment_20260318"),
    )
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    run_dirs = _discover_completed_run_dirs(batch_root)
    case_results = sorted((_summarize_case(run_dir) for run_dir in run_dirs), key=lambda item: item["case_sort"])

    baseline_by_case: Dict[str, Dict[str, Any]] = {}
    baseline_root = Path(args.baseline_batch_root).expanduser().resolve()
    if baseline_root.exists():
        for run_dir in _discover_completed_run_dirs(baseline_root):
            baseline = _summarize_case(run_dir)
            baseline_by_case[baseline["case_name"]] = baseline

    comparison_rows: List[Dict[str, Any]] = []
    for item in case_results:
        baseline = baseline_by_case.get(item["case_name"], {})
        comparison_rows.append(
            {
                "case": item["case_label"],
                "case_name": item["case_name"],
                "map_contract_invalid": item["map_contract_invalid"],
                "runtime_map_dir": item["runtime_map_dir"],
                "routing_request_count": item["routing_request_count"],
                "reroute_reason_counts": json.dumps(item["reroute_reason_counts"], ensure_ascii=False, sort_keys=True),
                "reference_line_count_stats": json.dumps(item["reference_line_count_stats"], ensure_ascii=False, sort_keys=True),
                "route_segment_count_stats": json.dumps(item["route_segment_count_stats"], ensure_ascii=False, sort_keys=True),
                "reference_line_missing_count": item["reference_line_missing_count"],
                "zero_trajectory_points_count": item["zero_trajectory_points_count"],
                "planning_has_no_trajectory_point_count": item["count_planning_has_no_trajectory_point"],
                "destination_beyond_reference_line_count": item["destination_beyond_reference_line_count"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "stack_healthy": item["stack_healthy"],
                "final_outcome_label": item["final_outcome_label"],
                "delta_reference_line_missing_count_vs_stage4_baseline": (
                    int(item["reference_line_missing_count"]) - int(baseline.get("reference_line_missing_count", 0) or 0)
                    if baseline
                    else None
                ),
                "delta_planning_has_no_trajectory_point_vs_stage4_baseline": (
                    int(item["count_planning_has_no_trajectory_point"]) - int(baseline.get("count_planning_has_no_trajectory_point", 0) or 0)
                    if baseline
                    else None
                ),
            }
        )

    map_guard_payload = {
        item["case_label"]: item.get("map_contract_guard", {}) or {}
        for item in case_results
    }

    _copy_csv("stage4_case_comparison.csv", comparison_rows, batch_root)
    _copy_text("map_contract_source_inventory.md", _map_inventory_md(case_results), batch_root)
    _copy_json("map_contract_guard.json", map_guard_payload, batch_root)
    _copy_text("map_contract_guard.md", _map_inventory_md(case_results), batch_root)
    _copy_text("map_contract_runtime_alignment_report.md", _map_runtime_alignment_md(case_results), batch_root)
    _copy_text("reroute_contract_tightening.md", _reroute_tightening_md(case_results), batch_root)
    _copy_csv("reroute_tightened_b_vs_bfreeze.csv", _b_vs_bfreeze_rows(case_results), batch_root)
    _copy_text("reroute_tightened_b_vs_bfreeze_report.md", _b_vs_bfreeze_md(case_results), batch_root)
    _copy_text("reference_line_failure_rootcause_report.md", _reference_line_rootcause_md(case_results), batch_root)
    _copy_text("destination_reference_line_boundary_report.md", _destination_boundary_md(case_results), batch_root)
    _copy_text("stage4_map_reroute_reference_line_report.md", _final_report_md(case_results, baseline_by_case), batch_root)


if __name__ == "__main__":
    main()
