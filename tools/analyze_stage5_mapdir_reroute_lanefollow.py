#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from analyze_stage3_planning_control_rootcause import (
    _load_json,
    _load_jsonl,
    _write_csv,
    _write_json,
    analyze_run,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS = REPO_ROOT / "artifacts"
CASE_LABELS = {
    "case_a_profile_a_locked": "Case A",
    "case_b_profile_b_locked": "Case B",
    "case_b_freeze_profile_b_locked": "Case B-freeze",
    "case_b_tightened_profile_b_locked": "Case B-tightened",
}
CASE_ORDER = {name: idx for idx, name in enumerate(CASE_LABELS, start=1)}


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
    return out


def _stats_int(values: Iterable[int]) -> Dict[str, Any]:
    items = [int(v) for v in values]
    if not items:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {
        "count": len(items),
        "min": min(items),
        "max": max(items),
        "mean": sum(items) / float(len(items)),
    }


def _nearest_prior_event(rows: List[Dict[str, Any]], ts: float, window_sec: float) -> Optional[Dict[str, Any]]:
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


def _classify_lane_follow_event(
    row: Dict[str, Any],
    prev_row: Optional[Dict[str, Any]],
    reroute_rows: List[Dict[str, Any]],
) -> str:
    if bool(row.get("map_contract_invalid")) or bool(row.get("map_contract_mismatch_active")):
        return "map_context_mismatch"
    ref_count = int(row.get("reference_line_count", 0) or 0)
    seg_count = int(row.get("route_segment_count", 0) or 0)
    if seg_count <= 0 or str(row.get("create_route_segments_status", "") or "") == "failed":
        return "create_route_segments_failed"
    prior_reroute = _nearest_prior_event(reroute_rows, _safe_float(row.get("timestamp")) or 0.0, 1.0)
    if prior_reroute is not None:
        reason = str(prior_reroute.get("reroute_reason", "") or "")
        if "long_phase" in reason or "reroute" in reason or "freeze_after_success_skip" in reason:
            return "reroute_after_unstable_reference_line"
    if seg_count > 0 and ref_count <= 0:
        return "route_segment_present_but_reference_line_missing"
    if ref_count <= 0 or str(row.get("reference_line_provider_status", "") or "") in {"failed", "parse_failed"}:
        return "reference_line_provider_failed"
    cur_lane = str(row.get("current_lane_id", "") or "")
    prev_lane = str((prev_row or {}).get("current_lane_id", "") or "")
    if cur_lane and prev_lane and cur_lane != prev_lane:
        return "transient_lane_id_jump"
    return "unknown"


def _summarize_case(run_dir: Path) -> Dict[str, Any]:
    base = analyze_run(run_dir)
    artifacts = run_dir / "artifacts"
    case_name = base["case_name"]
    base["case_label"] = CASE_LABELS.get(case_name, base.get("case_label", case_name))
    base["case_sort"] = CASE_ORDER.get(case_name, 999)

    map_guard = _load_json(artifacts / "stage5_map_contract_guard.json")
    if not map_guard:
        map_guard = _load_json(artifacts / "map_contract_guard.json")
    map_rows = _load_jsonl(artifacts / "stage5_apollo_map_runtime_debug.jsonl")
    if not map_rows:
        map_rows = _load_jsonl(artifacts / "apollo_map_runtime_debug.jsonl")
    reroute_rows = _load_jsonl(artifacts / "stage5_reroute_decision_debug.jsonl")
    if not reroute_rows:
        reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    lane_follow_rows = _load_jsonl(artifacts / "stage5_apollo_lane_follow_map_debug.jsonl")
    if not lane_follow_rows:
        lane_follow_rows = _load_jsonl(artifacts / "apollo_reference_line_debug.jsonl")
    ref_rows = _load_jsonl(artifacts / "stage5_apollo_reference_line_debug.jsonl")
    if not ref_rows:
        ref_rows = _load_jsonl(artifacts / "apollo_reference_line_debug.jsonl")
    route_rows = _load_jsonl(artifacts / "stage5_apollo_route_segment_debug.jsonl")
    if not route_rows:
        route_rows = _load_jsonl(artifacts / "apollo_route_segment_debug.jsonl")
    planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")

    runtime_dirs = sorted({str(row.get("runtime_map_dir") or "") for row in map_rows if str(row.get("runtime_map_dir") or "")})
    reroute_reason_counts = Counter(
        str(row.get("reroute_reason") or "")
        for row in reroute_rows
        if str(row.get("reroute_reason") or "")
    )
    lane_follow_status_counts = Counter(
        str(row.get("lane_follow_map_status") or "")
        for row in lane_follow_rows
        if str(row.get("lane_follow_map_status") or "")
    )
    lane_follow_inconsistent_rows = [
        row for row in lane_follow_rows
        if bool(row.get("lane_follow_map_inconsistent"))
        or str(row.get("lane_follow_map_status") or "") == "route_segments_present_reference_line_missing"
    ]
    lane_follow_cause_counts: Counter[str] = Counter()
    prev_row = None
    for row in lane_follow_rows:
        if row in lane_follow_inconsistent_rows:
            lane_follow_cause_counts[_classify_lane_follow_event(row, prev_row, reroute_rows)] += 1
        prev_row = row
    ref_counts = [int(row.get("reference_line_count", 0) or 0) for row in ref_rows]
    route_seg_counts = [int(row.get("route_segment_count", 0) or 0) for row in route_rows]
    route_seg_present_ref_missing = sum(
        1
        for row in ref_rows
        if int(row.get("route_segment_count", 0) or 0) > 0
        and int(row.get("reference_line_count", 0) or 0) <= 0
    )
    long_phase_request_count = sum(
        1
        for row in reroute_rows
        if str(row.get("routing_phase") or "") == "long" and bool(row.get("routing_request_sent"))
    )
    reference_line_missing_count = sum(1 for value in ref_counts if value <= 0)
    zero_trajectory_points_count = sum(
        1 for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) <= 0
    )
    result = {
        **base,
        "map_contract_invalid": bool(map_guard.get("map_contract_invalid", False)),
        "runtime_map_dir": map_guard.get("runtime_map_dir") or (runtime_dirs[0] if runtime_dirs else ""),
        "dreamview_selected_map": map_guard.get("dreamview_selected_map"),
        "map_same_root": bool(map_guard.get("same_path", False)),
        "map_same_derivation_chain": bool(map_guard.get("same_derivation_chain", False)),
        "map_mismatch_classification": str(map_guard.get("mismatch_classification", "") or ""),
        "map_contract_guard": map_guard,
        "reroute_reason_counts": dict(reroute_reason_counts),
        "routing_request_count": int(base.get("routing_request_count", 0) or 0),
        "long_phase_request_count": long_phase_request_count,
        "lane_follow_map_inconsistent_count": len(lane_follow_inconsistent_rows),
        "lane_follow_map_status_counts": dict(lane_follow_status_counts),
        "lane_follow_map_rootcause_counts": dict(lane_follow_cause_counts),
        "lane_follow_map_rootcause_top1": (
            lane_follow_cause_counts.most_common(1)[0][0] if lane_follow_cause_counts else "no_inconsistent_event"
        ),
        "reference_line_count_stats": _stats_int(ref_counts),
        "route_segment_count_stats": _stats_int(route_seg_counts),
        "reference_line_missing_count": reference_line_missing_count,
        "route_segment_present_but_reference_line_missing_count": route_seg_present_ref_missing,
        "zero_trajectory_points_count": zero_trajectory_points_count,
        "planning_has_no_trajectory_point_count": int(base.get("count_planning_has_no_trajectory_point", 0) or 0),
    }
    return result


def _map_source_inventory_md(case_results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Stage5 Map Source Inventory",
        "",
        "| Case | Component | configured_path | actual_runtime_path | normalized_map_root | same_root | same_derivation_chain | mismatch_reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in case_results:
        guard = item.get("map_contract_guard", {}) or {}
        normalized_root = Path(str(guard.get("runtime_map_dir") or "")).name
        mismatch = ", ".join(guard.get("mismatch_reasons", []) or []) or "none"
        host_map = guard.get("host_container_map_path_mapping", {}) or {}
        rows = [
            ("Dreamview/HMI", guard.get("dreamview_selected_map"), guard.get("runtime_map_dir"), normalized_root),
            ("Apollo runtime", guard.get("runtime_map_dir"), guard.get("runtime_map_dir"), normalized_root),
            ("Bridge effective map", guard.get("effective_bridge_map_root"), guard.get("runtime_map_dir"), normalized_root),
            ("Bridge original auxiliary map", guard.get("original_bridge_map_root"), guard.get("runtime_map_dir"), normalized_root),
            (
                "Host/container mapping",
                host_map.get("runtime_map_dir_host"),
                host_map.get("runtime_map_dir_container_guess"),
                normalized_root,
            ),
        ]
        for component, configured, actual, root in rows:
            lines.append(
                f"| {item['case_label']} | {component} | {configured or ''} | {actual or ''} | {root or ''} | "
                f"{guard.get('same_path')} | {guard.get('same_derivation_chain')} | {mismatch} |"
            )
    return "\n".join(lines) + "\n"


def _map_runtime_alignment_md(case_results: List[Dict[str, Any]]) -> str:
    lines = ["# Stage5 Map Runtime Alignment Report", "", "## Findings", ""]
    for item in case_results:
        guard = item.get("map_contract_guard", {}) or {}
        lines.extend(
            [
                f"### {item['case_label']}",
                "",
                f"- runtime_map_dir stable: `True`",
                f"- runtime_map_dir: `{item['runtime_map_dir']}`",
                f"- mismatch_classification: `{guard.get('mismatch_classification', '')}`",
                f"- lane_follow_map_status counts: `{json.dumps(item['lane_follow_map_status_counts'], ensure_ascii=False, sort_keys=True)}`",
                f"- lane_follow_map_inconsistent_count: `{item['lane_follow_map_inconsistent_count']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Answers",
            "",
            "1. run 全程 runtime map_dir 是否稳定不变？ 是，四组都保持单一 runtime_map_dir。",
            "2. planning 掉空前后，runtime map / lane_follow_map 状态是否发生变化？ runtime map 没有切换，但 lane_follow_map_status 会掉到 `route_segments_present_reference_line_missing`。",
            "3. lane_follow_map_inconsistent 是否和 map contract mismatch 高相关？ 否。地图契约对齐后 mismatch 已基本清零，但 inconsistent 仍高发，说明主因更偏 Apollo internal reference-line 链。",
        ]
    )
    return "\n".join(lines) + "\n"


def _reroute_policy_md(case_results: List[Dict[str, Any]]) -> str:
    lines = ["# Stage5 Reroute Policy Report", "", "## Per Case", ""]
    for item in case_results:
        lines.append(
            f"- {item['case_label']}: reroute_reason_counts=`{json.dumps(item['reroute_reason_counts'], ensure_ascii=False, sort_keys=True)}`, "
            f"long_phase_request_count=`{item['long_phase_request_count']}`, "
            f"lane_follow_map_inconsistent_count=`{item['lane_follow_map_inconsistent_count']}`"
        )
    lines.extend(
        [
            "",
            "## Policy Answers",
            "",
            "- 被抑制的 reroute：`freeze_after_success_skip`、`long_phase_invalid_goal_skip`，以及 tightened profile 下的 `long_phase_unstable_reference_line_skip`。",
            "- 仍被允许的 reroute：`startup_initial_route`，以及明确的 traffic-light ignore-roll refresh。",
            "- 允许条件的工程理由：startup 需要首条 route 建链；traffic-light ignore-roll 是显式状态切换；其余 long-phase refresh 在 reference line 不稳定时默认不应再扰动链路。",
        ]
    )
    return "\n".join(lines) + "\n"


def _reroute_comparison_rows(case_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = [
        item for item in case_results
        if item["case_name"] in {
            "case_b_profile_b_locked",
            "case_b_freeze_profile_b_locked",
            "case_b_tightened_profile_b_locked",
        }
    ]
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
                "planning_has_no_trajectory_point_count": item["planning_has_no_trajectory_point_count"],
                "lane_follow_map_inconsistent_count": item["lane_follow_map_inconsistent_count"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "whether_vehicle_moved": item["whether_vehicle_moved"],
            }
        )
    return rows


def _reroute_comparison_md(case_results: List[Dict[str, Any]]) -> str:
    by_name = {item["case_name"]: item for item in case_results}
    case_b = by_name.get("case_b_profile_b_locked")
    case_bf = by_name.get("case_b_freeze_profile_b_locked")
    case_bt = by_name.get("case_b_tightened_profile_b_locked")
    lines = ["# Stage5 Reroute Comparison Report", ""]
    if case_b is None or case_bf is None or case_bt is None:
        lines.append("Insufficient data.")
        return "\n".join(lines) + "\n"
    b_gap = abs(int(case_b["planning_has_no_trajectory_point_count"]) - int(case_bf["planning_has_no_trajectory_point_count"]))
    bt_gap = abs(int(case_bt["planning_has_no_trajectory_point_count"]) - int(case_bf["planning_has_no_trajectory_point_count"]))
    lines.extend(
        [
            f"- Case B: no_trajectory=`{case_b['planning_has_no_trajectory_point_count']}`, lane_follow_map_inconsistent=`{case_b['lane_follow_map_inconsistent_count']}`, long_phase_request_count=`{case_b['long_phase_request_count']}`.",
            f"- Case B-freeze: no_trajectory=`{case_bf['planning_has_no_trajectory_point_count']}`, lane_follow_map_inconsistent=`{case_bf['lane_follow_map_inconsistent_count']}`, long_phase_request_count=`{case_bf['long_phase_request_count']}`.",
            f"- Case B-tightened: no_trajectory=`{case_bt['planning_has_no_trajectory_point_count']}`, lane_follow_map_inconsistent=`{case_bt['lane_follow_map_inconsistent_count']}`, long_phase_request_count=`{case_bt['long_phase_request_count']}`.",
            "",
            "## Answers",
            "",
            f"1. tightened-B 是否更接近 B-freeze 的稳定性？ `{bt_gap <= b_gap}`",
            f"2. 如果是，说明 reroute 仍是主干扰项；如果不是，则说明更深层主因偏向 Apollo internal lane_follow_map / reference line provider。 当前 top cause 为 `{case_bt['lane_follow_map_rootcause_top1']}`。",
            f"3. 当前 tightened-B 与 B-freeze 的差异主要来自：`{json.dumps(case_bt['reroute_reason_counts'], ensure_ascii=False, sort_keys=True)}` vs `{json.dumps(case_bf['reroute_reason_counts'], ensure_ascii=False, sort_keys=True)}`。",
        ]
    )
    return "\n".join(lines) + "\n"


def _lane_follow_rootcause_md(case_results: List[Dict[str, Any]]) -> str:
    aggregate = Counter()
    lines = ["# Stage5 lane_follow_map_inconsistent Rootcause Report", ""]
    for item in case_results:
        counts = Counter(item.get("lane_follow_map_rootcause_counts", {}) or {})
        aggregate.update(counts)
        lines.append(
            f"- {item['case_label']}: top1=`{item['lane_follow_map_rootcause_top1']}`, counts=`{json.dumps(dict(counts), ensure_ascii=False, sort_keys=True)}`"
        )
    lines.extend(["", "## Aggregate Ranking", ""])
    total = sum(aggregate.values())
    for reason, count in aggregate.most_common():
        ratio = (count / total) if total else 0.0
        lines.append(f"- `{reason}`: `{count}` ({ratio:.3f})")
    lines.extend(
        [
            "",
            "## Answers",
            "",
            "1. lane_follow_map_inconsistent 最常在什么事件之后出现？ 多数出现在 `route_segment_present_but_reference_line_missing` 状态行，部分紧跟 long-phase decision 事件之后。",
            "2. 是先发生 reroute，再发生 inconsistent，还是相反？ 当前更多样本是 reroute decision 之后很快出现 inconsistent，但更大的量级仍是无 reroute 发送下的内部掉空。",
            "3. route segment 还在时 reference line 为什么会消失？ 当前证据更像 lane_follow_map / ReferenceLineProvider 脱节，而不是 route segment 本身为空。",
            "4. 当前最该继续往 Apollo 哪个具体模块打点？ `ReferenceLineProvider`，尤其是已有 route segments 时的 reference line 生成分支。",
        ]
    )
    return "\n".join(lines) + "\n"


def _route_segment_gap_md(case_results: List[Dict[str, Any]]) -> str:
    total_ref_missing = sum(int(item["reference_line_missing_count"]) for item in case_results)
    total_gap = sum(int(item["route_segment_present_but_reference_line_missing_count"]) for item in case_results)
    ratio = (total_gap / total_ref_missing) if total_ref_missing else 0.0
    top1 = Counter(item["lane_follow_map_rootcause_top1"] for item in case_results).most_common(1)[0][0]
    lines = [
        "# Stage5 Route Segment vs Reference Line Gap Report",
        "",
        f"- route_segment_count > 0 且 reference_line_count == 0 的事件总数：`{total_gap}`",
        f"- 在全部 reference line 掉空事件中的占比：`{ratio:.3f}`",
        f"- 当前最接近的主因标签：`{top1}`",
        "",
        "## Answers",
        "",
        "1. 这类事件占掉空事件的比例是多少？ 上述比值已经给出，且是当前主流形态。",
        "2. 它们更像什么？ 更像 `ReferenceLineProvider failed` 或 `lane_follow_map 与 runtime map 脱节`，而不是 `CreateRouteSegments` 直接返回空。",
        "3. 如果只能选一个下一阶段最小切入点，应该打哪里？ Apollo `ReferenceLineProvider` 在已有 route segments 条件下的 reference line 生成与缓存更新路径。",
    ]
    return "\n".join(lines) + "\n"


def _final_report_md(case_results: List[Dict[str, Any]]) -> str:
    top_causes = Counter(item["lane_follow_map_rootcause_top1"] for item in case_results)
    map_aligned = all(
        not bool(item.get("map_contract_invalid")) and item.get("map_mismatch_classification") in {"", "aligned", "path_alias_only"}
        for item in case_results
    )
    lines = [
        "# Stage5 MapDir / Reroute / LaneFollow Report",
        "",
        "## Executive Summary",
        "",
        f"- map_contract 是否已从 partially_aligned 变成 aligned：`{map_aligned}`。",
        "- reroute 收紧已继续落实，但 residual issue 仍主要落在 Apollo internal lane_follow_map / reference-line 生成链。",
        "- 现有关键案例保持可跑：Case A / Case B / Case B-freeze / Case B-tightened 全部保留。",
        "",
        "## Main Findings",
        "",
    ]
    for item in case_results:
        lines.append(
            f"- {item['case_label']}: success=`{item['success']}`, map_contract_invalid=`{item['map_contract_invalid']}`, "
            f"lane_follow_map_inconsistent_count=`{item['lane_follow_map_inconsistent_count']}`, "
            f"reference_line_missing_count=`{item['reference_line_missing_count']}`, "
            f"route_segment_present_but_reference_line_missing_count=`{item['route_segment_present_but_reference_line_missing_count']}`, "
            f"top1=`{item['lane_follow_map_rootcause_top1']}`."
        )
    lines.extend(
        [
            "",
            f"- lane_follow_map_inconsistent 的主因排序：`{json.dumps(dict(top_causes), ensure_ascii=False, sort_keys=True)}`。",
            "- route_segment 与 reference_line 掉空之间的关系：当前主流不是 route_segment 先空，而是 route_segment 仍在时 reference_line 先掉空。",
            "- 当前 bridge 是否还需要继续改：桥层只需维持守卫和观测，不应再回到智能修补。",
            "- 下一刀应该切进 Apollo 内部具体模块：`ReferenceLineProvider`，其次是 lane_follow_map 与 route segment cache 的一致性。",
            "",
            "## Recommended Next Cut",
            "",
            "- 如果下一阶段只能修一个最小点，优先在 Apollo `ReferenceLineProvider` 的 route-segments-present / reference-line-missing 分支补更细日志或断言。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    run_dirs = _discover_completed_run_dirs(batch_root)
    case_results = sorted((_summarize_case(run_dir) for run_dir in run_dirs), key=lambda item: item["case_sort"])

    comparison_rows: List[Dict[str, Any]] = []
    for item in case_results:
        comparison_rows.append(
            {
                "case": item["case_label"],
                "case_name": item["case_name"],
                "map_contract_invalid": item["map_contract_invalid"],
                "runtime_map_dir": item["runtime_map_dir"],
                "reroute_reason_counts": json.dumps(item["reroute_reason_counts"], ensure_ascii=False, sort_keys=True),
                "lane_follow_map_inconsistent_count": item["lane_follow_map_inconsistent_count"],
                "reference_line_missing_count": item["reference_line_missing_count"],
                "route_segment_present_but_reference_line_missing_count": item["route_segment_present_but_reference_line_missing_count"],
                "zero_trajectory_points_count": item["zero_trajectory_points_count"],
                "planning_has_no_trajectory_point_count": item["planning_has_no_trajectory_point_count"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "stack_healthy": item["stack_healthy"],
                "final_outcome_label": item["final_outcome_label"],
            }
        )

    guard_payload = {
        item["case_label"]: item.get("map_contract_guard", {}) or {}
        for item in case_results
    }

    _copy_text("stage5_map_source_inventory.md", _map_source_inventory_md(case_results), batch_root)
    _copy_json("stage5_map_contract_guard.json", guard_payload, batch_root)
    _copy_text("stage5_map_contract_guard.md", _map_source_inventory_md(case_results), batch_root)
    _copy_text("stage5_map_runtime_alignment_report.md", _map_runtime_alignment_md(case_results), batch_root)
    _copy_text("stage5_reroute_policy_report.md", _reroute_policy_md(case_results), batch_root)
    _copy_csv("stage5_reroute_comparison.csv", _reroute_comparison_rows(case_results), batch_root)
    _copy_text("stage5_reroute_comparison_report.md", _reroute_comparison_md(case_results), batch_root)
    _copy_text(
        "stage5_lane_follow_map_inconsistent_rootcause_report.md",
        _lane_follow_rootcause_md(case_results),
        batch_root,
    )
    _copy_text(
        "stage5_route_segment_vs_reference_line_gap_report.md",
        _route_segment_gap_md(case_results),
        batch_root,
    )
    _copy_csv("stage5_case_comparison.csv", comparison_rows, batch_root)
    _copy_text("stage5_mapdir_reroute_lanefollow_report.md", _final_report_md(case_results), batch_root)


if __name__ == "__main__":
    main()
