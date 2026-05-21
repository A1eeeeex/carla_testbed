#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import json
import math
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS = REPO_ROOT / "artifacts"

CASE_LABELS = {
    "case_a_profile_a_locked": "Case A",
    "case_b_profile_b_locked": "Case B",
}

CASE_ORDER = {name: idx for idx, name in enumerate(CASE_LABELS, start=1)}


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", errors="ignore") as fp:
        for line in fp:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _count_pattern(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    return len(re.findall(pattern, path.read_text(errors="ignore")))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _discover_run_dirs(batch_root: Path) -> List[Path]:
    discovered: Dict[str, Path] = {}
    seen_resolved: set[Path] = set()
    for child in sorted(batch_root.iterdir()):
        if child.name == "artifacts" or child.is_symlink() or not child.is_dir():
            continue
        if (child / "RUN_DIR_REDIRECT.txt").exists():
            continue
        if not ((child / "summary.json").exists() or (child / "artifacts").exists()):
            continue
        canonical = re.sub(r"__\d+$", "", child.name)
        resolved = child.resolve()
        if resolved in seen_resolved:
            continue
        current = discovered.get(canonical)
        if current is None or child.name > current.name:
            discovered[canonical] = resolved
            seen_resolved.add(resolved)
    return list(discovered.values())


def _analysis_paths(batch_root: Path) -> Dict[str, Path]:
    batch_artifacts = batch_root / "artifacts"
    return {
        "baseline_lock_root": ROOT_ARTIFACTS / "planning_stage2_baseline_lock.md",
        "static_root": ROOT_ARTIFACTS / "planning_observer_static_analysis.md",
        "truth_check_root": ROOT_ARTIFACTS / "planning_nonempty_truth_check.md",
        "comparison_root": ROOT_ARTIFACTS / "planning_truth_case_comparison.csv",
        "validation_root": ROOT_ARTIFACTS / "planning_truth_validation_report.md",
        "stage2_root": ROOT_ARTIFACTS / "planning_stage2_report.md",
        "baseline_lock_batch": batch_artifacts / "planning_stage2_baseline_lock.md",
        "static_batch": batch_artifacts / "planning_observer_static_analysis.md",
        "truth_check_batch": batch_artifacts / "planning_nonempty_truth_check.md",
        "comparison_batch": batch_artifacts / "planning_truth_case_comparison.csv",
        "validation_batch": batch_artifacts / "planning_truth_validation_report.md",
        "stage2_batch": batch_artifacts / "planning_stage2_report.md",
    }


def _current_default_baseline_lock_md() -> str:
    lines = [
        "# Planning Stage2 Baseline Lock",
        "",
        "- Default baseline profile: `configs/io/examples/followstop_apollo_gt.yaml` and `configs/io/examples/followstop_apollo_gt_case2_locked.yaml`",
        "- Experimental mainline profile: `configs/io/examples/followstop_apollo_gt_case3_locked.yaml`",
        "",
        "## Locked Baselines",
        "",
        "- Profile A / default conservative baseline:",
        "  - `localization_back_offset_m = 0.0`",
        "  - `snap_start_to_lane = false`",
        "  - `start_nudge_m = 0.0`",
        "  - `start_nudge_retry_step_m = 0.0`",
        "  - `start_nudge_use_lane_heading = false`",
        "- Profile B / trusted lane-centerline snap mainline:",
        "  - trusted `routing_map.txt` / `sim_map.txt` lane centerline only",
        "  - heading gate enabled",
        "  - `start_nudge_m = 0.0`",
        "  - `start_nudge_use_lane_heading = false`",
        "",
        "## Default Policy",
        "",
        "- Default runtime stays on Profile A because it minimizes bridge-side startup geometry intervention.",
        "- The old bad chain (`base_map_text_xy_heuristic + lane_heading nudge`) is deprecated and kept only in the explicit reproduction config `followstop_apollo_gt_baseline.yaml`.",
        "- Large startup nudge is no longer part of the default startup geometry path.",
        "",
    ]
    return "\n".join(lines)


def _planning_import_diagnostic() -> Dict[str, Any]:
    script = """
import os, sys, importlib, json
sys.path.insert(0, '/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/python')
out = {}
try:
    importlib.import_module('modules.common_msgs.planning_msgs.planning_pb2')
    out['without_env'] = 'ok'
except Exception as exc:
    out['without_env'] = f'{type(exc).__name__}:{exc}'
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
try:
    importlib.invalidate_caches()
    mod = importlib.import_module('modules.common_msgs.planning_msgs.planning_pb2')
    out['with_env'] = 'ok'
    out['has_ADCTrajectory'] = hasattr(mod, 'ADCTrajectory')
except Exception as exc:
    out['with_env'] = f'{type(exc).__name__}:{exc}'
print(json.dumps(out, ensure_ascii=False))
"""
    proc = subprocess.run(
        ["/home/ubuntu/miniconda3/envs/carla16/bin/python", "-c", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return {
            "without_env": f"subprocess_failed:{proc.returncode}",
            "with_env": "",
            "has_ADCTrajectory": False,
            "stderr": proc.stderr.strip(),
        }
    try:
        payload = json.loads(proc.stdout.strip() or "{}")
    except Exception:
        payload = {"stdout": proc.stdout.strip(), "stderr": proc.stderr.strip()}
    return payload


def _static_analysis_markdown(import_diag: Dict[str, Any]) -> str:
    rows = [
        (
            "planning_reader_enabled",
            "tools/apollo10_cyber_bridge/bridge.py::__init__",
            "No YAML toggle controls it directly. Default starts as `False`, and it becomes `True` only if `planning_pb2.ADCTrajectory` imports successfully and the Cyber reader is created.",
        ),
        (
            "planning topic subscription",
            "tools/apollo10_cyber_bridge/bridge.py::__init__ -> _cyber_create_reader",
            "Topic comes from `cyber.planning_channel`, default `/apollo/planning`.",
        ),
        (
            "message type",
            "tools/apollo10_cyber_bridge/bridge.py::__init__",
            "Reader expects `modules.common_msgs.planning_msgs.planning_pb2.ADCTrajectory`.",
        ),
        (
            "old nonempty metric",
            "tools/apollo10_cyber_bridge/bridge.py::_on_planning",
            "Old metric increments only when the callback receives a message and `len(msg.trajectory_point) > 0`.",
        ),
        (
            "old metric storage",
            "tools/apollo10_cyber_bridge/bridge.py::_planning_status / _health_summary",
            "The exported `planning_nonempty_trajectory_count` comes from bridge callback counters, not from log keyword grep.",
        ),
        (
            "new topic debug",
            "tools/apollo10_cyber_bridge/bridge.py::_on_planning / _write_planning_topic_debug_summary",
            "Every planning message now emits a raw observation row and a summary with parse-fail reasons and nonzero-point counts.",
        ),
    ]
    lines = [
        "# Planning Observer Static Analysis",
        "",
        "| topic | code_path | conclusion |",
        "|---|---|---|",
    ]
    for topic, path, conclusion in rows:
        lines.append(f"| {topic} | {path} | {conclusion} |")
    lines.extend(
        [
            "",
            "## Effective Topic Contract",
            "",
            "- topic name: `/apollo/planning`",
            "- message type: `apollo.planning.ADCTrajectory`",
            "- old non-empty condition: `len(msg.trajectory_point) > 0`",
            "- old counter source: bridge callback counters only, not planning/control log grep",
            "",
            "## Potential Misread Points",
            "",
            "- If `planning_pb2` fails to import, the reader stays disabled and old metrics remain zero without distinguishing 'observer disabled' from 'trajectory empty'.",
            "- Before this stage-2 patch there was no raw planning-topic debug file, so zero counts could hide a dead observer chain.",
            "- The observer only understands `ADCTrajectory`; if Apollo publishes a different type or a topic remap is used, the reader will miss it.",
            "",
            "## Import Diagnostic",
            "",
            f"- direct import without forcing pure-Python protobuf: `{import_diag.get('without_env')}`",
            f"- direct import with `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`: `{import_diag.get('with_env')}`",
            f"- imported module exposes `ADCTrajectory`: `{import_diag.get('has_ADCTrajectory')}`",
            "- Bridge-side effective behavior: stage-2 patch now forces pure-Python protobuf mode and extends Apollo `modules.common_msgs.*` package paths before importing `planning_pb2`, so the reader can be enabled even though a direct plain import remains fragile.",
            "",
            "## First Suspect",
            "",
            "- Historical first suspect was protobuf runtime incompatibility disabling `planning_pb2` import, which kept `planning_reader_enabled=false` and made prior `planning_nonempty_trajectory_count=0` at least partially a false zero.",
            "",
        ]
    )
    return "\n".join(lines)


def analyze_run(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    summary = _load_json(run_dir / "summary.json")
    health = _load_json(artifacts / "bridge_health_summary.json")
    stats = _load_json(artifacts / "cyber_bridge_stats.json")
    planning_debug_summary = _load_json(artifacts / "planning_topic_debug_summary.json")
    planning_debug_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
    planning_log = artifacts / "apollo_planning.INFO"
    control_log = artifacts / "apollo_control.INFO"
    effective = _load_yaml(run_dir / "effective.yaml")
    case_name = re.sub(r"__\d+$", "", run_dir.name)

    old_metric = int(
        health.get("planning_nonempty_trajectory_count", 0)
        or ((stats.get("planning") or {}).get("nonempty_trajectory_count", 0) if isinstance(stats.get("planning"), dict) else 0)
        or 0
    )
    new_metric = int(planning_debug_summary.get("messages_with_nonzero_trajectory_points", 0) or 0)
    messages_received = int(planning_debug_summary.get("total_messages_received", 0) or 0)
    parse_fail_count = int(planning_debug_summary.get("parse_fail_count", 0) or 0)
    max_points = int(planning_debug_summary.get("max_trajectory_point_count", 0) or 0)
    first_nonzero_ts = _safe_float(planning_debug_summary.get("first_nonzero_trajectory_timestamp"))
    reader_enabled = bool(
        health.get("planning_reader_enabled")
        if "planning_reader_enabled" in health
        else ((stats.get("planning") or {}).get("reader_enabled") if isinstance(stats.get("planning"), dict) else False)
    )
    reader_reason = str(
        health.get("planning_reader_enable_reason")
        or ((stats.get("planning") or {}).get("reader_enable_reason") if isinstance(stats.get("planning"), dict) else "")
        or ""
    )
    routing_cfg = (((effective.get("algo") or {}).get("apollo") or {}).get("routing") or {})
    first_nonzero_row = next(
        (row for row in planning_debug_rows if int(row.get("trajectory_point_count", 0) or 0) > 0),
        None,
    )
    cross_truth = ""
    if not reader_enabled:
        cross_truth = "observer_disabled_false_zero"
    elif messages_received <= 0:
        cross_truth = "reader_enabled_but_no_messages"
    elif new_metric > 0 and old_metric <= 0:
        cross_truth = "old_metric_false_zero"
    elif messages_received > 0 and new_metric <= 0 and parse_fail_count > 0:
        cross_truth = "parse_failure_or_partial_observer_issue"
    elif messages_received > 0 and new_metric <= 0:
        cross_truth = "true_zero_empty_planning_messages"
    else:
        cross_truth = "nonzero_trajectory_observed"

    return {
        "case_name": case_name,
        "case_label": CASE_LABELS.get(case_name, case_name),
        "case_sort": CASE_ORDER.get(case_name, 999),
        "run_dir": str(run_dir),
        "profile_path": str(run_dir / "effective.yaml"),
        "planning_reader_enabled_effective": reader_enabled,
        "planning_reader_enable_reason": reader_reason,
        "planning_topic_name": str(health.get("planning_topic_name") or planning_debug_summary.get("topic_name") or "/apollo/planning"),
        "planning_message_type": str(health.get("planning_message_type") or planning_debug_summary.get("message_type") or "apollo.planning.ADCTrajectory"),
        "planning_topic_messages_received": messages_received,
        "planning_topic_parse_fail_count": parse_fail_count,
        "messages_with_nonzero_trajectory_points": new_metric,
        "max_trajectory_point_count": max_points,
        "first_nonzero_trajectory_timestamp": first_nonzero_ts,
        "planning_nonempty_trajectory_count_old_metric": old_metric,
        "planning_nonempty_trajectory_count_new_metric": new_metric,
        "first_nonzero_first_point_x": _safe_float((first_nonzero_row or {}).get("first_trajectory_point_x")),
        "first_nonzero_first_point_y": _safe_float((first_nonzero_row or {}).get("first_trajectory_point_y")),
        "count_path_data_is_empty": _count_pattern(planning_log, r"path data is empty"),
        "count_reference_line": _count_pattern(planning_log, r"reference line"),
        "count_fail_to_aggregate_planning_trajectory": _count_pattern(planning_log, r"Fail to aggregate planning trajectory"),
        "count_planner_failed_to_make_a_driving_plan": _count_pattern(planning_log, r"planner failed to make a driving plan"),
        "count_planning_has_no_trajectory_point": _count_pattern(control_log, r"planning has no trajectory point"),
        "count_failed_to_produce_control_command": _count_pattern(control_log, r"Failed to produce control command"),
        "success": bool(summary.get("success", False)),
        "fail_reason": summary.get("fail_reason"),
        "max_speed_mps": _safe_float(summary.get("max_speed_mps")),
        "whether_vehicle_moved": bool((_safe_float(summary.get("max_speed_mps")) or 0.0) > 0.5),
        "cross_truth": cross_truth,
        "freeze_after_success": routing_cfg.get("freeze_after_success"),
        "snap_start_to_lane": routing_cfg.get("snap_start_to_lane"),
        "start_nudge_m": routing_cfg.get("start_nudge_m"),
    }


def _truth_answers(results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Planning Nonempty Truth Check",
        "",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        "",
    ]
    for item in results:
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"1. bridge 是否真的收到了 planning topic？ {'是' if item['planning_topic_messages_received'] > 0 else '否'}",
                f"2. trajectory_point_count 是否曾经 > 0？ {'是' if item['messages_with_nonzero_trajectory_points'] > 0 else '否'}",
                f"3. 旧指标与新指标是否一致？ {'是' if item['planning_nonempty_trajectory_count_old_metric'] == item['planning_nonempty_trajectory_count_new_metric'] else '否'} "
                f"(old={item['planning_nonempty_trajectory_count_old_metric']}, new={item['planning_nonempty_trajectory_count_new_metric']})",
                f"4. 当前判定： `{item['cross_truth']}`",
                f"5. control 模块看到的是否仍是无轨迹？ {'是' if item['count_planning_has_no_trajectory_point'] > 0 else '否'} "
                f"(planning_has_no_trajectory_point={item['count_planning_has_no_trajectory_point']}, Failed_to_produce_control_command={item['count_failed_to_produce_control_command']})",
                f"6. 当前 success 判定是否依赖 planning_nonempty？ {'否' if item['success'] and item['planning_nonempty_trajectory_count_new_metric'] == 0 else '需要进一步判断'}",
                "",
            ]
        )
    overall: str
    if any(item["planning_reader_enabled_effective"] is False for item in results):
        overall = "部分假 0：至少有 case 的 planning observer 未真正打开。"
    elif any(item["messages_with_nonzero_trajectory_points"] > 0 for item in results):
        if any(item["planning_nonempty_trajectory_count_old_metric"] == 0 for item in results):
            overall = "假 0：bridge 新观测发现了非空 trajectory，而旧指标存在漏计。"
        else:
            overall = "上一轮的 planning_nonempty=0 属于假 0；当前锁定 A/B case 中，bridge 已稳定观测到非空 trajectory，且新旧指标一致。"
    else:
        overall = "真 0：bridge 已打开 planning observer，但实验期内没有观测到非空 trajectory。"
    lines.extend(["## Overall", "", f"- {overall}", ""])
    return "\n".join(lines)


def _comparison_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in results:
        rows.append(
            {
                "case": item["case_label"],
                "run_dir": item["run_dir"],
                "planning_reader_enabled_effective": item["planning_reader_enabled_effective"],
                "planning_reader_enable_reason": item["planning_reader_enable_reason"],
                "planning_topic_messages_received": item["planning_topic_messages_received"],
                "planning_topic_parse_fail_count": item["planning_topic_parse_fail_count"],
                "messages_with_nonzero_trajectory_points": item["messages_with_nonzero_trajectory_points"],
                "max_trajectory_point_count": item["max_trajectory_point_count"],
                "first_nonzero_trajectory_timestamp": item["first_nonzero_trajectory_timestamp"],
                "planning_nonempty_trajectory_count_old_metric": item["planning_nonempty_trajectory_count_old_metric"],
                "planning_nonempty_trajectory_count_new_metric": item["planning_nonempty_trajectory_count_new_metric"],
                "count(path data is empty)": item["count_path_data_is_empty"],
                "count(reference line)": item["count_reference_line"],
                "count(Fail to aggregate planning trajectory)": item["count_fail_to_aggregate_planning_trajectory"],
                "count(planner failed to make a driving plan)": item["count_planner_failed_to_make_a_driving_plan"],
                "count(planning has no trajectory point)": item["count_planning_has_no_trajectory_point"],
                "count(Failed to produce control command)": item["count_failed_to_produce_control_command"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "max_speed_mps": item["max_speed_mps"],
                "whether_vehicle_moved": item["whether_vehicle_moved"],
                "cross_truth": item["cross_truth"],
            }
        )
    return rows


def _validation_report(results: List[Dict[str, Any]]) -> str:
    case_lines = [
        "| Case | reader_enabled | messages_received | nonzero_points | old_metric | new_metric | path_empty | reference_line | planning_no_traj | control_cmd_fail | success | fail_reason |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for item in results:
        case_lines.append(
            f"| {item['case_label']} | {item['planning_reader_enabled_effective']} | "
            f"{item['planning_topic_messages_received']} | "
            f"{item['messages_with_nonzero_trajectory_points']} | "
            f"{item['planning_nonempty_trajectory_count_old_metric']} | "
            f"{item['planning_nonempty_trajectory_count_new_metric']} | "
            f"{item['count_path_data_is_empty']} | "
            f"{item['count_reference_line']} | "
            f"{item['count_planning_has_no_trajectory_point']} | "
            f"{item['count_failed_to_produce_control_command']} | "
            f"{item['success']} | {item['fail_reason']} |"
        )
    stage2_truth = "部分真部分假"
    if all(item["planning_reader_enabled_effective"] for item in results):
        if all(item["messages_with_nonzero_trajectory_points"] <= 0 for item in results):
            stage2_truth = "真 0"
        elif any(
            item["planning_nonempty_trajectory_count_old_metric"] != item["planning_nonempty_trajectory_count_new_metric"]
            for item in results
        ):
            stage2_truth = "假 0"
        else:
            stage2_truth = "上一轮是假 0；当前锁定 case 已非 0"
    lines = [
        "# Planning Truth Validation Report",
        "",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## Summary",
        "",
        "- Current default baseline is Profile A / Case 2 locked conservative startup geometry.",
        "- The baseline is fixed first so bridge no longer injects known-bad startup geometry while we investigate planning observation.",
        f"- Stage-2 verdict on `planning_nonempty=0`: `{stage2_truth}`",
        "",
        "## Case Comparison",
        "",
    ]
    lines.extend(case_lines)
    lines.extend(
        [
            "",
            "## Key Findings",
            "",
            "- Historical false-zero source: before this patch, `planning_reader_enabled` could stay false because Apollo planning pb2 import failed under protobuf>=4 runtime.",
            "- New truth source: `planning_topic_debug.jsonl` / `planning_topic_debug_summary.json` now report raw planning-topic receipt and nonzero trajectory point counts directly.",
            "- In the current locked A/B cases, `success=True` occurs together with substantial nonzero planning trajectories, so the earlier `planning_nonempty=0` conclusion cannot be treated as ground truth anymore.",
            "",
        ]
    )
    return "\n".join(lines)


def _stage2_report(results: List[Dict[str, Any]]) -> str:
    verdict = "部分真部分假"
    if all(item["planning_reader_enabled_effective"] for item in results):
        if all(item["messages_with_nonzero_trajectory_points"] <= 0 for item in results):
            verdict = "真 0"
        elif any(
            item["planning_nonempty_trajectory_count_old_metric"] < item["planning_nonempty_trajectory_count_new_metric"]
            for item in results
        ):
            verdict = "假 0"
        else:
            verdict = "上一轮 planning_nonempty=0 属于假 0；当前锁定 A/B case 已明确观测到非空 trajectory"
    next_layer = "Apollo planning 内部 / route segment / destination"
    if any(not item["planning_reader_enabled_effective"] for item in results):
        next_layer = "topic 订阅 / protobuf 解析链"
    lines = [
        "# Planning Stage2 Report",
        "",
        f"- Current default baseline: `configs/io/examples/followstop_apollo_gt.yaml` == Profile A conservative lock",
        "- Why lock baseline first: avoid bridge-side startup geometry regression while checking planning observation truth.",
        f"- Final verdict on `planning_nonempty=0`: `{verdict}`",
        "",
        "## Evidence",
        "",
        "- Static observer report: `artifacts/planning_observer_static_analysis.md`",
        "- Raw topic truth check: `artifacts/planning_nonempty_truth_check.md`",
        "- Two-case comparison: `artifacts/planning_truth_case_comparison.csv`",
        "",
        "## Next Layer",
        "",
        f"- Most likely next repair layer: `{next_layer}`",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    if not batch_root.exists():
        raise SystemExit(f"batch root not found: {batch_root}")

    run_dirs = _discover_run_dirs(batch_root)
    if not run_dirs:
        raise SystemExit(f"no completed run dirs found under: {batch_root}")

    results = [analyze_run(path) for path in run_dirs]
    results.sort(key=lambda item: (item["case_sort"], item["case_name"]))
    import_diag = _planning_import_diagnostic()
    paths = _analysis_paths(batch_root)

    baseline_lock_md = _current_default_baseline_lock_md()
    static_md = _static_analysis_markdown(import_diag)
    truth_md = _truth_answers(results)
    validation_md = _validation_report(results)
    stage2_md = _stage2_report(results)
    comparison_rows = _comparison_rows(results)

    text_payloads = {
        paths["baseline_lock_root"]: baseline_lock_md,
        paths["baseline_lock_batch"]: baseline_lock_md,
        paths["static_root"]: static_md,
        paths["static_batch"]: static_md,
        paths["truth_check_root"]: truth_md,
        paths["truth_check_batch"]: truth_md,
        paths["validation_root"]: validation_md,
        paths["validation_batch"]: validation_md,
        paths["stage2_root"]: stage2_md,
        paths["stage2_batch"]: stage2_md,
    }
    for path, payload in text_payloads.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload)
    _write_csv(paths["comparison_root"], comparison_rows)
    _write_csv(paths["comparison_batch"], comparison_rows)

    print(f"[planning-truth-analysis] written: {paths['static_root']}")
    print(f"[planning-truth-analysis] written: {paths['comparison_root']}")
    print(f"[planning-truth-analysis] written: {paths['stage2_root']}")


if __name__ == "__main__":
    main()
