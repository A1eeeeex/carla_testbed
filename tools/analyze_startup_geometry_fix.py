#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS = REPO_ROOT / "artifacts"

CASE_LABELS = {
    "case_1_baseline_legacy": "Case 1",
    "case_2_patched_conservative": "Case 2",
    "case_3_patched_snap_gate_lane_centerline": "Case 3",
    "case_4_patched_snap_vehicle_yaw_nudge": "Case 4",
}

CASE_ORDER = {name: idx for idx, name in enumerate(CASE_LABELS, start=1)}

STATIC_ANALYSIS_ROWS = [
    {
        "topic": "localization_back_offset_m",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_transform_pose",
        "conclusion": "Apollo localization x/y is shifted backward along vehicle yaw before publish.",
    },
    {
        "topic": "snap_start_to_lane",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_maybe_send_routing_request -> _evaluate_snap_candidate",
        "conclusion": "Routing start snap now runs through candidate availability, trusted-source check, and heading gate before acceptance.",
    },
    {
        "topic": "_snap_xy_to_lane data source",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_load_map_geometry / _extract_lane_centerline_polylines",
        "conclusion": "Default snap source is routing_map/sim_map central_curve only; legacy base_map full-text x/y extraction is retained only behind explicit legacy mode.",
    },
    {
        "topic": "start_nudge_m / retry / use_lane_heading",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_maybe_send_routing_request",
        "conclusion": "Lane-heading nudge is allowed only after accepted snap with heading diff <= 20 deg; rejected snap blocks conservative nudge by default.",
    },
    {
        "topic": "freeze_after_success",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_on_routing_response / _maybe_send_routing_request",
        "conclusion": "Already active: first non-empty RoutingResponse enables routing-request freeze when configured.",
    },
]


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _mean(values: Sequence[float]) -> Optional[float]:
    finite = [float(v) for v in values if math.isfinite(float(v))]
    if not finite:
        return None
    return sum(finite) / float(len(finite))


def _max(values: Sequence[float]) -> Optional[float]:
    finite = [float(v) for v in values if math.isfinite(float(v))]
    return max(finite) if finite else None


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text()) or {}
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


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="") as fp:
        return list(csv.DictReader(fp))


def _count_pattern(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    return len(re.findall(pattern, path.read_text(errors="ignore")))


def _discover_run_dirs(batch_root: Path) -> List[Path]:
    discovered: Dict[str, Path] = {}
    for child in sorted(batch_root.iterdir()):
        if not child.is_dir() or child.name == "artifacts":
            continue
        if (child / "RUN_DIR_REDIRECT.txt").exists():
            continue
        if not (
            (child / "summary.json").exists()
            or (child / "timeseries.csv").exists()
            or (child / "artifacts" / "startup_geometry_debug.jsonl").exists()
        ):
            continue
        resolved = child.resolve()
        canonical = _canonical_case_name(resolved.name)
        current = discovered.get(canonical)
        if current is None or resolved.name > current.name:
            discovered[canonical] = resolved
    return list(discovered.values())


def _canonical_case_name(name: str) -> str:
    return re.sub(r"__\d+$", "", name)


def _analysis_paths(batch_root: Path) -> Dict[str, Path]:
    batch_artifacts = batch_root / "artifacts"
    return {
        "static_md_root": ROOT_ARTIFACTS / "startup_geometry_fix_static_analysis.md",
        "comparison_csv_root": ROOT_ARTIFACTS / "startup_geometry_fix_case_comparison.csv",
        "comparison_json_root": ROOT_ARTIFACTS / "startup_geometry_fix_case_comparison.json",
        "report_md_root": ROOT_ARTIFACTS / "startup_geometry_fix_report.md",
        "static_md_batch": batch_artifacts / "startup_geometry_fix_static_analysis.md",
        "comparison_csv_batch": batch_artifacts / "startup_geometry_fix_case_comparison.csv",
        "comparison_json_batch": batch_artifacts / "startup_geometry_fix_case_comparison.json",
        "report_md_batch": batch_artifacts / "startup_geometry_fix_report.md",
    }


def _static_analysis_markdown() -> str:
    lines = [
        "# Startup Geometry Fix Static Analysis",
        "",
        "| topic | code_path | conclusion |",
        "|---|---|---|",
    ]
    for row in STATIC_ANALYSIS_ROWS:
        lines.append(f"| {row['topic']} | {row['code_path']} | {row['conclusion']} |")
    lines.extend(
        [
            "",
            "## Risk Conclusions",
            "",
            "- Current default snap no longer parses `base_map.txt` as full-text x/y for production snap decisions.",
            "- Legacy `base_map.txt` nearest-segment snap is still available only for experiment reproduction via `snap_source_mode=legacy_base_map_xy`.",
            "- Default snap path uses `routing_map.txt` / `sim_map.txt` `central_curve` only, so `signal` / `stop_line` / `overlap` / boundary geometry is excluded from snap candidates.",
            "- Current bridge now has heading consistency gates:",
            "  - accept snap only when `heading_diff_to_vehicle_deg <= 30`",
            "  - hard reject and mark suspicious when `heading_diff_to_vehicle_deg > 45`",
            "  - lane-heading nudge allowed only when accepted snap also satisfies `heading_diff_to_vehicle_deg <= 20`",
            "- `freeze_after_success` is already a live code path and is not the primary startup-geometry defect addressed here.",
            "",
        ]
    )
    return "\n".join(lines)


def analyze_run(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    summary = _load_json(run_dir / "summary.json")
    effective = _load_yaml(run_dir / "effective.yaml")
    bridge_stats = _load_json(artifacts / "cyber_bridge_stats.json")
    bridge_health = _load_json(artifacts / "bridge_health_summary.json")
    startup_records = _load_jsonl(artifacts / "startup_geometry_debug.jsonl")
    debug_rows = _load_csv(artifacts / "debug_timeseries.csv")
    case_name = _canonical_case_name(run_dir.name)

    planning_info = artifacts / "apollo_planning.INFO"
    control_info = artifacts / "apollo_control.INFO"

    saturated_frames = 0
    total_frames = 0
    desired_abs: List[float] = []
    for row in debug_rows:
        steer = _safe_float(row.get("apollo_desired_steer"))
        if steer is None:
            continue
        total_frames += 1
        desired_abs.append(abs(steer))
        if abs(steer) >= 0.99:
            saturated_frames += 1

    accepted_heading = [
        abs(float(row["heading_diff_to_vehicle_deg"]))
        for row in startup_records
        if bool(row.get("snap_applied")) and _safe_float(row.get("heading_diff_to_vehicle_deg")) is not None
    ]
    loc_dist = [
        float(row["localization_to_final_start_distance_m"])
        for row in startup_records
        if _safe_float(row.get("localization_to_final_start_distance_m")) is not None
    ]
    suspicious_rows = 0
    for row in startup_records:
        heading_diff = _safe_float(row.get("heading_diff_to_vehicle_deg"))
        if bool(row.get("suspicious_snap")) or (heading_diff is not None and abs(heading_diff) > 45.0):
            suspicious_rows += 1
    startup_summary = {
        "suspicious_snap_count": suspicious_rows,
        "snap_rejected_count": sum(1 for row in startup_records if bool(row.get("snap_rejected"))),
        "mean_heading_diff_to_vehicle_deg_for_accepted_snap": _mean(accepted_heading),
        "max_heading_diff_to_vehicle_deg_for_accepted_snap": _max(accepted_heading),
        "mean_localization_to_final_start_distance_m": _mean(loc_dist),
        "max_localization_to_final_start_distance_m": _max(loc_dist),
    }

    planning_first_ts = _safe_float(
        bridge_health.get("planning_first_nonempty_ts_sec")
        or ((bridge_stats.get("planning") or {}).get("first_nonempty_ts_sec") if isinstance(bridge_stats.get("planning"), dict) else None)
    )

    keywords = {
        "path_data_is_empty": _count_pattern(planning_info, r"path data is empty"),
        "reference_line": _count_pattern(planning_info, r"reference line"),
        "fail_to_aggregate_planning_trajectory": _count_pattern(planning_info, r"Fail to aggregate planning trajectory"),
        "planning_has_no_trajectory_point": _count_pattern(control_info, r"planning has no trajectory point"),
    }

    return {
        "case_name": run_dir.name,
        "case_label": CASE_LABELS.get(case_name, case_name),
        "case_sort": CASE_ORDER.get(case_name, 999),
        "run_dir": str(run_dir),
        "config": {
            "localization_back_offset_m": (((effective.get("algo") or {}).get("apollo") or {}).get("bridge") or {}).get(
                "localization_back_offset_m"
            ),
            "routing": (((effective.get("algo") or {}).get("apollo") or {}).get("routing") or {}),
        },
        "routing_planning": {
            "routing_request_count": int(bridge_stats.get("routing_request_count", 0) or 0),
            "planning_nonempty_trajectory_count": int(
                bridge_health.get("planning_nonempty_trajectory_count", 0) or 0
            ),
            "planning_first_nonempty_latency_sec": planning_first_ts,
            "path_data_is_empty_count": keywords["path_data_is_empty"],
            "reference_line_error_count": keywords["reference_line"],
            "fail_to_aggregate_planning_trajectory_count": keywords["fail_to_aggregate_planning_trajectory"],
            "planning_has_no_trajectory_point_count": keywords["planning_has_no_trajectory_point"],
        },
        "lateral_output": {
            "total_frames": total_frames,
            "saturated_steer_frames": saturated_frames,
            "saturated_steer_ratio": None if total_frames <= 0 else float(saturated_frames) / float(total_frames),
            "mean_abs_apollo_desired_steer": _mean(desired_abs),
        },
        "startup_geometry": startup_summary,
        "run_result": {
            "success": bool(summary.get("success", False)),
            "fail_reason": summary.get("fail_reason"),
            "max_speed_mps": _safe_float(summary.get("max_speed_mps") or bridge_health.get("speed_mps_max")),
            "whether_vehicle_moved": bool((_safe_float(summary.get("max_speed_mps") or bridge_health.get("speed_mps_max")) or 0.0) > 0.5),
        },
        "map_geometry": dict(bridge_stats.get("map_geometry", {}) or {}),
    }


def _csv_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in results:
        rr = item["routing_planning"]
        lat = item["lateral_output"]
        geo = item["startup_geometry"]
        run = item["run_result"]
        rows.append(
            {
                "case": item["case_label"],
                "run_dir": item["run_dir"],
                "routing_request_count": rr["routing_request_count"],
                "planning_nonempty_trajectory_count": rr["planning_nonempty_trajectory_count"],
                "planning_first_nonempty_latency_sec": rr["planning_first_nonempty_latency_sec"],
                "path_data_is_empty_count": rr["path_data_is_empty_count"],
                "reference_line_error_count": rr["reference_line_error_count"],
                "fail_to_aggregate_planning_trajectory_count": rr["fail_to_aggregate_planning_trajectory_count"],
                "planning_has_no_trajectory_point_count": rr["planning_has_no_trajectory_point_count"],
                "total_frames": lat["total_frames"],
                "saturated_steer_frames": lat["saturated_steer_frames"],
                "saturated_steer_ratio": lat["saturated_steer_ratio"],
                "mean_abs_apollo_desired_steer": lat["mean_abs_apollo_desired_steer"],
                "suspicious_snap_count": geo["suspicious_snap_count"],
                "snap_rejected_count": geo["snap_rejected_count"],
                "mean_heading_diff_to_vehicle_deg_for_accepted_snap": geo["mean_heading_diff_to_vehicle_deg_for_accepted_snap"],
                "max_heading_diff_to_vehicle_deg_for_accepted_snap": geo["max_heading_diff_to_vehicle_deg_for_accepted_snap"],
                "mean_localization_to_final_start_distance_m": geo["mean_localization_to_final_start_distance_m"],
                "max_localization_to_final_start_distance_m": geo["max_localization_to_final_start_distance_m"],
                "success": run["success"],
                "fail_reason": run["fail_reason"],
                "max_speed_mps": run["max_speed_mps"],
                "whether_vehicle_moved": run["whether_vehicle_moved"],
            }
        )
    return rows


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


def _baseline(results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for item in results:
        if item["case_label"] == "Case 1":
            return item
    return None


def _criterion(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {"criterion": name, "passed": passed, "detail": detail}


def _acceptance_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    base = _baseline(results)
    if base is None:
        return []
    base_rr = base["routing_planning"]
    base_lat = base["lateral_output"]
    base_geo = base["startup_geometry"]
    out: List[Dict[str, Any]] = []
    patched = [item for item in results if item["case_label"] != "Case 1"]
    accepted_heading_ok = all(
        (
            item["startup_geometry"]["max_heading_diff_to_vehicle_deg_for_accepted_snap"] is None
            or item["startup_geometry"]["max_heading_diff_to_vehicle_deg_for_accepted_snap"] <= 45.0
        )
        for item in patched
    )
    out.append(
        _criterion(
            "accepted snap heading <= 45 deg",
            accepted_heading_ok,
            "All patched cases keep accepted snap heading diff within 45 deg or do not accept snap.",
        )
    )
    suspicious_improved = any(
        (item["startup_geometry"]["suspicious_snap_count"] or 0) < (base_geo["suspicious_snap_count"] or 0)
        for item in patched
    )
    out.append(
        _criterion(
            "suspicious_snap_count decreased",
            suspicious_improved,
            f"Baseline={base_geo['suspicious_snap_count']}; patched min={min((item['startup_geometry']['suspicious_snap_count'] or 0) for item in patched)}",
        )
    )
    steer_improved = any(
        item["lateral_output"]["saturated_steer_ratio"] is not None
        and base_lat["saturated_steer_ratio"] is not None
        and item["lateral_output"]["saturated_steer_ratio"] < base_lat["saturated_steer_ratio"]
        for item in patched
    )
    out.append(
        _criterion(
            "saturated_steer_ratio decreased",
            steer_improved,
            f"Baseline={base_lat['saturated_steer_ratio']}",
        )
    )
    loc_improved = any(
        item["startup_geometry"]["mean_localization_to_final_start_distance_m"] is not None
        and base_geo["mean_localization_to_final_start_distance_m"] is not None
        and item["startup_geometry"]["mean_localization_to_final_start_distance_m"] < base_geo["mean_localization_to_final_start_distance_m"]
        for item in patched
    )
    out.append(
        _criterion(
            "mean localization->final_start distance decreased",
            loc_improved,
            f"Baseline={base_geo['mean_localization_to_final_start_distance_m']}",
        )
    )
    planning_not_worse = all(
        (item["routing_planning"]["planning_nonempty_trajectory_count"] or 0)
        >= (base_rr["planning_nonempty_trajectory_count"] or 0)
        for item in patched
    )
    out.append(
        _criterion(
            "planning_nonempty_trajectory_count not below baseline",
            planning_not_worse,
            f"Baseline={base_rr['planning_nonempty_trajectory_count']}",
        )
    )
    empty_errors_improved = any(
        (
            (item["routing_planning"]["path_data_is_empty_count"] or 0)
            + (item["routing_planning"]["reference_line_error_count"] or 0)
            + (item["routing_planning"]["fail_to_aggregate_planning_trajectory_count"] or 0)
            + (item["routing_planning"]["planning_has_no_trajectory_point_count"] or 0)
        )
        < (
            (base_rr["path_data_is_empty_count"] or 0)
            + (base_rr["reference_line_error_count"] or 0)
            + (base_rr["fail_to_aggregate_planning_trajectory_count"] or 0)
            + (base_rr["planning_has_no_trajectory_point_count"] or 0)
        )
        for item in patched
    )
    out.append(
        _criterion(
            "planning/control empty-trajectory errors decreased",
            empty_errors_improved,
            "Compared by aggregate count of path-empty/reference-line/fail-to-aggregate/no-trajectory-point errors.",
        )
    )
    regression_cases = [
        item["case_label"]
        for item in patched
        if (item["routing_planning"]["routing_request_count"] or 0) <= 0
        or bool(item["run_result"].get("fail_reason"))
    ]
    no_regression = len(regression_cases) == 0
    out.append(
        _criterion(
            "no obvious regression in patched set",
            no_regression,
            "Regression cases: " + (", ".join(regression_cases) if regression_cases else "none"),
        )
    )
    return out


def _results_table(results: List[Dict[str, Any]]) -> List[str]:
    lines = [
        "| Case | routing_request_count | planning_nonempty | path_data_is_empty | reference_line | no_trajectory_point | saturated_steer_ratio | suspicious_snap_count | snap_rejected_count | mean_loc_to_final_start_m | success | fail_reason |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for item in results:
        rr = item["routing_planning"]
        lat = item["lateral_output"]
        geo = item["startup_geometry"]
        run = item["run_result"]
        lines.append(
            f"| {item['case_label']} | "
            f"{rr['routing_request_count']} | "
            f"{rr['planning_nonempty_trajectory_count']} | "
            f"{rr['path_data_is_empty_count']} | "
            f"{rr['reference_line_error_count']} | "
            f"{rr['planning_has_no_trajectory_point_count']} | "
            f"{lat['saturated_steer_ratio']} | "
            f"{geo['suspicious_snap_count']} | "
            f"{geo['snap_rejected_count']} | "
            f"{geo['mean_localization_to_final_start_distance_m']} | "
            f"{run['success']} | "
            f"{run['fail_reason']} |"
        )
    return lines


def _report_markdown(batch_root: Path, results: List[Dict[str, Any]], acceptance: List[Dict[str, Any]]) -> str:
    base = _baseline(results)
    remaining = "planning 侧仍然长期拿不到 non-empty trajectory"
    recommended = (
        "`localization_back_offset_m=0.0`, `snap_start_to_lane=false`, `start_nudge_m=0.0`, "
        "`start_nudge_retry_step_m=0.0`, `start_nudge_use_lane_heading=false`, "
        "only re-enable snap on trusted lane-centerline source with heading gate."
    )
    lines = [
        "# Startup Geometry Fix Report",
        "",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        f"- batch_root: `{batch_root}`",
        "",
        "## Root Cause",
        "",
        "- The defect was not actuator mapping or visual lane lines.",
        "- The bridge could snap routing start onto an incorrect map segment whose heading was nearly orthogonal to the vehicle heading.",
        "- The old path then allowed lane-heading nudge to push routing start laterally away from localization, further damaging localization / route start / reference-line consistency.",
        "",
        "## What Changed",
        "",
        "- Snap source was moved to `routing_map.txt` / `sim_map.txt` `central_curve` lane-centerline extraction by default.",
        "- Legacy `base_map.txt` full-text x/y nearest-segment snap was kept only as an explicit experiment mode for reproducing the bad baseline.",
        "- Snap now requires candidate availability, trusted lane-centerline source, and `heading_diff_to_vehicle_deg <= 30`.",
        "- `heading_diff_to_vehicle_deg > 45` now hard-rejects snap and records `suspicious_snap_rejected`.",
        "- Lane-heading nudge is allowed only when accepted snap also satisfies `heading_diff_to_vehicle_deg <= 20`; otherwise conservative behavior falls back to vehicle yaw or disables nudge when snap was rejected.",
        "- `startup_geometry_debug.jsonl` now records source type, accept/reject reason, suspicious rejection, and final localization-to-start distance.",
        "",
        "## Why This Blocks The Bad Path",
        "",
        "- Wrong-heading snap no longer survives into routing-start construction.",
        "- Untrusted map geometry can no longer silently drive snap acceptance in default mode.",
        "- Lane-heading nudge is no longer allowed to amplify a wrong snap candidate.",
        "",
        "## Case Results",
        "",
    ]
    lines.extend(_results_table(results))
    lines.extend(
        [
            "",
            "## Acceptance Criteria",
            "",
            "| Criterion | Passed | Detail |",
            "|---|---|---|",
        ]
    )
    for item in acceptance:
        lines.append(f"| {item['criterion']} | {item['passed']} | {item['detail']} |")
    passed_count = sum(1 for item in acceptance if item["passed"])
    lines.extend(
        [
            "",
            f"- acceptance_pass_count: `{passed_count}` / `{len(acceptance)}`",
            "",
            "## Recommendation",
            "",
            f"- Recommended default config: {recommended}",
            "- Recommended retest entry: `configs/io/examples/followstop_apollo_gt.yaml` after this patch.",
            f"- Remaining first suspect if startup geometry is no longer wrong: {remaining}.",
        ]
    )
    if base is not None:
        lines.extend(
            [
                "",
                "## Baseline Reference",
                "",
                f"- Baseline case: `{base['case_label']}` at `{base['run_dir']}`",
            ]
        )
    lines.append("")
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
    acceptance = _acceptance_results(results)
    paths = _analysis_paths(batch_root)

    static_md = _static_analysis_markdown()
    report_md = _report_markdown(batch_root, results, acceptance)
    csv_rows = _csv_rows(results)
    results_json = json.dumps(results, indent=2, ensure_ascii=False)

    for key in ("static_md_root", "static_md_batch"):
        paths[key].parent.mkdir(parents=True, exist_ok=True)
        paths[key].write_text(static_md)
    for key in ("comparison_csv_root", "comparison_csv_batch"):
        _write_csv(paths[key], csv_rows)
    for key in ("comparison_json_root", "comparison_json_batch"):
        paths[key].parent.mkdir(parents=True, exist_ok=True)
        paths[key].write_text(results_json)
    for key in ("report_md_root", "report_md_batch"):
        paths[key].parent.mkdir(parents=True, exist_ok=True)
        paths[key].write_text(report_md)

    print(f"[startup-geometry-fix-analysis] written: {paths['static_md_root']}")
    print(f"[startup-geometry-fix-analysis] written: {paths['comparison_csv_root']}")
    print(f"[startup-geometry-fix-analysis] written: {paths['report_md_root']}")


if __name__ == "__main__":
    main()
