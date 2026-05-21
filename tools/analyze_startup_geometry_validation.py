#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]

CASE_DISPLAY = {
    "case_a_baseline": "Case A",
    "case_b_disable_all": "Case B",
    "case_c_back_offset_only": "Case C",
    "case_d_snap_only": "Case D",
    "case_e_snap_nudge_vehicle_yaw": "Case E",
    "case_f_snap_nudge_lane_heading_freeze": "Case F",
}


STATIC_ANALYSIS_ROWS = [
    {
        "parameter": "localization_back_offset_m",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_transform_pose",
        "input": "GT odom pose after carla_to_apollo transform",
        "output": "Apollo localization pose x/y shifted backward along vehicle yaw",
        "trigger_condition": "bridge.localization_back_offset_m != 0.0",
        "expected_positive_effect": "Reduce front-axle/reference-point mismatch and avoid startup projection edge cases",
        "possible_negative_effect": "Can increase localization/reference-line mismatch and move ADC reference behind intended start",
        "current_status": "active",
    },
    {
        "parameter": "snap_start_to_lane",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_maybe_send_routing_request -> _snap_xy_to_lane",
        "input": "Routing start point after localization back offset and map-bounds clamp",
        "output": "Routing start snapped to nearest parsed map segment",
        "trigger_condition": "routing.snap_start_to_lane=true and nearest text-geometry segment available",
        "expected_positive_effect": "May improve routing success and reduce s<0 at startup",
        "possible_negative_effect": "May snap to wrong lane, reverse direction, or non-lane geometry from base_map.txt",
        "current_status": "conditional",
    },
    {
        "parameter": "start_nudge_m",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_maybe_send_routing_request",
        "input": "Routing start after optional snap",
        "output": "Routing start advanced by configured nudge distance before final snap/send",
        "trigger_condition": "routing.start_nudge_m > 0.0",
        "expected_positive_effect": "Push startup waypoint forward to escape tiny-negative-s seams",
        "possible_negative_effect": "Can move route start too far from localization and distort startup geometry",
        "current_status": "conditional",
    },
    {
        "parameter": "start_nudge_retry_step_m",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_maybe_send_routing_request",
        "input": "Current routing_send_count",
        "output": "Adds extra nudge distance on later attempts within the same run",
        "trigger_condition": "routing.start_nudge_m > 0.0 and routing_request_count > 0",
        "expected_positive_effect": "Escape repeated boundary/projection failures across retries",
        "possible_negative_effect": "Retry attempts drift even farther from localization/start lane geometry",
        "current_status": "conditional",
    },
    {
        "parameter": "start_nudge_use_lane_heading",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::_maybe_send_routing_request",
        "input": "First snap candidate lane heading",
        "output": "Chooses lane heading instead of vehicle yaw for startup nudge direction",
        "trigger_condition": "routing.start_nudge_use_lane_heading=true and snap_start_to_lane=true and snap candidate exists",
        "expected_positive_effect": "Keep nudge aligned with snapped lane heading when vehicle yaw is noisy",
        "possible_negative_effect": "If snap source is wrong, nudge follows the wrong lane or wrong direction",
        "current_status": "conditional",
    },
    {
        "parameter": "freeze_after_success",
        "code_path": "tools/apollo10_cyber_bridge/bridge.py::__init__ / _on_routing_response / _maybe_send_routing_request",
        "input": "Routing success state",
        "output": "Suppresses later routing_request sends after first non-empty RoutingResponse",
        "trigger_condition": "routing.freeze_after_success=true and first routing response has road_count > 0",
        "expected_positive_effect": "Prevents startup success from being disturbed by later routing rewrites",
        "possible_negative_effect": "Can block legitimate long-phase route refresh if startup route was bad",
        "current_status": "dead config (pre-patch static); active in validation runs",
    },
]


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        out = float(value)
        return out if math.isfinite(out) else None
    text = str(value).strip()
    if not text:
        return None
    try:
        out = float(text)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _wrap_deg(deg: float) -> float:
    return ((deg + 180.0) % 360.0) - 180.0


def _mean(values: Sequence[float]) -> Optional[float]:
    arr = [float(v) for v in values if math.isfinite(float(v))]
    if not arr:
        return None
    return sum(arr) / float(len(arr))


def _median(values: Sequence[float]) -> Optional[float]:
    arr = sorted(float(v) for v in values if math.isfinite(float(v)))
    if not arr:
        return None
    mid = len(arr) // 2
    if len(arr) % 2 == 1:
        return float(arr[mid])
    return 0.5 * float(arr[mid - 1] + arr[mid])


def _summary_stats(values: Sequence[float]) -> Dict[str, Any]:
    arr = [float(v) for v in values if math.isfinite(float(v))]
    if not arr:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {
        "count": len(arr),
        "min": min(arr),
        "max": max(arr),
        "mean": sum(arr) / float(len(arr)),
    }


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
    rows: List[Dict[str, Any]] = []
    with path.open("r", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            rows.append(dict(row))
    return rows


def _count_pattern(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    text = path.read_text(errors="ignore")
    return len(re.findall(pattern, text))


def _first_valid(values: Iterable[Optional[float]]) -> Optional[float]:
    for value in values:
        if value is not None and math.isfinite(value):
            return float(value)
    return None


def _startup_summary_from_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    loc_to_final = [
        float(item["localization_to_final_start_distance_m"])
        for item in records
        if _safe_float(item.get("localization_to_final_start_distance_m")) is not None
    ]
    raw_to_snap = [
        float(item["raw_start_to_snapped_start_distance_m"])
        for item in records
        if _safe_float(item.get("raw_start_to_snapped_start_distance_m")) is not None
    ]
    heading = [
        abs(float(item["heading_diff_vehicle_vs_snap_lane_deg"]))
        for item in records
        if _safe_float(item.get("heading_diff_vehicle_vs_snap_lane_deg")) is not None
    ]
    phase_counts: Dict[str, int] = {}
    for item in records:
        phase = str(item.get("routing_phase", "") or "")
        phase_counts[phase] = phase_counts.get(phase, 0) + 1
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "routing_request_sent_count": sum(1 for item in records if bool(item.get("routing_request_sent"))),
        "routing_skipped_due_to_freeze_count": sum(
            1 for item in records if bool(item.get("routing_skipped_due_to_freeze"))
        ),
        "suspicious_snap_count": sum(1 for item in records if bool(item.get("suspicious_snap"))),
        "routing_phase_counts": phase_counts,
        "freeze_after_success_config": (
            bool(records[-1].get("freeze_after_success_config")) if records else False
        ),
        "freeze_after_success_effective": (
            bool(records[-1].get("freeze_after_success_effective")) if records else False
        ),
        "distance_stats": {
            "localization_to_final_start_distance_m": _summary_stats(loc_to_final),
            "raw_start_to_snapped_start_distance_m": _summary_stats(raw_to_snap),
            "heading_diff_vehicle_vs_snap_lane_deg_abs": _summary_stats(heading),
        },
        "first_record": dict(records[0]) if records else {},
        "last_record": dict(records[-1]) if records else {},
    }


def _numeric_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in (
            "ts_sec",
            "apollo_desired_steer",
            "commanded_steer",
            "measured_steer_deg",
            "target_front_wheel_angle_deg",
            "speed_mps",
            "map_x",
            "map_y",
            "lane_dist_m",
            "preview_x",
            "preview_y",
        ):
            item[key] = _safe_float(row.get(key))
        out.append(item)
    return out


def _discover_run_dirs(batch_root: Path) -> List[Path]:
    discovered: List[Path] = []
    seen: set[Path] = set()
    for child in sorted(batch_root.iterdir()):
        if not child.is_dir() or child.name == "artifacts":
            continue
        if (child / "RUN_DIR_REDIRECT.txt").exists():
            continue
        has_run_marker = any(
            path.exists()
            for path in (
                child / "summary.json",
                child / "timeseries.csv",
                child / "artifacts" / "startup_geometry_debug.jsonl",
                child / "artifacts" / "cyber_bridge_stats.json",
                child / "artifacts" / "debug_timeseries.csv",
            )
        )
        if not has_run_marker:
            continue
        resolved = child.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        discovered.append(resolved)
    return discovered


def _canonical_case_name(name: str) -> str:
    if "__" in name:
        prefix = name.split("__", 1)[0]
        if prefix in CASE_DISPLAY:
            return prefix
    return name


def _first_startup_record(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    for item in records:
        if bool(item.get("routing_request_sent")):
            return item
    return records[0] if records else {}


def analyze_run(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    summary = _load_json(run_dir / "summary.json")
    effective = _load_yaml(run_dir / "effective.yaml")
    bridge_stats = _load_json(artifacts / "cyber_bridge_stats.json")
    bridge_health = _load_json(artifacts / "bridge_health_summary.json")
    startup_records = _load_jsonl(artifacts / "startup_geometry_debug.jsonl")
    startup_summary = _startup_summary_from_records(startup_records)
    (artifacts / "startup_geometry_summary.json").write_text(
        json.dumps(startup_summary, indent=2, ensure_ascii=False)
    )

    debug_rows = _numeric_rows(_load_csv(artifacts / "debug_timeseries.csv"))
    lateral_rows = _numeric_rows(_load_csv(artifacts / "lateral_geometry_debug.csv"))

    planning_log = artifacts / "apollo_planning.INFO"
    control_log = artifacts / "apollo_control.INFO"
    routing_log = artifacts / "apollo_routing.INFO"

    first_debug_ts = _first_valid(row.get("ts_sec") for row in debug_rows)
    planning_first_ts = _safe_float(
        bridge_health.get("planning_first_nonempty_ts_sec")
        or ((bridge_stats.get("planning") or {}).get("first_nonempty_ts_sec") if isinstance(bridge_stats.get("planning"), dict) else None)
    )
    planning_first_latency = (
        None
        if planning_first_ts is None or first_debug_ts is None
        else float(planning_first_ts - first_debug_ts)
    )

    steering_abs = [abs(row["apollo_desired_steer"]) for row in debug_rows if row.get("apollo_desired_steer") is not None]
    commanded_abs = [abs(row["commanded_steer"]) for row in debug_rows if row.get("commanded_steer") is not None]
    measured_deg_abs = [abs(row["measured_steer_deg"]) for row in debug_rows if row.get("measured_steer_deg") is not None]
    angle_error = [
        abs(float(row["target_front_wheel_angle_deg"]) - float(row["measured_steer_deg"]))
        for row in debug_rows
        if row.get("target_front_wheel_angle_deg") is not None and row.get("measured_steer_deg") is not None
    ]
    saturated = [
        row for row in debug_rows if row.get("apollo_desired_steer") is not None and abs(float(row["apollo_desired_steer"])) >= 0.99
    ]

    first_record = _first_startup_record(startup_records)
    suspicious_snap = any(bool(item.get("suspicious_snap")) for item in startup_records)
    final_start_vs_first_reference_projection_distance_m = None
    final_start_projection_note = "unavailable"
    if first_record and lateral_rows:
        final_start_projection_note = "unavailable"

    map_geometry = bridge_stats.get("map_geometry") or bridge_health.get("map_geometry") or {}
    map_file = str(map_geometry.get("map_file", ""))
    map_text = ""
    if map_file and Path(map_file).exists():
        map_text = Path(map_file).read_text(errors="ignore")
    snap_source_risk = {
        "uses_base_map_text_xy_heuristic": "base_map_text_xy_heuristic" in json.dumps(map_geometry),
        "contains_signal_geometry": bool(re.search(r"\bsignal\s*\{", map_text)),
        "contains_stop_line_geometry": bool(re.search(r"\bstop_line\s*\{", map_text)),
        "contains_overlap_geometry": bool(re.search(r"\boverlap\s*\{", map_text)),
        "high_risk": True,
    }

    result = {
        "case_name": run_dir.name,
        "case_label": CASE_DISPLAY.get(_canonical_case_name(run_dir.name), run_dir.name),
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config": {
            "localization_back_offset_m": (
                (((effective.get("algo") or {}).get("apollo") or {}).get("bridge") or {}).get("localization_back_offset_m")
            ),
            "routing": (((effective.get("algo") or {}).get("apollo") or {}).get("routing") or {}),
        },
        "routing_reference_planning": {
            "routing_request_count": int(bridge_stats.get("routing_request_count", 0) or 0),
            "routing_phase_counts": dict(bridge_health.get("routing_phase_counts", {}) or bridge_stats.get("routing_phase_counts", {}) or {}),
            "planning_nonempty_trajectory_count": int(
                bridge_health.get("planning_nonempty_trajectory_count", 0) or 0
            ),
            "planning_has_trajectory_first_ts": planning_first_ts,
            "planning_first_nonempty_latency_sec": planning_first_latency,
            "planning_empty_or_missing_count": int(
                bridge_health.get("planning_empty_trajectory_count", 0)
                or ((bridge_stats.get("planning") or {}).get("empty_trajectory_count", 0) if isinstance(bridge_stats.get("planning"), dict) else 0)
                or 0
            ),
            "planning_keyword_counts": {
                "path data is empty": _count_pattern(planning_log, r"path data is empty"),
                "reference line": _count_pattern(planning_log, r"reference line"),
                "Fail to aggregate planning trajectory": _count_pattern(planning_log, r"Fail to aggregate planning trajectory"),
                "planner failed to make a driving plan": _count_pattern(planning_log, r"planner failed to make a driving plan"),
            },
            "control_keyword_counts": {
                "planning has no trajectory point": _count_pattern(control_log, r"planning has no trajectory point"),
                "Failed to produce control command": _count_pattern(control_log, r"Failed to produce control command"),
            },
        },
        "lateral_output": {
            "total_frames": len(debug_rows),
            "saturated_steer_frames": len(saturated),
            "saturated_steer_ratio": (
                None if not debug_rows else float(len(saturated) / float(len(debug_rows)))
            ),
            "mean_abs_apollo_desired_steer": _mean(steering_abs),
            "mean_abs_commanded_steer": _mean(commanded_abs),
            "mean_abs_measured_steer_deg": _mean(measured_deg_abs),
            "target_vs_measured_front_wheel_angle_error_deg_mean": _mean(angle_error),
            "target_vs_measured_front_wheel_angle_error_deg_median": _median(angle_error),
        },
        "geometry_consistency": {
            "localization_to_final_start_distance_m": _safe_float(first_record.get("localization_to_final_start_distance_m")),
            "raw_start_to_snapped_start_distance_m": _safe_float(first_record.get("raw_start_to_snapped_start_distance_m")),
            "snapped_start_to_nudged_start_distance_m": _safe_float(first_record.get("snapped_start_to_nudged_start_distance_m")),
            "heading_diff_vehicle_vs_snap_lane_deg": _safe_float(first_record.get("heading_diff_vehicle_vs_snap_lane_deg")),
            "final_start_vs_first_reference_projection_distance_m": final_start_vs_first_reference_projection_distance_m,
            "final_start_vs_first_reference_projection_distance_note": final_start_projection_note,
            "suspicious_snap": suspicious_snap,
        },
        "run_result": {
            "success": bool(summary.get("success", False)),
            "fail_reason": summary.get("fail_reason"),
            "max_speed_mps": _safe_float(summary.get("max_speed_mps") or bridge_health.get("speed_mps_max")),
            "whether_vehicle_moved": bool((_safe_float(summary.get("max_speed_mps")) or 0.0) > 0.5),
            "whether_followstop_completed": bool(summary.get("success", False)),
        },
        "startup_geometry_summary": startup_summary,
        "snap_source_risk": snap_source_risk,
        "artifacts_present": {
            "startup_geometry_debug_jsonl": (artifacts / "startup_geometry_debug.jsonl").exists(),
            "startup_geometry_summary_json": (artifacts / "startup_geometry_summary.json").exists(),
            "apollo_routing_info": routing_log.exists(),
            "apollo_planning_info": planning_log.exists(),
            "apollo_control_info": control_log.exists(),
        },
    }
    return result


def _csv_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in results:
        rr = item["routing_reference_planning"]
        lat = item["lateral_output"]
        geo = item["geometry_consistency"]
        run = item["run_result"]
        planning_kw = rr["planning_keyword_counts"]
        control_kw = rr["control_keyword_counts"]
        rows.append(
            {
                "case": item["case_label"],
                "run_dir": item["run_dir"],
                "routing_request_count": rr["routing_request_count"],
                "routing_phase_counts": json.dumps(rr["routing_phase_counts"], ensure_ascii=False),
                "planning_nonempty_trajectory_count": rr["planning_nonempty_trajectory_count"],
                "planning_has_trajectory_first_ts": rr["planning_has_trajectory_first_ts"],
                "planning_first_nonempty_latency_sec": rr["planning_first_nonempty_latency_sec"],
                "planning_empty_or_missing_count": rr["planning_empty_or_missing_count"],
                "planning_keyword_path_data_is_empty": planning_kw["path data is empty"],
                "planning_keyword_reference_line": planning_kw["reference line"],
                "planning_keyword_fail_to_aggregate_planning_trajectory": planning_kw["Fail to aggregate planning trajectory"],
                "planning_keyword_planner_failed_to_make_a_driving_plan": planning_kw["planner failed to make a driving plan"],
                "control_keyword_planning_has_no_trajectory_point": control_kw["planning has no trajectory point"],
                "control_keyword_failed_to_produce_control_command": control_kw["Failed to produce control command"],
                "total_frames": lat["total_frames"],
                "saturated_steer_frames": lat["saturated_steer_frames"],
                "saturated_steer_ratio": lat["saturated_steer_ratio"],
                "mean_abs_apollo_desired_steer": lat["mean_abs_apollo_desired_steer"],
                "mean_abs_commanded_steer": lat["mean_abs_commanded_steer"],
                "mean_abs_measured_steer_deg": lat["mean_abs_measured_steer_deg"],
                "target_vs_measured_front_wheel_angle_error_deg_mean": lat["target_vs_measured_front_wheel_angle_error_deg_mean"],
                "target_vs_measured_front_wheel_angle_error_deg_median": lat["target_vs_measured_front_wheel_angle_error_deg_median"],
                "localization_to_final_start_distance_m": geo["localization_to_final_start_distance_m"],
                "raw_start_to_snapped_start_distance_m": geo["raw_start_to_snapped_start_distance_m"],
                "snapped_start_to_nudged_start_distance_m": geo["snapped_start_to_nudged_start_distance_m"],
                "heading_diff_vehicle_vs_snap_lane_deg": geo["heading_diff_vehicle_vs_snap_lane_deg"],
                "final_start_vs_first_reference_projection_distance_m": geo["final_start_vs_first_reference_projection_distance_m"],
                "suspicious_snap": geo["suspicious_snap"],
                "success": run["success"],
                "fail_reason": run["fail_reason"],
                "max_speed_mps": run["max_speed_mps"],
                "whether_vehicle_moved": run["whether_vehicle_moved"],
                "whether_followstop_completed": run["whether_followstop_completed"],
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


def _static_analysis_markdown() -> str:
    lines = [
        "## Static Analysis",
        "",
        "| parameter | code_path | input | output | trigger_condition | expected_positive_effect | possible_negative_effect | current_status |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in STATIC_ANALYSIS_ROWS:
        lines.append(
            "| {parameter} | {code_path} | {input} | {output} | {trigger_condition} | {expected_positive_effect} | {possible_negative_effect} | {current_status} |".format(
                **row
            )
        )
    lines.append("")
    lines.append("- Current snap implementation risk: `snap_start_to_lane` is not using Apollo HDMap lane centerline projection.")
    lines.append("- Current data source: `base_map.txt` is parsed as generic x/y text geometry and projected via nearest-segment heuristics.")
    lines.append("- High-risk note: the parser does not filter lane-only geometry, so `signal` / `stop_line` / `overlap` / boundary content can enter the candidate set.")
    lines.append("")
    return "\n".join(lines)


def _case_results_markdown(results: List[Dict[str, Any]]) -> str:
    lines = [
        "## Experiment Results",
        "",
        "| Case | routing_request_count | planning_nonempty | planning_first_latency_sec | saturated_steer_ratio | localization_to_final_start_distance_m | raw_start_to_snapped_start_distance_m | suspicious_snap | success | fail_reason |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for item in results:
        rr = item["routing_reference_planning"]
        lat = item["lateral_output"]
        geo = item["geometry_consistency"]
        run = item["run_result"]
        lines.append(
            f"| {item['case_label']} | "
            f"{rr['routing_request_count']} | "
            f"{rr['planning_nonempty_trajectory_count']} | "
            f"{rr['planning_first_nonempty_latency_sec']} | "
            f"{lat['saturated_steer_ratio']} | "
            f"{geo['localization_to_final_start_distance_m']} | "
            f"{geo['raw_start_to_snapped_start_distance_m']} | "
            f"{geo['suspicious_snap']} | "
            f"{run['success']} | "
            f"{run['fail_reason']} |"
        )
    lines.append("")
    return "\n".join(lines)


def _case_by_label(results: List[Dict[str, Any]], label: str) -> Optional[Dict[str, Any]]:
    for item in results:
        if item["case_label"] == label:
            return item
    return None


def _executive_summary(results: List[Dict[str, Any]]) -> List[str]:
    case_a = _case_by_label(results, "Case A")
    case_b = _case_by_label(results, "Case B")
    suspect_case = None
    suspect_score = None
    for item in results:
        lat = item["lateral_output"]
        rr = item["routing_reference_planning"]
        score = (
            (lat.get("saturated_steer_ratio") or 0.0) * 100.0
            + float(rr.get("planning_empty_or_missing_count") or 0.0)
        )
        if suspect_score is None or score > suspect_score:
            suspect_case = item["case_label"]
            suspect_score = score

    overall_judgement = "insufficient data"
    if case_a and case_b:
        a_rr = case_a["routing_reference_planning"]
        b_rr = case_b["routing_reference_planning"]
        a_lat = case_a["lateral_output"]
        b_lat = case_b["lateral_output"]
        if (
            (a_rr["planning_nonempty_trajectory_count"] or 0) <= (b_rr["planning_nonempty_trajectory_count"] or 0)
            and (a_lat["saturated_steer_ratio"] or 0.0) > (b_lat["saturated_steer_ratio"] or 0.0)
        ):
            overall_judgement = "harmful overall"
        elif (
            (a_rr["planning_nonempty_trajectory_count"] or 0) > (b_rr["planning_nonempty_trajectory_count"] or 0)
            and (a_lat["saturated_steer_ratio"] or 0.0) <= (b_lat["saturated_steer_ratio"] or 0.0)
        ):
            overall_judgement = "helpful overall"
        else:
            overall_judgement = "mixed"

    lines = [
        "## Executive Summary",
        "",
        f"- Overall judgement: `{overall_judgement}`",
        f"- Top suspect combination for persistent lateral saturation: `{suspect_case or 'insufficient data'}`",
        "- Default recommendation now: `freeze_after_success` can be kept as an optional guard after the patch; `snap_start_to_lane` and lane-heading nudge should stay off by default until snap data is rewritten against real Apollo lane centerlines; `localization_back_offset_m` needs case-by-case validation instead of hard default trust.",
        "",
    ]
    return lines


def _positive_negative_markdown(results: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    positives = ["## Positive Effects", ""]
    negatives = ["## Negative Effects", ""]
    case_b = _case_by_label(results, "Case B")
    if case_b is None:
        positives.append("- Insufficient comparison data: Case B missing.")
        negatives.append("- Insufficient comparison data: Case B missing.")
        return positives + [""], negatives + [""]

    base_rr = case_b["routing_reference_planning"]
    base_lat = case_b["lateral_output"]
    base_geo = case_b["geometry_consistency"]

    for item in results:
        if item["case_label"] == "Case B":
            continue
        rr = item["routing_reference_planning"]
        lat = item["lateral_output"]
        geo = item["geometry_consistency"]
        if (rr["planning_nonempty_trajectory_count"] or 0) > (base_rr["planning_nonempty_trajectory_count"] or 0):
            positives.append(
                f"- {item['case_label']} increased `planning_nonempty_trajectory_count` from `{base_rr['planning_nonempty_trajectory_count']}` to `{rr['planning_nonempty_trajectory_count']}`."
            )
        if (
            rr["planning_first_nonempty_latency_sec"] is not None
            and base_rr["planning_first_nonempty_latency_sec"] is not None
            and rr["planning_first_nonempty_latency_sec"] < base_rr["planning_first_nonempty_latency_sec"]
        ):
            positives.append(
                f"- {item['case_label']} reduced `planning_first_nonempty_latency_sec` from `{base_rr['planning_first_nonempty_latency_sec']}` to `{rr['planning_first_nonempty_latency_sec']}`."
            )
        if (
            lat["saturated_steer_ratio"] is not None
            and base_lat["saturated_steer_ratio"] is not None
            and lat["saturated_steer_ratio"] < base_lat["saturated_steer_ratio"]
        ):
            positives.append(
                f"- {item['case_label']} reduced `saturated_steer_ratio` from `{base_lat['saturated_steer_ratio']}` to `{lat['saturated_steer_ratio']}`."
            )
        if (
            geo["raw_start_to_snapped_start_distance_m"] is not None
            and base_geo["raw_start_to_snapped_start_distance_m"] is not None
            and geo["raw_start_to_snapped_start_distance_m"] > base_geo["raw_start_to_snapped_start_distance_m"]
        ):
            negatives.append(
                f"- {item['case_label']} increased `raw_start_to_snapped_start_distance_m` from `{base_geo['raw_start_to_snapped_start_distance_m']}` to `{geo['raw_start_to_snapped_start_distance_m']}`, indicating larger start-point displacement."
            )
        if (
            lat["saturated_steer_ratio"] is not None
            and base_lat["saturated_steer_ratio"] is not None
            and lat["saturated_steer_ratio"] > base_lat["saturated_steer_ratio"]
        ):
            negatives.append(
                f"- {item['case_label']} increased `saturated_steer_ratio` from `{base_lat['saturated_steer_ratio']}` to `{lat['saturated_steer_ratio']}`."
            )
        if bool(geo["suspicious_snap"]):
            negatives.append(
                f"- {item['case_label']} triggered suspicious snap heuristics (`heading_diff_vehicle_vs_snap_lane_deg > 90` or `raw_start_to_snapped_start_distance_m > 2.5`)."
            )

    if len(positives) == 2:
        positives.append("- No data-backed positive deltas were found against Case B.")
    if len(negatives) == 2:
        negatives.append("- No data-backed negative deltas were found against Case B.")
    positives.append("")
    negatives.append("")
    return positives, negatives


def _recommendations_markdown(results: List[Dict[str, Any]]) -> List[str]:
    lines = [
        "## Recommendations",
        "",
        "- Final recommended config: `localization_back_offset_m=0.0`, `snap_start_to_lane=false`, `start_nudge_m=0.0`, `start_nudge_retry_step_m=0.0`, `start_nudge_use_lane_heading=false`, and keep `freeze_after_success=true` only if startup routing is already validated.",
        "- Most conservative stable config: equivalent to Case B plus optional `freeze_after_success=true` after a confirmed good startup route.",
        "- If `snap_start_to_lane` must be kept in the future, rewrite it against Apollo HDMap lane centerline APIs or prefiltered lane-center geometry; do not keep the current base_map text nearest-segment heuristic.",
        "- High-risk implementation note: the current snap source is not Apollo lane centerline projection. It parses `base_map.txt` as generic x/y geometry and can mix signal/stop-line/overlap/boundary content into the nearest-segment candidate set.",
        "",
    ]
    return lines


def _reproduction_commands_markdown(batch_root: Path) -> List[str]:
    lines = [
        "## Reproduction Commands",
        "",
        "```bash",
        "conda run -n carla16 python -m carla_testbed run --help",
        (
            "conda run -n carla16 python tools/run_startup_geometry_validation.py "
            f"--apollo-root <APOLLO_ROOT> --docker-container <APOLLO_CONTAINER> --batch-root {batch_root}"
        ),
        (
            "conda run -n carla16 python tools/run_startup_geometry_validation.py "
            f"--batch-root {batch_root} --analyze-only"
        ),
        "```",
        "",
    ]
    return lines


def _report_markdown(batch_root: Path, results: List[Dict[str, Any]]) -> str:
    lines: List[str] = [
        "# Apollo Startup Geometry Parameter Validation Report",
        "",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        f"- batch_root: `{batch_root}`",
        "",
    ]
    lines.extend(_executive_summary(results))
    lines.append(_static_analysis_markdown())
    lines.append(_case_results_markdown(results))
    positives, negatives = _positive_negative_markdown(results)
    lines.extend(positives)
    lines.extend(negatives)
    lines.extend(_recommendations_markdown(results))
    lines.extend(_reproduction_commands_markdown(batch_root))
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Apollo startup geometry validation runs")
    parser.add_argument("--batch-root", required=True, help="Batch root produced by run_startup_geometry_validation.py")
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    if not batch_root.exists():
        raise SystemExit(f"batch root not found: {batch_root}")

    run_dirs = _discover_run_dirs(batch_root)
    if not run_dirs:
        raise SystemExit(f"no run directories found under: {batch_root}")

    artifacts_dir = batch_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    results = [analyze_run(run_dir) for run_dir in run_dirs]
    results.sort(key=lambda item: item["case_label"])

    comparison_rows = _csv_rows(results)
    comparison_csv = artifacts_dir / "startup_geometry_case_comparison.csv"
    _write_csv(comparison_csv, comparison_rows)

    comparison_json = artifacts_dir / "startup_geometry_case_comparison.json"
    comparison_json.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    report_path = artifacts_dir / "startup_geometry_param_validation_report.md"
    report_path.write_text(_report_markdown(batch_root, results))

    print(f"[startup-geometry-analysis] written: {comparison_csv}")
    print(f"[startup-geometry-analysis] written: {comparison_json}")
    print(f"[startup-geometry-analysis] written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
