#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

from analyze_stage3_planning_control_rootcause import (
    CASE_LABELS,
    CASE_ORDER,
    _count_pattern,
    _discover_run_dirs,
    _load_json,
    _load_jsonl,
    _load_yaml,
    _safe_float,
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
        if not ((child / "summary.json").exists() or (child / "artifacts").exists()):
            continue
        current = discovered.get(canonical)
        child_has_summary = (child / "summary.json").exists()
        current_has_summary = (current / "summary.json").exists() if current is not None else False
        if current is None:
            discovered[canonical] = child.resolve()
            continue
        if child_has_summary and (not current_has_summary):
            discovered[canonical] = child.resolve()
            continue
        if child_has_summary == current_has_summary and child.name > current.name:
            discovered[canonical] = child.resolve()
    return list(discovered.values())


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _copy_to_root(name: str, text: str) -> None:
    _write_text(ROOT_ARTIFACTS / name, text)


def _copy_json_to_root(name: str, payload: Any) -> None:
    _write_json(ROOT_ARTIFACTS / name, payload)


def _copy_csv_to_root(name: str, rows: List[Dict[str, Any]]) -> None:
    _write_csv(ROOT_ARTIFACTS / name, rows)


def _load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open() as fp:
        return list(csv.DictReader(fp))


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    finite = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    if not finite:
        return None
    return sum(finite) / len(finite)


def _max(values: Iterable[Optional[float]]) -> Optional[float]:
    finite = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    if not finite:
        return None
    return max(finite)


def _stdev(values: Iterable[Optional[float]]) -> Optional[float]:
    finite = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    if len(finite) < 2:
        return None
    return statistics.pstdev(finite)


def _json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _lane_stability(planning_rows: List[Dict[str, Any]], lateral_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    lane_ids: List[str] = []
    for row in planning_rows:
        if int(row.get("trajectory_point_count", 0) or 0) <= 0:
            continue
        lane = row.get("lane_id_first") or row.get("target_lane_id_first")
        if lane:
            lane_ids.append(str(lane))
    switches = 0
    for prev, cur in zip(lane_ids, lane_ids[1:]):
        if prev != cur:
            switches += 1
    dominant_lane = Counter(lane_ids).most_common(1)[0][0] if lane_ids else None
    dominant_ratio = (
        Counter(lane_ids).most_common(1)[0][1] / float(len(lane_ids))
        if lane_ids
        else None
    )
    headings = [_safe_float(row.get("preview_heading_deg")) for row in lateral_rows]
    return {
        "lane_sample_count": len(lane_ids),
        "lane_switches": switches,
        "dominant_lane_id": dominant_lane,
        "dominant_lane_ratio": dominant_ratio,
        "preview_heading_std_deg": _stdev(headings),
    }


def _projection_jump_summary(lateral_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    jump_events = 0
    yaw_jump_events = 0
    prev: Optional[Tuple[float, float, float, float]] = None
    for row in lateral_rows:
        ts = _safe_float(row.get("ts_sec"))
        px = _safe_float(row.get("preview_x"))
        py = _safe_float(row.get("preview_y"))
        ey = _safe_float(row.get("e_y_m"))
        epsi = _safe_float(row.get("e_psi_deg"))
        if None in (ts, px, py):
            continue
        if prev is not None:
            dist = math.hypot(px - prev[1], py - prev[2])
            if dist > 3.0:
                jump_events += 1
            if epsi is not None and prev[3] is not None and abs(epsi - prev[3]) > 15.0:
                yaw_jump_events += 1
        prev = (ts, px, py, epsi if epsi is not None else float("nan"))
    ts_values = [_safe_float(row.get("ts_sec")) for row in lateral_rows]
    monotonic = all(
        ts_values[idx] is not None
        and ts_values[idx - 1] is not None
        and float(ts_values[idx]) >= float(ts_values[idx - 1])
        for idx in range(1, len(ts_values))
    )
    return {
        "timestamp_monotonic": monotonic,
        "lane_projection_jump_events": jump_events,
        "yaw_jump_events": yaw_jump_events,
    }


def _vehicle_state_summary(control_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    success_rows = [row for row in control_events if row.get("event_type") == "control_success"]
    drive_modes = Counter(str(row.get("drive_mode") or "") for row in success_rows if row.get("drive_mode") not in (None, ""))
    gears = Counter(str(row.get("gear") or "") for row in success_rows if row.get("gear") not in (None, ""))
    auto_modes = Counter(str(row.get("auto_drive_mode") or "") for row in success_rows if row.get("auto_drive_mode") not in (None, ""))
    planning_nonzero_mode_not_ready = 0
    for row in success_rows:
        if int(row.get("latest_planning_trajectory_point_count", 0) or 0) <= 0:
            continue
        text = " ".join(
            [
                str(row.get("auto_drive_mode") or ""),
                str(row.get("engage_state") or ""),
                str(row.get("drive_mode") or ""),
            ]
        ).lower()
        if any(token in text for token in ("manual", "disallow", "not_ready", "not ready")):
            planning_nonzero_mode_not_ready += 1
    return {
        "auto_drive_mode_timeline": dict(auto_modes),
        "drive_mode_timeline": dict(drive_modes),
        "gear_timeline": dict(gears),
        "planning_nonzero_mode_not_ready_cycles": planning_nonzero_mode_not_ready,
    }


def _obstacle_summary(obstacle_rows: List[Dict[str, Any]], control_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    found_rows = [row for row in obstacle_rows if bool(row.get("front_obstacle_actor_found"))]
    actor_ids = [int(row["front_obstacle_actor_id"]) for row in found_rows if row.get("front_obstacle_actor_id") is not None]
    id_switches = 0
    for prev, cur in zip(actor_ids, actor_ids[1:]):
        if prev != cur:
            id_switches += 1
    speed_vals = [_safe_float(row.get("front_obstacle_actor_speed_mps")) for row in found_rows]
    gap_vals = [_safe_float(row.get("front_obstacle_gap_distance_m")) for row in obstacle_rows]
    anomaly_near_fail = 0
    miss_ts = [float(row["timestamp"]) for row in control_events if row.get("event_type") == "control_no_trajectory" and row.get("timestamp") is not None]
    for row in obstacle_rows:
        ts = _safe_float(row.get("timestamp"))
        if ts is None:
            continue
        near_fail = any(abs(ts - event_ts) <= 0.3 for event_ts in miss_ts)
        if not near_fail:
            continue
        if (not bool(row.get("front_obstacle_actor_found"))) or _safe_float(row.get("front_obstacle_gap_distance_m")) is None:
            anomaly_near_fail += 1
    return {
        "actor_found_count": len(found_rows),
        "dominant_obstacle_id": Counter(actor_ids).most_common(1)[0][0] if actor_ids else None,
        "obstacle_id_switches": id_switches,
        "obstacle_speed_mean_mps": _mean(speed_vals),
        "obstacle_gap_mean_m": _mean(gap_vals),
        "obstacle_anomaly_near_failure_events": anomaly_near_fail,
    }


def _planning_age_stats(planning_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    ages_ms: List[float] = []
    for row in planning_rows:
        row_ts = _safe_float(row.get("timestamp"))
        header_ts = _safe_float(row.get("planning_header_timestamp_sec"))
        if row_ts is None or header_ts is None:
            continue
        ages_ms.append((row_ts - header_ts) * 1000.0)
    return {
        "planning_age_mean_ms": _mean(ages_ms),
        "planning_age_max_ms": _max(ages_ms),
        "planning_age_negative_count": sum(1 for value in ages_ms if value < 0.0),
    }


def _map_check(case_results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    lines = ["# Input Contract Map Check", ""]
    mismatch_detected = False
    first_suspect = ""
    status = {}
    for item in case_results:
        artifacts = Path(item["run_dir"]) / "artifacts"
        bridge_cfg = _load_yaml(artifacts / "apollo_bridge_effective.yaml")
        bridge_root = bridge_cfg.get("bridge", {}) or {}
        auto_routing = bridge_root.get("auto_routing", {}) or {}
        startup_summary = _load_json(artifacts / "startup_geometry_summary.json")
        map_geometry = startup_summary.get("map_geometry", {}) or {}
        planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
        lateral_rows = _load_csv_rows(artifacts / "lateral_geometry_debug.csv")
        lane_summary = _lane_stability(planning_rows, lateral_rows)
        bridge_map_file = str(bridge_root.get("map_file", "") or "")
        apollo_runtime_map_dir = str(bridge_root.get("apollo_runtime_map_dir", "") or "")
        case_mismatch = bool(
            bridge_map_file
            and apollo_runtime_map_dir
            and Path(bridge_map_file).resolve().parent != Path(apollo_runtime_map_dir).resolve()
        )
        mismatch_detected = mismatch_detected or case_mismatch
        if case_mismatch and not first_suspect:
            first_suspect = "bridge auxiliary map path differs from Apollo runtime map_dir"
        status[item["case_label"]] = {
            "official_map_source": apollo_runtime_map_dir or "not_captured_directly",
            "bridge_side_map_usage": {
                "map_file": bridge_map_file,
                "snap_source_mode": auto_routing.get("snap_source_mode"),
                "map_geometry_source_type": map_geometry.get("source_type"),
                "map_geometry_source_file": map_geometry.get("map_file"),
            },
            "mismatch_detected": case_mismatch,
            "lane_stability_summary": lane_summary,
        }
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- official map source: `{apollo_runtime_map_dir or 'not_captured_directly'}`",
                f"- bridge-side map usage: `map_file={bridge_map_file}`, `source_type={map_geometry.get('source_type')}`",
                f"- mismatch_detected: `{case_mismatch}`",
                f"- lane_stability_summary: `sample_count={lane_summary['lane_sample_count']}`, "
                f"`switches={lane_summary['lane_switches']}`, `dominant_lane={lane_summary['dominant_lane_id']}`, "
                f"`preview_heading_std_deg={lane_summary['preview_heading_std_deg']}`",
                "",
            ]
        )
    lines.extend(
        [
            f"- mismatch_detected(bool): `{mismatch_detected}`",
            f"- first_suspect_if_map_contract_broken: `{first_suspect or 'none'}`",
            "",
        ]
    )
    return "\n".join(lines), {
        "official_map_source": status,
        "mismatch_detected": mismatch_detected,
        "first_suspect_if_map_contract_broken": first_suspect or "none",
    }


def _routing_check(case_results: List[Dict[str, Any]], baseline_by_case: Dict[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    lines = ["# Input Contract Routing Check", ""]
    case_status: Dict[str, Any] = {}
    primary_suspect = False
    for item in case_results:
        routing_rows = item["routing_rows"]
        route_lengths = []
        route_rows = _load_jsonl(Path(item["run_dir"]) / "artifacts" / "planning_route_segment_debug.jsonl")
        for row in route_rows:
            value = _safe_float(row.get("reference_line_length"))
            if value is not None:
                route_lengths.append(value)
        timeline = [
            {
                "phase": row.get("routing_phase"),
                "kind": row.get("routing_request_kind"),
                "reason": row.get("reroute_reason"),
                "sent": row.get("routing_request_sent"),
                "skip_invalid_goal": row.get("routing_skipped_due_to_invalid_goal"),
                "skip_freeze": row.get("routing_skipped_due_to_freeze"),
            }
            for row in routing_rows
        ]
        long_rows = [row for row in routing_rows if str(row.get("routing_phase")) == "long"]
        baseline = baseline_by_case.get(item["case_name"]) or {}
        delta_no_traj = None
        if baseline:
            delta_no_traj = int(item["total_no_trajectory_events"]) - int(baseline.get("total_no_trajectory_events", 0) or 0)
        invalid_goal_skip_count = sum(1 for row in routing_rows if bool(row.get("routing_skipped_due_to_invalid_goal")))
        if item["case_label"] == "Case B" and invalid_goal_skip_count > 0 and (delta_no_traj is None or delta_no_traj < 0):
            primary_suspect = True
        case_status[item["case_label"]] = {
            "routing_request_timeline": timeline,
            "reroute_reason_counts": item["reroute_reason_counts"],
            "route_segment_length_stats": {
                "mean_reference_line_length": _mean(route_lengths),
                "max_reference_line_length": _max(route_lengths),
                "route_segment_empty_count": item["route_segment_empty_count"],
            },
            "startup_vs_long_route_diff": {
                "long_phase_request_count": item["long_phase_request_count"],
                "invalid_goal_count": item["invalid_goal_count"],
                "invalid_goal_skip_count": invalid_goal_skip_count,
                "delta_total_no_trajectory_vs_baseline": delta_no_traj,
            },
        }
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- routing_request_timeline: `{_json_compact(timeline)}`",
                f"- reroute_reason_counts: `{_json_compact(item['reroute_reason_counts'])}`",
                f"- route_segment_length_stats: `mean={_mean(route_lengths)}`, `max={_max(route_lengths)}`, "
                f"`empty_count={item['route_segment_empty_count']}`",
                f"- startup_vs_long_route_diff: `long_phase_request_count={item['long_phase_request_count']}`, "
                f"`invalid_goal_count={item['invalid_goal_count']}`, `invalid_goal_skip_count={invalid_goal_skip_count}`, "
                f"`delta_total_no_trajectory_vs_baseline={delta_no_traj}`",
                "",
            ]
        )
    lines.extend(
        [
            f"- is_reroute_a_primary_suspect(bool): `{primary_suspect}`",
            "",
        ]
    )
    return "\n".join(lines), {
        "routing": case_status,
        "is_reroute_a_primary_suspect": primary_suspect,
    }


def _localization_check(case_results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    lines = ["# Input Contract Localization Check", ""]
    out: Dict[str, Any] = {}
    broken = False
    for item in case_results:
        artifacts = Path(item["run_dir"]) / "artifacts"
        lateral_rows = _load_csv_rows(artifacts / "lateral_geometry_debug.csv")
        effective = _load_yaml(Path(item["run_dir"]) / "effective.yaml")
        bridge_cfg = (((effective.get("algo", {}) or {}).get("apollo", {}) or {}).get("bridge", {}) or {})
        back_offset = _safe_float(bridge_cfg.get("localization_back_offset_m"))
        proj = _projection_jump_summary(lateral_rows)
        case_broken = (not proj["timestamp_monotonic"]) or proj["lane_projection_jump_events"] > 0
        broken = broken or case_broken
        out[item["case_label"]] = {
            "pose_semantics": "transformed_ros_odom_pose_with_no_extra_back_offset" if (back_offset or 0.0) == 0.0 else f"transformed_ros_odom_pose_plus_back_offset_{back_offset}",
            "timestamp_monotonicity": proj["timestamp_monotonic"],
            "lane_projection_continuity": proj["lane_projection_jump_events"] == 0,
            "sl_jump_events": proj["lane_projection_jump_events"],
        }
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- pose_semantics: `{out[item['case_label']]['pose_semantics']}`",
                f"- timestamp_monotonicity: `{proj['timestamp_monotonic']}`",
                f"- lane_projection_continuity: `{proj['lane_projection_jump_events'] == 0}`",
                f"- sl_jump_events: `{proj['lane_projection_jump_events']}`",
                "",
            ]
        )
    lines.append(f"- localization_contract_broken(bool): `{broken}`")
    lines.append("")
    return "\n".join(lines), {"localization_contract_broken": broken, "per_case": out}


def _vehicle_state_check(case_results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    lines = ["# Input Contract Vehicle State Check", ""]
    broken = False
    out: Dict[str, Any] = {}
    for item in case_results:
        summary = _vehicle_state_summary(item["control_events"])
        mode_not_ready = int(item["control_mode_not_ready_events"])
        estop_events = int(item["estop_or_safety_guard_events"])
        case_broken = mode_not_ready > 0 or estop_events > 0 or summary["planning_nonzero_mode_not_ready_cycles"] > 0
        broken = broken or case_broken
        out[item["case_label"]] = {
            "auto_drive_mode_timeline": summary["auto_drive_mode_timeline"],
            "control_mode_not_ready_counts": mode_not_ready,
            "estop_or_guard_counts": estop_events,
            "chassis_planning_time_alignment": "aligned_by_sim_time_header_and_control_consume_debug",
        }
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- auto_drive_mode_timeline: `{_json_compact(summary['auto_drive_mode_timeline'])}`",
                f"- control_mode_not_ready_counts: `{mode_not_ready}`",
                f"- estop_or_guard_counts: `{estop_events}`",
                "- chassis_planning_time_alignment: `aligned_by_sim_time_header_and_control_consume_debug`",
                "",
            ]
        )
    lines.append(f"- vehicle_state_contract_broken(bool): `{broken}`")
    lines.append("")
    return "\n".join(lines), {"vehicle_state_contract_broken": broken, "per_case": out}


def _obstacle_check(case_results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    lines = ["# Input Contract Obstacle Check", ""]
    broken = False
    out: Dict[str, Any] = {}
    for item in case_results:
        rows = _load_jsonl(Path(item["run_dir"]) / "artifacts" / "obstacle_contract_debug.jsonl")
        summary = _obstacle_summary(rows, item["control_events"])
        case_broken = bool(summary["obstacle_id_switches"] > 0 and summary["actor_found_count"] > 0)
        broken = broken or case_broken
        out[item["case_label"]] = {
            "obstacle_id_stability": {
                "actor_found_count": summary["actor_found_count"],
                "dominant_obstacle_id": summary["dominant_obstacle_id"],
                "obstacle_id_switches": summary["obstacle_id_switches"],
            },
            "obstacle_state_continuity": {
                "obstacle_speed_mean_mps": summary["obstacle_speed_mean_mps"],
                "obstacle_gap_mean_m": summary["obstacle_gap_mean_m"],
            },
            "front_target_consistency": summary["obstacle_id_switches"] == 0,
            "obstacle_anomaly_near_failure_events": summary["obstacle_anomaly_near_failure_events"],
        }
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- obstacle_id_stability: `{_json_compact(out[item['case_label']]['obstacle_id_stability'])}`",
                f"- obstacle_state_continuity: `{_json_compact(out[item['case_label']]['obstacle_state_continuity'])}`",
                f"- front_target_consistency: `{out[item['case_label']]['front_target_consistency']}`",
                f"- obstacle_anomaly_near_failure_events: `{summary['obstacle_anomaly_near_failure_events']}`",
                "",
            ]
        )
    lines.append(f"- obstacle_contract_broken(bool): `{broken}`")
    lines.append("")
    return "\n".join(lines), {"obstacle_contract_broken": broken, "per_case": out}


def _timing_check(case_results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    lines = ["# Input Contract Timing Check", ""]
    broken = False
    out: Dict[str, Any] = {}
    primary = False
    for item in case_results:
        planning_rows = _load_jsonl(Path(item["run_dir"]) / "artifacts" / "planning_topic_debug.jsonl")
        ages = _planning_age_stats(planning_rows)
        no_traj = item["no_trajectory_by_reason"]
        timing_reasons = {
            key: int(no_traj.get(key, 0) or 0)
            for key in (
                "no_recent_planning_message",
                "planning_parse_failed",
                "trajectory_all_points_expired",
                "trajectory_not_started_yet",
            )
        }
        zero_point = int(no_traj.get("zero_trajectory_points", 0) or 0)
        timing_total = sum(timing_reasons.values())
        if timing_total > zero_point:
            primary = True
        case_broken = timing_total > max(50, int(item["total_no_trajectory_events"]) // 3)
        broken = broken or case_broken
        out[item["case_label"]] = {
            "tick_ownership_model": "runner_harness_world_tick",
            "timing_source_model": "sim_time_from_ros_odom_header_stamp_and_cyber_time_headers",
            "planning_age_distribution": ages,
            "control_miss_by_timing_reason": timing_reasons,
        }
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                "- tick_ownership_model: `runner_harness_world_tick`",
                "- timing_source_model: `sim_time_from_ros_odom_header_stamp_and_cyber_time_headers`",
                f"- planning_age_distribution: `{_json_compact(ages)}`",
                f"- control_miss_by_timing_reason: `{_json_compact(timing_reasons)}`",
                "",
            ]
        )
    lines.append(f"- timing_contract_broken(bool): `{broken}`")
    lines.append(f"- timing_is_primary_suspect(bool): `{primary}`")
    lines.append("")
    return "\n".join(lines), {
        "timing_contract_broken": broken,
        "timing_is_primary_suspect": primary,
        "per_case": out,
    }


def _matrix_rows(
    map_payload: Dict[str, Any],
    routing_payload: Dict[str, Any],
    localization_payload: Dict[str, Any],
    vehicle_payload: Dict[str, Any],
    obstacle_payload: Dict[str, Any],
    timing_payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    return [
        {
            "contract_name": "Map Contract",
            "official_dependency": "Apollo routing/planning/control must share one HDMap semantic source",
            "current_input_source": "Apollo runtime map_dir + bridge auxiliary lane-centerline map file",
            "current_status": "partially_aligned" if map_payload["mismatch_detected"] else "aligned",
            "evidence_files": "artifacts/input_contract_map_check.md; artifacts/startup_geometry_summary.json",
            "primary_failure_mode": "bridge auxiliary map path differs from Apollo runtime map_dir" if map_payload["mismatch_detected"] else "none",
            "likely_owner": "mixed",
            "recommended_fix": "Prefer bridge auxiliary map checks against Apollo runtime map_dir and keep bridge out of HDMap topology decisions",
            "priority": "P1",
        },
        {
            "contract_name": "Routing Contract",
            "official_dependency": "Stable startup route plus explicit, reasoned reroute only",
            "current_input_source": "routing_event_debug + goal_validity_debug + routing_response",
            "current_status": "partially_aligned",
            "evidence_files": "artifacts/input_contract_routing_check.md; artifacts/profile_b_vs_bfreeze_report.md",
            "primary_failure_mode": "long phase reroute can fire when goal validity already reports reference_line_unavailable",
            "likely_owner": "bridge",
            "recommended_fix": "Keep skip_invalid_long_route enabled and continue Apollo reference-line diagnostics",
            "priority": "P0",
        },
        {
            "contract_name": "Localization Contract",
            "official_dependency": "Continuous pose semantics with monotonic timestamps and stable lane projection",
            "current_input_source": "ROS odom -> bridge localization + lateral geometry debug",
            "current_status": "broken" if localization_payload["localization_contract_broken"] else "aligned",
            "evidence_files": "artifacts/input_contract_localization_check.md; artifacts/lateral_geometry_debug.csv",
            "primary_failure_mode": "projection jump or timestamp non-monotonicity" if localization_payload["localization_contract_broken"] else "none",
            "likely_owner": "bridge",
            "recommended_fix": "Keep zero back_offset locked profile and avoid hidden localization geometry edits",
            "priority": "P2",
        },
        {
            "contract_name": "Vehicle State Contract",
            "official_dependency": "Control consumes planning only in valid auto-drive / non-estop chassis state",
            "current_input_source": "bridge chassis publisher + control consume debug",
            "current_status": "broken" if vehicle_payload["vehicle_state_contract_broken"] else "aligned",
            "evidence_files": "artifacts/input_contract_vehicle_state_check.md; artifacts/control_trajectory_consume_summary.json",
            "primary_failure_mode": "mode_not_ready_or_estop" if vehicle_payload["vehicle_state_contract_broken"] else "none",
            "likely_owner": "bridge",
            "recommended_fix": "Keep chassis COMPLETE_AUTO_DRIVE / DRIVE contract explicit and monitor any future mode drift",
            "priority": "P2",
        },
        {
            "contract_name": "Obstacle Contract",
            "official_dependency": "Perception/prediction should see stable front-target identity and continuous kinematics",
            "current_input_source": "objects_json/markers/objects3d -> bridge obstacle stream + obstacle contract debug",
            "current_status": "broken" if obstacle_payload["obstacle_contract_broken"] else "aligned",
            "evidence_files": "artifacts/input_contract_obstacle_check.md; artifacts/obstacle_contract_debug.jsonl",
            "primary_failure_mode": "front obstacle id instability" if obstacle_payload["obstacle_contract_broken"] else "none",
            "likely_owner": "scenario_input",
            "recommended_fix": "Keep obstacle IDs stable and investigate any anomaly near planning/control failures",
            "priority": "P2",
        },
        {
            "contract_name": "Timing Contract",
            "official_dependency": "Planning/control must share one sim-time model and consume trajectories in-window",
            "current_input_source": "planning_topic_debug + control consume debug + timing snapshot",
            "current_status": "broken" if timing_payload["timing_contract_broken"] else "aligned",
            "evidence_files": "artifacts/input_contract_timing_check.md; artifacts/control_trajectory_consume_summary.json",
            "primary_failure_mode": "stale_or_expired_timing" if timing_payload["timing_contract_broken"] else "empty_planning_dominates_not_timing",
            "likely_owner": "mixed",
            "recommended_fix": "Keep sim-time headers authoritative; timing is not the first target unless stale/expired starts dominating",
            "priority": "P1",
        },
    ]


def _fix_plan_md(matrix_rows: List[Dict[str, Any]]) -> str:
    lines = [
        "# Input Contract Fix Plan",
        "",
        "## Immediate Fixes",
        "",
        "- Routing / reroute: keep `skip_invalid_long_route=true` so bridge no longer sends long-phase route when goal validity already reports an invalid goal.",
        "- Observability: keep `obstacle_contract_debug.jsonl` and Apollo runtime map-dir capture so contract reports come from raw event streams instead of inference only.",
        "",
        "## Apollo-Side Deeper Checks",
        "",
    ]
    for row in matrix_rows:
        if row["current_status"] in {"broken", "partially_aligned"} and row["likely_owner"] != "bridge":
            lines.append(
                f"- {row['contract_name']}: owner=`{row['likely_owner']}`, fix=`{row['recommended_fix']}`"
            )
    lines.append("")
    return "\n".join(lines)


def _case_comparison_rows(
    case_results: List[Dict[str, Any]],
    baseline_by_case: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in case_results:
        baseline = baseline_by_case.get(item["case_name"]) or {}
        rows.append(
            {
                "case": item["case_label"],
                "run_dir": item["run_dir"],
                "planning_nonempty_messages": item["messages_with_nonzero_trajectory_points"],
                "control_no_trajectory_total": item["total_no_trajectory_events"],
                "control_no_trajectory_by_reason": _json_compact(item["no_trajectory_by_reason"]),
                "count_reference_line": item["count_reference_line"],
                "count_path_data_is_empty": item["count_path_data_is_empty"],
                "count_fail_to_aggregate_planning_trajectory": item["count_fail_to_aggregate_planning_trajectory"],
                "reroute_reason_counts": _json_compact(item["reroute_reason_counts"]),
                "invalid_goal_count": item["invalid_goal_count"],
                "dest_beyond_reference_line_count": item["dest_beyond_reference_line_count"],
                "stack_healthy": item["stack_healthy"],
                "final_outcome_label": item["final_outcome_label"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "whether_vehicle_moved": item["whether_vehicle_moved"],
                "baseline_total_no_trajectory_events": baseline.get("total_no_trajectory_events"),
                "delta_total_no_trajectory_events": (
                    int(item["total_no_trajectory_events"]) - int(baseline.get("total_no_trajectory_events", 0) or 0)
                    if baseline
                    else None
                ),
                "baseline_count_reference_line": baseline.get("count_reference_line"),
                "delta_count_reference_line": (
                    int(item["count_reference_line"]) - int(baseline.get("count_reference_line", 0) or 0)
                    if baseline
                    else None
                ),
                "summary_finalized_from_event_stream": item["summary_finalized_from_event_stream"],
                "any_fake_zero_detected": item["any_fake_zero_detected"],
            }
        )
    return rows


def _validation_report_md(
    case_results: List[Dict[str, Any]],
    baseline_by_case: Dict[str, Dict[str, Any]],
    routing_payload: Dict[str, Any],
    timing_payload: Dict[str, Any],
) -> str:
    lines = [
        "# Input Contract Validation Report",
        "",
    ]
    for item in case_results:
        baseline = baseline_by_case.get(item["case_name"]) or {}
        delta_no_traj = (
            int(item["total_no_trajectory_events"]) - int(baseline.get("total_no_trajectory_events", 0) or 0)
            if baseline
            else None
        )
        delta_ref = (
            int(item["count_reference_line"]) - int(baseline.get("count_reference_line", 0) or 0)
            if baseline
            else None
        )
        lines.append(
            f"- {item['case_label']}: success=`{item['success']}`, total_no_trajectory=`{item['total_no_trajectory_events']}`, "
            f"delta_no_trajectory_vs_baseline=`{delta_no_traj}`, delta_reference_line_vs_baseline=`{delta_ref}`, "
            f"reroute_reason_counts=`{_json_compact(item['reroute_reason_counts'])}`"
        )
    lines.extend(
        [
            "",
            f"- 明确结论: routing/reroute 契约是否仍是第一嫌疑: `{routing_payload['is_reroute_a_primary_suspect']}`",
            f"- timing 契约是否第一嫌疑: `{timing_payload['timing_is_primary_suspect']}`",
            "",
        ]
    )
    return "\n".join(lines)


def _rootcause_report_md(
    matrix_rows: List[Dict[str, Any]],
    case_results: List[Dict[str, Any]],
    routing_payload: Dict[str, Any],
    map_payload: Dict[str, Any],
    timing_payload: Dict[str, Any],
) -> str:
    broken_or_partial = [
        row for row in matrix_rows if row["current_status"] in {"broken", "partially_aligned"}
    ]
    top3 = broken_or_partial[:3]
    lines = [
        "# Input Contract Root Cause And Alignment Report",
        "",
        "## Official Dependencies",
        "",
        "- Apollo routing/planning/control 默认依赖同一套 HDMap / routing / localization / chassis / obstacle / timing 语义。",
        "- 上游如果在 route、goal、timestamp、mode、obstacle identity 上间歇不满足前提，reference line / route segment / planning→control 就会抖动。",
        "",
        "## Community Pitfalls",
        "",
        "- bridge 自己解释地图几何但与 Apollo runtime map_dir 不一致。",
        "- long reroute 在 reference line 尚未稳定时继续重发。",
        "- 统计只看 summary，不追原始事件流，导致假 0 或时序误判。",
        "",
        "## Current Top3 Unaligned Inputs",
        "",
    ]
    for row in top3:
        lines.append(
            f"- {row['contract_name']}: status=`{row['current_status']}`, owner=`{row['likely_owner']}`, primary_failure_mode=`{row['primary_failure_mode']}`"
        )
    lines.extend(
        [
            "",
            "## Fixed This Round",
            "",
            "- 已修: long-phase route 在 `invalid_goal=true` 时默认不再继续发送，避免桥继续污染 Apollo reroute/reference-line 链。",
            "- 已补: obstacle contract 原始事件流，以及 Apollo runtime map_dir 记录，后续 summary 全都能回溯到 JSONL。",
            "",
            "## Still Needs Apollo-Side Deeper Checks",
            "",
            "- routing/planning 内部 reference line / route segment 为什么仍会间歇掉空。",
            "- Apollo runtime 真正使用的 HDMap 与 lane_follow_map 内部状态是否仍有阶段性不一致。",
            "",
            "## Recommended Default",
            "",
            "- 继续保持 locked profile 思路：Profile A 作为默认保守基线，Profile B 作为受控实验主线，B-freeze 作为 reroute 干预对照。",
            "",
            f"- 当前 map_contract mismatch_detected: `{map_payload['mismatch_detected']}`",
            f"- 当前 routing/reroute primary suspect: `{routing_payload['is_reroute_a_primary_suspect']}`",
            f"- 当前 timing primary suspect: `{timing_payload['timing_is_primary_suspect']}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--baseline-batch-root", default="")
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_artifacts = batch_root / "artifacts"
    batch_artifacts.mkdir(parents=True, exist_ok=True)

    run_dirs = sorted(_discover_completed_run_dirs(batch_root), key=lambda path: CASE_ORDER.get(path.name.split("__")[0], 999))
    case_results = [analyze_run(run_dir) for run_dir in run_dirs]
    case_results.sort(key=lambda item: item["case_sort"])

    baseline_by_case: Dict[str, Dict[str, Any]] = {}
    baseline_root = Path(args.baseline_batch_root).expanduser().resolve() if args.baseline_batch_root else None
    if baseline_root is not None and baseline_root.exists():
        for run_dir in _discover_completed_run_dirs(baseline_root):
            result = analyze_run(run_dir)
            baseline_by_case[result["case_name"]] = result

    map_md, map_payload = _map_check(case_results)
    routing_md, routing_payload = _routing_check(case_results, baseline_by_case)
    localization_md, localization_payload = _localization_check(case_results)
    vehicle_md, vehicle_payload = _vehicle_state_check(case_results)
    obstacle_md, obstacle_payload = _obstacle_check(case_results)
    timing_md, timing_payload = _timing_check(case_results)

    matrix_rows = _matrix_rows(
        map_payload,
        routing_payload,
        localization_payload,
        vehicle_payload,
        obstacle_payload,
        timing_payload,
    )
    fix_plan_md = _fix_plan_md(matrix_rows)
    comparison_rows = _case_comparison_rows(case_results, baseline_by_case)
    validation_md = _validation_report_md(case_results, baseline_by_case, routing_payload, timing_payload)
    alignment_report_md = "\n".join(
        [
            "# Input Contract Alignment Report",
            "",
            "- 当前 6 类契约状态见下表与各分报告。",
            f"- top suspect: `{'Routing Contract' if routing_payload['is_reroute_a_primary_suspect'] else 'Timing Contract' if timing_payload['timing_is_primary_suspect'] else 'Map / routing-reference-line mixed'}`",
            "",
        ]
    )
    rootcause_md = _rootcause_report_md(
        matrix_rows,
        case_results,
        routing_payload,
        map_payload,
        timing_payload,
    )

    outputs_text = {
        "input_contract_map_check.md": map_md,
        "input_contract_routing_check.md": routing_md,
        "input_contract_localization_check.md": localization_md,
        "input_contract_vehicle_state_check.md": vehicle_md,
        "input_contract_obstacle_check.md": obstacle_md,
        "input_contract_timing_check.md": timing_md,
        "input_contract_alignment_report.md": alignment_report_md,
        "input_contract_fix_plan.md": fix_plan_md,
        "input_contract_validation_report.md": validation_md,
        "input_contract_rootcause_and_alignment_report.md": rootcause_md,
    }
    for name, text in outputs_text.items():
        _write_text(batch_artifacts / name, text)
        _copy_to_root(name, text)

    _write_csv(batch_artifacts / "input_contract_alignment_matrix.csv", matrix_rows)
    _copy_csv_to_root("input_contract_alignment_matrix.csv", matrix_rows)
    _write_csv(batch_artifacts / "input_contract_case_comparison.csv", comparison_rows)
    _copy_csv_to_root("input_contract_case_comparison.csv", comparison_rows)


if __name__ == "__main__":
    main()
