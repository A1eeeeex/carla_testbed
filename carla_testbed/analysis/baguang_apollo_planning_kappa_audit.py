from __future__ import annotations

import argparse
import csv
import json
import math
import re
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .baguang_apollo_lateral_blocker import DEFAULT_NO_LATERAL_RUN
from .baguang_apollo_lateral_stabilizer_ab import DEFAULT_STABILIZER_RUN

BAGUANG_APOLLO_PLANNING_KAPPA_AUDIT_SCHEMA_VERSION = "baguang_apollo_planning_kappa_audit.v1"


def analyze_baguang_apollo_planning_kappa_audit(
    *,
    no_lateral_run: str | Path = DEFAULT_NO_LATERAL_RUN,
    stabilizer_run: str | Path = DEFAULT_STABILIZER_RUN,
    kappa_spike_threshold: float = 0.05,
) -> dict[str, Any]:
    runs = {
        "no_lateral": analyze_baguang_apollo_planning_kappa_audit_run(
            no_lateral_run,
            kappa_spike_threshold=kappa_spike_threshold,
        ),
        "stabilizer_enabled": analyze_baguang_apollo_planning_kappa_audit_run(
            stabilizer_run,
            kappa_spike_threshold=kappa_spike_threshold,
        ),
    }
    findings = _pair_findings(runs)
    return {
        "schema_version": BAGUANG_APOLLO_PLANNING_KAPPA_AUDIT_SCHEMA_VERSION,
        "status": "warn" if findings else "pass",
        "reason": "planning_kappa_reference_line_semantics_require_audit" if findings else "no_planning_kappa_anomaly",
        "thresholds": {"kappa_spike": float(kappa_spike_threshold)},
        "runs": runs,
        "findings": findings,
        "claim_boundary": {
            "proves_apollo_algorithm_limitation": False,
            "reason": (
                "This audit shows whether planning trajectory curvature, reference-line debug, route segments, "
                "and route/lane geometry are mutually consistent. It does not by itself prove an Apollo "
                "algorithm limitation independent of map/reference-line/input-contract issues."
            ),
        },
    }


def analyze_baguang_apollo_planning_kappa_audit_run(
    run_dir: str | Path,
    *,
    kappa_spike_threshold: float = 0.05,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    summary = _read_json(root / "summary.json")
    map_contract = _read_json(root / "artifacts" / "map_contract_guard.json")
    planning_rows = _read_jsonl(root / "artifacts" / "planning_topic_debug.jsonl")
    ref_rows = _read_jsonl(root / "artifacts" / "apollo_reference_line_debug.jsonl")
    if not ref_rows:
        ref_rows = _read_jsonl(root / "artifacts" / "planning_route_segment_debug.jsonl")
    lateral_rows = _read_csv_rows(root / "artifacts" / "lateral_geometry_debug.csv")
    timeseries_rows = _read_csv_rows(root / "timeseries.csv")

    ref_by_sequence = {
        row.get("planning_header_sequence_num"): row
        for row in ref_rows
        if row.get("planning_header_sequence_num") is not None
    }
    high_kappa_rows = [
        row for row in planning_rows if abs(_num(row.get("first_trajectory_point_kappa"))) >= kappa_spike_threshold
    ]
    high_kappa_context = [_planning_ref_alignment(row, ref_by_sequence) for row in high_kappa_rows]
    reference_curvature = _numeric_csv(lateral_rows, "reference_lane_curvature")
    lane_centerlines = _load_apollo_lane_centerlines(map_contract)
    findings = _run_findings(
        planning_rows,
        ref_rows,
        high_kappa_context,
        reference_curvature,
        map_contract,
    )
    return {
        "run_dir": str(root),
        "summary": {
            "success": summary.get("success"),
            "exit_reason": summary.get("exit_reason"),
            "fail_reason": summary.get("fail_reason"),
            "collision_count": summary.get("collision_count"),
        },
        "map_contract": {
            "map_contract_invalid": map_contract.get("map_contract_invalid"),
            "map_contract_mismatch_active": map_contract.get("map_contract_mismatch_active"),
            "map_contract_mismatch_classification": map_contract.get("map_contract_mismatch_classification")
            or map_contract.get("mismatch_classification"),
            "high_risk_mismatch": map_contract.get("high_risk_mismatch"),
            "effective_bridge_map_complete": map_contract.get("effective_bridge_map_complete"),
            "container_runtime_map_complete": map_contract.get("container_runtime_map_complete"),
            "dreamview_selected_map": map_contract.get("dreamview_selected_map"),
            "runtime_map_dir": map_contract.get("runtime_map_dir"),
            "map_identity_hash_or_signature": map_contract.get("map_identity_hash_or_signature")
            or map_contract.get("effective_bridge_map_identity_signature")
            or map_contract.get("runtime_map_identity_signature"),
        },
        "planning": _planning_summary(planning_rows, kappa_spike_threshold),
        "reference_line_debug": _reference_line_summary(ref_rows),
        "high_kappa_context": _high_kappa_context_summary(high_kappa_context),
        "apollo_map_alignment": _trajectory_map_alignment_summary(high_kappa_context, lane_centerlines),
        "apollo_localization_alignment": _planning_localization_alignment_summary(
            root,
            planning_rows,
            high_kappa_rows,
            timeseries_rows,
        ),
        "lateral_geometry": {"reference_lane_curvature": _series_stats(reference_curvature)},
        "samples": high_kappa_context[:5],
        "findings": findings,
        "missing_inputs": _missing_inputs(root),
    }


def write_baguang_apollo_planning_kappa_audit_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_apollo_planning_kappa_audit_report.json"
    md_path = out / "baguang_apollo_planning_kappa_audit_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _planning_ref_alignment(
    planning: Mapping[str, Any],
    ref_by_sequence: Mapping[Any, Mapping[str, Any]],
) -> dict[str, Any]:
    seq = planning.get("planning_header_sequence_num")
    ref = ref_by_sequence.get(seq, {})
    return {
        "planning_sequence": seq,
        "trajectory_point_count": planning.get("trajectory_point_count"),
        "first_trajectory_point_kappa": planning.get("first_trajectory_point_kappa"),
        "planning_trajectory_kappa": planning.get("trajectory_kappa"),
        "planning_trajectory_theta_delta_abs": planning.get("trajectory_theta_delta_abs"),
        "planning_trajectory_xy_step_m": planning.get("trajectory_xy_step_m"),
        "planning_trajectory_kappa_spike_count_abs_ge_0_05": planning.get(
            "trajectory_kappa_spike_count_abs_ge_0_05"
        ),
        "planning_trajectory_kappa_spike_count_abs_ge_0_10": planning.get(
            "trajectory_kappa_spike_count_abs_ge_0_10"
        ),
        "planning_trajectory_sample_points": planning.get("trajectory_sample_points"),
        "planning_trajectory_first_segment_heading": planning.get("trajectory_first_segment_heading"),
        "planning_trajectory_first_theta_minus_first_segment_heading_rad": planning.get(
            "trajectory_first_theta_minus_first_segment_heading_rad"
        ),
        "first_trajectory_point_theta": planning.get("first_trajectory_point_theta"),
        "first_trajectory_point_x": planning.get("first_trajectory_point_x"),
        "first_trajectory_point_y": planning.get("first_trajectory_point_y"),
        "planning_reference_line_count": planning.get("reference_line_count"),
        "planning_routing_unique_lane_signature": planning.get("routing_unique_lane_signature"),
        "planning_routing_lane_window_signature": planning.get("routing_lane_window_signature"),
        "ref_debug_found": bool(ref),
        "ref_debug_reference_line_count": ref.get("reference_line_count"),
        "ref_debug_route_segment_count": ref.get("route_segment_count"),
        "ref_debug_route_segment_total_length": ref.get("route_segment_total_length"),
        "ref_debug_reference_line_provider_status": ref.get("reference_line_provider_status"),
        "ref_debug_lane_follow_map_status": ref.get("lane_follow_map_status"),
        "ref_debug_planning_empty_reason_guess": ref.get("planning_empty_reason_guess"),
        "ref_debug_missing_but_trajectory_nonzero": ref.get("reference_line_debug_missing_but_trajectory_nonzero"),
        "ref_debug_path_end_like_condition": ref.get("path_end_like_condition"),
        "ref_debug_current_lane_id": ref.get("current_lane_id"),
        "ref_debug_target_lane_id_first": ref.get("target_lane_id_first"),
    }


def _planning_summary(rows: Sequence[Mapping[str, Any]], kappa_spike_threshold: float) -> dict[str, Any]:
    point_counts = _numeric(rows, "trajectory_point_count")
    kappas = _numeric(rows, "first_trajectory_point_kappa")
    ref_counts = _numeric(rows, "reference_line_count")
    statuses = Counter(str(row.get("trajectory_header_status") or "<empty>") for row in rows)
    return {
        "rows": len(rows),
        "nonempty_trajectory_rows": sum(1 for value in point_counts if value > 0),
        "kappa_spike_rows": sum(1 for value in kappas if abs(value) >= kappa_spike_threshold),
        "trajectory_kappa_debug_available_rows": sum(
            1
            for row in rows
            if isinstance(row.get("trajectory_kappa"), Mapping) and _num(row["trajectory_kappa"].get("count")) > 0
        ),
        "trajectory_kappa_debug_max_abs": _series_stats(_nested_stat_values(rows, "trajectory_kappa", "max_abs")),
        "trajectory_theta_delta_abs": _series_stats(
            _nested_stat_values(rows, "trajectory_theta_delta_abs", "max_abs")
        ),
        "trajectory_xy_step_m": _series_stats(_nested_stat_values(rows, "trajectory_xy_step_m", "max_abs")),
        "trajectory_first_theta_minus_first_segment_heading_rad": _series_stats(
            _numeric(rows, "trajectory_first_theta_minus_first_segment_heading_rad")
        ),
        "trajectory_kappa_spike_count_abs_ge_0_05_sum": sum(
            int(_num(row.get("trajectory_kappa_spike_count_abs_ge_0_05"))) for row in rows
        ),
        "trajectory_sample_points_available_rows": sum(
            1 for row in rows if isinstance(row.get("trajectory_sample_points"), list) and row["trajectory_sample_points"]
        ),
        "trajectory_point_count": _series_stats(point_counts),
        "first_trajectory_point_kappa": _series_stats(kappas),
        "reference_line_count": _series_stats(ref_counts),
        "reference_line_nonzero_rows": sum(1 for value in ref_counts if value > 0),
        "routing_unique_lane_signatures": _top_counts(row.get("routing_unique_lane_signature") for row in rows),
        "routing_lane_window_signatures": _top_counts(row.get("routing_lane_window_signature") for row in rows),
        "status_counts": dict(statuses.most_common(8)),
    }


def _reference_line_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ref_counts = _numeric(rows, "reference_line_count")
    route_segment_counts = _numeric(rows, "route_segment_count")
    return {
        "rows": len(rows),
        "reference_line_count": _series_stats(ref_counts),
        "reference_line_nonzero_rows": sum(1 for value in ref_counts if value > 0),
        "route_segment_count": _series_stats(route_segment_counts),
        "route_segment_present_reference_line_zero_rows": sum(
            1 for row in rows if _num(row.get("route_segment_count")) > 0 and _num(row.get("reference_line_count")) == 0
        ),
        "reference_line_provider_status_counts": _top_counts(row.get("reference_line_provider_status") for row in rows),
        "lane_follow_map_status_counts": _top_counts(row.get("lane_follow_map_status") for row in rows),
        "planning_empty_reason_guess_counts": _top_counts(row.get("planning_empty_reason_guess") for row in rows),
        "reference_line_debug_missing_but_trajectory_nonzero_count": sum(
            1 for row in rows if row.get("reference_line_debug_missing_but_trajectory_nonzero") is True
        ),
        "path_end_like_condition_count": sum(1 for row in rows if row.get("path_end_like_condition") is True),
    }


def _high_kappa_context_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(rows),
        "ref_debug_found_count": sum(1 for row in rows if row.get("ref_debug_found")),
        "planning_reference_line_zero_count": sum(
            1 for row in rows if _num(row.get("planning_reference_line_count")) == 0
        ),
        "ref_debug_reference_line_zero_count": sum(
            1 for row in rows if _num(row.get("ref_debug_reference_line_count")) == 0
        ),
        "route_segment_present_count": sum(1 for row in rows if _num(row.get("ref_debug_route_segment_count")) > 0),
        "provider_failed_count": sum(
            1 for row in rows if str(row.get("ref_debug_reference_line_provider_status") or "") == "failed"
        ),
        "route_segments_present_reference_line_missing_count": sum(
            1
            for row in rows
            if str(row.get("ref_debug_lane_follow_map_status") or "")
            == "route_segments_present_reference_line_missing"
        ),
        "missing_but_nonzero_count": sum(
            1 for row in rows if row.get("ref_debug_missing_but_trajectory_nonzero") is True
        ),
        "path_end_like_condition_count": sum(1 for row in rows if row.get("ref_debug_path_end_like_condition") is True),
        "first_trajectory_point_kappa": _series_stats(_numeric(rows, "first_trajectory_point_kappa")),
        "planning_trajectory_kappa_debug_max_abs": _series_stats(
            _nested_stat_values(rows, "planning_trajectory_kappa", "max_abs")
        ),
        "planning_trajectory_xy_step_m_max_abs": _series_stats(
            _nested_stat_values(rows, "planning_trajectory_xy_step_m", "max_abs")
        ),
        "planning_trajectory_theta_delta_abs_max_abs": _series_stats(
            _nested_stat_values(rows, "planning_trajectory_theta_delta_abs", "max_abs")
        ),
        "route_segment_total_length": _series_stats(_numeric(rows, "ref_debug_route_segment_total_length")),
    }


def _trajectory_map_alignment_summary(
    rows: Sequence[Mapping[str, Any]],
    lane_centerlines: Mapping[str, Sequence[tuple[float, float]]],
) -> dict[str, Any]:
    if not lane_centerlines:
        return {
            "status": "missing_map",
            "sample_count": 0,
            "lane_ids": {},
            "sample_to_lane_distance_m": _series_stats([]),
            "sample_heading_error_rad": _series_stats([]),
        }
    distances: list[float] = []
    heading_errors: list[float] = []
    lane_ids: list[str] = []
    for row in rows:
        lane_id = _single_lane_id(row.get("planning_routing_unique_lane_signature"))
        if not lane_id or lane_id not in lane_centerlines:
            continue
        lane_ids.append(lane_id)
        centerline = lane_centerlines[lane_id]
        for point in row.get("planning_trajectory_sample_points") or []:
            if not isinstance(point, Mapping):
                continue
            x = _maybe_num(point.get("x"))
            y = _maybe_num(point.get("y"))
            if x is None or y is None:
                continue
            nearest = _nearest_polyline_segment(centerline, x, y)
            if nearest is None:
                continue
            distance, lane_heading = nearest
            distances.append(distance)
            theta = _maybe_num(point.get("theta"))
            if theta is not None:
                heading_errors.append(_wrap_to_pi(theta - lane_heading))
    status = "available" if distances else "insufficient_samples"
    return {
        "status": status,
        "sample_count": len(distances),
        "lane_ids": _top_counts(lane_ids),
        "sample_to_lane_distance_m": _series_stats(distances),
        "sample_heading_error_rad": _series_stats(heading_errors),
    }


def _planning_localization_alignment_summary(
    root: Path,
    planning_rows: Sequence[Mapping[str, Any]],
    high_kappa_rows: Sequence[Mapping[str, Any]],
    timeseries_rows: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    if not timeseries_rows:
        return {
            "status": "missing_timeseries",
            "matched_planning_rows": 0,
            "localization_back_offset_m": _localization_back_offset(root),
            "invert_tf_assumed": True,
            "first_point_to_localization_distance_m": _series_stats([]),
            "first_point_heading_error_rad": _series_stats([]),
            "high_kappa_first_point_to_localization_distance_m": _series_stats([]),
            "high_kappa_first_point_heading_error_rad": _series_stats([]),
        }
    by_frame: dict[int, Mapping[str, str]] = {}
    for row in timeseries_rows:
        frame = _maybe_int(row.get("frame_id"))
        if frame is not None:
            by_frame[frame] = row
    offset = _localization_back_offset(root)
    all_dist: list[float] = []
    all_heading: list[float] = []
    high_dist: list[float] = []
    high_heading: list[float] = []
    high_ids = {id(row) for row in high_kappa_rows}
    matched = 0
    for row in planning_rows:
        frame = _maybe_int(row.get("world_frame"))
        ts = by_frame.get(frame) if frame is not None else None
        if ts is None:
            continue
        loc = _apollo_localization_from_timeseries(ts, localization_back_offset_m=offset)
        if loc is None:
            continue
        first_x = _maybe_num(row.get("first_trajectory_point_x"))
        first_y = _maybe_num(row.get("first_trajectory_point_y"))
        first_theta = _maybe_num(row.get("first_trajectory_point_theta"))
        if first_x is None or first_y is None:
            continue
        matched += 1
        dist = math.hypot(first_x - loc["x"], first_y - loc["y"])
        all_dist.append(dist)
        heading_error = None
        if first_theta is not None:
            heading_error = _wrap_to_pi(first_theta - loc["yaw"])
            all_heading.append(heading_error)
        if id(row) in high_ids:
            high_dist.append(dist)
            if heading_error is not None:
                high_heading.append(heading_error)
    return {
        "status": "available" if matched else "no_frame_matches",
        "matched_planning_rows": matched,
        "localization_back_offset_m": offset,
        "invert_tf_assumed": True,
        "first_point_to_localization_distance_m": _series_stats(all_dist),
        "first_point_heading_error_rad": _series_stats(all_heading),
        "high_kappa_first_point_to_localization_distance_m": _series_stats(high_dist),
        "high_kappa_first_point_heading_error_rad": _series_stats(high_heading),
    }


def _apollo_localization_from_timeseries(
    row: Mapping[str, str],
    *,
    localization_back_offset_m: float,
) -> dict[str, float] | None:
    ego_x = _maybe_num(row.get("ego_x"))
    ego_y = _maybe_num(row.get("ego_y"))
    ego_heading = _maybe_num(row.get("ego_heading"))
    if ego_x is None or ego_y is None or ego_heading is None:
        return None
    # carla_direct_transport publishes Apollo-frame localization with y/yaw inverted.
    yaw = -ego_heading
    y = -ego_y
    x = ego_x - localization_back_offset_m * math.cos(yaw)
    y = y - localization_back_offset_m * math.sin(yaw)
    return {"x": x, "y": y, "yaw": yaw}


def _localization_back_offset(root: Path) -> float:
    for rel in ("artifacts/apollo_bridge_effective.yaml", "effective.yaml"):
        path = root / rel
        if not path.exists():
            continue
        match = re.search(r"^\s*localization_back_offset_m:\s*([0-9.eE+-]+)", path.read_text(errors="replace"), re.M)
        if match:
            parsed = _maybe_num(match.group(1))
            if parsed is not None:
                return parsed
    return 1.4235


def _load_apollo_lane_centerlines(map_contract: Mapping[str, Any]) -> dict[str, list[tuple[float, float]]]:
    runtime_map_dir = map_contract.get("runtime_map_dir")
    if not runtime_map_dir:
        return {}
    xml_path = Path(str(runtime_map_dir)).expanduser() / "base_map.xml"
    if not xml_path.exists():
        return {}
    try:
        root = ET.parse(xml_path).getroot()
    except ET.ParseError:
        return {}
    lanes: dict[str, list[tuple[float, float]]] = {}
    for lane in root.findall(".//lane"):
        lane_id = lane.get("uid") or lane.get("id")
        if not lane_id:
            continue
        center = lane.find("./centralCurve")
        if center is None:
            center = lane.find("./central_curve")
        if center is None:
            center = lane.find("./centerLine")
        if center is None:
            continue
        points: list[tuple[float, float]] = []
        for point in center.findall(".//point"):
            x = _maybe_num(point.get("x"))
            y = _maybe_num(point.get("y"))
            if x is not None and y is not None:
                points.append((x, y))
        if len(points) >= 2:
            lanes[str(lane_id)] = points
    return lanes


def _single_lane_id(signature: Any) -> str | None:
    value = str(signature or "").strip()
    if not value or value == "none":
        return None
    # Current Baguang route signatures are single-lane values such as "0_0_2".
    if "," in value or ";" in value or " " in value:
        return None
    return value


def _nearest_polyline_segment(points: Sequence[tuple[float, float]], x: float, y: float) -> tuple[float, float] | None:
    best: tuple[float, float] | None = None
    for index in range(1, len(points)):
        x0, y0 = points[index - 1]
        x1, y1 = points[index]
        dx = x1 - x0
        dy = y1 - y0
        denom = dx * dx + dy * dy
        if denom <= 0:
            continue
        t = max(0.0, min(1.0, ((x - x0) * dx + (y - y0) * dy) / denom))
        px = x0 + t * dx
        py = y0 + t * dy
        distance = math.hypot(x - px, y - py)
        heading = math.atan2(dy, dx)
        if best is None or distance < best[0]:
            best = (distance, heading)
    return best


def _run_findings(
    planning_rows: Sequence[Mapping[str, Any]],
    ref_rows: Sequence[Mapping[str, Any]],
    high_kappa_context: Sequence[Mapping[str, Any]],
    reference_curvature: Sequence[float],
    map_contract: Mapping[str, Any],
) -> list[str]:
    findings: list[str] = []
    if any(_num(row.get("trajectory_point_count")) > 0 for row in planning_rows):
        findings.append("planning_nonempty_available")
    if high_kappa_context:
        findings.append("planning_first_point_kappa_spike_present")
    if high_kappa_context and max((abs(value) for value in reference_curvature), default=0.0) < 0.001:
        findings.append("planning_kappa_spike_on_straight_route_geometry")
    if ref_rows and not any(_num(row.get("reference_line_count")) > 0 for row in ref_rows):
        findings.append("reference_line_debug_zero_for_all_rows")
    if any(
        str(row.get("ref_debug_lane_follow_map_status") or "") == "route_segments_present_reference_line_missing"
        for row in high_kappa_context
    ):
        findings.append("high_kappa_with_route_segments_present_reference_line_missing")
    if any(row.get("ref_debug_missing_but_trajectory_nonzero") is True for row in high_kappa_context):
        findings.append("high_kappa_with_reference_line_debug_missing_but_trajectory_nonzero")
    if any(str(row.get("ref_debug_reference_line_provider_status") or "") == "failed" for row in high_kappa_context):
        findings.append("high_kappa_with_reference_line_provider_failed")
    if map_contract.get("map_contract_invalid") is False and map_contract.get("high_risk_mismatch") is False:
        findings.append("map_contract_not_currently_mismatched")
    return findings


def _pair_findings(runs: Mapping[str, Mapping[str, Any]]) -> list[str]:
    all_findings = [item for run in runs.values() for item in run.get("findings", [])]
    findings: list[str] = []
    if "planning_kappa_spike_on_straight_route_geometry" in all_findings:
        findings.append("planning_kappa_spike_not_explained_by_route_geometry")
    if "high_kappa_with_route_segments_present_reference_line_missing" in all_findings:
        findings.append("reference_line_provider_or_debug_gap_is_on_kappa_path")
    if "high_kappa_with_reference_line_debug_missing_but_trajectory_nonzero" in all_findings:
        findings.append("reference_line_debug_missing_but_planning_nonzero_is_on_kappa_path")
    if "planning_nonempty_available" in all_findings and "reference_line_debug_zero_for_all_rows" in all_findings:
        findings.append("planning_outputs_exist_despite_zero_reference_line_debug")
    if "map_contract_not_currently_mismatched" in all_findings:
        findings.append("map_contract_mismatch_not_primary_evidence")
    return findings


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _missing_inputs(root: Path) -> list[str]:
    rels = (
        "summary.json",
        "artifacts/map_contract_guard.json",
        "artifacts/planning_topic_debug.jsonl",
        "artifacts/apollo_reference_line_debug.jsonl",
        "artifacts/lateral_geometry_debug.csv",
    )
    return [rel for rel in rels if not (root / rel).exists()]


def _numeric(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _maybe_num(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _numeric_csv(rows: Sequence[Mapping[str, str]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _maybe_num(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _nested_stat_values(rows: Sequence[Mapping[str, Any]], field: str, stat: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        nested = row.get(field)
        if not isinstance(nested, Mapping):
            continue
        value = _maybe_num(nested.get(stat))
        if value is not None:
            values.append(value)
    return values


def _series_stats(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None}
    abs_values = sorted(abs(value) for value in values)
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "max_abs": max(abs_values),
        "p95_abs": abs_values[int(0.95 * (len(abs_values) - 1))],
    }


def _top_counts(values: Sequence[Any]) -> dict[str, int]:
    counts = Counter(str(value) for value in values if value not in (None, ""))
    return dict(counts.most_common(8))


def _maybe_num(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _maybe_int(value: Any) -> int | None:
    parsed = _maybe_num(value)
    return int(parsed) if parsed is not None else None


def _num(value: Any) -> float:
    parsed = _maybe_num(value)
    return 0.0 if parsed is None else parsed


def _wrap_to_pi(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Baguang Apollo Planning Kappa Audit",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        "",
        "## Run Results",
    ]
    for name, run in (report.get("runs") or {}).items():
        if not isinstance(run, Mapping):
            continue
        planning = run.get("planning") if isinstance(run.get("planning"), Mapping) else {}
        ref = run.get("reference_line_debug") if isinstance(run.get("reference_line_debug"), Mapping) else {}
        high = run.get("high_kappa_context") if isinstance(run.get("high_kappa_context"), Mapping) else {}
        alignment = run.get("apollo_map_alignment") if isinstance(run.get("apollo_map_alignment"), Mapping) else {}
        loc_alignment = (
            run.get("apollo_localization_alignment")
            if isinstance(run.get("apollo_localization_alignment"), Mapping)
            else {}
        )
        geom = run.get("lateral_geometry") if isinstance(run.get("lateral_geometry"), Mapping) else {}
        lines.extend(
            [
                "",
                f"### {name}",
                f"- run_dir: `{run.get('run_dir')}`",
                f"- planning_nonempty_rows: `{planning.get('nonempty_trajectory_rows')}`",
                f"- planning_kappa_spike_rows: `{planning.get('kappa_spike_rows')}`",
                f"- planning.first_kappa.max_abs: `{_stat(planning.get('first_trajectory_point_kappa'), 'max_abs')}`",
                f"- planning.trajectory_kappa_debug_available_rows: `{planning.get('trajectory_kappa_debug_available_rows')}`",
                f"- planning.trajectory_kappa_debug.max_abs: `{_stat(planning.get('trajectory_kappa_debug_max_abs'), 'max_abs')}`",
                f"- planning.trajectory_xy_step_m.max_abs: `{_stat(planning.get('trajectory_xy_step_m'), 'max_abs')}`",
                f"- planning.trajectory_theta_delta_abs.max_abs: `{_stat(planning.get('trajectory_theta_delta_abs'), 'max_abs')}`",
                f"- planning.first_theta_minus_first_segment_heading.max_abs: `{_stat(planning.get('trajectory_first_theta_minus_first_segment_heading_rad'), 'max_abs')}`",
                f"- reference_line_nonzero_rows: `{ref.get('reference_line_nonzero_rows')}`",
                f"- route_segment_present_reference_line_zero_rows: `{ref.get('route_segment_present_reference_line_zero_rows')}`",
                f"- high_kappa_provider_failed_count: `{high.get('provider_failed_count')}`",
                f"- high_kappa_route_segments_present_reference_line_missing_count: `{high.get('route_segments_present_reference_line_missing_count')}`",
                f"- high_kappa_sample_map_alignment.status: `{alignment.get('status')}`",
                f"- high_kappa_sample_to_lane_distance.max_abs: `{_stat(alignment.get('sample_to_lane_distance_m'), 'max_abs')}`",
                f"- high_kappa_sample_heading_error.max_abs: `{_stat(alignment.get('sample_heading_error_rad'), 'max_abs')}`",
                f"- planning_first_point_localization_alignment.status: `{loc_alignment.get('status')}`",
                f"- high_kappa_first_point_to_localization_distance.max_abs: `{_stat(loc_alignment.get('high_kappa_first_point_to_localization_distance_m'), 'max_abs')}`",
                f"- high_kappa_first_point_heading_error.max_abs: `{_stat(loc_alignment.get('high_kappa_first_point_heading_error_rad'), 'max_abs')}`",
                f"- route_geometry.reference_curvature.max_abs: `{_stat((geom.get('reference_lane_curvature') if isinstance(geom, Mapping) else {}), 'max_abs')}`",
                f"- findings: `{', '.join(run.get('findings') or [])}`",
            ]
        )
    lines.extend(["", "## Findings"])
    lines.extend(f"- `{item}`" for item in report.get("findings") or [])
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "This audit is evidence for a planning/reference-line semantics investigation, not a standalone Apollo capability verdict.",
            "",
        ]
    )
    return "\n".join(lines)


def _stat(value: Any, field: str) -> Any:
    return value.get(field) if isinstance(value, Mapping) else None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit Apollo planning kappa/reference-line semantics on Baguang.")
    parser.add_argument("--no-lateral-run", default=str(DEFAULT_NO_LATERAL_RUN))
    parser.add_argument("--stabilizer-run", default=str(DEFAULT_STABILIZER_RUN))
    parser.add_argument("--kappa-spike-threshold", type=float, default=0.05)
    parser.add_argument("--out", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_baguang_apollo_planning_kappa_audit(
        no_lateral_run=args.no_lateral_run,
        stabilizer_run=args.stabilizer_run,
        kappa_spike_threshold=args.kappa_spike_threshold,
    )
    outputs = write_baguang_apollo_planning_kappa_audit_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "reason": report.get("reason"), "outputs": outputs}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
