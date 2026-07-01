from __future__ import annotations

import csv
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from carla_testbed.analysis.apollo_hdmap_projection import (
    read_apollo_hdmap_projection,
    summarize_apollo_hdmap_projection,
)

REPORT_SCHEMA_VERSION = "apollo_reference_line_contract.v1"
INTERPRETATION_BOUNDARY = (
    "This verifies Apollo planning/control reference-line evidence, not full "
    "perception/localization reproduction."
)

PASS_HEADING_RAD = 0.05
WARN_HEADING_RAD = 0.10
FAIL_HEADING_RAD = 0.20
PASS_LATERAL_M = 0.50
WARN_LATERAL_M = 1.00
MIN_NONEMPTY_TRAJECTORY_RATIO = 0.80
MIN_PROVIDER_READY_RATIO = 0.80
FALLBACK_JOIN_TOLERANCE_S = 0.05
PLANNING_PROJECTION_JOIN_TOLERANCE_S = 0.25
CONTROL_TARGET_TRAJECTORY_SAMPLE_WARN_M = 0.50
CONTROL_TARGET_PATH_CANDIDATE_WARN_M = 0.50
PLANNING_PATH_CANDIDATE_TRAJECTORY_WARN_M = 2.00
PLANNING_PATH_CANDIDATE_TRAJECTORY_SUPPORT_MARGIN_M = 5.00


def analyze_apollo_reference_line_contract_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    sources = _resolve_run_sources(root)
    return analyze_apollo_reference_line_contract_files(**sources, run_dir=root)


def analyze_apollo_reference_line_contract_files(
    *,
    contract_path: str | Path | None = None,
    planning_topic_debug_path: str | Path | None = None,
    planning_topic_debug_summary_path: str | Path | None = None,
    planning_route_segment_debug_path: str | Path | None = None,
    planning_info_log_path: str | Path | None = None,
    control_decode_debug_path: str | Path | None = None,
    debug_timeseries_path: str | Path | None = None,
    localization_contract_path: str | Path | None = None,
    planning_materialization_path: str | Path | None = None,
    hdmap_projection_path: str | Path | None = None,
    route_contract_path: str | Path | None = None,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    source = {
        "run_dir": str(run_dir) if run_dir else None,
        "contract_path": _path_str(contract_path),
        "planning_topic_debug_path": _path_str(planning_topic_debug_path),
        "planning_topic_debug_summary_path": _path_str(planning_topic_debug_summary_path),
        "planning_route_segment_debug_path": _path_str(planning_route_segment_debug_path),
        "planning_info_log_path": _path_str(planning_info_log_path),
        "control_decode_debug_path": _path_str(control_decode_debug_path),
        "debug_timeseries_path": _path_str(debug_timeseries_path),
        "localization_contract_path": _path_str(localization_contract_path),
        "planning_materialization_path": _path_str(planning_materialization_path),
        "hdmap_projection_path": _path_str(hdmap_projection_path),
        "route_contract_path": _path_str(route_contract_path),
    }
    contract_rows = _read_jsonl(contract_path)
    planning_rows = _read_jsonl(planning_topic_debug_path)
    planning_debug_summary = _read_json(planning_topic_debug_summary_path)
    route_segment_rows = _read_jsonl(planning_route_segment_debug_path)
    planning_info_log_reference_line_evidence = _planning_info_log_reference_line_evidence(
        planning_info_log_path
    )
    control_rows = _read_jsonl(control_decode_debug_path)
    timeseries_rows = _read_csv(debug_timeseries_path)
    localization_contract = _read_json(localization_contract_path)
    planning_materialization = _read_json(planning_materialization_path)
    route_contract = _read_json(route_contract_path)
    hdmap_rows = _read_hdmap_projection(hdmap_projection_path)
    hdmap_projection_artifact_exists = _path_exists(hdmap_projection_path)

    rows = _normalized_rows(
        contract_rows=contract_rows,
        planning_rows=planning_rows,
        route_segment_rows=route_segment_rows,
        control_rows=control_rows,
        timeseries_rows=timeseries_rows,
    )
    return analyze_apollo_reference_line_contract(
        rows,
        localization_contract=localization_contract,
        planning_materialization=planning_materialization,
        planning_debug_summary=planning_debug_summary,
        planning_info_log_reference_line_evidence=planning_info_log_reference_line_evidence,
        route_contract=route_contract,
        hdmap_projection_rows=hdmap_rows,
        hdmap_projection_artifact_exists=hdmap_projection_artifact_exists,
        source=source,
    )


def analyze_apollo_reference_line_contract(
    rows: Sequence[Mapping[str, Any]],
    *,
    localization_contract: Mapping[str, Any] | None = None,
    planning_materialization: Mapping[str, Any] | None = None,
    planning_debug_summary: Mapping[str, Any] | None = None,
    planning_info_log_reference_line_evidence: Mapping[str, Any] | None = None,
    route_contract: Mapping[str, Any] | None = None,
    hdmap_projection_rows: Sequence[Mapping[str, Any]] | None = None,
    hdmap_projection_artifact_exists: bool | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = [dict(row) for row in rows]
    hdmap_projection = summarize_apollo_hdmap_projection(
        hdmap_projection_rows or [],
        artifact_file_exists=hdmap_projection_artifact_exists,
    )
    if not normalized:
        contracts = _contracts(
            rows=[],
            evidence=_evidence([], hdmap_projection),
            metrics=_metrics([]),
            hdmap_projection=hdmap_projection,
        )
        warnings, blocking = _contract_messages(contracts)
        warnings.append("reference_line_evidence_missing")
        return _report(
            status=_overall_status(contracts, extra_blocking=blocking, extra_warnings=warnings),
            blocking_reasons=blocking,
            warnings=warnings,
            rows=[],
            localization_contract=localization_contract,
            planning_materialization=planning_materialization,
            planning_debug_summary=planning_debug_summary,
            planning_info_log_reference_line_evidence=planning_info_log_reference_line_evidence,
            route_contract=route_contract,
            hdmap_projection_rows=hdmap_projection_rows,
            hdmap_projection_artifact_exists=hdmap_projection_artifact_exists,
            source=source,
            contracts=contracts,
        )

    evidence = _evidence(normalized, hdmap_projection)
    metrics = _metrics(normalized)
    contracts = _contracts(
        rows=normalized,
        evidence=evidence,
        metrics=metrics,
        hdmap_projection=hdmap_projection,
    )
    warnings, blocking = _contract_messages(contracts)
    if not evidence["planning_reference_available"] and not evidence["control_reference_available"]:
        warnings.append("planning_control_reference_evidence_missing")
        return _report(
            status=_overall_status(contracts, extra_blocking=blocking, extra_warnings=warnings),
            blocking_reasons=blocking,
            warnings=warnings,
            rows=normalized,
            localization_contract=localization_contract,
            planning_materialization=planning_materialization,
            planning_debug_summary=planning_debug_summary,
            planning_info_log_reference_line_evidence=planning_info_log_reference_line_evidence,
            route_contract=route_contract,
            hdmap_projection_rows=hdmap_projection_rows,
            hdmap_projection_artifact_exists=hdmap_projection_artifact_exists,
            source=source,
            contracts=contracts,
        )

    required_metrics_available = (
        metrics["planning_ref_heading_error_p95_rad"] is not None
        or metrics["control_ref_heading_error_p95_rad"] is not None
    )
    if not required_metrics_available:
        warnings.append("reference_heading_metrics_missing")
        return _report(
            status=_overall_status(contracts, extra_blocking=blocking, extra_warnings=warnings),
            blocking_reasons=blocking,
            warnings=warnings,
            rows=normalized,
            localization_contract=localization_contract,
            planning_materialization=planning_materialization,
            planning_debug_summary=planning_debug_summary,
            planning_info_log_reference_line_evidence=planning_info_log_reference_line_evidence,
            route_contract=route_contract,
            hdmap_projection_rows=hdmap_projection_rows,
            hdmap_projection_artifact_exists=hdmap_projection_artifact_exists,
            source=source,
            contracts=contracts,
        )
    status = _overall_status(contracts, extra_blocking=blocking, extra_warnings=warnings)
    return _report(
        status=status,
        blocking_reasons=blocking,
        warnings=warnings,
        rows=normalized,
        localization_contract=localization_contract,
        planning_materialization=planning_materialization,
        planning_debug_summary=planning_debug_summary,
        planning_info_log_reference_line_evidence=planning_info_log_reference_line_evidence,
        route_contract=route_contract,
        hdmap_projection_rows=hdmap_projection_rows,
        hdmap_projection_artifact_exists=hdmap_projection_artifact_exists,
        source=source,
        contracts=contracts,
    )


def write_apollo_reference_line_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "apollo_reference_line_contract_report.json"
    summary_path = output_dir / "apollo_reference_line_contract_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_reference_line_contract_summary_md(report), encoding="utf-8")
    return {
        "apollo_reference_line_contract_report": str(json_path),
        "apollo_reference_line_contract_summary": str(summary_path),
    }


def ensure_apollo_reference_line_contract_report(run_dir: str | Path, *, refresh: bool = False) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    report_path = root / "analysis" / "apollo_reference_line_contract" / "apollo_reference_line_contract_report.json"
    existing = _find_first(
        root,
        [
            "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
            "apollo_reference_line_contract_report.json",
        ],
    )
    if existing is not None and not refresh:
        report = _read_json(existing)
        if not (
            _has_reference_line_regeneration_inputs(root)
            and _reference_line_report_needs_regeneration(report)
        ):
            return {
                "status": "existing",
                "path": str(existing),
                "report_status": report.get("status"),
            }
    if existing is not None and not _has_reference_line_regeneration_inputs(root):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        if existing.resolve() != report_path.resolve():
            report_path.write_text(existing.read_text(encoding="utf-8"), encoding="utf-8")
            summary = existing.parent / "apollo_reference_line_contract_summary.md"
            if summary.exists():
                (report_path.parent / "apollo_reference_line_contract_summary.md").write_text(
                    summary.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
        report = _read_json(report_path if report_path.exists() else existing)
        return {
            "status": "existing_report_copied",
            "path": str(report_path if report_path.exists() else existing),
            "report_status": report.get("status"),
            "source_report": str(existing),
        }
    report = analyze_apollo_reference_line_contract_run_dir(root)
    outputs = write_apollo_reference_line_contract_report(report, report_path.parent)
    return {
        "status": "generated",
        "path": outputs["apollo_reference_line_contract_report"],
        "summary_path": outputs["apollo_reference_line_contract_summary"],
        "report_status": report.get("status"),
    }


def ensure_apollo_reference_line_contract_reports_for_root(root: str | Path, *, refresh: bool = False) -> list[dict[str, Any]]:
    base = Path(root).expanduser()
    run_dirs = [base] if (base / "summary.json").exists() else sorted({path.parent for path in base.rglob("summary.json")})
    return [ensure_apollo_reference_line_contract_report(path, refresh=refresh) for path in run_dirs]


def _has_reference_line_regeneration_inputs(root: Path) -> bool:
    for relative in (
        "artifacts/apollo_reference_line_contract.jsonl",
        "apollo_reference_line_contract.jsonl",
        "artifacts/planning_topic_debug.jsonl",
        "planning_topic_debug.jsonl",
        "artifacts/apollo_planning.INFO",
        "apollo_planning.INFO",
        "artifacts/planning_route_segment_debug.jsonl",
        "planning_route_segment_debug.jsonl",
        "artifacts/control_decode_debug.jsonl",
        "artifacts/bridge_control_decode.jsonl",
        "control_decode_debug.jsonl",
    ):
        if (root / relative).exists():
            return True
    timeseries = _find_first(root, ["artifacts/debug_timeseries.csv", "debug_timeseries.csv", "timeseries.csv"])
    return _timeseries_has_reference_line_fields(timeseries)


def _reference_line_report_needs_regeneration(report: Mapping[str, Any]) -> bool:
    status = str(report.get("status") or "").strip().lower()
    blockers = {str(item) for item in report.get("blocking_reasons") or [] if item}
    evidence = report.get("evidence") if isinstance(report.get("evidence"), Mapping) else {}
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    stale_heading_blockers = {
        "planning_reference_heading_error_high",
        "planning_path_fallback_heading_error_high",
    }
    if (
        status == "fail"
        and blockers & stale_heading_blockers
        and "path_fallback_trajectory_first_theta_vs_segment_heading_p95_rad" not in metrics
    ):
        return True
    if status == "fail":
        return False
    missing_runtime = "apollo_reference_line_runtime_evidence_missing" in blockers
    missing_evidence = not any(
        evidence.get(key) is not None
        for key in (
            "planning_reference_available",
            "control_reference_available",
            "nonempty_trajectory_ratio",
        )
    )
    missing_metrics = not any(
        metrics.get(key) is not None
        for key in (
            "planning_ref_heading_error_p95_rad",
            "control_ref_heading_error_p95_rad",
            "control_lateral_error_p95_m",
        )
    )
    return status == "insufficient_data" and (
        missing_runtime or missing_evidence or missing_metrics
    )


def _timeseries_has_reference_line_fields(path: Path | None) -> bool:
    if path is None or not path.exists() or path.suffix.lower() != ".csv":
        return False
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            fields = set(csv.DictReader(handle).fieldnames or [])
    except OSError:
        return False
    return bool(
        fields
        & {
            "first_trajectory_point_theta",
            "planning_first_theta",
            "trajectory_first_point_theta",
            "debug_simple_lat_ref_heading",
            "apollo_debug_simple_lat_ref_heading",
            "debug_simple_lat_heading_error",
            "reference_line_count",
        }
    )


def build_reference_line_contract_event(row: Mapping[str, Any], *, source_confidence: str = "bridge_debug") -> dict[str, Any]:
    loc_heading = _num(_first(row, "localization_heading", "ego_heading", "map_yaw"))
    planning_theta = _num(_first(row, "first_trajectory_point_theta", "planning_first_theta", "trajectory_first_point_theta"))
    control_ref = _num(_first(row, "debug_simple_lat_ref_heading", "apollo_debug_simple_lat_ref_heading"))
    control_heading = _num(_first(row, "debug_simple_lat_heading", "apollo_debug_simple_lat_heading", "localization_heading", "ego_heading"))
    control_heading_error = _num(_first(row, "debug_simple_lat_heading_error", "apollo_debug_simple_lat_heading_error"))
    planning_error = _angle_delta(loc_heading, planning_theta)
    if control_heading_error is not None:
        control_error = abs(control_heading_error)
    else:
        control_error = _angle_delta(loc_heading, control_ref if control_ref is not None else control_heading)
    first_segment_heading = _num(_first(row, "trajectory_first_segment_heading", "first_segment_heading"))
    future_segment_heading = _num(_first(row, "trajectory_future_first_segment_heading", "future_segment_heading"))
    future_lookahead_heading = _num(
        _first(row, "trajectory_future_lookahead_heading", "future_lookahead_heading")
    )
    future_lookahead_index = _first(
        row,
        "trajectory_future_lookahead_point_index",
        "future_lookahead_point_index",
    )
    future_lookahead_distance = _num(
        _first(row, "trajectory_future_lookahead_distance_m", "future_lookahead_distance_m")
    )
    future_lookahead_time = _num(
        _first(row, "trajectory_future_lookahead_time_s", "future_lookahead_time_s")
    )
    first_nonexpired_theta = _num(
        _first(row, "trajectory_first_nonexpired_point_theta", "first_nonexpired_point_theta")
    )
    first_nonexpired_relative_time = _num(
        _first(
            row,
            "trajectory_first_nonexpired_point_relative_time",
            "first_nonexpired_point_relative_time",
        )
    )
    first_nonexpired_index = _first(
        row,
        "trajectory_first_nonexpired_point_index",
        "first_nonexpired_point_index",
    )
    expired_prefix_count = _first(row, "trajectory_expired_prefix_count", "expired_prefix_count")
    first_nonexpired_remaining_point_count = _first(
        row,
        "trajectory_first_nonexpired_remaining_point_count",
        "first_nonexpired_remaining_point_count",
    )
    reference_line_field_present = _first(row, "planning_debug_reference_line_field_present")
    if reference_line_field_present is None and "planning_debug_reference_line_count" in row:
        reference_line_field_present = True
    routing_field_present = _first(row, "planning_debug_routing_field_present")
    if routing_field_present is None and "planning_debug_routing_segment_count" in row:
        routing_field_present = True
    return {
        "timestamp": _first(row, "timestamp", "ts_sec", "sim_time"),
        "wall_time_sec": _first(row, "wall_time_sec"),
        "sim_time_sec": _first(row, "sim_time_sec", "sim_time", "ts_sec"),
        "world_frame": _first(row, "world_frame", "frame_id"),
        "localization": {
            "x": _num(_first(row, "localization_x", "ego_x", "map_x")),
            "y": _num(_first(row, "localization_y", "ego_y", "map_y")),
            "heading": loc_heading,
            "qx": _num(_first(row, "localization_orientation_qx")),
            "qy": _num(_first(row, "localization_orientation_qy")),
            "qz": _num(_first(row, "localization_orientation_qz")),
            "qw": _num(_first(row, "localization_orientation_qw")),
            "decoded_orientation_heading": _num(_first(row, "decoded_orientation_heading")),
            "orientation_heading_diff_rad": _num(_first(row, "orientation_heading_diff_rad", "decoded_orientation_heading_diff_rad")),
        },
        "routing": {
            "latest_routing_request_timestamp": _first(row, "latest_routing_request_timestamp", "last_reroute_timestamp"),
            "routing_response_status": _first(row, "routing_response_status", "routing_status"),
            "routing_road_count": _num(_first(row, "routing_road_count", "routing_last_road_count")),
            "routing_passage_count": _num(_first(row, "routing_passage_count")),
            "routing_segment_count": _num(
                _first(
                    row,
                    "routing_segment_count",
                    "route_segment_count",
                    "planning_debug_routing_segment_count",
                )
            ),
            "routing_lane_window_signature": _first(row, "routing_lane_window_signature"),
            "routing_unique_lane_signature": _first(row, "routing_unique_lane_signature"),
        },
        "planning": {
            "planning_header_timestamp_sec": _first(row, "planning_header_timestamp_sec", "planning_timestamp"),
            "planning_header_sequence_num": _first(row, "planning_header_sequence_num", "planning_lateral_latest_sequence_num"),
            "trajectory_type": _first(row, "trajectory_type"),
            "trajectory_point_count": _num(_first(row, "trajectory_point_count", "planning_lateral_latest_point_count")),
            "first_trajectory_point_x": _num(_first(row, "first_trajectory_point_x")),
            "first_trajectory_point_y": _num(_first(row, "first_trajectory_point_y")),
            "first_trajectory_point_theta": planning_theta,
            "first_trajectory_point_kappa": _num(_first(row, "first_trajectory_point_kappa")),
            "first_trajectory_point_v": _num(_first(row, "first_trajectory_point_v")),
            "trajectory_sample_points": _sample_points(_first(row, "trajectory_sample_points")),
            "trajectory_future_window_points": _sample_points(_first(row, "trajectory_future_window_points")),
            "trajectory_total_path_length": _num(_first(row, "trajectory_total_path_length")),
            "trajectory_first_segment_heading": first_segment_heading,
            "trajectory_future_first_segment_heading": future_segment_heading,
            "trajectory_future_lookahead_heading": future_lookahead_heading,
            "trajectory_future_lookahead_point_index": future_lookahead_index,
            "trajectory_future_lookahead_distance_m": future_lookahead_distance,
            "trajectory_future_lookahead_time_s": future_lookahead_time,
            "trajectory_first_nonexpired_point_theta": first_nonexpired_theta,
            "trajectory_first_nonexpired_point_relative_time": first_nonexpired_relative_time,
            "trajectory_first_nonexpired_point_index": first_nonexpired_index,
            "trajectory_first_nonexpired_remaining_point_count": first_nonexpired_remaining_point_count,
            "trajectory_expired_prefix_count": expired_prefix_count,
            "lane_ids": _list_value(_first(row, "lane_ids", "lane_id_first")),
            "target_lane_ids": _list_value(_first(row, "target_lane_ids", "target_lane_id_first")),
            "reference_line_count": _num(
                _first(row, "reference_line_count", "planning_debug_reference_line_count")
            ),
            "reference_line_length_max": _num(_first(row, "reference_line_length_max", "reference_line_length")),
            "routing_segment_count": _num(
                _first(
                    row,
                    "routing_segment_count",
                    "route_segment_count",
                    "planning_debug_routing_segment_count",
                )
            ),
            "lane_follow_map_status": _first(row, "lane_follow_map_status"),
            "reference_line_provider_status": _first(row, "reference_line_provider_status"),
            "create_route_segments_status": _first(row, "create_route_segments_status"),
            "planning_empty_reason_guess": _first(row, "planning_empty_reason_guess"),
            "path_end_like_condition": _bool_value(_first(row, "path_end_like_condition")),
            "planning_stage_name": _first(row, "planning_stage_name", "stage_plugin_type"),
            "task_name": _first(row, "task_name", "scenario_plugin_type"),
        },
        "planning_debug_presence": {
            "debug_present": _first(row, "planning_debug_debug_present"),
            "planning_data_present": _first(row, "planning_debug_planning_data_present"),
            "reference_line_path": _first(row, "planning_debug_reference_line_path"),
            "reference_line_field_present": reference_line_field_present,
            "reference_line_count": _num(_first(row, "planning_debug_reference_line_count")),
            "routing_path": _first(row, "planning_debug_routing_path"),
            "routing_field_present": routing_field_present,
            "routing_segment_count": _num(_first(row, "planning_debug_routing_segment_count")),
            "diagnosis": _first(row, "planning_debug_diagnosis"),
            "last_field_inventory": _mapping(_first(row, "planning_debug_field_inventory")),
            "path_candidate_summary": _mapping(_first(row, "planning_debug_path_candidate_summary")),
        },
        "control": {
            "debug_simple_lat_lateral_error": _num(_first(row, "debug_simple_lat_lateral_error", "apollo_debug_simple_lat_lateral_error")),
            "debug_simple_lat_ref_heading": control_ref,
            "debug_simple_lat_heading": control_heading,
            "debug_simple_lat_heading_error": control_heading_error,
            "debug_simple_lat_current_target_point_x": _num(_first(row, "debug_simple_lat_current_target_point_x")),
            "debug_simple_lat_current_target_point_y": _num(_first(row, "debug_simple_lat_current_target_point_y")),
            "debug_simple_lat_current_target_point_theta": _num(_first(row, "debug_simple_lat_current_target_point_theta")),
            "debug_simple_lat_current_reference_point_x": _num(_first(row, "debug_simple_lat_current_reference_point_x")),
            "debug_simple_lat_current_reference_point_y": _num(_first(row, "debug_simple_lat_current_reference_point_y")),
            "debug_simple_lat_current_reference_point_theta": _num(_first(row, "debug_simple_lat_current_reference_point_theta")),
            "debug_simple_mpc_current_reference_point_x": _num(_first(row, "debug_simple_mpc_current_reference_point_x")),
            "debug_simple_mpc_current_reference_point_y": _num(_first(row, "debug_simple_mpc_current_reference_point_y")),
            "debug_simple_mpc_current_reference_point_theta": _num(_first(row, "debug_simple_mpc_current_reference_point_theta")),
        },
        "computed": {
            "localization_to_planning_first_heading_error_rad": planning_error,
            "localization_to_control_ref_heading_error_rad": control_error,
            "localization_to_control_lateral_error_m": _num(_first(row, "debug_simple_lat_lateral_error", "apollo_debug_simple_lat_lateral_error", "cross_track_error")),
            "trajectory_first_theta_vs_segment_heading_p95_rad": _angle_delta(planning_theta, first_segment_heading),
            "trajectory_first_nonexpired_theta_vs_future_segment_heading_rad": _angle_delta(
                first_nonexpired_theta,
                future_segment_heading,
            ),
            "trajectory_first_nonexpired_theta_vs_future_lookahead_heading_rad": _angle_delta(
                first_nonexpired_theta,
                future_lookahead_heading,
            ),
            "planning_reference_available": planning_theta is not None or _num(_first(row, "reference_line_count")) not in {None, 0.0},
            "control_reference_available": control_ref is not None or control_heading_error is not None,
            "apollo_reference_line_verified_candidate": False,
            "source_confidence": source_confidence,
        },
    }


def apollo_reference_line_contract_summary_md(report: Mapping[str, Any]) -> str:
    evidence = _mapping(report.get("evidence"))
    metrics = _mapping(report.get("metrics"))
    hdmap_projection = _mapping(report.get("apollo_hdmap_projection"))
    contracts = _mapping(report.get("contracts"))
    planning_contract = _mapping(contracts.get("planning_trajectory"))
    control_contract = _mapping(contracts.get("control_reference"))
    projection_contract = _mapping(contracts.get("apollo_hdmap_projection"))
    planning_projection_alignment = _mapping(report.get("planning_first_point_projection_alignment"))
    reference_debug_export_policy = _mapping(report.get("reference_line_debug_export_policy"))
    planning_debug_presence = _mapping(report.get("planning_debug_presence"))
    materialization_summary = _mapping(report.get("planning_materialization_summary"))
    materialization_claim_window = _mapping(materialization_summary.get("claim_window"))
    planning_debug_path_candidate = _mapping(report.get("planning_debug_path_candidate_evidence"))
    path_candidate_hdmap_alignment = _mapping(report.get("planning_debug_path_candidate_hdmap_projection_alignment"))
    trajectory_sample_surrogate = _mapping(report.get("planning_trajectory_sample_surrogate"))
    control_target_surrogate = _mapping(report.get("control_target_point_vs_planning_trajectory_sample"))
    control_target_path_candidate = _mapping(report.get("control_target_point_vs_planning_path_candidate_sample"))
    path_candidate_trajectory_surrogate = _mapping(report.get("planning_debug_path_candidate_vs_trajectory_sample"))
    planning_info_log_evidence = _mapping(report.get("planning_info_log_reference_line_evidence"))
    planning_debug_field_inventory = _mapping(planning_debug_presence.get("last_field_inventory"))
    field_inventory = _mapping(_mapping(report.get("reference_debug_diagnostic")).get("field_inventory"))
    claim_window_inventory = _mapping(field_inventory.get("claim_window"))
    return "\n".join(
        [
            "# Apollo Reference-Line Contract Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            f"- Planning reference available: `{evidence.get('planning_reference_available')}`",
            f"- Control reference available: `{evidence.get('control_reference_available')}`",
            f"- Planning trajectory contract: `{planning_contract.get('status')}`",
            f"- Control reference contract: `{control_contract.get('status')}`",
            f"- Apollo HDMap projection contract: `{projection_contract.get('status')}`",
            f"- Non-empty trajectory ratio: `{evidence.get('nonempty_trajectory_ratio')}`",
            f"- Reference-line provider ready ratio: `{evidence.get('reference_line_provider_ready_ratio')}`",
            f"- Reference debug classification: `{_mapping(report.get('reference_debug_diagnostic')).get('classification')}`",
            f"- Planning debug presence classification: `{planning_debug_presence.get('classification')}`",
            f"- Planning debug reference-line nonempty ratio: `{planning_debug_presence.get('reference_line_nonempty_ratio')}`",
            f"- Planning debug routing segment nonempty ratio: `{planning_debug_presence.get('routing_segment_nonempty_ratio')}`",
            f"- Planning debug candidate paths: `{planning_debug_field_inventory.get('reference_line_candidate_paths')}`",
            f"- Planning debug path candidate classification: `{planning_debug_path_candidate.get('classification')}`",
            f"- Planning debug path candidate max count: `{planning_debug_path_candidate.get('path_candidate_max_count')}`",
            f"- Planning debug path point-sequence candidates: `{planning_debug_path_candidate.get('point_sequence_candidate_count')}`",
            f"- Planning debug path candidate claim-grade allowed: `{planning_debug_path_candidate.get('reference_line_claim_grade_allowed')}`",
            f"- Planning debug path HDMap projection alignment: `{path_candidate_hdmap_alignment.get('classification')}`",
            f"- Planning debug path HDMap lane-l p95 m: `{path_candidate_hdmap_alignment.get('path_candidate_lane_l_abs_p95_m')}`",
            f"- Planning debug path HDMap routing-lane compatible: `{path_candidate_hdmap_alignment.get('routing_lane_window_compatible')}`",
            f"- Planning debug path HDMap claim-grade allowed: `{path_candidate_hdmap_alignment.get('reference_line_claim_grade_allowed')}`",
            f"- Planning debug path vs trajectory sample: `{path_candidate_trajectory_surrogate.get('classification')}`",
            f"- Planning debug path vs trajectory p95 m: `{path_candidate_trajectory_surrogate.get('path_candidate_to_planning_sample_line_abs_p95_m')}`",
            f"- Planning debug path vs trajectory claim-grade allowed: `{path_candidate_trajectory_surrogate.get('reference_line_claim_grade_allowed')}`",
            f"- Planning materialization classification: `{materialization_summary.get('classification')}`",
            f"- Planning materialization claim-window source: `{materialization_summary.get('claim_window_source')}`",
            f"- Planning materialization route-segments ready ratio: `{materialization_claim_window.get('route_segments_ready_ratio')}`",
            f"- Planning materialization reference-line empty while ready ratio: `{materialization_claim_window.get('reference_line_empty_with_route_segments_ready_ratio')}`",
            f"- Planning materialization lane-follow stage ratio: `{materialization_claim_window.get('lane_follow_stage_ratio')}`",
            f"- Planning materialization stage topk: `{materialization_claim_window.get('planning_stage_name_topk')}`",
            f"- Planning materialization task topk: `{materialization_claim_window.get('task_name_topk')}`",
            f"- Reference debug field inventory: `{field_inventory.get('field_gap_classification')}`",
            f"- Reference debug counter positive rows: `{field_inventory.get('reference_line_count_positive_count')}`",
            f"- Reference debug claim-window source: `{field_inventory.get('claim_window_source')}`",
            f"- Claim-window reference debug counter positive rows: `{claim_window_inventory.get('reference_line_count_positive_count')}`",
            f"- Claim-window reference debug provider status: `{claim_window_inventory.get('reference_line_provider_status_topk')}`",
            f"- Planning route-segment positive rows: `{field_inventory.get('planning_route_segment_count_positive_count')}`",
            f"- Routing segment positive rows: `{field_inventory.get('routing_segment_count_positive_count')}`",
            f"- Control simple_lat reference available: `{_mapping(report.get('reference_debug_diagnostic')).get('control_simple_lat_reference_available')}`",
            f"- Reference debug export policy: `{reference_debug_export_policy.get('classification')}`",
            f"- Reference debug claim-grade allowed: `{reference_debug_export_policy.get('reference_line_debug_claim_grade_allowed')}`",
            f"- Reference debug evidence policy: `{reference_debug_export_policy.get('recommended_evidence_policy')}`",
            f"- Planning INFO log reference-line evidence: `{planning_info_log_evidence.get('classification')}`",
            f"- Planning INFO log reference-line prints present: `{planning_info_log_evidence.get('reference_line_text_prints_present')}`",
            f"- Planning INFO log reference-line claim-grade allowed: `{planning_info_log_evidence.get('text_log_reference_line_claim_grade_allowed')}`",
            f"- Planning trajectory sample surrogate: `{trajectory_sample_surrogate.get('classification')}`",
            f"- Planning trajectory sample claim-grade allowed: `{trajectory_sample_surrogate.get('reference_line_claim_grade_allowed')}`",
            f"- Control target vs Planning sample: `{control_target_surrogate.get('classification')}`",
            f"- Control target vs Planning sample p95 m: `{control_target_surrogate.get('target_point_to_planning_sample_line_abs_p95_m')}`",
            f"- Control target vs Planning path candidate: `{control_target_path_candidate.get('classification')}`",
            f"- Control target vs path candidate p95 m: `{control_target_path_candidate.get('target_point_to_path_candidate_line_abs_p95_m')}`",
            f"- Control target lane-l p95 m: `{control_target_path_candidate.get('target_point_lane_l_abs_p95_m')}`",
            f"- Planning ref heading p95 rad: `{metrics.get('planning_ref_heading_error_p95_rad')}`",
            f"- Normal trajectory heading p95 rad: `{metrics.get('normal_trajectory_heading_error_p95_rad')}`",
            f"- PATH_FALLBACK trajectory ratio: `{metrics.get('path_fallback_trajectory_ratio')}`",
            f"- PATH_FALLBACK heading p95 rad: `{metrics.get('path_fallback_heading_error_p95_rad')}`",
            f"- PATH_FALLBACK trajectory-vs-segment heading p95 rad: `{metrics.get('path_fallback_trajectory_first_theta_vs_segment_heading_p95_rad')}`",
            f"- PATH_FALLBACK future short-segment heading p95 rad: `{metrics.get('path_fallback_first_nonexpired_theta_vs_future_segment_heading_p95_rad')}`",
            f"- PATH_FALLBACK future lookahead heading p95 rad: `{metrics.get('path_fallback_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad')}`",
            f"- PATH_FALLBACK future lookahead distance p95 m: `{metrics.get('path_fallback_future_lookahead_distance_p95_m')}`",
            f"- PATH_FALLBACK future lookahead min distance m: `{metrics.get('path_fallback_future_lookahead_distance_min_m')}`",
            f"- PATH_FALLBACK future lookahead <1m ratio: `{metrics.get('path_fallback_future_lookahead_distance_lt_1m_ratio')}`",
            f"- PATH_FALLBACK future lookahead reverse-heading ratio: `{metrics.get('path_fallback_future_lookahead_reverse_heading_ratio')}`",
            f"- Normal trajectory-vs-segment heading p95 rad: `{metrics.get('normal_trajectory_first_theta_vs_segment_heading_p95_rad')}`",
            f"- Normal future short-segment heading p95 rad: `{metrics.get('normal_first_nonexpired_theta_vs_future_segment_heading_p95_rad')}`",
            f"- Normal future lookahead heading p95 rad: `{metrics.get('normal_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad')}`",
            f"- Control ref heading p95 rad: `{metrics.get('control_ref_heading_error_p95_rad')}`",
            f"- Control lateral error p95 m: `{metrics.get('control_lateral_error_p95_m')}`",
            f"- Apollo HDMap projection status: `{hdmap_projection.get('status')}`",
            f"- Apollo HDMap projection claim-grade: `{hdmap_projection.get('claim_grade')}`",
            f"- Apollo HDMap projection heading p95 rad: `{hdmap_projection.get('heading_error_p95_rad')}`",
            f"- Apollo HDMap projection lateral p95 m: `{hdmap_projection.get('lateral_error_p95_m')}`",
            f"- Planning first point projection classification: `{planning_projection_alignment.get('classification')}`",
            f"- Planning first point lane-l p95 m: `{planning_projection_alignment.get('planning_first_point_lane_l_abs_p95_m')}`",
            f"- Planning first point lane-heading p95 rad: `{planning_projection_alignment.get('planning_first_point_lane_heading_error_p95_rad')}`",
            "",
            INTERPRETATION_BOUNDARY,
            "",
        ]
    )


def _report(
    *,
    status: str,
    blocking_reasons: list[str],
    warnings: list[str],
    rows: Sequence[Mapping[str, Any]],
    localization_contract: Mapping[str, Any] | None,
    planning_materialization: Mapping[str, Any] | None,
    planning_debug_summary: Mapping[str, Any] | None,
    planning_info_log_reference_line_evidence: Mapping[str, Any] | None,
    route_contract: Mapping[str, Any] | None,
    hdmap_projection_rows: Sequence[Mapping[str, Any]] | None,
    hdmap_projection_artifact_exists: bool | None,
    source: Mapping[str, Any] | None,
    contracts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    hdmap_projection = summarize_apollo_hdmap_projection(
        hdmap_projection_rows or [],
        artifact_file_exists=hdmap_projection_artifact_exists,
    )
    evidence = _evidence(rows, hdmap_projection)
    metrics = _metrics(rows)
    planning_projection_alignment = _planning_first_point_projection_alignment(
        rows,
        hdmap_projection_rows or [],
    )
    planning_debug_presence = _planning_debug_presence_summary(rows, planning_debug_summary)
    planning_debug_path_candidate = _planning_debug_path_candidate_evidence(
        rows,
        planning_debug_presence,
    )
    path_candidate_hdmap_alignment = _planning_debug_path_candidate_hdmap_projection_alignment(
        rows,
        hdmap_projection_rows or [],
    )
    planning_materialization_summary = _planning_materialization_summary(rows)
    reference_debug_diagnostic = _reference_debug_diagnostic(
        rows,
        evidence,
        metrics,
        planning_debug_presence=planning_debug_presence,
        planning_projection_alignment=planning_projection_alignment,
    )
    trajectory_sample_surrogate = _planning_trajectory_sample_surrogate(rows)
    control_target_surrogate = _control_target_point_vs_planning_trajectory_sample(rows)
    control_target_path_candidate = _control_target_point_vs_planning_path_candidate_sample(
        rows,
        hdmap_projection_rows or [],
    )
    path_candidate_trajectory_surrogate = _planning_debug_path_candidate_vs_trajectory_sample(rows)
    reference_debug_export_policy = _reference_line_debug_export_policy(
        reference_debug_diagnostic,
        trajectory_sample_surrogate=trajectory_sample_surrogate,
        planning_materialization_summary=planning_materialization_summary,
        planning_info_log_reference_line_evidence=planning_info_log_reference_line_evidence,
    )
    report_contracts = dict(contracts or _contracts(rows=rows, evidence=evidence, metrics=metrics, hdmap_projection=hdmap_projection))
    status, warnings = _downgrade_for_planning_materialization(
        status,
        warnings=list(warnings),
        planning_materialization=planning_materialization,
    )
    status, warnings, blocking_reasons = _gate_on_route_contract(
        status,
        warnings=warnings,
        blocking_reasons=list(blocking_reasons),
        route_contract=route_contract,
    )
    if planning_debug_presence.get("classification") == "routing_present_reference_line_empty":
        warnings.append("planning_debug_presence_routing_present_reference_line_empty")
    if planning_materialization_summary.get("classification") in {
        "route_segments_ready_reference_line_empty",
        "route_segments_ready_trajectory_nonzero_reference_line_empty",
    }:
        warnings.append("planning_materialization_route_segments_ready_reference_line_empty")
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "warnings": sorted(set(warnings)),
        "evidence": evidence,
        "metrics": metrics,
        "reference_debug_diagnostic": reference_debug_diagnostic,
        "planning_debug_presence": planning_debug_presence,
        "planning_debug_path_candidate_evidence": planning_debug_path_candidate,
        "planning_debug_path_candidate_hdmap_projection_alignment": path_candidate_hdmap_alignment,
        "planning_materialization_summary": planning_materialization_summary,
        "planning_info_log_reference_line_evidence": dict(
            planning_info_log_reference_line_evidence or {}
        ),
        "reference_line_debug_export_policy": reference_debug_export_policy,
        "planning_trajectory_sample_surrogate": trajectory_sample_surrogate,
        "control_target_point_vs_planning_trajectory_sample": control_target_surrogate,
        "control_target_point_vs_planning_path_candidate_sample": control_target_path_candidate,
        "planning_debug_path_candidate_vs_trajectory_sample": path_candidate_trajectory_surrogate,
        "planning_first_point_projection_alignment": planning_projection_alignment,
        "contracts": report_contracts,
        "planning_trajectory_contract": report_contracts.get("planning_trajectory") or {},
        "control_reference_contract": report_contracts.get("control_reference") or {},
        "apollo_hdmap_projection_contract": report_contracts.get("apollo_hdmap_projection") or {},
        "lane_ids": _lane_ids(rows),
        "apollo_hdmap_projection": hdmap_projection,
        "localization_contract_status": _report_status(localization_contract),
        "planning_materialization_status": _report_status(planning_materialization),
        "apollo_route_contract_status": _report_status(route_contract),
        "source": dict(source or {}),
        "interpretation_boundary": INTERPRETATION_BOUNDARY,
    }


def _contracts(
    *,
    rows: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Any],
    metrics: Mapping[str, Any],
    hdmap_projection: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "planning_trajectory": _planning_trajectory_contract(rows, evidence, metrics),
        "control_reference": _control_reference_contract(evidence, metrics),
        "apollo_hdmap_projection": _apollo_hdmap_projection_contract(hdmap_projection),
        "apollo_hdmap_projection_lane_compatibility": _lane_compatibility_contract(
            rows,
            hdmap_projection,
        ),
    }


def _planning_trajectory_contract(
    rows: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    blocking: list[str] = []
    missing: list[str] = []
    available = bool(evidence.get("planning_reference_available"))
    nonempty_ratio = _num(evidence.get("nonempty_trajectory_ratio"))
    claim_window_ratio = _num(evidence.get("planning_claim_window_nonempty_trajectory_ratio"))
    effective_nonempty_ratio = claim_window_ratio if claim_window_ratio is not None else nonempty_ratio
    reference_line_zero_ratio = _num(metrics.get("reference_line_count_zero_ratio"))
    ready_ratio = _num(evidence.get("reference_line_provider_ready_ratio"))
    planning_heading_p95 = _num(metrics.get("planning_ref_heading_error_p95_rad"))
    normal_heading_p95 = _num(metrics.get("normal_trajectory_heading_error_p95_rad"))
    path_fallback_ratio = _num(metrics.get("path_fallback_trajectory_ratio"))
    path_fallback_heading_p95 = _num(metrics.get("path_fallback_heading_error_p95_rad"))
    path_fallback_segment_heading_p95 = _num(
        metrics.get("path_fallback_trajectory_first_theta_vs_segment_heading_p95_rad")
    )
    path_fallback_future_segment_heading_p95 = _num(
        metrics.get("path_fallback_first_nonexpired_theta_vs_future_segment_heading_p95_rad")
    )
    path_fallback_future_lookahead_heading_p95 = _num(
        metrics.get("path_fallback_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad")
    )
    path_fallback_future_lookahead_distance_p95 = _num(
        metrics.get("path_fallback_future_lookahead_distance_p95_m")
    )
    path_fallback_future_lookahead_distance_min = _num(
        metrics.get("path_fallback_future_lookahead_distance_min_m")
    )
    path_fallback_future_lookahead_distance_lt_1m_ratio = _num(
        metrics.get("path_fallback_future_lookahead_distance_lt_1m_ratio")
    )
    path_fallback_future_lookahead_reverse_heading_ratio = _num(
        metrics.get("path_fallback_future_lookahead_reverse_heading_ratio")
    )
    trajectory_segment_heading_p95 = _num(
        metrics.get("trajectory_first_theta_vs_segment_heading_p95_rad")
    )
    trajectory_reference_self_consistent = (
        trajectory_segment_heading_p95 is not None
        and trajectory_segment_heading_p95 < PASS_HEADING_RAD
    )
    claim_window_materialized = (
        effective_nonempty_ratio is not None
        and effective_nonempty_ratio >= MIN_NONEMPTY_TRAJECTORY_RATIO
    )
    heading_evidence_available = planning_heading_p95 is not None
    ego_heading_diverged_from_aligned_reference = (
        planning_heading_p95 is not None
        and planning_heading_p95 >= FAIL_HEADING_RAD
        and trajectory_reference_self_consistent
    )

    if not rows:
        missing.append("planning_trajectory_rows")
    if not available:
        missing.append("planning_trajectory_reference")
    if available and effective_nonempty_ratio is not None and effective_nonempty_ratio < MIN_NONEMPTY_TRAJECTORY_RATIO:
        if effective_nonempty_ratio <= 0.0 and (reference_line_zero_ratio or 0.0) >= 0.50:
            blocking.append("planning_trajectory_empty_with_zero_reference_line_count")
        else:
            warnings.append("nonempty_trajectory_ratio_low")
    if available and reference_line_zero_ratio is not None and reference_line_zero_ratio >= 0.50:
        if claim_window_materialized and heading_evidence_available:
            pass
        elif effective_nonempty_ratio is not None and effective_nonempty_ratio > 0.0:
            warnings.append("reference_line_count_zero_debug_counter_with_nonempty_trajectory")
    if (
        ready_ratio is not None
        and ready_ratio < MIN_PROVIDER_READY_RATIO
        and not (claim_window_materialized and heading_evidence_available)
    ):
        warnings.append("reference_line_provider_ready_ratio_low")
    fallback_driven_heading_error = (
        planning_heading_p95 is not None
        and planning_heading_p95 >= FAIL_HEADING_RAD
        and path_fallback_ratio is not None
        and path_fallback_ratio > 0.0
        and path_fallback_heading_p95 is not None
        and path_fallback_heading_p95 >= FAIL_HEADING_RAD
        and (normal_heading_p95 is None or normal_heading_p95 < FAIL_HEADING_RAD)
    )
    path_fallback_segment_metric = (
        path_fallback_future_lookahead_heading_p95
        if path_fallback_future_lookahead_heading_p95 is not None
        else (
            path_fallback_future_segment_heading_p95
            if path_fallback_future_segment_heading_p95 is not None
            else path_fallback_segment_heading_p95
        )
    )
    path_fallback_segment_heading_mismatch = (
        path_fallback_ratio is not None
        and path_fallback_ratio > 0.0
        and path_fallback_segment_metric is not None
        and path_fallback_segment_metric >= FAIL_HEADING_RAD
    )
    if path_fallback_segment_heading_mismatch:
        blocking.append("planning_path_fallback_segment_heading_mismatch")
    elif ego_heading_diverged_from_aligned_reference:
        blocking.append("ego_heading_diverged_from_aligned_planning_reference")
    elif fallback_driven_heading_error:
        blocking.append("planning_path_fallback_heading_error_high")
    else:
        _apply_heading_threshold(
            planning_heading_p95,
            warnings=warnings,
            blocking=blocking,
            warn_reason="planning_reference_heading_error_elevated",
            fail_reason="planning_reference_heading_error_high",
        )
    if path_fallback_ratio is not None and path_fallback_ratio > 0.0:
        warnings.append("planning_path_fallback_trajectory_present")
    if (
        path_fallback_future_lookahead_reverse_heading_ratio is not None
        and path_fallback_future_lookahead_reverse_heading_ratio > 0.0
    ):
        warnings.append("planning_path_fallback_lookahead_reverse_heading_present")
    if (
        path_fallback_future_lookahead_distance_lt_1m_ratio is not None
        and path_fallback_future_lookahead_distance_lt_1m_ratio >= 0.50
    ):
        warnings.append("planning_path_fallback_lookahead_distance_short")
    if available and planning_heading_p95 is None:
        missing.append("planning_ref_heading_error_p95_rad")
    status = _contract_status(
        available=available,
        blocking=blocking,
        warnings=warnings,
        missing=missing,
    )
    return {
        "status": status,
        "available": available,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "key_metrics": {
            "nonempty_trajectory_ratio": nonempty_ratio,
            "planning_claim_window_nonempty_trajectory_ratio": claim_window_ratio,
            "planning_claim_window_source": evidence.get("planning_claim_window_source"),
            "effective_nonempty_trajectory_ratio": effective_nonempty_ratio,
            "nonempty_trajectory_ratio_after_routing_segment_available": evidence.get(
                "nonempty_trajectory_ratio_after_routing_segment_available"
            ),
            "nonempty_trajectory_ratio_after_first_nonempty": evidence.get(
                "nonempty_trajectory_ratio_after_first_nonempty"
            ),
            "reference_line_provider_ready_ratio": ready_ratio,
            "planning_ref_heading_error_p95_rad": planning_heading_p95,
            "normal_trajectory_heading_error_p95_rad": normal_heading_p95,
            "path_fallback_trajectory_ratio": path_fallback_ratio,
            "path_fallback_heading_error_p95_rad": path_fallback_heading_p95,
            "path_fallback_trajectory_first_theta_vs_segment_heading_p95_rad": path_fallback_segment_heading_p95,
            "path_fallback_first_nonexpired_theta_vs_future_segment_heading_p95_rad": path_fallback_future_segment_heading_p95,
            "path_fallback_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad": path_fallback_future_lookahead_heading_p95,
            "path_fallback_future_lookahead_distance_p95_m": path_fallback_future_lookahead_distance_p95,
            "path_fallback_future_lookahead_distance_min_m": path_fallback_future_lookahead_distance_min,
            "path_fallback_future_lookahead_distance_lt_1m_ratio": path_fallback_future_lookahead_distance_lt_1m_ratio,
            "path_fallback_future_lookahead_reverse_heading_ratio": path_fallback_future_lookahead_reverse_heading_ratio,
            "path_fallback_segment_heading_metric_source": (
                "first_nonexpired_theta_vs_future_lookahead"
                if path_fallback_future_lookahead_heading_p95 is not None
                else (
                    "first_nonexpired_theta_vs_future_segment"
                    if path_fallback_future_segment_heading_p95 is not None
                    else "first_theta_vs_first_segment"
                )
            ),
            "path_fallback_first_kappa_p95_abs": metrics.get("path_fallback_first_kappa_p95_abs"),
            "path_fallback_onset": metrics.get("path_fallback_onset"),
            "normal_trajectory_first_theta_vs_segment_heading_p95_rad": metrics.get(
                "normal_trajectory_first_theta_vs_segment_heading_p95_rad"
            ),
            "normal_first_nonexpired_theta_vs_future_segment_heading_p95_rad": metrics.get(
                "normal_first_nonexpired_theta_vs_future_segment_heading_p95_rad"
            ),
            "normal_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad": metrics.get(
                "normal_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad"
            ),
            "reference_line_count_zero_ratio": reference_line_zero_ratio,
            "routing_segment_count_zero_ratio": metrics.get("routing_segment_count_zero_ratio"),
            "trajectory_first_theta_vs_segment_heading_p95_rad": metrics.get("trajectory_first_theta_vs_segment_heading_p95_rad"),
            "trajectory_reference_heading_self_consistent": trajectory_reference_self_consistent,
        },
        "interpretation": (
            "Planning trajectory evidence checks Apollo trajectory continuity and heading semantics. "
            "It does not by itself prove Apollo HDMap lane projection is verified."
        ),
    }


def _control_reference_contract(
    evidence: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    blocking: list[str] = []
    missing: list[str] = []
    available = bool(evidence.get("control_reference_available"))
    control_heading_p95 = _num(metrics.get("control_ref_heading_error_p95_rad"))
    lateral_p95 = _num(metrics.get("control_lateral_error_p95_m"))
    if not available:
        missing.append("control_reference_debug")
    _apply_heading_threshold(
        control_heading_p95,
        warnings=warnings,
        blocking=blocking,
        warn_reason="control_reference_heading_error_elevated",
        fail_reason="reference_line_heading_error_high",
    )
    _apply_lateral_threshold(lateral_p95, warnings=warnings, blocking=blocking)
    if available and control_heading_p95 is None:
        missing.append("control_ref_heading_error_p95_rad")
    status = _contract_status(
        available=available,
        blocking=blocking,
        warnings=warnings,
        missing=missing,
    )
    return {
        "status": status,
        "available": available,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "key_metrics": {
            "control_ref_heading_error_p95_rad": control_heading_p95,
            "control_lateral_error_p95_m": lateral_p95,
        },
        "interpretation": (
            "Control reference evidence checks Apollo control debug reference semantics. "
            "Missing control reference does not prove Planning failed, but it blocks claim-grade attribution."
        ),
    }


def _apollo_hdmap_projection_contract(hdmap_projection: Mapping[str, Any]) -> dict[str, Any]:
    status = str(hdmap_projection.get("status") or "insufficient_data")
    warnings = list(hdmap_projection.get("warnings") or [])
    blocking = list(hdmap_projection.get("blocking_reasons") or [])
    insufficient = list(hdmap_projection.get("insufficient_reasons") or [])
    missing = list(hdmap_projection.get("missing_fields") or [])
    available = bool(hdmap_projection.get("official_source_available"))
    return {
        "status": status,
        "available": available,
        "claim_grade": bool(hdmap_projection.get("claim_grade")),
        "blocking_reasons": sorted(set(blocking)),
        "insufficient_reasons": sorted(set(insufficient)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "key_metrics": {
            "file_present": hdmap_projection.get("file_present"),
            "official_source_available": hdmap_projection.get("official_source_available"),
            "ok_ratio": hdmap_projection.get("ok_ratio"),
            "heading_error_p95_rad": hdmap_projection.get("heading_error_p95_rad"),
            "lateral_error_p95_m": hdmap_projection.get("lateral_error_p95_m"),
            "nearest_lane_id_topk": hdmap_projection.get("nearest_lane_id_topk"),
        },
        "interpretation": (
            "Claim-grade HDMap projection requires source=apollo_hdmap_api. "
            "Planner trajectory evidence or CARLA route projection cannot substitute for this layer."
        ),
    }


def _lane_compatibility_contract(
    rows: Sequence[Mapping[str, Any]],
    hdmap_projection: Mapping[str, Any],
) -> dict[str, Any]:
    projection_lane_ids = [str(item) for item in hdmap_projection.get("nearest_lane_id_topk") or [] if item]
    planning_lane_ids = [
        lane_id
        for row in rows
        for lane_id in (
            list(_list_value(_nested(row, "planning.lane_ids")))
            + list(_list_value(_nested(row, "planning.target_lane_ids")))
        )
        if lane_id
    ]
    normalized_projection = {_normalize_lane_id(item) for item in projection_lane_ids}
    normalized_planning = {_normalize_lane_id(item) for item in planning_lane_ids}
    warnings: list[str] = []
    blocking: list[str] = []
    missing: list[str] = []
    available = bool(projection_lane_ids and planning_lane_ids)
    compatible = bool(normalized_projection & normalized_planning) if available else None
    if not projection_lane_ids:
        missing.append("apollo_hdmap_projection.nearest_lane_id")
    if not planning_lane_ids:
        missing.append("planning.lane_ids_or_target_lane_ids")
    if available and compatible is False:
        blocking.append("apollo_hdmap_projection_lane_id_not_compatible_with_planning")
    status = _contract_status(
        available=available,
        blocking=blocking,
        warnings=warnings,
        missing=missing,
    )
    return {
        "status": status,
        "available": available,
        "compatible": compatible,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "key_metrics": {
            "projection_lane_id_topk": projection_lane_ids,
            "planning_lane_id_topk": _topk(planning_lane_ids),
            "normalized_projection_lane_ids": sorted(normalized_projection),
            "normalized_planning_lane_ids": sorted(normalized_planning),
        },
        "interpretation": (
            "Claim-grade reference-line evidence requires Apollo HDMap projection lane ids "
            "to be compatible with Planning lane_id or target_lane_id evidence."
        ),
    }


def _contract_status(
    *,
    available: bool,
    blocking: Sequence[str],
    warnings: Sequence[str],
    missing: Sequence[str],
) -> str:
    if blocking:
        return "fail"
    if not available or missing:
        return "insufficient_data"
    return "warn" if warnings else "pass"


def _contract_messages(contracts: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    blocking: list[str] = []
    for name, contract in contracts.items():
        if not isinstance(contract, Mapping):
            continue
        warnings.extend(str(item) for item in contract.get("warnings") or [] if item)
        if contract.get("status") == "insufficient_data":
            for item in contract.get("insufficient_reasons") or []:
                if item:
                    warnings.append(str(item))
            for item in contract.get("missing_fields") or []:
                if not item:
                    continue
                if name == "apollo_hdmap_projection" and item == "apollo_hdmap_projection":
                    warnings.append("apollo_hdmap_projection_missing")
                else:
                    warnings.append(f"{name}.{item}")
        blocking.extend(str(item) for item in contract.get("blocking_reasons") or [] if item)
    return warnings, blocking


def _overall_status(
    contracts: Mapping[str, Any],
    *,
    extra_blocking: Sequence[str],
    extra_warnings: Sequence[str],
) -> str:
    statuses = [
        str(contract.get("status") or "insufficient_data")
        for contract in contracts.values()
        if isinstance(contract, Mapping)
    ]
    if extra_blocking or "fail" in statuses:
        return "fail"
    if "insufficient_data" in statuses:
        return "insufficient_data"
    if extra_warnings or "warn" in statuses:
        return "warn"
    return "pass"


def _downgrade_for_planning_materialization(
    status: str,
    *,
    warnings: list[str],
    planning_materialization: Mapping[str, Any] | None,
) -> tuple[str, list[str]]:
    if not isinstance(planning_materialization, Mapping) or not planning_materialization:
        return status, warnings
    materialization_status = _report_status(planning_materialization)
    if materialization_status != "fail":
        return status, warnings
    warnings.append("planning_materialization_failed_reference_line_claim_downgraded")
    if status in {"pass", "warn"}:
        return "insufficient_data", warnings
    return status, warnings


def _gate_on_route_contract(
    status: str,
    *,
    warnings: list[str],
    blocking_reasons: list[str],
    route_contract: Mapping[str, Any] | None,
) -> tuple[str, list[str], list[str]]:
    if not isinstance(route_contract, Mapping) or not route_contract:
        warnings.append("apollo_route_contract_missing_reference_line_claim_downgraded")
        if status in {"pass", "warn"}:
            return "insufficient_data", warnings, blocking_reasons
        return status, warnings, blocking_reasons
    route_status = _report_status(route_contract)
    if route_status == "pass":
        return status, warnings, blocking_reasons
    route_blocking = list(route_contract.get("blocking_reasons") or [])
    if route_status == "warn" and not route_blocking:
        warnings.append("apollo_route_contract_warn_reference_line_claim_warning_propagated")
        if status == "pass":
            return "warn", warnings, blocking_reasons
        return status, warnings, blocking_reasons
    reason = f"apollo_route_contract_{route_status or 'unknown'}"
    if route_status == "fail":
        blocking_reasons.append("apollo_route_contract_not_pass")
        if status in {"pass", "warn", "insufficient_data"}:
            return "fail", warnings, blocking_reasons
        return status, warnings, blocking_reasons
    warnings.append(f"{reason}_reference_line_claim_downgraded")
    if status in {"pass", "warn"}:
        return "insufficient_data", warnings, blocking_reasons
    return status, warnings, blocking_reasons


def _evidence(rows: Sequence[Mapping[str, Any]], hdmap_projection: Mapping[str, Any]) -> dict[str, Any]:
    total = len(rows)
    if total <= 0:
        return {
            "planning_reference_available": False,
            "control_reference_available": False,
            "apollo_hdmap_projection_file_present": bool(hdmap_projection.get("file_present")),
            "apollo_hdmap_projection_available": bool(hdmap_projection.get("official_source_available")),
            "apollo_hdmap_projection_claim_grade": bool(hdmap_projection.get("claim_grade")),
            "reference_line_provider_ready_ratio": None,
            "nonempty_trajectory_ratio": None,
            "nonempty_trajectory_ratio_after_routing_segment_available": None,
            "nonempty_trajectory_ratio_after_first_nonempty": None,
            "planning_claim_window_nonempty_trajectory_ratio": None,
            "planning_claim_window_source": "missing",
        }
    planning_available = any(_bool_value(_nested(row, "computed.planning_reference_available")) or _planning_reference_available(row) for row in rows)
    control_available = any(_bool_value(_nested(row, "computed.control_reference_available")) or _control_reference_available(row) for row in rows)
    nonempty_count = sum(1 for row in rows if (_num(_nested(row, "planning.trajectory_point_count")) or 0.0) > 0.0)
    ready_count = sum(1 for row in rows if str(_nested(row, "planning.reference_line_provider_status") or "").lower() in {"ready", "pass", "ok"})
    provider_known = sum(1 for row in rows if _nested(row, "planning.reference_line_provider_status") not in {None, ""})
    after_routing_ratio = _tail_nonempty_ratio(rows, predicate=_routing_segment_available)
    after_first_nonempty_ratio = _tail_nonempty_ratio(rows, predicate=_planning_trajectory_nonempty)
    claim_ratio, claim_source = _planning_claim_window_ratio(
        overall_ratio=nonempty_count / total,
        after_routing_ratio=after_routing_ratio,
        after_first_nonempty_ratio=after_first_nonempty_ratio,
    )
    return {
        "planning_reference_available": planning_available,
        "control_reference_available": control_available,
        "apollo_hdmap_projection_file_present": bool(hdmap_projection.get("file_present")),
        "apollo_hdmap_projection_available": bool(hdmap_projection.get("official_source_available")),
        "apollo_hdmap_projection_claim_grade": bool(hdmap_projection.get("claim_grade")),
        "reference_line_provider_ready_ratio": ready_count / provider_known if provider_known else None,
        "nonempty_trajectory_ratio": nonempty_count / total,
        "nonempty_trajectory_ratio_after_routing_segment_available": after_routing_ratio,
        "nonempty_trajectory_ratio_after_first_nonempty": after_first_nonempty_ratio,
        "planning_claim_window_nonempty_trajectory_ratio": claim_ratio,
        "planning_claim_window_source": claim_source,
    }


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, float | None]:
    return {
        "planning_ref_heading_error_p95_rad": _p95_abs(
            _first_number(row, "computed.localization_to_planning_first_heading_error_rad")
            for row in rows
        ),
        "normal_trajectory_heading_error_p95_rad": _p95_abs(
            _first_number(row, "computed.localization_to_planning_first_heading_error_rad")
            for row in rows
            if _planning_trajectory_nonempty(row) and not _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_heading_error_p95_rad": _p95_abs(
            _first_number(row, "computed.localization_to_planning_first_heading_error_rad")
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_trajectory_first_theta_vs_segment_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_theta_vs_segment_heading_p95_rad",
                "computed.trajectory_first_theta_vs_segment_heading_error_rad",
            )
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_first_nonexpired_theta_vs_future_segment_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_nonexpired_theta_vs_future_segment_heading_rad",
            )
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_nonexpired_theta_vs_future_lookahead_heading_rad",
            )
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_future_lookahead_distance_p95_m": _p95_abs(
            _first_number(row, "planning.trajectory_future_lookahead_distance_m")
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_future_lookahead_distance_min_m": _min_number(
            _first_number(row, "planning.trajectory_future_lookahead_distance_m")
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_future_lookahead_distance_lt_1m_ratio": _ratio_where(
            (
                _first_number(row, "planning.trajectory_future_lookahead_distance_m")
                for row in rows
                if _planning_trajectory_is_path_fallback(row)
            ),
            lambda value: value < 1.0,
        ),
        "path_fallback_future_lookahead_reverse_heading_ratio": _ratio_where(
            (
                _first_number(
                    row,
                    "computed.trajectory_first_nonexpired_theta_vs_future_lookahead_heading_rad",
                )
                for row in rows
                if _planning_trajectory_is_path_fallback(row)
            ),
            lambda value: abs(value) >= 2.50,
        ),
        "normal_trajectory_first_theta_vs_segment_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_theta_vs_segment_heading_p95_rad",
                "computed.trajectory_first_theta_vs_segment_heading_error_rad",
            )
            for row in rows
            if _planning_trajectory_nonempty(row) and not _planning_trajectory_is_path_fallback(row)
        ),
        "normal_first_nonexpired_theta_vs_future_segment_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_nonexpired_theta_vs_future_segment_heading_rad",
            )
            for row in rows
            if _planning_trajectory_nonempty(row) and not _planning_trajectory_is_path_fallback(row)
        ),
        "normal_first_nonexpired_theta_vs_future_lookahead_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_nonexpired_theta_vs_future_lookahead_heading_rad",
            )
            for row in rows
            if _planning_trajectory_nonempty(row) and not _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_first_kappa_p95_abs": _p95_abs(
            _first_number(row, "planning.first_trajectory_point_kappa")
            for row in rows
            if _planning_trajectory_is_path_fallback(row)
        ),
        "path_fallback_onset": _path_fallback_onset(rows),
        "path_fallback_trajectory_ratio": _trajectory_type_ratio(rows, "PATH_FALLBACK"),
        "control_ref_heading_error_p95_rad": _p95_abs(
            _first_number(row, "computed.localization_to_control_ref_heading_error_rad")
            for row in rows
        ),
        "control_lateral_error_p95_m": _p95_abs(
            _first_number(row, "computed.localization_to_control_lateral_error_m")
            for row in rows
        ),
        "trajectory_first_theta_vs_segment_heading_p95_rad": _p95_abs(
            _first_number(
                row,
                "computed.trajectory_first_theta_vs_segment_heading_p95_rad",
                "computed.trajectory_first_theta_vs_segment_heading_error_rad",
            )
            for row in rows
        ),
        "reference_line_count_zero_ratio": _zero_ratio(rows, "planning.reference_line_count"),
        "routing_segment_count_zero_ratio": _zero_ratio(rows, "planning.routing_segment_count", "routing.routing_segment_count"),
        "fallback_join_coverage_ratio": _fallback_join_coverage_ratio(rows),
        "fallback_join_tolerance_ms": FALLBACK_JOIN_TOLERANCE_S * 1000.0,
        "fallback_join_dropped_unaligned_rows": _fallback_join_dropped_unaligned_rows(rows),
    }


def _planning_debug_presence_summary(
    rows: Sequence[Mapping[str, Any]],
    planning_debug_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    summary_presence = _mapping(_mapping(planning_debug_summary).get("planning_debug_presence"))
    if summary_presence:
        out = dict(summary_presence)
        out["available"] = True
        out["source"] = "planning_topic_debug_summary"
        out["classification"] = _planning_debug_presence_classification(out)
        out["claim_boundary"] = (
            "Planning debug presence narrows reference-line diagnosis. It does not "
            "make reference-line evidence claim-grade and does not prove behavior success."
        )
        return out

    row_presence = [
        _mapping(row.get("planning_debug_presence"))
        for row in rows
        if isinstance(row.get("planning_debug_presence"), Mapping)
    ]
    if not row_presence:
        return {
            "available": False,
            "source": "missing",
            "classification": "not_available",
            "claim_boundary": (
                "No planning_debug_presence summary or row-level fields were found."
            ),
        }
    last = row_presence[-1]
    out = {
        "available": True,
        "source": "row_level_planning_debug_presence",
        "last_diagnosis": last.get("diagnosis"),
        "last_reference_line_path": last.get("reference_line_path"),
        "last_routing_path": last.get("routing_path"),
        "last_planning_data_present": last.get("planning_data_present"),
        "last_reference_line_field_present": last.get("reference_line_field_present"),
        "last_reference_line_count": last.get("reference_line_count"),
        "last_routing_segment_count": last.get("routing_segment_count"),
        "last_field_inventory": _mapping(last.get("last_field_inventory")),
        "last_path_candidate_summary": _mapping(last.get("path_candidate_summary")),
        "planning_data_present_ratio": _presence_bool_ratio(row_presence, "planning_data_present"),
        "reference_line_field_present_ratio": _presence_bool_ratio(row_presence, "reference_line_field_present"),
        "reference_line_nonempty_ratio": _presence_nonzero_ratio(row_presence, "reference_line_count"),
        "routing_field_present_ratio": _presence_bool_ratio(row_presence, "routing_field_present"),
        "routing_segment_nonempty_ratio": _presence_nonzero_ratio(row_presence, "routing_segment_count"),
    }
    out["classification"] = _planning_debug_presence_classification(out)
    out["claim_boundary"] = (
        "Planning debug presence narrows reference-line diagnosis. It does not "
        "make reference-line evidence claim-grade and does not prove behavior success."
    )
    return out


def _planning_debug_presence_classification(presence: Mapping[str, Any]) -> str:
    explicit = str(presence.get("last_diagnosis") or "").strip()
    if explicit:
        return explicit
    planning_ratio = _num(presence.get("planning_data_present_ratio"))
    reference_field_ratio = _num(presence.get("reference_line_field_present_ratio"))
    reference_nonempty_ratio = _num(presence.get("reference_line_nonempty_ratio"))
    routing_nonempty_ratio = _num(presence.get("routing_segment_nonempty_ratio"))
    if planning_ratio == 0.0 or presence.get("last_planning_data_present") is False:
        return "planning_data_missing"
    if (
        (reference_field_ratio or 0.0) > 0.0
        and (reference_nonempty_ratio or 0.0) == 0.0
        and (routing_nonempty_ratio or 0.0) > 0.0
    ):
        return "routing_present_reference_line_empty"
    if (reference_nonempty_ratio or 0.0) > 0.0 and (routing_nonempty_ratio or 0.0) > 0.0:
        return "reference_line_and_routing_present"
    if (routing_nonempty_ratio or 0.0) > 0.0:
        return "routing_present_reference_line_field_missing_or_empty"
    if (reference_nonempty_ratio or 0.0) > 0.0:
        return "reference_line_present_routing_missing_or_empty"
    if reference_field_ratio is None and routing_nonempty_ratio is None:
        return "not_available"
    return "reference_line_and_routing_empty"


def _planning_debug_path_candidate_evidence(
    rows: Sequence[Mapping[str, Any]],
    planning_debug_presence: Mapping[str, Any],
) -> dict[str, Any]:
    """Summarize Planning debug path-like candidates without upgrading claims.

    Apollo 10 can populate ``debug.planning_data.path`` even when
    ``debug.planning_data.reference_line`` is an empty repeated field. The path
    entries are useful to steer the next parser/bridge iteration, but they are
    not equivalent to claim-grade reference-line evidence.
    """

    inventories = [
        _mapping(_mapping(row.get("planning_debug_presence")).get("last_field_inventory"))
        for row in rows
        if isinstance(row.get("planning_debug_presence"), Mapping)
    ]
    last_inventory = _mapping(planning_debug_presence.get("last_field_inventory"))
    if last_inventory:
        inventories.append(last_inventory)

    path_counts: list[int] = []
    nonempty_paths: list[str] = []
    candidate_paths_by_path: dict[str, dict[str, Any]] = {}
    path_summaries = [
        _mapping(_mapping(row.get("planning_debug_presence")).get("path_candidate_summary"))
        for row in rows
        if isinstance(row.get("planning_debug_presence"), Mapping)
    ]
    summary_candidate = _mapping(planning_debug_presence.get("last_path_candidate_summary"))
    if summary_candidate:
        path_summaries.append(summary_candidate)
    for inventory in inventories:
        candidates = inventory.get("reference_line_candidate_paths")
        if isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes, bytearray)):
            for candidate in candidates:
                item = _mapping(candidate)
                path = str(item.get("path") or "")
                if not path:
                    continue
                count = int(_num(item.get("repeated_count")) or 0)
                if "path" in path.lower() or "trajectory" in path.lower():
                    path_counts.append(count)
                    if count > 0:
                        nonempty_paths.append(path)
                existing = candidate_paths_by_path.get(path)
                field_name_match = bool(item.get("field_name_match"))
                if existing is None or count > int(existing.get("repeated_count") or 0):
                    candidate_paths_by_path[path] = {
                        "path": path,
                        "repeated_count": count,
                        "field_name_match": field_name_match,
                    }
                elif field_name_match and not bool(existing.get("field_name_match")):
                    existing["field_name_match"] = True

    candidate_paths = sorted(
        candidate_paths_by_path.values(),
        key=lambda item: (-int(item.get("repeated_count") or 0), str(item.get("path") or "")),
    )

    point_sequence_by_path: dict[str, int] = {}
    summarized_by_path: dict[str, dict[str, Any]] = {}
    for summary in path_summaries:
        for candidate in _sequence_of_mappings(summary.get("candidates")):
            path = str(candidate.get("path") or "")
            if not path:
                continue
            point_count = _point_sequence_candidate_count(candidate)
            point_sequence_by_path[path] = max(point_sequence_by_path.get(path, 0), point_count)
            current = summarized_by_path.get(path)
            candidate_payload = {
                "path": path,
                "item_count": candidate.get("item_count"),
                "field_name_match": candidate.get("field_name_match"),
                "item_summaries": candidate.get("item_summaries"),
            }
            if current is None or point_count > _point_sequence_candidate_count(current):
                summarized_by_path[path] = candidate_payload
    point_sequence_count = sum(point_sequence_by_path.values())
    summarized_candidates = sorted(
        summarized_by_path.values(),
        key=lambda item: (-_point_sequence_candidate_count(item), str(item.get("path") or "")),
    )

    reference_nonempty_ratio = _num(planning_debug_presence.get("reference_line_nonempty_ratio"))
    path_max = max(path_counts) if path_counts else 0
    path_nonempty = path_max > 0
    reference_nonempty = reference_nonempty_ratio is not None and reference_nonempty_ratio > 0.0
    if not candidate_paths:
        classification = "not_available"
        status = "insufficient_data"
    elif point_sequence_count > 0 and not reference_nonempty:
        classification = "path_candidate_points_present_reference_line_empty"
        status = "warn"
    elif path_nonempty and not reference_nonempty:
        classification = "path_candidate_present_reference_line_empty"
        status = "warn"
    elif path_nonempty and reference_nonempty:
        classification = "path_candidate_present_with_reference_line"
        status = "warn"
    else:
        classification = "path_candidate_missing_or_empty"
        status = "insufficient_data"

    return {
        "status": status,
        "classification": classification,
        "available": path_nonempty,
        "path_candidate_max_count": path_max,
        "path_candidate_nonempty_paths": sorted(set(nonempty_paths)),
        "point_sequence_candidate_count": point_sequence_count,
        "candidate_paths": candidate_paths[:20],
        "candidate_summaries": summarized_candidates[:20],
        "reference_line_claim_grade_allowed": False,
        "recommended_next_action": (
            "Inspect debug.planning_data.path as diagnostic planning path evidence and "
            "compare it with HDMap projection, route windows, and control target points. "
            "Do not treat it as a substitute for exported reference-line debug evidence."
        ),
        "claim_boundary": (
            "Planning debug path candidates are diagnostic only. They can show Apollo "
            "materialized path-like Planning debug data, but they do not by themselves "
            "prove Apollo reference-line correctness or behavior success."
        ),
    }


def _sequence_of_mappings(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _point_sequence_candidate_count(candidate: Mapping[str, Any]) -> int:
    count = 0
    for item in _sequence_of_mappings(candidate.get("item_summaries")):
        point_sequences = item.get("point_sequence_candidates")
        if not isinstance(point_sequences, Sequence) or isinstance(
            point_sequences, (str, bytes, bytearray)
        ):
            continue
        for point_candidate in point_sequences:
            if not isinstance(point_candidate, Mapping):
                continue
            if (_num(point_candidate.get("point_like_count")) or 0.0) > 0.0:
                count += 1
    return count


def _planning_info_log_reference_line_evidence(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {
            "available": False,
            "source": "missing",
            "classification": "planning_info_log_missing",
            "reference_line_text_prints_present": False,
            "text_log_reference_line_claim_grade_allowed": False,
            "claim_boundary": (
                "Apollo Planning INFO text logs are optional diagnostic evidence and "
                "cannot replace exported Planning reference-line debug fields."
            ),
        }
    log_path = Path(path).expanduser()
    if not log_path.exists():
        return {
            "available": False,
            "source": str(log_path),
            "classification": "planning_info_log_missing",
            "reference_line_text_prints_present": False,
            "text_log_reference_line_claim_grade_allowed": False,
            "claim_boundary": (
                "Apollo Planning INFO text logs are optional diagnostic evidence and "
                "cannot replace exported Planning reference-line debug fields."
            ),
        }
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return {
            "available": False,
            "source": str(log_path),
            "classification": "planning_info_log_read_error",
            "read_error": str(exc),
            "reference_line_text_prints_present": False,
            "text_log_reference_line_claim_grade_allowed": False,
            "claim_boundary": (
                "Apollo Planning INFO text logs are optional diagnostic evidence and "
                "cannot replace exported Planning reference-line debug fields."
            ),
        }

    print_counts: Counter[str] = Counter()
    st_boundary_ids: list[str] = []
    destination_warning_count = 0
    fallback_l_lower_values: list[float] = []
    fallback_l_upper_values: list[float] = []
    fallback_opt_l_values: list[float] = []
    fallback_opt_l_out_of_bounds_margins: list[float] = []
    current_fallback_l_lower: tuple[float, float] | None = None
    current_fallback_l_upper: tuple[float, float] | None = None
    current_fallback_l_lower_points: list[tuple[float, float]] = []
    current_fallback_l_upper_points: list[tuple[float, float]] = []
    first_fallback_path_bound_violation: dict[str, Any] | None = None
    for line_number, line in enumerate(lines, start=1):
        if "print_regular/self_ref_l:" in line:
            print_counts["print_regular_self_ref_l"] += 1
        if "print_regular/self_opt_l:" in line:
            print_counts["print_regular_self_opt_l"] += 1
        if "print_fallback/self_l_lower:" in line:
            print_counts["print_fallback_self_l_lower"] += 1
            points = _extract_debug_curve_points(line, "print_fallback/self_l_lower:")
            values = [value for _, value in points]
            fallback_l_lower_values.extend(values)
            current_fallback_l_lower_points = points
            if values:
                current_fallback_l_lower = (min(values), max(values))
        if "print_fallback/self_l_upper:" in line:
            print_counts["print_fallback_self_l_upper"] += 1
            points = _extract_debug_curve_points(line, "print_fallback/self_l_upper:")
            values = [value for _, value in points]
            fallback_l_upper_values.extend(values)
            current_fallback_l_upper_points = points
            if values:
                current_fallback_l_upper = (min(values), max(values))
        if "print_fallback/self_opt_l:" in line:
            print_counts["print_fallback_self_opt_l"] += 1
            opt_points = _extract_debug_curve_points(line, "print_fallback/self_opt_l:")
            values = [value for _, value in opt_points]
            fallback_opt_l_values.extend(values)
            if current_fallback_l_lower is not None and current_fallback_l_upper is not None:
                lower = current_fallback_l_lower[0]
                upper = current_fallback_l_upper[1]
                for index, value in enumerate(values):
                    lower_at_index = (
                        current_fallback_l_lower_points[index][1]
                        if index < len(current_fallback_l_lower_points)
                        else lower
                    )
                    upper_at_index = (
                        current_fallback_l_upper_points[index][1]
                        if index < len(current_fallback_l_upper_points)
                        else upper
                    )
                    s_at_index = opt_points[index][0] if index < len(opt_points) else None
                    if value < lower:
                        margin = lower - value
                        fallback_opt_l_out_of_bounds_margins.append(margin)
                        if first_fallback_path_bound_violation is None:
                            first_fallback_path_bound_violation = {
                                "line_number": line_number,
                                "s_m": s_at_index,
                                "opt_l_m": value,
                                "lower_l_m": lower_at_index,
                                "upper_l_m": upper_at_index,
                                "violated_bound": "lower",
                                "margin_m": margin,
                            }
                    elif value > upper:
                        margin = value - upper
                        fallback_opt_l_out_of_bounds_margins.append(margin)
                        if first_fallback_path_bound_violation is None:
                            first_fallback_path_bound_violation = {
                                "line_number": line_number,
                                "s_m": s_at_index,
                                "opt_l_m": value,
                                "lower_l_m": lower_at_index,
                                "upper_l_m": upper_at_index,
                                "violated_bound": "upper",
                                "margin_m": margin,
                            }
        if "print_st_reference_line:" in line:
            print_counts["print_st_reference_line"] += 1
        if "print_vt_reference_line:" in line:
            print_counts["print_vt_reference_line"] += 1
        if "print_trajxy:" in line:
            print_counts["print_trajxy"] += 1
        if "lane_follow_path.cc" in line:
            print_counts["lane_follow_path"] += 1
        if "Planning Perf: task name [LANE_FOLLOW_PATH]" in line:
            print_counts["lane_follow_path_task"] += 1
        if "Decide path bound failed" in line:
            print_counts["decide_path_bound_failed"] += 1
        if "fallback/selfpiecewise jerk path optimizer failed" in line:
            print_counts["fallback_self_piecewise_jerk_path_optimizer_failed"] += 1
        if "Failed to make decisions for static obstacles" in line:
            print_counts["path_decider_static_obstacle_decision_failed"] += 1
        if "Failed to make decision based on tunnel" in line:
            print_counts["path_decider_tunnel_decision_failed"] += 1
        if "Path fallback due to algorithm failure" in line:
            print_counts["path_fallback_due_to_algorithm_failure"] += 1
        if "build reference line st boundary. id:" in line:
            print_counts["build_reference_line_st_boundary"] += 1
            st_boundary_ids.append(line.rsplit("id:", 1)[-1].strip())
        if "dest_sl.s() + ego_front_to_center > reference_line->length()" in line:
            destination_warning_count += 1

    reference_line_text_prints_present = any(
        print_counts.get(key, 0) > 0
        for key in (
            "print_regular_self_ref_l",
            "print_st_reference_line",
            "print_vt_reference_line",
            "build_reference_line_st_boundary",
        )
    )
    if reference_line_text_prints_present:
        classification = "planning_info_log_reference_line_text_prints_present"
    elif lines:
        classification = "planning_info_log_present_reference_line_text_prints_missing"
    else:
        classification = "planning_info_log_empty"
    return {
        "available": True,
        "source": str(log_path),
        "classification": classification,
        "line_count": len(lines),
        "reference_line_text_prints_present": reference_line_text_prints_present,
        "print_counts": dict(sorted(print_counts.items())),
        "st_boundary_id_topk": _topk_counts(st_boundary_ids),
        "destination_warning_count": destination_warning_count,
        "planning_info_log_path_bound_evidence": {
            "classification": _path_bound_evidence_classification(
                print_counts, fallback_opt_l_out_of_bounds_margins
            ),
            "decide_path_bound_failed_count": print_counts.get(
                "decide_path_bound_failed", 0
            ),
            "fallback_self_piecewise_jerk_path_optimizer_failed_count": print_counts.get(
                "fallback_self_piecewise_jerk_path_optimizer_failed", 0
            ),
            "path_decider_tunnel_decision_failed_count": print_counts.get(
                "path_decider_tunnel_decision_failed", 0
            ),
            "path_decider_static_obstacle_decision_failed_count": print_counts.get(
                "path_decider_static_obstacle_decision_failed", 0
            ),
            "path_fallback_due_to_algorithm_failure_count": print_counts.get(
                "path_fallback_due_to_algorithm_failure", 0
            ),
            "fallback_self_l_lower_min_m": _min_number(fallback_l_lower_values),
            "fallback_self_l_upper_max_m": _max_number(fallback_l_upper_values),
            "fallback_self_opt_l_max_abs_m": _max_abs_number(fallback_opt_l_values),
            "fallback_self_opt_l_out_of_bounds_count": len(
                fallback_opt_l_out_of_bounds_margins
            ),
            "fallback_self_opt_l_out_of_bounds_max_margin_m": _max_number(
                fallback_opt_l_out_of_bounds_margins
            ),
            "first_fallback_path_bound_violation": first_fallback_path_bound_violation,
            "claim_boundary": (
                "Planning INFO path-bound evidence is diagnostic only. It can "
                "show that Apollo Planning considered the lateral state outside "
                "the current corridor, but it does not by itself prove the root "
                "cause or justify changing control mapping."
            ),
        },
        "text_log_reference_line_claim_grade_allowed": False,
        "interpretation": (
            "Planning INFO text traces can show that Apollo Planning internally printed "
            "reference-line-related curves or ST boundaries. They are diagnostic only; "
            "claim-grade evidence still requires exported Planning reference-line debug "
            "or an explicit equivalent contract."
        ),
        "claim_boundary": (
            "Text-log reference-line evidence cannot by itself prove reference-line "
            "correctness, route equivalence, or behavior success."
        ),
    }


def _presence_bool_ratio(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = [row.get(key) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return sum(1 for value in values if bool(value)) / float(len(values))


def _presence_nonzero_ratio(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = [_num(row.get(key)) for row in rows if row.get(key) is not None]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return sum(1 for value in values if value > 0.0) / float(len(values))


def _reference_debug_diagnostic(
    rows: Sequence[Mapping[str, Any]],
    evidence: Mapping[str, Any],
    metrics: Mapping[str, Any],
    *,
    planning_debug_presence: Mapping[str, Any] | None = None,
    planning_projection_alignment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    row_count = len(rows)
    route_segment_available_count = sum(1 for row in rows if _routing_segment_available(row))
    trajectory_nonempty_count = sum(1 for row in rows if _planning_trajectory_nonempty(row))
    reference_line_positive_count = sum(
        1
        for row in rows
        if (_num(_nested(row, "planning.reference_line_count")) or 0.0) > 0.0
    )
    provider_ready_ratio = _num(evidence.get("reference_line_provider_ready_ratio"))
    reference_line_zero_ratio = _num(metrics.get("reference_line_count_zero_ratio"))
    claim_window_ratio = _num(evidence.get("planning_claim_window_nonempty_trajectory_ratio"))
    control_available = bool(evidence.get("control_reference_available"))
    route_segment_available = route_segment_available_count > 0
    trajectory_nonempty = trajectory_nonempty_count > 0
    reference_line_debug_available = reference_line_positive_count > 0 or (
        provider_ready_ratio is not None and provider_ready_ratio > 0.0
    )
    if row_count <= 0:
        classification = "insufficient_data"
    elif reference_line_debug_available:
        classification = "reference_line_debug_available"
    elif trajectory_nonempty and route_segment_available and control_available:
        classification = "planning_reference_line_debug_export_gap"
    elif trajectory_nonempty and route_segment_available:
        classification = "planning_reference_line_debug_missing_control_reference_missing"
    elif trajectory_nonempty:
        classification = "planning_nonempty_route_segment_missing"
    else:
        classification = "planning_not_materialized"
    field_inventory = _reference_debug_field_inventory(rows)
    planning_debug_presence = dict(planning_debug_presence or {})
    return {
        "classification": classification,
        "planning_debug_presence_classification": planning_debug_presence.get("classification"),
        "planning_debug_presence": planning_debug_presence,
        "row_count": row_count,
        "trajectory_nonempty_count": trajectory_nonempty_count,
        "trajectory_nonempty_ratio": evidence.get("nonempty_trajectory_ratio"),
        "claim_window_nonempty_ratio": claim_window_ratio,
        "claim_window_source": evidence.get("planning_claim_window_source"),
        "route_segment_available_count": route_segment_available_count,
        "route_segment_available": route_segment_available,
        "reference_line_debug_available": reference_line_debug_available,
        "reference_line_count_positive_count": reference_line_positive_count,
        "reference_line_count_zero_ratio": reference_line_zero_ratio,
        "reference_line_provider_ready_ratio": provider_ready_ratio,
        "control_simple_lat_reference_available": control_available,
        "control_reference_join_coverage_ratio": metrics.get("fallback_join_coverage_ratio"),
        "planning_first_point_projection_alignment": dict(planning_projection_alignment or {}),
        "field_inventory": field_inventory,
        "interpretation": (
            "classification separates Planning route/trajectory materialization from whether "
            "reference-line debug counters are exported and whether Control simple_lat reference "
            "semantics are visible."
        ),
    }


def _reference_debug_field_inventory(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    inventory = _reference_debug_field_inventory_counts(rows)
    claim_window_source, claim_window_rows = _reference_debug_claim_window_rows(rows)
    inventory["claim_window_source"] = claim_window_source
    inventory["claim_window"] = _reference_debug_field_inventory_counts(claim_window_rows)
    return inventory


def _reference_debug_claim_window_rows(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[str, Sequence[Mapping[str, Any]]]:
    for index, row in enumerate(rows):
        if _routing_segment_available(row):
            return "after_routing_segment_available", rows[index:]
    for index, row in enumerate(rows):
        if _planning_trajectory_nonempty(row):
            return "after_first_nonempty_trajectory", rows[index:]
    return "overall", rows


def _reference_debug_field_inventory_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    row_count = len(rows)
    reference_line_count_known = sum(
        1 for row in rows if _num(_nested(row, "planning.reference_line_count")) is not None
    )
    reference_line_count_positive = sum(
        1 for row in rows if (_num(_nested(row, "planning.reference_line_count")) or 0.0) > 0.0
    )
    provider_statuses = [
        str(_nested(row, "planning.reference_line_provider_status"))
        for row in rows
        if _nested(row, "planning.reference_line_provider_status") not in {None, ""}
    ]
    planning_route_segment_count_positive = sum(
        1
        for row in rows
        if (_num(_nested(row, "planning.routing_segment_count")) or 0.0) > 0.0
    )
    routing_segment_count_positive = sum(
        1
        for row in rows
        if (_num(_nested(row, "routing.routing_segment_count")) or 0.0) > 0.0
    )
    routing_road_count_positive = sum(
        1
        for row in rows
        if (_num(_nested(row, "routing.routing_road_count")) or 0.0) > 0.0
    )
    route_segment_available_broad = sum(1 for row in rows if _routing_segment_available(row))
    route_segment_total_length_available = sum(
        1
        for row in rows
        if _first_number(
            row,
            "planning.route_segment_total_length",
            "routing.route_segment_total_length",
        )
        is not None
    )
    lane_id_available = sum(
        1
        for row in rows
        if _present_value(_nested(row, "planning.lane_id_first"))
        or _present_value(_nested(row, "planning.lane_ids"))
        or _present_value(_nested(row, "planning.target_lane_id_first"))
        or _present_value(_nested(row, "planning.target_lane_ids"))
    )
    trajectory_nonempty_count = sum(1 for row in rows if _planning_trajectory_nonempty(row))
    trajectory_sample_rows = sum(
        1 for row in rows if _sample_points(_nested(row, "planning.trajectory_sample_points"))
    )
    control_reference_rows = sum(1 for row in rows if _control_reference_available(row))
    control_target_point_rows = sum(1 for row in rows if _control_target_point_available(row))
    planning_surrogate_available = (
        route_segment_available_broad > 0
        or route_segment_total_length_available > 0
        or lane_id_available > 0
        or trajectory_sample_rows > 0
    )
    control_surrogate_available = control_reference_rows > 0 or control_target_point_rows > 0
    planning_route_surrogate_available = (
        planning_route_segment_count_positive > 0
        or routing_segment_count_positive > 0
        or routing_road_count_positive > 0
        or route_segment_total_length_available > 0
        or lane_id_available > 0
    )
    if row_count <= 0:
        classification = "insufficient_data"
    elif reference_line_count_positive > 0:
        classification = "reference_line_debug_counter_available"
    elif planning_surrogate_available and control_surrogate_available:
        classification = "reference_line_counter_missing_but_planning_control_surrogates_present"
    elif planning_surrogate_available:
        classification = "reference_line_counter_missing_but_planning_surrogates_present"
    elif control_surrogate_available:
        classification = "reference_line_counter_missing_but_control_surrogates_present"
    else:
        classification = "reference_line_debug_fields_missing"
    return {
        "field_gap_classification": classification,
        "row_count": row_count,
        "reference_line_count_known_count": reference_line_count_known,
        "reference_line_count_positive_count": reference_line_count_positive,
        "reference_line_count_zero_ratio": (
            (reference_line_count_known - reference_line_count_positive) / reference_line_count_known
            if reference_line_count_known
            else None
        ),
        "reference_line_provider_status_topk": _topk(provider_statuses),
        "route_segment_count_positive_count": route_segment_available_broad,
        "route_segment_count_positive_count_semantics": (
            "deprecated_broad_route_or_routing_segment_availability"
        ),
        "planning_route_segment_count_positive_count": planning_route_segment_count_positive,
        "routing_segment_count_positive_count": routing_segment_count_positive,
        "routing_road_count_positive_count": routing_road_count_positive,
        "route_segment_total_length_available_count": route_segment_total_length_available,
        "planning_lane_id_available_count": lane_id_available,
        "trajectory_nonempty_count": trajectory_nonempty_count,
        "trajectory_sample_rows": trajectory_sample_rows,
        "control_reference_rows": control_reference_rows,
        "control_target_point_rows": control_target_point_rows,
        "planning_surrogate_available": planning_surrogate_available,
        "planning_route_surrogate_available": planning_route_surrogate_available,
        "control_surrogate_available": control_surrogate_available,
        "claim_boundary": (
            "This inventory reports which debug fields are present in existing artifacts. "
            "Surrogate Planning/control fields can narrow an export gap, but they cannot "
            "replace exported Planning reference-line debug for claim-grade evidence."
        ),
    }


def _planning_materialization_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize Planning route-segment/reference-line materialization fields.

    This is an operator diagnostic layer. It tells whether Planning appears to
    have entered lane-follow/route-segment-ready state while exported
    reference-line debug remains empty. It is not behavior success evidence.
    """

    claim_window_source, claim_window_rows = _reference_debug_claim_window_rows(rows)
    claim_window = _planning_materialization_counts(claim_window_rows)
    classification = _planning_materialization_classification(claim_window)
    return {
        "classification": classification,
        "claim_window_source": claim_window_source,
        "overall": _planning_materialization_counts(rows),
        "claim_window": claim_window,
        "recommended_action": _planning_materialization_recommended_action(classification),
        "claim_boundary": (
            "Planning materialization summary narrows reference-line diagnosis. "
            "It does not prove reference-line correctness or behavior success."
        ),
    }


def _planning_materialization_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    row_count = len(rows)
    route_segments_ready = [
        row for row in rows if _planning_route_segments_ready(row)
    ]
    reference_line_positive = [
        row
        for row in rows
        if (_num(_nested(row, "planning.reference_line_count")) or 0.0) > 0.0
    ]
    reference_line_known = [
        row for row in rows if _num(_nested(row, "planning.reference_line_count")) is not None
    ]
    reference_line_empty_with_route_ready = [
        row
        for row in route_segments_ready
        if (_num(_nested(row, "planning.reference_line_count")) or 0.0) == 0.0
    ]
    trajectory_nonempty = [row for row in rows if _planning_trajectory_nonempty(row)]
    trajectory_nonempty_ready_ref_empty = [
        row
        for row in rows
        if _planning_trajectory_nonempty(row)
        and _planning_route_segments_ready(row)
        and (_num(_nested(row, "planning.reference_line_count")) or 0.0) == 0.0
    ]
    path_end_like_true = [
        row for row in rows if _bool_value(_nested(row, "planning.path_end_like_condition"))
    ]
    lane_follow_stage = [
        row for row in rows if _nested(row, "planning.planning_stage_name") == "LANE_FOLLOW_STAGE"
    ]
    lane_follow_task = [
        row for row in rows if _nested(row, "planning.task_name") == "LANE_FOLLOW"
    ]
    return {
        "row_count": row_count,
        "create_route_segments_status_topk": _topk_counts(
            _nested(row, "planning.create_route_segments_status") for row in rows
        ),
        "lane_follow_map_status_topk": _topk_counts(
            _nested(row, "planning.lane_follow_map_status") for row in rows
        ),
        "planning_empty_reason_guess_topk": _topk_counts(
            _nested(row, "planning.planning_empty_reason_guess") for row in rows
        ),
        "reference_line_provider_status_topk": _topk_counts(
            _nested(row, "planning.reference_line_provider_status") for row in rows
        ),
        "planning_stage_name_topk": _topk_counts(
            _nested(row, "planning.planning_stage_name") for row in rows
        ),
        "task_name_topk": _topk_counts(_nested(row, "planning.task_name") for row in rows),
        "routing_lane_window_signature_topk": _topk_counts(
            _nested(row, "routing.routing_lane_window_signature") for row in rows
        ),
        "route_segments_ready_count": len(route_segments_ready),
        "route_segments_ready_ratio": _ratio(len(route_segments_ready), row_count),
        "reference_line_count_known_count": len(reference_line_known),
        "reference_line_positive_count": len(reference_line_positive),
        "reference_line_positive_ratio": _ratio(len(reference_line_positive), row_count),
        "reference_line_empty_with_route_segments_ready_count": len(reference_line_empty_with_route_ready),
        "reference_line_empty_with_route_segments_ready_ratio": _ratio(
            len(reference_line_empty_with_route_ready),
            len(route_segments_ready),
        ),
        "trajectory_nonempty_count": len(trajectory_nonempty),
        "trajectory_nonempty_ratio": _ratio(len(trajectory_nonempty), row_count),
        "trajectory_nonempty_route_segments_ready_reference_line_empty_count": len(
            trajectory_nonempty_ready_ref_empty
        ),
        "trajectory_nonempty_route_segments_ready_reference_line_empty_ratio": _ratio(
            len(trajectory_nonempty_ready_ref_empty),
            len(trajectory_nonempty),
        ),
        "path_end_like_condition_true_count": len(path_end_like_true),
        "path_end_like_condition_true_ratio": _ratio(len(path_end_like_true), row_count),
        "lane_follow_stage_count": len(lane_follow_stage),
        "lane_follow_stage_ratio": _ratio(len(lane_follow_stage), row_count),
        "lane_follow_task_count": len(lane_follow_task),
        "lane_follow_task_ratio": _ratio(len(lane_follow_task), row_count),
    }


def _planning_materialization_classification(counts: Mapping[str, Any]) -> str:
    row_count = int(counts.get("row_count") or 0)
    if row_count <= 0:
        return "insufficient_data"
    if int(counts.get("reference_line_positive_count") or 0) > 0:
        return "reference_line_materialized"
    ready_count = int(counts.get("route_segments_ready_count") or 0)
    trajectory_ready_empty_count = int(
        counts.get("trajectory_nonempty_route_segments_ready_reference_line_empty_count") or 0
    )
    if ready_count > 0 and trajectory_ready_empty_count > 0:
        return "route_segments_ready_trajectory_nonzero_reference_line_empty"
    if ready_count > 0:
        return "route_segments_ready_reference_line_empty"
    path_end_ratio = _num(counts.get("path_end_like_condition_true_ratio")) or 0.0
    if path_end_ratio >= 0.50:
        return "route_segments_failed_path_end_like"
    return "route_segments_not_ready"


def _planning_materialization_recommended_action(classification: str) -> str:
    if classification == "reference_line_materialized":
        return "Use exported reference-line debug with route, projection, localization, and control evidence."
    if classification == "route_segments_ready_trajectory_nonzero_reference_line_empty":
        return (
            "Planning has route segments, lane-follow stage/task, and non-empty trajectories, "
            "but exported reference-line debug remains empty. Inspect Planning reference-line "
            "debug population/materialization for this route before changing control mapping."
        )
    if classification == "route_segments_ready_reference_line_empty":
        return (
            "Planning route segments are ready while exported reference-line debug remains empty. "
            "Inspect Planning reference-line materialization/config/debug content for this route."
        )
    if classification == "route_segments_failed_path_end_like":
        return "Inspect routing destination/path-end semantics before judging Planning reference-line behavior."
    return "Collect Planning route-segment and reference-line debug rows before judging reference-line materialization."


def _planning_route_segments_ready(row: Mapping[str, Any]) -> bool:
    status = _nested(row, "planning.create_route_segments_status")
    if status == "ready":
        return True
    return (_num(_nested(row, "planning.routing_segment_count")) or 0.0) > 0.0


def _reference_line_debug_export_policy(
    reference_debug_diagnostic: Mapping[str, Any],
    *,
    trajectory_sample_surrogate: Mapping[str, Any] | None = None,
    planning_materialization_summary: Mapping[str, Any] | None = None,
    planning_info_log_reference_line_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    classification = str(reference_debug_diagnostic.get("classification") or "")
    planning_alignment = _mapping(reference_debug_diagnostic.get("planning_first_point_projection_alignment"))
    trajectory_sample_surrogate = _mapping(trajectory_sample_surrogate)
    planning_materialization_summary = _mapping(planning_materialization_summary)
    planning_info_log_reference_line_evidence = _mapping(
        planning_info_log_reference_line_evidence
    )
    planning_debug_presence = _mapping(reference_debug_diagnostic.get("planning_debug_presence"))
    presence_classification = str(
        reference_debug_diagnostic.get("planning_debug_presence_classification")
        or planning_debug_presence.get("classification")
        or ""
    )
    materialization_classification = str(planning_materialization_summary.get("classification") or "")
    field_present_but_empty = presence_classification == "routing_present_reference_line_empty"
    lane_follow_materialized_empty = materialization_classification in {
        "route_segments_ready_reference_line_empty",
        "route_segments_ready_trajectory_nonzero_reference_line_empty",
    }
    if field_present_but_empty and lane_follow_materialized_empty:
        debug_field_state = "field_present_but_empty_after_lane_follow_materialization"
    elif field_present_but_empty:
        debug_field_state = "field_present_but_empty"
    elif presence_classification:
        debug_field_state = presence_classification
    else:
        debug_field_state = "unknown"
    planning_local_alignment = (
        planning_alignment.get("classification") == "planning_first_point_on_hdmap_projection_line_candidate"
        and planning_alignment.get("status") == "pass"
    )
    trajectory_sample_surrogate_available = bool(trajectory_sample_surrogate.get("available"))
    control_reference_available = bool(reference_debug_diagnostic.get("control_simple_lat_reference_available"))
    route_segment_available = bool(reference_debug_diagnostic.get("route_segment_available"))
    reference_debug_available = bool(reference_debug_diagnostic.get("reference_line_debug_available"))
    planning_info_log_reference_line_available = bool(
        planning_info_log_reference_line_evidence.get("reference_line_text_prints_present")
    )
    planning_info_log_classification = planning_info_log_reference_line_evidence.get("classification")

    if classification == "reference_line_debug_available" or reference_debug_available:
        return {
            "status": "available",
            "classification": "reference_line_debug_available",
            "reference_line_debug_claim_grade_allowed": True,
            "planning_first_point_local_alignment_available": planning_local_alignment,
            "planning_trajectory_sample_surrogate_available": trajectory_sample_surrogate_available,
            "control_simple_lat_reference_available": control_reference_available,
            "route_segment_available": route_segment_available,
            "planning_debug_presence_classification": presence_classification or None,
            "planning_materialization_classification": materialization_classification or None,
            "reference_line_debug_field_state": debug_field_state,
            "planning_info_log_reference_line_available": planning_info_log_reference_line_available,
            "planning_info_log_reference_line_classification": planning_info_log_classification,
            "planning_info_log_reference_line_claim_grade_allowed": False,
            "recommended_evidence_policy": "reference_line_debug_can_support_claim_if_other_gates_pass",
            "recommended_next_action": (
                "Use exported Planning reference-line debug together with route, projection, "
                "localization, and control-handoff contracts before making behavior claims."
            ),
            "reason": "Planning reference-line debug counters are available.",
            "claim_boundary": (
                "Reference-line debug availability is necessary but not sufficient for behavior "
                "success or natural-driving claims."
            ),
        }

    if classification == "planning_reference_line_debug_export_gap":
        if planning_local_alignment and control_reference_available:
            export_classification = (
                "reference_line_debug_export_gap_with_local_planning_and_control_reference_evidence"
            )
            if (
                debug_field_state == "field_present_but_empty_after_lane_follow_materialization"
                and planning_info_log_reference_line_available
            ):
                reason = (
                    "Planning first trajectory points align locally with official HDMap projection, "
                    "Control simple_lat reference evidence is visible, and Planning is in lane-follow "
                    "with route segments ready and non-empty trajectories. "
                    "debug.planning_data.reference_line is present with an empty list, while "
                    "apollo_planning.INFO contains internal reference-line text traces."
                )
            elif debug_field_state == "field_present_but_empty_after_lane_follow_materialization":
                reason = (
                    "Planning first trajectory points align locally with official HDMap projection, "
                    "Control simple_lat reference evidence is visible, and Planning is in lane-follow "
                    "with route segments ready and non-empty trajectories, but "
                    "debug.planning_data.reference_line is present with an empty list."
                )
            elif debug_field_state == "field_present_but_empty":
                reason = (
                    "Planning first trajectory points align locally with official HDMap projection "
                    "and Control simple_lat reference evidence is visible, but "
                    "debug.planning_data.reference_line is present with an empty list."
                )
            else:
                reason = (
                    "Planning first trajectory points align locally with official HDMap projection "
                    "and Control simple_lat reference evidence is visible, but Planning reference-line "
                    "debug counters are not exported."
                )
        elif planning_local_alignment:
            export_classification = "reference_line_debug_export_gap_with_planning_local_alignment_only"
            reason = (
                "Planning first trajectory points align locally with official HDMap projection, "
                "but Control simple_lat reference evidence is not available and Planning "
                "reference-line debug counters are not exported."
            )
        elif control_reference_available:
            export_classification = "reference_line_debug_export_gap_with_control_reference_only"
            reason = (
                "Control simple_lat reference evidence is visible, but Planning first-point "
                "projection alignment is unavailable and Planning reference-line debug counters "
                "are not exported."
            )
        else:
            export_classification = "reference_line_debug_export_gap_without_local_surrogate_evidence"
            reason = (
                "Planning has route/trajectory evidence but neither Planning first-point "
                "projection alignment nor Control simple_lat reference evidence is sufficient."
            )
        return {
            "status": "warn",
            "classification": export_classification,
            "reference_line_debug_claim_grade_allowed": False,
            "planning_first_point_local_alignment_available": planning_local_alignment,
            "planning_trajectory_sample_surrogate_available": trajectory_sample_surrogate_available,
            "control_simple_lat_reference_available": control_reference_available,
            "route_segment_available": route_segment_available,
            "planning_debug_presence_classification": presence_classification or None,
            "planning_materialization_classification": materialization_classification or None,
            "reference_line_debug_field_state": debug_field_state,
            "planning_info_log_reference_line_available": planning_info_log_reference_line_available,
            "planning_info_log_reference_line_classification": planning_info_log_classification,
            "planning_info_log_reference_line_claim_grade_allowed": False,
            "recommended_evidence_policy": "local_surrogate_only_until_reference_line_debug_exported",
            "recommended_next_action": (
                (
                    (
                        "Inspect why debug.planning_data.reference_line is empty after lane-follow "
                        "route segments and non-empty trajectories materialize even though "
                        "apollo_planning.INFO prints internal reference-line traces; compare Planning "
                        "ReferenceLineInfo/debug population with Control simple_lat station before "
                        "changing steer scale, smoothing, PID, or actuation mapping."
                    )
                    if planning_info_log_reference_line_available
                    else (
                        "Inspect why debug.planning_data.reference_line is empty after lane-follow "
                        "route segments and non-empty trajectories materialize; compare Planning "
                        "ReferenceLineInfo/debug population with Control simple_lat station before "
                        "changing steer scale, smoothing, PID, or actuation mapping."
                    )
                )
                if debug_field_state == "field_present_but_empty_after_lane_follow_materialization"
                else (
                    "Inspect why debug.planning_data.reference_line is present but empty before "
                    "changing steer scale, smoothing, PID, or actuation mapping."
                )
                if debug_field_state == "field_present_but_empty"
                else (
                    "Export or decode Planning reference-line debug, route segments, and Control "
                    "simple_lat station in one frame before changing steer scale, smoothing, PID, "
                    "or actuation mapping."
                )
            ),
            "reason": reason,
            "claim_boundary": (
                "Local Planning first-point and Control simple_lat evidence can narrow diagnosis, "
                "but cannot by itself support claim-grade reference-line correctness."
            ),
        }

    if (
        debug_field_state == "field_present_but_empty_after_lane_follow_materialization"
        and planning_info_log_reference_line_available
    ):
        insufficient_reason = (
            "debug.planning_data.reference_line is present with an empty list after "
            "lane-follow route segments and non-empty trajectories materialize, while "
            "apollo_planning.INFO contains internal reference-line text traces. "
            f"Reference debug diagnostic classification `{classification or 'missing'}` "
            "still lacks enough exported/same-frame surrogate evidence for claim-grade "
            "reference-line correctness."
        )
        insufficient_next_action = (
            "Inspect why ADCTrajectory debug.planning_data.reference_line remains empty "
            "despite internal apollo_planning.INFO reference-line traces; add same-frame Planning "
            "ReferenceLineInfo/control reference evidence before changing steer scale, smoothing, "
            "PID, or actuation mapping."
        )
    elif debug_field_state == "field_present_but_empty_after_lane_follow_materialization":
        insufficient_reason = (
            "debug.planning_data.reference_line is present with an empty list after "
            "lane-follow route segments and non-empty trajectories materialize, but "
            f"reference debug diagnostic classification `{classification or 'missing'}` "
            "still lacks enough local surrogate evidence for claim-grade reference-line correctness."
        )
        insufficient_next_action = (
            "Inspect why debug.planning_data.reference_line remains empty after lane-follow "
            "route segments and non-empty trajectories materialize; add same-frame Planning "
            "ReferenceLineInfo/control reference evidence before changing steer scale, smoothing, "
            "PID, or actuation mapping."
        )
    elif debug_field_state == "field_present_but_empty":
        insufficient_reason = (
            "debug.planning_data.reference_line is present with an empty list, but "
            f"reference debug diagnostic classification `{classification or 'missing'}` "
            "still lacks enough local surrogate evidence for claim-grade reference-line correctness."
        )
        insufficient_next_action = (
            "Inspect why debug.planning_data.reference_line is present but empty; add same-frame "
            "Planning ReferenceLineInfo/control reference evidence before changing steer scale, "
            "smoothing, PID, or actuation mapping."
        )
    else:
        insufficient_reason = (
            f"Reference debug diagnostic classification `{classification or 'missing'}` is not claim-grade."
        )
        insufficient_next_action = (
            "Collect Planning reference-line debug/export evidence or local Planning/projection "
            "and Control simple_lat surrogate evidence before drawing reference-line conclusions."
        )

    return {
        "status": "insufficient_data",
        "classification": "reference_line_debug_export_policy_insufficient_data",
        "reference_line_debug_claim_grade_allowed": False,
        "planning_first_point_local_alignment_available": planning_local_alignment,
        "planning_trajectory_sample_surrogate_available": trajectory_sample_surrogate_available,
        "control_simple_lat_reference_available": control_reference_available,
        "route_segment_available": route_segment_available,
        "planning_debug_presence_classification": presence_classification or None,
        "planning_materialization_classification": materialization_classification or None,
        "reference_line_debug_field_state": debug_field_state,
        "planning_info_log_reference_line_available": planning_info_log_reference_line_available,
        "planning_info_log_reference_line_classification": planning_info_log_classification,
        "planning_info_log_reference_line_claim_grade_allowed": False,
        "recommended_evidence_policy": "insufficient_reference_line_debug_evidence",
        "recommended_next_action": insufficient_next_action,
        "reason": insufficient_reason,
        "claim_boundary": (
            "Missing reference-line debug/export evidence must remain insufficient_data or "
            "diagnostic-only; it cannot be upgraded from field presence alone."
        ),
    }


def _planning_trajectory_sample_surrogate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    nonempty_rows = [row for row in rows if _planning_trajectory_nonempty(row)]
    rows_with_samples = [
        row
        for row in nonempty_rows
        if _sample_points(_nested(row, "planning.trajectory_sample_points"))
    ]
    if not rows:
        return {
            "status": "insufficient_data",
            "classification": "planning_trajectory_rows_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": 0,
            "nonempty_trajectory_row_count": 0,
            "rows_with_sample_points": 0,
            "sample_coverage_ratio": None,
            "interpretation": (
                "No Planning rows are available; trajectory samples cannot support even a "
                "local reference-line surrogate diagnostic."
            ),
        }
    if not nonempty_rows:
        return {
            "status": "insufficient_data",
            "classification": "planning_trajectory_nonempty_rows_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "nonempty_trajectory_row_count": 0,
            "rows_with_sample_points": 0,
            "sample_coverage_ratio": None,
            "interpretation": (
                "Planning trajectory samples require non-empty Planning trajectories; this "
                "cannot substitute for reference-line debug."
            ),
        }
    if not rows_with_samples:
        return {
            "status": "insufficient_data",
            "classification": "planning_trajectory_sample_points_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "nonempty_trajectory_row_count": len(nonempty_rows),
            "rows_with_sample_points": 0,
            "sample_coverage_ratio": 0.0,
            "interpretation": (
                "Planning trajectories are non-empty, but sampled trajectory points are not "
                "present in the artifacts. Reference-line debug remains required for claims."
            ),
        }

    heading_errors: list[float] = []
    path_lengths: list[float] = []
    sample_counts: list[float] = []
    lane_ids: list[str] = []
    target_lane_ids: list[str] = []
    routing_signatures: list[str] = []
    for row in rows_with_samples:
        samples = _sample_points(_nested(row, "planning.trajectory_sample_points"))
        sample_counts.append(float(len(samples)))
        lane_ids.extend(_list_value(_nested(row, "planning.lane_ids")))
        target_lane_ids.extend(_list_value(_nested(row, "planning.target_lane_ids")))
        routing_signature = str(_nested(row, "routing.routing_unique_lane_signature") or "").strip()
        if routing_signature:
            routing_signatures.append(routing_signature)
        path_length = _num(_nested(row, "planning.trajectory_total_path_length"))
        if path_length is None:
            path_length = _sample_path_length(samples)
        if path_length is not None:
            path_lengths.append(path_length)
        first_theta = _num(_nested(row, "planning.first_trajectory_point_theta"))
        sample_heading = _sample_first_segment_heading(samples)
        heading_error = _angle_delta(first_theta, sample_heading)
        if heading_error is not None:
            heading_errors.append(abs(heading_error))

    sample_coverage_ratio = len(rows_with_samples) / len(nonempty_rows)
    normalized_planning_lanes = {_normalize_lane_id(item) for item in lane_ids + target_lane_ids}
    normalized_routing_lanes = {
        _normalize_lane_id(item)
        for signature in routing_signatures
        for item in signature.replace(";", ",").replace("|", ",").split(",")
        if item.strip()
    }
    lane_window_compatible = (
        bool(normalized_planning_lanes & normalized_routing_lanes)
        if normalized_planning_lanes and normalized_routing_lanes
        else None
    )
    if lane_window_compatible is True:
        classification = "planning_trajectory_sample_surrogate_same_route_lane_window"
    elif lane_window_compatible is False:
        classification = "planning_trajectory_sample_surrogate_lane_window_mismatch"
    else:
        classification = "planning_trajectory_sample_surrogate_available_lane_window_unknown"
    return {
        "status": "available" if lane_window_compatible is not False else "warn",
        "classification": classification,
        "available": True,
        "reference_line_claim_grade_allowed": False,
        "row_count": len(rows),
        "nonempty_trajectory_row_count": len(nonempty_rows),
        "rows_with_sample_points": len(rows_with_samples),
        "sample_coverage_ratio": sample_coverage_ratio,
        "sample_count_p95": _p95_abs(sample_counts),
        "trajectory_path_length_p95_m": _p95_abs(path_lengths),
        "first_theta_vs_sample_segment_heading_p95_rad": _p95_abs(heading_errors),
        "planning_lane_id_topk": _topk(lane_ids),
        "planning_target_lane_id_topk": _topk(target_lane_ids),
        "routing_unique_lane_signature_topk": _topk(routing_signatures),
        "lane_window_compatible": lane_window_compatible,
        "recommended_evidence_policy": "trajectory_shape_surrogate_only_until_reference_line_debug_exported",
        "interpretation": (
            "Planning trajectory samples show local trajectory shape and lane-window consistency. "
            "They are diagnostic surrogate evidence only and cannot replace exported Planning "
            "reference-line debug for claim-grade reference-line correctness."
        ),
    }


def _control_target_point_vs_planning_trajectory_sample(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows_with_samples = [
        row
        for row in rows
        if _planning_trajectory_nonempty(row)
        and _sample_points(_nested(row, "planning.trajectory_sample_points"))
    ]
    rows_with_target = [
        row
        for row in rows_with_samples
        if _num(_nested(row, "control.debug_simple_lat_current_target_point_x")) is not None
        and _num(_nested(row, "control.debug_simple_lat_current_target_point_y")) is not None
    ]
    if not rows:
        return {
            "status": "insufficient_data",
            "classification": "reference_rows_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": 0,
            "rows_with_planning_samples": 0,
            "rows_with_control_target_point": 0,
            "target_point_to_planning_sample_line_abs_p95_m": None,
            "interpretation": (
                "No reference-line rows are available; Control target point cannot be compared "
                "with Planning trajectory samples."
            ),
        }
    if not rows_with_samples:
        return {
            "status": "insufficient_data",
            "classification": "planning_trajectory_sample_points_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_planning_samples": 0,
            "rows_with_control_target_point": 0,
            "target_point_to_planning_sample_line_abs_p95_m": None,
            "interpretation": (
                "Planning trajectory samples are required before Control target point can be "
                "checked against the local Planning trajectory shape."
            ),
        }
    if not rows_with_target:
        return {
            "status": "insufficient_data",
            "classification": "control_target_point_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_planning_samples": len(rows_with_samples),
            "rows_with_control_target_point": 0,
            "target_point_to_planning_sample_line_abs_p95_m": None,
            "interpretation": (
                "Control simple_lat target point coordinates are missing or not time-aligned "
                "with Planning trajectory samples."
            ),
        }

    distances: list[float] = []
    sample_counts: list[float] = []
    dropped_short_sample_rows = 0
    for row in rows_with_target:
        samples = _sample_points(_nested(row, "planning.trajectory_sample_points"))
        sample_counts.append(float(len(samples)))
        target_x = _num(_nested(row, "control.debug_simple_lat_current_target_point_x"))
        target_y = _num(_nested(row, "control.debug_simple_lat_current_target_point_y"))
        if target_x is None or target_y is None:
            continue
        distance = _point_to_polyline_distance((target_x, target_y), samples)
        if distance is None:
            dropped_short_sample_rows += 1
            continue
        distances.append(abs(distance))

    p95 = _p95_abs(distances)
    if p95 is None:
        classification = "planning_trajectory_sample_line_too_short"
        status = "insufficient_data"
    elif p95 <= CONTROL_TARGET_TRAJECTORY_SAMPLE_WARN_M:
        classification = "control_target_point_on_planning_trajectory_sample_line_candidate"
        status = "available"
    else:
        classification = "control_target_point_off_planning_trajectory_sample_line_candidate"
        status = "warn"
    return {
        "status": status,
        "classification": classification,
        "available": p95 is not None,
        "reference_line_claim_grade_allowed": False,
        "row_count": len(rows),
        "rows_with_planning_samples": len(rows_with_samples),
        "rows_with_control_target_point": len(rows_with_target),
        "sample_coverage_ratio": len(rows_with_target) / len(rows_with_samples),
        "target_point_to_planning_sample_line_abs_p95_m": p95,
        "target_point_to_planning_sample_line_abs_max_m": max(distances) if distances else None,
        "planning_sample_count_p95": _p95_abs(sample_counts),
        "dropped_short_sample_rows": dropped_short_sample_rows,
        "threshold_warn_m": CONTROL_TARGET_TRAJECTORY_SAMPLE_WARN_M,
        "recommended_evidence_policy": "control_target_planning_sample_surrogate_only_until_reference_line_debug_exported",
        "interpretation": (
            "This compares Control simple_lat target point coordinates with the sampled Planning "
            "trajectory polyline in the same artifacts. It narrows Planning/Control local geometry "
            "alignment, but cannot replace exported Planning reference-line debug for claim-grade "
            "reference-line correctness."
        ),
    }


def _control_target_point_vs_planning_path_candidate_sample(
    rows: Sequence[Mapping[str, Any]],
    hdmap_projection_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    projection_rows = [
        row
        for row in hdmap_projection_rows
        if str(row.get("source") or "") == "apollo_hdmap_api"
        and str(row.get("status") or row.get("projection_status") or "").lower() in {"ok", "pass"}
        and _projection_timestamp(row) is not None
        and _num(
            row.get("localization_x")
            if row.get("localization_x") is not None
            else row.get("x_apollo")
        )
        is not None
        and _num(
            row.get("localization_y")
            if row.get("localization_y") is not None
            else row.get("y_apollo")
        )
        is not None
        and _num(row.get("lane_heading_at_s")) is not None
    ]
    rows_with_path_points = [
        row for row in rows if _planning_debug_path_candidate_sample_points(row)
    ]
    rows_with_target = [
        row
        for row in rows_with_path_points
        if _num(_nested(row, "control.debug_simple_lat_current_target_point_x")) is not None
        and _num(_nested(row, "control.debug_simple_lat_current_target_point_y")) is not None
    ]
    if not rows:
        return {
            "status": "insufficient_data",
            "classification": "reference_rows_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": 0,
            "rows_with_path_candidate_sample_points": 0,
            "rows_with_control_target_point": 0,
            "target_point_to_path_candidate_line_abs_p95_m": None,
            "target_point_lane_l_abs_p95_m": None,
            "interpretation": (
                "No reference-line rows are available; Control target point cannot be "
                "compared with Planning debug path candidates."
            ),
        }
    if not rows_with_path_points:
        return {
            "status": "insufficient_data",
            "classification": "planning_debug_path_candidate_sample_points_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_path_candidate_sample_points": 0,
            "rows_with_control_target_point": 0,
            "target_point_to_path_candidate_line_abs_p95_m": None,
            "target_point_lane_l_abs_p95_m": None,
            "interpretation": (
                "Planning debug path candidate sample points are missing, so Control "
                "simple_lat target point cannot be compared with candidate path geometry."
            ),
        }
    if not rows_with_target:
        return {
            "status": "insufficient_data",
            "classification": "control_target_point_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_path_candidate_sample_points": len(rows_with_path_points),
            "rows_with_control_target_point": 0,
            "target_point_to_path_candidate_line_abs_p95_m": None,
            "target_point_lane_l_abs_p95_m": None,
            "interpretation": (
                "Control simple_lat target point coordinates are missing or not aligned "
                "with sampled Planning debug path candidates."
            ),
        }

    target_to_path_distances: list[float] = []
    target_lane_l_values: list[float] = []
    path_lane_l_values: list[float] = []
    join_deltas_ms: list[float] = []
    candidate_names: list[str] = []
    candidate_paths: list[str] = []
    rows_with_distance = 0
    rows_with_projection = 0
    rows_with_path_lane_l = 0
    for row in rows_with_target:
        target_x = _num(_nested(row, "control.debug_simple_lat_current_target_point_x"))
        target_y = _num(_nested(row, "control.debug_simple_lat_current_target_point_y"))
        if target_x is None or target_y is None:
            continue
        row_points = _planning_debug_path_candidate_sample_points(row)
        samples = _sample_points(_nested(row, "planning.trajectory_sample_points"))
        supported_points = (
            _points_within_sample_support(
                row_points,
                samples,
                margin_m=PLANNING_PATH_CANDIDATE_TRAJECTORY_SUPPORT_MARGIN_M,
            )
            if samples
            else row_points
        )
        grouped_points = _group_path_candidate_points(supported_points or row_points)
        distances = [
            distance
            for points in grouped_points.values()
            if (distance := _point_to_polyline_distance((target_x, target_y), points)) is not None
        ]
        if distances:
            target_to_path_distances.append(min(abs(distance) for distance in distances))
            rows_with_distance += 1
        row_ts = _row_sim_timestamp(row)
        projection = (
            _nearest_projection_row(
                row_ts,
                projection_rows,
                tolerance_s=PLANNING_PROJECTION_JOIN_TOLERANCE_S,
            )
            if row_ts is not None
            else None
        )
        if projection is None:
            continue
        rows_with_projection += 1
        matched_ts = _projection_timestamp(projection)
        if row_ts is not None and matched_ts is not None:
            join_deltas_ms.append(abs(row_ts - matched_ts) * 1000.0)
        target_lane_l = _point_lane_l_from_projection(target_x, target_y, projection)
        if target_lane_l is not None:
            target_lane_l_values.append(target_lane_l)
        row_path_lane_l_count = 0
        for point in supported_points or row_points:
            x = _num(point.get("x"))
            y = _num(point.get("y"))
            if x is None or y is None:
                continue
            lane_l = _point_lane_l_from_projection(x, y, projection)
            if lane_l is None:
                continue
            path_lane_l_values.append(lane_l)
            row_path_lane_l_count += 1
            candidate_names.append(str(point.get("candidate_name") or ""))
            candidate_paths.append(str(point.get("candidate_path") or ""))
        if row_path_lane_l_count:
            rows_with_path_lane_l += 1

    distance_p95 = _p95_abs(target_to_path_distances)
    target_lane_l_p95 = _p95_abs(target_lane_l_values)
    path_lane_l_p95 = _p95_abs(path_lane_l_values)
    path_lane_l_min = min(path_lane_l_values) if path_lane_l_values else None
    path_lane_l_max = max(path_lane_l_values) if path_lane_l_values else None
    target_inside_path_lateral_envelope = (
        path_lane_l_min is not None
        and path_lane_l_max is not None
        and bool(target_lane_l_values)
        and min(target_lane_l_values) >= path_lane_l_min - PASS_LATERAL_M
        and max(target_lane_l_values) <= path_lane_l_max + PASS_LATERAL_M
    )
    path_candidates_straddle_lane_center = (
        path_lane_l_min is not None
        and path_lane_l_max is not None
        and path_lane_l_min < -PASS_LATERAL_M
        and path_lane_l_max > PASS_LATERAL_M
    )
    if distance_p95 is None:
        status = "insufficient_data"
        classification = "control_target_path_candidate_join_no_distance"
    elif distance_p95 <= CONTROL_TARGET_PATH_CANDIDATE_WARN_M:
        status = "available"
        classification = "control_target_on_planning_path_candidate_line"
    elif (
        target_lane_l_p95 is not None
        and target_lane_l_p95 <= PASS_LATERAL_M
        and target_inside_path_lateral_envelope
        and path_candidates_straddle_lane_center
    ):
        status = "available"
        classification = "control_target_between_planning_path_candidate_lateral_bounds"
    elif target_lane_l_p95 is not None and target_lane_l_p95 > WARN_LATERAL_M:
        status = "warn"
        classification = "control_target_lateral_offset_from_hdmap_lane_center"
    else:
        status = "warn"
        classification = "control_target_offset_from_planning_path_candidate_line"
    return {
        "status": status,
        "classification": classification,
        "available": distance_p95 is not None,
        "reference_line_claim_grade_allowed": False,
        "row_count": len(rows),
        "rows_with_path_candidate_sample_points": len(rows_with_path_points),
        "rows_with_control_target_point": len(rows_with_target),
        "rows_with_path_candidate_distance": rows_with_distance,
        "rows_with_projection": rows_with_projection,
        "rows_with_path_lane_l": rows_with_path_lane_l,
        "sample_coverage_ratio": len(rows_with_target) / len(rows_with_path_points),
        "target_point_to_path_candidate_line_abs_p95_m": distance_p95,
        "target_point_to_path_candidate_line_abs_max_m": (
            max(target_to_path_distances) if target_to_path_distances else None
        ),
        "target_point_lane_l_abs_p95_m": target_lane_l_p95,
        "path_candidate_lane_l_abs_p95_m": path_lane_l_p95,
        "path_candidate_lane_l_min_m": path_lane_l_min,
        "path_candidate_lane_l_max_m": path_lane_l_max,
        "target_inside_path_lateral_envelope": target_inside_path_lateral_envelope,
        "path_candidates_straddle_lane_center": path_candidates_straddle_lane_center,
        "join_delta_p95_ms": _p95_abs(join_deltas_ms),
        "join_tolerance_ms": PLANNING_PROJECTION_JOIN_TOLERANCE_S * 1000.0,
        "threshold_warn_m": CONTROL_TARGET_PATH_CANDIDATE_WARN_M,
        "candidate_path_topk": _topk(candidate_paths),
        "candidate_name_topk": _topk(candidate_names),
        "recommended_evidence_policy": "control_target_path_candidate_surrogate_only_until_reference_line_debug_exported",
        "interpretation": (
            "This compares Control simple_lat target point coordinates with sampled "
            "debug.planning_data.path candidates and, when official HDMap projection rows "
            "exist, reports whether the target point lies near the lane center while path "
            "candidates span a lateral envelope. It is diagnostic-only because path "
            "candidates may be optimizer or boundary debug paths, not exported Planning "
            "reference-line centerlines."
        ),
    }


def _planning_debug_path_candidate_hdmap_projection_alignment(
    rows: Sequence[Mapping[str, Any]],
    hdmap_projection_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    projection_rows = [
        row
        for row in hdmap_projection_rows
        if str(row.get("source") or "") == "apollo_hdmap_api"
        and str(row.get("status") or row.get("projection_status") or "").lower() in {"ok", "pass"}
        and _projection_timestamp(row) is not None
        and _num(row.get("localization_x") if row.get("localization_x") is not None else row.get("x_apollo")) is not None
        and _num(row.get("localization_y") if row.get("localization_y") is not None else row.get("y_apollo")) is not None
        and _num(row.get("lane_heading_at_s")) is not None
    ]
    rows_with_path_points = [
        row for row in rows if _planning_debug_path_candidate_sample_points(row)
    ]
    if not rows:
        return {
            "status": "insufficient_data",
            "classification": "reference_rows_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": 0,
            "rows_with_path_candidate_sample_points": 0,
            "official_projection_rows": len(projection_rows),
            "path_candidate_lane_l_abs_p95_m": None,
            "interpretation": (
                "No reference-line rows are available; Planning debug path candidates "
                "cannot be joined to official Apollo HDMap projection rows."
            ),
        }
    if not projection_rows:
        return {
            "status": "insufficient_data",
            "classification": "official_projection_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_path_candidate_sample_points": len(rows_with_path_points),
            "official_projection_rows": 0,
            "path_candidate_lane_l_abs_p95_m": None,
            "interpretation": (
                "Official Apollo HDMap projection rows are required before debug path "
                "candidates can be compared with lane heading/lateral semantics."
            ),
        }
    if not rows_with_path_points:
        return {
            "status": "insufficient_data",
            "classification": "planning_debug_path_candidate_sample_points_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_path_candidate_sample_points": 0,
            "official_projection_rows": len(projection_rows),
            "path_candidate_lane_l_abs_p95_m": None,
            "interpretation": (
                "Planning debug path candidate sample points are missing, so no official "
                "HDMap projection alignment can be computed."
            ),
        }

    lane_l_values: list[float] = []
    heading_errors: list[float] = []
    join_deltas_ms: list[float] = []
    candidate_names: list[str] = []
    candidate_paths: list[str] = []
    projection_lanes: list[str] = []
    route_lanes: list[str] = []
    sample_sources: list[str] = []
    support_point_counts: list[float] = []
    unmatched_rows = 0
    rows_with_projection = 0
    rows_with_projected_points = 0
    for row in rows_with_path_points:
        row_ts = _row_sim_timestamp(row)
        if row_ts is None:
            unmatched_rows += 1
            continue
        projection = _nearest_projection_row(
            row_ts,
            projection_rows,
            tolerance_s=PLANNING_PROJECTION_JOIN_TOLERANCE_S,
        )
        if projection is None:
            unmatched_rows += 1
            continue
        rows_with_projection += 1
        matched_ts = _projection_timestamp(projection)
        if matched_ts is not None:
            join_deltas_ms.append(abs(row_ts - matched_ts) * 1000.0)
        lane_id = projection.get("nearest_lane_id")
        if lane_id not in {None, ""}:
            projection_lanes.append(str(lane_id))
        route_lanes.extend(_route_lane_candidates(row))
        row_points = _planning_debug_path_candidate_sample_points(row)
        samples = _sample_points(_nested(row, "planning.trajectory_sample_points"))
        supported_points = (
            _points_within_sample_support(
                row_points,
                samples,
                margin_m=PLANNING_PATH_CANDIDATE_TRAJECTORY_SUPPORT_MARGIN_M,
            )
            if samples
            else row_points
        )
        support_point_counts.append(float(len(supported_points)))
        row_projected = 0
        for point in supported_points:
            x = _num(point.get("x"))
            y = _num(point.get("y"))
            if x is None or y is None:
                continue
            lane_l = _point_lane_l_from_projection(x, y, projection)
            if lane_l is None:
                continue
            lane_l_values.append(lane_l)
            row_projected += 1
            candidate_names.append(str(point.get("candidate_name") or ""))
            candidate_paths.append(str(point.get("candidate_path") or ""))
            sample_sources.append(str(point.get("sample_source") or "unknown"))
            theta = _num(point.get("theta"))
            lane_heading = _num(projection.get("lane_heading_at_s"))
            if theta is not None and lane_heading is not None:
                heading_errors.append(_angle_delta(theta, lane_heading) or 0.0)
        if row_projected > 0:
            rows_with_projected_points += 1

    sample_count = len(lane_l_values)
    lane_l_p95 = _p95_abs(lane_l_values)
    heading_p95 = _p95_abs(heading_errors)
    normalized_projection_lanes = {_normalize_lane_id(item) for item in projection_lanes if item}
    normalized_route_lanes = {
        _normalize_lane_id(item)
        for item in route_lanes
        if item and str(item).lower() != "none"
    }
    routing_lane_window_compatible = (
        bool(normalized_projection_lanes & normalized_route_lanes)
        if normalized_projection_lanes and normalized_route_lanes
        else None
    )
    if sample_count <= 0:
        status = "insufficient_data"
        classification = "projection_join_missing"
    elif routing_lane_window_compatible is False:
        status = "warn"
        classification = "planning_debug_path_candidate_projection_lane_window_mismatch"
    elif (lane_l_p95 or 0.0) <= PASS_LATERAL_M and (
        heading_p95 is None or heading_p95 <= PASS_HEADING_RAD
    ):
        status = "available"
        classification = "planning_debug_path_candidate_near_hdmap_lane_center"
    elif (lane_l_p95 or 0.0) <= 3.50 and (
        heading_p95 is None or heading_p95 <= WARN_HEADING_RAD
    ):
        status = "available"
        classification = "planning_debug_path_candidate_lateral_offset_from_hdmap_lane_center"
    else:
        status = "warn"
        classification = "planning_debug_path_candidate_hdmap_projection_alignment_elevated"
    return {
        "status": status,
        "classification": classification,
        "available": sample_count > 0,
        "reference_line_claim_grade_allowed": False,
        "row_count": len(rows),
        "rows_with_path_candidate_sample_points": len(rows_with_path_points),
        "rows_with_projection": rows_with_projection,
        "rows_with_projected_points": rows_with_projected_points,
        "official_projection_rows": len(projection_rows),
        "sample_count": sample_count,
        "unmatched_rows": unmatched_rows,
        "join_tolerance_ms": PLANNING_PROJECTION_JOIN_TOLERANCE_S * 1000.0,
        "join_delta_p95_ms": _p95_abs(join_deltas_ms),
        "path_candidate_support_sample_point_count_p95": _p95_abs(support_point_counts),
        "path_candidate_lane_l_abs_p95_m": lane_l_p95,
        "path_candidate_lane_l_min_m": min(lane_l_values) if lane_l_values else None,
        "path_candidate_lane_l_max_m": max(lane_l_values) if lane_l_values else None,
        "path_candidate_lane_heading_error_p95_rad": heading_p95,
        "nearest_lane_id_topk": _topk(projection_lanes),
        "route_lane_candidate_topk": _topk(route_lanes),
        "routing_lane_window_compatible": routing_lane_window_compatible,
        "candidate_path_topk": _topk(candidate_paths),
        "candidate_name_topk": _topk(candidate_names),
        "sample_source_topk": _topk(sample_sources),
        "recommended_evidence_policy": "path_candidate_hdmap_projection_surrogate_only_until_reference_line_debug_exported",
        "interpretation": (
            "This joins sampled debug.planning_data.path candidates to official Apollo HDMap "
            "projection rows and reports local lane-lateral distribution. It can distinguish "
            "a centerline-like candidate from boundary/optimizer path candidates, but it is "
            "still diagnostic-only and cannot replace exported Planning reference-line debug "
            "or prove behavior success."
        ),
    }


def _route_lane_candidates(row: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    out.extend(_list_value(_nested(row, "planning.lane_ids")))
    out.extend(_list_value(_nested(row, "planning.target_lane_ids")))
    signature = str(_nested(row, "routing.routing_unique_lane_signature") or "")
    for item in signature.replace(";", ",").replace("|", ",").split(","):
        item = item.strip()
        if item and item.lower() != "none":
            out.append(item)
    return out


def _point_lane_l_from_projection(x: float, y: float, projection: Mapping[str, Any]) -> float | None:
    lane_heading = _num(projection.get("lane_heading_at_s"))
    loc_x = _num(
        projection.get("localization_x")
        if projection.get("localization_x") is not None
        else projection.get("x_apollo")
    )
    loc_y = _num(
        projection.get("localization_y")
        if projection.get("localization_y") is not None
        else projection.get("y_apollo")
    )
    if lane_heading is None or loc_x is None or loc_y is None:
        return None
    projection_l = _num(projection.get("projection_l")) or 0.0
    left_x = -math.sin(lane_heading)
    left_y = math.cos(lane_heading)
    return projection_l + (x - loc_x) * left_x + (y - loc_y) * left_y


def _planning_debug_path_candidate_vs_trajectory_sample(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows_with_samples = [
        row
        for row in rows
        if _planning_trajectory_nonempty(row)
        and _sample_points(_nested(row, "planning.trajectory_sample_points"))
    ]
    rows_with_path_points = [
        row for row in rows_with_samples if _planning_debug_path_candidate_sample_points(row)
    ]
    if not rows:
        return {
            "status": "insufficient_data",
            "classification": "reference_rows_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": 0,
            "rows_with_planning_samples": 0,
            "rows_with_path_candidate_sample_points": 0,
            "path_candidate_to_planning_sample_line_abs_p95_m": None,
            "interpretation": (
                "No reference-line rows are available; Planning debug path candidates "
                "cannot be compared with Planning trajectory samples."
            ),
        }
    if not rows_with_samples:
        return {
            "status": "insufficient_data",
            "classification": "planning_trajectory_sample_points_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_planning_samples": 0,
            "rows_with_path_candidate_sample_points": 0,
            "path_candidate_to_planning_sample_line_abs_p95_m": None,
            "interpretation": (
                "Planning trajectory sample points are required before debug path "
                "candidates can be checked against the local trajectory shape."
            ),
        }
    if not rows_with_path_points:
        return {
            "status": "insufficient_data",
            "classification": "planning_debug_path_candidate_sample_points_missing",
            "available": False,
            "reference_line_claim_grade_allowed": False,
            "row_count": len(rows),
            "rows_with_planning_samples": len(rows_with_samples),
            "rows_with_path_candidate_sample_points": 0,
            "path_candidate_to_planning_sample_line_abs_p95_m": None,
            "interpretation": (
                "Planning debug path candidates are present only as counts or first-point "
                "metadata, or are missing from the aligned rows. A sampled candidate "
                "polyline is needed for this diagnostic join."
            ),
        }

    support_distances: list[float] = []
    full_sample_distances: list[float] = []
    candidate_paths: list[str] = []
    candidate_names: list[str] = []
    sample_sources: list[str] = []
    path_point_counts: list[float] = []
    support_point_counts: list[float] = []
    dropped_short_sample_rows = 0
    rows_with_distance = 0
    rows_with_support_distance = 0
    for row in rows_with_path_points:
        samples = _sample_points(_nested(row, "planning.trajectory_sample_points"))
        row_points = _planning_debug_path_candidate_sample_points(row)
        path_point_counts.append(float(len(row_points)))
        supported_points = _points_within_sample_support(
            row_points,
            samples,
            margin_m=PLANNING_PATH_CANDIDATE_TRAJECTORY_SUPPORT_MARGIN_M,
        )
        support_point_counts.append(float(len(supported_points)))
        row_full_distances = []
        row_support_distances = []
        for point in row_points:
            x = _num(point.get("x"))
            y = _num(point.get("y"))
            if x is None or y is None:
                continue
            distance = _point_to_polyline_distance((x, y), samples)
            if distance is None:
                continue
            row_full_distances.append(abs(distance))
            candidate_paths.append(str(point.get("candidate_path") or ""))
            candidate_names.append(str(point.get("candidate_name") or ""))
            sample_sources.append(str(point.get("sample_source") or "unknown"))
        for point in supported_points:
            x = _num(point.get("x"))
            y = _num(point.get("y"))
            if x is None or y is None:
                continue
            distance = _point_to_polyline_distance((x, y), samples)
            if distance is not None:
                row_support_distances.append(abs(distance))
        if row_full_distances:
            rows_with_distance += 1
            full_sample_distances.extend(row_full_distances)
        if row_support_distances:
            rows_with_support_distance += 1
            support_distances.extend(row_support_distances)
        if not row_full_distances and not row_support_distances:
            dropped_short_sample_rows += 1

    p95 = _p95_abs(support_distances)
    full_p95 = _p95_abs(full_sample_distances)
    if p95 is None:
        if full_p95 is not None:
            classification = "planning_debug_path_candidate_samples_outside_planning_trajectory_sample_support"
            status = "warn"
        else:
            classification = "planning_debug_path_candidate_join_no_distance"
            status = "insufficient_data"
    elif p95 <= PLANNING_PATH_CANDIDATE_TRAJECTORY_WARN_M:
        classification = "planning_debug_path_candidate_near_planning_trajectory_sample_support"
        status = "available"
    else:
        classification = "planning_debug_path_candidate_offset_from_planning_trajectory_sample_support"
        status = "warn"
    return {
        "status": status,
        "classification": classification,
        "available": p95 is not None,
        "reference_line_claim_grade_allowed": False,
        "row_count": len(rows),
        "rows_with_planning_samples": len(rows_with_samples),
        "rows_with_path_candidate_sample_points": len(rows_with_path_points),
        "rows_with_distance": rows_with_distance,
        "rows_with_support_distance": rows_with_support_distance,
        "sample_coverage_ratio": len(rows_with_path_points) / len(rows_with_samples),
        "path_candidate_sample_point_count_p95": _p95_abs(path_point_counts),
        "path_candidate_support_sample_point_count_p95": _p95_abs(support_point_counts),
        "path_candidate_to_planning_sample_line_abs_p95_m": p95,
        "path_candidate_to_planning_sample_line_abs_max_m": max(support_distances) if support_distances else None,
        "path_candidate_full_sample_to_planning_sample_line_abs_p95_m": full_p95,
        "path_candidate_full_sample_to_planning_sample_line_abs_max_m": (
            max(full_sample_distances) if full_sample_distances else None
        ),
        "threshold_warn_m": PLANNING_PATH_CANDIDATE_TRAJECTORY_WARN_M,
        "support_margin_m": PLANNING_PATH_CANDIDATE_TRAJECTORY_SUPPORT_MARGIN_M,
        "dropped_short_sample_rows": dropped_short_sample_rows,
        "candidate_path_topk": _topk(candidate_paths),
        "candidate_name_topk": _topk(candidate_names),
        "sample_source_topk": _topk(sample_sources),
        "recommended_evidence_policy": "path_candidate_trajectory_surrogate_only_until_reference_line_debug_exported",
        "interpretation": (
            "This compares sampled debug.planning_data.path candidates with the same-row "
            "sampled Planning trajectory polyline. The primary p95 is computed only over "
            "path samples inside the trajectory sample support window; full-horizon distances "
            "are reported separately because debug path candidates can extend far beyond the "
            "small trajectory sample slice. Path candidates can be boundary or optimizer "
            "debug paths rather than the reference-line centerline, so this narrows "
            "Planning debug/export attribution but cannot replace exported Planning "
            "reference-line debug for claim-grade reference-line correctness."
        ),
    }


def _points_within_sample_support(
    points: Sequence[Mapping[str, Any]],
    samples: Sequence[Mapping[str, Any]],
    *,
    margin_m: float,
) -> list[Mapping[str, Any]]:
    sample_xy = [
        (x, y)
        for sample in samples
        for x, y in [(_num(sample.get("x")), _num(sample.get("y")))]
        if x is not None and y is not None
    ]
    if not sample_xy:
        return []
    min_x = min(item[0] for item in sample_xy) - margin_m
    max_x = max(item[0] for item in sample_xy) + margin_m
    min_y = min(item[1] for item in sample_xy) - margin_m
    max_y = max(item[1] for item in sample_xy) + margin_m
    out: list[Mapping[str, Any]] = []
    for point in points:
        x = _num(point.get("x"))
        y = _num(point.get("y"))
        if x is None or y is None:
            continue
        if min_x <= x <= max_x and min_y <= y <= max_y:
            out.append(point)
    return out


def _planning_debug_path_candidate_sample_points(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    summary = _mapping(_nested(row, "planning_debug_presence.path_candidate_summary"))
    out: list[dict[str, Any]] = []
    for candidate in _sequence_of_mappings(summary.get("candidates")):
        path = str(candidate.get("path") or "")
        if "path" not in path.lower() and "trajectory" not in path.lower():
            continue
        for item in _sequence_of_mappings(candidate.get("item_summaries")):
            scalar_fields = _mapping(item.get("scalar_fields"))
            item_name = str(scalar_fields.get("name") or "")
            item_index = item.get("index")
            for point_sequence in _sequence_of_mappings(item.get("point_sequence_candidates")):
                field = str(point_sequence.get("field") or "")
                samples = _sample_points(point_sequence.get("sample_points"))
                sample_source = "sample_points"
                if not samples:
                    samples = _sample_points([point_sequence.get("first_point")])
                    sample_source = "first_point_only"
                for point in samples:
                    payload = dict(point)
                    payload.update(
                        {
                            "candidate_path": path,
                            "candidate_name": item_name,
                            "candidate_item_index": item_index,
                            "candidate_field": field,
                            "sample_source": sample_source,
                        }
                    )
                    out.append(payload)
    return out


def _group_path_candidate_points(points: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str, str, str], list[Mapping[str, Any]]]:
    grouped: dict[tuple[str, str, str, str], list[Mapping[str, Any]]] = {}
    for point in points:
        key = (
            str(point.get("candidate_path") or ""),
            str(point.get("candidate_name") or ""),
            str(point.get("candidate_item_index") or ""),
            str(point.get("candidate_field") or ""),
        )
        grouped.setdefault(key, []).append(point)
    return grouped


def _planning_first_point_projection_alignment(
    rows: Sequence[Mapping[str, Any]],
    hdmap_projection_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    projection_rows = [
        row
        for row in hdmap_projection_rows
        if str(row.get("source") or "") == "apollo_hdmap_api"
        and str(row.get("status") or row.get("projection_status") or "").lower() in {"ok", "pass"}
        and _projection_timestamp(row) is not None
        and _num(row.get("localization_x") if row.get("localization_x") is not None else row.get("x_apollo")) is not None
        and _num(row.get("localization_y") if row.get("localization_y") is not None else row.get("y_apollo")) is not None
        and _num(row.get("lane_heading_at_s")) is not None
    ]
    rows_with_planning = [
        row
        for row in rows
        if _planning_trajectory_nonempty(row)
        and _num(_nested(row, "planning.first_trajectory_point_x")) is not None
        and _num(_nested(row, "planning.first_trajectory_point_y")) is not None
    ]
    if not rows_with_planning:
        return {
            "status": "insufficient_data",
            "classification": "planning_first_point_missing",
            "sample_count": 0,
            "planning_rows_with_first_point": 0,
            "official_projection_rows": len(projection_rows),
            "join_tolerance_ms": PLANNING_PROJECTION_JOIN_TOLERANCE_S * 1000.0,
            "interpretation": (
                "No non-empty Planning first trajectory point rows were available for "
                "local HDMap projection alignment."
            ),
        }
    if not projection_rows:
        return {
            "status": "insufficient_data",
            "classification": "official_projection_missing",
            "sample_count": 0,
            "planning_rows_with_first_point": len(rows_with_planning),
            "official_projection_rows": 0,
            "join_tolerance_ms": PLANNING_PROJECTION_JOIN_TOLERANCE_S * 1000.0,
            "interpretation": (
                "Official Apollo HDMap projection rows are required; CARLA route or "
                "Planning trajectory points cannot substitute for this diagnostic."
            ),
        }

    lateral_offsets: list[float] = []
    heading_errors: list[float] = []
    join_deltas_ms: list[float] = []
    lane_ids: list[str] = []
    unmatched = 0
    for row in rows_with_planning:
        row_ts = _row_sim_timestamp(row)
        if row_ts is None:
            unmatched += 1
            continue
        projection = _nearest_projection_row(
            row_ts,
            projection_rows,
            tolerance_s=PLANNING_PROJECTION_JOIN_TOLERANCE_S,
        )
        if projection is None:
            unmatched += 1
            continue
        matched_ts = _projection_timestamp(projection)
        if matched_ts is not None:
            join_deltas_ms.append(abs(row_ts - matched_ts) * 1000.0)
        lane_heading = _num(projection.get("lane_heading_at_s"))
        planning_x = _num(_nested(row, "planning.first_trajectory_point_x"))
        planning_y = _num(_nested(row, "planning.first_trajectory_point_y"))
        loc_x = _num(projection.get("localization_x") if projection.get("localization_x") is not None else projection.get("x_apollo"))
        loc_y = _num(projection.get("localization_y") if projection.get("localization_y") is not None else projection.get("y_apollo"))
        if lane_heading is None or planning_x is None or planning_y is None or loc_x is None or loc_y is None:
            unmatched += 1
            continue
        projection_l = _num(projection.get("projection_l"))
        left_x = -math.sin(lane_heading)
        left_y = math.cos(lane_heading)
        planning_l = (projection_l or 0.0) + (planning_x - loc_x) * left_x + (planning_y - loc_y) * left_y
        lateral_offsets.append(planning_l)
        planning_theta = _num(_nested(row, "planning.first_trajectory_point_theta"))
        if planning_theta is not None:
            heading_errors.append(_angle_delta(planning_theta, lane_heading) or 0.0)
        lane_id = projection.get("nearest_lane_id")
        if lane_id not in {None, ""}:
            lane_ids.append(str(lane_id))

    sample_count = len(lateral_offsets)
    lateral_p95 = _p95_abs(lateral_offsets)
    heading_p95 = _p95_abs(heading_errors)
    if sample_count <= 0:
        status = "insufficient_data"
        classification = "projection_join_missing"
    elif (lateral_p95 or 0.0) <= PASS_LATERAL_M and (heading_p95 is None or heading_p95 <= PASS_HEADING_RAD):
        status = "pass"
        classification = "planning_first_point_on_hdmap_projection_line_candidate"
    elif (lateral_p95 or 0.0) <= WARN_LATERAL_M and (heading_p95 is None or heading_p95 <= WARN_HEADING_RAD):
        status = "warn"
        classification = "planning_first_point_near_hdmap_projection_line_candidate"
    else:
        status = "warn"
        classification = "planning_first_point_projection_alignment_elevated"
    return {
        "status": status,
        "classification": classification,
        "sample_count": sample_count,
        "planning_rows_with_first_point": len(rows_with_planning),
        "official_projection_rows": len(projection_rows),
        "unmatched_planning_rows": unmatched,
        "join_tolerance_ms": PLANNING_PROJECTION_JOIN_TOLERANCE_S * 1000.0,
        "join_delta_p95_ms": _p95_abs(join_deltas_ms),
        "planning_first_point_lane_l_abs_p95_m": lateral_p95,
        "planning_first_point_lane_heading_error_p95_rad": heading_p95,
        "nearest_lane_id_topk": _topk(lane_ids),
        "claim_boundary": (
            "This is a local time-aligned diagnostic comparing Planning first trajectory "
            "points with official Apollo HDMap projection rows. It does not prove full "
            "reference-line validity, route equivalence, or behavior success."
        ),
    }


def _nearest_projection_row(
    timestamp: float,
    rows: Sequence[Mapping[str, Any]],
    *,
    tolerance_s: float,
) -> Mapping[str, Any] | None:
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    for row in rows:
        row_ts = _projection_timestamp(row)
        if row_ts is None:
            continue
        delta = abs(timestamp - row_ts)
        if delta <= tolerance_s:
            candidates.append((delta, row))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _projection_timestamp(row: Mapping[str, Any]) -> float | None:
    return _num(row.get("sim_time") if row.get("sim_time") is not None else row.get("timestamp"))


def _row_sim_timestamp(row: Mapping[str, Any]) -> float | None:
    return _num(_nested(row, "sim_time_sec") if _nested(row, "sim_time_sec") is not None else _nested(row, "timestamp"))


def _fallback_join_coverage_ratio(rows: Sequence[Mapping[str, Any]]) -> float | None:
    values = [
        _num(_nested(row, "computed.fallback_join_coverage_ratio"))
        for row in rows
        if _nested(row, "computed.fallback_join_coverage_ratio") is not None
    ]
    values.extend(
        _num(_nested(row, "computed.control_reference_join_coverage_ratio"))
        for row in rows
        if _nested(row, "computed.control_reference_join_coverage_ratio") is not None
    )
    finite = [value for value in values if value is not None and math.isfinite(value)]
    if not finite:
        return None
    return sum(finite) / len(finite)


def _fallback_join_dropped_unaligned_rows(rows: Sequence[Mapping[str, Any]]) -> float | None:
    values = [
        _num(_nested(row, "computed.control_reference_dropped_unaligned_rows"))
        for row in rows
        if _nested(row, "computed.control_reference_dropped_unaligned_rows") is not None
    ]
    if values:
        return max(value for value in values if value is not None)
    dropped = 0
    observed = False
    for row in rows:
        missing = _nested(row, "computed.fallback_join_missing_sources")
        if isinstance(missing, list):
            observed = True
            dropped += len(missing)
    return float(dropped) if observed else None


def _tail_nonempty_ratio(
    rows: Sequence[Mapping[str, Any]],
    *,
    predicate,
) -> float | None:
    start_index = None
    for index, row in enumerate(rows):
        if predicate(row):
            start_index = index
            break
    if start_index is None:
        return None
    tail = list(rows[start_index:])
    if not tail:
        return None
    nonempty = sum(1 for row in tail if _planning_trajectory_nonempty(row))
    return nonempty / len(tail)


def _planning_claim_window_ratio(
    *,
    overall_ratio: float | None,
    after_routing_ratio: float | None,
    after_first_nonempty_ratio: float | None,
) -> tuple[float | None, str]:
    if after_routing_ratio is not None:
        return after_routing_ratio, "after_routing_segment_available"
    if after_first_nonempty_ratio is not None:
        return after_first_nonempty_ratio, "after_first_nonempty_trajectory"
    if overall_ratio is not None:
        return overall_ratio, "overall"
    return None, "missing"


def _routing_segment_available(row: Mapping[str, Any]) -> bool:
    return any(
        (value is not None and value > 0.0)
        for value in (
            _num(_nested(row, "planning.routing_segment_count")),
            _num(_nested(row, "routing.routing_segment_count")),
            _num(_nested(row, "routing.routing_road_count")),
        )
    )


def _planning_trajectory_nonempty(row: Mapping[str, Any]) -> bool:
    return (_num(_nested(row, "planning.trajectory_point_count")) or 0.0) > 0.0


def _planning_trajectory_is_path_fallback(row: Mapping[str, Any]) -> bool:
    return str(_nested(row, "planning.trajectory_type") or "").strip().upper() == "PATH_FALLBACK"


def _trajectory_type_ratio(rows: Sequence[Mapping[str, Any]], trajectory_type: str) -> float | None:
    nonempty = [row for row in rows if _planning_trajectory_nonempty(row)]
    if not nonempty:
        return None
    expected = trajectory_type.strip().upper()
    count = sum(
        1
        for row in nonempty
        if str(_nested(row, "planning.trajectory_type") or "").strip().upper() == expected
    )
    return count / len(nonempty)


def _path_fallback_onset(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    previous_nonempty: Mapping[str, Any] | None = None
    for index, row in enumerate(rows):
        if not _planning_trajectory_nonempty(row):
            continue
        if _planning_trajectory_is_path_fallback(row):
            return {
                "row_index": index,
                "timestamp": _nested(row, "timestamp"),
                "sim_time_sec": _nested(row, "sim_time_sec"),
                "planning_sequence_num": _nested(row, "planning.planning_header_sequence_num"),
                "trajectory_point_count": _nested(row, "planning.trajectory_point_count"),
                "first_trajectory_point_theta": _nested(row, "planning.first_trajectory_point_theta"),
                "first_trajectory_point_kappa": _nested(row, "planning.first_trajectory_point_kappa"),
                "heading_error_rad": _nested(row, "computed.localization_to_planning_first_heading_error_rad"),
                "lane_ids": _nested(row, "planning.lane_ids"),
                "target_lane_ids": _nested(row, "planning.target_lane_ids"),
                "previous_trajectory_type": (
                    _nested(previous_nonempty, "planning.trajectory_type")
                    if previous_nonempty is not None
                    else None
                ),
                "previous_heading_error_rad": (
                    _nested(previous_nonempty, "computed.localization_to_planning_first_heading_error_rad")
                    if previous_nonempty is not None
                    else None
                ),
                "previous_first_trajectory_point_kappa": (
                    _nested(previous_nonempty, "planning.first_trajectory_point_kappa")
                    if previous_nonempty is not None
                    else None
                ),
            }
        previous_nonempty = row
    return None


def _lane_ids(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[str]]:
    return {
        "planning_lane_id_first_topk": _topk(row_value for row in rows for row_value in _list_value(_nested(row, "planning.lane_ids"))),
        "target_lane_id_first_topk": _topk(row_value for row in rows for row_value in _list_value(_nested(row, "planning.target_lane_ids"))),
        "routing_unique_lane_signature_topk": _topk(str(_nested(row, "routing.routing_unique_lane_signature") or "") for row in rows),
    }


def _normalized_rows(
    *,
    contract_rows: Sequence[Mapping[str, Any]],
    planning_rows: Sequence[Mapping[str, Any]],
    route_segment_rows: Sequence[Mapping[str, Any]],
    control_rows: Sequence[Mapping[str, Any]],
    timeseries_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if contract_rows:
        rows = _augment_contract_rows_with_planning_debug(contract_rows, planning_rows)
        rows = _augment_contract_rows_with_route_segment_debug(rows, route_segment_rows)
        return _augment_contract_rows_with_control_debug(rows, control_rows)
    return _fallback_asof_join_rows(
        planning_rows=planning_rows,
        route_segment_rows=route_segment_rows,
        control_rows=control_rows,
        timeseries_rows=timeseries_rows,
    )


def _augment_contract_rows_with_planning_debug(
    contract_rows: Sequence[Mapping[str, Any]],
    planning_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in contract_rows]
    if not rows or not planning_rows:
        return rows
    planning_events = [
        build_reference_line_contract_event(row, source_confidence="planning_topic_debug")
        for row in planning_rows
    ]
    planning_events = [
        event
        for event in planning_events
        if _planning_trajectory_nonempty(event) and _sample_points(_nested(event, "planning.trajectory_sample_points"))
    ]
    if not planning_events:
        return rows

    matched_rows = 0
    dropped_rows = 0
    for row in rows:
        row_ts = _event_timestamp(row)
        planning_event: Mapping[str, Any] | None = None
        if row_ts is not None:
            planning_event = _nearest_asof_event(row_ts, planning_events, tolerance_s=FALLBACK_JOIN_TOLERANCE_S)
        row_computed = dict(_mapping(row.get("computed")))
        if planning_event is None:
            dropped_rows += 1
            row_computed["planning_sample_source_confidence"] = "planning_topic_debug_unaligned"
            row_computed["planning_sample_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
            row["computed"] = row_computed
            continue
        matched_rows += 1
        planning_payload = _mapping(planning_event.get("planning"))
        row_planning = dict(_mapping(row.get("planning")))
        for key in (
            "trajectory_sample_points",
            "trajectory_total_path_length",
            "trajectory_first_segment_heading",
            "trajectory_future_first_segment_heading",
            "trajectory_future_lookahead_heading",
            "trajectory_future_lookahead_point_index",
            "trajectory_future_lookahead_distance_m",
            "trajectory_future_lookahead_time_s",
            "trajectory_first_nonexpired_point_theta",
            "trajectory_first_nonexpired_point_relative_time",
            "trajectory_first_nonexpired_point_index",
            "trajectory_first_nonexpired_remaining_point_count",
            "trajectory_expired_prefix_count",
            "trajectory_future_window_points",
        ):
            value = planning_payload.get(key)
            if _present_value(value) and not _present_value(row_planning.get(key)):
                row_planning[key] = value
        presence_payload = _mapping(planning_event.get("planning_debug_presence"))
        if presence_payload and not _mapping(row.get("planning_debug_presence")):
            row["planning_debug_presence"] = presence_payload
        row_computed["planning_sample_source_confidence"] = "planning_topic_debug"
        row_computed["planning_sample_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
        row_computed["planning_sample_join_delta_ms"] = _join_delta_ms(
            row_ts,
            _event_timestamp(planning_event),
        )
        future_heading_delta = _nested(
            planning_event,
            "computed.trajectory_first_nonexpired_theta_vs_future_segment_heading_rad",
        )
        if _present_value(future_heading_delta):
            row_computed["trajectory_first_nonexpired_theta_vs_future_segment_heading_rad"] = future_heading_delta
        future_lookahead_delta = _nested(
            planning_event,
            "computed.trajectory_first_nonexpired_theta_vs_future_lookahead_heading_rad",
        )
        if _present_value(future_lookahead_delta):
            row_computed["trajectory_first_nonexpired_theta_vs_future_lookahead_heading_rad"] = future_lookahead_delta
        row_computed["planning_sample_join_matched"] = True
        row["planning"] = row_planning
        row["computed"] = row_computed
    if rows:
        for row in rows:
            row_computed = dict(_mapping(row.get("computed")))
            row_computed["planning_sample_join_coverage_ratio"] = matched_rows / len(rows)
            row_computed["planning_sample_dropped_unaligned_rows"] = dropped_rows
            row["computed"] = row_computed
    return rows


def _augment_contract_rows_with_route_segment_debug(
    contract_rows: Sequence[Mapping[str, Any]],
    route_segment_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in contract_rows]
    if not rows or not route_segment_rows:
        return rows

    route_events = [
        build_reference_line_contract_event(row, source_confidence="planning_route_segment_debug")
        for row in route_segment_rows
    ]
    route_events = [
        event
        for event in route_events
        if _routing_segment_available(event)
        or _nested(event, "planning.lane_follow_map_status") not in {None, ""}
        or _nested(event, "planning.reference_line_provider_status") not in {None, ""}
    ]
    if not route_events:
        return rows

    matched_rows = 0
    dropped_rows = 0
    for row in rows:
        row_ts = _event_timestamp(row)
        route_event: Mapping[str, Any] | None = None
        if row_ts is not None:
            route_event = _nearest_asof_event(row_ts, route_events, tolerance_s=FALLBACK_JOIN_TOLERANCE_S)
        row_computed = dict(_mapping(row.get("computed")))
        if route_event is None:
            dropped_rows += 1
            row_computed["route_segment_debug_join_matched"] = False
            row_computed["route_segment_debug_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
            row["computed"] = row_computed
            continue
        matched_rows += 1
        route_payload = _mapping(route_event.get("planning"))
        row_planning = dict(_mapping(row.get("planning")))
        for key in (
            "create_route_segments_status",
            "lane_follow_map_status",
            "planning_empty_reason_guess",
            "reference_line_provider_status",
            "reference_line_count",
            "path_end_like_condition",
            "planning_stage_name",
            "task_name",
            "routing_segment_count",
            "route_segment_total_length",
            "lane_ids",
            "target_lane_ids",
        ):
            value = route_payload.get(key)
            if _present_value(value) and not _present_value(row_planning.get(key)):
                row_planning[key] = value
        route_presence = _mapping(route_event.get("planning_debug_presence"))
        if route_presence and not isinstance(row.get("planning_debug_presence"), Mapping):
            row["planning_debug_presence"] = dict(route_presence)
        row_computed["route_segment_debug_join_matched"] = True
        row_computed["route_segment_debug_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
        row_computed["route_segment_debug_join_delta_ms"] = _join_delta_ms(
            row_ts,
            _event_timestamp(route_event),
        )
        row["planning"] = row_planning
        row["computed"] = row_computed
    if rows:
        for row in rows:
            row_computed = dict(_mapping(row.get("computed")))
            row_computed["route_segment_debug_join_coverage_ratio"] = matched_rows / len(rows)
            row_computed["route_segment_debug_dropped_unaligned_rows"] = dropped_rows
            row["computed"] = row_computed
    return rows


def _augment_contract_rows_with_control_debug(
    contract_rows: Sequence[Mapping[str, Any]],
    control_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in contract_rows]
    if not rows or not control_rows:
        return rows
    if all(
        (
            _bool_value(_nested(row, "computed.control_reference_available"))
            or _control_reference_available(row)
        )
        and _control_target_point_available(row)
        for row in rows
    ):
        return rows

    control_events = [
        _control_debug_contract_event(row)
        for row in control_rows
    ]
    control_events = [
        event
        for event in control_events
        if _control_reference_available(event) or _control_target_point_available(event)
    ]
    if not control_events:
        return rows

    matched_rows = 0
    dropped_rows = 0
    for row in rows:
        row_ts = _event_timestamp(row)
        control_event: Mapping[str, Any] | None = None
        if row_ts is not None:
            control_event = _nearest_asof_event(row_ts, control_events, tolerance_s=FALLBACK_JOIN_TOLERANCE_S)
        if control_event is None:
            dropped_rows += 1
            row_computed = dict(_mapping(row.get("computed")))
            row_computed["control_reference_source_confidence"] = "control_decode_debug_unaligned"
            row_computed["control_reference_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
            row["computed"] = row_computed
            continue
        matched_rows += 1
        control_payload = _mapping(control_event.get("control"))
        computed_payload = _mapping(control_event.get("computed"))
        row_control = dict(_mapping(row.get("control")))
        row_computed = dict(_mapping(row.get("computed")))
        for key, value in control_payload.items():
            if value not in {None, ""} and row_control.get(key) in {None, ""}:
                row_control[key] = value
        for key in (
            "localization_to_control_ref_heading_error_rad",
            "localization_to_control_lateral_error_m",
            "control_reference_available",
        ):
            value = computed_payload.get(key)
            if value not in {None, ""} and row_computed.get(key) in {None, ""}:
                row_computed[key] = value
        row_computed["control_reference_source_confidence"] = "control_decode_debug"
        row_computed["control_reference_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
        row_computed["control_reference_join_delta_ms"] = _join_delta_ms(
            row_ts,
            _event_timestamp(control_event),
        )
        row_computed["control_reference_join_matched"] = True
        row["control"] = row_control
        row["computed"] = row_computed
    if rows:
        for row in rows:
            row_computed = dict(_mapping(row.get("computed")))
            row_computed["control_reference_join_coverage_ratio"] = matched_rows / len(rows)
            row_computed["control_reference_dropped_unaligned_rows"] = dropped_rows
            row["computed"] = row_computed
    return rows


def _control_debug_contract_event(row: Mapping[str, Any]) -> dict[str, Any]:
    flat = dict(row)
    nested_control = row.get("apollo_control_raw")
    if isinstance(nested_control, Mapping):
        flat.update(nested_control)
        if "timestamp" not in flat and row.get("ts_sec") not in {None, ""}:
            flat["timestamp"] = row.get("ts_sec")
    return build_reference_line_contract_event(flat, source_confidence="control_decode_debug")


def _fallback_asof_join_rows(
    *,
    planning_rows: Sequence[Mapping[str, Any]],
    route_segment_rows: Sequence[Mapping[str, Any]],
    control_rows: Sequence[Mapping[str, Any]],
    timeseries_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    sources: list[tuple[str, list[Mapping[str, Any]]]] = [
        ("planning", list(planning_rows)),
        ("route_segment", list(route_segment_rows)),
        ("timeseries", list(timeseries_rows)),
        ("control", list(control_rows)),
    ]
    base_name, base_rows = next(((name, rows) for name, rows in sources if rows), ("missing", []))
    rows: list[dict[str, Any]] = []
    if not base_rows:
        return rows
    for base in base_rows:
        flat = dict(base)
        base_ts = _flat_timestamp(base)
        matched_sources = [base_name]
        missing_sources: list[str] = []
        max_delta_ms = 0.0
        for source_name, source_rows in sources:
            if source_name == base_name or not source_rows:
                continue
            matched = None
            if base_ts is not None:
                matched = _nearest_asof_flat_row(base_ts, source_rows, tolerance_s=FALLBACK_JOIN_TOLERANCE_S)
            if matched is None:
                missing_sources.append(source_name)
                continue
            matched_sources.append(source_name)
            delta_ms = abs((_flat_timestamp(matched) or base_ts) - base_ts) * 1000.0
            max_delta_ms = max(max_delta_ms, delta_ms)
            flat.update(matched)
        event = build_reference_line_contract_event(flat, source_confidence="fallback_artifacts_asof_join")
        computed = dict(_mapping(event.get("computed")))
        computed["fallback_join_tolerance_ms"] = FALLBACK_JOIN_TOLERANCE_S * 1000.0
        computed["fallback_join_base_source"] = base_name
        computed["fallback_join_matched_sources"] = matched_sources
        computed["fallback_join_missing_sources"] = missing_sources
        computed["fallback_join_max_delta_ms"] = max_delta_ms if base_ts is not None else None
        computed["fallback_join_coverage_ratio"] = (
            (len(matched_sources) - 1)
            / max(1, sum(1 for name, source_rows in sources if name != base_name and source_rows))
            if base_ts is not None
            else 0.0
        )
        event["computed"] = computed
        rows.append(event)
    return rows


def _nearest_asof_flat_row(
    timestamp: float,
    rows: Sequence[Mapping[str, Any]],
    *,
    tolerance_s: float,
) -> Mapping[str, Any] | None:
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    for row in rows:
        row_ts = _flat_timestamp(row)
        if row_ts is None:
            continue
        delta = timestamp - row_ts
        if 0.0 <= delta <= tolerance_s:
            candidates.append((delta, row))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _nearest_asof_event(
    timestamp: float,
    rows: Sequence[Mapping[str, Any]],
    *,
    tolerance_s: float,
) -> Mapping[str, Any] | None:
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    for row in rows:
        row_ts = _event_timestamp(row)
        if row_ts is None:
            continue
        delta = timestamp - row_ts
        if 0.0 <= delta <= tolerance_s:
            candidates.append((delta, row))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _flat_timestamp(row: Mapping[str, Any]) -> float | None:
    return _num(_first(row, "timestamp", "ts_sec", "sim_time_sec", "sim_time", "planning_header_timestamp_sec", "wall_time_sec"))


def _event_timestamp(row: Mapping[str, Any]) -> float | None:
    return _first_number(row, "timestamp", "sim_time_sec", "wall_time_sec", "planning.planning_header_timestamp_sec")


def _join_delta_ms(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return abs(left - right) * 1000.0


def _normalize_lane_id(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _apply_heading_threshold(value: float | None, *, warnings: list[str], blocking: list[str], warn_reason: str, fail_reason: str) -> None:
    if value is None:
        return
    if abs(value) >= FAIL_HEADING_RAD:
        blocking.append(fail_reason)
    elif abs(value) >= WARN_HEADING_RAD:
        warnings.append(warn_reason)


def _apply_lateral_threshold(value: float | None, *, warnings: list[str], blocking: list[str]) -> None:
    if value is None:
        return
    if abs(value) >= WARN_LATERAL_M:
        warnings.append("control_lateral_error_elevated")


def _resolve_run_sources(root: Path) -> dict[str, Path | None]:
    return {
        "contract_path": _find_first(root, ["artifacts/apollo_reference_line_contract.jsonl", "apollo_reference_line_contract.jsonl"]),
        "planning_topic_debug_path": _find_first(root, ["artifacts/planning_topic_debug.jsonl", "planning_topic_debug.jsonl"]),
        "planning_topic_debug_summary_path": _find_first(
            root,
            [
                "artifacts/planning_topic_debug_summary.json",
                "planning_topic_debug_summary.json",
            ],
        ),
        "planning_route_segment_debug_path": _find_first(root, ["artifacts/planning_route_segment_debug.jsonl", "planning_route_segment_debug.jsonl"]),
        "planning_info_log_path": _find_first(root, ["artifacts/apollo_planning.INFO", "apollo_planning.INFO"]),
        "control_decode_debug_path": _find_control_decode_source(
            root,
            [
                "artifacts/control_decode_debug.jsonl",
                "artifacts/bridge_control_decode.jsonl",
                "artifacts/apollo_control_raw.jsonl",
                "control_decode_debug.jsonl",
                "bridge_control_decode.jsonl",
                "apollo_control_raw.jsonl",
            ],
        ),
        "debug_timeseries_path": _find_first(root, ["artifacts/debug_timeseries.csv", "debug_timeseries.csv", "timeseries.csv"]),
        "localization_contract_path": _find_first(root, ["analysis/localization_contract/localization_contract_report.json", "localization_contract_report.json"]),
        "planning_materialization_path": _find_first(
            root,
            [
                "analysis/apollo_planning_materialization/planning_materialization_report.json",
                "analysis/planning_materialization/planning_materialization_report.json",
                "planning_materialization_report.json",
            ],
        ),
        "hdmap_projection_path": _find_first(root, ["artifacts/apollo_hdmap_projection.jsonl", "analysis/apollo_hdmap_projection/apollo_hdmap_projection.json"]),
        "route_contract_path": _find_first(
            root,
            [
                "analysis/apollo_route_contract/apollo_route_contract_report.json",
                "apollo_route_contract_report.json",
            ],
        ),
    }


def _find_control_decode_source(root: Path, relatives: Sequence[str]) -> Path | None:
    candidates: list[Path] = []
    for relative in relatives:
        path = root / relative
        if path.exists():
            candidates.append(path)
    basenames = {Path(relative).name for relative in relatives}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames and candidate not in candidates:
            candidates.append(candidate)
    for candidate in candidates:
        if _jsonl_has_control_reference(candidate):
            return candidate
    return candidates[0] if candidates else None


def _jsonl_has_control_reference(path: Path) -> bool:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, Mapping):
            continue
        nested_control = payload.get("apollo_control_raw")
        if _flat_row_has_control_reference(payload):
            return True
        if isinstance(nested_control, Mapping) and _flat_row_has_control_reference(nested_control):
            return True
    return False


def _flat_row_has_control_reference(row: Mapping[str, Any]) -> bool:
    return any(
        row.get(key) not in {None, ""}
        for key in (
            "debug_simple_lat_ref_heading",
            "debug_simple_lat_heading_error",
            "debug_simple_lat_current_reference_point_theta",
            "debug_simple_lat_current_target_point_theta",
            "debug_simple_mpc_current_reference_point_theta",
        )
    )


def _read_jsonl(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    file_path = Path(path)
    if not file_path.exists():
        return []
    rows = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _read_csv(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    file_path = Path(path)
    if not file_path.exists():
        return []
    with file_path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_hdmap_projection(path: str | Path | None) -> list[dict[str, Any]]:
    return read_apollo_hdmap_projection(path)


def _path_exists(path: str | Path | None) -> bool:
    return bool(path and Path(path).exists())


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relatives}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _planning_reference_available(row: Mapping[str, Any]) -> bool:
    return any(
        value not in {None, "", 0, 0.0}
        for value in (
            _nested(row, "planning.first_trajectory_point_theta"),
            _nested(row, "planning.reference_line_count"),
            _nested(row, "planning.reference_line_length_max"),
        )
    )


def _control_reference_available(row: Mapping[str, Any]) -> bool:
    return any(
        value not in {None, ""}
        for value in (
            _nested(row, "control.debug_simple_lat_ref_heading"),
            _nested(row, "control.debug_simple_lat_heading_error"),
            _nested(row, "control.debug_simple_lat_current_reference_point_theta"),
        )
    )


def _control_target_point_available(row: Mapping[str, Any]) -> bool:
    return (
        _num(_nested(row, "control.debug_simple_lat_current_target_point_x")) is not None
        and _num(_nested(row, "control.debug_simple_lat_current_target_point_y")) is not None
    )


def _zero_ratio(rows: Sequence[Mapping[str, Any]], *paths: str) -> float | None:
    values = []
    for row in rows:
        value = _first_number(row, *paths)
        if value is not None:
            values.append(value)
    if not values:
        return None
    return sum(1 for value in values if value <= 0.0) / len(values)


def _p95_abs(values: Iterable[float | None]) -> float | None:
    finite = sorted(abs(value) for value in values if value is not None and math.isfinite(float(value)))
    if not finite:
        return None
    if len(finite) == 1:
        return finite[0]
    position = (len(finite) - 1) * 0.95
    lower = int(position)
    upper = min(lower + 1, len(finite) - 1)
    fraction = position - lower
    return finite[lower] * (1.0 - fraction) + finite[upper] * fraction


def _min_number(values: Iterable[float | None]) -> float | None:
    finite = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return min(finite) if finite else None


def _max_number(values: Iterable[float | None]) -> float | None:
    finite = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return max(finite) if finite else None


def _max_abs_number(values: Iterable[float | None]) -> float | None:
    finite = [abs(float(value)) for value in values if value is not None and math.isfinite(float(value))]
    return max(finite) if finite else None


def _ratio_where(values: Iterable[float | None], predicate: Any) -> float | None:
    finite = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not finite:
        return None
    return sum(1 for value in finite if predicate(value)) / len(finite)


def _topk(values: Iterable[str], *, k: int = 5) -> list[str]:
    counter = Counter(value for value in values if str(value).strip())
    return [item for item, _ in counter.most_common(k)]


def _topk_counts(values: Iterable[Any], *, k: int = 5) -> list[dict[str, Any]]:
    cleaned = [str(value) for value in values if value not in {None, ""}]
    counter = Counter(cleaned)
    return [{"value": item, "count": count} for item, count in counter.most_common(k)]


def _extract_debug_curve_second_values(line: str, marker: str) -> list[float]:
    return [value for _, value in _extract_debug_curve_points(line, marker)]


def _extract_debug_curve_points(line: str, marker: str) -> list[tuple[float, float]]:
    if marker not in line:
        return []
    tail = line.split(marker, 1)[1]
    values: list[tuple[float, float]] = []
    for part in tail.split(";"):
        part = part.strip()
        if not part.startswith("(") or "," not in part:
            continue
        part = part.strip("()")
        pieces = part.split(",")
        if len(pieces) < 2:
            continue
        s_value = _num(pieces[0].strip())
        l_value = _num(pieces[1].strip())
        if s_value is not None and l_value is not None:
            values.append((s_value, l_value))
    return values


def _path_bound_evidence_classification(
    print_counts: Mapping[str, int],
    out_of_bounds_margins: Sequence[float],
) -> str:
    if out_of_bounds_margins:
        return "fallback_lateral_state_outside_path_bound"
    if print_counts.get("decide_path_bound_failed", 0) > 0:
        return "path_bound_failure_logged"
    if print_counts.get("path_fallback_due_to_algorithm_failure", 0) > 0:
        return "path_fallback_algorithm_failure_logged"
    return "path_bound_failure_not_observed"


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / float(denominator)


def _first_number(row: Mapping[str, Any], *paths: str) -> float | None:
    for path in paths:
        value = _num(_nested(row, path))
        if value is not None:
            return value
    return None


def _nested(row: Mapping[str, Any], path: str) -> Any:
    value: Any = row
    for part in path.split("."):
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if _present_value(value):
            return value
    return None


def _num(value: Any) -> float | None:
    if value in {None, "", "nan", "NaN"}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _present_value(value: Any) -> bool:
    if value is None or value == "":
        return False
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "pass", "ready", "available"}


def _list_value(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item).strip()]
    if value in {None, ""}:
        return []
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass
    return [item.strip() for item in text.split(",") if item.strip()]


def _sample_points(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        value = parsed
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            continue
        point = {
            "index": item.get("index", index),
            "x": _num(item.get("x")),
            "y": _num(item.get("y")),
            "theta": _num(item.get("theta")),
            "kappa": _num(item.get("kappa")),
            "v": _num(item.get("v")),
            "relative_time": _num(item.get("relative_time")),
        }
        if point["x"] is None or point["y"] is None:
            continue
        out.append(point)
    return out


def _sample_path_length(samples: Sequence[Mapping[str, Any]]) -> float | None:
    total = 0.0
    previous: tuple[float, float] | None = None
    segment_count = 0
    for sample in samples:
        x = _num(sample.get("x"))
        y = _num(sample.get("y"))
        if x is None or y is None:
            previous = None
            continue
        if previous is not None:
            total += math.hypot(x - previous[0], y - previous[1])
            segment_count += 1
        previous = (x, y)
    return total if segment_count > 0 else None


def _point_to_polyline_distance(
    point: tuple[float, float],
    samples: Sequence[Mapping[str, Any]],
) -> float | None:
    previous: tuple[float, float] | None = None
    best: float | None = None
    for sample in samples:
        x = _num(sample.get("x"))
        y = _num(sample.get("y"))
        if x is None or y is None:
            previous = None
            continue
        current = (x, y)
        if previous is not None:
            distance = _point_to_segment_distance(point, previous, current)
            best = distance if best is None else min(best, distance)
        previous = current
    return best


def _point_to_segment_distance(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> float:
    px, py = point
    sx, sy = start
    ex, ey = end
    dx = ex - sx
    dy = ey - sy
    length_sq = dx * dx + dy * dy
    if length_sq <= 0.0:
        return math.hypot(px - sx, py - sy)
    t = ((px - sx) * dx + (py - sy) * dy) / length_sq
    t = max(0.0, min(1.0, t))
    return math.hypot(px - (sx + t * dx), py - (sy + t * dy))


def _sample_first_segment_heading(samples: Sequence[Mapping[str, Any]]) -> float | None:
    first: tuple[float, float] | None = None
    for sample in samples:
        x = _num(sample.get("x"))
        y = _num(sample.get("y"))
        if x is None or y is None:
            continue
        if first is None:
            first = (x, y)
            continue
        dx = x - first[0]
        dy = y - first[1]
        if abs(dx) > 1e-9 or abs(dy) > 1e-9:
            return math.atan2(dy, dx)
    return None


def _angle_delta(lhs: float | None, rhs: float | None) -> float | None:
    if lhs is None or rhs is None:
        return None
    return (float(lhs) - float(rhs) + math.pi) % (2.0 * math.pi) - math.pi


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _report_status(report: Mapping[str, Any] | None) -> str:
    if not isinstance(report, Mapping) or not report:
        return "insufficient_data"
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping) and verdict.get("status"):
        return str(verdict.get("status"))
    if isinstance(verdict, str) and verdict.strip():
        return verdict.strip()
    return str(report.get("status") or "insufficient_data")


def _path_str(path: str | Path | None) -> str | None:
    return str(path) if path is not None else None
