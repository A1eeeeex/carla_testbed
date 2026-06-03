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


def analyze_apollo_reference_line_contract_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    sources = _resolve_run_sources(root)
    return analyze_apollo_reference_line_contract_files(**sources, run_dir=root)


def analyze_apollo_reference_line_contract_files(
    *,
    contract_path: str | Path | None = None,
    planning_topic_debug_path: str | Path | None = None,
    planning_route_segment_debug_path: str | Path | None = None,
    control_decode_debug_path: str | Path | None = None,
    debug_timeseries_path: str | Path | None = None,
    localization_contract_path: str | Path | None = None,
    hdmap_projection_path: str | Path | None = None,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    source = {
        "run_dir": str(run_dir) if run_dir else None,
        "contract_path": _path_str(contract_path),
        "planning_topic_debug_path": _path_str(planning_topic_debug_path),
        "planning_route_segment_debug_path": _path_str(planning_route_segment_debug_path),
        "control_decode_debug_path": _path_str(control_decode_debug_path),
        "debug_timeseries_path": _path_str(debug_timeseries_path),
        "localization_contract_path": _path_str(localization_contract_path),
        "hdmap_projection_path": _path_str(hdmap_projection_path),
    }
    contract_rows = _read_jsonl(contract_path)
    planning_rows = _read_jsonl(planning_topic_debug_path)
    route_segment_rows = _read_jsonl(planning_route_segment_debug_path)
    control_rows = _read_jsonl(control_decode_debug_path)
    timeseries_rows = _read_csv(debug_timeseries_path)
    localization_contract = _read_json(localization_contract_path)
    hdmap_rows = _read_hdmap_projection(hdmap_projection_path)

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
        hdmap_projection_rows=hdmap_rows,
        source=source,
    )


def analyze_apollo_reference_line_contract(
    rows: Sequence[Mapping[str, Any]],
    *,
    localization_contract: Mapping[str, Any] | None = None,
    hdmap_projection_rows: Sequence[Mapping[str, Any]] | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = [dict(row) for row in rows]
    warnings: list[str] = []
    blocking: list[str] = []
    hdmap_projection = summarize_apollo_hdmap_projection(hdmap_projection_rows or [])
    if hdmap_projection["file_present"]:
        warnings.extend(str(item) for item in hdmap_projection.get("warnings", []) if item)
        blocking.extend(str(item) for item in hdmap_projection.get("blocking_reasons", []) if item)
    if not normalized:
        return _report(
            status="fail" if blocking else "insufficient_data",
            blocking_reasons=blocking,
            warnings=warnings + ["reference_line_evidence_missing"],
            rows=[],
            localization_contract=localization_contract,
            hdmap_projection_rows=hdmap_projection_rows,
            source=source,
        )

    evidence = _evidence(normalized, hdmap_projection)
    metrics = _metrics(normalized)
    if not evidence["planning_reference_available"] and not evidence["control_reference_available"]:
        return _report(
            status="fail" if blocking else "insufficient_data",
            blocking_reasons=blocking,
            warnings=warnings + ["planning_control_reference_evidence_missing"],
            rows=normalized,
            localization_contract=localization_contract,
            hdmap_projection_rows=hdmap_projection_rows,
            source=source,
        )
    if not evidence["planning_reference_available"]:
        warnings.append("planning_reference_missing_control_debug_only")
    if not evidence["control_reference_available"]:
        warnings.append("control_reference_missing_planning_debug_only")

    if evidence["nonempty_trajectory_ratio"] is not None and evidence["nonempty_trajectory_ratio"] < MIN_NONEMPTY_TRAJECTORY_RATIO:
        if (metrics["reference_line_count_zero_ratio"] or 0.0) >= 0.50:
            blocking.append("reference_line_count_zero_with_empty_trajectory")
        else:
            warnings.append("nonempty_trajectory_ratio_low")
    ready_ratio = evidence["reference_line_provider_ready_ratio"]
    if ready_ratio is not None and ready_ratio < MIN_PROVIDER_READY_RATIO:
        if not (evidence["control_reference_available"] and (evidence["nonempty_trajectory_ratio"] or 0.0) >= MIN_NONEMPTY_TRAJECTORY_RATIO):
            warnings.append("reference_line_provider_ready_ratio_low")

    _apply_heading_threshold(
        metrics["planning_ref_heading_error_p95_rad"],
        warnings=warnings,
        blocking=blocking,
        warn_reason="planning_reference_heading_error_elevated",
        fail_reason="planning_reference_heading_error_high",
    )
    _apply_heading_threshold(
        metrics["control_ref_heading_error_p95_rad"],
        warnings=warnings,
        blocking=blocking,
        warn_reason="control_reference_heading_error_elevated",
        fail_reason="reference_line_heading_error_high",
    )
    _apply_lateral_threshold(metrics["control_lateral_error_p95_m"], warnings=warnings, blocking=blocking)

    required_metrics_available = (
        metrics["planning_ref_heading_error_p95_rad"] is not None
        or metrics["control_ref_heading_error_p95_rad"] is not None
    )
    if not required_metrics_available:
        return _report(
            status="insufficient_data",
            blocking_reasons=blocking,
            warnings=warnings + ["reference_heading_metrics_missing"],
            rows=normalized,
            localization_contract=localization_contract,
            hdmap_projection_rows=hdmap_projection_rows,
            source=source,
        )
    status = "fail" if blocking else ("warn" if warnings else "pass")
    return _report(
        status=status,
        blocking_reasons=blocking,
        warnings=warnings,
        rows=normalized,
        localization_contract=localization_contract,
        hdmap_projection_rows=hdmap_projection_rows,
        source=source,
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
            "routing_segment_count": _num(_first(row, "routing_segment_count", "route_segment_count")),
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
            "trajectory_first_segment_heading": first_segment_heading,
            "lane_ids": _list_value(_first(row, "lane_ids", "lane_id_first")),
            "target_lane_ids": _list_value(_first(row, "target_lane_ids", "target_lane_id_first")),
            "reference_line_count": _num(_first(row, "reference_line_count")),
            "reference_line_length_max": _num(_first(row, "reference_line_length_max", "reference_line_length")),
            "routing_segment_count": _num(_first(row, "routing_segment_count", "route_segment_count")),
            "lane_follow_map_status": _first(row, "lane_follow_map_status"),
            "reference_line_provider_status": _first(row, "reference_line_provider_status"),
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
    return "\n".join(
        [
            "# Apollo Reference-Line Contract Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            f"- Planning reference available: `{evidence.get('planning_reference_available')}`",
            f"- Control reference available: `{evidence.get('control_reference_available')}`",
            f"- Non-empty trajectory ratio: `{evidence.get('nonempty_trajectory_ratio')}`",
            f"- Reference-line provider ready ratio: `{evidence.get('reference_line_provider_ready_ratio')}`",
            f"- Planning ref heading p95 rad: `{metrics.get('planning_ref_heading_error_p95_rad')}`",
            f"- Control ref heading p95 rad: `{metrics.get('control_ref_heading_error_p95_rad')}`",
            f"- Control lateral error p95 m: `{metrics.get('control_lateral_error_p95_m')}`",
            f"- Apollo HDMap projection status: `{hdmap_projection.get('status')}`",
            f"- Apollo HDMap projection claim-grade: `{hdmap_projection.get('claim_grade')}`",
            f"- Apollo HDMap projection heading p95 rad: `{hdmap_projection.get('heading_error_p95_rad')}`",
            f"- Apollo HDMap projection lateral p95 m: `{hdmap_projection.get('lateral_error_p95_m')}`",
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
    hdmap_projection_rows: Sequence[Mapping[str, Any]] | None,
    source: Mapping[str, Any] | None,
) -> dict[str, Any]:
    hdmap_projection = summarize_apollo_hdmap_projection(hdmap_projection_rows or [])
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "warnings": sorted(set(warnings)),
        "evidence": _evidence(rows, hdmap_projection),
        "metrics": _metrics(rows),
        "lane_ids": _lane_ids(rows),
        "apollo_hdmap_projection": hdmap_projection,
        "localization_contract_status": _report_status(localization_contract),
        "source": dict(source or {}),
        "interpretation_boundary": INTERPRETATION_BOUNDARY,
    }


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
        }
    planning_available = any(_bool_value(_nested(row, "computed.planning_reference_available")) or _planning_reference_available(row) for row in rows)
    control_available = any(_bool_value(_nested(row, "computed.control_reference_available")) or _control_reference_available(row) for row in rows)
    nonempty_count = sum(1 for row in rows if (_num(_nested(row, "planning.trajectory_point_count")) or 0.0) > 0.0)
    ready_count = sum(1 for row in rows if str(_nested(row, "planning.reference_line_provider_status") or "").lower() in {"ready", "pass", "ok"})
    provider_known = sum(1 for row in rows if _nested(row, "planning.reference_line_provider_status") not in {None, ""})
    return {
        "planning_reference_available": planning_available,
        "control_reference_available": control_available,
        "apollo_hdmap_projection_file_present": bool(hdmap_projection.get("file_present")),
        "apollo_hdmap_projection_available": bool(hdmap_projection.get("official_source_available")),
        "apollo_hdmap_projection_claim_grade": bool(hdmap_projection.get("claim_grade")),
        "reference_line_provider_ready_ratio": ready_count / provider_known if provider_known else None,
        "nonempty_trajectory_ratio": nonempty_count / total,
    }


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, float | None]:
    return {
        "planning_ref_heading_error_p95_rad": _p95_abs(
            _first_number(row, "computed.localization_to_planning_first_heading_error_rad")
            for row in rows
        ),
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
    }


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
        return [dict(row) for row in contract_rows]
    rows: list[dict[str, Any]] = []
    max_len = max(len(planning_rows), len(route_segment_rows), len(control_rows), len(timeseries_rows), 0)
    for index in range(max_len):
        flat: dict[str, Any] = {}
        for collection in (planning_rows, route_segment_rows, control_rows, timeseries_rows):
            if index < len(collection):
                flat.update(collection[index])
        if flat:
            rows.append(build_reference_line_contract_event(flat, source_confidence="fallback_artifacts"))
    return rows


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
        "planning_route_segment_debug_path": _find_first(root, ["artifacts/planning_route_segment_debug.jsonl", "planning_route_segment_debug.jsonl"]),
        "control_decode_debug_path": _find_first(root, ["artifacts/control_decode_debug.jsonl", "artifacts/bridge_control_decode.jsonl", "control_decode_debug.jsonl"]),
        "debug_timeseries_path": _find_first(root, ["artifacts/debug_timeseries.csv", "debug_timeseries.csv", "timeseries.csv"]),
        "localization_contract_path": _find_first(root, ["analysis/localization_contract/localization_contract_report.json", "localization_contract_report.json"]),
        "hdmap_projection_path": _find_first(root, ["artifacts/apollo_hdmap_projection.jsonl", "analysis/apollo_hdmap_projection/apollo_hdmap_projection.json"]),
    }


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


def _topk(values: Iterable[str], *, k: int = 5) -> list[str]:
    counter = Counter(value for value in values if str(value).strip())
    return [item for item, _ in counter.most_common(k)]


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
        if value not in {None, ""}:
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
    return str(report.get("status") or "insufficient_data")


def _path_str(path: str | Path | None) -> str | None:
    return str(path) if path is not None else None
