from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

CONTROL_HEALTH_REPORT_SCHEMA_VERSION = "control_health_report.v1"
EXPECTED_HANDOFF_STATUS = "control_consuming_with_nonzero_planning"
DEFAULT_THRESHOLDS = {
    "max_control_latency_p95_ms": 500.0,
    "max_brake_throttle_conflict_frames": 0,
    "max_mapped_applied_abs_error_p95": 0.10,
    "max_control_apply_observation_delay_s": 5.0,
    "min_route_progress_after_applied_control_m": 1.0,
    "min_control_bridge_apply_frame_hz": 10.0,
    "max_control_bridge_same_frame_drop_ratio": 0.80,
    "min_control_bridge_apply_frame_coverage_ratio": 0.95,
    "max_control_bridge_ego_bind_delay_s": 5.0,
    "max_control_bridge_bind_to_first_apply_s": 5.0,
    "max_applied_throttle_brake_switch_count": 10,
    "max_raw_throttle_brake_switch_count": 10,
    "max_mapped_throttle_brake_switch_count": 10,
    "max_vehicle_response_sign_switch_count": 10,
}
CONTROL_TRACE_FIELDS = [
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "throttle_raw",
    "throttle_mapped",
    "throttle_applied",
    "brake_raw",
    "brake_mapped",
    "brake_applied",
]
APPLIED_CONTROL_TRACE_FIELDS = [
    "carla_steer_applied",
    "throttle_applied",
    "brake_applied",
]
SIMPLE_LON_POINT_FIELDS = {
    "debug_simple_lon_current_matched_point_s_m": "debug_simple_lon_current_matched_point_s",
    "debug_simple_lon_current_matched_point_x": "debug_simple_lon_current_matched_point_x",
    "debug_simple_lon_current_matched_point_y": "debug_simple_lon_current_matched_point_y",
    "debug_simple_lon_current_matched_point_theta_rad": "debug_simple_lon_current_matched_point_theta",
    "debug_simple_lon_current_matched_point_kappa": "debug_simple_lon_current_matched_point_kappa",
    "debug_simple_lon_current_reference_point_s_m": "debug_simple_lon_current_reference_point_s",
    "debug_simple_lon_current_reference_point_x": "debug_simple_lon_current_reference_point_x",
    "debug_simple_lon_current_reference_point_y": "debug_simple_lon_current_reference_point_y",
    "debug_simple_lon_current_reference_point_theta_rad": "debug_simple_lon_current_reference_point_theta",
    "debug_simple_lon_current_reference_point_kappa": "debug_simple_lon_current_reference_point_kappa",
    "debug_simple_lon_preview_reference_point_s_m": "debug_simple_lon_preview_reference_point_s",
    "debug_simple_lon_preview_reference_point_x": "debug_simple_lon_preview_reference_point_x",
    "debug_simple_lon_preview_reference_point_y": "debug_simple_lon_preview_reference_point_y",
    "debug_simple_lon_preview_reference_point_theta_rad": "debug_simple_lon_preview_reference_point_theta",
    "debug_simple_lon_preview_reference_point_kappa": "debug_simple_lon_preview_reference_point_kappa",
}
LONGITUDINAL_CONTEXT_FIELDS = (
    "debug_simple_lon_current_speed_mps",
    "debug_simple_lon_speed_reference_mps",
    "debug_simple_lon_speed_error_mps",
    "debug_simple_lon_current_acceleration_mps2",
    "debug_simple_lon_acceleration_reference_mps2",
    "debug_simple_lon_acceleration_error_mps2",
    "debug_simple_lon_acceleration_cmd_mps2",
    "debug_simple_lon_acceleration_cmd_closeloop_mps2",
    "debug_simple_lon_acceleration_lookup_mps2",
    "debug_simple_lon_throttle_cmd_pct",
    "debug_simple_lon_brake_cmd_pct",
    "debug_simple_lon_path_remain_m",
    "debug_simple_lon_station_error_m",
    "debug_simple_lon_preview_station_error_m",
    "debug_simple_lon_is_full_stop",
    *tuple(SIMPLE_LON_POINT_FIELDS),
)


def analyze_control_health_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})

    summary_path = root / "summary.json"
    manifest_path = root / "manifest.json"
    timeseries_path = _find_first(root, ["timeseries.csv", "timeseries.jsonl"])
    control_bridge_log_path = _find_first(
        root,
        [
            "artifacts/cyber_control_bridge.err.log",
            "artifacts/cyber_control_bridge.out.log",
            "cyber_control_bridge.err.log",
            "cyber_control_bridge.out.log",
        ],
    )
    control_decode_debug_path = _find_first(
        root,
        [
            "artifacts/control_decode_debug.jsonl",
            "artifacts/bridge_control_decode.jsonl",
            "control_decode_debug.jsonl",
            "bridge_control_decode.jsonl",
        ],
    )
    control_trajectory_consume_path = _find_first(
        root,
        [
            "artifacts/control_trajectory_consume_debug.jsonl",
            "artifacts/control_trajectory_consume_debug_live.jsonl",
            "control_trajectory_consume_debug.jsonl",
            "control_trajectory_consume_debug_live.jsonl",
        ],
    )
    control_apply_trace_path = _find_first(
        root,
        [
            "artifacts/control_apply_trace.jsonl",
            "control_apply_trace.jsonl",
        ],
    )
    direct_control_apply_path = _find_first(
        root,
        [
            "artifacts/direct_bridge_control_apply.jsonl",
            "direct_bridge_control_apply.jsonl",
        ],
    )
    control_handoff_path = _find_first(
        root,
        [
            "artifacts/control_handoff_summary.json",
            "control_handoff_summary.json",
        ],
    )
    planning_summary_path = _find_first(
        root,
        [
            "artifacts/planning_topic_debug_summary.json",
            "planning_topic_debug_summary.json",
        ],
    )
    planning_topic_debug_path = _find_first(
        root,
        [
            "artifacts/planning_topic_debug.jsonl",
            "planning_topic_debug.jsonl",
        ],
    )
    planning_route_segment_debug_path = _find_first(
        root,
        [
            "artifacts/planning_route_segment_debug.jsonl",
            "artifacts/apollo_route_segment_debug.jsonl",
            "planning_route_segment_debug.jsonl",
            "apollo_route_segment_debug.jsonl",
        ],
    )
    apollo_planning_log_path = _find_first(
        root,
        [
            "artifacts/apollo_planning.INFO",
            "apollo_planning.INFO",
        ],
    )
    cyber_bridge_stats_path = _find_first(
        root,
        [
            "artifacts/cyber_bridge_stats.json",
            "cyber_bridge_stats.json",
        ],
    )
    control_attribution_path = _find_first(
        root,
        [
            "analysis/control_attribution/control_attribution_report.json",
            "control_attribution_report.json",
        ],
    )
    apollo_control_handoff_report_path = _find_first(
        root,
        [
            "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
            "apollo_control_handoff_report.json",
        ],
    )
    localization_contract_path = _find_first(
        root,
        [
            "analysis/localization_contract/localization_contract_report.json",
            "localization_contract_report.json",
        ],
    )
    apollo_reference_line_contract_path = _find_first(
        root,
        [
            "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
            "apollo_reference_line_contract_report.json",
        ],
    )
    routing_response_decoded_report_path = _find_first(
        root,
        [
            "analysis/routing_response_decoded/routing_response_decoded_report.json",
            "routing_response_decoded_report.json",
        ],
    )
    planning_materialization_report_path = _find_first(
        root,
        [
            "analysis/planning_materialization/planning_materialization_report.json",
            "planning_materialization_report.json",
        ],
    )
    summary = _read_json(summary_path)
    manifest = _read_json(manifest_path)
    control_handoff_debug = _read_json(control_handoff_path) if control_handoff_path else {}
    planning_debug = _read_json(planning_summary_path) if planning_summary_path else {}
    cyber_bridge_stats = _read_json(cyber_bridge_stats_path) if cyber_bridge_stats_path else {}
    control_attribution = _read_json(control_attribution_path) if control_attribution_path else {}
    apollo_control_handoff = (
        _read_json(apollo_control_handoff_report_path)
        if apollo_control_handoff_report_path
        else {}
    )
    localization_contract = (
        _read_json(localization_contract_path)
        if localization_contract_path
        else {}
    )
    apollo_reference_line_contract = (
        _read_json(apollo_reference_line_contract_path)
        if apollo_reference_line_contract_path
        else {}
    )
    routing_response_decoded = (
        _read_json(routing_response_decoded_report_path)
        if routing_response_decoded_report_path
        else {}
    )
    planning_materialization = (
        _read_json(planning_materialization_report_path)
        if planning_materialization_report_path
        else {}
    )
    materialization_evidence = _effective_materialization_evidence(
        summary=summary,
        routing_response_decoded=routing_response_decoded,
        routing_response_decoded_path=routing_response_decoded_report_path,
        planning_debug=planning_debug,
        planning_summary_path=planning_summary_path,
        planning_materialization=planning_materialization,
        planning_materialization_path=planning_materialization_report_path,
        cyber_bridge_stats=cyber_bridge_stats,
        cyber_bridge_stats_path=cyber_bridge_stats_path,
        apollo_control_handoff=apollo_control_handoff,
        apollo_control_handoff_path=apollo_control_handoff_report_path,
    )
    effective_summary = dict(summary)
    if materialization_evidence.get("routing_materialized") is not None:
        effective_summary["routing_materialized"] = materialization_evidence.get("routing_materialized")
    if materialization_evidence.get("planning_materialized") is not None:
        effective_summary["planning_materialized"] = materialization_evidence.get("planning_materialized")
    if materialization_evidence.get("control_handoff_status") not in {None, ""}:
        effective_summary["control_handoff_status"] = materialization_evidence.get("control_handoff_status")
    rows = _read_rows(timeseries_path)
    control_bridge_log = _analyze_control_bridge_log(control_bridge_log_path)
    control_row_level_trace_path = control_apply_trace_path or control_decode_debug_path
    control_decode_debug = _analyze_control_decode_debug(
        control_row_level_trace_path,
        trajectory_consume_path=control_trajectory_consume_path,
        planning_topic_debug_path=planning_topic_debug_path,
        planning_route_segment_debug_path=planning_route_segment_debug_path,
        auxiliary_context_path=(
            control_decode_debug_path
            if control_row_level_trace_path == control_apply_trace_path
            else None
        ),
    )
    auxiliary_control_decode_debug = {}
    if (
        control_apply_trace_path is not None
        and control_row_level_trace_path == control_apply_trace_path
        and control_decode_debug_path is not None
        and control_decode_debug_path != control_apply_trace_path
    ):
        auxiliary_control_decode_debug = _analyze_control_decode_debug(
            control_decode_debug_path,
            trajectory_consume_path=control_trajectory_consume_path,
            planning_topic_debug_path=planning_topic_debug_path,
            planning_route_segment_debug_path=planning_route_segment_debug_path,
        )
        control_decode_debug = _merge_auxiliary_control_decode_debug(
            control_decode_debug,
            auxiliary_control_decode_debug,
        )
    direct_control_apply = _analyze_direct_control_apply_log(direct_control_apply_path)
    control_application_timeseries = _control_application_metrics(rows)
    control_apply_delay = _control_apply_delay_metrics(
        control_application=control_application_timeseries,
        direct_control_apply=direct_control_apply,
    )
    control_application = _augment_control_application_with_decode(
        control_application_timeseries,
        control_decode_debug=control_decode_debug,
    )
    steer_error = _paired_abs_error_metrics(
        rows,
        "bridge_steer_mapped",
        "carla_steer_applied",
    )
    throttle_error = _paired_abs_error_metrics(
        rows,
        "throttle_mapped",
        "throttle_applied",
    )
    brake_error = _paired_abs_error_metrics(
        rows,
        "brake_mapped",
        "brake_applied",
    )

    missing_inputs: list[str] = []
    if not summary_path.exists():
        missing_inputs.append("summary")
    if timeseries_path is None:
        missing_inputs.append("timeseries")

    handoff = _control_handoff_status(effective_summary)
    metrics = _summary_metrics(summary)
    summary_latency = _num(metrics.get("control_latency_p95_ms"))
    timeseries_latency = _percentile(_series(rows, "control_latency_ms"), 0.95)
    decode_latency = _num(control_decode_debug.get("control_latency_p95_ms"))
    control_latency_p95_ms = _first_number(summary_latency, timeseries_latency, decode_latency)
    report_metrics = {
        "control_latency_p95_ms": control_latency_p95_ms,
        "control_latency_source": _control_latency_source(
            summary_latency=summary_latency,
            timeseries_latency=timeseries_latency,
            decode_latency=decode_latency,
        ),
        **control_application,
        **control_apply_delay,
        **_route_progress_metrics(rows),
        "brake_throttle_conflict_frames": _brake_throttle_conflicts(rows),
        **_applied_throttle_brake_switch_metrics(rows),
        "mapped_applied_steer_abs_error_p95": steer_error["best_lag_abs_error_p95"],
        "mapped_applied_steer_abs_error_p95_zero_lag": steer_error["zero_lag_abs_error_p95"],
        "mapped_applied_steer_best_lag_frames": steer_error["best_lag_frames"],
        "mapped_applied_steer_best_lag_sample_count": steer_error["best_lag_sample_count"],
        "mapped_applied_throttle_abs_error_p95": throttle_error["best_lag_abs_error_p95"],
        "mapped_applied_throttle_abs_error_p95_zero_lag": throttle_error["zero_lag_abs_error_p95"],
        "mapped_applied_throttle_best_lag_frames": throttle_error["best_lag_frames"],
        "mapped_applied_throttle_best_lag_sample_count": throttle_error["best_lag_sample_count"],
        "mapped_applied_brake_abs_error_p95": brake_error["best_lag_abs_error_p95"],
        "mapped_applied_brake_abs_error_p95_zero_lag": brake_error["zero_lag_abs_error_p95"],
        "mapped_applied_brake_best_lag_frames": brake_error["best_lag_frames"],
        "mapped_applied_brake_best_lag_sample_count": brake_error["best_lag_sample_count"],
        "lateral_guard_apply_count": _bool_count(rows, "lateral_guard_applied"),
        "trajectory_contract_guard_apply_count": _bool_count(
            rows,
            "trajectory_contract_guard_applied",
            "trajectory_contract_lateral_guard_applied",
        ),
        "link_delay_decomposition": _link_delay_decomposition(
            summary=summary,
            control_handoff_debug=control_handoff_debug,
            planning_debug=planning_debug,
            control_bridge_log=control_bridge_log,
            cyber_bridge_stats=cyber_bridge_stats,
        ),
        "gt_state_sampling_cadence": _gt_state_sampling_cadence(
            summary=summary,
            cyber_bridge_stats=cyber_bridge_stats,
            control_decode_debug=control_decode_debug,
        ),
        "control_bridge_log": control_bridge_log,
        "control_decode_debug": control_decode_debug,
        "control_process_health": _control_process_health(apollo_control_handoff),
        "planning_log_fallback_diagnostics": _planning_log_fallback_diagnostics(
            apollo_planning_log_path
        ),
        "direct_control_apply_log": direct_control_apply,
        "control_attribution": _control_attribution_summary(control_attribution, control_attribution_path),
        "upstream_contract_preconditions": _upstream_contract_preconditions(
            localization_contract=localization_contract,
            localization_contract_path=localization_contract_path,
            apollo_reference_line_contract=apollo_reference_line_contract,
            apollo_reference_line_contract_path=apollo_reference_line_contract_path,
        ),
        "materialization_evidence": materialization_evidence,
    }
    report_metrics["oscillation_decomposition"] = _oscillation_decomposition(
        rows=rows,
        control_bridge_log=control_bridge_log,
        control_decode_debug=control_decode_debug,
        thresholds=active_thresholds,
    )
    report_metrics["control_mapping_claim_boundary"] = _control_mapping_claim_boundary(
        rows=rows,
        summary=summary,
        manifest=manifest,
        cyber_bridge_stats=cyber_bridge_stats,
    )
    report_metrics["control_oscillation_diagnosis"] = _control_oscillation_diagnosis(
        report_metrics
    )
    missing_fields = _missing_control_fields(rows)
    external_control_evidence_available = _external_control_evidence_available(
        control_bridge_log=control_bridge_log,
        control_decode_debug=control_decode_debug,
    )
    external_control_source = _external_control_source(control_row_level_trace_path)
    status, failure_reason, verdict_missing, warnings = _verdict(
        summary=effective_summary,
        handoff_status=handoff,
        metrics=report_metrics,
        missing_inputs=missing_inputs,
        missing_fields=missing_fields,
        external_control_evidence_available=external_control_evidence_available,
        thresholds=active_thresholds,
    )
    missing_fields.extend(verdict_missing)
    control_semantics = _control_semantics_summary(report_metrics)
    return {
        "schema_version": CONTROL_HEALTH_REPORT_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=root.name),
        "scenario_class": _first_text(summary, "scenario_class", manifest, "scenario_class"),
        "route_id": _first_text(summary, "route_id", manifest, "route_id"),
        "status": status,
        "failure_reason": failure_reason,
        "runtime_contract_status": _runtime_contract_status(summary, manifest),
        "routing_materialized": _summary_bool(effective_summary, "routing_materialized"),
        "planning_materialized": _summary_bool(effective_summary, "planning_materialized"),
        "control_handoff_status": handoff,
        "apollo_control_handoff_status": apollo_control_handoff.get("verdict"),
        "apollo_control_handoff_failure_stage": apollo_control_handoff.get("failure_stage"),
        "apollo_control_handoff_blocking_reasons": list(
            apollo_control_handoff.get("blocking_reasons") or []
        ),
        "apollo_control_handoff_report_path": (
            str(apollo_control_handoff_report_path)
            if apollo_control_handoff_report_path
            else None
        ),
        "raw_mapped_applied_control_available": not missing_fields or external_control_evidence_available,
        "raw_mapped_applied_control_source": (
            "timeseries"
            if not missing_fields
            else (external_control_source if external_control_evidence_available else None)
        ),
        "control_semantics_primary_factor": control_semantics.get("primary_factor"),
        "control_semantics_suspected_factors": control_semantics.get("suspected_factors", []),
        "control_semantics_evidence": control_semantics,
        "metrics": report_metrics,
        "missing_fields": sorted(set(missing_fields)),
        "missing_inputs": sorted(set(missing_inputs)),
        "warnings": sorted(set(warnings)),
        "thresholds": active_thresholds,
        "source": {
            "run_dir": str(root),
            "summary_path": str(summary_path) if summary_path.exists() else None,
            "manifest_path": str(manifest_path) if manifest_path.exists() else None,
            "timeseries_path": str(timeseries_path) if timeseries_path else None,
            "control_bridge_log_path": str(control_bridge_log_path) if control_bridge_log_path else None,
            "control_decode_debug_path": str(control_decode_debug_path) if control_decode_debug_path else None,
            "auxiliary_control_decode_debug_path": (
                str(control_decode_debug_path)
                if auxiliary_control_decode_debug.get("available") is True
                else None
            ),
            "control_trajectory_consume_path": (
                str(control_trajectory_consume_path) if control_trajectory_consume_path else None
            ),
            "control_apply_trace_path": str(control_apply_trace_path) if control_apply_trace_path else None,
            "control_row_level_trace_path": (
                str(control_row_level_trace_path) if control_row_level_trace_path else None
            ),
            "direct_control_apply_path": str(direct_control_apply_path) if direct_control_apply_path else None,
            "control_handoff_path": str(control_handoff_path) if control_handoff_path else None,
            "planning_summary_path": str(planning_summary_path) if planning_summary_path else None,
            "planning_topic_debug_path": (
                str(planning_topic_debug_path) if planning_topic_debug_path else None
            ),
            "planning_route_segment_debug_path": (
                str(planning_route_segment_debug_path)
                if planning_route_segment_debug_path
                else None
            ),
            "apollo_planning_log_path": (
                str(apollo_planning_log_path) if apollo_planning_log_path else None
            ),
            "cyber_bridge_stats_path": str(cyber_bridge_stats_path) if cyber_bridge_stats_path else None,
            "control_attribution_path": str(control_attribution_path) if control_attribution_path else None,
            "apollo_control_handoff_report_path": (
                str(apollo_control_handoff_report_path)
                if apollo_control_handoff_report_path
                else None
            ),
            "localization_contract_path": (
                str(localization_contract_path) if localization_contract_path else None
            ),
            "apollo_reference_line_contract_path": (
                str(apollo_reference_line_contract_path)
                if apollo_reference_line_contract_path
                else None
            ),
            "routing_response_decoded_report_path": (
                str(routing_response_decoded_report_path)
                if routing_response_decoded_report_path
                else None
            ),
            "planning_materialization_report_path": (
                str(planning_materialization_report_path)
                if planning_materialization_report_path
                else None
            ),
        },
        "interpretation_boundary": (
            "Control-health report checks handoff and raw/mapped/applied command evidence only. "
            "It does not prove Apollo planning quality or full perception/localization."
        ),
    }


def _effective_materialization_evidence(
    *,
    summary: Mapping[str, Any],
    routing_response_decoded: Mapping[str, Any],
    routing_response_decoded_path: Path | None,
    planning_debug: Mapping[str, Any],
    planning_summary_path: Path | None,
    planning_materialization: Mapping[str, Any],
    planning_materialization_path: Path | None,
    cyber_bridge_stats: Mapping[str, Any],
    cyber_bridge_stats_path: Path | None,
    apollo_control_handoff: Mapping[str, Any],
    apollo_control_handoff_path: Path | None,
) -> dict[str, Any]:
    summary_routing = _summary_bool(summary, "routing_materialized")
    summary_planning = _summary_bool(summary, "planning_materialized")
    summary_handoff = _control_handoff_status(summary)

    routing_artifact = _routing_materialized_from_artifacts(
        routing_response_decoded=routing_response_decoded,
        cyber_bridge_stats=cyber_bridge_stats,
    )
    planning_artifact = _planning_materialized_from_artifacts(
        planning_debug=planning_debug,
        planning_materialization=planning_materialization,
    )
    handoff_artifact = _handoff_status_from_artifacts(apollo_control_handoff)

    warnings: list[str] = []
    routing = summary_routing
    routing_source = "summary.routing_materialized" if summary_routing is not None else None
    if routing_artifact is True and summary_routing is not True:
        routing = True
        routing_source = _routing_materialization_source(
            routing_response_decoded_path=routing_response_decoded_path,
            cyber_bridge_stats_path=cyber_bridge_stats_path,
            routing_response_decoded=routing_response_decoded,
            cyber_bridge_stats=cyber_bridge_stats,
        )
        if summary_routing is False:
            warnings.append("summary_routing_materialized_false_overridden_by_artifact")

    planning = summary_planning
    planning_source = "summary.planning_materialized" if summary_planning is not None else None
    if planning_artifact is True and summary_planning is not True:
        planning = True
        planning_source = _planning_materialization_source(
            planning_summary_path=planning_summary_path,
            planning_materialization_path=planning_materialization_path,
            planning_debug=planning_debug,
            planning_materialization=planning_materialization,
        )
        if summary_planning is False:
            warnings.append("summary_planning_materialized_false_overridden_by_artifact")

    handoff = summary_handoff
    handoff_source = "summary.control_handoff_status" if summary_handoff is not None else None
    stale_handoff_values = {None, "", "planning_not_materialized", "planning_missing", "not_materialized"}
    if (
        handoff_artifact == EXPECTED_HANDOFF_STATUS
        and summary_handoff != EXPECTED_HANDOFF_STATUS
        and summary_handoff in stale_handoff_values
    ):
        handoff = EXPECTED_HANDOFF_STATUS
        handoff_source = str(apollo_control_handoff_path) if apollo_control_handoff_path else "apollo_control_handoff_report"
        if summary_handoff not in {None, ""}:
            warnings.append("summary_control_handoff_status_overridden_by_apollo_handoff_report")

    return {
        "routing_materialized": routing,
        "routing_materialized_source": routing_source,
        "routing_materialized_summary_value": summary_routing,
        "routing_materialized_artifact_value": routing_artifact,
        "planning_materialized": planning,
        "planning_materialized_source": planning_source,
        "planning_materialized_summary_value": summary_planning,
        "planning_materialized_artifact_value": planning_artifact,
        "control_handoff_status": handoff,
        "control_handoff_status_source": handoff_source,
        "control_handoff_summary_value": summary_handoff,
        "control_handoff_artifact_value": handoff_artifact,
        "warnings": warnings,
    }


def _routing_materialized_from_artifacts(
    *,
    routing_response_decoded: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> bool | None:
    status = str(routing_response_decoded.get("status") or "").strip().lower()
    lane_count = _num(routing_response_decoded.get("lane_segment_count"))
    if status in {"pass", "warn"} and (lane_count is None or lane_count > 0):
        return True
    routing_success_count = _num(cyber_bridge_stats.get("routing_success_count"))
    if routing_success_count is not None:
        return routing_success_count > 0
    return None


def _planning_materialized_from_artifacts(
    *,
    planning_debug: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
) -> bool | None:
    nonzero = _num(planning_debug.get("messages_with_nonzero_trajectory_points"))
    total = _num(planning_debug.get("total_messages_received"))
    if nonzero is not None:
        return nonzero > 0
    if total is not None and total <= 0:
        return False
    status = str(planning_materialization.get("status") or "").strip().lower()
    if status in {"pass", "warn"}:
        return True
    blocking = planning_materialization.get("blocking_reasons")
    if status == "fail" or blocking:
        return False
    return None


def _handoff_status_from_artifacts(apollo_control_handoff: Mapping[str, Any]) -> str | None:
    if not apollo_control_handoff:
        return None
    verdict = str(
        apollo_control_handoff.get("verdict")
        or apollo_control_handoff.get("status")
        or ""
    ).strip().lower()
    failure_stage = str(apollo_control_handoff.get("failure_stage") or "").strip().lower()
    blocking = [item for item in (apollo_control_handoff.get("blocking_reasons") or []) if item]
    if verdict in {"pass", "warn"} and failure_stage in {"", "none"} and not blocking:
        return EXPECTED_HANDOFF_STATUS
    return None


def _routing_materialization_source(
    *,
    routing_response_decoded_path: Path | None,
    cyber_bridge_stats_path: Path | None,
    routing_response_decoded: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> str | None:
    status = str(routing_response_decoded.get("status") or "").strip().lower()
    if status in {"pass", "warn"} and routing_response_decoded_path is not None:
        return str(routing_response_decoded_path)
    if _num(cyber_bridge_stats.get("routing_success_count")) is not None and cyber_bridge_stats_path is not None:
        return str(cyber_bridge_stats_path)
    return None


def _planning_materialization_source(
    *,
    planning_summary_path: Path | None,
    planning_materialization_path: Path | None,
    planning_debug: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
) -> str | None:
    if _num(planning_debug.get("messages_with_nonzero_trajectory_points")) is not None and planning_summary_path is not None:
        return str(planning_summary_path)
    status = str(planning_materialization.get("status") or "").strip().lower()
    if status in {"pass", "warn"} and planning_materialization_path is not None:
        return str(planning_materialization_path)
    return None


def _control_attribution_summary(
    report: Mapping[str, Any],
    path: Path | None,
) -> dict[str, Any]:
    attribution = report.get("attribution") if isinstance(report.get("attribution"), Mapping) else {}
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    return {
        "available": bool(report),
        "path": str(path) if path else None,
        "status": verdict.get("status"),
        "dominant_breakpoint": attribution.get("dominant_breakpoint"),
        "raw_control_available": report.get("raw_control_available"),
        "mapped_control_available": report.get("mapped_control_available"),
        "applied_control_available": report.get("applied_control_available"),
        "vehicle_response_available": report.get("vehicle_response_available"),
    }


def _upstream_contract_preconditions(
    *,
    localization_contract: Mapping[str, Any],
    localization_contract_path: Path | None,
    apollo_reference_line_contract: Mapping[str, Any],
    apollo_reference_line_contract_path: Path | None,
) -> dict[str, Any]:
    localization = _contract_precondition(
        "localization_contract",
        localization_contract,
        localization_contract_path,
    )
    reference_line = _contract_precondition(
        "apollo_reference_line_contract",
        apollo_reference_line_contract,
        apollo_reference_line_contract_path,
    )
    blocking = [
        *list(localization.get("blocking_reasons") or []),
        *list(reference_line.get("blocking_reasons") or []),
    ]
    eligible = bool(localization.get("non_blocking") and reference_line.get("non_blocking"))
    return {
        "control_oscillation_analysis_eligible": eligible,
        "localization_contract": localization,
        "apollo_reference_line_contract": reference_line,
        "blocking_reasons": sorted(set(str(item) for item in blocking if item)),
        "interpretation": (
            "Applied actuation oscillation is claim-grade only after localization_contract "
            "and apollo_reference_line_contract are pass/warn with no blocking reasons. "
            "Otherwise treat applied oscillation as deferred secondary evidence."
        ),
    }


def _contract_precondition(
    name: str,
    report: Mapping[str, Any],
    path: Path | None,
) -> dict[str, Any]:
    if not report:
        return {
            "status": "insufficient_data",
            "non_blocking": False,
            "blocking_reasons": [f"{name}_missing"],
            "warnings": [],
            "path": str(path) if path else None,
        }
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    status = str(verdict.get("status") or report.get("status") or "insufficient_data")
    blocking = [
        str(item)
        for item in (
            list(verdict.get("blocking_reasons") or [])
            + list(report.get("blocking_reasons") or [])
        )
        if item
    ]
    warnings = [str(item) for item in (report.get("warnings") or []) if item]
    non_blocking = status in {"pass", "warn"} and not blocking
    return {
        "status": status,
        "non_blocking": non_blocking,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "path": str(path) if path else None,
    }


def _oscillation_decomposition(
    *,
    rows: Sequence[Mapping[str, Any]],
    control_bridge_log: Mapping[str, Any],
    control_decode_debug: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    raw = _command_oscillation_layer(
        rows,
        layer="apollo_raw_command",
        throttle_fields=("throttle_raw", "apollo_desired_throttle", "commanded_throttle_raw"),
        brake_fields=("brake_raw", "apollo_desired_brake", "commanded_brake_raw"),
        steer_fields=("apollo_steer_raw", "apollo_desired_steer", "steering_target", "source_steer"),
        max_switch_count=thresholds["max_raw_throttle_brake_switch_count"],
    )
    mapped = _command_oscillation_layer(
        rows,
        layer="bridge_mapped_command",
        throttle_fields=("throttle_mapped", "mapped_throttle_cmd", "commanded_throttle"),
        brake_fields=("brake_mapped", "mapped_brake_cmd", "commanded_brake"),
        steer_fields=("bridge_steer_mapped", "mapped_carla_steer_cmd", "mapped_steer"),
        max_switch_count=thresholds["max_mapped_throttle_brake_switch_count"],
    )
    applied = _command_oscillation_layer(
        rows,
        layer="carla_applied_command",
        throttle_fields=("throttle_applied", "measured_throttle"),
        brake_fields=("brake_applied", "measured_brake"),
        steer_fields=("carla_steer_applied", "measured_steer"),
        max_switch_count=thresholds["max_applied_throttle_brake_switch_count"],
    )
    vehicle = _vehicle_response_oscillation_layer(
        rows,
        max_switch_count=thresholds["max_vehicle_response_sign_switch_count"],
    )
    trace_source_name = _trace_source_name(control_decode_debug)
    raw = _prefer_decode_layer(
        current=raw,
        decoded=control_decode_debug.get("apollo_raw_command_layer"),
        source_name=trace_source_name,
    )
    mapped = _prefer_decode_layer(
        current=mapped,
        decoded=control_decode_debug.get("bridge_mapped_command_layer"),
        source_name=trace_source_name,
    )
    cadence = _bridge_apply_cadence_layer(control_bridge_log, thresholds)
    layers = {
        "apollo_raw_command": raw,
        "bridge_mapped_command": mapped,
        "carla_applied_command": applied,
        "vehicle_response": vehicle,
        "bridge_apply_cadence": cadence,
    }
    priority = [
        "bridge_apply_cadence",
        "apollo_raw_command",
        "bridge_mapped_command",
        "carla_applied_command",
        "vehicle_response",
    ]
    dominant = "insufficient_data"
    for name in priority:
        status = layers[name].get("status")
        if status == "fail":
            dominant = name
            break
    else:
        for name in priority:
            if layers[name].get("status") == "warn":
                dominant = name
                break
        else:
            if any(layer.get("status") == "pass" for layer in layers.values()):
                dominant = "none"
    return {
        "layers": layers,
        "dominant_oscillation_layer": dominant,
        "interpretation": (
            "Oscillation is decomposed into Apollo raw command, bridge mapped command, "
            "CARLA applied command, vehicle response, and bridge apply cadence. Do not "
            "smooth or clamp commands to hide reference-line/localization errors."
        ),
    }


def _trace_source_name(control_decode_debug: Mapping[str, Any]) -> str:
    path = control_decode_debug.get("path")
    if isinstance(path, str) and path:
        return Path(path).name
    return "control_decode_debug.jsonl"


def _control_semantics_summary(metrics: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize raw-control semantic evidence without changing control verdicts."""
    control_debug = metrics.get("control_decode_debug")
    if not isinstance(control_debug, Mapping):
        control_debug = {}
    sources: dict[str, Mapping[str, Any]] = {}
    for name in (
        "steering_mapping_saturation",
        "longitudinal_oscillation_attribution",
        "trajectory_consume_correlation",
        "planning_trajectory_correlation",
    ):
        value = control_debug.get(name)
        if isinstance(value, Mapping):
            sources[name] = value
    planning_log = metrics.get("planning_log_fallback_diagnostics")
    if isinstance(planning_log, Mapping):
        sources["planning_log_fallback_diagnostics"] = planning_log

    suspected: list[str] = []
    dominant_by_source: dict[str, Any] = {}
    availability: dict[str, Any] = {}
    transition_counts: dict[str, Any] = {}
    for name, payload in sources.items():
        availability[name] = payload.get("available")
        if payload.get("transition_count") is not None:
            transition_counts[name] = payload.get("transition_count")
        dominant = payload.get("dominant_suspected_factor")
        if dominant:
            dominant_by_source[name] = dominant
            suspected.append(str(dominant))
        for factor in payload.get("suspected_factors") or []:
            suspected.append(str(factor))

    deduped_suspected = _dedupe_preserve_order(suspected)
    primary_source = None
    primary_factor = None
    for name in (
        "planning_trajectory_correlation",
        "trajectory_consume_correlation",
        "longitudinal_oscillation_attribution",
        "steering_mapping_saturation",
        "planning_log_fallback_diagnostics",
    ):
        factor = dominant_by_source.get(name)
        if factor:
            primary_source = name
            primary_factor = factor
            break
    if primary_factor is None and deduped_suspected:
        primary_factor = deduped_suspected[0]

    return {
        "available": bool(deduped_suspected or dominant_by_source),
        "primary_source": primary_source,
        "primary_factor": primary_factor,
        "suspected_factors": deduped_suspected,
        "dominant_by_source": dominant_by_source,
        "source_availability": availability,
        "transition_counts": transition_counts,
        "interpretation_caveat": (
            "This summarizes Apollo raw-control semantic evidence only. It narrows "
            "the next diagnostic target but does not override localization, HDMap "
            "projection, reference-line, or natural-driving claim gates."
        ),
    }


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _merge_auxiliary_control_decode_debug(
    primary: Mapping[str, Any],
    auxiliary: Mapping[str, Any],
) -> dict[str, Any]:
    out = dict(primary)
    if auxiliary.get("available") is not True:
        return out
    out["auxiliary_source_path"] = auxiliary.get("path")
    out["auxiliary_source_used_for"] = []

    primary_longitudinal = primary.get("longitudinal_debug")
    auxiliary_longitudinal = auxiliary.get("longitudinal_debug")
    if not _report_available(primary_longitudinal) and _report_available(auxiliary_longitudinal):
        out["longitudinal_debug"] = auxiliary_longitudinal
        out["auxiliary_source_used_for"].append("longitudinal_debug")

    primary_attribution = primary.get("longitudinal_oscillation_attribution")
    auxiliary_attribution = auxiliary.get("longitudinal_oscillation_attribution")
    if (
        not _longitudinal_attribution_has_context(primary_attribution)
        and _longitudinal_attribution_has_context(auxiliary_attribution)
    ):
        out["longitudinal_oscillation_attribution"] = auxiliary_attribution
        out["auxiliary_source_used_for"].append("longitudinal_oscillation_attribution")

    primary_steering = primary.get("steering_mapping_saturation")
    auxiliary_steering = auxiliary.get("steering_mapping_saturation")
    if not _report_available(primary_steering) and _report_available(auxiliary_steering):
        out["steering_mapping_saturation"] = auxiliary_steering
        out["auxiliary_source_used_for"].append("steering_mapping_saturation")

    return out


def _report_available(value: Any) -> bool:
    return isinstance(value, Mapping) and value.get("available") is True


def _longitudinal_attribution_has_context(value: Any) -> bool:
    if not isinstance(value, Mapping) or value.get("available") is not True:
        return False
    if value.get("suspected_factors"):
        return True
    examples = value.get("examples")
    if not isinstance(examples, Sequence) or isinstance(examples, (str, bytes)):
        return False
    context_keys = {
        "acceleration_cmd_mps2",
        "brake_cmd_pct",
        "current_speed_mps",
        "path_remain_m",
        "speed_error_mps",
        "speed_reference_mps",
        "station_error_m",
        "throttle_cmd_pct",
    }
    for example in examples:
        if not isinstance(example, Mapping):
            continue
        for key in context_keys:
            values = example.get(key)
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                if any(item is not None for item in values):
                    return True
            elif values is not None:
                return True
    return False


def _external_control_source(path: Path | None) -> str:
    if path is not None and path.name == "control_apply_trace.jsonl":
        return "control_apply_trace_plus_timeseries"
    return "bridge_decode_plus_timeseries"


def _prefer_decode_layer(
    *,
    current: Mapping[str, Any],
    decoded: Any,
    source_name: str,
) -> dict[str, Any]:
    current_layer = dict(current)
    if not isinstance(decoded, Mapping):
        return current_layer
    decoded_layer = dict(decoded)
    decoded_count = _num(decoded_layer.get("sample_count")) or 0.0
    current_count = _num(current_layer.get("sample_count")) or 0.0
    decoded_field_count = _resolved_field_count(decoded_layer)
    current_field_count = _resolved_field_count(current_layer)
    if decoded_count <= 0:
        return current_layer
    if (
        current_layer.get("status") == "insufficient_data"
        or decoded_field_count > current_field_count
        or decoded_count > current_count
    ):
        decoded_layer["source"] = source_name
        return decoded_layer
    current_layer.setdefault("alternate_source", source_name)
    return current_layer


def _resolved_field_count(layer: Mapping[str, Any]) -> int:
    fields = layer.get("resolved_fields")
    if not isinstance(fields, Mapping):
        return 0
    return sum(1 for value in fields.values() if value not in {None, ""})


def _command_oscillation_layer(
    rows: Sequence[Mapping[str, Any]],
    *,
    layer: str,
    throttle_fields: Sequence[str],
    brake_fields: Sequence[str],
    steer_fields: Sequence[str],
    max_switch_count: float,
) -> dict[str, Any]:
    states: list[str] = []
    steer_values: list[float] = []
    conflict_frames = 0
    sample_count = 0
    resolved_fields = {
        "throttle": _first_present_field(rows, throttle_fields),
        "brake": _first_present_field(rows, brake_fields),
        "steer": _first_present_field(rows, steer_fields),
    }
    for row in rows:
        throttle = _first_row_number(row, *throttle_fields)
        brake = _first_row_number(row, *brake_fields)
        steer = _first_row_number(row, *steer_fields)
        if throttle is None and brake is None and steer is None:
            continue
        sample_count += 1
        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        if throttle_f > 0.05 and brake_f > 0.05:
            conflict_frames += 1
            states.append("conflict")
        elif throttle_f > 0.05:
            states.append("throttle")
        elif brake_f > 0.05:
            states.append("brake")
        else:
            states.append("coast")
        if steer is not None:
            steer_values.append(float(steer))
    compact_states = [state for state in states if state in {"throttle", "brake"}]
    switch_count = sum(1 for prev, cur in zip(compact_states, compact_states[1:]) if prev != cur)
    steer_sign_switch_count = _sign_switch_count(steer_values, deadband=0.01)
    if sample_count <= 0:
        status = "insufficient_data"
        reason = "missing_layer_fields"
    elif conflict_frames > 0:
        status = "fail"
        reason = "throttle_brake_conflict"
    elif switch_count > max_switch_count:
        status = "fail"
        reason = "throttle_brake_switching"
    elif steer_sign_switch_count > max_switch_count:
        status = "warn"
        reason = "steer_sign_switching"
    else:
        status = "pass"
        reason = None
    return {
        "layer": layer,
        "status": status,
        "reason": reason,
        "sample_count": sample_count,
        "resolved_fields": resolved_fields,
        "throttle_brake_switch_count": switch_count if sample_count else None,
        "throttle_brake_conflict_frames": conflict_frames if sample_count else None,
        "steer_sign_switch_count": steer_sign_switch_count if steer_values else None,
    }


def _vehicle_response_oscillation_layer(
    rows: Sequence[Mapping[str, Any]],
    *,
    max_switch_count: float,
) -> dict[str, Any]:
    yaw_fields = ("ego_yaw_rate", "ego_yaw_rate_rad_s", "yaw_rate_rps", "measured_yaw_rate_rps")
    accel_fields = (
        "measured_forward_accel_mps2",
        "ego_accel_mps2",
        "linear_acceleration_x",
        "forward_accel_mps2",
    )
    yaw_values = [value for row in rows if (value := _first_row_number(row, *yaw_fields)) is not None]
    accel_values = [value for row in rows if (value := _first_row_number(row, *accel_fields)) is not None]
    yaw_switches = _sign_switch_count(yaw_values, deadband=0.01)
    accel_switches = _sign_switch_count(accel_values, deadband=0.05)
    sample_count = max(len(yaw_values), len(accel_values))
    if sample_count <= 0:
        status = "insufficient_data"
        reason = "missing_vehicle_response_fields"
    elif yaw_switches > max_switch_count or accel_switches > max_switch_count:
        status = "warn"
        reason = "vehicle_response_sign_switching"
    else:
        status = "pass"
        reason = None
    return {
        "layer": "vehicle_response",
        "status": status,
        "reason": reason,
        "sample_count": sample_count,
        "resolved_fields": {
            "yaw_rate": _first_present_field(rows, yaw_fields),
            "forward_accel": _first_present_field(rows, accel_fields),
        },
        "yaw_rate_sign_switch_count": yaw_switches if yaw_values else None,
        "forward_accel_sign_switch_count": accel_switches if accel_values else None,
    }


def _bridge_apply_cadence_layer(
    control_bridge_log: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    if not isinstance(control_bridge_log, Mapping) or control_bridge_log.get("available") is not True:
        return {
            "layer": "bridge_apply_cadence",
            "status": "insufficient_data",
            "reason": "control_bridge_log_missing",
        }
    configured = _num(control_bridge_log.get("configured_apply_hz"))
    observed = _num(control_bridge_log.get("apply_world_frame_hz"))
    drop_ratio = _num(control_bridge_log.get("same_frame_drop_ratio"))
    coverage_ratio = _num(control_bridge_log.get("apply_frame_coverage_ratio"))
    sync_to_world_tick = _parse_bool(control_bridge_log.get("sync_to_world_tick"))
    min_hz = _num(thresholds.get("min_control_bridge_apply_frame_hz"))
    sync_tick_cadence_explained = _sync_tick_cadence_explained(
        sync_to_world_tick=sync_to_world_tick,
        coverage_ratio=coverage_ratio,
        thresholds=thresholds,
    )
    low_cadence = bool(
        configured is not None
        and observed is not None
        and min_hz is not None
        and observed < min_hz
        and not sync_tick_cadence_explained
    )
    low_cadence_unconfigured = bool(
        configured is None
        and observed is not None
        and min_hz is not None
        and observed < min_hz
        and not sync_tick_cadence_explained
    )
    expected_same_frame_drop = sync_tick_cadence_explained
    high_drop = bool(
        drop_ratio is not None
        and drop_ratio > thresholds["max_control_bridge_same_frame_drop_ratio"]
        and not expected_same_frame_drop
    )
    if low_cadence:
        status = "fail"
        reason = "control_bridge_world_frame_cadence_low"
    elif low_cadence_unconfigured:
        status = "warn"
        reason = "control_bridge_world_frame_cadence_low_unconfigured"
    elif high_drop:
        status = "fail"
        reason = "control_bridge_drop_same_frame_high"
    else:
        status = "pass"
        reason = None
    return {
        "layer": "bridge_apply_cadence",
        "status": status,
        "reason": reason,
        "configured_apply_hz": configured,
        "observed_apply_world_frame_hz": observed,
        "sync_to_world_tick": sync_to_world_tick,
        "same_frame_drop_ratio": drop_ratio,
        "apply_frame_coverage_ratio": coverage_ratio,
        "same_frame_drop_expected_from_sync_tick": expected_same_frame_drop,
        "wall_cadence_low_explained_by_sync_tick": sync_tick_cadence_explained
        and observed is not None
        and min_hz is not None
        and observed < min_hz,
    }


def _control_mapping_claim_boundary(
    *,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> dict[str, Any]:
    mode = _first_text(manifest, "actuator_mapping_mode", summary, "actuator_mapping_mode")
    if mode is None:
        mode = _first_present_value(rows, ("actuator_mapping_mode",))
    if mode is None:
        stats_mapping = cyber_bridge_stats.get("actuator_mapping")
        if isinstance(stats_mapping, Mapping):
            mode = _text_or_none(stats_mapping.get("mode"))
    steer_scale = _first_number(
        manifest.get("steer_scale"),
        summary.get("steer_scale"),
        _first_present_number(rows, ("steer_scale",)),
    )
    steering_sign = _first_number(
        manifest.get("steering_sign"),
        summary.get("steering_sign"),
        _first_present_number(rows, ("steering_sign",)),
    )
    calibration_profile_id = _first_text(
        manifest,
        "calibration_profile_id",
        summary,
        "calibration_profile_id",
    )
    stats_mapping = cyber_bridge_stats.get("actuator_mapping")
    calibration_status = None
    if isinstance(stats_mapping, Mapping):
        calibration = stats_mapping.get("calibration")
        if isinstance(calibration, Mapping):
            calibration_status = calibration.get("loaded") or calibration.get("status")
    claim_grade = bool(str(mode or "").lower() == "physical" and calibration_profile_id)
    steering_parameters_source = (
        "calibration_profile" if calibration_profile_id else "runtime_config_or_unknown"
    )
    warnings: list[str] = []
    if str(mode or "").lower() == "legacy":
        warnings.append("legacy_mapping_smoke_only")
    if not calibration_profile_id:
        warnings.append("calibration_profile_missing")
        warnings.append("steering_parameters_not_backed_by_calibration_profile")
    return {
        "actuator_mapping_mode": mode,
        "steer_scale": steer_scale,
        "steering_sign": steering_sign,
        "steering_parameters_source": steering_parameters_source,
        "calibration_profile_id": calibration_profile_id,
        "calibration_loaded_or_status": calibration_status,
        "claim_grade_control_mapping": claim_grade,
        "warnings": warnings,
        "interpretation": (
            "Legacy mapping can support smoke/debug evidence only. Claim-grade actuation "
            "requires physical/calibrated mapping or an explicit vehicle calibration profile."
        ),
    }


def _control_oscillation_diagnosis(metrics: Mapping[str, Any]) -> dict[str, Any]:
    """Roll up raw-control, GT-state cadence, and mapping-boundary evidence.

    This is intentionally diagnostic. It narrows the next inspection target but
    must not be used to bypass upstream localization/reference-line gates.
    """
    semantics = _control_semantics_summary(metrics)
    factors = [str(item) for item in semantics.get("suspected_factors") or []]
    dominant_by_source = semantics.get("dominant_by_source")
    if not isinstance(dominant_by_source, Mapping):
        dominant_by_source = {}

    cadence = metrics.get("gt_state_sampling_cadence")
    cadence = cadence if isinstance(cadence, Mapping) else {}
    cadence_warnings = _gt_state_sampling_warnings(metrics)
    control_to_chassis_ratio = _num(cadence.get("control_to_chassis_count_ratio"))
    control_to_localization_ratio = _num(cadence.get("control_to_localization_count_ratio"))
    gt_state_oversampling_present = (
        "apollo_control_oversamples_gt_state" in cadence_warnings
        or (control_to_chassis_ratio is not None and control_to_chassis_ratio > 5.0)
        or (control_to_localization_ratio is not None and control_to_localization_ratio > 5.0)
    )

    claim_boundary = metrics.get("control_mapping_claim_boundary")
    claim_boundary = claim_boundary if isinstance(claim_boundary, Mapping) else {}
    claim_grade_mapping = bool(claim_boundary.get("claim_grade_control_mapping"))
    mapping_warnings = [str(item) for item in claim_boundary.get("warnings") or []]
    legacy_mapping_smoke_only = "legacy_mapping_smoke_only" in mapping_warnings

    preconditions = metrics.get("upstream_contract_preconditions")
    preconditions = preconditions if isinstance(preconditions, Mapping) else {}
    upstream_nonblocking = bool(preconditions.get("control_oscillation_analysis_eligible"))

    oscillation = metrics.get("oscillation_decomposition")
    oscillation = oscillation if isinstance(oscillation, Mapping) else {}
    layers = oscillation.get("layers")
    layers = layers if isinstance(layers, Mapping) else {}
    layer_statuses = {
        str(name): layer.get("status")
        for name, layer in layers.items()
        if isinstance(layer, Mapping)
    }

    raw_command_oscillation_present = (
        layer_statuses.get("apollo_raw_command") == "fail"
        or "apollo_simple_lon_acceleration_cmd_sign_switching" in factors
    )
    same_trajectory_switching_present = (
        "same_trajectory_longitudinal_control_switching" in factors
        or "intra_trajectory_longitudinal_switches_present" in factors
    )
    planning_sequence_update_present = (
        "planning_sequence_update_correlates_with_switches" in factors
    )
    control_debug = metrics.get("control_decode_debug")
    control_debug = control_debug if isinstance(control_debug, Mapping) else {}
    planning_correlation = control_debug.get("planning_trajectory_correlation")
    planning_correlation = planning_correlation if isinstance(planning_correlation, Mapping) else {}
    transition_window_summary = planning_correlation.get("transition_window_summary")
    transition_window_summary = (
        transition_window_summary
        if isinstance(transition_window_summary, Mapping)
        else {}
    )
    planning_debug_exact_pair_coverage_ratio = _num(
        planning_correlation.get("planning_debug_exact_pair_coverage_ratio")
    )
    planning_debug_missing_transition_ratio = _num(
        planning_correlation.get("planning_debug_missing_transition_ratio")
    )
    planning_debug_sequence_coverage_low = (
        "planning_debug_sequence_coverage_low" in factors
        or (
            planning_debug_exact_pair_coverage_ratio is not None
            and planning_debug_exact_pair_coverage_ratio < 0.5
        )
    )
    matched_reference_jump_present = (
        "matched_or_reference_point_jump" in factors
        or "trajectory_station_or_path_remain_jump" in factors
    )
    steering_mapping_saturation_present = "bridge_steering_normalization_or_scale" in factors

    suspected_layers: list[str] = []
    if raw_command_oscillation_present:
        suspected_layers.append("apollo_raw_control_semantics")
    if planning_debug_sequence_coverage_low:
        suspected_layers.append("planning_debug_sequence_coverage")
    if same_trajectory_switching_present or planning_sequence_update_present:
        suspected_layers.append("planning_control_semantics")
    if matched_reference_jump_present:
        suspected_layers.append("reference_line_or_target_point_semantics")
    if steering_mapping_saturation_present:
        suspected_layers.append("bridge_steering_normalization_or_scale")
    if gt_state_oversampling_present:
        suspected_layers.append("gt_state_sampling_cadence")
    if layer_statuses.get("bridge_apply_cadence") == "fail":
        suspected_layers.append("bridge_apply_cadence")
    if layer_statuses.get("bridge_mapped_command") == "fail":
        suspected_layers.append("bridge_mapping")
    if layer_statuses.get("carla_applied_command") == "fail":
        suspected_layers.append("carla_apply")
    if layer_statuses.get("vehicle_response") == "fail":
        suspected_layers.append("vehicle_response")
    if legacy_mapping_smoke_only or not claim_grade_mapping:
        suspected_layers.append("control_mapping_claim_boundary")
    suspected_layers = _dedupe_preserve_order(suspected_layers)

    primary_suspected_layer = "insufficient_data"
    if (
        raw_command_oscillation_present
        and same_trajectory_switching_present
        and gt_state_oversampling_present
        and not planning_sequence_update_present
    ):
        primary_suspected_layer = "gt_state_sampling_cadence"
    elif raw_command_oscillation_present and planning_debug_sequence_coverage_low:
        primary_suspected_layer = "planning_debug_sequence_coverage"
    elif raw_command_oscillation_present and (
        same_trajectory_switching_present or planning_sequence_update_present
    ):
        primary_suspected_layer = "planning_control_semantics"
    elif raw_command_oscillation_present:
        primary_suspected_layer = "apollo_raw_control_semantics"
    elif steering_mapping_saturation_present:
        primary_suspected_layer = "bridge_steering_normalization_or_scale"
    elif layer_statuses.get("bridge_apply_cadence") == "fail":
        primary_suspected_layer = "bridge_apply_cadence"
    elif layer_statuses.get("bridge_mapped_command") == "fail":
        primary_suspected_layer = "bridge_mapping"
    elif layer_statuses.get("carla_applied_command") == "fail":
        primary_suspected_layer = "carla_apply"
    elif layer_statuses.get("vehicle_response") == "fail":
        primary_suspected_layer = "vehicle_response"
    elif gt_state_oversampling_present:
        primary_suspected_layer = "gt_state_sampling_cadence"
    elif suspected_layers:
        primary_suspected_layer = suspected_layers[0]

    if primary_suspected_layer == "planning_debug_sequence_coverage":
        next_debug_target = (
            "Close planning_topic_debug exact sequence coverage for throttle/brake "
            "transition windows before claiming a Planning/simple_lon root cause; compare "
            "control_input trajectory sequence with observed /apollo/planning debug rows."
        )
    elif primary_suspected_layer == "planning_control_semantics":
        next_debug_target = (
            "Inspect control_trajectory_consume_debug and planning_topic_debug rows around "
            "throttle/brake transitions; confirm whether Apollo simple_lon switches on the "
            "same consumed trajectory before tuning bridge mapping."
        )
    elif primary_suspected_layer == "gt_state_sampling_cadence":
        next_debug_target = (
            "Compare Apollo control loop cadence with GT localization/chassis publish cadence "
            "and CARLA sync tick timing; treat cadence changes as diagnostic until behavior "
            "gates pass."
        )
    elif primary_suspected_layer in {"bridge_mapping", "bridge_apply_cadence", "carla_apply"}:
        next_debug_target = (
            "Inspect raw->mapped->applied trace and bridge apply cadence before changing PID "
            "or actuator mapping parameters."
        )
    elif primary_suspected_layer == "bridge_steering_normalization_or_scale":
        next_debug_target = (
            "Run a diagnostic-only steering normalization A/B after reference-line evidence is "
            "non-blocking; do not change nominal steer_scale or promote the run as claim-grade."
        )
    elif primary_suspected_layer == "apollo_raw_control_semantics":
        next_debug_target = (
            "Inspect Apollo raw ControlCommand debug fields and consumed planning trajectory "
            "near command sign switches; do not smooth mapped output to hide raw switching."
        )
    else:
        next_debug_target = (
            "Collect row-level Apollo control, consumed trajectory, GT state cadence, and "
            "raw/mapped/applied traces."
        )

    return {
        "available": bool(semantics.get("available") or cadence.get("available") or layer_statuses),
        "primary_suspected_layer": primary_suspected_layer,
        "suspected_layers": suspected_layers,
        "raw_command_oscillation_present": raw_command_oscillation_present,
        "same_trajectory_switching_present": same_trajectory_switching_present,
        "planning_sequence_update_correlates_with_switches": planning_sequence_update_present,
        "transition_window_dominant_mode": transition_window_summary.get(
            "dominant_transition_mode"
        ),
        "same_planning_sequence_transition_ratio": transition_window_summary.get(
            "same_planning_sequence_ratio"
        ),
        "planning_sequence_changed_transition_ratio": transition_window_summary.get(
            "planning_sequence_changed_ratio"
        ),
        "planning_debug_sequence_coverage_low": planning_debug_sequence_coverage_low,
        "planning_debug_exact_pair_coverage_ratio": planning_debug_exact_pair_coverage_ratio,
        "planning_debug_missing_transition_ratio": planning_debug_missing_transition_ratio,
        "transition_window_summary_available": bool(
            transition_window_summary.get("available")
        ),
        "matched_or_reference_point_jump_present": matched_reference_jump_present,
        "steering_mapping_saturation_present": steering_mapping_saturation_present,
        "gt_state_oversampling_present": gt_state_oversampling_present,
        "control_input_freshness_root_cause_candidate": bool(
            raw_command_oscillation_present and gt_state_oversampling_present
        ),
        "gt_state_sampling_warnings": cadence_warnings,
        "control_to_chassis_count_ratio": control_to_chassis_ratio,
        "control_to_localization_count_ratio": control_to_localization_ratio,
        "control_rx_wall_hz": _num(cadence.get("control_rx_wall_hz")),
        "chassis_wall_hz": _num(cadence.get("chassis_wall_hz")),
        "localization_wall_hz": _num(cadence.get("localization_wall_hz")),
        "legacy_mapping_smoke_only": legacy_mapping_smoke_only,
        "claim_grade_control_mapping": claim_grade_mapping,
        "upstream_contracts_nonblocking": upstream_nonblocking,
        "dominant_oscillation_layer": oscillation.get("dominant_oscillation_layer"),
        "oscillation_layer_statuses": layer_statuses,
        "control_semantics_primary_source": semantics.get("primary_source"),
        "control_semantics_primary_factor": semantics.get("primary_factor"),
        "control_semantics_dominant_by_source": dict(dominant_by_source),
        "control_semantics_suspected_factors": factors,
        "next_debug_target": next_debug_target,
        "interpretation_caveat": (
            "This diagnosis is evidence for attribution, not a capability verdict. It must not "
            "turn a run into natural-driving pass, and it does not prove Apollo algorithm "
            "limitation unless localization, HDMap projection, reference-line, and control "
            "handoff evidence are non-blocking."
        ),
    }


def _first_present_field(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str | None:
    for field in fields:
        if any(row.get(field) not in {None, ""} for row in rows):
            return field
    return None


def _first_present_value(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str | None:
    for field in fields:
        for row in rows:
            value = row.get(field)
            if value not in {None, ""}:
                return str(value)
    return None


def _first_present_number(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> float | None:
    for field in fields:
        for row in rows:
            value = _num(row.get(field))
            if value is not None:
                return value
    return None


def _sign_switch_count(values: Sequence[float], *, deadband: float) -> int:
    signs: list[int] = []
    for value in values:
        number = float(value)
        if abs(number) <= deadband:
            continue
        signs.append(1 if number > 0.0 else -1)
    return sum(1 for prev, cur in zip(signs, signs[1:]) if prev != cur)


def write_control_health_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "control_health_report.json"
    md_path = output_dir / "control_health_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "control_health_report": str(json_path),
        "control_health_summary": str(md_path),
    }


def _control_process_health(apollo_control_handoff: Mapping[str, Any]) -> dict[str, Any]:
    if not apollo_control_handoff:
        return {"status": "insufficient_data", "available": False}
    failure_stage = str(apollo_control_handoff.get("failure_stage") or "").strip()
    process_health = apollo_control_handoff.get("process_health")
    process_map = process_health if isinstance(process_health, Mapping) else {}
    crash_reason = str(
        process_map.get("crash_reason")
        or process_map.get("fatal_signal")
        or process_map.get("failure_reason")
        or ""
    ).strip()
    crash_detected = (
        _parse_bool(process_map.get("crash_detected")) is True
        or bool(crash_reason)
        or "crash" in failure_stage
    )
    if failure_stage == "process_health" and crash_detected:
        return {
            "status": "fail",
            "available": True,
            "failure_reason": "control_process_crash_before_control_output",
            "failure_stage": failure_stage,
            "crash_detected": True,
            "crash_reason": crash_reason or None,
        }
    if failure_stage == "process_health":
        return {
            "status": "fail",
            "available": True,
            "failure_reason": "control_process_failed_before_control_output",
            "failure_stage": failure_stage,
            "crash_detected": crash_detected,
            "crash_reason": crash_reason or None,
        }
    return {
        "status": "pass",
        "available": True,
        "failure_stage": failure_stage or None,
        "crash_detected": crash_detected,
        "crash_reason": crash_reason or None,
    }


def _verdict(
    *,
    summary: Mapping[str, Any],
    handoff_status: str | None,
    metrics: Mapping[str, Any],
    missing_inputs: Sequence[str],
    missing_fields: Sequence[str],
    external_control_evidence_available: bool,
    thresholds: Mapping[str, float],
) -> tuple[str, str | None, list[str], list[str]]:
    verdict_missing: list[str] = []
    warnings: list[str] = []
    control_decode = metrics.get("control_decode_debug")
    if (
        missing_fields
        and isinstance(control_decode, Mapping)
        and control_decode.get("command_payload_available") is False
    ):
        warnings.append("control_row_level_trace_no_command_payload")
    if missing_inputs:
        return "insufficient_data", "missing_control_inputs", verdict_missing, warnings
    runtime_status = _runtime_contract_status(summary, {})
    if runtime_status != "aligned":
        verdict_missing.append("runtime_contract.status")
        if runtime_status:
            return "fail", "runtime_contract_not_aligned", verdict_missing, warnings
        return "insufficient_data", "runtime_contract_missing_status", verdict_missing, warnings
    routing = _summary_bool(summary, "routing_materialized")
    planning = _summary_bool(summary, "planning_materialized")
    if routing is None:
        verdict_missing.append("routing_materialized")
    if planning is None:
        verdict_missing.append("planning_materialized")
    if handoff_status is None:
        verdict_missing.append("control_handoff_status")
    if verdict_missing:
        return "insufficient_data", "missing_link_health_fields", verdict_missing, warnings
    if routing is False:
        return "fail", "routing_missing", verdict_missing, warnings
    if planning is False:
        return "fail", "planning_missing", verdict_missing, warnings
    process_health = metrics.get("control_process_health")
    if isinstance(process_health, Mapping) and process_health.get("status") == "fail":
        reason = str(process_health.get("failure_reason") or "control_process_failed")
        return "fail", reason, verdict_missing, warnings
    if handoff_status != EXPECTED_HANDOFF_STATUS:
        return "fail", "control_handoff_not_consuming", verdict_missing, warnings
    if missing_fields:
        missing_applied_fields = [field for field in missing_fields if field in APPLIED_CONTROL_TRACE_FIELDS]
        if external_control_evidence_available and not missing_applied_fields:
            warnings.append("external_control_trace_from_bridge_artifacts")
        else:
            return "insufficient_data", "missing_control_trace_fields", verdict_missing, warnings

    if missing_fields and not external_control_evidence_available:
        return "insufficient_data", "missing_control_trace_fields", verdict_missing, warnings

    mapped_frames = _num(metrics.get("nonzero_mapped_control_frames"))
    applied_frames = _num(metrics.get("nonzero_applied_control_frames"))
    if mapped_frames is None:
        verdict_missing.append("nonzero_mapped_control_frames")
    if applied_frames is None:
        verdict_missing.append("nonzero_applied_control_frames")
    if verdict_missing:
        return "insufficient_data", "missing_control_metrics", verdict_missing, warnings
    if mapped_frames > 0 and applied_frames <= 0:
        return "fail", "control_apply_missing", verdict_missing, warnings
    apply_delay = _num(metrics.get("control_apply_observation_delay_s"))
    if (
        apply_delay is not None
        and apply_delay > thresholds["max_control_apply_observation_delay_s"]
    ):
        warnings.append("control_apply_observation_delay_high")
    warnings.extend(_control_bridge_log_warnings(metrics, thresholds))
    warnings.extend(_gt_state_sampling_warnings(metrics))
    materialization = metrics.get("materialization_evidence")
    if isinstance(materialization, Mapping):
        warnings.extend(str(item) for item in (materialization.get("warnings") or []) if item)

    progress_after_apply = _num(metrics.get("route_s_after_first_applied_control_delta_m"))
    if (
        applied_frames is not None
        and applied_frames > 0
        and progress_after_apply is not None
        and progress_after_apply < thresholds["min_route_progress_after_applied_control_m"]
    ):
        warnings.append("route_progress_stalled_after_control")
    oscillation = metrics.get("oscillation_decomposition")
    oscillation_layers = oscillation.get("layers") if isinstance(oscillation, Mapping) else {}
    if isinstance(oscillation_layers, Mapping):
        cadence_layer = oscillation_layers.get("bridge_apply_cadence")
        raw_layer = oscillation_layers.get("apollo_raw_command")
        mapped_layer = oscillation_layers.get("bridge_mapped_command")
        if isinstance(cadence_layer, Mapping) and cadence_layer.get("status") == "fail":
            reason = str(cadence_layer.get("reason") or "control_bridge_apply_cadence_failed")
            return "fail", reason, verdict_missing, warnings
        if isinstance(raw_layer, Mapping) and raw_layer.get("status") == "fail":
            return "fail", "apollo_raw_command_oscillation", verdict_missing, warnings
        if isinstance(mapped_layer, Mapping) and mapped_layer.get("status") == "fail":
            return "fail", "bridge_mapped_command_oscillation", verdict_missing, warnings

    conflict_frames = _num(metrics.get("brake_throttle_conflict_frames"))
    if conflict_frames is not None and conflict_frames > thresholds["max_brake_throttle_conflict_frames"]:
        return "fail", "brake_throttle_conflict", verdict_missing, warnings
    switch_count = _num(metrics.get("applied_throttle_brake_switch_count"))
    if (
        switch_count is not None
        and switch_count > thresholds["max_applied_throttle_brake_switch_count"]
    ):
        if "control_bridge_world_frame_cadence_low" in warnings:
            return "fail", "control_bridge_world_frame_cadence_low", verdict_missing, warnings
        if "control_bridge_drop_same_frame_high" in warnings:
            return "fail", "control_bridge_drop_same_frame_high", verdict_missing, warnings
        upstream_preconditions = metrics.get("upstream_contract_preconditions")
        if not _control_oscillation_analysis_eligible(upstream_preconditions):
            warnings.append("applied_actuation_oscillation_deferred_until_upstream_contracts_nonblocking")
            return "warn", "control_health_warn", verdict_missing, warnings
        return "fail", "applied_actuation_oscillation", verdict_missing, warnings
    for field in (
        "mapped_applied_steer_abs_error_p95",
        "mapped_applied_throttle_abs_error_p95",
        "mapped_applied_brake_abs_error_p95",
    ):
        value = _num(metrics.get(field))
        if value is None:
            if external_control_evidence_available and missing_fields:
                axis = field.replace("mapped_applied_", "").replace("_abs_error_p95", "")
                warnings.append(f"mapped_applied_{axis}_pairing_unavailable_without_p0_raw_mapped")
            else:
                verdict_missing.append(field)
        elif value > thresholds["max_mapped_applied_abs_error_p95"]:
            axis = field.replace("mapped_applied_", "").replace("_abs_error_p95", "")
            warnings.append(f"mapped_applied_{axis}_mismatch")
    latency = _num(metrics.get("control_latency_p95_ms"))
    if latency is None:
        warnings.append("control_latency_missing")
    elif latency > thresholds["max_control_latency_p95_ms"]:
        warnings.append("control_latency_high")
    if verdict_missing:
        return "insufficient_data", "missing_control_metrics", verdict_missing, warnings
    if warnings:
        return "warn", "control_health_warn", verdict_missing, warnings
    return "pass", None, verdict_missing, warnings


def _control_oscillation_analysis_eligible(preconditions: Any) -> bool:
    if not isinstance(preconditions, Mapping):
        return False
    return _parse_bool(preconditions.get("control_oscillation_analysis_eligible")) is True


def _missing_control_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return list(CONTROL_TRACE_FIELDS)
    missing: list[str] = []
    for field in CONTROL_TRACE_FIELDS:
        if not any(row.get(field) not in {None, ""} for row in rows):
            missing.append(field)
    return missing


def _external_control_evidence_available(
    *,
    control_bridge_log: Mapping[str, Any],
    control_decode_debug: Mapping[str, Any],
) -> bool:
    """Return true when external-stack control can be diagnosed outside P0.

    For Apollo/Autoware runs the harness does not own actuation, so P0
    raw/mapped fields may intentionally be null. In that case bridge decode
    artifacts plus CARLA apply logs are the authoritative external control
    evidence; they are weaker than per-frame P0 pairing but better than
    treating the run as blind.
    """

    decode_available = bool(control_decode_debug.get("available") is True)
    if control_decode_debug.get("command_payload_available") is False:
        return False
    decode_rows = _num(control_decode_debug.get("parsed_line_count"))
    if decode_rows is None:
        decode_rows = _num(control_decode_debug.get("line_count"))
    trace_path = str(control_decode_debug.get("path") or "")
    if Path(trace_path).name == "control_apply_trace.jsonl":
        applied_rows = _num(control_decode_debug.get("applied_payload_row_count"))
        return bool(decode_available and (decode_rows or 0) > 0 and (applied_rows or 0) > 0)
    bridge_available = bool(control_bridge_log.get("available") is True)
    applied_count = _num(control_bridge_log.get("final_applied_count"))
    return bool(decode_available and (decode_rows or 0) > 0 and bridge_available and (applied_count or 0) > 0)


def _brake_throttle_conflicts(rows: Sequence[Mapping[str, Any]]) -> int | None:
    seen = False
    conflicts = 0
    for row in rows:
        throttle = _num(row.get("throttle_applied"))
        brake = _num(row.get("brake_applied"))
        if throttle is None or brake is None:
            continue
        seen = True
        if throttle > 0.05 and brake > 0.05:
            conflicts += 1
    return conflicts if seen else None


def _applied_throttle_brake_switch_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    states: list[str] = []
    for row in rows:
        throttle = _num(row.get("throttle_applied"))
        brake = _num(row.get("brake_applied"))
        if throttle is None and brake is None:
            continue
        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        if throttle_f > 0.05 and brake_f > 0.05:
            states.append("conflict")
        elif throttle_f > 0.05:
            states.append("throttle")
        elif brake_f > 0.05:
            states.append("brake")
        else:
            states.append("coast")
    compact_states = [state for state in states if state in {"throttle", "brake"}]
    switch_count = sum(1 for prev, cur in zip(compact_states, compact_states[1:]) if prev != cur)
    return {
        "applied_throttle_brake_switch_count": switch_count if states else None,
        "applied_throttle_frames": states.count("throttle"),
        "applied_brake_frames": states.count("brake"),
        "applied_throttle_brake_conflict_frames": states.count("conflict"),
    }


def _control_application_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    mapped_frames = 0
    applied_frames = 0
    first_mapped_time: float | None = None
    first_applied_time: float | None = None
    seen = False
    for row in rows:
        mapped_active = _row_has_nonzero_control(
            row,
            throttle_field="throttle_mapped",
            brake_field="brake_mapped",
            steer_field="bridge_steer_mapped",
        )
        applied_active = _row_has_nonzero_control(
            row,
            throttle_field="throttle_applied",
            brake_field="brake_applied",
            steer_field="carla_steer_applied",
        )
        if mapped_active is None and applied_active is None:
            continue
        seen = True
        row_time = _row_time_s(row)
        if mapped_active is True:
            mapped_frames += 1
            if first_mapped_time is None:
                first_mapped_time = row_time
        if applied_active is True:
            applied_frames += 1
            if first_applied_time is None:
                first_applied_time = row_time
    delay = None
    if first_mapped_time is not None and first_applied_time is not None:
        delay = first_applied_time - first_mapped_time
    return {
        "nonzero_mapped_control_frames": mapped_frames if seen else None,
        "nonzero_applied_control_frames": applied_frames if seen else None,
        "first_nonzero_mapped_control_s": first_mapped_time,
        "first_nonzero_applied_control_s": first_applied_time,
        "control_apply_observation_delay_s": delay,
        "control_apply_observation_delay_source": "timeseries_applied_control",
    }


def _link_delay_decomposition(
    *,
    summary: Mapping[str, Any],
    control_handoff_debug: Mapping[str, Any],
    planning_debug: Mapping[str, Any],
    control_bridge_log: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> dict[str, Any]:
    command_materialization = cyber_bridge_stats.get("command_materialization")
    command_gate = (
        command_materialization.get("gate_state")
        if isinstance(command_materialization, Mapping)
        else None
    )
    if not isinstance(command_gate, Mapping):
        command_gate = {}
    bridge_bind_s = _num(control_bridge_log.get("ego_bind_wall_s"))
    bridge_first_apply_s = _num(control_bridge_log.get("first_apply_wall_s"))
    routing_first_request_s = _first_number(
        summary.get("routing_first_request_at"),
        planning_debug.get("routing_first_response_after_last_routing_send_boundary_ts_sec"),
        planning_debug.get("routing_first_response_ts_sec"),
    )
    planning_first_message_s = _first_number(
        summary.get("planning_first_message_at"),
        planning_debug.get("first_msg_ts_sec"),
    )
    planning_first_nonempty_s = _first_number(
        summary.get("planning_first_nonempty_at"),
        control_handoff_debug.get("planning_first_nonempty_at"),
        planning_debug.get("first_nonzero_trajectory_timestamp"),
    )
    control_first_consume_s = _first_number(
        summary.get("control_first_consume_at"),
        control_handoff_debug.get("control_first_consume_at"),
    )
    command_first_eval_s = _num(command_gate.get("first_eval_ts_sec"))
    command_first_eligible_s = _num(command_gate.get("first_eligible_ts_sec"))
    command_first_ready_s = _num(command_gate.get("first_ready_to_send_ts_sec"))
    segments = {
        "bridge_bind_to_routing_first_request_s": _positive_span(bridge_bind_s, routing_first_request_s),
        "command_gate_startup_delay_observed_s": _positive_span(
            command_first_eval_s,
            command_first_eligible_s,
        ),
        "command_gate_apollo_warmup_delay_observed_s": _positive_span(
            command_first_eligible_s,
            command_first_ready_s,
        ),
        "command_gate_ready_to_routing_first_request_s": _positive_span(
            command_first_ready_s,
            routing_first_request_s,
        ),
        "routing_first_request_to_planning_first_message_s": _positive_span(
            routing_first_request_s,
            planning_first_message_s,
        ),
        "routing_first_request_to_planning_nonempty_s": _positive_span(
            routing_first_request_s,
            planning_first_nonempty_s,
        ),
        "planning_nonempty_to_control_first_consume_s": _positive_span(
            planning_first_nonempty_s,
            control_first_consume_s,
        ),
        "control_first_consume_to_bridge_first_apply_s": _positive_span(
            control_first_consume_s,
            bridge_first_apply_s,
        ),
        "bridge_bind_to_first_apply_s": _positive_span(bridge_bind_s, bridge_first_apply_s),
    }
    stage_candidates = {
        key: value
        for key, value in segments.items()
        if value is not None
        and key
        not in {
            "bridge_bind_to_first_apply_s",
            "routing_first_request_to_planning_first_message_s",
        }
    }
    primary_stage = None
    if stage_candidates:
        primary_stage = max(stage_candidates.items(), key=lambda item: item[1])[0]
    return {
        "bridge_ego_bind_wall_s": bridge_bind_s,
        "routing_first_request_at_s": routing_first_request_s,
        "planning_first_message_at_s": planning_first_message_s,
        "planning_first_nonempty_at_s": planning_first_nonempty_s,
        "control_first_consume_at_s": control_first_consume_s,
        "bridge_first_apply_wall_s": bridge_first_apply_s,
        "command_gate_first_eval_ts_sec": command_first_eval_s,
        "command_gate_first_eligible_ts_sec": command_first_eligible_s,
        "command_gate_first_ready_to_send_ts_sec": command_first_ready_s,
        "command_gate_first_blocking_reason": _text_or_none(command_gate.get("first_blocking_reason")),
        "command_gate_last_blocking_reason": _text_or_none(command_gate.get("last_blocking_reason")),
        "command_gate_last_error_snapshot": _text_or_none(command_gate.get("last_error_snapshot")),
        **segments,
        "primary_delay_stage": primary_stage,
    }


def _gt_state_sampling_cadence(
    *,
    summary: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
    control_decode_debug: Mapping[str, Any],
) -> dict[str, Any]:
    elapsed = _num(cyber_bridge_stats.get("publish_elapsed_wall_sec"))
    loc_count = _num(cyber_bridge_stats.get("loc_count"))
    chassis_count = _num(cyber_bridge_stats.get("chassis_count"))
    control_rx_count = _num(cyber_bridge_stats.get("control_rx_count"))
    control_tx_count = _num(cyber_bridge_stats.get("control_tx_count"))
    tick_health = summary.get("carla_tick_health")
    tick_health = tick_health if isinstance(tick_health, Mapping) else {}
    tick_count = _num(tick_health.get("tick_count"))
    tick_wall_span = _span(
        _num(tick_health.get("first_tick_wall_time_s")),
        _num(tick_health.get("last_tick_wall_time_s")),
    )
    longitudinal = control_decode_debug.get("longitudinal_debug")
    longitudinal = longitudinal if isinstance(longitudinal, Mapping) else {}
    current_accel_abs_p95 = _nested(
        longitudinal,
        "stats.debug_simple_lon_current_acceleration_mps2.p95_abs",
    )
    speed_fd_accel_abs_p95 = _nested(
        longitudinal,
        "speed_finite_difference_acceleration_abs_p95_mps2",
    )
    loc_hz = _safe_div(loc_count, elapsed)
    chassis_hz = _safe_div(chassis_count, elapsed)
    control_rx_hz = _safe_div(control_rx_count, elapsed)
    return {
        "available": bool(elapsed and (loc_count is not None or chassis_count is not None)),
        "publish_elapsed_wall_sec": elapsed,
        "localization_publish_count": int(loc_count) if loc_count is not None else None,
        "chassis_publish_count": int(chassis_count) if chassis_count is not None else None,
        "control_rx_count": int(control_rx_count) if control_rx_count is not None else None,
        "control_tx_count": int(control_tx_count) if control_tx_count is not None else None,
        "localization_wall_hz": loc_hz,
        "chassis_wall_hz": chassis_hz,
        "control_rx_wall_hz": control_rx_hz,
        "control_to_localization_count_ratio": _safe_div(control_rx_count, loc_count),
        "control_to_chassis_count_ratio": _safe_div(control_rx_count, chassis_count),
        "carla_tick_count": int(tick_count) if tick_count is not None else None,
        "carla_tick_wall_span_s": tick_wall_span,
        "carla_tick_wall_hz": _safe_div(tick_count, tick_wall_span),
        "carla_inter_tick_wall_interval_p95_s": _num(
            tick_health.get("inter_tick_wall_interval_p95_s")
        ),
        "current_acceleration_abs_p95_mps2": current_accel_abs_p95,
        "speed_fd_acceleration_abs_p95_mps2": speed_fd_accel_abs_p95,
        "interpretation": (
            "Apollo control can run many wall-clock cycles per CARLA world tick. If GT "
            "localization/chassis arrives slowly in wall time while control keeps cycling, "
            "Apollo longitudinal debug may show stale-state oversampling and acceleration spikes."
        ),
    }


def _gt_state_sampling_warnings(metrics: Mapping[str, Any]) -> list[str]:
    cadence = metrics.get("gt_state_sampling_cadence")
    if not isinstance(cadence, Mapping) or cadence.get("available") is not True:
        return []
    warnings: list[str] = []
    ratio = _num(cadence.get("control_to_chassis_count_ratio"))
    control_hz = _num(cadence.get("control_rx_wall_hz"))
    chassis_hz = _num(cadence.get("chassis_wall_hz"))
    accel_abs_p95 = _num(cadence.get("current_acceleration_abs_p95_mps2"))
    if ratio is not None and ratio > 5.0:
        warnings.append("apollo_control_oversamples_gt_state")
    if (
        control_hz is not None
        and chassis_hz is not None
        and control_hz >= 10.0
        and chassis_hz < 10.0
    ):
        warnings.append("gt_state_wall_rate_low_relative_to_control")
    if accel_abs_p95 is not None and accel_abs_p95 > 5.0:
        warnings.append("apollo_control_current_acceleration_spiky")
    return warnings


def _positive_span(start: float | None, end: float | None) -> float | None:
    value = _span(start, end)
    if value is None:
        return None
    return max(0.0, value)


def _control_apply_delay_metrics(
    *,
    control_application: Mapping[str, Any],
    direct_control_apply: Mapping[str, Any],
) -> dict[str, Any]:
    timeseries_delay = _num(control_application.get("control_apply_observation_delay_s"))
    first_mapped = _num(control_application.get("first_nonzero_mapped_control_s"))
    first_direct_apply = _num(direct_control_apply.get("first_apply_ts_sec"))
    if direct_control_apply.get("available") is True and first_mapped is not None and first_direct_apply is not None:
        return {
            "control_apply_observation_delay_s": max(0.0, first_direct_apply - first_mapped),
            "control_apply_observation_delay_source": "direct_bridge_control_apply.jsonl",
            "control_apply_observation_delay_timeseries_s": timeseries_delay,
        }
    return {
        "control_apply_observation_delay_s": timeseries_delay,
        "control_apply_observation_delay_source": control_application.get("control_apply_observation_delay_source"),
        "control_apply_observation_delay_timeseries_s": timeseries_delay,
    }


def _augment_control_application_with_decode(
    control_application: Mapping[str, Any],
    *,
    control_decode_debug: Mapping[str, Any],
) -> dict[str, Any]:
    out = dict(control_application)
    decoded_mapped_frames = _num(control_decode_debug.get("nonzero_mapped_control_frames"))
    if decoded_mapped_frames is None:
        return out
    current_mapped = _num(out.get("nonzero_mapped_control_frames"))
    if current_mapped is None or current_mapped <= 0:
        out["nonzero_mapped_control_frames"] = int(decoded_mapped_frames)
        out["nonzero_mapped_control_frames_source"] = "control_decode_debug.jsonl"
        out["first_nonzero_mapped_control_s"] = control_decode_debug.get("first_nonzero_mapped_control_ts_sec")
        out["first_nonzero_mapped_control_time_base"] = "control_decode_wall_time"
    else:
        out["nonzero_mapped_control_frames_source"] = "timeseries"
    return out


def _route_progress_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    route_samples: list[tuple[int, float | None, float]] = []
    speed_samples: list[tuple[int, float | None, float]] = []
    first_mapped_idx: int | None = None
    first_applied_idx: int | None = None
    for index, row in enumerate(rows):
        row_time = _row_time_s(row)
        route_s = _first_row_number(row, "route_s", "nearest_route_s")
        if route_s is not None:
            route_samples.append((index, row_time, route_s))
        speed = _first_row_number(row, "ego_speed", "speed_mps", "ego_speed_mps")
        if speed is not None:
            speed_samples.append((index, row_time, speed))
        if first_mapped_idx is None and _row_has_nonzero_control(
            row,
            throttle_field="throttle_mapped",
            brake_field="brake_mapped",
            steer_field="bridge_steer_mapped",
        ):
            first_mapped_idx = index
        if first_applied_idx is None and _row_has_nonzero_control(
            row,
            throttle_field="throttle_applied",
            brake_field="brake_applied",
            steer_field="carla_steer_applied",
        ):
            first_applied_idx = index

    route_values = [sample[2] for sample in route_samples]
    speed_values = [sample[2] for sample in speed_samples]
    return {
        "route_s_start_m": route_values[0] if route_values else None,
        "route_s_end_m": route_values[-1] if route_values else None,
        "route_s_delta_m": (route_values[-1] - route_values[0]) if len(route_values) >= 2 else None,
        "route_s_range_m": (max(route_values) - min(route_values)) if route_values else None,
        "route_s_after_first_mapped_control_delta_m": _sample_delta_from_index(
            route_samples,
            first_mapped_idx,
        ),
        "route_s_after_first_applied_control_delta_m": _sample_delta_from_index(
            route_samples,
            first_applied_idx,
        ),
        "ego_speed_mean_mps": _mean(speed_values),
        "ego_speed_p95_mps": _percentile(speed_values, 0.95),
        "ego_speed_max_mps": max(speed_values) if speed_values else None,
        "stopped_ratio": _stopped_ratio(speed_values),
        "stopped_ratio_after_first_applied_control": _stopped_ratio(
            [sample[2] for sample in speed_samples if first_applied_idx is not None and sample[0] >= first_applied_idx]
        ),
    }


def _sample_delta_from_index(
    samples: Sequence[tuple[int, float | None, float]],
    start_index: int | None,
) -> float | None:
    if start_index is None:
        return None
    values = [value for index, _time_s, value in samples if index >= start_index]
    if len(values) < 2:
        return None
    return values[-1] - values[0]


def _first_row_number(row: Mapping[str, Any], *fields: str) -> float | None:
    for field in fields:
        value = _num(row.get(field))
        if value is not None:
            return value
    return None


_CONTROL_BRIDGE_LOG_LINE_RE = re.compile(
    r"^\[(?P<level>[A-Z]+)\] \[(?P<timestamp>[0-9]+(?:\.[0-9]+)?)\] "
    r"\[carla_control_bridge\]: (?P<message>.*)$"
)
_CONTROL_BRIDGE_KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^,\s]+)")


def _analyze_control_bridge_log(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return {
            "available": False,
            "read_error": str(exc),
        }

    buffered_warning_count = 0
    no_ego_skip_count = 0
    first_ego_warning_wall_s: float | None = None
    last_ego_warning_wall_s: float | None = None
    bind_wall_s: float | None = None
    bind_actor_id: int | None = None
    bind_role: str | None = None
    configured_apply_hz: float | None = None
    sync_to_world_tick: bool | None = None
    apply_events: list[dict[str, Any]] = []

    for line in lines:
        parsed = _parse_control_bridge_log_line(line)
        if parsed is None:
            continue
        timestamp = parsed["timestamp"]
        message = str(parsed["message"])
        if "Bridge listening on" in message:
            values = _parse_control_bridge_key_values(message)
            configured_apply_hz = _num(values.get("apply_hz"))
            sync_to_world_tick = _parse_bool(values.get("sync_to_world_tick"))
            continue
        if "ego vehicle not found; control commands will be buffered" in message:
            buffered_warning_count += 1
            first_ego_warning_wall_s = _first_number(first_ego_warning_wall_s, timestamp)
            last_ego_warning_wall_s = timestamp
            continue
        if "no ego found yet; skip control" in message:
            no_ego_skip_count += 1
            first_ego_warning_wall_s = _first_number(first_ego_warning_wall_s, timestamp)
            last_ego_warning_wall_s = timestamp
            continue
        if message.startswith("control target bound "):
            values = _parse_control_bridge_key_values(message)
            bind_wall_s = timestamp
            bind_actor_id = _int(values.get("actor_id"))
            bind_role = values.get("role")
            continue
        if message.startswith("apply frame="):
            event = _parse_control_bridge_apply_event(timestamp, message)
            if event:
                apply_events.append(event)

    source_counts: dict[str, int] = {}
    for event in apply_events:
        source = str(event.get("source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1

    first_apply = apply_events[0] if apply_events else None
    last_apply = apply_events[-1] if apply_events else None
    first_pending = _first_event_with_source(apply_events, "pending")
    first_watchdog = _first_event_with_source(apply_events, "watchdog")
    first_nonzero_throttle = _first_event_with_nonzero_throttle(apply_events)
    last_rx_change = _last_rx_change_event(apply_events)
    final_drop = _event_int(last_apply, "drop_same_frame") if last_apply else None
    final_applied = _event_int(last_apply, "applied") if last_apply else None
    final_rx = _event_int(last_apply, "rx") if last_apply else None

    apply_wall_span_s = _span(
        _event_num(first_apply, "wall_s"),
        _event_num(last_apply, "wall_s"),
    )
    apply_frame_span = _span(
        _event_num(first_apply, "frame"),
        _event_num(last_apply, "frame"),
    )
    same_frame_drop_ratio = None
    if final_drop is not None and final_applied is not None and (final_drop + final_applied) > 0:
        same_frame_drop_ratio = final_drop / (final_drop + final_applied)
    apply_frame_coverage_ratio = None
    if final_applied is not None and apply_frame_span is not None and apply_frame_span > 0:
        apply_frame_coverage_ratio = min(1.0, final_applied / apply_frame_span)
    bind_to_first_apply_s = _span(bind_wall_s, _event_num(first_apply, "wall_s"))
    rx_stopped_before_watchdog_s = _span(
        _event_num(last_rx_change, "wall_s"),
        _event_num(first_watchdog, "wall_s"),
    )

    return {
        "available": True,
        "line_count": len(lines),
        "buffered_before_ego_count": buffered_warning_count,
        "no_ego_skip_count": no_ego_skip_count,
        "first_ego_warning_wall_s": first_ego_warning_wall_s,
        "last_ego_warning_wall_s": last_ego_warning_wall_s,
        "ego_bind_wall_s": bind_wall_s,
        "ego_bind_actor_id": bind_actor_id,
        "ego_bind_role": bind_role,
        "configured_apply_hz": configured_apply_hz,
        "sync_to_world_tick": sync_to_world_tick,
        "ego_bind_delay_s": _span(first_ego_warning_wall_s, bind_wall_s),
        "apply_log_count": len(apply_events),
        "apply_source_counts": source_counts,
        "first_apply_wall_s": _event_num(first_apply, "wall_s"),
        "first_pending_apply_wall_s": _event_num(first_pending, "wall_s"),
        "first_watchdog_apply_wall_s": _event_num(first_watchdog, "wall_s"),
        "first_nonzero_throttle_apply_wall_s": _event_num(first_nonzero_throttle, "wall_s"),
        "bind_to_first_apply_s": bind_to_first_apply_s,
        "first_pending_to_watchdog_s": _span(
            _event_num(first_pending, "wall_s"),
            _event_num(first_watchdog, "wall_s"),
        ),
        "last_rx_change_wall_s": _event_num(last_rx_change, "wall_s"),
        "rx_stopped_before_watchdog_s": rx_stopped_before_watchdog_s,
        "apply_frame_start": _event_int(first_apply, "frame"),
        "apply_frame_end": _event_int(last_apply, "frame"),
        "apply_frame_span": apply_frame_span,
        "apply_wall_span_s": apply_wall_span_s,
        "apply_world_frame_hz": _safe_div(apply_frame_span, apply_wall_span_s),
        "apply_frame_coverage_ratio": apply_frame_coverage_ratio,
        "final_rx_count": final_rx,
        "final_applied_count": final_applied,
        "final_drop_same_frame_count": final_drop,
        "same_frame_drop_ratio": same_frame_drop_ratio,
        "max_rx_count": _max_event_int(apply_events, "rx"),
        "max_applied_count": _max_event_int(apply_events, "applied"),
        "max_drop_same_frame_count": _max_event_int(apply_events, "drop_same_frame"),
    }


def _parse_control_bridge_log_line(line: str) -> dict[str, Any] | None:
    match = _CONTROL_BRIDGE_LOG_LINE_RE.match(line.strip())
    if not match:
        return None
    timestamp = _num(match.group("timestamp"))
    if timestamp is None:
        return None
    return {
        "level": match.group("level"),
        "timestamp": timestamp,
        "message": match.group("message"),
    }


def _parse_control_bridge_key_values(message: str) -> dict[str, str]:
    return {
        match.group("key"): match.group("value")
        for match in _CONTROL_BRIDGE_KEY_VALUE_RE.finditer(message)
    }


def _parse_control_bridge_apply_event(timestamp: float, message: str) -> dict[str, Any] | None:
    values = _parse_control_bridge_key_values(message)
    frame = _int(values.get("frame"))
    if frame is None:
        return None
    return {
        "wall_s": timestamp,
        "frame": frame,
        "source": values.get("source"),
        "actor_id": _int(values.get("actor_id")),
        "role": values.get("role"),
        "throttle": _num(values.get("throttle")),
        "brake": _num(values.get("brake")),
        "rx": _int(values.get("rx")),
        "applied": _int(values.get("applied")),
        "drop_same_frame": _int(values.get("drop_same_frame")),
    }


def _control_bridge_log_warnings(
    metrics: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> list[str]:
    log_metrics = metrics.get("control_bridge_log")
    if not isinstance(log_metrics, Mapping) or log_metrics.get("available") is not True:
        return []
    warnings: list[str] = []
    frame_hz = _num(log_metrics.get("apply_world_frame_hz"))
    sync_tick_cadence_explained = _sync_tick_cadence_explained(
        sync_to_world_tick=_parse_bool(log_metrics.get("sync_to_world_tick")),
        coverage_ratio=_num(log_metrics.get("apply_frame_coverage_ratio")),
        thresholds=thresholds,
    )
    if (
        frame_hz is not None
        and frame_hz < thresholds["min_control_bridge_apply_frame_hz"]
        and _num(log_metrics.get("apply_log_count")) not in {None, 0}
        and not sync_tick_cadence_explained
    ):
        warnings.append("control_bridge_world_frame_cadence_low")
    drop_ratio = _num(log_metrics.get("same_frame_drop_ratio"))
    coverage_ratio = _num(log_metrics.get("apply_frame_coverage_ratio"))
    sync_to_world_tick = _parse_bool(log_metrics.get("sync_to_world_tick"))
    same_frame_drop_expected = _sync_tick_cadence_explained(
        sync_to_world_tick=sync_to_world_tick,
        coverage_ratio=coverage_ratio,
        thresholds=thresholds,
    )
    if (
        frame_hz is not None
        and frame_hz < thresholds["min_control_bridge_apply_frame_hz"]
        and _num(log_metrics.get("apply_log_count")) not in {None, 0}
        and same_frame_drop_expected
    ):
        warnings.append("control_bridge_sync_tick_wall_cadence_low_explained")
    if (
        drop_ratio is not None
        and drop_ratio > thresholds["max_control_bridge_same_frame_drop_ratio"]
        and not same_frame_drop_expected
    ):
        warnings.append("control_bridge_drop_same_frame_high")
    bind_delay = _num(log_metrics.get("ego_bind_delay_s"))
    if (
        bind_delay is not None
        and bind_delay > thresholds["max_control_bridge_ego_bind_delay_s"]
    ):
        warnings.append("control_bridge_ego_bind_delay_high")
    bind_to_first_apply = _num(log_metrics.get("bind_to_first_apply_s"))
    if (
        bind_to_first_apply is not None
        and bind_to_first_apply > thresholds["max_control_bridge_bind_to_first_apply_s"]
    ):
        warnings.append("control_bridge_first_apply_delay_high")
    if log_metrics.get("first_watchdog_apply_wall_s") is not None and _num(log_metrics.get("final_rx_count")):
        warnings.append("control_bridge_watchdog_after_rx_stop")
    return warnings


def _sync_tick_cadence_explained(
    *,
    sync_to_world_tick: bool | None,
    coverage_ratio: float | None,
    thresholds: Mapping[str, float],
) -> bool:
    return bool(
        sync_to_world_tick is True
        and coverage_ratio is not None
        and coverage_ratio >= thresholds["min_control_bridge_apply_frame_coverage_ratio"]
    )


def _control_latency_source(
    *,
    summary_latency: float | None,
    timeseries_latency: float | None,
    decode_latency: float | None,
) -> str | None:
    if summary_latency is not None:
        return "summary.metrics.control_latency_p95_ms"
    if timeseries_latency is not None:
        return "timeseries.control_latency_ms"
    if decode_latency is not None:
        return "control_decode_debug_jsonl.control_latency_ms"
    return None


def _analyze_control_decode_debug(
    path: Path | None,
    *,
    trajectory_consume_path: Path | None = None,
    planning_topic_debug_path: Path | None = None,
    planning_route_segment_debug_path: Path | None = None,
    auxiliary_context_path: Path | None = None,
) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}

    control_latency_values: list[float] = []
    control_message_age_values: list[float] = []
    planning_message_age_values: list[float] = []
    trace_rows: list[dict[str, Any]] = []
    command_payload_row_count = 0
    applied_payload_row_count = 0
    no_command_placeholder_count = 0
    line_count = 0
    parsed_count = 0
    malformed_count = 0

    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                line_count += 1
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if not isinstance(payload, Mapping):
                    malformed_count += 1
                    continue
                parsed_count += 1
                if (value := _nested_first_number(payload, _CONTROL_LATENCY_PATHS)) is not None:
                    control_latency_values.append(value)
                if (value := _nested_first_number(payload, _CONTROL_MESSAGE_AGE_PATHS)) is not None:
                    control_message_age_values.append(value)
                if (value := _nested_first_number(payload, _PLANNING_MESSAGE_AGE_PATHS)) is not None:
                    planning_message_age_values.append(value)
                trace = _control_decode_payload_to_trace_row(payload)
                if _trace_row_has_command_payload(trace):
                    command_payload_row_count += 1
                else:
                    no_command_placeholder_count += 1
                if _trace_row_has_applied_payload(trace):
                    applied_payload_row_count += 1
                if trace:
                    trace_rows.append(trace)
    except OSError as exc:
        return {
            "available": False,
            "path": str(path),
            "read_error": str(exc),
        }

    auxiliary_context = _load_auxiliary_longitudinal_context(
        auxiliary_context_path,
        primary_path=path,
    )
    enriched_rows = 0
    if auxiliary_context.get("available") is True:
        enriched_rows = _enrich_trace_rows_from_auxiliary_context(
            trace_rows,
            auxiliary_context.get("trace_rows") or [],
        )

    raw_layer = _command_oscillation_layer(
        trace_rows,
        layer="apollo_raw_command",
        throttle_fields=("throttle_raw", "apollo_desired_throttle", "raw_throttle"),
        brake_fields=("brake_raw", "apollo_desired_brake", "raw_brake"),
        steer_fields=("apollo_steer_raw", "raw_steer", "steering_normalized_for_mapping"),
        max_switch_count=DEFAULT_THRESHOLDS["max_raw_throttle_brake_switch_count"],
    )
    mapped_layer = _command_oscillation_layer(
        trace_rows,
        layer="bridge_mapped_command",
        throttle_fields=("throttle_mapped", "mapped_throttle_cmd", "commanded_throttle"),
        brake_fields=("brake_mapped", "mapped_brake_cmd", "commanded_brake"),
        steer_fields=("bridge_steer_mapped", "mapped_carla_steer_cmd", "commanded_steer"),
        max_switch_count=DEFAULT_THRESHOLDS["max_mapped_throttle_brake_switch_count"],
    )
    nonzero_mapped = sum(
        1
        for row in trace_rows
        if _row_has_nonzero_control(
            row,
            throttle_field="throttle_mapped",
            brake_field="brake_mapped",
            steer_field="bridge_steer_mapped",
        )
    )
    first_nonzero_mapped_ts = None
    for row in trace_rows:
        if _row_has_nonzero_control(
            row,
            throttle_field="throttle_mapped",
            brake_field="brake_mapped",
            steer_field="bridge_steer_mapped",
        ):
            first_nonzero_mapped_ts = _num(row.get("ts_sec"))
            break

    return {
        "available": True,
        "path": str(path),
        "line_count": line_count,
        "parsed_line_count": parsed_count,
        "malformed_line_count": malformed_count,
        "auxiliary_context_path": auxiliary_context.get("path"),
        "auxiliary_context_line_count": auxiliary_context.get("line_count"),
        "auxiliary_context_malformed_line_count": auxiliary_context.get("malformed_line_count"),
        "auxiliary_context_enriched_rows": enriched_rows,
        "trace_row_count": len(trace_rows),
        "command_payload_row_count": command_payload_row_count,
        "applied_payload_row_count": applied_payload_row_count,
        "no_command_placeholder_count": no_command_placeholder_count,
        "command_payload_available": command_payload_row_count > 0,
        "applied_payload_available": applied_payload_row_count > 0,
        "nonzero_mapped_control_frames": nonzero_mapped if trace_rows else None,
        "first_nonzero_mapped_control_ts_sec": first_nonzero_mapped_ts,
        "apollo_raw_command_layer": raw_layer,
        "bridge_mapped_command_layer": mapped_layer,
        "throttle_brake_mutual_exclusion_applied_count": sum(
            1 for row in trace_rows if _parse_bool(row.get("throttle_brake_mutual_exclusion_applied")) is True
        ),
        "throttle_brake_hysteresis_held_count": sum(
            1 for row in trace_rows if _parse_bool(row.get("throttle_brake_hysteresis_held")) is True
        ),
        "longitudinal_debug": _longitudinal_debug_summary(trace_rows),
        "longitudinal_oscillation_attribution": _longitudinal_oscillation_attribution(trace_rows),
        "control_sequence_diagnostics": _control_sequence_diagnostics(trace_rows),
        "steering_mapping_saturation": _steering_mapping_saturation(trace_rows),
        "trajectory_consume_correlation": _trajectory_consume_correlation(
            trace_rows,
            trajectory_consume_path=trajectory_consume_path,
        ),
        "planning_trajectory_correlation": _planning_trajectory_correlation(
            trace_rows,
            trajectory_consume_path=trajectory_consume_path,
            planning_topic_debug_path=planning_topic_debug_path,
            planning_route_segment_debug_path=planning_route_segment_debug_path,
        ),
        "control_latency_sample_count": len(control_latency_values),
        "control_latency_p50_ms": _percentile(control_latency_values, 0.50),
        "control_latency_p95_ms": _percentile(control_latency_values, 0.95),
        "control_latency_max_ms": max(control_latency_values) if control_latency_values else None,
        "control_message_age_sample_count": len(control_message_age_values),
        "control_message_age_p95_ms": _percentile(control_message_age_values, 0.95),
        "planning_message_age_sample_count": len(planning_message_age_values),
        "planning_message_age_p95_ms": _percentile(planning_message_age_values, 0.95),
    }


def _analyze_direct_control_apply_log(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}

    line_count = 0
    parsed_count = 0
    malformed_count = 0
    first_apply: Mapping[str, Any] | None = None
    last_apply: Mapping[str, Any] | None = None
    max_throttle: float | None = None
    max_brake: float | None = None
    source_counts: dict[str, int] = {}
    actor_id: int | None = None

    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                line_count += 1
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if not isinstance(payload, Mapping):
                    malformed_count += 1
                    continue
                parsed_count += 1
                if first_apply is None:
                    first_apply = payload
                last_apply = payload
                source = str(payload.get("source") or "unknown")
                source_counts[source] = source_counts.get(source, 0) + 1
                actor_id = _int(payload.get("actor_id")) if actor_id is None else actor_id
                throttle = _num(payload.get("throttle"))
                brake = _num(payload.get("brake"))
                if throttle is not None:
                    max_throttle = throttle if max_throttle is None else max(max_throttle, throttle)
                if brake is not None:
                    max_brake = brake if max_brake is None else max(max_brake, brake)
    except OSError as exc:
        return {
            "available": False,
            "path": str(path),
            "read_error": str(exc),
        }

    first_ts = _event_num(first_apply, "ts_sec")
    last_ts = _event_num(last_apply, "ts_sec")
    first_frame = _event_int(first_apply, "frame_id")
    last_frame = _event_int(last_apply, "frame_id")
    return {
        "available": True,
        "path": str(path),
        "line_count": line_count,
        "parsed_line_count": parsed_count,
        "malformed_line_count": malformed_count,
        "apply_count": parsed_count,
        "first_apply_ts_sec": first_ts,
        "last_apply_ts_sec": last_ts,
        "apply_wall_span_s": _span(first_ts, last_ts),
        "first_apply_frame_id": first_frame,
        "last_apply_frame_id": last_frame,
        "apply_frame_span": _span(
            float(first_frame) if first_frame is not None else None,
            float(last_frame) if last_frame is not None else None,
        ),
        "source_counts": source_counts,
        "actor_id": actor_id,
        "max_throttle": max_throttle,
        "max_brake": max_brake,
    }


def _control_decode_payload_to_trace_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    parsed = payload.get("parsed_control")
    parsed_map = parsed if isinstance(parsed, Mapping) else {}
    output = payload.get("output_to_carla")
    output_map = output if isinstance(output, Mapping) else {}
    apollo_raw = payload.get("apollo_raw")
    apollo_raw_map = apollo_raw if isinstance(apollo_raw, Mapping) else {}
    raw_dump = payload.get("raw_control_msg_dump")
    raw_dump_map = raw_dump if isinstance(raw_dump, Mapping) else {}
    bridge_mapped = payload.get("bridge_mapped")
    bridge_mapped_map = bridge_mapped if isinstance(bridge_mapped, Mapping) else {}
    carla_applied = payload.get("carla_applied")
    carla_applied_map = carla_applied if isinstance(carla_applied, Mapping) else {}
    apollo_control = payload.get("apollo_control")
    apollo_control_map = apollo_control if isinstance(apollo_control, Mapping) else {}
    row = {
        "ts_sec": _first_number(
            payload.get("ts_sec"),
            payload.get("timestamp"),
            parsed_map.get("control_rx_timestamp"),
            output_map.get("control_rx_timestamp"),
        ),
        "control_header_sequence_num": _first_number(
            parsed_map.get("control_header_sequence_num"),
            payload.get("control_header_sequence_num"),
            apollo_control_map.get("header_sequence_num"),
            raw_dump_map.get("control_header_sequence_num"),
        ),
        "control_header_timestamp_sec": _first_number(
            parsed_map.get("control_header_timestamp_sec"),
            payload.get("control_header_timestamp_sec"),
            apollo_control_map.get("header_timestamp_sec"),
            raw_dump_map.get("control_header_timestamp_sec"),
        ),
        "control_rx_timestamp": _first_number(
            parsed_map.get("control_rx_timestamp"),
            payload.get("control_rx_timestamp"),
            apollo_control_map.get("rx_timestamp"),
            output_map.get("control_rx_timestamp"),
        ),
        "control_timestamp": _first_number(
            parsed_map.get("control_timestamp"),
            payload.get("control_timestamp"),
            apollo_control_map.get("control_timestamp"),
            output_map.get("control_timestamp"),
        ),
        "throttle_raw": _first_number(
            payload.get("raw_throttle"),
            apollo_raw_map.get("throttle"),
            parsed_map.get("throttle"),
            _percent_to_unit(_nested_raw(payload, ("raw_control_msg_dump", "throttle"))),
        ),
        "brake_raw": _first_number(
            payload.get("raw_brake"),
            apollo_raw_map.get("brake"),
            parsed_map.get("brake"),
            _percent_to_unit(_nested_raw(payload, ("raw_control_msg_dump", "brake"))),
        ),
        "apollo_steer_raw": _first_number(
            payload.get("raw_steer"),
            apollo_raw_map.get("steer"),
            parsed_map.get("steer"),
            parsed_map.get("steering_normalized_for_mapping"),
            output_map.get("steering_normalized_for_mapping"),
        ),
        "apollo_steering_target_pct": _first_number(
            payload.get("apollo_steering_target_pct"),
            payload.get("steering_target"),
            apollo_raw_map.get("steering_target"),
            parsed_map.get("steering_target"),
            output_map.get("steering_target"),
            raw_dump_map.get("steering_target"),
        ),
        "throttle_mapped": _first_number(
            payload.get("mapped_throttle_cmd"),
            bridge_mapped_map.get("throttle"),
            bridge_mapped_map.get("mapped_throttle_cmd"),
            output_map.get("mapped_throttle_cmd"),
            payload.get("commanded_throttle"),
            output_map.get("throttle"),
        ),
        "brake_mapped": _first_number(
            payload.get("mapped_brake_cmd"),
            bridge_mapped_map.get("brake"),
            bridge_mapped_map.get("mapped_brake_cmd"),
            output_map.get("mapped_brake_cmd"),
            payload.get("commanded_brake"),
            output_map.get("brake"),
        ),
        "bridge_steer_mapped": _first_number(
            payload.get("mapped_carla_steer_cmd"),
            bridge_mapped_map.get("steer"),
            bridge_mapped_map.get("mapped_carla_steer_cmd"),
            output_map.get("mapped_carla_steer_cmd"),
            payload.get("commanded_steer"),
            output_map.get("steer"),
        ),
        "throttle_applied": _first_number(
            payload.get("throttle_applied"),
            carla_applied_map.get("throttle"),
        ),
        "brake_applied": _first_number(
            payload.get("brake_applied"),
            carla_applied_map.get("brake"),
        ),
        "carla_steer_applied": _first_number(
            payload.get("carla_steer_applied"),
            carla_applied_map.get("steer"),
        ),
        "throttle_before_mutual_exclusion": _first_number(
            payload.get("throttle_before_mutual_exclusion"),
            bridge_mapped_map.get("throttle_before_mutual_exclusion"),
            output_map.get("throttle_before_mutual_exclusion"),
        ),
        "brake_before_mutual_exclusion": _first_number(
            payload.get("brake_before_mutual_exclusion"),
            bridge_mapped_map.get("brake_before_mutual_exclusion"),
            output_map.get("brake_before_mutual_exclusion"),
        ),
        "throttle_brake_mutual_exclusion_applied": _first_raw(
            payload,
            "throttle_brake_mutual_exclusion_applied",
            bridge_mapped_map,
            "throttle_brake_mutual_exclusion_applied",
            output_map,
            "throttle_brake_mutual_exclusion_applied",
        ),
        "throttle_brake_hysteresis_held": _first_raw(
            payload,
            "throttle_brake_hysteresis_held",
            bridge_mapped_map,
            "throttle_brake_hysteresis_held",
            output_map,
            "throttle_brake_hysteresis_held",
        ),
        "debug_simple_lon_current_speed_mps": _first_number(
            parsed_map.get("debug_simple_lon_current_speed_mps"),
            payload.get("debug_simple_lon_current_speed_mps"),
            raw_dump_map.get("debug_simple_lon_current_speed"),
        ),
        "debug_simple_lon_speed_reference_mps": _first_number(
            parsed_map.get("debug_simple_lon_speed_reference_mps"),
            payload.get("debug_simple_lon_speed_reference_mps"),
            raw_dump_map.get("debug_simple_lon_speed_reference"),
        ),
        "debug_simple_lon_speed_error_mps": _first_number(
            parsed_map.get("debug_simple_lon_speed_error_mps"),
            payload.get("debug_simple_lon_speed_error_mps"),
            raw_dump_map.get("debug_simple_lon_speed_error"),
        ),
        "debug_simple_lon_current_acceleration_mps2": _first_number(
            parsed_map.get("debug_simple_lon_current_acceleration_mps2"),
            payload.get("debug_simple_lon_current_acceleration_mps2"),
            raw_dump_map.get("debug_simple_lon_current_acceleration"),
        ),
        "debug_simple_lon_acceleration_reference_mps2": _first_number(
            parsed_map.get("debug_simple_lon_acceleration_reference_mps2"),
            payload.get("debug_simple_lon_acceleration_reference_mps2"),
            raw_dump_map.get("debug_simple_lon_acceleration_reference"),
        ),
        "debug_simple_lon_acceleration_error_mps2": _first_number(
            parsed_map.get("debug_simple_lon_acceleration_error_mps2"),
            payload.get("debug_simple_lon_acceleration_error_mps2"),
            raw_dump_map.get("debug_simple_lon_acceleration_error"),
        ),
        "debug_simple_lon_acceleration_cmd_mps2": _first_number(
            parsed_map.get("debug_simple_lon_acceleration_cmd_mps2"),
            payload.get("debug_simple_lon_acceleration_cmd_mps2"),
            raw_dump_map.get("debug_simple_lon_acceleration_cmd"),
        ),
        "debug_simple_lon_acceleration_cmd_closeloop_mps2": _first_number(
            parsed_map.get("debug_simple_lon_acceleration_cmd_closeloop_mps2"),
            payload.get("debug_simple_lon_acceleration_cmd_closeloop_mps2"),
            raw_dump_map.get("debug_simple_lon_acceleration_cmd_closeloop"),
        ),
        "debug_simple_lon_acceleration_lookup_mps2": _first_number(
            parsed_map.get("debug_simple_lon_acceleration_lookup_mps2"),
            payload.get("debug_simple_lon_acceleration_lookup_mps2"),
            raw_dump_map.get("debug_simple_lon_acceleration_lookup"),
        ),
        "debug_simple_lon_throttle_cmd_pct": _first_number(
            parsed_map.get("debug_simple_lon_throttle_cmd_pct"),
            payload.get("debug_simple_lon_throttle_cmd_pct"),
            raw_dump_map.get("debug_simple_lon_throttle_cmd"),
        ),
        "debug_simple_lon_brake_cmd_pct": _first_number(
            parsed_map.get("debug_simple_lon_brake_cmd_pct"),
            payload.get("debug_simple_lon_brake_cmd_pct"),
            raw_dump_map.get("debug_simple_lon_brake_cmd"),
        ),
        "debug_simple_lon_path_remain_m": _first_number(
            parsed_map.get("debug_simple_lon_path_remain_m"),
            payload.get("debug_simple_lon_path_remain_m"),
            raw_dump_map.get("debug_simple_lon_path_remain"),
        ),
        "debug_simple_lon_station_error_m": _first_number(
            parsed_map.get("debug_simple_lon_station_error_m"),
            payload.get("debug_simple_lon_station_error_m"),
            raw_dump_map.get("debug_simple_lon_station_error"),
        ),
        "debug_simple_lon_preview_station_error_m": _first_number(
            parsed_map.get("debug_simple_lon_preview_station_error_m"),
            payload.get("debug_simple_lon_preview_station_error_m"),
            raw_dump_map.get("debug_simple_lon_preview_station_error"),
        ),
        "debug_simple_lon_is_full_stop": _first_raw(
            payload,
            "debug_simple_lon_is_full_stop",
            parsed_map,
            "debug_simple_lon_is_full_stop",
            raw_dump_map,
            "debug_simple_lon_is_full_stop",
        ),
    }
    for target, raw_key in SIMPLE_LON_POINT_FIELDS.items():
        row[target] = _first_number(
            parsed_map.get(target),
            payload.get(target),
            raw_dump_map.get(raw_key),
        )
    return {key: value for key, value in row.items() if value not in {None, ""}}


def _load_auxiliary_longitudinal_context(
    path: Path | None,
    *,
    primary_path: Path | None,
) -> dict[str, Any]:
    if path is None or primary_path is None or path == primary_path or not path.exists():
        return {"available": False}
    trace_rows: list[dict[str, Any]] = []
    line_count = 0
    malformed_count = 0
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                line_count += 1
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if not isinstance(payload, Mapping):
                    malformed_count += 1
                    continue
                trace = _control_decode_payload_to_trace_row(payload)
                if _int(trace.get("control_header_sequence_num")) is None:
                    continue
                if any(trace.get(field) not in {None, ""} for field in LONGITUDINAL_CONTEXT_FIELDS):
                    trace_rows.append(trace)
    except OSError as exc:
        return {
            "available": False,
            "path": str(path),
            "read_error": str(exc),
        }
    return {
        "available": bool(trace_rows),
        "path": str(path),
        "line_count": line_count,
        "malformed_line_count": malformed_count,
        "trace_rows": trace_rows,
    }


def _enrich_trace_rows_from_auxiliary_context(
    rows: list[dict[str, Any]],
    auxiliary_rows: Sequence[Mapping[str, Any]],
) -> int:
    auxiliary_by_sequence: dict[int, Mapping[str, Any]] = {}
    for auxiliary in auxiliary_rows:
        sequence = _int(auxiliary.get("control_header_sequence_num"))
        if sequence is not None:
            auxiliary_by_sequence[sequence] = auxiliary

    enriched_rows = 0
    for row in rows:
        sequence = _int(row.get("control_header_sequence_num"))
        if sequence is None:
            continue
        auxiliary = auxiliary_by_sequence.get(sequence)
        if auxiliary is None:
            continue
        enriched = False
        for field in LONGITUDINAL_CONTEXT_FIELDS:
            if row.get(field) in {None, ""} and auxiliary.get(field) not in {None, ""}:
                row[field] = auxiliary[field]
                enriched = True
        if enriched:
            enriched_rows += 1
    return enriched_rows


def _trace_row_has_command_payload(row: Mapping[str, Any]) -> bool:
    command_fields = (
        "throttle_raw",
        "brake_raw",
        "apollo_steer_raw",
        "throttle_mapped",
        "brake_mapped",
        "bridge_steer_mapped",
    )
    return any(_num(row.get(field)) is not None for field in command_fields)


def _trace_row_has_applied_payload(row: Mapping[str, Any]) -> bool:
    applied_fields = (
        "throttle_applied",
        "brake_applied",
        "carla_steer_applied",
    )
    return any(_num(row.get(field)) is not None for field in applied_fields)


def _control_sequence_diagnostics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    command_rows = [row for row in rows if _trace_row_has_command_payload(row)]
    rows_with_sequence: list[Mapping[str, Any]] = []
    sequence_values: list[int] = []
    for row in command_rows:
        sequence = _int(row.get("control_header_sequence_num"))
        if sequence is None:
            continue
        rows_with_sequence.append(row)
        sequence_values.append(sequence)

    unique_sequences = sorted(set(sequence_values))
    by_sequence: dict[int, Mapping[str, Any]] = {}
    for row in rows_with_sequence:
        sequence = _int(row.get("control_header_sequence_num"))
        if sequence is not None:
            by_sequence[sequence] = row
    unique_rows = [by_sequence[sequence] for sequence in unique_sequences]

    row_count = len(command_rows)
    with_sequence_count = len(rows_with_sequence)
    unique_count = len(unique_sequences)
    duplicate_ratio = None
    if with_sequence_count:
        duplicate_ratio = max(0.0, 1.0 - (unique_count / with_sequence_count))

    return {
        "command_payload_row_count": row_count,
        "rows_with_sequence_count": with_sequence_count,
        "rows_missing_sequence_count": row_count - with_sequence_count,
        "unique_sequence_count": unique_count,
        "duplicate_sequence_ratio": duplicate_ratio,
        "raw_throttle_brake_switch_count_rows": _throttle_brake_switch_count(
            command_rows,
            throttle_fields=("throttle_raw",),
            brake_fields=("brake_raw",),
        ),
        "raw_throttle_brake_switch_count_unique_sequence": _throttle_brake_switch_count(
            unique_rows,
            throttle_fields=("throttle_raw",),
            brake_fields=("brake_raw",),
        ),
        "mapped_throttle_brake_switch_count_rows": _throttle_brake_switch_count(
            command_rows,
            throttle_fields=("throttle_mapped",),
            brake_fields=("brake_mapped",),
        ),
        "mapped_throttle_brake_switch_count_unique_sequence": _throttle_brake_switch_count(
            unique_rows,
            throttle_fields=("throttle_mapped",),
            brake_fields=("brake_mapped",),
        ),
        "applied_throttle_brake_switch_count_rows": _throttle_brake_switch_count(
            command_rows,
            throttle_fields=("throttle_applied",),
            brake_fields=("brake_applied",),
        ),
        "applied_throttle_brake_switch_count_unique_sequence": _throttle_brake_switch_count(
            unique_rows,
            throttle_fields=("throttle_applied",),
            brake_fields=("brake_applied",),
        ),
    }


def _steering_mapping_saturation(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    raw_targets: list[float] = []
    normalized_values: list[float] = []
    mapped_values_when_saturated: list[float] = []
    applied_values_when_saturated: list[float] = []
    saturated_count = 0
    saturated_low_mapped_count = 0
    saturated_low_applied_count = 0
    examples: list[dict[str, Any]] = []
    raw_pct_saturation_threshold = 99.0
    low_mapped_abs_threshold = 0.02
    low_applied_abs_threshold = 0.02

    for index, row in enumerate(rows):
        raw_pct = _first_row_number(
            row,
            "apollo_steering_target_pct",
            "raw_steering_target_pct",
            "steering_target",
        )
        if raw_pct is None:
            continue
        raw_targets.append(float(raw_pct))
        normalized = _first_row_number(
            row,
            "apollo_steer_raw",
            "raw_steer",
            "steering_normalized_for_mapping",
        )
        mapped = _first_row_number(row, "bridge_steer_mapped", "mapped_carla_steer_cmd")
        applied = _first_row_number(row, "carla_steer_applied")
        if normalized is not None:
            normalized_values.append(float(normalized))

        saturated = abs(float(raw_pct)) >= raw_pct_saturation_threshold
        if not saturated:
            continue
        saturated_count += 1
        low_mapped = mapped is not None and abs(float(mapped)) <= low_mapped_abs_threshold
        low_applied = applied is not None and abs(float(applied)) <= low_applied_abs_threshold
        if mapped is not None:
            mapped_values_when_saturated.append(abs(float(mapped)))
        if applied is not None:
            applied_values_when_saturated.append(abs(float(applied)))
        if low_mapped:
            saturated_low_mapped_count += 1
        if low_applied:
            saturated_low_applied_count += 1
        if len(examples) < 5 and (low_mapped or low_applied):
            examples.append(
                {
                    "row_index": index,
                    "ts_sec": row.get("ts_sec"),
                    "control_header_sequence_num": row.get("control_header_sequence_num"),
                    "apollo_steering_target_pct": raw_pct,
                    "apollo_steer_raw": normalized,
                    "bridge_steer_mapped": mapped,
                    "carla_steer_applied": applied,
                }
            )

    low_mapped_ratio = _safe_div(float(saturated_low_mapped_count), float(saturated_count))
    low_applied_ratio = _safe_div(float(saturated_low_applied_count), float(saturated_count))
    if not raw_targets:
        status = "insufficient_data"
        reason = "apollo_steering_target_missing"
    elif saturated_count > 0 and (low_mapped_ratio or 0.0) >= 0.5:
        status = "warn"
        reason = "raw_steering_target_saturated_but_mapped_steer_low"
    else:
        status = "pass"
        reason = None

    suspected_factors = []
    dominant = None
    if status == "warn":
        dominant = "bridge_steering_normalization_or_scale"
        suspected_factors.append(dominant)

    return {
        "available": bool(raw_targets),
        "status": status,
        "reason": reason,
        "sample_count": len(raw_targets),
        "raw_pct_saturation_threshold": raw_pct_saturation_threshold,
        "low_mapped_abs_threshold": low_mapped_abs_threshold,
        "low_applied_abs_threshold": low_applied_abs_threshold,
        "raw_steering_target_abs_p95": _percentile([abs(value) for value in raw_targets], 0.95),
        "raw_steering_target_abs_max": max((abs(value) for value in raw_targets), default=None),
        "normalized_steer_abs_p95": _percentile([abs(value) for value in normalized_values], 0.95),
        "saturated_raw_steering_target_count": saturated_count,
        "saturated_raw_low_mapped_count": saturated_low_mapped_count,
        "saturated_raw_low_mapped_ratio": low_mapped_ratio,
        "saturated_raw_low_applied_count": saturated_low_applied_count,
        "saturated_raw_low_applied_ratio": low_applied_ratio,
        "mapped_steer_abs_p95_when_raw_saturated": _percentile(
            mapped_values_when_saturated,
            0.95,
        ),
        "applied_steer_abs_p95_when_raw_saturated": _percentile(
            applied_values_when_saturated,
            0.95,
        ),
        "dominant_suspected_factor": dominant,
        "suspected_factors": suspected_factors,
        "examples": examples,
        "interpretation": (
            "This is diagnostic evidence only. A saturated Apollo steering_target with "
            "near-zero mapped/applied steer suggests a steering normalization or scale "
            "candidate for A/B validation, not permission to change steer_scale blindly."
        ),
    }


def _throttle_brake_switch_count(
    rows: Sequence[Mapping[str, Any]],
    *,
    throttle_fields: Sequence[str],
    brake_fields: Sequence[str],
) -> int | None:
    states: list[str] = []
    for row in rows:
        throttle = _first_row_number(row, *throttle_fields)
        brake = _first_row_number(row, *brake_fields)
        if throttle is None and brake is None:
            continue
        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        if throttle_f > 0.05 and brake_f > 0.05:
            state = "conflict"
        elif throttle_f > 0.05:
            state = "throttle"
        elif brake_f > 0.05:
            state = "brake"
        else:
            state = "neutral"
        states.append(state)
    if not states:
        return None
    return sum(1 for previous, current in zip(states, states[1:]) if previous != current)


def _longitudinal_debug_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    fields = (
        "debug_simple_lon_current_speed_mps",
        "debug_simple_lon_speed_reference_mps",
        "debug_simple_lon_speed_error_mps",
        "debug_simple_lon_current_acceleration_mps2",
        "debug_simple_lon_acceleration_reference_mps2",
        "debug_simple_lon_acceleration_error_mps2",
        "debug_simple_lon_acceleration_cmd_mps2",
        "debug_simple_lon_acceleration_cmd_closeloop_mps2",
        "debug_simple_lon_acceleration_lookup_mps2",
        "debug_simple_lon_throttle_cmd_pct",
        "debug_simple_lon_brake_cmd_pct",
        "debug_simple_lon_path_remain_m",
        "debug_simple_lon_station_error_m",
        "debug_simple_lon_preview_station_error_m",
        *tuple(SIMPLE_LON_POINT_FIELDS),
    )
    sample_count = len(rows)
    available_counts: dict[str, int] = {}
    stats: dict[str, Any] = {}
    for field in fields:
        values = [_num(row.get(field)) for row in rows]
        numeric = [float(value) for value in values if value is not None]
        available_counts[field] = len(numeric)
        stats[field] = {
            "sample_count": len(numeric),
            "p50": _percentile(numeric, 0.50),
            "p95": _percentile(numeric, 0.95),
            "p95_abs": _percentile([abs(value) for value in numeric], 0.95),
            "min": min(numeric) if numeric else None,
            "max": max(numeric) if numeric else None,
        }
    full_stop_count = sum(1 for row in rows if _parse_bool(row.get("debug_simple_lon_is_full_stop")) is True)
    required_for_context = (
        "debug_simple_lon_current_speed_mps",
        "debug_simple_lon_speed_reference_mps",
        "debug_simple_lon_speed_error_mps",
        "debug_simple_lon_acceleration_cmd_mps2",
        "debug_simple_lon_throttle_cmd_pct",
        "debug_simple_lon_brake_cmd_pct",
    )
    missing_context_fields = [
        field for field in required_for_context if available_counts.get(field, 0) <= 0
    ]
    return {
        "available": bool(sample_count and not missing_context_fields),
        "sample_count": sample_count,
        "available_counts": available_counts,
        "missing_context_fields": missing_context_fields,
        "full_stop_count": full_stop_count,
        "stats": stats,
    }


def _longitudinal_oscillation_attribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    states: list[tuple[int, Mapping[str, Any], str]] = []
    for index, row in enumerate(rows):
        state = _longitudinal_command_state(row)
        if state in {"throttle", "brake", "coast", "conflict"}:
            states.append((index, row, state))
    state_counts = {
        name: sum(1 for _, _, state in states if state == name)
        for name in ("throttle", "brake", "coast", "conflict")
    }
    transitions: list[tuple[int, Mapping[str, Any], str, int, Mapping[str, Any], str]] = []
    previous: tuple[int, Mapping[str, Any], str] | None = None
    for current in states:
        if previous is not None and current[2] != previous[2]:
            if {previous[2], current[2]}.issubset({"throttle", "brake", "coast"}):
                transitions.append((*previous, *current))
        previous = current

    buckets = {
        "acceleration_cmd_sign_flip": 0,
        "path_remain_large_drop": 0,
        "path_remain_large_jump": 0,
        "station_error_large_drop": 0,
        "station_error_large_jump": 0,
        "speed_reference_large_drop": 0,
        "speed_reference_large_jump": 0,
        "current_speed_large_jump": 0,
        "matched_point_s_large_jump": 0,
        "matched_point_xy_large_jump": 0,
        "reference_point_s_large_jump": 0,
        "reference_point_xy_large_jump": 0,
        "preview_reference_point_s_large_jump": 0,
        "preview_reference_point_xy_large_jump": 0,
        "sequence_gap_gt_one": 0,
    }
    examples: list[dict[str, Any]] = []
    for prev_index, prev_row, prev_state, cur_index, cur_row, cur_state in transitions:
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_path_remain_m",
            drop_key="path_remain_large_drop",
            jump_key="path_remain_large_jump",
            threshold=5.0,
        )
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_station_error_m",
            drop_key="station_error_large_drop",
            jump_key="station_error_large_jump",
            threshold=1.0,
        )
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_speed_reference_mps",
            drop_key="speed_reference_large_drop",
            jump_key="speed_reference_large_jump",
            threshold=0.5,
        )
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_current_speed_mps",
            drop_key="current_speed_large_jump",
            jump_key="current_speed_large_jump",
            threshold=0.5,
        )
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_current_matched_point_s_m",
            drop_key="matched_point_s_large_jump",
            jump_key="matched_point_s_large_jump",
            threshold=2.0,
        )
        _count_xy_large_delta(
            buckets,
            prev_row,
            cur_row,
            x_field="debug_simple_lon_current_matched_point_x",
            y_field="debug_simple_lon_current_matched_point_y",
            key="matched_point_xy_large_jump",
            threshold=2.0,
        )
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_current_reference_point_s_m",
            drop_key="reference_point_s_large_jump",
            jump_key="reference_point_s_large_jump",
            threshold=2.0,
        )
        _count_xy_large_delta(
            buckets,
            prev_row,
            cur_row,
            x_field="debug_simple_lon_current_reference_point_x",
            y_field="debug_simple_lon_current_reference_point_y",
            key="reference_point_xy_large_jump",
            threshold=2.0,
        )
        _count_large_delta(
            buckets,
            prev_row,
            cur_row,
            field="debug_simple_lon_preview_reference_point_s_m",
            drop_key="preview_reference_point_s_large_jump",
            jump_key="preview_reference_point_s_large_jump",
            threshold=2.0,
        )
        _count_xy_large_delta(
            buckets,
            prev_row,
            cur_row,
            x_field="debug_simple_lon_preview_reference_point_x",
            y_field="debug_simple_lon_preview_reference_point_y",
            key="preview_reference_point_xy_large_jump",
            threshold=2.0,
        )
        prev_acc = _num(prev_row.get("debug_simple_lon_acceleration_cmd_mps2"))
        cur_acc = _num(cur_row.get("debug_simple_lon_acceleration_cmd_mps2"))
        if prev_acc is not None and cur_acc is not None and prev_acc * cur_acc < 0:
            buckets["acceleration_cmd_sign_flip"] += 1
        prev_seq = _int(prev_row.get("control_header_sequence_num"))
        cur_seq = _int(cur_row.get("control_header_sequence_num"))
        if prev_seq is not None and cur_seq is not None and cur_seq - prev_seq > 1:
            buckets["sequence_gap_gt_one"] += 1
        if len(examples) < 5:
            examples.append(
                _longitudinal_transition_example(
                    prev_index=prev_index,
                    prev_row=prev_row,
                    prev_state=prev_state,
                    cur_index=cur_index,
                    cur_row=cur_row,
                    cur_state=cur_state,
                )
            )

    transition_count = len(transitions)
    full_stop_count = sum(1 for _, row, _ in states if _parse_bool(row.get("debug_simple_lon_is_full_stop")) is True)
    suspected_factors: list[str] = []
    if transition_count and buckets["acceleration_cmd_sign_flip"] / transition_count >= 0.8:
        suspected_factors.append("apollo_simple_lon_acceleration_cmd_sign_switching")
    path_jump_count = buckets["path_remain_large_drop"] + buckets["path_remain_large_jump"]
    station_jump_count = buckets["station_error_large_drop"] + buckets["station_error_large_jump"]
    if transition_count and (path_jump_count + station_jump_count) / transition_count >= 0.4:
        suspected_factors.append("trajectory_station_or_path_remain_jump")
    matched_reference_jump_count = (
        buckets["matched_point_s_large_jump"]
        + buckets["matched_point_xy_large_jump"]
        + buckets["reference_point_s_large_jump"]
        + buckets["reference_point_xy_large_jump"]
        + buckets["preview_reference_point_s_large_jump"]
        + buckets["preview_reference_point_xy_large_jump"]
    )
    if transition_count and matched_reference_jump_count / transition_count >= 0.25:
        suspected_factors.append("matched_or_reference_point_jump")
    speed_reference_jump_count = buckets["speed_reference_large_drop"] + buckets["speed_reference_large_jump"]
    if transition_count and speed_reference_jump_count / transition_count >= 0.25:
        suspected_factors.append("speed_reference_jump")
    if full_stop_count:
        suspected_factors.append("full_stop_logic_active")

    return {
        "available": bool(rows),
        "sample_count": len(rows),
        "state_counts": state_counts,
        "transition_count": transition_count,
        "transition_buckets": buckets,
        "full_stop_count": full_stop_count,
        "suspected_factors": suspected_factors,
        "dominant_suspected_factor": suspected_factors[0] if suspected_factors else None,
        "interpretation_caveat": (
            "This is attribution evidence for Apollo raw longitudinal command switching; "
            "it does not by itself prove an Apollo algorithm limitation. Check planning "
            "trajectory, reference-line, routing segment, localization, and control debug together."
        ),
        "thresholds": {
            "path_remain_jump_m": 5.0,
            "station_error_jump_m": 1.0,
            "speed_reference_jump_mps": 0.5,
            "current_speed_jump_mps": 0.5,
            "matched_reference_point_s_jump_m": 2.0,
            "matched_reference_point_xy_jump_m": 2.0,
            "acceleration_cmd_sign_flip_ratio_for_factor": 0.8,
        },
        "examples": examples,
    }


def _trajectory_consume_correlation(
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    trajectory_consume_path: Path | None,
) -> dict[str, Any]:
    if trajectory_consume_path is None or not trajectory_consume_path.exists():
        return {"available": False, "missing_inputs": ["control_trajectory_consume_debug.jsonl"]}
    consume_rows: list[Mapping[str, Any]] = []
    malformed_count = 0
    try:
        with trajectory_consume_path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if isinstance(payload, Mapping):
                    consume_rows.append(payload)
                else:
                    malformed_count += 1
    except OSError as exc:
        return {
            "available": False,
            "path": str(trajectory_consume_path),
            "read_error": str(exc),
        }

    transition_indices = _longitudinal_transition_indices(trace_rows)
    aligned_transition_indices = [
        index for index in transition_indices if index < len(trace_rows) and index < len(consume_rows)
    ]
    buckets = {
        "planning_sequence_changed": 0,
        "same_planning_sequence": 0,
        "planning_sequence_gap_gt_one": 0,
        "trajectory_point_count_changed": 0,
        "candidate_source_changed": 0,
        "effective_planning_source_changed": 0,
        "planning_age_drop_gt_50ms": 0,
        "planning_age_after_gt_500ms": 0,
        "trajectory_window_changed": 0,
    }
    examples: list[dict[str, Any]] = []
    for index in aligned_transition_indices:
        prev_consume = consume_rows[index - 1]
        cur_consume = consume_rows[index]
        prev_trace = trace_rows[index - 1]
        cur_trace = trace_rows[index]
        prev_seq = _int(prev_consume.get("planning_header_sequence_num_used"))
        cur_seq = _int(cur_consume.get("planning_header_sequence_num_used"))
        if prev_seq != cur_seq:
            buckets["planning_sequence_changed"] += 1
        else:
            buckets["same_planning_sequence"] += 1
        if prev_seq is not None and cur_seq is not None and cur_seq - prev_seq > 1:
            buckets["planning_sequence_gap_gt_one"] += 1
        if prev_consume.get("latest_planning_trajectory_point_count") != cur_consume.get(
            "latest_planning_trajectory_point_count"
        ):
            buckets["trajectory_point_count_changed"] += 1
        if prev_consume.get("control_input_candidate_source") != cur_consume.get("control_input_candidate_source"):
            buckets["candidate_source_changed"] += 1
        if prev_consume.get("effective_planning_source") != cur_consume.get("effective_planning_source"):
            buckets["effective_planning_source_changed"] += 1
        prev_age = _num(prev_consume.get("latest_planning_msg_age_ms"))
        cur_age = _num(cur_consume.get("latest_planning_msg_age_ms"))
        if prev_age is not None and cur_age is not None and prev_age - cur_age > 50.0:
            buckets["planning_age_drop_gt_50ms"] += 1
        if cur_age is not None and cur_age > 500.0:
            buckets["planning_age_after_gt_500ms"] += 1
        if (
            prev_consume.get("trajectory_first_point_relative_time")
            != cur_consume.get("trajectory_first_point_relative_time")
            or prev_consume.get("trajectory_last_point_relative_time")
            != cur_consume.get("trajectory_last_point_relative_time")
        ):
            buckets["trajectory_window_changed"] += 1
        if len(examples) < 5:
            examples.append(
                {
                    "row_index": [index - 1, index],
                    "state": [
                        _longitudinal_command_state(prev_trace),
                        _longitudinal_command_state(cur_trace),
                    ],
                    "control_sequence_num": [
                        _int(prev_trace.get("control_header_sequence_num")),
                        _int(cur_trace.get("control_header_sequence_num")),
                    ],
                    "planning_sequence_num": [prev_seq, cur_seq],
                    "planning_age_ms": [prev_age, cur_age],
                    "trajectory_point_count": [
                        _int(prev_consume.get("latest_planning_trajectory_point_count")),
                        _int(cur_consume.get("latest_planning_trajectory_point_count")),
                    ],
                    "effective_planning_source": [
                        prev_consume.get("effective_planning_source"),
                        cur_consume.get("effective_planning_source"),
                    ],
                    "trajectory_first_point_relative_time": [
                        _num(prev_consume.get("trajectory_first_point_relative_time")),
                        _num(cur_consume.get("trajectory_first_point_relative_time")),
                    ],
                    "trajectory_last_point_relative_time": [
                        _num(prev_consume.get("trajectory_last_point_relative_time")),
                        _num(cur_consume.get("trajectory_last_point_relative_time")),
                    ],
                    "path_remain_m": [
                        _num(prev_trace.get("debug_simple_lon_path_remain_m")),
                        _num(cur_trace.get("debug_simple_lon_path_remain_m")),
                    ],
                    "station_error_m": [
                        _num(prev_trace.get("debug_simple_lon_station_error_m")),
                        _num(cur_trace.get("debug_simple_lon_station_error_m")),
                    ],
                    "acceleration_cmd_mps2": [
                        _num(prev_trace.get("debug_simple_lon_acceleration_cmd_mps2")),
                        _num(cur_trace.get("debug_simple_lon_acceleration_cmd_mps2")),
                    ],
                }
            )

    transition_count = len(transition_indices)
    aligned_count = len(aligned_transition_indices)
    suspected_factors: list[str] = []
    if aligned_count and buckets["planning_sequence_changed"] / aligned_count >= 0.5:
        suspected_factors.append("planning_sequence_update_correlates_with_switches")
    if aligned_count and buckets["same_planning_sequence"] > 0:
        suspected_factors.append("intra_trajectory_longitudinal_switches_present")
    if aligned_count and buckets["effective_planning_source_changed"] / aligned_count >= 0.25:
        suspected_factors.append("effective_planning_source_switching")
    return {
        "available": bool(consume_rows),
        "path": str(trajectory_consume_path),
        "trace_row_count": len(trace_rows),
        "consume_row_count": len(consume_rows),
        "aligned_row_count": min(len(trace_rows), len(consume_rows)),
        "malformed_line_count": malformed_count,
        "transition_count": transition_count,
        "aligned_transition_count": aligned_count,
        "transition_buckets": buckets,
        "suspected_factors": suspected_factors,
        "dominant_suspected_factor": suspected_factors[0] if suspected_factors else None,
        "examples": examples,
    }


def _planning_trajectory_correlation(
    trace_rows: Sequence[Mapping[str, Any]],
    *,
    trajectory_consume_path: Path | None,
    planning_topic_debug_path: Path | None,
    planning_route_segment_debug_path: Path | None,
) -> dict[str, Any]:
    missing_inputs: list[str] = []
    if trajectory_consume_path is None or not trajectory_consume_path.exists():
        missing_inputs.append("control_trajectory_consume_debug.jsonl")
    if planning_topic_debug_path is None or not planning_topic_debug_path.exists():
        missing_inputs.append("planning_topic_debug.jsonl")
    if missing_inputs:
        return {"available": False, "missing_inputs": missing_inputs}

    consume_rows, consume_malformed = _read_jsonl_mappings(trajectory_consume_path)
    planning_rows, planning_malformed = _read_jsonl_mappings(planning_topic_debug_path)
    route_rows: list[Mapping[str, Any]] = []
    route_malformed = 0
    if planning_route_segment_debug_path is not None and planning_route_segment_debug_path.exists():
        route_rows, route_malformed = _read_jsonl_mappings(planning_route_segment_debug_path)

    planning_by_seq = _index_by_sequence(planning_rows, "planning_header_sequence_num")
    route_by_seq = _index_by_sequence(route_rows, "planning_header_sequence_num")
    transition_indices = _longitudinal_transition_indices(trace_rows)
    aligned_transition_indices = [
        index
        for index in transition_indices
        if 0 < index < len(trace_rows) and index < len(consume_rows)
    ]
    buckets = {
        "planning_sequence_changed": 0,
        "same_planning_sequence": 0,
        "planning_debug_missing_for_sequence": 0,
        "planning_debug_exact_pair_found": 0,
        "trajectory_point_count_changed": 0,
        "trajectory_path_length_delta_gt_5m": 0,
        "trajectory_total_time_delta_gt_1s": 0,
        "trajectory_relative_window_changed": 0,
        "trajectory_first_point_jump_gt_5m": 0,
        "trajectory_last_point_jump_gt_5m": 0,
        "trajectory_type_changed": 0,
        "speed_fallback_involved": 0,
        "scenario_or_stage_changed": 0,
        "route_signature_changed": 0,
        "reference_line_count_changed": 0,
        "route_segment_status_changed": 0,
    }
    examples: list[dict[str, Any]] = []
    for index in aligned_transition_indices:
        prev_trace = trace_rows[index - 1]
        cur_trace = trace_rows[index]
        prev_consume = consume_rows[index - 1]
        cur_consume = consume_rows[index]
        prev_seq = _int(prev_consume.get("planning_header_sequence_num_used"))
        cur_seq = _int(cur_consume.get("planning_header_sequence_num_used"))
        if prev_seq != cur_seq:
            buckets["planning_sequence_changed"] += 1
        else:
            buckets["same_planning_sequence"] += 1

        prev_planning = planning_by_seq.get(prev_seq)
        cur_planning = planning_by_seq.get(cur_seq)
        if prev_planning is None or cur_planning is None:
            buckets["planning_debug_missing_for_sequence"] += 1
        else:
            buckets["planning_debug_exact_pair_found"] += 1
            _compare_planning_trajectory_transition(
                buckets,
                prev_planning=prev_planning,
                cur_planning=cur_planning,
            )

        prev_route = route_by_seq.get(prev_seq)
        cur_route = route_by_seq.get(cur_seq)
        if prev_route is not None and cur_route is not None:
            if (
                prev_route.get("reference_line_provider_status")
                != cur_route.get("reference_line_provider_status")
                or prev_route.get("lane_follow_map_status")
                != cur_route.get("lane_follow_map_status")
                or prev_route.get("create_route_segments_status")
                != cur_route.get("create_route_segments_status")
            ):
                buckets["route_segment_status_changed"] += 1

        if len(examples) < 5:
            examples.append(
                _planning_trajectory_transition_example(
                    index=index,
                    prev_trace=prev_trace,
                    cur_trace=cur_trace,
                    prev_consume=prev_consume,
                    cur_consume=cur_consume,
                    prev_planning=prev_planning,
                    cur_planning=cur_planning,
                    prev_route=prev_route,
                    cur_route=cur_route,
                )
            )

    transition_count = len(transition_indices)
    aligned_count = len(aligned_transition_indices)
    exact_pair_count = int(buckets["planning_debug_exact_pair_found"])
    exact_pair_coverage_ratio = _safe_div(exact_pair_count, aligned_count)
    missing_transition_ratio = _safe_div(
        int(buckets["planning_debug_missing_for_sequence"]),
        aligned_count,
    )
    suspected_factors: list[str] = []
    if aligned_count and (exact_pair_coverage_ratio or 0.0) < 0.5:
        suspected_factors.append("planning_debug_sequence_coverage_low")
    if aligned_count and buckets["trajectory_path_length_delta_gt_5m"] / aligned_count >= 0.4:
        suspected_factors.append("planning_trajectory_length_switching")
    if aligned_count and buckets["trajectory_last_point_jump_gt_5m"] / aligned_count >= 0.4:
        suspected_factors.append("planning_trajectory_endpoint_jump")
    if aligned_count and buckets["speed_fallback_involved"] / aligned_count >= 0.25:
        suspected_factors.append("planning_speed_fallback_involved")
    if aligned_count and buckets["same_planning_sequence"] > 0:
        suspected_factors.append("same_trajectory_longitudinal_control_switching")
    if aligned_count and buckets["route_segment_status_changed"] / aligned_count >= 0.25:
        suspected_factors.append("route_segment_status_switching")

    return {
        "available": bool(consume_rows and planning_rows),
        "paths": {
            "control_trajectory_consume_debug": str(trajectory_consume_path),
            "planning_topic_debug": str(planning_topic_debug_path),
            "planning_route_segment_debug": (
                str(planning_route_segment_debug_path)
                if planning_route_segment_debug_path
                else None
            ),
        },
        "trace_row_count": len(trace_rows),
        "consume_row_count": len(consume_rows),
        "planning_row_count": len(planning_rows),
        "route_segment_row_count": len(route_rows),
        "malformed_line_count": consume_malformed + planning_malformed + route_malformed,
        "transition_count": transition_count,
        "aligned_transition_count": aligned_count,
        "planning_debug_exact_pair_count": exact_pair_count,
        "planning_debug_exact_pair_coverage_ratio": exact_pair_coverage_ratio,
        "planning_debug_missing_transition_ratio": missing_transition_ratio,
        "transition_buckets": buckets,
        "transition_window_summary": _planning_transition_window_summary(
            trace_rows=trace_rows,
            consume_rows=consume_rows,
            planning_by_seq=planning_by_seq,
            route_by_seq=route_by_seq,
            transition_indices=transition_indices,
            aligned_transition_indices=aligned_transition_indices,
        ),
        "suspected_factors": suspected_factors,
        "dominant_suspected_factor": suspected_factors[0] if suspected_factors else None,
        "interpretation_caveat": (
            "This correlates Apollo raw longitudinal command switches with planning "
            "trajectory debug rows. It is not a standalone root-cause proof; inspect "
            "localization, HDMap projection, reference-line contract, and control debug together."
        ),
        "thresholds": {
            "trajectory_path_length_delta_m": 5.0,
            "trajectory_point_jump_m": 5.0,
            "trajectory_total_time_delta_s": 1.0,
        },
        "examples": examples,
    }


def _planning_transition_window_summary(
    *,
    trace_rows: Sequence[Mapping[str, Any]],
    consume_rows: Sequence[Mapping[str, Any]],
    planning_by_seq: Mapping[int, Mapping[str, Any]],
    route_by_seq: Mapping[int, Mapping[str, Any]],
    transition_indices: Sequence[int],
    aligned_transition_indices: Sequence[int],
) -> dict[str, Any]:
    windows: list[dict[str, Any]] = []
    planning_sequence_counts: dict[str, int] = {}
    same_sequence_count = 0
    changed_sequence_count = 0
    speed_fallback_count = 0
    speed_reference_deltas: list[float] = []
    acceleration_cmd_deltas: list[float] = []
    path_remain_deltas: list[float] = []
    planning_ages: list[float] = []

    for index in aligned_transition_indices:
        if not (0 < index < len(trace_rows) and index < len(consume_rows)):
            continue
        prev_trace = trace_rows[index - 1]
        cur_trace = trace_rows[index]
        prev_consume = consume_rows[index - 1]
        cur_consume = consume_rows[index]
        prev_seq = _int(prev_consume.get("planning_header_sequence_num_used"))
        cur_seq = _int(cur_consume.get("planning_header_sequence_num_used"))
        if prev_seq == cur_seq:
            same_sequence_count += 1
        else:
            changed_sequence_count += 1
        if cur_seq is not None:
            key = str(cur_seq)
            planning_sequence_counts[key] = planning_sequence_counts.get(key, 0) + 1
        cur_planning = planning_by_seq.get(cur_seq) if cur_seq is not None else None
        cur_route = route_by_seq.get(cur_seq) if cur_seq is not None else None
        if isinstance(cur_planning, Mapping) and cur_planning.get("trajectory_type") == "SPEED_FALLBACK":
            speed_fallback_count += 1
        if (
            delta := _delta(
                prev_trace.get("debug_simple_lon_speed_reference_mps"),
                cur_trace.get("debug_simple_lon_speed_reference_mps"),
            )
        ) is not None:
            speed_reference_deltas.append(delta)
        if (
            delta := _delta(
                prev_trace.get("debug_simple_lon_acceleration_cmd_mps2"),
                cur_trace.get("debug_simple_lon_acceleration_cmd_mps2"),
            )
        ) is not None:
            acceleration_cmd_deltas.append(delta)
        if (
            delta := _delta(
                prev_trace.get("debug_simple_lon_path_remain_m"),
                cur_trace.get("debug_simple_lon_path_remain_m"),
            )
        ) is not None:
            path_remain_deltas.append(delta)
        if (age := _num(cur_consume.get("latest_planning_msg_age_ms"))) is not None:
            planning_ages.append(age)
        if len(windows) < 8:
            windows.append(
                {
                    "row_index": [index - 1, index],
                    "state": [
                        _longitudinal_command_state(prev_trace),
                        _longitudinal_command_state(cur_trace),
                    ],
                    "trace_ts_sec": [
                        _num(prev_trace.get("ts_sec")),
                        _num(cur_trace.get("ts_sec")),
                    ],
                    "control_sequence_num": [
                        _int(prev_trace.get("control_header_sequence_num")),
                        _int(cur_trace.get("control_header_sequence_num")),
                    ],
                    "planning_sequence_num": [prev_seq, cur_seq],
                    "same_planning_sequence": prev_seq == cur_seq,
                    "effective_planning_source": [
                        prev_consume.get("effective_planning_source"),
                        cur_consume.get("effective_planning_source"),
                    ],
                    "planning_age_ms": [
                        _num(prev_consume.get("latest_planning_msg_age_ms")),
                        _num(cur_consume.get("latest_planning_msg_age_ms")),
                    ],
                    "trajectory_type": (
                        cur_planning.get("trajectory_type")
                        if isinstance(cur_planning, Mapping)
                        else None
                    ),
                    "route_segment_status": {
                        "reference_line_provider_status": (
                            cur_route.get("reference_line_provider_status")
                            if isinstance(cur_route, Mapping)
                            else None
                        ),
                        "lane_follow_map_status": (
                            cur_route.get("lane_follow_map_status")
                            if isinstance(cur_route, Mapping)
                            else None
                        ),
                        "create_route_segments_status": (
                            cur_route.get("create_route_segments_status")
                            if isinstance(cur_route, Mapping)
                            else None
                        ),
                    },
                    "speed_reference_mps": [
                        _num(prev_trace.get("debug_simple_lon_speed_reference_mps")),
                        _num(cur_trace.get("debug_simple_lon_speed_reference_mps")),
                    ],
                    "acceleration_cmd_mps2": [
                        _num(prev_trace.get("debug_simple_lon_acceleration_cmd_mps2")),
                        _num(cur_trace.get("debug_simple_lon_acceleration_cmd_mps2")),
                    ],
                    "path_remain_m": [
                        _num(prev_trace.get("debug_simple_lon_path_remain_m")),
                        _num(cur_trace.get("debug_simple_lon_path_remain_m")),
                    ],
                    "station_error_m": [
                        _num(prev_trace.get("debug_simple_lon_station_error_m")),
                        _num(cur_trace.get("debug_simple_lon_station_error_m")),
                    ],
                }
            )

    aligned_count = len(aligned_transition_indices)
    same_ratio = _safe_div(same_sequence_count, aligned_count)
    changed_ratio = _safe_div(changed_sequence_count, aligned_count)
    if same_ratio is not None and same_ratio >= 0.75:
        dominant_mode = "same_planning_sequence"
    elif changed_ratio is not None and changed_ratio >= 0.75:
        dominant_mode = "planning_sequence_update"
    elif aligned_count:
        dominant_mode = "mixed"
    else:
        dominant_mode = "insufficient_data"
    top_sequences = [
        {"planning_sequence_num": key, "transition_count": count}
        for key, count in sorted(
            planning_sequence_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:8]
    ]
    return {
        "available": bool(aligned_count),
        "transition_count": len(transition_indices),
        "aligned_transition_count": aligned_count,
        "same_planning_sequence_count": same_sequence_count,
        "planning_sequence_changed_count": changed_sequence_count,
        "same_planning_sequence_ratio": same_ratio,
        "planning_sequence_changed_ratio": changed_ratio,
        "dominant_transition_mode": dominant_mode,
        "speed_fallback_transition_count": speed_fallback_count,
        "top_planning_sequences": top_sequences,
        "planning_age_p95_ms": _percentile(planning_ages, 0.95),
        "speed_reference_delta_p95_abs_mps": _percentile(
            [abs(item) for item in speed_reference_deltas],
            0.95,
        ),
        "acceleration_cmd_delta_p95_abs_mps2": _percentile(
            [abs(item) for item in acceleration_cmd_deltas],
            0.95,
        ),
        "path_remain_delta_p95_abs_m": _percentile(
            [abs(item) for item in path_remain_deltas],
            0.95,
        ),
        "sample_windows": windows,
        "interpretation_caveat": (
            "Transition windows are row-level attribution evidence. Same-planning-sequence "
            "switching narrows the next debug target to Apollo Control/simple_lon behavior "
            "on a consumed trajectory; it does not prove a standalone algorithm root cause."
        ),
    }


def _planning_log_fallback_diagnostics(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False, "missing_inputs": ["apollo_planning.INFO"]}
    patterns = {
        "reference_line_latest_history_fail_count": "Failed to use reference line latest history",
        "control_interactive_timeout_count": "control_interactive_msg time out",
        "large_station_error_replan_count": "replan to avoid large station error",
        "matched_point_lon_diff_replan_count": "the distance between matched point and actual position is too large",
        "piecewise_jerk_speed_optimizer_fail_count": "Piecewise jerk speed optimizer failed",
        "lane_follow_piecewise_jerk_task_fail_count": "Failed to run tasks[PIECEWISE_JERK_SPEED]",
        "speed_fallback_due_to_algorithm_failure_count": "Speed fallback due to algorithm failure",
        "fallback_using_piecewise_jerk_speed_count": "Fallback using piecewise jerk speed",
    }
    counts = {key: 0 for key in patterns}
    lon_diffs: list[float] = []
    examples: list[dict[str, Any]] = []
    line_count = 0
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line_number, line in enumerate(handle, start=1):
                line_count += 1
                matched_any = False
                for key, pattern in patterns.items():
                    if pattern in line:
                        counts[key] += 1
                        matched_any = True
                lon_match = re.search(r"lon_diff\s*=\s*([-+]?[0-9]*\.?[0-9]+)", line)
                lon_diff = _num(lon_match.group(1)) if lon_match else None
                if lon_diff is not None:
                    lon_diffs.append(lon_diff)
                    matched_any = True
                if matched_any and len(examples) < 8:
                    examples.append(
                        {
                            "line_number": line_number,
                            "lon_diff": lon_diff,
                            "message": line.strip()[:280],
                        }
                    )
    except OSError as exc:
        return {
            "available": False,
            "path": str(path),
            "read_error": str(exc),
        }

    suspected_factors: list[str] = []
    if counts["matched_point_lon_diff_replan_count"]:
        suspected_factors.append("trajectory_stitcher_matched_point_lon_diff_replans")
    if counts["piecewise_jerk_speed_optimizer_fail_count"]:
        suspected_factors.append("piecewise_jerk_speed_optimizer_failures")
    if counts["speed_fallback_due_to_algorithm_failure_count"]:
        suspected_factors.append("planning_speed_fallback_due_to_algorithm_failure")
    if counts["control_interactive_timeout_count"]:
        suspected_factors.append("planning_control_interactive_timeout")

    return {
        "available": True,
        "path": str(path),
        "line_count": line_count,
        **counts,
        "matched_point_lon_diff_sample_count": len(lon_diffs),
        "matched_point_lon_diff_p95_m": _percentile(lon_diffs, 0.95),
        "matched_point_lon_diff_max_m": max(lon_diffs) if lon_diffs else None,
        "suspected_factors": suspected_factors,
        "dominant_suspected_factor": suspected_factors[0] if suspected_factors else None,
        "examples": examples,
        "interpretation_caveat": (
            "Planning log fallback diagnostics identify Apollo planning/stitcher messages "
            "that correlate with control oscillation. They do not prove actuator mapping "
            "or Apollo algorithm fault without trajectory, localization, and HDMap evidence."
        ),
    }


def _compare_planning_trajectory_transition(
    buckets: dict[str, int],
    *,
    prev_planning: Mapping[str, Any],
    cur_planning: Mapping[str, Any],
) -> None:
    if prev_planning.get("trajectory_point_count") != cur_planning.get("trajectory_point_count"):
        buckets["trajectory_point_count_changed"] += 1
    if _abs_delta(prev_planning, cur_planning, "trajectory_total_path_length") > 5.0:
        buckets["trajectory_path_length_delta_gt_5m"] += 1
    if _abs_delta(prev_planning, cur_planning, "trajectory_total_time") > 1.0:
        buckets["trajectory_total_time_delta_gt_1s"] += 1
    if (
        prev_planning.get("trajectory_relative_time_min_sec")
        != cur_planning.get("trajectory_relative_time_min_sec")
        or prev_planning.get("trajectory_relative_time_max_sec")
        != cur_planning.get("trajectory_relative_time_max_sec")
    ):
        buckets["trajectory_relative_window_changed"] += 1
    first_jump = _xy_distance(
        prev_planning,
        cur_planning,
        left_x="first_trajectory_point_x",
        left_y="first_trajectory_point_y",
        right_x="first_trajectory_point_x",
        right_y="first_trajectory_point_y",
    )
    if first_jump is not None and first_jump > 5.0:
        buckets["trajectory_first_point_jump_gt_5m"] += 1
    last_jump = _xy_distance(
        prev_planning,
        cur_planning,
        left_x="last_trajectory_point_x",
        left_y="last_trajectory_point_y",
        right_x="last_trajectory_point_x",
        right_y="last_trajectory_point_y",
    )
    if last_jump is not None and last_jump > 5.0:
        buckets["trajectory_last_point_jump_gt_5m"] += 1
    if prev_planning.get("trajectory_type") != cur_planning.get("trajectory_type"):
        buckets["trajectory_type_changed"] += 1
    if "SPEED_FALLBACK" in {
        str(prev_planning.get("trajectory_type") or ""),
        str(cur_planning.get("trajectory_type") or ""),
    }:
        buckets["speed_fallback_involved"] += 1
    if (
        prev_planning.get("scenario_plugin_type") != cur_planning.get("scenario_plugin_type")
        or prev_planning.get("stage_plugin_type") != cur_planning.get("stage_plugin_type")
    ):
        buckets["scenario_or_stage_changed"] += 1
    if prev_planning.get("routing_lane_window_signature") != cur_planning.get(
        "routing_lane_window_signature"
    ):
        buckets["route_signature_changed"] += 1
    if prev_planning.get("reference_line_count") != cur_planning.get("reference_line_count"):
        buckets["reference_line_count_changed"] += 1


def _planning_trajectory_transition_example(
    *,
    index: int,
    prev_trace: Mapping[str, Any],
    cur_trace: Mapping[str, Any],
    prev_consume: Mapping[str, Any],
    cur_consume: Mapping[str, Any],
    prev_planning: Mapping[str, Any] | None,
    cur_planning: Mapping[str, Any] | None,
    prev_route: Mapping[str, Any] | None,
    cur_route: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "row_index": [index - 1, index],
        "state": [_longitudinal_command_state(prev_trace), _longitudinal_command_state(cur_trace)],
        "planning_sequence_num": [
            _int(prev_consume.get("planning_header_sequence_num_used")),
            _int(cur_consume.get("planning_header_sequence_num_used")),
        ],
        "effective_planning_source": [
            prev_consume.get("effective_planning_source"),
            cur_consume.get("effective_planning_source"),
        ],
        "planning_age_ms": [
            _num(prev_consume.get("latest_planning_msg_age_ms")),
            _num(cur_consume.get("latest_planning_msg_age_ms")),
        ],
        "path_remain_m": [
            _num(prev_trace.get("debug_simple_lon_path_remain_m")),
            _num(cur_trace.get("debug_simple_lon_path_remain_m")),
        ],
        "speed_reference_mps": [
            _num(prev_trace.get("debug_simple_lon_speed_reference_mps")),
            _num(cur_trace.get("debug_simple_lon_speed_reference_mps")),
        ],
        "acceleration_cmd_mps2": [
            _num(prev_trace.get("debug_simple_lon_acceleration_cmd_mps2")),
            _num(cur_trace.get("debug_simple_lon_acceleration_cmd_mps2")),
        ],
        "planning_trajectory": _planning_transition_payload(prev_planning, cur_planning),
        "route_segment_status": _route_segment_transition_payload(prev_route, cur_route),
    }


def _planning_transition_payload(
    prev_planning: Mapping[str, Any] | None,
    cur_planning: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if prev_planning is None or cur_planning is None:
        return None
    return {
        "trajectory_point_count": [
            _int(prev_planning.get("trajectory_point_count")),
            _int(cur_planning.get("trajectory_point_count")),
        ],
        "trajectory_total_path_length_m": [
            _num(prev_planning.get("trajectory_total_path_length")),
            _num(cur_planning.get("trajectory_total_path_length")),
        ],
        "trajectory_total_time_s": [
            _num(prev_planning.get("trajectory_total_time")),
            _num(cur_planning.get("trajectory_total_time")),
        ],
        "trajectory_relative_time_window_s": [
            [
                _num(prev_planning.get("trajectory_relative_time_min_sec")),
                _num(prev_planning.get("trajectory_relative_time_max_sec")),
            ],
            [
                _num(cur_planning.get("trajectory_relative_time_min_sec")),
                _num(cur_planning.get("trajectory_relative_time_max_sec")),
            ],
        ],
        "first_point_jump_m": _xy_distance(
            prev_planning,
            cur_planning,
            left_x="first_trajectory_point_x",
            left_y="first_trajectory_point_y",
            right_x="first_trajectory_point_x",
            right_y="first_trajectory_point_y",
        ),
        "last_point_jump_m": _xy_distance(
            prev_planning,
            cur_planning,
            left_x="last_trajectory_point_x",
            left_y="last_trajectory_point_y",
            right_x="last_trajectory_point_x",
            right_y="last_trajectory_point_y",
        ),
        "first_trajectory_point_v_mps": [
            _num(prev_planning.get("first_trajectory_point_v")),
            _num(cur_planning.get("first_trajectory_point_v")),
        ],
        "trajectory_type": [
            prev_planning.get("trajectory_type"),
            cur_planning.get("trajectory_type"),
        ],
        "scenario_plugin_type": [
            prev_planning.get("scenario_plugin_type"),
            cur_planning.get("scenario_plugin_type"),
        ],
        "stage_plugin_type": [
            prev_planning.get("stage_plugin_type"),
            cur_planning.get("stage_plugin_type"),
        ],
        "reference_line_count": [
            _int(prev_planning.get("reference_line_count")),
            _int(cur_planning.get("reference_line_count")),
        ],
        "routing_lane_window_signature": [
            prev_planning.get("routing_lane_window_signature"),
            cur_planning.get("routing_lane_window_signature"),
        ],
    }


def _route_segment_transition_payload(
    prev_route: Mapping[str, Any] | None,
    cur_route: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if prev_route is None or cur_route is None:
        return None
    return {
        "reference_line_provider_status": [
            prev_route.get("reference_line_provider_status"),
            cur_route.get("reference_line_provider_status"),
        ],
        "lane_follow_map_status": [
            prev_route.get("lane_follow_map_status"),
            cur_route.get("lane_follow_map_status"),
        ],
        "create_route_segments_status": [
            prev_route.get("create_route_segments_status"),
            cur_route.get("create_route_segments_status"),
        ],
        "planning_empty_reason_guess": [
            prev_route.get("planning_empty_reason_guess"),
            cur_route.get("planning_empty_reason_guess"),
        ],
    }


def _read_jsonl_mappings(path: Path) -> tuple[list[Mapping[str, Any]], int]:
    rows: list[Mapping[str, Any]] = []
    malformed_count = 0
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if isinstance(payload, Mapping):
                    rows.append(payload)
                else:
                    malformed_count += 1
    except OSError:
        return [], 0
    return rows, malformed_count


def _index_by_sequence(rows: Sequence[Mapping[str, Any]], sequence_field: str) -> dict[int, Mapping[str, Any]]:
    indexed: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        sequence = _int(row.get(sequence_field))
        if sequence is not None:
            indexed[sequence] = row
    return indexed


def _abs_delta(left: Mapping[str, Any], right: Mapping[str, Any], field: str) -> float:
    left_value = _num(left.get(field))
    right_value = _num(right.get(field))
    if left_value is None or right_value is None:
        return 0.0
    return abs(right_value - left_value)


def _xy_distance(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    left_x: str,
    left_y: str,
    right_x: str,
    right_y: str,
) -> float | None:
    lx = _num(left.get(left_x))
    ly = _num(left.get(left_y))
    rx = _num(right.get(right_x))
    ry = _num(right.get(right_y))
    if lx is None or ly is None or rx is None or ry is None:
        return None
    return math.hypot(rx - lx, ry - ly)


def _longitudinal_transition_indices(rows: Sequence[Mapping[str, Any]]) -> list[int]:
    transitions: list[int] = []
    previous_state: str | None = None
    for index, row in enumerate(rows):
        state = _longitudinal_command_state(row)
        if state not in {"throttle", "brake", "coast"}:
            continue
        if previous_state is not None and state != previous_state:
            transitions.append(index)
        previous_state = state
    return transitions


def _longitudinal_command_state(row: Mapping[str, Any]) -> str | None:
    throttle = _first_row_number(row, "throttle_raw")
    brake = _first_row_number(row, "brake_raw")
    throttle_active = throttle is not None and throttle > 0.05
    brake_active = brake is not None and brake > 0.05
    if throttle is None and brake is None:
        throttle_pct = _num(row.get("debug_simple_lon_throttle_cmd_pct"))
        brake_pct = _num(row.get("debug_simple_lon_brake_cmd_pct"))
        throttle_active = throttle_pct is not None and throttle_pct > 1.0
        brake_active = brake_pct is not None and brake_pct > 1.0
        if throttle_pct is None and brake_pct is None:
            return None
    if throttle_active and brake_active:
        return "conflict"
    if throttle_active:
        return "throttle"
    if brake_active:
        return "brake"
    return "coast"


def _count_large_delta(
    buckets: dict[str, int],
    prev_row: Mapping[str, Any],
    cur_row: Mapping[str, Any],
    *,
    field: str,
    drop_key: str,
    jump_key: str,
    threshold: float,
) -> None:
    prev_value = _num(prev_row.get(field))
    cur_value = _num(cur_row.get(field))
    if prev_value is None or cur_value is None:
        return
    delta = cur_value - prev_value
    if delta <= -threshold:
        buckets[drop_key] += 1
    elif delta >= threshold:
        buckets[jump_key] += 1


def _count_xy_large_delta(
    buckets: dict[str, int],
    prev_row: Mapping[str, Any],
    cur_row: Mapping[str, Any],
    *,
    x_field: str,
    y_field: str,
    key: str,
    threshold: float,
) -> None:
    distance = _xy_distance(
        prev_row,
        cur_row,
        left_x=x_field,
        left_y=y_field,
        right_x=x_field,
        right_y=y_field,
    )
    if distance is not None and distance >= threshold:
        buckets[key] += 1


def _longitudinal_transition_example(
    *,
    prev_index: int,
    prev_row: Mapping[str, Any],
    prev_state: str,
    cur_index: int,
    cur_row: Mapping[str, Any],
    cur_state: str,
) -> dict[str, Any]:
    return {
        "row_index": [prev_index, cur_index],
        "state": [prev_state, cur_state],
        "dt_s": _delta(prev_row.get("ts_sec"), cur_row.get("ts_sec")),
        "sequence_num": [
            _int(prev_row.get("control_header_sequence_num")),
            _int(cur_row.get("control_header_sequence_num")),
        ],
        "path_remain_m": [
            _num(prev_row.get("debug_simple_lon_path_remain_m")),
            _num(cur_row.get("debug_simple_lon_path_remain_m")),
        ],
        "station_error_m": [
            _num(prev_row.get("debug_simple_lon_station_error_m")),
            _num(cur_row.get("debug_simple_lon_station_error_m")),
        ],
        "speed_reference_mps": [
            _num(prev_row.get("debug_simple_lon_speed_reference_mps")),
            _num(cur_row.get("debug_simple_lon_speed_reference_mps")),
        ],
        "current_speed_mps": [
            _num(prev_row.get("debug_simple_lon_current_speed_mps")),
            _num(cur_row.get("debug_simple_lon_current_speed_mps")),
        ],
        "speed_error_mps": [
            _num(prev_row.get("debug_simple_lon_speed_error_mps")),
            _num(cur_row.get("debug_simple_lon_speed_error_mps")),
        ],
        "acceleration_cmd_mps2": [
            _num(prev_row.get("debug_simple_lon_acceleration_cmd_mps2")),
            _num(cur_row.get("debug_simple_lon_acceleration_cmd_mps2")),
        ],
        "throttle_cmd_pct": [
            _num(prev_row.get("debug_simple_lon_throttle_cmd_pct")),
            _num(cur_row.get("debug_simple_lon_throttle_cmd_pct")),
        ],
        "brake_cmd_pct": [
            _num(prev_row.get("debug_simple_lon_brake_cmd_pct")),
            _num(cur_row.get("debug_simple_lon_brake_cmd_pct")),
        ],
        "matched_point": _point_pair(
            prev_row,
            cur_row,
            prefix="debug_simple_lon_current_matched_point",
        ),
        "current_reference_point": _point_pair(
            prev_row,
            cur_row,
            prefix="debug_simple_lon_current_reference_point",
        ),
        "preview_reference_point": _point_pair(
            prev_row,
            cur_row,
            prefix="debug_simple_lon_preview_reference_point",
        ),
        "is_full_stop": [
            _parse_bool(prev_row.get("debug_simple_lon_is_full_stop")),
            _parse_bool(cur_row.get("debug_simple_lon_is_full_stop")),
        ],
    }


def _point_pair(
    prev_row: Mapping[str, Any],
    cur_row: Mapping[str, Any],
    *,
    prefix: str,
) -> dict[str, Any]:
    return {
        "s_m": [
            _num(prev_row.get(f"{prefix}_s_m")),
            _num(cur_row.get(f"{prefix}_s_m")),
        ],
        "x": [
            _num(prev_row.get(f"{prefix}_x")),
            _num(cur_row.get(f"{prefix}_x")),
        ],
        "y": [
            _num(prev_row.get(f"{prefix}_y")),
            _num(cur_row.get(f"{prefix}_y")),
        ],
        "theta_rad": [
            _num(prev_row.get(f"{prefix}_theta_rad")),
            _num(cur_row.get(f"{prefix}_theta_rad")),
        ],
        "kappa": [
            _num(prev_row.get(f"{prefix}_kappa")),
            _num(cur_row.get(f"{prefix}_kappa")),
        ],
    }


def _delta(left: Any, right: Any) -> float | None:
    left_num = _num(left)
    right_num = _num(right)
    if left_num is None or right_num is None:
        return None
    return right_num - left_num


def _nested_raw(payload: Mapping[str, Any], path: Sequence[str]) -> Any:
    current: Any = payload
    for item in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(item)
    return current


def _percent_to_unit(value: Any) -> float | None:
    number = _num(value)
    if number is None:
        return None
    return number / 100.0


_CONTROL_LATENCY_PATHS = (
    ("output_to_carla", "control_latency_ms"),
    ("parsed_control", "control_latency_ms"),
    ("control_latency_ms",),
)
_CONTROL_MESSAGE_AGE_PATHS = (
    ("output_to_carla", "control_message_age_ms"),
    ("parsed_control", "control_message_age_ms"),
    ("control_message_age_ms",),
)
_PLANNING_MESSAGE_AGE_PATHS = (
    ("output_to_carla", "planning_message_age_ms"),
    ("planning_message_age_ms",),
)


def _nested_first_number(
    payload: Mapping[str, Any],
    paths: Sequence[Sequence[str]],
) -> float | None:
    for path in paths:
        current: Any = payload
        for key in path:
            if not isinstance(current, Mapping):
                current = None
                break
            current = current.get(key)
        value = _num(current)
        if value is not None:
            return value
    return None


def _first_event_with_source(
    events: Sequence[Mapping[str, Any]],
    source: str,
) -> Mapping[str, Any] | None:
    for event in events:
        if event.get("source") == source:
            return event
    return None


def _first_event_with_nonzero_throttle(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for event in events:
        throttle = _num(event.get("throttle"))
        if throttle is not None and throttle > 0.05:
            return event
    return None


def _last_rx_change_event(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    previous_rx: int | None = None
    last_change: Mapping[str, Any] | None = None
    for event in events:
        rx = _int(event.get("rx"))
        if rx is None:
            continue
        if previous_rx is None or rx != previous_rx:
            last_change = event
            previous_rx = rx
    return last_change


def _event_num(event: Mapping[str, Any] | None, key: str) -> float | None:
    if event is None:
        return None
    return _num(event.get(key))


def _event_int(event: Mapping[str, Any] | None, key: str) -> int | None:
    if event is None:
        return None
    return _int(event.get(key))


def _max_event_int(events: Sequence[Mapping[str, Any]], key: str) -> int | None:
    values = [value for event in events if (value := _int(event.get(key))) is not None]
    return max(values) if values else None


def _span(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    return end - start


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return numerator / denominator


def _nested(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _int(value: Any) -> int | None:
    number = _num(value)
    if number is None:
        return None
    return int(number)


def _mean(values: Sequence[float]) -> float | None:
    cleaned = [value for value in values if math.isfinite(float(value))]
    return sum(cleaned) / len(cleaned) if cleaned else None


def _stopped_ratio(values: Sequence[float], *, threshold_mps: float = 0.2) -> float | None:
    cleaned = [abs(value) for value in values if math.isfinite(float(value))]
    if not cleaned:
        return None
    return sum(1 for value in cleaned if value < threshold_mps) / len(cleaned)


def _row_has_nonzero_control(
    row: Mapping[str, Any],
    *,
    throttle_field: str,
    brake_field: str,
    steer_field: str,
) -> bool | None:
    throttle = _num(row.get(throttle_field))
    brake = _num(row.get(brake_field))
    steer = _num(row.get(steer_field))
    if throttle is None and brake is None and steer is None:
        return None
    return (
        (throttle is not None and throttle > 0.05)
        or (brake is not None and brake > 0.05)
        or (steer is not None and abs(steer) > 0.001)
    )


def _row_time_s(row: Mapping[str, Any]) -> float | None:
    for field in ("sim_time", "time_s", "wall_time_s"):
        value = _num(row.get(field))
        if value is not None:
            return value
    return None


def _paired_abs_error_metrics(
    rows: Sequence[Mapping[str, Any]],
    left_field: str,
    right_field: str,
    *,
    max_lag_frames: int = 5,
) -> dict[str, Any]:
    start_index = _first_applied_control_index(rows)
    if start_index is None:
        return {
            "zero_lag_abs_error_p95": None,
            "best_lag_abs_error_p95": None,
            "best_lag_frames": None,
            "best_lag_sample_count": 0,
        }
    by_lag: list[tuple[int, float | None, int]] = []
    for lag in range(max_lag_frames + 1):
        values: list[float] = []
        for index in range(start_index + lag, len(rows)):
            left = _num(rows[index - lag].get(left_field))
            right = _num(rows[index].get(right_field))
            if left is not None and right is not None:
                values.append(abs(left - right))
        by_lag.append((lag, _percentile(values, 0.95), len(values)))

    zero_lag = by_lag[0][1] if by_lag else None
    candidates = [item for item in by_lag if item[1] is not None]
    best = min(candidates, key=lambda item: (float(item[1]), item[0])) if candidates else (None, None, 0)
    return {
        "zero_lag_abs_error_p95": zero_lag,
        "best_lag_abs_error_p95": best[1],
        "best_lag_frames": best[0],
        "best_lag_sample_count": best[2],
    }


def _first_applied_control_index(rows: Sequence[Mapping[str, Any]]) -> int | None:
    for index, row in enumerate(rows):
        if _row_has_nonzero_control(
            row,
            throttle_field="throttle_applied",
            brake_field="brake_applied",
            steer_field="carla_steer_applied",
        ):
            return index
    return None


def _bool_count(rows: Sequence[Mapping[str, Any]], *fields: str) -> int:
    count = 0
    for row in rows:
        if any(_parse_bool(row.get(field)) is True for field in fields):
            count += 1
    return count


def _series(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    return [value for row in rows if (value := _num(row.get(field))) is not None]


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(value for value in values if math.isfinite(float(value)))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (len(cleaned) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return cleaned[lower]
    weight = rank - lower
    return cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight


def _first_number(*values: Any) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _first_raw(*args: Any) -> Any:
    pairs = list(zip(args[0::2], args[1::2]))
    for mapping, key in pairs:
        if isinstance(mapping, Mapping):
            value = mapping.get(str(key))
            if value not in {None, ""}:
                return value
    return None


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _summary_bool(summary: Mapping[str, Any], name: str) -> bool | None:
    value = _parse_bool(summary.get(name))
    if value is not None:
        return value
    metrics = summary.get("metrics")
    if isinstance(metrics, Mapping):
        return _parse_bool(metrics.get(name))
    return None


def _summary_metrics(summary: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = summary.get("metrics")
    return metrics if isinstance(metrics, Mapping) else summary


def _runtime_contract_status(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> str | None:
    for source in (summary, manifest):
        runtime_contract = source.get("runtime_contract")
        if isinstance(runtime_contract, Mapping) and runtime_contract.get("status") not in {None, ""}:
            return str(runtime_contract.get("status"))
        value = source.get("runtime_contract_status")
        if value not in {None, ""}:
            return str(value)
    return None


def _control_handoff_status(summary: Mapping[str, Any]) -> str | None:
    value = summary.get("control_handoff_status")
    if value not in {None, ""}:
        return str(value)
    handoff = summary.get("control_handoff")
    if isinstance(handoff, Mapping) and handoff.get("control_handoff_status") not in {None, ""}:
        return str(handoff.get("control_handoff_status"))
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
        return rows
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _first_text(*args: Any, default: str | None = None) -> str | None:
    pairs = list(zip(args[0::2], args[1::2]))
    for mapping, key in pairs:
        if isinstance(mapping, Mapping):
            value = mapping.get(str(key))
            if value not in {None, ""}:
                return str(value)
    return default


def _text_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _markdown(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    lines = [
        "# Control Health Summary",
        "",
        f"- status: `{report.get('status')}`",
        f"- failure_reason: `{report.get('failure_reason')}`",
        f"- control_handoff_status: `{report.get('control_handoff_status')}`",
        f"- control_latency_p95_ms: `{metrics.get('control_latency_p95_ms')}`",
        f"- brake_throttle_conflict_frames: `{metrics.get('brake_throttle_conflict_frames')}`",
        f"- nonzero_mapped_control_frames: `{metrics.get('nonzero_mapped_control_frames')}`",
        f"- nonzero_applied_control_frames: `{metrics.get('nonzero_applied_control_frames')}`",
        f"- control_apply_observation_delay_s: `{metrics.get('control_apply_observation_delay_s')}`",
        f"- control_apply_observation_delay_source: `{metrics.get('control_apply_observation_delay_source')}`",
        f"- control_apply_observation_delay_timeseries_s: `{metrics.get('control_apply_observation_delay_timeseries_s')}`",
        f"- route_s_delta_m: `{metrics.get('route_s_delta_m')}`",
        (
            "- route_s_after_first_applied_control_delta_m: "
            f"`{metrics.get('route_s_after_first_applied_control_delta_m')}`"
        ),
        f"- link_delay_decomposition: `{metrics.get('link_delay_decomposition')}`",
        f"- stopped_ratio_after_first_applied_control: `{metrics.get('stopped_ratio_after_first_applied_control')}`",
        f"- mapped_applied_steer_abs_error_p95: `{metrics.get('mapped_applied_steer_abs_error_p95')}`",
        f"- mapped_applied_throttle_abs_error_p95: `{metrics.get('mapped_applied_throttle_abs_error_p95')}`",
        (
            "- mapped_applied_throttle_abs_error_p95_zero_lag: "
            f"`{metrics.get('mapped_applied_throttle_abs_error_p95_zero_lag')}`"
        ),
        f"- mapped_applied_throttle_best_lag_frames: `{metrics.get('mapped_applied_throttle_best_lag_frames')}`",
        f"- mapped_applied_brake_abs_error_p95: `{metrics.get('mapped_applied_brake_abs_error_p95')}`",
        f"- upstream_contract_preconditions: `{_markdown_upstream_contract_preconditions(metrics)}`",
        f"- oscillation_decomposition: `{_markdown_oscillation_decomposition(metrics)}`",
        f"- steering_mapping_saturation: `{_markdown_steering_mapping_saturation(metrics)}`",
        f"- control_mapping_claim_boundary: `{metrics.get('control_mapping_claim_boundary')}`",
        f"- control_bridge_log: `{_markdown_control_bridge_log(metrics)}`",
        f"- direct_control_apply_log: `{_markdown_direct_control_apply_log(metrics)}`",
        f"- warnings: `{report.get('warnings')}`",
        "",
        "This report is artifact-derived and does not prove full Apollo perception.",
        "",
    ]
    return "\n".join(lines)


def _markdown_control_bridge_log(metrics: Mapping[str, Any]) -> dict[str, Any]:
    log_metrics = metrics.get("control_bridge_log")
    if not isinstance(log_metrics, Mapping):
        return {"available": False}
    return {
        "available": log_metrics.get("available"),
        "configured_apply_hz": log_metrics.get("configured_apply_hz"),
        "sync_to_world_tick": log_metrics.get("sync_to_world_tick"),
        "apply_world_frame_hz": log_metrics.get("apply_world_frame_hz"),
        "apply_frame_coverage_ratio": log_metrics.get("apply_frame_coverage_ratio"),
        "same_frame_drop_ratio": log_metrics.get("same_frame_drop_ratio"),
        "bind_to_first_apply_s": log_metrics.get("bind_to_first_apply_s"),
        "first_watchdog_apply_wall_s": log_metrics.get("first_watchdog_apply_wall_s"),
        "final_rx_count": log_metrics.get("final_rx_count"),
        "final_applied_count": log_metrics.get("final_applied_count"),
    }


def _markdown_upstream_contract_preconditions(metrics: Mapping[str, Any]) -> dict[str, Any]:
    preconditions = metrics.get("upstream_contract_preconditions")
    if not isinstance(preconditions, Mapping):
        return {"available": False}
    localization = preconditions.get("localization_contract")
    reference_line = preconditions.get("apollo_reference_line_contract")
    if not isinstance(localization, Mapping):
        localization = {}
    if not isinstance(reference_line, Mapping):
        reference_line = {}
    return {
        "available": True,
        "control_oscillation_analysis_eligible": preconditions.get(
            "control_oscillation_analysis_eligible"
        ),
        "localization_contract_status": localization.get("status"),
        "apollo_reference_line_contract_status": reference_line.get("status"),
        "blocking_reasons": preconditions.get("blocking_reasons") or [],
    }


def _markdown_oscillation_decomposition(metrics: Mapping[str, Any]) -> dict[str, Any]:
    decomposition = metrics.get("oscillation_decomposition")
    if not isinstance(decomposition, Mapping):
        return {"available": False}
    layers = decomposition.get("layers") if isinstance(decomposition.get("layers"), Mapping) else {}
    return {
        "dominant_oscillation_layer": decomposition.get("dominant_oscillation_layer"),
        "apollo_raw_command": (layers.get("apollo_raw_command") or {}).get("status")
        if isinstance(layers.get("apollo_raw_command"), Mapping)
        else None,
        "bridge_mapped_command": (layers.get("bridge_mapped_command") or {}).get("status")
        if isinstance(layers.get("bridge_mapped_command"), Mapping)
        else None,
        "carla_applied_command": (layers.get("carla_applied_command") or {}).get("status")
        if isinstance(layers.get("carla_applied_command"), Mapping)
        else None,
        "vehicle_response": (layers.get("vehicle_response") or {}).get("status")
        if isinstance(layers.get("vehicle_response"), Mapping)
        else None,
        "bridge_apply_cadence": (layers.get("bridge_apply_cadence") or {}).get("status")
        if isinstance(layers.get("bridge_apply_cadence"), Mapping)
        else None,
    }


def _markdown_steering_mapping_saturation(metrics: Mapping[str, Any]) -> dict[str, Any]:
    control_decode = metrics.get("control_decode_debug")
    if not isinstance(control_decode, Mapping):
        return {"available": False}
    saturation = control_decode.get("steering_mapping_saturation")
    if not isinstance(saturation, Mapping):
        return {"available": False}
    return {
        "available": saturation.get("available"),
        "status": saturation.get("status"),
        "reason": saturation.get("reason"),
        "raw_steering_target_abs_p95": saturation.get("raw_steering_target_abs_p95"),
        "saturated_raw_steering_target_count": saturation.get(
            "saturated_raw_steering_target_count"
        ),
        "saturated_raw_low_mapped_ratio": saturation.get("saturated_raw_low_mapped_ratio"),
        "mapped_steer_abs_p95_when_raw_saturated": saturation.get(
            "mapped_steer_abs_p95_when_raw_saturated"
        ),
    }


def _markdown_direct_control_apply_log(metrics: Mapping[str, Any]) -> dict[str, Any]:
    apply_metrics = metrics.get("direct_control_apply_log")
    if not isinstance(apply_metrics, Mapping):
        return {"available": False}
    return {
        "available": apply_metrics.get("available"),
        "apply_count": apply_metrics.get("apply_count"),
        "first_apply_ts_sec": apply_metrics.get("first_apply_ts_sec"),
        "first_apply_frame_id": apply_metrics.get("first_apply_frame_id"),
        "actor_id": apply_metrics.get("actor_id"),
    }
