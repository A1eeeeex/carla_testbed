from __future__ import annotations

import csv
import json
import math
from bisect import bisect_left
from pathlib import Path
from typing import Any, Mapping, Sequence

APOLLO_LATERAL_SEMANTICS_SCHEMA_VERSION = "apollo_lateral_semantics.v1"
CONTROL_TRACE_MERGE_MAX_DT_S = 0.075
CONTROL_TRACE_HEADER_EXTRAPOLATE_MAX_DT_S = 1.5

FIELD_ALIASES = {
    "apollo_steer_raw": [
        "apollo_steer_raw",
        "apollo_desired_steer",
        "steering_target",
        "source_steer",
        "control_steer_raw",
        "steering_normalized_for_mapping",
    ],
    "bridge_steer_mapped": [
        "bridge_steer_mapped",
        "mapped_carla_steer_cmd",
        "mapped_steer",
        "control_steer_mapped",
        "commanded_steer",
        "cmd_steer",
        "clamped_steer",
    ],
    "carla_steer_applied": [
        "carla_steer_applied",
        "measured_steer",
        "applied_steer",
        "vehicle_steer_applied",
    ],
    "apollo_target_point_kappa": [
        "apollo_target_point_kappa",
        "target_point_kappa",
        "apollo_debug_simple_lat_target_point_kappa",
        "debug_simple_lat_target_point_kappa",
    ],
    "apollo_planning_first_kappa": [
        "apollo_planning_first_kappa",
        "planning_first_kappa",
        "first_point_kappa",
        "first_trajectory_point_kappa",
    ],
    "reference_lane_curvature": [
        "reference_lane_curvature",
        "reference_line_curvature",
        "apollo_reference_curvature",
        "apollo_debug_simple_lat_curvature",
        "debug_simple_lat_curvature",
        "debug_simple_lon_current_reference_point_kappa",
        "debug_simple_lon_preview_reference_point_kappa",
        "target_curvature",
    ],
    "route_curvature": ["route_curvature", "curvature_at_nearest"],
    "matched_point_distance": ["apollo_matched_point_distance", "matched_point_distance"],
    "target_point_distance": ["apollo_target_point_distance", "target_point_distance"],
    "ego_yaw_rate": [
        "ego_yaw_rate",
        "ego_yaw_rate_rad_s",
        "localization_yaw_rate_rad_s",
        "yaw_rate",
        "vehicle_yaw_rate",
    ],
    "cross_track_error": [
        "cross_track_error",
        "lateral_error",
        "apollo_debug_simple_lat_lateral_error_m",
        "debug_simple_lat_lateral_error_m",
        "e_y_m",
    ],
    "apollo_simple_lat_lateral_error": [
        "apollo_simple_lat_lateral_error",
        "apollo_debug_simple_lat_lateral_error_m",
        "debug_simple_lat_lateral_error_m",
        "e_y_m",
    ],
    "apollo_simple_lon_current_station": [
        "apollo_simple_lon_current_station",
        "apollo_debug_simple_lon_current_station_m",
        "debug_simple_lon_current_station_m",
    ],
    "apollo_simple_lon_station_reference": [
        "apollo_simple_lon_station_reference",
        "apollo_debug_simple_lon_station_reference_m",
        "debug_simple_lon_station_reference_m",
    ],
    "apollo_simple_lat_target_point_s": [
        "apollo_simple_lat_target_point_s",
        "apollo_debug_simple_lat_target_point_s",
        "debug_simple_lat_target_point_s",
        "debug_simple_lon_preview_reference_point_s_m",
        "apollo_target_point_s",
    ],
    "apollo_simple_lon_matched_point_s": [
        "apollo_simple_lon_matched_point_s",
        "apollo_debug_simple_lon_matched_point_s",
        "debug_simple_lon_matched_point_s",
        "debug_simple_lon_current_matched_point_s_m",
        "apollo_matched_point_s",
    ],
    "ego_x": ["ego_x", "localization_x", "apollo_localization_x"],
    "ego_y": ["ego_y", "localization_y", "apollo_localization_y"],
    "route_x": ["route_x", "nearest_route_x"],
    "route_y": ["route_y", "nearest_route_y"],
    "apollo_matched_point_x": [
        "apollo_debug_simple_lon_matched_point_x",
        "debug_simple_lon_current_matched_point_x",
        "apollo_debug_simple_mpc_matched_point_x",
        "apollo_matched_point_x",
    ],
    "apollo_matched_point_y": [
        "apollo_debug_simple_lon_matched_point_y",
        "debug_simple_lon_current_matched_point_y",
        "apollo_debug_simple_mpc_matched_point_y",
        "apollo_matched_point_y",
    ],
    "apollo_current_reference_point_x": [
        "apollo_debug_simple_lat_current_reference_point_x",
        "debug_simple_lat_current_reference_point_x",
        "debug_simple_lon_current_reference_point_x",
    ],
    "apollo_current_reference_point_y": [
        "apollo_debug_simple_lat_current_reference_point_y",
        "debug_simple_lat_current_reference_point_y",
        "debug_simple_lon_current_reference_point_y",
    ],
    "apollo_target_point_x": [
        "apollo_debug_simple_lat_target_point_x",
        "debug_simple_lon_preview_reference_point_x",
        "apollo_target_point_x",
    ],
    "apollo_target_point_y": [
        "apollo_debug_simple_lat_target_point_y",
        "debug_simple_lon_preview_reference_point_y",
        "apollo_target_point_y",
    ],
    "heading_error": [
        "heading_error",
        "apollo_debug_simple_lat_heading_error_rad",
        "debug_simple_lat_heading_error_rad",
        "e_psi_rad",
    ],
    "steer_scale": ["steer_scale"],
    "steering_sign": ["steering_sign"],
}

REQUIRED_SEMANTIC_FIELDS = (
    "route_curvature",
    "reference_lane_curvature",
    "apollo_planning_first_kappa",
    "apollo_target_point_kappa",
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "ego_yaw_rate",
    "cross_track_error",
    "heading_error",
    "matched_point_distance",
    "target_point_distance",
)

DEFAULT_THRESHOLDS = {
    "straight_curvature_abs_max": 0.005,
    "high_kappa_abs": 0.05,
    "target_kappa_spike_abs": 0.05,
    "high_source_steer_abs": 0.85,
    "small_lateral_error_abs_p95_m": 0.20,
    "small_heading_error_abs_p95_rad": 0.10,
    "matched_point_distance_max_m": 5.0,
    "target_point_jump_m": 3.0,
    "mapped_applied_steer_error_p95": 0.10,
    "raw_mapped_expected_error_p95": 0.10,
    "applied_steer_active_abs": 0.05,
    "yaw_response_min_abs_p95": 0.005,
    "high_cross_track_error_abs_p95_m": 0.50,
    "low_source_steer_abs_p95": 0.05,
    "low_applied_steer_abs_p95": 0.05,
    "hdmap_route_lateral_p95_m": 0.50,
    "drift_window_high_lateral_abs_m": 0.50,
    "drift_context_window_s": 2.0,
    "drift_context_window_route_s_m": 10.0,
    "critical_window_pre_s": 0.50,
    "critical_window_post_s": 1.50,
    "critical_window_max_samples": 16,
    "simple_lat_lateral_error_low_abs_p95_m": 0.05,
    "route_s_station_delta_high_p95_m": 20.0,
    "matched_point_near_ego_p95_m": 0.30,
    "matched_point_far_from_route_p95_m": 0.50,
    "lateral_sign_active_abs_m": 0.20,
    "steer_sign_active_abs": 0.02,
    "sign_alignment_ratio_high": 0.80,
    "sign_alignment_min_samples": 3,
    "route_simple_lat_sign_convention_abs_delta_p95_m": 0.10,
    "station_lane_s_alignment_p95_m": 2.0,
    "station_local_frame_offset_min_p95_m": 5.0,
    "station_target_current_alignment_p95_m": 2.0,
}

DRIFT_WINDOW_FIELD_ALIASES = {
    "sim_time": ["sim_time", "ts_sec", "timestamp", "time"],
    "route_s": ["route_s", "apollo_debug_simple_lon_current_station_m"],
    **FIELD_ALIASES,
}

ROUTE_LATERAL_ERROR_ALIASES = [
    "cross_track_error",
    "lateral_error",
    "route_cross_track_error",
    "route_lateral_error",
]


def analyze_apollo_lateral_semantics_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    timeseries_paths = [
        path
        for path in (
            _find_first_deep(root, ["artifacts/debug_timeseries.csv"]),
            _find_first(root, ["timeseries.csv", "timeseries.jsonl"]),
        )
        if path is not None
    ]
    return analyze_apollo_lateral_semantics(
        timeseries=timeseries_paths,
        route_health=_find_first(root, ["analysis/route_health/route_health.json", "route_health.json"]),
        planning_debug=_find_first(
            root,
            [
                "artifacts/planning_topic_debug.jsonl",
                "planning_topic_debug.jsonl",
                "planning_debug.csv",
                "planning_debug.json",
            ],
        ),
        control_trace=[
            path
            for path in (
                _find_first_deep(root, ["artifacts/control_apply_trace.jsonl", "control_apply_trace.jsonl"]),
                _find_first_deep(root, ["artifacts/control_decode_debug.jsonl", "control_decode_debug.jsonl"]),
                _find_first_deep(root, ["artifacts/apollo_control_raw.jsonl", "apollo_control_raw.jsonl"]),
            )
            if path is not None
        ],
        source_steer_summary=_find_first(
            root,
            ["artifacts/source_steer_summary.json", "source_steer_summary.json"],
        ),
        kappa_audit_summary=_find_first(
            root,
            ["artifacts/kappa_audit_summary.json", "kappa_audit_summary.json"],
        ),
        localization_contract=_find_first(
            root,
            [
                "analysis/localization_contract/localization_contract_report.json",
                "localization_contract_report.json",
            ],
        ),
        hdmap_projection=_find_first(
            root,
            [
                "artifacts/apollo_hdmap_projection.jsonl",
                "apollo_hdmap_projection.jsonl",
            ],
        ),
        reference_line_contract=_find_first(
            root,
            [
                "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
                "apollo_reference_line_contract_report.json",
            ],
        ),
        route_definition=_find_best_route_definition(
            root,
            [
                "route.json",
                "artifacts/route.json",
                "artifacts/route_definition_claim.json",
                "analysis/apollo_route_contract/route_definition_claim.json",
            ],
        ),
        control_attribution=_find_first(
            root,
            [
                "analysis/control_attribution/control_attribution_report.json",
                "control_attribution_report.json",
            ],
        ),
        run_dir=root,
        thresholds=thresholds,
    )


def analyze_apollo_lateral_semantics(
    *,
    timeseries: str | Path | Sequence[str | Path] | None = None,
    route_health: str | Path | None = None,
    planning_debug: str | Path | None = None,
    control_trace: str | Path | Sequence[str | Path] | None = None,
    source_steer_summary: str | Path | None = None,
    kappa_audit_summary: str | Path | None = None,
    localization_contract: str | Path | None = None,
    hdmap_projection: str | Path | Sequence[str | Path] | None = None,
    reference_line_contract: str | Path | None = None,
    route_definition: str | Path | None = None,
    control_attribution: str | Path | None = None,
    run_dir: str | Path | None = None,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})
    timeseries_rows = _read_rows(timeseries)
    planning_rows = _read_rows(planning_debug)
    control_trace_rows = _infer_control_trace_sim_times(_read_rows(control_trace))
    route_health_payload = _read_json(route_health)
    source_summary = _read_json(source_steer_summary)
    kappa_summary = _read_json(kappa_audit_summary)
    localization_contract_payload = _read_json(localization_contract)
    hdmap_projection_rows = _read_rows(hdmap_projection)
    reference_line_contract_payload = _read_json(reference_line_contract)
    route_definition_payload = _read_json(route_definition)
    control_attribution_payload = _read_json(control_attribution)
    semantic_rows = _merge_control_trace_fields([*timeseries_rows, *planning_rows], control_trace_rows)
    rows = _derive_point_distance_fields([*semantic_rows, *control_trace_rows])
    supplemental = _supplemental_values(route_health_payload, source_summary, kappa_summary)

    resolved_fields = {
        field: _resolved_field(rows, aliases)
        for field, aliases in FIELD_ALIASES.items()
    }
    values = {
        field: _values_for_field(rows, field, supplemental= supplemental)
        for field in FIELD_ALIASES
    }
    route_curvature = values["route_curvature"] or _route_health_curvature_values(route_health_payload)
    if route_curvature:
        values["route_curvature"] = route_curvature
        if resolved_fields.get("route_curvature") is None and route_health_payload:
            resolved_fields["route_curvature"] = "route_health.route_geometry.curvature"
    missing_fields = [
        field
        for field in REQUIRED_SEMANTIC_FIELDS
        if not values.get(field)
    ]

    stats = {
        "route_curvature_abs": _stats_abs(values["route_curvature"]),
        "reference_lane_curvature_abs": _stats_abs(values["reference_lane_curvature"]),
        "apollo_planning_first_kappa_abs": _stats_abs(values["apollo_planning_first_kappa"]),
        "apollo_target_point_kappa_abs": _stats_abs(values["apollo_target_point_kappa"]),
        "apollo_steer_raw_abs": _stats_abs(values["apollo_steer_raw"]),
        "bridge_steer_mapped_abs": _stats_abs(values["bridge_steer_mapped"]),
        "carla_steer_applied_abs": _stats_abs(values["carla_steer_applied"]),
        "ego_yaw_rate_abs": _stats_abs(values["ego_yaw_rate"]),
        "cross_track_error_abs": _stats_abs(values["cross_track_error"]),
        "apollo_simple_lat_lateral_error_abs": _stats_abs(values["apollo_simple_lat_lateral_error"]),
        "apollo_simple_lon_current_station_abs": _stats_abs(values["apollo_simple_lon_current_station"]),
        "apollo_simple_lat_target_point_s_abs": _stats_abs(values["apollo_simple_lat_target_point_s"]),
        "apollo_simple_lon_matched_point_s_abs": _stats_abs(values["apollo_simple_lon_matched_point_s"]),
        "route_s_vs_apollo_current_station_abs_delta": _paired_abs_delta_stats(
            rows,
            DRIFT_WINDOW_FIELD_ALIASES["route_s"],
            FIELD_ALIASES["apollo_simple_lon_current_station"],
        ),
        "route_s_vs_apollo_target_point_s_abs_delta": _paired_abs_delta_stats(
            rows,
            DRIFT_WINDOW_FIELD_ALIASES["route_s"],
            FIELD_ALIASES["apollo_simple_lat_target_point_s"],
        ),
        "ego_to_route_xy_distance": _xy_distance_stats(
            rows,
            FIELD_ALIASES["ego_x"],
            FIELD_ALIASES["ego_y"],
            FIELD_ALIASES["route_x"],
            FIELD_ALIASES["route_y"],
        ),
        "ego_to_apollo_matched_point_xy_distance": _xy_distance_stats(
            rows,
            FIELD_ALIASES["ego_x"],
            FIELD_ALIASES["ego_y"],
            FIELD_ALIASES["apollo_matched_point_x"],
            FIELD_ALIASES["apollo_matched_point_y"],
        ),
        "route_to_apollo_matched_point_xy_distance": _xy_distance_stats(
            rows,
            FIELD_ALIASES["route_x"],
            FIELD_ALIASES["route_y"],
            FIELD_ALIASES["apollo_matched_point_x"],
            FIELD_ALIASES["apollo_matched_point_y"],
        ),
        "ego_to_apollo_target_point_xy_distance": _xy_distance_stats(
            rows,
            FIELD_ALIASES["ego_x"],
            FIELD_ALIASES["ego_y"],
            FIELD_ALIASES["apollo_target_point_x"],
            FIELD_ALIASES["apollo_target_point_y"],
        ),
        "route_to_apollo_target_point_xy_distance": _xy_distance_stats(
            rows,
            FIELD_ALIASES["route_x"],
            FIELD_ALIASES["route_y"],
            FIELD_ALIASES["apollo_target_point_x"],
            FIELD_ALIASES["apollo_target_point_y"],
        ),
        "heading_error_abs": _stats_abs(values["heading_error"]),
        "matched_point_distance_abs": _stats_abs(values["matched_point_distance"]),
        "target_point_distance_abs": _stats_abs(values["target_point_distance"]),
        "route_vs_planning_kappa_correlation": _pearson_pair(
            values["route_curvature"],
            values["apollo_planning_first_kappa"],
        ),
        "route_vs_target_kappa_correlation": _pearson_pair(
            values["route_curvature"],
            values["apollo_target_point_kappa"],
        ),
        "target_kappa_vs_source_steer_correlation": _pearson_pair(
            values["apollo_target_point_kappa"],
            values["apollo_steer_raw"],
        ),
    }
    hdmap_route_lateral_consistency = _hdmap_route_lateral_consistency(localization_contract_payload)
    reference_debug_summary = _reference_debug_summary(reference_line_contract_payload)
    drift_window_summary = _drift_window_summary(rows, active_thresholds)
    lateral_sign_alignment = _lateral_sign_alignment_summary(
        rows,
        active_thresholds,
        hdmap_route_lateral_consistency=hdmap_route_lateral_consistency,
        hdmap_projection_rows=hdmap_projection_rows,
        route_definition_payload=route_definition_payload,
    )
    critical_window_samples = _critical_window_samples(
        rows,
        active_thresholds,
        reference_line_contract=reference_line_contract_payload,
    )
    anomalies = _anomalies(
        values,
        stats,
        active_thresholds,
        rows=rows,
        hdmap_route_lateral_consistency=hdmap_route_lateral_consistency,
        reference_debug_summary=reference_debug_summary,
        lateral_sign_alignment=lateral_sign_alignment,
    )
    control_attribution_summary = _control_attribution_summary(control_attribution_payload)
    anomalies, suppressed_control_mapping_anomalies = _suppress_control_mapping_anomalies(
        anomalies,
        control_attribution_summary,
    )
    if not rows and not route_health_payload:
        anomalies.append(_anomaly("insufficient_data", "missing_timeseries_and_route_health", "insufficient_data"))
    elif missing_fields and not anomalies:
        anomalies.append(
            _anomaly(
                "insufficient_data",
                "missing_semantic_fields",
                "insufficient_data",
                fields=missing_fields,
            )
        )
    suspected_layer = _suspected_layer(anomalies)
    confidence = _confidence(anomalies, missing_fields)
    status = "pass"
    failure_reason = None
    if suspected_layer == "insufficient_data":
        status = "insufficient_data"
        failure_reason = "missing_lateral_semantics_fields"
    elif anomalies:
        status = "warn"
        failure_reason = "lateral_semantics_anomaly"

    return {
        "schema_version": APOLLO_LATERAL_SEMANTICS_SCHEMA_VERSION,
        "run_id": _first_text(rows, "run_id", default=Path(run_dir).name if run_dir else None),
        "route_id": _first_text(rows, "route_id") or _route_health_route_id(route_health_payload),
        "backend": _first_text(rows, "backend") or _first_text(rows, "backend_name"),
        "route_curvature_available": bool(values["route_curvature"]),
        "reference_line_curvature_available": bool(values["reference_lane_curvature"]),
        "planning_kappa_available": bool(values["apollo_planning_first_kappa"]),
        "target_point_available": bool(values["apollo_target_point_kappa"] or values["target_point_distance"]),
        "source_steer_available": bool(values["apollo_steer_raw"]),
        "matched_point_available": bool(values["matched_point_distance"]),
        "anomalies": anomalies,
        "correlation_summary": stats,
        "hdmap_route_lateral_consistency": hdmap_route_lateral_consistency,
        "reference_debug_summary": reference_debug_summary,
        "control_attribution_summary": control_attribution_summary,
        "suppressed_control_mapping_anomalies": suppressed_control_mapping_anomalies,
        "drift_window_summary": drift_window_summary,
        "lateral_sign_alignment": lateral_sign_alignment,
        "critical_window_samples": critical_window_samples,
        "suspected_layer": suspected_layer,
        "confidence": confidence,
        "missing_fields": sorted(set(missing_fields)),
        "resolved_fields": {key: value for key, value in resolved_fields.items() if value},
        "verdict": {
            "status": status,
            "failure_reason": failure_reason,
            "suspected_layer": suspected_layer,
            "confidence": confidence,
        },
        "thresholds": active_thresholds,
        "source": {
            "run_dir": None if run_dir is None else str(Path(run_dir)),
            "timeseries": _path_repr(timeseries),
            "control_trace": _path_repr(control_trace),
            "route_health": None if route_health is None else str(Path(route_health)),
            "planning_debug": None if planning_debug is None else str(Path(planning_debug)),
            "source_steer_summary": None if source_steer_summary is None else str(Path(source_steer_summary)),
            "kappa_audit_summary": None if kappa_audit_summary is None else str(Path(kappa_audit_summary)),
            "localization_contract": None if localization_contract is None else str(Path(localization_contract)),
            "hdmap_projection": _path_repr(hdmap_projection),
            "reference_line_contract": None if reference_line_contract is None else str(Path(reference_line_contract)),
            "route_definition": None if route_definition is None else str(Path(route_definition)),
            "control_attribution": None if control_attribution is None else str(Path(control_attribution)),
        },
        "interpretation_boundary": (
            "This report identifies a suspected lateral-semantics layer with confidence; "
            "it is not a definitive root-cause proof, does not change steer_scale, "
            "and does not prove the bridge irrelevant when lateral guards are inactive."
        ),
    }


def write_apollo_lateral_semantics_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "apollo_lateral_semantics_report.json"
    summary_path = output_dir / "apollo_lateral_semantics_summary.md"
    projection_pairing_path = output_dir / "apollo_lateral_projection_pairing.csv"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_markdown(report), encoding="utf-8")
    outputs = {
        "apollo_lateral_semantics_report": str(report_path),
        "apollo_lateral_semantics_summary": str(summary_path),
    }
    if _write_projection_pairing_csv(report, projection_pairing_path):
        outputs["apollo_lateral_projection_pairing_csv"] = str(projection_pairing_path)
    return outputs


def _write_projection_pairing_csv(report: Mapping[str, Any], path: Path) -> bool:
    alignment = report.get("lateral_sign_alignment")
    if not isinstance(alignment, Mapping):
        return False
    projection = alignment.get("official_hdmap_projection_alignment")
    if not isinstance(projection, Mapping):
        return False
    samples = projection.get("matched_samples")
    if not isinstance(samples, list) or not samples:
        return False
    fieldnames = [
        "sim_time",
        "projection_sim_time",
        "match_dt_s",
        "route_lateral_error_m",
        "apollo_simple_lat_lateral_error_m",
        "hdmap_projection_l_m",
        "hdmap_projection_s_m",
        "hdmap_projection_center_x",
        "hdmap_projection_center_y",
        "hdmap_projection_lane_heading_rad",
        "hdmap_projection_l_recomputed_m",
        "projection_l_recompute_delta_m",
        "projection_centerline_geometry_available",
        "apollo_simple_lon_current_station_m",
        "apollo_simple_lon_matched_point_s_m",
        "apollo_simple_lat_target_point_s_m",
        "current_station_minus_projection_s_m",
        "matched_s_minus_projection_s_m",
        "target_s_minus_projection_s_m",
        "matched_s_minus_current_station_m",
        "target_s_minus_current_station_m",
        "target_s_minus_matched_s_m",
        "apollo_matched_point_hdmap_line_lateral_m",
        "apollo_matched_point_hdmap_line_longitudinal_m",
        "apollo_current_reference_point_hdmap_line_lateral_m",
        "apollo_current_reference_point_hdmap_line_longitudinal_m",
        "apollo_target_point_hdmap_line_lateral_m",
        "apollo_target_point_hdmap_line_longitudinal_m",
        "nearest_lane_id",
        "route_lateral_vs_projection_lateral",
        "simple_lat_vs_projection_lateral",
        "route_projection_abs_sum_m",
        "route_projection_abs_magnitude_delta_m",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for sample in samples:
            if isinstance(sample, Mapping):
                writer.writerow({key: sample.get(key) for key in fieldnames})
    return True


def _anomalies(
    values: Mapping[str, list[float]],
    stats: Mapping[str, Any],
    thresholds: Mapping[str, float],
    *,
    rows: Sequence[Mapping[str, Any]],
    hdmap_route_lateral_consistency: Mapping[str, Any] | None = None,
    reference_debug_summary: Mapping[str, Any] | None = None,
    lateral_sign_alignment: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    route_max = _stat(stats, "route_curvature_abs", "max")
    ref_max = _stat(stats, "reference_lane_curvature_abs", "max")
    planning_max = _stat(stats, "apollo_planning_first_kappa_abs", "max")
    target_max = _stat(stats, "apollo_target_point_kappa_abs", "max")
    source_p95 = _stat(stats, "apollo_steer_raw_abs", "p95")
    cte_p95 = _stat(stats, "cross_track_error_abs", "p95")
    heading_p95 = _stat(stats, "heading_error_abs", "p95")
    matched_max = _stat(stats, "matched_point_distance_abs", "max")
    straight_route = route_max is not None and route_max <= thresholds["straight_curvature_abs_max"]
    straight_ref = ref_max is not None and ref_max <= thresholds["straight_curvature_abs_max"]
    if (straight_route or straight_ref) and planning_max is not None and planning_max >= thresholds["high_kappa_abs"]:
        anomalies.append(
            _anomaly(
                "route_straight_but_planning_kappa_high",
                "planning kappa is high while route/reference curvature is near zero",
                "reference_line_semantics",
                route_curvature_max_abs=route_max,
                reference_curvature_max_abs=ref_max,
                planning_first_kappa_max_abs=planning_max,
            )
        )
    hdmap_route_lateral_consistency = (
        hdmap_route_lateral_consistency if isinstance(hdmap_route_lateral_consistency, Mapping) else {}
    )
    hdmap_lateral_p95 = _num(
        hdmap_route_lateral_consistency.get("projection_lateral_p95_m")
        or hdmap_route_lateral_consistency.get("route_cross_track_p95_m")
    )
    if (
        hdmap_route_lateral_consistency.get("status") == "pass"
        and hdmap_route_lateral_consistency.get("interpretation")
        == "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"
        and hdmap_lateral_p95 is not None
        and hdmap_lateral_p95 >= thresholds["hdmap_route_lateral_p95_m"]
    ):
        anomalies.append(
            _anomaly(
                "actual_lateral_drift_matches_hdmap_projection",
                "Apollo HDMap projection_l matches CARLA route cross-track while lateral error is high",
                "target_point_semantics",
                projection_lateral_p95_m=hdmap_route_lateral_consistency.get("projection_lateral_p95_m"),
                route_cross_track_p95_m=hdmap_route_lateral_consistency.get("route_cross_track_p95_m"),
                best_abs_delta_p95_m=hdmap_route_lateral_consistency.get("best_abs_delta_p95_m"),
                alignment_mode=hdmap_route_lateral_consistency.get("alignment_mode"),
            )
        )
    if target_max is not None and target_max >= thresholds["target_kappa_spike_abs"]:
        anomalies.append(
            _anomaly(
                "target_kappa_spike",
                "target point kappa exceeds threshold",
                "target_point_semantics",
                target_point_kappa_max_abs=target_max,
            )
        )
    reference_debug_summary = reference_debug_summary if isinstance(reference_debug_summary, Mapping) else {}
    if reference_debug_summary.get("nonempty_planning_with_reference_debug_missing"):
        presence_classification = str(
            reference_debug_summary.get("planning_debug_presence_classification") or ""
        )
        if presence_classification == "routing_present_reference_line_empty":
            anomaly_type = "planning_routing_present_but_reference_line_empty"
            reason = (
                "Planning debug contains planning_data and routing segments, but "
                "debug.planning_data.reference_line is present with an empty reference-line list"
            )
        else:
            anomaly_type = "planning_nonempty_but_reference_line_debug_missing"
            reason = (
                "Planning trajectories are non-empty while reference-line debug reports zero "
                "reference lines or provider-not-ready state"
            )
        anomalies.append(
            _anomaly(
                anomaly_type,
                reason,
                "reference_line_semantics",
                nonempty_trajectory_ratio=reference_debug_summary.get("nonempty_trajectory_ratio"),
                reference_line_provider_ready_ratio=reference_debug_summary.get("reference_line_provider_ready_ratio"),
                reference_line_count_zero_ratio=reference_debug_summary.get("reference_line_count_zero_ratio"),
                routing_segment_count_zero_ratio=reference_debug_summary.get("routing_segment_count_zero_ratio"),
                debug_gap_classification=reference_debug_summary.get("debug_gap_classification"),
                planning_debug_presence_classification=presence_classification or None,
                planning_debug_presence=reference_debug_summary.get("planning_debug_presence"),
                route_segment_available=reference_debug_summary.get("route_segment_available"),
                control_simple_lat_reference_available=reference_debug_summary.get("control_simple_lat_reference_available"),
                control_reference_join_coverage_ratio=reference_debug_summary.get("control_reference_join_coverage_ratio"),
            )
        )
    lateral_sign_alignment = lateral_sign_alignment if isinstance(lateral_sign_alignment, Mapping) else {}
    source_vs_route = lateral_sign_alignment.get("source_steer_vs_route_lateral_error")
    source_vs_simple_lat = lateral_sign_alignment.get("source_steer_vs_simple_lat_lateral_error")
    mapped_vs_route = lateral_sign_alignment.get("mapped_steer_vs_route_lateral_error")
    applied_vs_route = lateral_sign_alignment.get("applied_steer_vs_route_lateral_error")
    route_vs_simple_lat = lateral_sign_alignment.get("route_lateral_error_vs_simple_lat_lateral_error")
    first_high_alignment = lateral_sign_alignment.get("first_high_lateral_sample")
    route_lateral_provenance = lateral_sign_alignment.get("route_lateral_provenance")
    source_route_same = _num(source_vs_route.get("same_sign_ratio")) if isinstance(source_vs_route, Mapping) else None
    source_simple_same = (
        _num(source_vs_simple_lat.get("same_sign_ratio")) if isinstance(source_vs_simple_lat, Mapping) else None
    )
    source_route_count = _num(source_vs_route.get("sample_count")) if isinstance(source_vs_route, Mapping) else 0.0
    source_simple_count = _num(source_vs_simple_lat.get("sample_count")) if isinstance(source_vs_simple_lat, Mapping) else 0.0
    min_samples = int(thresholds["sign_alignment_min_samples"])
    high_ratio = thresholds["sign_alignment_ratio_high"]
    first_high_source_same = (
        isinstance(first_high_alignment, Mapping)
        and first_high_alignment.get("source_steer_vs_route_lateral_error") == "same_sign"
    )
    route_simple_opposite = (
        _num(route_vs_simple_lat.get("opposite_sign_ratio")) if isinstance(route_vs_simple_lat, Mapping) else None
    )
    route_simple_count = _num(route_vs_simple_lat.get("sample_count")) if isinstance(route_vs_simple_lat, Mapping) else 0.0
    first_high_route_simple_opposite = (
        isinstance(first_high_alignment, Mapping)
        and first_high_alignment.get("route_lateral_error_vs_simple_lat_lateral_error") == "opposite_sign"
    )
    route_simple_magnitude = lateral_sign_alignment.get("route_simple_lat_magnitude_alignment")
    route_simple_sign_convention_candidate = (
        isinstance(route_simple_magnitude, Mapping)
        and route_simple_magnitude.get("magnitude_agreement_candidate") is True
    )
    if route_simple_sign_convention_candidate:
        anomalies.append(
            _anomaly(
                "route_simple_lat_sign_convention_mismatch_candidate",
                (
                    "CARLA route cross-track error and Apollo simple_lat lateral error are "
                    "opposite-sign with closely matching magnitudes during active samples; "
                    "treat this as a likely frame/sign-convention mismatch candidate before "
                    "changing control mapping or steer scale"
                ),
                "reference_line_semantics",
                route_simple_lat_magnitude_alignment=route_simple_magnitude,
                route_lateral_provenance=route_lateral_provenance,
                first_high_lateral_sample=first_high_alignment,
            )
        )
    if (
        (route_simple_opposite is not None and route_simple_opposite >= high_ratio and route_simple_count >= min_samples)
        or first_high_route_simple_opposite
    ):
        anomalies.append(
            _anomaly(
                "route_lateral_error_opposes_simple_lat_lateral_error",
                (
                    "CARLA route cross-track error and Apollo simple_lat lateral error have opposite signs "
                    "during active lateral-error samples; this is a reference-line/frame-convention "
                    "semantics check, not a control mapping fix by itself"
                ),
                "reference_line_semantics",
                route_lateral_error_vs_simple_lat_lateral_error=route_vs_simple_lat,
                first_high_lateral_sample=first_high_alignment,
            )
        )
    if (
        (source_route_same is not None and source_route_same >= high_ratio and source_route_count >= min_samples)
        or (
            source_simple_same is not None
            and source_simple_same >= high_ratio
            and source_simple_count >= min_samples
        )
        or first_high_source_same
    ):
        anomalies.append(
            _anomaly(
                "source_steer_same_sign_as_lateral_error",
                (
                    "Apollo source steer has the same sign as route/simple_lat lateral error "
                    "during active lateral-error samples; this is a sign-convention/semantics "
                    "sense check, not proof of a steering-sign bug by itself"
                ),
                "reference_line_semantics",
                source_steer_vs_route_lateral_error=source_vs_route,
                source_steer_vs_simple_lat_lateral_error=source_vs_simple_lat,
                route_lateral_error_vs_simple_lat_lateral_error=route_vs_simple_lat,
                mapped_steer_vs_route_lateral_error=mapped_vs_route,
                applied_steer_vs_route_lateral_error=applied_vs_route,
                first_high_lateral_sample=first_high_alignment,
            )
        )
    applied_p95 = _stat(stats, "carla_steer_applied_abs", "p95")
    simple_lat_lateral_p95 = _stat(stats, "apollo_simple_lat_lateral_error_abs", "p95")
    route_station_delta_p95 = _stat(stats, "route_s_vs_apollo_current_station_abs_delta", "p95")
    ego_to_matched_p95 = _stat(stats, "ego_to_apollo_matched_point_xy_distance", "p95")
    route_to_matched_p95 = _stat(stats, "route_to_apollo_matched_point_xy_distance", "p95")
    if (
        (straight_route or straight_ref)
        and cte_p95 is not None
        and cte_p95 >= thresholds["high_cross_track_error_abs_p95_m"]
        and source_p95 is not None
        and source_p95 <= thresholds["low_source_steer_abs_p95"]
        and applied_p95 is not None
        and applied_p95 <= thresholds["low_applied_steer_abs_p95"]
    ):
        anomalies.append(
            _anomaly(
                "high_lateral_drift_with_low_source_steer",
                "cross-track error is high on a near-straight route while Apollo source/applied steer stays near zero",
                "target_point_semantics",
                cross_track_error_abs_p95=cte_p95,
                source_steer_abs_p95=source_p95,
                applied_steer_abs_p95=applied_p95,
                route_curvature_max_abs=route_max,
                reference_curvature_max_abs=ref_max,
            )
        )
    if (
        cte_p95 is not None
        and cte_p95 >= thresholds["high_cross_track_error_abs_p95_m"]
        and simple_lat_lateral_p95 is not None
        and simple_lat_lateral_p95 <= thresholds["simple_lat_lateral_error_low_abs_p95_m"]
        and source_p95 is not None
        and source_p95 <= thresholds["low_source_steer_abs_p95"]
    ):
        anomalies.append(
            _anomaly(
                "route_lateral_high_but_simple_lat_and_source_steer_near_zero",
                (
                    "route/HDMap lateral error is high while Apollo simple_lat lateral error "
                    "and source steer stay near zero"
                ),
                "target_point_semantics",
                cross_track_error_abs_p95=cte_p95,
                apollo_simple_lat_lateral_error_abs_p95=simple_lat_lateral_p95,
                source_steer_abs_p95=source_p95,
            )
        )
    if (
        cte_p95 is not None
        and cte_p95 >= thresholds["high_cross_track_error_abs_p95_m"]
        and simple_lat_lateral_p95 is not None
        and simple_lat_lateral_p95 <= thresholds["simple_lat_lateral_error_low_abs_p95_m"]
        and route_station_delta_p95 is not None
        and route_station_delta_p95 >= thresholds["route_s_station_delta_high_p95_m"]
    ):
        anomalies.append(
            _anomaly(
                "simple_lat_station_frame_not_route_s_aligned",
                (
                    "Apollo simple_lon station differs materially from route_s while route/HDMap "
                    "lateral error is high and simple_lat lateral error stays near zero"
                ),
                "target_point_semantics",
                cross_track_error_abs_p95=cte_p95,
                apollo_simple_lat_lateral_error_abs_p95=simple_lat_lateral_p95,
                route_s_vs_apollo_current_station_abs_delta_p95=route_station_delta_p95,
            )
        )
    if (
        cte_p95 is not None
        and cte_p95 >= thresholds["high_cross_track_error_abs_p95_m"]
        and ego_to_matched_p95 is not None
        and ego_to_matched_p95 <= thresholds["matched_point_near_ego_p95_m"]
        and route_to_matched_p95 is not None
        and route_to_matched_p95 >= thresholds["matched_point_far_from_route_p95_m"]
    ):
        anomalies.append(
            _anomaly(
                "matched_point_tracks_ego_not_route_centerline",
                (
                    "Apollo matched point is close to ego pose while the route centerline is far "
                    "from the same matched point during high route/HDMap lateral error"
                ),
                "target_point_semantics",
                cross_track_error_abs_p95=cte_p95,
                ego_to_apollo_matched_point_xy_distance_p95=ego_to_matched_p95,
                route_to_apollo_matched_point_xy_distance_p95=route_to_matched_p95,
            )
        )
    if (
        source_p95 is not None
        and source_p95 >= thresholds["high_source_steer_abs"]
        and cte_p95 is not None
        and cte_p95 <= thresholds["small_lateral_error_abs_p95_m"]
        and heading_p95 is not None
        and heading_p95 <= thresholds["small_heading_error_abs_p95_rad"]
    ):
        anomalies.append(
            _anomaly(
                "source_steer_high_with_small_lateral_error",
                "raw source steer is high before large lateral/heading error appears",
                "target_point_semantics",
                source_steer_abs_p95=source_p95,
                cross_track_error_abs_p95=cte_p95,
                heading_error_abs_p95=heading_p95,
            )
        )
    if matched_max is not None and matched_max > thresholds["matched_point_distance_max_m"]:
        anomalies.append(
            _anomaly(
                "matched_point_too_large",
                "matched point distance exceeds threshold",
                "reference_line_semantics",
                matched_point_distance_max_abs=matched_max,
            )
        )
    target_jump = _max_abs_delta(values["target_point_distance"])
    if target_jump is not None and target_jump > thresholds["target_point_jump_m"]:
        anomalies.append(
            _anomaly(
                "target_point_jump",
                "target point distance jumps between adjacent samples",
                "target_point_semantics",
                target_point_distance_max_jump=target_jump,
            )
        )
    raw_mapping_error = _raw_mapped_expected_error(rows)
    mapped_applied_error = _paired_field_error(rows, "bridge_steer_mapped", "carla_steer_applied")
    if (
        raw_mapping_error is not None
        and raw_mapping_error > thresholds["raw_mapped_expected_error_p95"]
    ) or (
        mapped_applied_error is not None
        and mapped_applied_error > thresholds["mapped_applied_steer_error_p95"]
    ):
        anomalies.append(
            _anomaly(
                "raw_mapped_applied_mismatch",
                "raw/mapped/applied steer fields are inconsistent",
                "control_mapping",
                raw_to_mapped_expected_error_p95=raw_mapping_error,
                mapped_to_applied_error_p95=mapped_applied_error,
            )
        )
    yaw_p95 = _stat(stats, "ego_yaw_rate_abs", "p95")
    if (
        applied_p95 is not None
        and applied_p95 >= thresholds["applied_steer_active_abs"]
        and yaw_p95 is not None
        and yaw_p95 < thresholds["yaw_response_min_abs_p95"]
    ):
        anomalies.append(
            _anomaly(
                "applied_steer_no_yaw_response",
                "CARLA-applied steer is active but yaw-rate response is near zero",
                "vehicle_response",
                applied_steer_abs_p95=applied_p95,
                ego_yaw_rate_abs_p95=yaw_p95,
            )
        )
    return anomalies


def _suspected_layer(anomalies: Sequence[Mapping[str, Any]]) -> str:
    if not anomalies:
        return "none"
    layers = [str(item.get("suspected_layer")) for item in anomalies if item.get("type") != "insufficient_data"]
    if not layers:
        return "insufficient_data"
    for layer in (
        "control_mapping",
        "vehicle_response",
        "target_point_semantics",
        "reference_line_semantics",
        "bridge_assist",
    ):
        if layer in layers:
            return layer
    return layers[0]


def _confidence(anomalies: Sequence[Mapping[str, Any]], missing_fields: Sequence[str]) -> str:
    semantic_anomalies = [item for item in anomalies if item.get("type") != "insufficient_data"]
    if not semantic_anomalies:
        return "low"
    if len(missing_fields) <= 2 and len(semantic_anomalies) >= 2:
        return "high"
    if len(missing_fields) <= 5:
        return "medium"
    return "low"


def _anomaly(kind: str, reason: str, layer: str, **evidence: Any) -> dict[str, Any]:
    return {
        "type": kind,
        "reason": reason,
        "suspected_layer": layer,
        "evidence": evidence,
    }


def _values_for_field(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    supplemental: Mapping[str, list[float]],
) -> list[float]:
    values: list[float] = []
    for alias in FIELD_ALIASES.get(field, [field]):
        alias_values = [_num(row.get(alias)) for row in rows]
        values.extend(value for value in alias_values if value is not None)
        if values:
            break
    if not values:
        values.extend(supplemental.get(field, []))
    return values


def _resolved_field(rows: Sequence[Mapping[str, Any]], aliases: Sequence[str]) -> str | None:
    for alias in aliases:
        if any(_num(row.get(alias)) is not None for row in rows):
            return alias
    return None


def _supplemental_values(*payloads: Mapping[str, Any]) -> dict[str, list[float]]:
    result: dict[str, list[float]] = {field: [] for field in FIELD_ALIASES}
    for payload in payloads:
        if not payload:
            continue
        flat = _flatten_mapping(payload)
        for field, aliases in FIELD_ALIASES.items():
            for alias in aliases:
                for key, value in flat.items():
                    if key.endswith(alias):
                        number = _num(value)
                        if number is not None:
                            result[field].append(number)
    return result


def _merge_control_trace_fields(
    semantic_rows: Sequence[Mapping[str, Any]],
    control_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not semantic_rows or not control_rows:
        return [dict(row) for row in semantic_rows]
    indexed = [
        (sim_time, row)
        for row in control_rows
        if (sim_time := _row_value(row, DRIFT_WINDOW_FIELD_ALIASES["sim_time"])) is not None
    ]
    if not indexed:
        return [dict(row) for row in semantic_rows]
    indexed.sort(key=lambda item: item[0])
    times = [item[0] for item in indexed]
    merged: list[dict[str, Any]] = []
    control_fields = (
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "ego_yaw_rate",
        "steer_scale",
        "steering_sign",
        "apollo_simple_lat_lateral_error",
        "apollo_simple_lon_current_station",
        "apollo_simple_lon_station_reference",
        "apollo_simple_lat_target_point_s",
        "apollo_simple_lon_matched_point_s",
        "heading_error",
        "reference_lane_curvature",
        "apollo_target_point_kappa",
        "apollo_matched_point_x",
        "apollo_matched_point_y",
        "apollo_current_reference_point_x",
        "apollo_current_reference_point_y",
        "apollo_target_point_x",
        "apollo_target_point_y",
    )
    for row in semantic_rows:
        current = dict(row)
        sim_time = _row_value(current, DRIFT_WINDOW_FIELD_ALIASES["sim_time"])
        merge_dts: list[float] = []
        for field in control_fields:
            nearest = _nearest_control_row_with_field(sim_time, times, indexed, FIELD_ALIASES.get(field, [field]))
            if nearest is None:
                continue
            nearest_time = _row_value(nearest, DRIFT_WINDOW_FIELD_ALIASES["sim_time"])
            if nearest_time is not None and sim_time is not None:
                merge_dts.append(abs(float(sim_time) - float(nearest_time)))
            value = _row_value(nearest, FIELD_ALIASES.get(field, [field]))
            if value is not None:
                current[field] = value
        if merge_dts:
            current["_control_trace_merge_dt_s"] = min(merge_dts)
        merged.append(current)
    return merged


def _infer_control_trace_sim_times(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    normalized = [dict(row) for row in rows]
    anchors: list[tuple[float, float]] = []
    for row in normalized:
        sim_time = _num(row.get("sim_time"))
        header_timestamp = _row_value(row, ["control_header_timestamp_sec"])
        if sim_time is not None and header_timestamp is not None:
            anchors.append((float(header_timestamp), float(sim_time)))
    if not anchors:
        return normalized
    anchors = sorted(set(anchors), key=lambda item: item[0])
    for row in normalized:
        if _num(row.get("sim_time")) is not None:
            continue
        header_timestamp = _row_value(row, ["control_header_timestamp_sec"])
        if header_timestamp is None:
            continue
        sim_time = _interpolate_control_header_to_sim_time(float(header_timestamp), anchors)
        if sim_time is None:
            continue
        row["sim_time"] = sim_time
        row["_sim_time_inferred_from"] = "control_header_timestamp_sec"
    return normalized


def _interpolate_control_header_to_sim_time(
    header_timestamp: float,
    anchors: Sequence[tuple[float, float]],
) -> float | None:
    if not anchors:
        return None
    headers = [item[0] for item in anchors]
    insert_at = bisect_left(headers, header_timestamp)
    if 0 < insert_at < len(anchors):
        left_header, left_sim = anchors[insert_at - 1]
        right_header, right_sim = anchors[insert_at]
        if right_header == left_header:
            return left_sim
        ratio = (header_timestamp - left_header) / (right_header - left_header)
        return left_sim + ratio * (right_sim - left_sim)
    if insert_at == 0:
        nearest_header, nearest_sim = anchors[0]
        if abs(header_timestamp - nearest_header) > CONTROL_TRACE_HEADER_EXTRAPOLATE_MAX_DT_S:
            return None
        if len(anchors) >= 2 and anchors[1][0] != anchors[0][0]:
            slope = (anchors[1][1] - anchors[0][1]) / (anchors[1][0] - anchors[0][0])
        else:
            slope = 1.0
        return nearest_sim + (header_timestamp - nearest_header) * slope
    nearest_header, nearest_sim = anchors[-1]
    if abs(header_timestamp - nearest_header) > CONTROL_TRACE_HEADER_EXTRAPOLATE_MAX_DT_S:
        return None
    if len(anchors) >= 2 and anchors[-1][0] != anchors[-2][0]:
        slope = (anchors[-1][1] - anchors[-2][1]) / (anchors[-1][0] - anchors[-2][0])
    else:
        slope = 1.0
    return nearest_sim + (header_timestamp - nearest_header) * slope


def _derive_point_distance_fields(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ego_x_aliases = ["localization_x", "apollo_localization_x", "apollo_map_x", "map_x"]
    ego_y_aliases = ["localization_y", "apollo_localization_y", "apollo_map_y", "map_y"]
    derived: list[dict[str, Any]] = []
    for row in rows:
        current = dict(row)
        if _row_value(current, FIELD_ALIASES["matched_point_distance"]) is None:
            distance = _xy_distance(
                current,
                ego_x_aliases,
                ego_y_aliases,
                FIELD_ALIASES["apollo_matched_point_x"],
                FIELD_ALIASES["apollo_matched_point_y"],
            )
            if distance is not None:
                current["matched_point_distance"] = distance
        if _row_value(current, FIELD_ALIASES["target_point_distance"]) is None:
            distance = _xy_distance(
                current,
                ego_x_aliases,
                ego_y_aliases,
                FIELD_ALIASES["apollo_target_point_x"],
                FIELD_ALIASES["apollo_target_point_y"],
            )
            if distance is not None:
                current["target_point_distance"] = distance
        derived.append(current)
    return derived


def _xy_distance(
    row: Mapping[str, Any],
    left_x_aliases: Sequence[str],
    left_y_aliases: Sequence[str],
    right_x_aliases: Sequence[str],
    right_y_aliases: Sequence[str],
) -> float | None:
    left_x = _row_value(row, left_x_aliases)
    left_y = _row_value(row, left_y_aliases)
    right_x = _row_value(row, right_x_aliases)
    right_y = _row_value(row, right_y_aliases)
    if None in (left_x, left_y, right_x, right_y):
        return None
    return math.hypot(float(left_x) - float(right_x), float(left_y) - float(right_y))


def _nearest_control_row_with_field(
    sim_time: float | None,
    times: Sequence[float],
    indexed: Sequence[tuple[float, Mapping[str, Any]]],
    aliases: Sequence[str],
) -> Mapping[str, Any] | None:
    candidates = _nearest_control_candidates(sim_time, times, indexed)
    rows_with_field = [row for _, row in candidates if _row_value(row, aliases) is not None]
    return rows_with_field[0] if rows_with_field else None


def _nearest_control_row(
    sim_time: float | None,
    times: Sequence[float],
    indexed: Sequence[tuple[float, Mapping[str, Any]]],
) -> Mapping[str, Any] | None:
    candidates = _nearest_control_candidates(sim_time, times, indexed)
    return candidates[0][1] if candidates else None


def _nearest_control_candidates(
    sim_time: float | None,
    times: Sequence[float],
    indexed: Sequence[tuple[float, Mapping[str, Any]]],
) -> list[tuple[float, Mapping[str, Any]]]:
    if sim_time is None or not times:
        return []
    insert_at = bisect_left(times, sim_time)
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    cursor = insert_at
    while cursor < len(indexed):
        item = indexed[cursor]
        if abs(float(item[0]) - float(sim_time)) > CONTROL_TRACE_MERGE_MAX_DT_S:
            break
        candidates.append(item)
        cursor += 1
    cursor = insert_at - 1
    while cursor >= 0:
        item = indexed[cursor]
        if abs(float(item[0]) - float(sim_time)) > CONTROL_TRACE_MERGE_MAX_DT_S:
            break
        candidates.append(item)
        cursor -= 1
    return [
        item
        for item in sorted(candidates, key=lambda item: abs(float(item[0]) - float(sim_time)))
        if abs(float(item[0]) - float(sim_time)) <= CONTROL_TRACE_MERGE_MAX_DT_S
    ]


def _flatten_mapping(payload: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in payload.items():
        current = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            flat.update(_flatten_mapping(value, current))
        else:
            flat[current] = value
    return flat


def _route_health_curvature_values(payload: Mapping[str, Any]) -> list[float]:
    geometry = payload.get("route_geometry") if isinstance(payload.get("route_geometry"), Mapping) else {}
    curvature = geometry.get("curvature") if isinstance(geometry.get("curvature"), Mapping) else {}
    values: list[float] = []
    for key in ("mean_abs", "p95_abs", "max_abs"):
        value = _num(curvature.get(key) or geometry.get(f"curvature.{key}"))
        if value is not None:
            values.append(value)
    return values


def _route_health_route_id(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("route_id") if isinstance(payload, Mapping) else None
    return None if value in {None, ""} else str(value)


def _drift_window_summary(
    rows: Sequence[Mapping[str, Any]],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    samples = _drift_samples(rows)
    if not samples:
        return {
            "status": "insufficient_data",
            "sample_count": 0,
            "reason": "missing_route_s_or_cross_track_error",
        }
    high_threshold = thresholds["drift_window_high_lateral_abs_m"]
    first = samples[0]
    final = samples[-1]
    max_sample = max(samples, key=lambda sample: abs(sample["cross_track_error"]))
    high_index = next(
        (index for index, sample in enumerate(samples) if abs(sample["cross_track_error"]) >= high_threshold),
        None,
    )
    first_high = samples[high_index] if high_index is not None else None
    summary: dict[str, Any] = {
        "status": "available" if first_high is not None else "no_high_lateral_drift",
        "sample_count": len(samples),
        "high_lateral_threshold_m": high_threshold,
        "start": _sample_location(first),
        "end": _sample_location(final),
        "max_abs_lateral": _sample_location(max_sample),
        "phase_samples": {
            "start": _sample_snapshot(first),
            "first_high_lateral": _sample_snapshot(first_high) if first_high is not None else None,
            "max_abs_lateral": _sample_snapshot(max_sample),
            "end": _sample_snapshot(final),
        },
        "growth_start_to_max_abs_m": abs(max_sample["cross_track_error"]) - abs(first["cross_track_error"]),
        "first_high_lateral": _sample_location(first_high) if first_high is not None else None,
        "growth_start_to_first_high_abs_m": (
            abs(first_high["cross_track_error"]) - abs(first["cross_track_error"]) if first_high is not None else None
        ),
        "context_window_s": thresholds["drift_context_window_s"],
        "context_window_route_s_m": thresholds["drift_context_window_route_s_m"],
    }
    if first_high is not None and high_index is not None:
        summary["first_high_lateral_context"] = _drift_context(
            samples,
            center_index=high_index,
            time_window_s=thresholds["drift_context_window_s"],
            route_s_window_m=thresholds["drift_context_window_route_s_m"],
        )
    max_index = samples.index(max_sample)
    summary["max_abs_lateral_context"] = _drift_context(
        samples,
        center_index=max_index,
        time_window_s=thresholds["drift_context_window_s"],
        route_s_window_m=thresholds["drift_context_window_route_s_m"],
    )
    return summary


def _drift_samples(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, float]]:
    samples: list[dict[str, float]] = []
    seen: set[tuple[float | None, float | None]] = set()
    fields = [
        "sim_time",
        "route_s",
        "cross_track_error",
        "apollo_simple_lat_lateral_error",
        "apollo_simple_lon_current_station",
        "apollo_simple_lon_station_reference",
        "apollo_simple_lat_target_point_s",
        "apollo_simple_lon_matched_point_s",
        "heading_error",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "matched_point_distance",
        "target_point_distance",
        "apollo_target_point_kappa",
        "apollo_planning_first_kappa",
        "reference_lane_curvature",
        "route_curvature",
        "ego_yaw_rate",
    ]
    for row in rows:
        sample = {
            field: _row_value(row, DRIFT_WINDOW_FIELD_ALIASES.get(field, [field]))
            for field in fields
        }
        sample["cross_track_error"] = _row_value(row, ROUTE_LATERAL_ERROR_ALIASES)
        if sample["cross_track_error"] is None:
            continue
        if sample["sim_time"] is None and sample["route_s"] is None:
            continue
        key = (sample["sim_time"], sample["route_s"])
        if key in seen:
            continue
        seen.add(key)
        samples.append({key: value for key, value in sample.items() if value is not None})
    return sorted(samples, key=lambda sample: (sample.get("sim_time", float("inf")), sample.get("route_s", float("inf"))))


def _row_value(row: Mapping[str, Any], aliases: Sequence[str]) -> float | None:
    for alias in aliases:
        value = _num(row.get(alias))
        if value is not None:
            return value
    return None


def _sample_location(sample: Mapping[str, float] | None) -> dict[str, float | None] | None:
    if sample is None:
        return None
    return {
        "sim_time": sample.get("sim_time"),
        "route_s": sample.get("route_s"),
        "cross_track_error": sample.get("cross_track_error"),
        "heading_error": sample.get("heading_error"),
    }


def _sample_snapshot(sample: Mapping[str, float] | None) -> dict[str, float | None] | None:
    if sample is None:
        return None
    fields = [
        "sim_time",
        "route_s",
        "cross_track_error",
        "apollo_simple_lat_lateral_error",
        "apollo_simple_lon_current_station",
        "apollo_simple_lon_station_reference",
        "apollo_simple_lat_target_point_s",
        "apollo_simple_lon_matched_point_s",
        "heading_error",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "matched_point_distance",
        "target_point_distance",
        "apollo_target_point_kappa",
        "apollo_planning_first_kappa",
        "reference_lane_curvature",
        "route_curvature",
        "ego_yaw_rate",
    ]
    return {field: sample.get(field) for field in fields if field in sample}


def _drift_context(
    samples: Sequence[Mapping[str, float]],
    *,
    center_index: int,
    time_window_s: float,
    route_s_window_m: float,
) -> dict[str, Any]:
    center = samples[center_index]
    center_time = center.get("sim_time")
    center_route_s = center.get("route_s")
    window: list[Mapping[str, float]] = []
    for sample in samples:
        in_time = (
            center_time is not None
            and sample.get("sim_time") is not None
            and abs(float(sample["sim_time"]) - float(center_time)) <= time_window_s
        )
        in_route_s = (
            center_route_s is not None
            and sample.get("route_s") is not None
            and abs(float(sample["route_s"]) - float(center_route_s)) <= route_s_window_m
        )
        if in_time or in_route_s:
            window.append(sample)
    if not window:
        window = [center]
    context_fields = [
        "cross_track_error",
        "apollo_simple_lat_lateral_error",
        "apollo_simple_lon_current_station",
        "apollo_simple_lon_station_reference",
        "apollo_simple_lat_target_point_s",
        "apollo_simple_lon_matched_point_s",
        "heading_error",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "matched_point_distance",
        "target_point_distance",
        "apollo_target_point_kappa",
        "apollo_planning_first_kappa",
        "reference_lane_curvature",
        "route_curvature",
        "ego_yaw_rate",
    ]
    return {
        "sample_count": len(window),
        "start": _sample_location(window[0]),
        "end": _sample_location(window[-1]),
        "center": _sample_location(center),
        "stats": {
            field: _stats_abs([sample[field] for sample in window if field in sample])
            for field in context_fields
        },
    }


def _critical_window_samples(
    rows: Sequence[Mapping[str, Any]],
    thresholds: Mapping[str, float],
    *,
    reference_line_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    samples = _drift_samples(rows)
    if not samples:
        return {
            "available": False,
            "status": "insufficient_data",
            "reason": "missing_route_lateral_samples",
            "samples": [],
        }

    lateral_min = float(thresholds["drift_window_high_lateral_abs_m"])
    sign_lateral_min = float(thresholds["lateral_sign_active_abs_m"])
    steer_min = float(thresholds["steer_sign_active_abs"])
    source_steer_high = float(thresholds["high_source_steer_abs"])
    anchor_index: int | None = None
    anchor_reason: str | None = None

    for index, sample in enumerate(samples):
        source_steer = sample.get("apollo_steer_raw")
        cte = sample.get("cross_track_error")
        simple_lat = sample.get("apollo_simple_lat_lateral_error")
        if (
            source_steer is not None
            and cte is not None
            and simple_lat is not None
            and abs(float(source_steer)) >= source_steer_high
            and abs(float(cte)) >= lateral_min
        ):
            anchor_index = index
            anchor_reason = "source_steer_saturation_with_lateral_error"
            break

    for index, sample in enumerate(samples):
        if anchor_index is not None:
            break
        source_steer = sample.get("apollo_steer_raw")
        cte = sample.get("cross_track_error")
        if (
            source_steer is not None
            and cte is not None
            and abs(float(source_steer)) >= source_steer_high
            and abs(float(cte)) >= lateral_min
        ):
            anchor_index = index
            anchor_reason = "source_steer_saturation_with_lateral_error"
            break

    if anchor_index is None:
        for index, sample in enumerate(samples):
            source_steer = sample.get("apollo_steer_raw")
            if source_steer is not None and abs(float(source_steer)) >= source_steer_high:
                anchor_index = index
                anchor_reason = "source_steer_saturation"
                break

    if anchor_index is None:
        for index, sample in enumerate(samples):
            cte = sample.get("cross_track_error")
            if cte is not None and abs(float(cte)) >= lateral_min:
                anchor_index = index
                anchor_reason = "first_high_lateral_error"
                break

    if anchor_index is None:
        return {
            "available": False,
            "status": "no_critical_window",
            "reason": "no_high_source_steer_or_lateral_error_sample",
            "high_source_steer_abs": source_steer_high,
            "high_lateral_threshold_m": lateral_min,
            "samples": [],
        }

    anchor = samples[anchor_index]
    anchor_time = anchor.get("sim_time")
    pre_s = float(thresholds["critical_window_pre_s"])
    post_s = float(thresholds["critical_window_post_s"])
    if anchor_time is not None:
        window = [
            sample
            for sample in samples
            if sample.get("sim_time") is not None
            and float(anchor_time) - pre_s <= float(sample["sim_time"]) <= float(anchor_time) + post_s
        ]
    else:
        start = max(0, anchor_index - 4)
        stop = min(len(samples), anchor_index + 8)
        window = list(samples[start:stop])
    if not window:
        window = [anchor]

    limited = _limit_evenly(window, int(thresholds["critical_window_max_samples"]))
    report = {
        "available": True,
        "status": "available",
        "anchor_reason": anchor_reason,
        "anchor_has_simple_lat": anchor.get("apollo_simple_lat_lateral_error") is not None,
        "anchor": _critical_window_sample(anchor, lateral_min_abs=sign_lateral_min, steer_min_abs=steer_min),
        "sample_count": len(window),
        "returned_sample_count": len(limited),
        "time_window_s": {"pre": pre_s, "post": post_s},
        "samples": [
            _critical_window_sample(sample, lateral_min_abs=sign_lateral_min, steer_min_abs=steer_min)
            for sample in limited
        ],
        "interpretation_boundary": (
            "This is a same-window diagnostic slice only. It aligns route lateral error, "
            "Apollo simple_lat lateral error, source steer, mapped steer, and applied steer; "
            "it does not prove a definitive root cause or justify changing control gains."
        ),
    }
    report["reference_line_context"] = _critical_reference_line_context(
        reference_line_contract or {},
        anchor,
    )
    return report


def _critical_window_sample(
    sample: Mapping[str, float],
    *,
    lateral_min_abs: float,
    steer_min_abs: float,
) -> dict[str, Any]:
    cross_track = sample.get("cross_track_error")
    simple_lat = sample.get("apollo_simple_lat_lateral_error")
    source_steer = sample.get("apollo_steer_raw")
    mapped_steer = sample.get("bridge_steer_mapped")
    applied_steer = sample.get("carla_steer_applied")
    snapshot = _sample_snapshot(sample) or {}
    snapshot.update(
        {
            "source_steer_vs_route_lateral_error": _sign_relation(cross_track, source_steer, steer_min_abs),
            "source_steer_vs_simple_lat_lateral_error": _sign_relation(simple_lat, source_steer, steer_min_abs),
            "route_lateral_error_vs_simple_lat_lateral_error": _sign_relation(
                cross_track,
                simple_lat,
                lateral_min_abs,
            ),
            "mapped_steer_vs_route_lateral_error": _sign_relation(cross_track, mapped_steer, steer_min_abs),
            "applied_steer_vs_route_lateral_error": _sign_relation(cross_track, applied_steer, steer_min_abs),
            "route_simple_lat_abs_magnitude_delta_m": (
                abs(abs(float(cross_track)) - abs(float(simple_lat)))
                if cross_track is not None and simple_lat is not None
                else None
            ),
        }
    )
    return snapshot


def _limit_evenly(items: Sequence[Mapping[str, float]], max_count: int) -> list[Mapping[str, float]]:
    if max_count <= 0 or len(items) <= max_count:
        return list(items)
    if max_count == 1:
        return [items[0]]
    step = (len(items) - 1) / float(max_count - 1)
    indexes = sorted({round(index * step) for index in range(max_count)})
    return [items[index] for index in indexes]


def _critical_reference_line_context(
    reference_line_contract: Mapping[str, Any],
    anchor: Mapping[str, float],
) -> dict[str, Any]:
    if not reference_line_contract:
        return {
            "available": False,
            "reason": "missing_reference_line_contract",
        }
    metrics = reference_line_contract.get("metrics")
    if not isinstance(metrics, Mapping):
        metrics = _nested(reference_line_contract, "planning_trajectory_contract.key_metrics")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    onset = metrics.get("path_fallback_onset") if isinstance(metrics.get("path_fallback_onset"), Mapping) else {}
    path_bound = _nested(
        reference_line_contract,
        "planning_info_log_reference_line_evidence.planning_info_log_path_bound_evidence",
    )
    path_bound = path_bound if isinstance(path_bound, Mapping) else {}
    anchor_time = _num(anchor.get("sim_time"))
    onset_time = _num(onset.get("sim_time_sec")) if isinstance(onset, Mapping) else None
    return {
        "available": True,
        "status": reference_line_contract.get("status"),
        "blocking_reasons": list(reference_line_contract.get("blocking_reasons") or []),
        "path_fallback_trajectory_ratio": _num(metrics.get("path_fallback_trajectory_ratio")),
        "path_fallback_future_lookahead_reverse_heading_ratio": _num(
            metrics.get("path_fallback_future_lookahead_reverse_heading_ratio")
        ),
        "path_fallback_future_lookahead_distance_p95_m": _num(
            metrics.get("path_fallback_future_lookahead_distance_p95_m")
        ),
        "path_fallback_onset": dict(onset) if isinstance(onset, Mapping) else {},
        "critical_anchor_to_path_fallback_onset_dt_s": (
            float(onset_time) - float(anchor_time)
            if onset_time is not None and anchor_time is not None
            else None
        ),
        "path_bound_evidence": {
            "classification": path_bound.get("classification"),
            "decide_path_bound_failed_count": path_bound.get("decide_path_bound_failed_count"),
            "fallback_self_opt_l_out_of_bounds_count": path_bound.get(
                "fallback_self_opt_l_out_of_bounds_count"
            ),
            "fallback_self_opt_l_out_of_bounds_max_margin_m": path_bound.get(
                "fallback_self_opt_l_out_of_bounds_max_margin_m"
            ),
            "first_fallback_path_bound_violation": path_bound.get(
                "first_fallback_path_bound_violation"
            ),
        },
        "interpretation_boundary": (
            "This context aligns the lateral-semantics critical window with the "
            "reference-line contract summary. It is diagnostic; it does not prove "
            "the exact Planning root cause without raw Planning row inspection."
        ),
    }


def _lateral_sign_alignment_summary(
    rows: Sequence[Mapping[str, Any]],
    thresholds: Mapping[str, float],
    *,
    hdmap_route_lateral_consistency: Mapping[str, Any] | None = None,
    hdmap_projection_rows: Sequence[Mapping[str, Any]] | None = None,
    route_definition_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not rows:
        return {
            "available": False,
            "status": "insufficient_data",
            "reason": "missing_rows",
        }
    lateral_min = thresholds["lateral_sign_active_abs_m"]
    steer_min = thresholds["steer_sign_active_abs"]
    pairs = {
        "source_steer_vs_route_lateral_error": _sign_pair_stats(
            rows,
            ROUTE_LATERAL_ERROR_ALIASES,
            FIELD_ALIASES["apollo_steer_raw"],
            left_min_abs=lateral_min,
            right_min_abs=steer_min,
        ),
        "source_steer_vs_simple_lat_lateral_error": _sign_pair_stats(
            rows,
            FIELD_ALIASES["apollo_simple_lat_lateral_error"],
            FIELD_ALIASES["apollo_steer_raw"],
            left_min_abs=lateral_min,
            right_min_abs=steer_min,
        ),
        "route_lateral_error_vs_simple_lat_lateral_error": _sign_pair_stats(
            rows,
            ROUTE_LATERAL_ERROR_ALIASES,
            FIELD_ALIASES["apollo_simple_lat_lateral_error"],
            left_min_abs=lateral_min,
            right_min_abs=lateral_min,
        ),
        "mapped_steer_vs_route_lateral_error": _sign_pair_stats(
            rows,
            ROUTE_LATERAL_ERROR_ALIASES,
            FIELD_ALIASES["bridge_steer_mapped"],
            left_min_abs=lateral_min,
            right_min_abs=steer_min,
        ),
        "applied_steer_vs_route_lateral_error": _sign_pair_stats(
            rows,
            ROUTE_LATERAL_ERROR_ALIASES,
            FIELD_ALIASES["carla_steer_applied"],
            left_min_abs=lateral_min,
            right_min_abs=steer_min,
        ),
    }
    first_high_alignment = _first_high_lateral_sign_alignment(
        rows,
        lateral_min_abs=thresholds["drift_window_high_lateral_abs_m"],
        steer_min_abs=steer_min,
    )
    route_simple_magnitude = _route_simple_lat_magnitude_alignment(
        rows,
        lateral_min_abs=lateral_min,
        high_ratio=thresholds["sign_alignment_ratio_high"],
        min_samples=int(thresholds["sign_alignment_min_samples"]),
        abs_delta_p95_threshold_m=thresholds["route_simple_lat_sign_convention_abs_delta_p95_m"],
    )
    route_lateral_provenance = _route_lateral_provenance(
        rows,
        hdmap_route_lateral_consistency=hdmap_route_lateral_consistency or {},
        route_definition_payload=route_definition_payload or {},
        thresholds=thresholds,
    )
    official_projection_alignment = _official_hdmap_projection_alignment(
        rows,
        hdmap_projection_rows or [],
        lateral_min_abs=lateral_min,
        thresholds=thresholds,
    )
    route_station_alignment = _route_station_frame_alignment(
        rows,
        official_projection_alignment,
        thresholds=thresholds,
    )
    lateral_frame_convention = _lateral_frame_convention_diagnostic(
        route_simple_pair=pairs["route_lateral_error_vs_simple_lat_lateral_error"],
        route_simple_magnitude=route_simple_magnitude,
        official_projection_alignment=official_projection_alignment,
        route_lateral_provenance=route_lateral_provenance,
        route_station_alignment=route_station_alignment,
        thresholds=thresholds,
    )
    route_lateral_field_semantics = _route_lateral_field_semantics_recommendation(
        route_lateral_provenance=route_lateral_provenance,
        lateral_frame_convention=lateral_frame_convention,
    )
    sample_count = max((int(value["sample_count"]) for value in pairs.values()), default=0)
    return {
        "available": sample_count > 0,
        "status": "available" if sample_count > 0 else "insufficient_data",
        "lateral_min_abs_m": lateral_min,
        "steer_min_abs": steer_min,
        "first_high_lateral_sample": first_high_alignment,
        "interpretation_boundary": (
            "Same-sign steer/lateral-error alignment is only a convention and semantics "
            "sense-check. It must be interpreted with the route frame, steering convention, "
            "and raw/mapped/applied control chain before any runtime control change."
        ),
        "route_lateral_provenance": route_lateral_provenance,
        "route_simple_lat_magnitude_alignment": route_simple_magnitude,
        "official_hdmap_projection_alignment": official_projection_alignment,
        "route_station_frame_alignment": route_station_alignment,
        "lateral_frame_convention_diagnostic": lateral_frame_convention,
        "route_lateral_field_semantics": route_lateral_field_semantics,
        **pairs,
    }


def _lateral_frame_convention_diagnostic(
    *,
    route_simple_pair: Mapping[str, Any],
    route_simple_magnitude: Mapping[str, Any],
    official_projection_alignment: Mapping[str, Any],
    route_lateral_provenance: Mapping[str, Any],
    route_station_alignment: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    route_projection = official_projection_alignment.get("route_lateral_vs_projection_lateral")
    if not isinstance(route_projection, Mapping):
        route_projection = {}
    simple_projection = official_projection_alignment.get("simple_lat_vs_projection_lateral")
    if not isinstance(simple_projection, Mapping):
        simple_projection = {}
    projection_route_contract = route_lateral_provenance.get("projection_route_sample_sign_contract")
    if not isinstance(projection_route_contract, Mapping):
        projection_route_contract = {}
    projection_route_timeseries = projection_route_contract.get(
        "timeseries_lateral_vs_projection_route_sample_signed_lateral"
    )
    if not isinstance(projection_route_timeseries, Mapping):
        projection_route_timeseries = {}
    projection_route_simple = projection_route_contract.get(
        "simple_lat_vs_projection_route_sample_signed_lateral"
    )
    if not isinstance(projection_route_simple, Mapping):
        projection_route_simple = {}

    min_samples = int(thresholds["sign_alignment_min_samples"])
    high_ratio = float(thresholds["sign_alignment_ratio_high"])
    route_projection_samples = int(route_projection.get("sample_count") or 0)
    simple_projection_samples = int(simple_projection.get("sample_count") or 0)
    route_simple_samples = int(route_simple_pair.get("sample_count") or 0)
    route_projection_opposite = _num(route_projection.get("opposite_sign_ratio"))
    route_projection_same = _num(route_projection.get("same_sign_ratio"))
    simple_projection_same = _num(simple_projection.get("same_sign_ratio"))
    simple_projection_opposite = _num(simple_projection.get("opposite_sign_ratio"))
    route_simple_opposite = _num(route_simple_pair.get("opposite_sign_ratio"))
    route_simple_same = _num(route_simple_pair.get("same_sign_ratio"))
    magnitude_candidate = bool(route_simple_magnitude.get("magnitude_agreement_candidate"))

    has_projection_evidence = (
        route_projection_samples >= min_samples and simple_projection_samples >= min_samples
    )
    has_route_simple_evidence = route_simple_samples >= min_samples
    if not has_projection_evidence:
        status = "insufficient_data"
        classification = "official_projection_lateral_sign_evidence_missing"
        reason = "official_apollo_hdmap_projection_rows_missing_or_too_sparse"
    elif (
        route_projection_opposite is not None
        and route_projection_opposite >= high_ratio
        and simple_projection_same is not None
        and simple_projection_same >= high_ratio
        and route_simple_opposite is not None
        and route_simple_opposite >= high_ratio
        and magnitude_candidate
    ):
        status = "available"
        classification = "route_lateral_sign_inverted_vs_apollo_projection_candidate"
        reason = (
            "CARLA route lateral error opposes official Apollo projection_l while Apollo "
            "simple_lat lateral error matches projection_l; route/simple_lat magnitudes also agree."
        )
    elif (
        route_projection_same is not None
        and route_projection_same >= high_ratio
        and simple_projection_same is not None
        and simple_projection_same >= high_ratio
    ):
        status = "available"
        classification = "route_lateral_matches_apollo_projection_candidate"
        reason = "CARLA route lateral error and Apollo simple_lat both match official Apollo projection_l."
    elif (
        simple_projection_opposite is not None
        and simple_projection_opposite >= high_ratio
    ):
        status = "warn"
        classification = "simple_lat_sign_opposes_apollo_projection_candidate"
        reason = "Apollo simple_lat lateral error opposes official Apollo projection_l."
    elif (
        route_projection_opposite is not None
        and route_projection_opposite >= high_ratio
        and not has_route_simple_evidence
    ):
        status = "warn"
        classification = "route_lateral_opposes_projection_without_simple_lat_confirmation"
        reason = "CARLA route lateral error opposes official Apollo projection_l, but simple_lat evidence is sparse."
    elif (
        route_simple_same is not None
        and route_simple_same >= high_ratio
        and route_projection_opposite is not None
        and route_projection_opposite >= high_ratio
    ):
        status = "warn"
        classification = "route_and_simple_lat_jointly_oppose_projection_candidate"
        reason = "Route and simple_lat agree with each other but oppose official Apollo projection_l."
    else:
        status = "warn"
        classification = "lateral_frame_convention_ambiguous"
        reason = "Lateral sign ratios are mixed or below the diagnostic threshold."

    return {
        "status": status,
        "classification": classification,
        "reason": reason,
        "thresholds": {
            "min_samples": min_samples,
            "high_ratio": high_ratio,
        },
        "route_lateral_vs_projection_lateral": {
            "sample_count": route_projection_samples,
            "same_sign_ratio": route_projection_same,
            "opposite_sign_ratio": route_projection_opposite,
            "abs_magnitude_delta_p95_m": route_projection.get("abs_magnitude_delta_p95_m"),
            "opposite_sign_abs_sum_p95_m": route_projection.get("opposite_sign_abs_sum_p95_m"),
        },
        "simple_lat_vs_projection_lateral": {
            "sample_count": simple_projection_samples,
            "same_sign_ratio": simple_projection_same,
            "opposite_sign_ratio": simple_projection_opposite,
        },
        "route_lateral_vs_simple_lat_lateral_error": {
            "sample_count": route_simple_samples,
            "same_sign_ratio": route_simple_same,
            "opposite_sign_ratio": route_simple_opposite,
        },
        "route_simple_lat_magnitude_agreement_candidate": magnitude_candidate,
        "route_simple_lat_abs_magnitude_delta_p95_m": route_simple_magnitude.get(
            "abs_magnitude_delta_p95_m"
        ),
        "route_simple_lat_opposite_sign_abs_sum_p95_m": route_simple_magnitude.get(
            "opposite_sign_abs_sum_p95_m"
        ),
        "route_lateral_provenance_evidence_level": route_lateral_provenance.get("evidence_level"),
        "route_lateral_source_field": route_lateral_provenance.get("route_lateral_source_field"),
        "projection_route_sample_sign_contract_status": projection_route_contract.get("status"),
        "projection_route_sample_sign_contract_classification": projection_route_contract.get("classification"),
        "projection_route_sample_timeseries_opposite_sign_ratio": projection_route_timeseries.get(
            "opposite_sign_ratio"
        ),
        "projection_route_sample_simple_lat_same_sign_ratio": projection_route_simple.get(
            "same_sign_ratio"
        ),
        "route_station_frame_classification": route_station_alignment.get("classification"),
        "interpretation": _lateral_frame_convention_interpretation(classification),
        "claim_boundary": (
            "This is a lateral frame/sign-convention diagnostic. It can explain why route-level "
            "cross-track metrics disagree with Apollo simple_lat/projection metrics, but it does "
            "not prove behavior success and must not flip signs in runtime without a separate "
            "route-frame contract update."
        ),
    }


def _lateral_frame_convention_interpretation(classification: str) -> str:
    if classification == "route_lateral_sign_inverted_vs_apollo_projection_candidate":
        return (
            "The current evidence points to CARLA route cross-track using the opposite sign "
            "from Apollo HDMap projection_l/simple_lat lateral error for this run."
        )
    if classification == "route_lateral_matches_apollo_projection_candidate":
        return "Route cross-track, Apollo projection_l, and simple_lat lateral error share the same sign."
    if classification == "simple_lat_sign_opposes_apollo_projection_candidate":
        return "Apollo simple_lat lateral error appears sign-inverted relative to official projection_l."
    if classification == "route_lateral_opposes_projection_without_simple_lat_confirmation":
        return "Route cross-track opposes official projection_l, but simple_lat coverage is insufficient."
    if classification == "route_and_simple_lat_jointly_oppose_projection_candidate":
        return "Route cross-track and simple_lat agree with each other but oppose official projection_l."
    if classification == "lateral_frame_convention_ambiguous":
        return "The available lateral sign evidence is mixed or below the confidence threshold."
    return "Official projection lateral sign evidence is missing or too sparse."


def _route_lateral_field_semantics_recommendation(
    *,
    route_lateral_provenance: Mapping[str, Any],
    lateral_frame_convention: Mapping[str, Any],
) -> dict[str, Any]:
    projection_contract = route_lateral_provenance.get("projection_route_sample_sign_contract")
    if not isinstance(projection_contract, Mapping):
        projection_contract = {}
    contract_classification = str(projection_contract.get("classification") or "")
    convention_classification = str(lateral_frame_convention.get("classification") or "")
    source_field = route_lateral_provenance.get(
        "route_lateral_source_field"
    ) or projection_contract.get("route_lateral_source_field")
    projection_confirmed = (
        contract_classification
        == "timeseries_route_lateral_sign_inverted_vs_projection_route_samples"
    )
    projection_same = (
        contract_classification
        == "timeseries_route_lateral_matches_projection_route_samples"
    )
    if projection_confirmed:
        status = "available"
        classification = "route_lateral_field_opposite_signed_to_apollo_projection"
        sign_sensitive_gate_allowed = False
        absolute_magnitude_gate_allowed = True
        recommended_gate_policy = "absolute_magnitude_only_until_canonical_sign_declared"
        recommended_field_action = "relabel_or_explicitly_convert_before_sign_sensitive_gate"
        reason = (
            "Projection-route samples confirm this run's route-lateral field has the opposite "
            "sign from Apollo projection/simple_lat lateral semantics."
        )
    elif projection_same:
        status = "available"
        classification = "route_lateral_field_matches_apollo_projection"
        sign_sensitive_gate_allowed = True
        absolute_magnitude_gate_allowed = True
        recommended_gate_policy = "signed_and_absolute_metrics_allowed_for_this_contract"
        recommended_field_action = "document_canonical_sign_convention_before_hard_gate"
        reason = "Projection-route samples confirm this run's route-lateral field matches Apollo projection semantics."
    elif convention_classification == "route_lateral_sign_inverted_vs_apollo_projection_candidate":
        status = "warn"
        classification = "route_lateral_field_sign_inversion_candidate_unconfirmed_by_route_samples"
        sign_sensitive_gate_allowed = False
        absolute_magnitude_gate_allowed = True
        recommended_gate_policy = "absolute_magnitude_only_pending_projection_route_contract"
        recommended_field_action = "materialize_projection_route_samples_or_route_geometry"
        reason = "Official projection pairing suggests sign inversion, but projection-route sample contract is not yet available."
    elif source_field:
        status = "warn"
        classification = "route_lateral_field_semantics_ambiguous"
        sign_sensitive_gate_allowed = False
        absolute_magnitude_gate_allowed = True
        recommended_gate_policy = "absolute_magnitude_only_pending_semantics_contract"
        recommended_field_action = "declare_route_lateral_frame_and_sign_convention"
        reason = "A route-lateral field is present, but its signed semantics are not contract-grade."
    else:
        status = "insufficient_data"
        classification = "route_lateral_field_missing"
        sign_sensitive_gate_allowed = False
        absolute_magnitude_gate_allowed = False
        recommended_gate_policy = "do_not_use_route_lateral_field"
        recommended_field_action = "record_route_lateral_field_with_declared_frame_and_sign"
        reason = "No route-lateral field is available."
    return {
        "status": status,
        "classification": classification,
        "source_field": source_field,
        "projection_route_sample_sign_contract_classification": contract_classification or None,
        "lateral_frame_convention_classification": convention_classification or None,
        "sign_sensitive_gate_allowed": sign_sensitive_gate_allowed,
        "absolute_magnitude_gate_allowed": absolute_magnitude_gate_allowed,
        "recommended_gate_policy": recommended_gate_policy,
        "recommended_field_action": recommended_field_action,
        "reason": reason,
        "claim_boundary": (
            "This is a field-semantics recommendation for analysis/gating. It does not change "
            "runtime control, does not prove behavior success, and must not be used to flip "
            "control signs without a separate runtime contract."
        ),
    }


def _route_station_frame_alignment(
    rows: Sequence[Mapping[str, Any]],
    official_projection_alignment: Mapping[str, Any],
    *,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    route_current = _paired_abs_delta_stats(
        rows,
        DRIFT_WINDOW_FIELD_ALIASES["route_s"],
        FIELD_ALIASES["apollo_simple_lon_current_station"],
    )
    route_target = _paired_abs_delta_stats(
        rows,
        DRIFT_WINDOW_FIELD_ALIASES["route_s"],
        FIELD_ALIASES["apollo_simple_lat_target_point_s"],
    )
    station_vs_projection = {}
    if isinstance(official_projection_alignment, Mapping):
        station_vs_projection = official_projection_alignment.get("simple_lat_station_vs_projection_s") or {}
        if not isinstance(station_vs_projection, Mapping):
            station_vs_projection = {}
    route_current_p95 = _num(route_current.get("p95"))
    route_target_p95 = _num(route_target.get("p95"))
    projection_classification = station_vs_projection.get("station_frame_classification")
    current_projection_p95 = _num(station_vs_projection.get("current_station_minus_projection_s_abs_p95_m"))
    route_aligned = (
        route_current_p95 is not None
        and route_current_p95 <= thresholds["station_lane_s_alignment_p95_m"]
    )
    target_aligned = (
        route_target_p95 is None
        or route_target_p95 <= thresholds["station_target_current_alignment_p95_m"]
    )
    if route_current.get("count", 0) <= 0:
        status = "insufficient_data"
        classification = "route_s_station_alignment_missing"
    elif route_aligned and projection_classification == "local_station_frame_offset_candidate":
        status = "available"
        classification = "route_s_and_simple_lat_share_local_station_frame_candidate"
    elif route_aligned and projection_classification == "lane_s_aligned":
        status = "available"
        classification = "route_s_and_simple_lat_lane_s_aligned_candidate"
    elif route_aligned and target_aligned:
        status = "available"
        classification = "route_s_simple_lat_station_aligned_projection_unknown"
    elif (
        route_current_p95 is not None
        and route_current_p95 >= thresholds["route_s_station_delta_high_p95_m"]
    ):
        status = "warn"
        classification = "route_s_simple_lat_station_mismatch"
    else:
        status = "warn"
        classification = "route_s_simple_lat_station_ambiguous"
    return {
        "status": status,
        "classification": classification,
        "route_s_current_station_abs_delta": route_current,
        "route_s_target_point_s_abs_delta": route_target,
        "simple_lat_station_vs_projection_s_classification": projection_classification,
        "simple_lat_current_station_projection_s_delta_p95_m": current_projection_p95,
        "thresholds": {
            "station_lane_s_alignment_p95_m": thresholds["station_lane_s_alignment_p95_m"],
            "station_target_current_alignment_p95_m": thresholds[
                "station_target_current_alignment_p95_m"
            ],
            "route_s_station_delta_high_p95_m": thresholds["route_s_station_delta_high_p95_m"],
        },
        "interpretation": _route_station_frame_interpretation(classification),
        "claim_boundary": (
            "This compares run-local route_s with Apollo simple_lat station fields and, "
            "when available, official HDMap projection_s. It is frame-origin diagnostics "
            "only and does not prove reference-line validity or behavior success."
        ),
    }


def _route_station_frame_interpretation(classification: str) -> str:
    if classification == "route_s_and_simple_lat_share_local_station_frame_candidate":
        return (
            "Run-local route_s and Apollo simple_lat station are close to each other while "
            "both are offset from official HDMap projection_s; treat route_s/simple_lat "
            "as a shared local or stitching station frame candidate."
        )
    if classification == "route_s_and_simple_lat_lane_s_aligned_candidate":
        return (
            "Run-local route_s, Apollo simple_lat station, and official projection_s appear "
            "compatible under current thresholds."
        )
    if classification == "route_s_simple_lat_station_aligned_projection_unknown":
        return (
            "Run-local route_s and Apollo simple_lat station align, but official projection_s "
            "evidence is missing or ambiguous."
        )
    if classification == "route_s_simple_lat_station_mismatch":
        return "Run-local route_s and Apollo simple_lat station differ materially."
    if classification == "route_s_simple_lat_station_ambiguous":
        return "Run-local route_s and Apollo simple_lat station are partially aligned or ambiguous."
    return "Route/station frame alignment cannot be classified because route_s or station data is missing."


def _first_high_lateral_sign_alignment(
    rows: Sequence[Mapping[str, Any]],
    *,
    lateral_min_abs: float,
    steer_min_abs: float,
) -> dict[str, Any]:
    samples = _drift_samples(rows)
    first_high = next(
        (sample for sample in samples if abs(float(sample.get("cross_track_error", 0.0))) >= lateral_min_abs),
        None,
    )
    if first_high is None:
        return {
            "available": False,
            "reason": "no_high_lateral_sample",
            "lateral_min_abs_m": lateral_min_abs,
        }
    cross_track = first_high.get("cross_track_error")
    simple_lat = first_high.get("apollo_simple_lat_lateral_error")
    source_steer = first_high.get("apollo_steer_raw")
    mapped_steer = first_high.get("bridge_steer_mapped")
    applied_steer = first_high.get("carla_steer_applied")
    return {
        "available": True,
        "sim_time": first_high.get("sim_time"),
        "route_s": first_high.get("route_s"),
        "cross_track_error": cross_track,
        "apollo_simple_lat_lateral_error": simple_lat,
        "apollo_steer_raw": source_steer,
        "bridge_steer_mapped": mapped_steer,
        "carla_steer_applied": applied_steer,
        "source_steer_vs_route_lateral_error": _sign_relation(cross_track, source_steer, steer_min_abs),
        "source_steer_vs_simple_lat_lateral_error": _sign_relation(simple_lat, source_steer, steer_min_abs),
        "route_lateral_error_vs_simple_lat_lateral_error": _sign_relation(cross_track, simple_lat, lateral_min_abs),
        "mapped_steer_vs_route_lateral_error": _sign_relation(cross_track, mapped_steer, steer_min_abs),
        "applied_steer_vs_route_lateral_error": _sign_relation(cross_track, applied_steer, steer_min_abs),
        "route_simple_lat_abs_sum_m": (
            abs(float(cross_track) + float(simple_lat))
            if cross_track is not None and simple_lat is not None
            else None
        ),
        "route_simple_lat_abs_magnitude_delta_m": (
            abs(abs(float(cross_track)) - abs(float(simple_lat)))
            if cross_track is not None and simple_lat is not None
            else None
        ),
    }


def _sign_relation(left: float | None, right: float | None, active_min_abs: float) -> str:
    if left is None or right is None:
        return "missing"
    if abs(float(right)) < active_min_abs:
        return "inactive"
    product = float(left) * float(right)
    if product > 0.0:
        return "same_sign"
    if product < 0.0:
        return "opposite_sign"
    return "zero"


def _sign_pair_stats(
    rows: Sequence[Mapping[str, Any]],
    left_aliases: Sequence[str],
    right_aliases: Sequence[str],
    *,
    left_min_abs: float,
    right_min_abs: float,
) -> dict[str, Any]:
    same_sign = 0
    opposite_sign = 0
    ignored_inactive = 0
    for row in rows:
        left = _row_value(row, left_aliases)
        right = _row_value(row, right_aliases)
        if left is None or right is None:
            continue
        if abs(left) < left_min_abs or abs(right) < right_min_abs:
            ignored_inactive += 1
            continue
        if left * right > 0.0:
            same_sign += 1
        elif left * right < 0.0:
            opposite_sign += 1
        else:
            ignored_inactive += 1
    sample_count = same_sign + opposite_sign
    return {
        "sample_count": sample_count,
        "same_sign_count": same_sign,
        "opposite_sign_count": opposite_sign,
        "same_sign_ratio": (same_sign / sample_count) if sample_count else None,
        "opposite_sign_ratio": (opposite_sign / sample_count) if sample_count else None,
        "ignored_inactive_count": ignored_inactive,
    }


def _route_simple_lat_magnitude_alignment(
    rows: Sequence[Mapping[str, Any]],
    *,
    lateral_min_abs: float,
    high_ratio: float,
    min_samples: int,
    abs_delta_p95_threshold_m: float,
) -> dict[str, Any]:
    opposite_abs_sum: list[float] = []
    same_abs_delta: list[float] = []
    abs_magnitude_delta: list[float] = []
    same_sign = 0
    opposite_sign = 0
    ignored_inactive = 0
    for row in rows:
        route_lateral = _row_value(row, ROUTE_LATERAL_ERROR_ALIASES)
        simple_lat = _row_value(row, FIELD_ALIASES["apollo_simple_lat_lateral_error"])
        if route_lateral is None or simple_lat is None:
            continue
        if abs(route_lateral) < lateral_min_abs or abs(simple_lat) < lateral_min_abs:
            ignored_inactive += 1
            continue
        abs_magnitude_delta.append(abs(abs(route_lateral) - abs(simple_lat)))
        product = route_lateral * simple_lat
        if product < 0.0:
            opposite_sign += 1
            opposite_abs_sum.append(abs(route_lateral + simple_lat))
        elif product > 0.0:
            same_sign += 1
            same_abs_delta.append(abs(route_lateral - simple_lat))
        else:
            ignored_inactive += 1
    sample_count = same_sign + opposite_sign
    opposite_ratio = (opposite_sign / sample_count) if sample_count else None
    opposite_abs_sum_p95 = _percentile(opposite_abs_sum, 0.95)
    abs_magnitude_delta_p95 = _percentile(abs_magnitude_delta, 0.95)
    candidate = bool(
        sample_count >= min_samples
        and opposite_ratio is not None
        and opposite_ratio >= high_ratio
        and opposite_abs_sum_p95 is not None
        and opposite_abs_sum_p95 <= abs_delta_p95_threshold_m
        and abs_magnitude_delta_p95 is not None
        and abs_magnitude_delta_p95 <= abs_delta_p95_threshold_m
    )
    return {
        "available": sample_count > 0,
        "sample_count": sample_count,
        "same_sign_count": same_sign,
        "opposite_sign_count": opposite_sign,
        "opposite_sign_ratio": opposite_ratio,
        "ignored_inactive_count": ignored_inactive,
        "opposite_sign_abs_sum_p95_m": opposite_abs_sum_p95,
        "same_sign_abs_delta_p95_m": _percentile(same_abs_delta, 0.95),
        "abs_magnitude_delta_p95_m": abs_magnitude_delta_p95,
        "magnitude_agreement_candidate": candidate,
        "threshold_abs_delta_p95_m": abs_delta_p95_threshold_m,
        "interpretation": (
            "opposite_sign_matching_magnitude_suggests_route_simple_lat_sign_convention_mismatch"
            if candidate
            else "insufficient_or_nonmatching_route_simple_lat_magnitude_evidence"
        ),
    }


def _route_lateral_provenance(
    rows: Sequence[Mapping[str, Any]],
    *,
    hdmap_route_lateral_consistency: Mapping[str, Any],
    route_definition_payload: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    route_lateral_source = _resolved_field(rows, ROUTE_LATERAL_ERROR_ALIASES)
    route_definition = _route_definition_geometry_summary(route_definition_payload)
    projection_route_contract = _projection_route_sample_sign_contract(
        rows,
        route_definition_payload,
        lateral_min_abs=thresholds["lateral_sign_active_abs_m"],
        high_ratio=thresholds["sign_alignment_ratio_high"],
        min_samples=int(thresholds["sign_alignment_min_samples"]),
    )
    route_geometry_count = 0
    route_geometry_cte_deltas: list[float] = []
    for row in rows:
        ego_x = _row_value(row, FIELD_ALIASES["ego_x"])
        ego_y = _row_value(row, FIELD_ALIASES["ego_y"])
        route_x = _row_value(row, FIELD_ALIASES["route_x"])
        route_y = _row_value(row, FIELD_ALIASES["route_y"])
        route_heading = _row_value(row, ["route_heading"])
        route_lateral = _row_value(row, ROUTE_LATERAL_ERROR_ALIASES)
        if None in {ego_x, ego_y, route_x, route_y, route_heading}:
            continue
        route_geometry_count += 1
        if route_lateral is not None:
            recomputed = -math.sin(float(route_heading)) * (float(ego_x) - float(route_x)) + math.cos(
                float(route_heading)
            ) * (float(ego_y) - float(route_y))
            route_geometry_cte_deltas.append(abs(float(route_lateral) - recomputed))
    hdmap_status = hdmap_route_lateral_consistency.get("status")
    hdmap_alignment = hdmap_route_lateral_consistency.get("alignment_mode")
    hdmap_delta = hdmap_route_lateral_consistency.get("best_abs_delta_p95_m")
    if route_geometry_count:
        interpretation = "route_geometry_fields_allow_signed_cte_recompute"
        evidence_level = "route_geometry_available"
    elif hdmap_status == "pass":
        interpretation = "route_lateral_sign_supported_by_hdmap_projection_consistency"
        evidence_level = "hdmap_projection_consistency"
    elif route_lateral_source:
        interpretation = "route_lateral_alias_present_without_geometry_or_projection_provenance"
        evidence_level = "alias_only"
    else:
        interpretation = "route_lateral_source_missing"
        evidence_level = "missing"
    return {
        "route_lateral_source_field": route_lateral_source,
        "route_geometry_fields_available": route_geometry_count > 0,
        "route_geometry_sample_count": route_geometry_count,
        "route_geometry_recomputed_cte_abs_delta_p95_m": _percentile(route_geometry_cte_deltas, 0.95),
        "route_definition_geometry_status": route_definition["status"],
        "route_definition_schema_version": route_definition["schema_version"],
        "route_definition_status": route_definition["route_status"],
        "route_definition_point_count": route_definition["point_count"],
        "route_definition_sample_count": route_definition["sample_count"],
        "route_definition_declared_sample_count": route_definition["declared_sample_count"],
        "route_definition_reason": route_definition["reason"],
        "projection_route_sample_sign_contract": projection_route_contract,
        "hdmap_route_lateral_consistency_status": hdmap_status,
        "hdmap_route_lateral_alignment_mode": hdmap_alignment,
        "hdmap_route_lateral_delta_p95_m": hdmap_delta,
        "evidence_level": evidence_level,
        "interpretation": interpretation,
        "claim_boundary": (
            "Route/simple_lat sign diagnostics are attribution evidence. Without route_x/route_y/"
            "route_heading samples, they identify a convention candidate but do not verify the "
            "route-frame sign convention from route geometry alone."
        ),
    }


def _route_definition_geometry_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or not payload:
        return {
            "status": "missing",
            "schema_version": None,
            "route_status": None,
            "point_count": 0,
            "sample_count": 0,
            "declared_sample_count": None,
            "reason": "route_definition_artifact_missing",
        }
    points = payload.get("points")
    samples = payload.get("scenario_route_samples")
    point_count = len(points) if isinstance(points, list) else 0
    sample_count = len(samples) if isinstance(samples, list) else 0
    declared_sample_count = payload.get("scenario_route_sample_count")
    schema_version = payload.get("schema_version")
    route_status = payload.get("status")
    if point_count > 0 or sample_count > 0:
        status = "materialized_geometry_available"
        reason = "route_definition_contains_materialized_points_or_samples"
    elif schema_version == "route_stub.v1" or route_status == "insufficient_data":
        status = "stub_or_insufficient"
        reason = "route_definition_stub_cannot_support_route_geometry_recompute"
    elif declared_sample_count:
        status = "declared_only"
        reason = "route_definition_declares_sample_count_without_materialized_samples"
    else:
        status = "no_materialized_geometry"
        reason = "route_definition_has_no_points_or_samples"
    return {
        "status": status,
        "schema_version": schema_version,
        "route_status": route_status,
        "point_count": point_count,
        "sample_count": sample_count,
        "declared_sample_count": declared_sample_count,
        "reason": reason,
    }


def _projection_route_sample_sign_contract(
    rows: Sequence[Mapping[str, Any]],
    route_definition_payload: Mapping[str, Any],
    *,
    lateral_min_abs: float,
    high_ratio: float,
    min_samples: int,
) -> dict[str, Any]:
    route_samples = _projection_route_sample_points(route_definition_payload)
    route_lateral_aliases = [
        "cross_track_error",
        "route_cross_track_error",
        "route_lateral_error",
        "lateral_error",
        "e_y_m",
    ]
    route_lateral_source = _resolved_field(rows, route_lateral_aliases)
    map_x_source = _resolved_field(rows, ["map_x", "localization_x", "apollo_localization_x"])
    map_y_source = _resolved_field(rows, ["map_y", "localization_y", "apollo_localization_y"])
    simple_lat_source = _resolved_field(rows, FIELD_ALIASES["apollo_simple_lat_lateral_error"])
    if not route_samples:
        return {
            "available": False,
            "status": "insufficient_data",
            "classification": "projection_route_sample_sign_contract_missing",
            "reason": "route_definition_has_no_apollo_map_route_samples",
            "route_sample_count": 0,
            "matched_sample_count": 0,
            "route_lateral_source_field": route_lateral_source,
            "map_position_source_fields": {"x": map_x_source, "y": map_y_source},
            "simple_lat_source_field": simple_lat_source,
        }
    if route_lateral_source is None or map_x_source is None or map_y_source is None:
        missing = []
        if route_lateral_source is None:
            missing.append("timeseries_lateral")
        if map_x_source is None or map_y_source is None:
            missing.append("apollo_map_position")
        return {
            "available": False,
            "status": "insufficient_data",
            "classification": "projection_route_sample_sign_contract_missing_fields",
            "reason": "same-row Apollo-map ego position and lateral field are required",
            "missing_fields": missing,
            "route_sample_count": len(route_samples),
            "matched_sample_count": 0,
            "route_sample_source": _route_sample_source_summary(route_samples),
            "route_sample_frame": _route_sample_frame_summary(route_samples),
            "route_lateral_source_field": route_lateral_source,
            "map_position_source_fields": {"x": map_x_source, "y": map_y_source},
            "simple_lat_source_field": simple_lat_source,
        }

    ts_same = 0
    ts_opposite = 0
    simple_same = 0
    simple_opposite = 0
    ignored_inactive = 0
    signed_abs: list[float] = []
    route_lateral_abs: list[float] = []
    simple_lat_abs: list[float] = []
    ts_abs_magnitude_delta: list[float] = []
    ts_opposite_abs_sum: list[float] = []
    simple_abs_magnitude_delta: list[float] = []
    simple_opposite_abs_sum: list[float] = []
    nearest_distances: list[float] = []
    sample_preview: list[dict[str, Any]] = []

    for row in rows:
        x = _row_value(row, ["map_x", "localization_x", "apollo_localization_x"])
        y = _row_value(row, ["map_y", "localization_y", "apollo_localization_y"])
        route_lateral = _row_value(row, route_lateral_aliases)
        if x is None or y is None or route_lateral is None:
            continue
        nearest = _nearest_projection_route_sample(float(x), float(y), route_samples)
        if nearest is None:
            continue
        signed = _signed_lateral_from_route_sample(float(x), float(y), nearest)
        distance = math.hypot(float(x) - nearest["x"], float(y) - nearest["y"])
        nearest_distances.append(distance)
        signed_abs.append(abs(signed))
        route_lateral_abs.append(abs(float(route_lateral)))
        simple_lat = _row_value(row, FIELD_ALIASES["apollo_simple_lat_lateral_error"])
        if simple_lat is not None:
            simple_lat_abs.append(abs(float(simple_lat)))
        row_preview = {
            "sim_time": _row_value(row, ["sim_time", "ts_sec", "timestamp", "time"]),
            "map_x": float(x),
            "map_y": float(y),
            "nearest_route_sample_index": nearest.get("index"),
            "nearest_route_s": nearest.get("route_s"),
            "nearest_route_sample_distance_m": distance,
            "projection_route_sample_signed_lateral_m": signed,
            "timeseries_lateral_m": float(route_lateral),
            "apollo_simple_lat_lateral_error_m": simple_lat,
        }
        ts_relation = _active_sign_relation(float(route_lateral), signed, lateral_min_abs)
        if ts_relation == "same_sign":
            ts_same += 1
            ts_abs_magnitude_delta.append(abs(abs(float(route_lateral)) - abs(signed)))
        elif ts_relation == "opposite_sign":
            ts_opposite += 1
            ts_opposite_abs_sum.append(abs(float(route_lateral) + signed))
            ts_abs_magnitude_delta.append(abs(abs(float(route_lateral)) - abs(signed)))
        else:
            ignored_inactive += 1
        row_preview["timeseries_lateral_vs_projection_route_sample_signed_lateral"] = ts_relation

        simple_relation = "missing"
        if simple_lat is not None:
            simple_relation = _active_sign_relation(float(simple_lat), signed, lateral_min_abs)
            if simple_relation == "same_sign":
                simple_same += 1
                simple_abs_magnitude_delta.append(abs(abs(float(simple_lat)) - abs(signed)))
            elif simple_relation == "opposite_sign":
                simple_opposite += 1
                simple_opposite_abs_sum.append(abs(float(simple_lat) + signed))
                simple_abs_magnitude_delta.append(abs(abs(float(simple_lat)) - abs(signed)))
        row_preview["simple_lat_vs_projection_route_sample_signed_lateral"] = simple_relation
        if len(sample_preview) < 20:
            sample_preview.append(row_preview)

    ts_count = ts_same + ts_opposite
    simple_count = simple_same + simple_opposite
    ts_same_ratio = (ts_same / ts_count) if ts_count else None
    ts_opposite_ratio = (ts_opposite / ts_count) if ts_count else None
    simple_same_ratio = (simple_same / simple_count) if simple_count else None
    simple_opposite_ratio = (simple_opposite / simple_count) if simple_count else None

    if ts_count < min_samples:
        status = "insufficient_data"
        classification = "projection_route_sample_sign_contract_too_sparse"
        reason = "too_few_active_timeseries_lateral_samples_with_apollo_map_position"
    elif (
        ts_opposite_ratio is not None
        and ts_opposite_ratio >= high_ratio
        and simple_same_ratio is not None
        and simple_same_ratio >= high_ratio
    ):
        status = "available"
        classification = "timeseries_route_lateral_sign_inverted_vs_projection_route_samples"
        reason = (
            "Timeseries route-lateral field opposes projection-route signed lateral, while "
            "Apollo simple_lat lateral matches it."
        )
    elif (
        ts_same_ratio is not None
        and ts_same_ratio >= high_ratio
        and (simple_same_ratio is None or simple_same_ratio >= high_ratio)
    ):
        status = "available"
        classification = "timeseries_route_lateral_matches_projection_route_samples"
        reason = "Timeseries route-lateral field matches projection-route signed lateral."
    elif simple_opposite_ratio is not None and simple_opposite_ratio >= high_ratio:
        status = "warn"
        classification = "simple_lat_sign_opposes_projection_route_samples"
        reason = "Apollo simple_lat lateral opposes projection-route signed lateral."
    elif ts_opposite_ratio is not None and ts_opposite_ratio >= high_ratio:
        status = "warn"
        classification = "timeseries_lateral_opposes_projection_route_samples_without_simple_lat_confirmation"
        reason = (
            "Timeseries route-lateral field opposes projection-route signed lateral, but "
            "Apollo simple_lat evidence is missing or sparse."
        )
    else:
        status = "warn"
        classification = "projection_route_sample_sign_contract_ambiguous"
        reason = "Projection-route sample sign ratios are mixed or below threshold."

    return {
        "available": ts_count >= min_samples,
        "status": status,
        "classification": classification,
        "reason": reason,
        "route_sample_count": len(route_samples),
        "matched_sample_count": len(nearest_distances),
        "route_sample_source": _route_sample_source_summary(route_samples),
        "route_sample_frame": _route_sample_frame_summary(route_samples),
        "route_lateral_source_field": route_lateral_source,
        "map_position_source_fields": {"x": map_x_source, "y": map_y_source},
        "simple_lat_source_field": simple_lat_source,
        "nearest_route_sample_distance_p95_m": _percentile(nearest_distances, 0.95),
        "projection_route_sample_signed_lateral_abs_p95_m": _percentile(signed_abs, 0.95),
        "timeseries_lateral_abs_p95_m": _percentile(route_lateral_abs, 0.95),
        "simple_lat_lateral_abs_p95_m": _percentile(simple_lat_abs, 0.95),
        "timeseries_lateral_vs_projection_route_sample_signed_lateral": {
            "sample_count": ts_count,
            "same_sign_count": ts_same,
            "opposite_sign_count": ts_opposite,
            "same_sign_ratio": ts_same_ratio,
            "opposite_sign_ratio": ts_opposite_ratio,
            "ignored_inactive_count": ignored_inactive,
            "opposite_sign_abs_sum_p95_m": _percentile(ts_opposite_abs_sum, 0.95),
            "abs_magnitude_delta_p95_m": _percentile(ts_abs_magnitude_delta, 0.95),
        },
        "simple_lat_vs_projection_route_sample_signed_lateral": {
            "sample_count": simple_count,
            "same_sign_count": simple_same,
            "opposite_sign_count": simple_opposite,
            "same_sign_ratio": simple_same_ratio,
            "opposite_sign_ratio": simple_opposite_ratio,
            "opposite_sign_abs_sum_p95_m": _percentile(simple_opposite_abs_sum, 0.95),
            "abs_magnitude_delta_p95_m": _percentile(simple_abs_magnitude_delta, 0.95),
        },
        "sample_preview": sample_preview,
        "thresholds": {
            "lateral_min_abs_m": lateral_min_abs,
            "high_ratio": high_ratio,
            "min_samples": min_samples,
        },
        "claim_boundary": (
            "Projection-derived route samples can validate the sign convention between "
            "same-row Apollo-map ego positions and route-lateral fields. They are still "
            "diagnostic route/projection evidence and must not be used to rewrite runtime "
            "control signs or claim behavior success without the full Phase 1 evidence chain."
        ),
    }


def _projection_route_sample_points(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    samples = payload.get("scenario_route_samples") if isinstance(payload, Mapping) else None
    if not isinstance(samples, list):
        return []
    points: list[dict[str, Any]] = []
    for index, sample in enumerate(samples):
        if not isinstance(sample, Mapping):
            continue
        x = _row_value(sample, ["x", "x_apollo", "localization_x"])
        y = _row_value(sample, ["y", "y_apollo", "localization_y"])
        if x is None or y is None:
            continue
        status = str(sample.get("status") or "ok")
        if status not in {"ok", "pass", ""}:
            continue
        points.append(
            {
                "index": sample.get("index", index),
                "x": float(x),
                "y": float(y),
                "heading": _row_value(sample, ["lane_heading_at_s", "route_heading", "heading"]),
                "route_s": _row_value(sample, ["route_s", "expected_route_distance_m"]),
                "projection_s": _row_value(sample, ["projection_s"]),
                "projection_l": _row_value(sample, ["projection_l"]),
                "source": sample.get("source"),
                "frame": sample.get("frame"),
                "lane_id": sample.get("lane_id"),
                "lane_key": sample.get("lane_key"),
                "sample_type": sample.get("sample_type"),
            }
        )
    for index, point in enumerate(points):
        if point.get("heading") is None:
            point["heading"] = _infer_route_sample_heading(points, index)
    return [point for point in points if point.get("heading") is not None]


def _infer_route_sample_heading(points: Sequence[Mapping[str, Any]], index: int) -> float | None:
    if not points:
        return None
    current = points[index]
    prev_point = points[index - 1] if index > 0 else None
    next_point = points[index + 1] if index + 1 < len(points) else None
    if prev_point is not None and next_point is not None:
        dx = float(next_point["x"]) - float(prev_point["x"])
        dy = float(next_point["y"]) - float(prev_point["y"])
    elif next_point is not None:
        dx = float(next_point["x"]) - float(current["x"])
        dy = float(next_point["y"]) - float(current["y"])
    elif prev_point is not None:
        dx = float(current["x"]) - float(prev_point["x"])
        dy = float(current["y"]) - float(prev_point["y"])
    else:
        return None
    if dx == 0.0 and dy == 0.0:
        return None
    return math.atan2(dy, dx)


def _nearest_projection_route_sample(
    x: float,
    y: float,
    samples: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    if not samples:
        return None
    return min(samples, key=lambda sample: math.hypot(x - float(sample["x"]), y - float(sample["y"])))


def _signed_lateral_from_route_sample(x: float, y: float, sample: Mapping[str, Any]) -> float:
    heading = float(sample["heading"])
    dx = x - float(sample["x"])
    dy = y - float(sample["y"])
    return -math.sin(heading) * dx + math.cos(heading) * dy


def _active_sign_relation(left: float, right: float, active_min_abs: float) -> str:
    if abs(left) < active_min_abs or abs(right) < active_min_abs:
        return "inactive"
    product = left * right
    if product > 0.0:
        return "same_sign"
    if product < 0.0:
        return "opposite_sign"
    return "zero"


def _route_sample_source_summary(samples: Sequence[Mapping[str, Any]]) -> str | None:
    values = sorted({str(sample.get("source")) for sample in samples if sample.get("source") not in {None, ""}})
    return ",".join(values) if values else None


def _route_sample_frame_summary(samples: Sequence[Mapping[str, Any]]) -> str | None:
    values = sorted({str(sample.get("frame")) for sample in samples if sample.get("frame") not in {None, ""}})
    return ",".join(values) if values else None


def _route_definition_evidence_rank(payload: Mapping[str, Any]) -> int:
    summary = _route_definition_geometry_summary(payload)
    status = summary["status"]
    if status == "materialized_geometry_available":
        return 30
    if status == "declared_only":
        return 20
    if status == "stub_or_insufficient":
        return 10
    if status == "no_materialized_geometry":
        return 5
    return 0


def _official_hdmap_projection_alignment(
    rows: Sequence[Mapping[str, Any]],
    projection_rows: Sequence[Mapping[str, Any]],
    *,
    lateral_min_abs: float,
    thresholds: Mapping[str, float],
    max_dt_s: float = 0.25,
) -> dict[str, Any]:
    candidates: list[tuple[float, float, Mapping[str, Any]]] = []
    official_count = 0
    for projection in projection_rows:
        if projection.get("source") == "apollo_hdmap_api":
            official_count += 1
        if projection.get("source") != "apollo_hdmap_api":
            continue
        status = projection.get("status") or projection.get("projection_status")
        if status not in {None, "", "ok", "pass"}:
            continue
        timestamp = _row_value(projection, ["sim_time", "timestamp", "apollo_time"])
        lateral = _row_value(projection, ["projection_l", "lateral_error_m"])
        if timestamp is None or lateral is None:
            continue
        candidates.append((float(timestamp), float(lateral), projection))
    candidates.sort(key=lambda item: item[0])
    candidate_times = [item[0] for item in candidates]
    route_projection_same = 0
    route_projection_opposite = 0
    simple_projection_same = 0
    simple_projection_opposite = 0
    route_projection_abs_sum: list[float] = []
    route_projection_abs_magnitude_delta: list[float] = []
    projection_l_recompute_delta: list[float] = []
    matched_point_lateral_offsets: list[float] = []
    current_reference_point_lateral_offsets: list[float] = []
    target_point_lateral_offsets: list[float] = []
    current_station_projection_delta: list[float] = []
    matched_s_projection_delta: list[float] = []
    target_s_projection_delta: list[float] = []
    matched_s_current_delta: list[float] = []
    target_s_current_delta: list[float] = []
    target_s_matched_delta: list[float] = []
    matched_dt: list[float] = []
    matched_lanes: set[str] = set()
    matched_samples: list[dict[str, Any]] = []
    for row in rows:
        row_time = _row_value(row, ["sim_time", "timestamp", "ts_sec", "time"])
        route_lateral = _row_value(row, ROUTE_LATERAL_ERROR_ALIASES)
        if row_time is None or route_lateral is None or not candidates:
            continue
        index = bisect_left(candidate_times, float(row_time))
        nearest: list[tuple[float, float, Mapping[str, Any]]] = []
        for candidate_index in (index - 1, index, index + 1):
            if 0 <= candidate_index < len(candidates):
                nearest.append(candidates[candidate_index])
        if not nearest:
            continue
        projection_time, projection_lateral, projection = min(
            nearest,
            key=lambda item: abs(item[0] - float(row_time)),
        )
        dt = abs(projection_time - float(row_time))
        if dt > max_dt_s:
            continue
        if abs(route_lateral) < lateral_min_abs or abs(projection_lateral) < lateral_min_abs:
            continue
        matched_dt.append(dt)
        lane_id = projection.get("nearest_lane_id")
        if lane_id not in {None, ""}:
            matched_lanes.add(str(lane_id))
        route_projection_abs_sum.append(abs(float(route_lateral) + projection_lateral))
        route_projection_abs_magnitude_delta.append(abs(abs(float(route_lateral)) - abs(projection_lateral)))
        route_projection_relation = "zero"
        if float(route_lateral) * projection_lateral > 0.0:
            route_projection_same += 1
            route_projection_relation = "same_sign"
        elif float(route_lateral) * projection_lateral < 0.0:
            route_projection_opposite += 1
            route_projection_relation = "opposite_sign"
        simple_lat = _row_value(row, FIELD_ALIASES["apollo_simple_lat_lateral_error"])
        simple_projection_relation = "missing"
        if simple_lat is not None and abs(simple_lat) >= lateral_min_abs:
            if float(simple_lat) * projection_lateral > 0.0:
                simple_projection_same += 1
                simple_projection_relation = "same_sign"
            elif float(simple_lat) * projection_lateral < 0.0:
                simple_projection_opposite += 1
                simple_projection_relation = "opposite_sign"
            else:
                simple_projection_relation = "zero"
        elif simple_lat is not None:
            simple_projection_relation = "inactive"
        projection_geometry = _projection_centerline_geometry(projection, projection_lateral)
        if projection_geometry.get("projection_l_recompute_delta_m") is not None:
            projection_l_recompute_delta.append(float(projection_geometry["projection_l_recompute_delta_m"]))
        matched_point = _point_offset_from_projection_line(
            row,
            projection_geometry,
            FIELD_ALIASES["apollo_matched_point_x"],
            FIELD_ALIASES["apollo_matched_point_y"],
        )
        current_reference_point = _point_offset_from_projection_line(
            row,
            projection_geometry,
            FIELD_ALIASES["apollo_current_reference_point_x"],
            FIELD_ALIASES["apollo_current_reference_point_y"],
        )
        target_point = _point_offset_from_projection_line(
            row,
            projection_geometry,
            FIELD_ALIASES["apollo_target_point_x"],
            FIELD_ALIASES["apollo_target_point_y"],
        )
        if matched_point.get("lateral_m") is not None:
            matched_point_lateral_offsets.append(abs(float(matched_point["lateral_m"])))
        if current_reference_point.get("lateral_m") is not None:
            current_reference_point_lateral_offsets.append(abs(float(current_reference_point["lateral_m"])))
        if target_point.get("lateral_m") is not None:
            target_point_lateral_offsets.append(abs(float(target_point["lateral_m"])))
        projection_s = _row_value(projection, ["projection_s", "s"])
        current_station = _row_value(row, FIELD_ALIASES["apollo_simple_lon_current_station"])
        matched_s = _row_value(row, FIELD_ALIASES["apollo_simple_lon_matched_point_s"])
        target_s = _row_value(row, FIELD_ALIASES["apollo_simple_lat_target_point_s"])
        current_station_minus_projection = _delta(current_station, projection_s)
        matched_s_minus_projection = _delta(matched_s, projection_s)
        target_s_minus_projection = _delta(target_s, projection_s)
        matched_s_minus_current = _delta(matched_s, current_station)
        target_s_minus_current = _delta(target_s, current_station)
        target_s_minus_matched = _delta(target_s, matched_s)
        if current_station_minus_projection is not None:
            current_station_projection_delta.append(abs(current_station_minus_projection))
        if matched_s_minus_projection is not None:
            matched_s_projection_delta.append(abs(matched_s_minus_projection))
        if target_s_minus_projection is not None:
            target_s_projection_delta.append(abs(target_s_minus_projection))
        if matched_s_minus_current is not None:
            matched_s_current_delta.append(abs(matched_s_minus_current))
        if target_s_minus_current is not None:
            target_s_current_delta.append(abs(target_s_minus_current))
        if target_s_minus_matched is not None:
            target_s_matched_delta.append(abs(target_s_minus_matched))
        matched_samples.append(
            {
                "sim_time": float(row_time),
                "projection_sim_time": projection_time,
                "match_dt_s": dt,
                "route_lateral_error_m": float(route_lateral),
                "apollo_simple_lat_lateral_error_m": simple_lat,
                "hdmap_projection_l_m": projection_lateral,
                "hdmap_projection_s_m": projection_s,
                "apollo_simple_lon_current_station_m": current_station,
                "apollo_simple_lon_matched_point_s_m": matched_s,
                "apollo_simple_lat_target_point_s_m": target_s,
                "current_station_minus_projection_s_m": current_station_minus_projection,
                "matched_s_minus_projection_s_m": matched_s_minus_projection,
                "target_s_minus_projection_s_m": target_s_minus_projection,
                "matched_s_minus_current_station_m": matched_s_minus_current,
                "target_s_minus_current_station_m": target_s_minus_current,
                "target_s_minus_matched_s_m": target_s_minus_matched,
                "nearest_lane_id": lane_id,
                "route_lateral_vs_projection_lateral": route_projection_relation,
                "simple_lat_vs_projection_lateral": simple_projection_relation,
                "route_projection_abs_sum_m": abs(float(route_lateral) + projection_lateral),
                "route_projection_abs_magnitude_delta_m": abs(abs(float(route_lateral)) - abs(projection_lateral)),
                "apollo_matched_point_hdmap_line_lateral_m": matched_point.get("lateral_m"),
                "apollo_matched_point_hdmap_line_longitudinal_m": matched_point.get("longitudinal_m"),
                "apollo_current_reference_point_hdmap_line_lateral_m": current_reference_point.get("lateral_m"),
                "apollo_current_reference_point_hdmap_line_longitudinal_m": current_reference_point.get("longitudinal_m"),
                "apollo_target_point_hdmap_line_lateral_m": target_point.get("lateral_m"),
                "apollo_target_point_hdmap_line_longitudinal_m": target_point.get("longitudinal_m"),
                **projection_geometry,
            }
        )
    route_sample_count = route_projection_same + route_projection_opposite
    simple_sample_count = simple_projection_same + simple_projection_opposite
    projection_s_count = sum(
        1 for sample in matched_samples if sample.get("hdmap_projection_s_m") is not None
    )
    current_station_count = sum(
        1 for sample in matched_samples if sample.get("apollo_simple_lon_current_station_m") is not None
    )
    matched_s_count = sum(
        1 for sample in matched_samples if sample.get("apollo_simple_lon_matched_point_s_m") is not None
    )
    target_s_count = sum(
        1 for sample in matched_samples if sample.get("apollo_simple_lat_target_point_s_m") is not None
    )
    station_summary = _simple_lat_station_vs_projection_s_summary(
        projection_s_count=projection_s_count,
        current_station_count=current_station_count,
        matched_s_count=matched_s_count,
        target_s_count=target_s_count,
        current_station_projection_delta=current_station_projection_delta,
        matched_s_projection_delta=matched_s_projection_delta,
        target_s_projection_delta=target_s_projection_delta,
        matched_s_current_delta=matched_s_current_delta,
        target_s_current_delta=target_s_current_delta,
        target_s_matched_delta=target_s_matched_delta,
        thresholds=thresholds,
    )
    return {
        "available": official_count > 0,
        "status": "available" if route_sample_count else ("insufficient_data" if official_count else "missing"),
        "source": "apollo_hdmap_api" if official_count else None,
        "projection_row_count": len(projection_rows),
        "official_projection_row_count": official_count,
        "matched_sample_count": route_sample_count,
        "max_match_dt_s": max_dt_s,
        "match_dt_p95_s": _percentile(matched_dt, 0.95),
        "nearest_lane_ids": sorted(matched_lanes),
        "projection_centerline_geometry_sample_count": len(projection_l_recompute_delta),
        "projection_l_recompute_delta_p95_m": _percentile(projection_l_recompute_delta, 0.95),
        "simple_lat_points_vs_projection_line": {
            "matched_point_lateral_abs_p95_m": _percentile(matched_point_lateral_offsets, 0.95),
            "current_reference_point_lateral_abs_p95_m": _percentile(
                current_reference_point_lateral_offsets,
                0.95,
            ),
            "target_point_lateral_abs_p95_m": _percentile(target_point_lateral_offsets, 0.95),
            "matched_point_sample_count": len(matched_point_lateral_offsets),
            "current_reference_point_sample_count": len(current_reference_point_lateral_offsets),
            "target_point_sample_count": len(target_point_lateral_offsets),
            "point_coverage_status": _simple_lat_point_coverage_status(
                matched_count=len(matched_point_lateral_offsets),
                current_reference_count=len(current_reference_point_lateral_offsets),
                target_count=len(target_point_lateral_offsets),
            ),
            "missing_point_fields": _simple_lat_missing_point_fields(
                matched_count=len(matched_point_lateral_offsets),
                current_reference_count=len(current_reference_point_lateral_offsets),
                target_count=len(target_point_lateral_offsets),
            ),
            "interpretation": (
                "Point lateral offsets are measured against the local official HDMap projection "
                "tangent line. They are diagnostic only and do not replace full reference-line "
                "geometry."
            ),
        },
        "simple_lat_station_vs_projection_s": station_summary,
        "matched_samples": matched_samples,
        "route_lateral_vs_projection_lateral": {
            "sample_count": route_sample_count,
            "same_sign_count": route_projection_same,
            "opposite_sign_count": route_projection_opposite,
            "same_sign_ratio": (route_projection_same / route_sample_count) if route_sample_count else None,
            "opposite_sign_ratio": (
                route_projection_opposite / route_sample_count
                if route_sample_count
                else None
            ),
            "opposite_sign_abs_sum_p95_m": _percentile(route_projection_abs_sum, 0.95),
            "abs_magnitude_delta_p95_m": _percentile(route_projection_abs_magnitude_delta, 0.95),
        },
        "simple_lat_vs_projection_lateral": {
            "sample_count": simple_sample_count,
            "same_sign_count": simple_projection_same,
            "opposite_sign_count": simple_projection_opposite,
            "same_sign_ratio": (simple_projection_same / simple_sample_count) if simple_sample_count else None,
            "opposite_sign_ratio": (
                simple_projection_opposite / simple_sample_count
                if simple_sample_count
                else None
            ),
        },
        "claim_boundary": (
            "Official Apollo HDMap projection rows can support lateral sign attribution, "
            "but they do not replace materialized scenario route geometry or prove ego "
            "behavior success."
        ),
    }


def _delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left) - float(right)


def _simple_lat_station_vs_projection_s_summary(
    *,
    projection_s_count: int,
    current_station_count: int,
    matched_s_count: int,
    target_s_count: int,
    current_station_projection_delta: Sequence[float],
    matched_s_projection_delta: Sequence[float],
    target_s_projection_delta: Sequence[float],
    matched_s_current_delta: Sequence[float],
    target_s_current_delta: Sequence[float],
    target_s_matched_delta: Sequence[float],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    current_projection_p95 = _percentile(current_station_projection_delta, 0.95)
    matched_projection_p95 = _percentile(matched_s_projection_delta, 0.95)
    target_projection_p95 = _percentile(target_s_projection_delta, 0.95)
    matched_current_p95 = _percentile(matched_s_current_delta, 0.95)
    target_current_p95 = _percentile(target_s_current_delta, 0.95)
    target_matched_p95 = _percentile(target_s_matched_delta, 0.95)
    coverage_status = _simple_lat_station_coverage_status(
        projection_s_count=projection_s_count,
        current_station_count=current_station_count,
        matched_s_count=matched_s_count,
        target_s_count=target_s_count,
    )
    classification = _simple_lat_station_frame_classification(
        coverage_status=coverage_status,
        current_projection_p95=current_projection_p95,
        target_current_p95=target_current_p95,
        thresholds=thresholds,
    )
    return {
        "projection_s_sample_count": projection_s_count,
        "current_station_sample_count": current_station_count,
        "matched_point_s_sample_count": matched_s_count,
        "target_point_s_sample_count": target_s_count,
        "current_station_minus_projection_s_abs_p95_m": current_projection_p95,
        "matched_s_minus_projection_s_abs_p95_m": matched_projection_p95,
        "target_s_minus_projection_s_abs_p95_m": target_projection_p95,
        "matched_s_minus_current_station_abs_p95_m": matched_current_p95,
        "target_s_minus_current_station_abs_p95_m": target_current_p95,
        "target_s_minus_matched_s_abs_p95_m": target_matched_p95,
        "station_coverage_status": coverage_status,
        "station_frame_classification": classification,
        "missing_station_fields": _simple_lat_missing_station_fields(
            projection_s_count=projection_s_count,
            current_station_count=current_station_count,
            matched_s_count=matched_s_count,
            target_s_count=target_s_count,
        ),
        "thresholds": {
            "station_lane_s_alignment_p95_m": thresholds["station_lane_s_alignment_p95_m"],
            "station_local_frame_offset_min_p95_m": thresholds["station_local_frame_offset_min_p95_m"],
            "station_target_current_alignment_p95_m": thresholds[
                "station_target_current_alignment_p95_m"
            ],
        },
        "interpretation": _simple_lat_station_frame_interpretation(classification),
    }


def _simple_lat_station_frame_classification(
    *,
    coverage_status: str,
    current_projection_p95: float | None,
    target_current_p95: float | None,
    thresholds: Mapping[str, float],
) -> str:
    if coverage_status == "missing_station_coverage":
        return "missing_station_data"
    if coverage_status != "projection_current_matched_target_s_available":
        return "partial_station_data"
    if current_projection_p95 is None:
        return "missing_station_data"
    if current_projection_p95 <= thresholds["station_lane_s_alignment_p95_m"]:
        return "lane_s_aligned"
    if (
        current_projection_p95 >= thresholds["station_local_frame_offset_min_p95_m"]
        and target_current_p95 is not None
        and target_current_p95 <= thresholds["station_target_current_alignment_p95_m"]
    ):
        return "local_station_frame_offset_candidate"
    return "station_frame_inconsistent"


def _simple_lat_station_frame_interpretation(classification: str) -> str:
    if classification == "lane_s_aligned":
        return (
            "Apollo Control station values are close to official HDMap projection_s. "
            "This supports a lane-s-aligned debug interpretation for the matched samples."
        )
    if classification == "local_station_frame_offset_candidate":
        return (
            "Apollo Control current/target station values are internally aligned but offset "
            "from official HDMap projection_s, consistent with a local or stitching station "
            "frame. This is diagnostic and not a reference-line pass/fail by itself."
        )
    if classification == "station_frame_inconsistent":
        return (
            "Apollo Control station values are available but neither align with projection_s "
            "nor form a compact local station frame under the configured thresholds."
        )
    if classification == "partial_station_data":
        return "Only partial Apollo Control station evidence is available for projection pairing."
    return "Station deltas cannot be classified because station evidence is missing."


def _simple_lat_station_coverage_status(
    *,
    projection_s_count: int,
    current_station_count: int,
    matched_s_count: int,
    target_s_count: int,
) -> str:
    if projection_s_count > 0 and current_station_count > 0 and matched_s_count > 0 and target_s_count > 0:
        return "projection_current_matched_target_s_available"
    if projection_s_count > 0 and current_station_count > 0 and target_s_count > 0:
        return "projection_current_target_s_available_matched_s_missing"
    if projection_s_count > 0 and (current_station_count > 0 or matched_s_count > 0 or target_s_count > 0):
        return "partial_station_coverage"
    return "missing_station_coverage"


def _simple_lat_missing_station_fields(
    *,
    projection_s_count: int,
    current_station_count: int,
    matched_s_count: int,
    target_s_count: int,
) -> list[str]:
    missing: list[str] = []
    if projection_s_count == 0:
        missing.append("hdmap_projection_s")
    if current_station_count == 0:
        missing.append("apollo_simple_lon_current_station")
    if matched_s_count == 0:
        missing.append("apollo_simple_lon_matched_point_s")
    if target_s_count == 0:
        missing.append("apollo_simple_lat_target_point_s")
    return missing


def _simple_lat_point_coverage_status(
    *,
    matched_count: int,
    current_reference_count: int,
    target_count: int,
) -> str:
    if matched_count > 0 and current_reference_count > 0 and target_count > 0:
        return "matched_current_reference_target_available"
    if matched_count > 0 and target_count > 0 and current_reference_count == 0:
        return "matched_and_target_available_current_reference_missing"
    if matched_count > 0 or current_reference_count > 0 or target_count > 0:
        return "partial_point_coordinate_coverage"
    return "missing_point_coordinate_coverage"


def _simple_lat_missing_point_fields(
    *,
    matched_count: int,
    current_reference_count: int,
    target_count: int,
) -> list[str]:
    missing: list[str] = []
    if matched_count == 0:
        missing.append("apollo_matched_point_x_y")
    if current_reference_count == 0:
        missing.append("apollo_current_reference_point_x_y")
    if target_count == 0:
        missing.append("apollo_target_point_x_y")
    return missing


def _projection_centerline_geometry(
    projection: Mapping[str, Any],
    projection_lateral: float,
) -> dict[str, Any]:
    x = _row_value(projection, ["localization_x", "x_apollo"])
    y = _row_value(projection, ["localization_y", "y_apollo"])
    heading = _row_value(projection, ["lane_heading_at_s"])
    if x is None or y is None or heading is None:
        return {
            "hdmap_projection_center_x": None,
            "hdmap_projection_center_y": None,
            "hdmap_projection_lane_heading_rad": heading,
            "hdmap_projection_l_recomputed_m": None,
            "projection_l_recompute_delta_m": None,
            "projection_centerline_geometry_available": False,
        }
    left_x = -math.sin(float(heading))
    left_y = math.cos(float(heading))
    center_x = float(x) - float(projection_lateral) * left_x
    center_y = float(y) - float(projection_lateral) * left_y
    recomputed_l = left_x * (float(x) - center_x) + left_y * (float(y) - center_y)
    return {
        "hdmap_projection_center_x": center_x,
        "hdmap_projection_center_y": center_y,
        "hdmap_projection_lane_heading_rad": float(heading),
        "hdmap_projection_l_recomputed_m": recomputed_l,
        "projection_l_recompute_delta_m": abs(recomputed_l - float(projection_lateral)),
        "projection_centerline_geometry_available": True,
    }


def _point_offset_from_projection_line(
    row: Mapping[str, Any],
    projection_geometry: Mapping[str, Any],
    x_aliases: Sequence[str],
    y_aliases: Sequence[str],
) -> dict[str, float | None]:
    point_x = _row_value(row, x_aliases)
    point_y = _row_value(row, y_aliases)
    center_x = projection_geometry.get("hdmap_projection_center_x")
    center_y = projection_geometry.get("hdmap_projection_center_y")
    heading = projection_geometry.get("hdmap_projection_lane_heading_rad")
    if point_x is None or point_y is None or center_x is None or center_y is None or heading is None:
        return {"lateral_m": None, "longitudinal_m": None}
    forward_x = math.cos(float(heading))
    forward_y = math.sin(float(heading))
    left_x = -math.sin(float(heading))
    left_y = math.cos(float(heading))
    dx = float(point_x) - float(center_x)
    dy = float(point_y) - float(center_y)
    return {
        "lateral_m": left_x * dx + left_y * dy,
        "longitudinal_m": forward_x * dx + forward_y * dy,
    }


def _hdmap_route_lateral_consistency(payload: Mapping[str, Any]) -> dict[str, Any]:
    consistency = payload.get("hdmap_route_lateral_consistency") if isinstance(payload, Mapping) else None
    if not isinstance(consistency, Mapping):
        return {
            "available": False,
            "status": "not_available",
            "interpretation": None,
        }
    keys = {
        "available",
        "status",
        "alignment_mode",
        "best_abs_delta_p95_m",
        "projection_lateral_p95_m",
        "route_cross_track_p95_m",
        "interpretation",
        "sample_count",
    }
    return {key: consistency.get(key) for key in keys if key in consistency}


def _reference_debug_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or not payload:
        return {
            "available": False,
            "status": "not_available",
            "nonempty_planning_with_reference_debug_missing": False,
        }
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else {}
    diagnostic = payload.get("reference_debug_diagnostic") if isinstance(payload.get("reference_debug_diagnostic"), Mapping) else {}
    planning_debug_presence = (
        diagnostic.get("planning_debug_presence")
        if isinstance(diagnostic.get("planning_debug_presence"), Mapping)
        else payload.get("planning_debug_presence")
        if isinstance(payload.get("planning_debug_presence"), Mapping)
        else {}
    )
    planning_debug_presence_classification = str(
        diagnostic.get("planning_debug_presence_classification")
        or planning_debug_presence.get("classification")
        or ""
    )
    warnings = [str(item) for item in payload.get("warnings", []) if item not in {None, ""}]
    nonempty_ratio = _num(evidence.get("nonempty_trajectory_ratio"))
    ready_ratio = _num(evidence.get("reference_line_provider_ready_ratio"))
    count_zero_ratio = _num(metrics.get("reference_line_count_zero_ratio"))
    routing_segment_zero_ratio = _num(metrics.get("routing_segment_count_zero_ratio"))
    debug_missing = (
        "reference_line_count_zero_debug_counter_with_nonempty_trajectory" in warnings
        or planning_debug_presence_classification == "routing_present_reference_line_empty"
        or (nonempty_ratio is not None and nonempty_ratio > 0.0 and count_zero_ratio == 1.0)
        or (nonempty_ratio is not None and nonempty_ratio > 0.0 and ready_ratio == 0.0)
    )
    return {
        "available": True,
        "status": payload.get("status"),
        "warnings": warnings,
        "blocking_reasons": payload.get("blocking_reasons", []),
        "nonempty_trajectory_ratio": nonempty_ratio,
        "reference_line_provider_ready_ratio": ready_ratio,
        "reference_line_count_zero_ratio": count_zero_ratio,
        "routing_segment_count_zero_ratio": routing_segment_zero_ratio,
        "debug_gap_classification": diagnostic.get("classification"),
        "planning_debug_presence_classification": planning_debug_presence_classification or None,
        "planning_debug_presence": planning_debug_presence,
        "route_segment_available": diagnostic.get("route_segment_available"),
        "control_simple_lat_reference_available": diagnostic.get("control_simple_lat_reference_available"),
        "control_reference_join_coverage_ratio": diagnostic.get("control_reference_join_coverage_ratio"),
        "nonempty_planning_with_reference_debug_missing": bool(debug_missing),
    }


def _control_attribution_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or not payload:
        return {
            "available": False,
            "status": "not_available",
            "dominant_breakpoint": None,
            "raw_to_mapped_steer_status": None,
            "mapped_to_applied_steer_status": None,
            "control_mapping_consistent": False,
        }
    attribution = payload.get("attribution") if isinstance(payload.get("attribution"), Mapping) else {}
    raw_to_mapped = (
        attribution.get("raw_to_mapped_steer_consistency")
        if isinstance(attribution.get("raw_to_mapped_steer_consistency"), Mapping)
        else {}
    )
    mapped_to_applied = (
        attribution.get("mapped_to_applied_steer_consistency")
        if isinstance(attribution.get("mapped_to_applied_steer_consistency"), Mapping)
        else {}
    )
    verdict = payload.get("verdict") if isinstance(payload.get("verdict"), Mapping) else {}
    raw_status = raw_to_mapped.get("status")
    applied_status = mapped_to_applied.get("status")
    return {
        "available": True,
        "status": verdict.get("status") or payload.get("status"),
        "dominant_breakpoint": attribution.get("dominant_breakpoint") or verdict.get("dominant_breakpoint"),
        "raw_to_mapped_steer_status": raw_status,
        "raw_to_mapped_steer_error_p95": raw_to_mapped.get("error_p95"),
        "mapped_to_applied_steer_status": applied_status,
        "mapped_to_applied_steer_error_p95": mapped_to_applied.get("error_p95"),
        "control_mapping_consistent": raw_status == "pass" and applied_status == "pass",
    }


def _suppress_control_mapping_anomalies(
    anomalies: Sequence[Mapping[str, Any]],
    control_attribution_summary: Mapping[str, Any],
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    if control_attribution_summary.get("control_mapping_consistent") is not True:
        return list(anomalies), []
    kept: list[Mapping[str, Any]] = []
    suppressed: list[Mapping[str, Any]] = []
    for anomaly in anomalies:
        if anomaly.get("type") == "raw_mapped_applied_mismatch":
            copy = dict(anomaly)
            copy["suppression_reason"] = (
                "control_attribution_report marks raw->mapped and mapped->applied "
                "steer consistency as pass; keep the lateral report focused on "
                "remaining source/reference-line semantics"
            )
            copy["control_attribution_summary"] = dict(control_attribution_summary)
            suppressed.append(copy)
        else:
            kept.append(anomaly)
    return kept, suppressed


def _raw_mapped_expected_error(rows: Sequence[Mapping[str, Any]]) -> float | None:
    errors: list[float] = []
    for row in rows:
        raw = _row_value(row, FIELD_ALIASES["apollo_steer_raw"])
        mapped = _row_value(row, FIELD_ALIASES["bridge_steer_mapped"])
        scale = _row_value(row, FIELD_ALIASES["steer_scale"])
        sign = _row_value(row, FIELD_ALIASES["steering_sign"])
        if raw is None or mapped is None or scale is None:
            continue
        errors.append(abs(_clamp(raw * scale * (sign if sign is not None else 1.0), -1.0, 1.0) - mapped))
    return _percentile(errors, 0.95) if errors else None


def _paired_field_error(rows: Sequence[Mapping[str, Any]], left_field: str, right_field: str) -> float | None:
    errors: list[float] = []
    left_aliases = FIELD_ALIASES.get(left_field, [left_field])
    right_aliases = FIELD_ALIASES.get(right_field, [right_field])
    for row in rows:
        left = _row_value(row, left_aliases)
        right = _row_value(row, right_aliases)
        if left is None or right is None:
            continue
        errors.append(abs(left - right))
    return _percentile(errors, 0.95) if errors else None


def _pair_error(left: Sequence[float], right: Sequence[float]) -> float | None:
    count = min(len(left), len(right))
    if count <= 0:
        return None
    return _percentile([abs(left[index] - right[index]) for index in range(count)], 0.95)


def _max_abs_delta(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    return max(abs(cur - prev) for prev, cur in zip(values, values[1:]))


def _pearson_pair(left: Sequence[float], right: Sequence[float]) -> float | None:
    count = min(len(left), len(right))
    if count < 2:
        return None
    xs = list(left[:count])
    ys = list(right[:count])
    mean_x = sum(xs) / count
    mean_y = sum(ys) / count
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x <= 0.0 or den_y <= 0.0:
        return None
    return num / (den_x * den_y)


def _stats_abs(values: Sequence[float]) -> dict[str, Any]:
    absolute = [abs(value) for value in values if math.isfinite(float(value))]
    if not absolute:
        return {"count": 0, "mean": None, "p95": None, "max": None}
    return {
        "count": len(absolute),
        "mean": sum(absolute) / len(absolute),
        "p95": _percentile(absolute, 0.95),
        "max": max(absolute),
    }


def _paired_abs_delta_stats(
    rows: Sequence[Mapping[str, Any]],
    left_aliases: Sequence[str],
    right_aliases: Sequence[str],
) -> dict[str, Any]:
    deltas: list[float] = []
    for row in rows:
        left = _row_value(row, left_aliases)
        right = _row_value(row, right_aliases)
        if left is None or right is None:
            continue
        deltas.append(abs(left - right))
    return _stats_abs(deltas)


def _xy_distance_stats(
    rows: Sequence[Mapping[str, Any]],
    left_x_aliases: Sequence[str],
    left_y_aliases: Sequence[str],
    right_x_aliases: Sequence[str],
    right_y_aliases: Sequence[str],
) -> dict[str, Any]:
    distances: list[float] = []
    for row in rows:
        left_x = _row_value(row, left_x_aliases)
        left_y = _row_value(row, left_y_aliases)
        right_x = _row_value(row, right_x_aliases)
        right_y = _row_value(row, right_y_aliases)
        if None in (left_x, left_y, right_x, right_y):
            continue
        distances.append(math.hypot(float(left_x) - float(right_x), float(left_y) - float(right_y)))
    return _stats_abs(distances)


def _stat(stats: Mapping[str, Any], group: str, key: str) -> float | None:
    value = stats.get(group)
    if not isinstance(value, Mapping):
        return None
    return _num(value.get(key))


def _read_rows(path: str | Path | Sequence[str | Path] | None) -> list[dict[str, Any]]:
    if path in (None, ""):
        return []
    if isinstance(path, Sequence) and not isinstance(path, (str, bytes, Path)):
        rows: list[dict[str, Any]] = []
        for item in path:
            rows.extend(_read_rows(item))
        return rows
    resolved = Path(str(path)).expanduser()
    if not resolved.exists():
        return []
    if resolved.suffix == ".jsonl":
        rows = _read_jsonl(resolved)
        if "control_apply_trace" in resolved.name:
            return [_normalize_control_apply_trace_row(row) for row in rows]
        if "control_decode_debug" in resolved.name:
            return [_normalize_control_decode_debug_row(row) for row in rows]
        if "apollo_control_raw" in resolved.name:
            return [_normalize_apollo_control_raw_row(row) for row in rows]
        return rows
    if resolved.suffix == ".json":
        payload = _read_json(resolved)
        rows = payload.get("rows") or payload.get("data") or payload.get("messages")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, Mapping)]
        return [payload] if payload else []
    with resolved.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _normalize_control_apply_trace_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    apollo_raw = row.get("apollo_raw") if isinstance(row.get("apollo_raw"), Mapping) else {}
    bridge_mapped = row.get("bridge_mapped") if isinstance(row.get("bridge_mapped"), Mapping) else {}
    carla_applied = row.get("carla_applied") if isinstance(row.get("carla_applied"), Mapping) else {}
    vehicle_response = row.get("vehicle_response") if isinstance(row.get("vehicle_response"), Mapping) else {}
    gt_state = row.get("gt_state") if isinstance(row.get("gt_state"), Mapping) else {}
    apollo_control = row.get("apollo_control") if isinstance(row.get("apollo_control"), Mapping) else {}
    _set_if_numeric(normalized, "sim_time", _first_numeric_value(row.get("sim_time"), row.get("timestamp"), gt_state.get("sim_time_sec")))
    _set_if_numeric(
        normalized,
        "control_header_timestamp_sec",
        _first_numeric_value(
            row.get("control_header_timestamp_sec"),
            apollo_control.get("header_timestamp_sec"),
            apollo_control.get("control_timestamp"),
        ),
    )
    _set_if_numeric(
        normalized,
        "control_header_sequence_num",
        _first_numeric_value(row.get("control_header_sequence_num"), apollo_control.get("header_sequence_num")),
    )
    _set_if_numeric(normalized, "apollo_steer_raw", _first_numeric_value(row.get("apollo_steer_raw"), apollo_raw.get("steer")))
    _set_if_numeric(
        normalized,
        "bridge_steer_mapped",
        _first_numeric_value(
            row.get("bridge_steer_mapped"),
            bridge_mapped.get("mapped_carla_steer_cmd"),
            bridge_mapped.get("steer"),
        ),
    )
    _set_if_numeric(
        normalized,
        "carla_steer_applied",
        _first_numeric_value(row.get("carla_steer_applied"), carla_applied.get("steer")),
    )
    _set_if_numeric(
        normalized,
        "ego_yaw_rate",
        _first_numeric_value(row.get("ego_yaw_rate"), vehicle_response.get("yaw_rate_rad_s")),
    )
    _set_if_numeric(normalized, "steer_scale", row.get("steer_scale"))
    _set_if_numeric(normalized, "steering_sign", row.get("steering_sign"))
    return normalized


def _normalize_control_decode_debug_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    parsed = row.get("parsed_control") if isinstance(row.get("parsed_control"), Mapping) else {}
    raw = row.get("raw_control_msg_dump") if isinstance(row.get("raw_control_msg_dump"), Mapping) else {}
    output = row.get("output_to_carla") if isinstance(row.get("output_to_carla"), Mapping) else {}
    _set_if_numeric(normalized, "sim_time", _first_numeric_value(output.get("gt_state_sim_time_sec")))
    _set_if_numeric(
        normalized,
        "control_header_timestamp_sec",
        _first_numeric_value(raw.get("control_header_timestamp_sec"), parsed.get("control_header_timestamp_sec")),
    )
    _set_if_numeric(
        normalized,
        "control_header_sequence_num",
        _first_numeric_value(raw.get("control_header_sequence_num"), parsed.get("control_header_sequence_num")),
    )
    _set_if_numeric(normalized, "apollo_steer_raw", _first_numeric_value(parsed.get("steer")))
    _set_if_numeric(
        normalized,
        "bridge_steer_mapped",
        _first_numeric_value(output.get("mapped_carla_steer_cmd"), output.get("steer")),
    )
    _set_if_numeric(normalized, "steer_scale", output.get("steer_scale"))
    _set_if_numeric(normalized, "steering_sign", output.get("steer_sign"))
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_lateral_error_m",
        _first_numeric_value(
            parsed.get("debug_simple_lat_lateral_error_m"),
            raw.get("debug_simple_lat_lateral_error"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_heading_error_rad",
        _first_numeric_value(
            parsed.get("debug_simple_lat_heading_error_rad"),
            raw.get("debug_simple_lat_heading_error"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_curvature",
        _first_numeric_value(parsed.get("debug_simple_lat_curvature"), raw.get("debug_simple_lat_curvature")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_kappa",
        _first_numeric_value(
            parsed.get("debug_simple_lat_target_point_kappa"),
            raw.get("debug_simple_lat_current_target_point_kappa"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_s",
        _first_numeric_value(
            parsed.get("debug_simple_lat_target_point_s"),
            raw.get("debug_simple_lat_current_target_point_s"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_x",
        _first_numeric_value(
            parsed.get("debug_simple_lat_target_point_x"),
            raw.get("debug_simple_lat_current_target_point_x"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_y",
        _first_numeric_value(
            parsed.get("debug_simple_lat_target_point_y"),
            raw.get("debug_simple_lat_current_target_point_y"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_current_reference_point_x",
        _first_numeric_value(parsed.get("debug_simple_lat_current_reference_point_x")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_current_reference_point_y",
        _first_numeric_value(parsed.get("debug_simple_lat_current_reference_point_y")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_current_station_m",
        _first_numeric_value(parsed.get("debug_simple_lon_current_station_m"), raw.get("debug_simple_lon_current_station")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_station_reference_m",
        _first_numeric_value(
            parsed.get("debug_simple_lon_station_reference_m"),
            raw.get("debug_simple_lon_station_reference"),
        ),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_matched_point_s",
        _first_numeric_value(parsed.get("debug_simple_lon_matched_point_s"), raw.get("debug_simple_lon_current_matched_point_s")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_matched_point_x",
        _first_numeric_value(parsed.get("debug_simple_lon_matched_point_x"), raw.get("debug_simple_lon_current_matched_point_x")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_matched_point_y",
        _first_numeric_value(parsed.get("debug_simple_lon_matched_point_y"), raw.get("debug_simple_lon_current_matched_point_y")),
    )
    return normalized


def _normalize_apollo_control_raw_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    raw = row.get("apollo_control_raw") if isinstance(row.get("apollo_control_raw"), Mapping) else row
    _set_if_numeric(
        normalized,
        "control_header_timestamp_sec",
        _first_numeric_value(row.get("control_header_timestamp_sec"), raw.get("control_header_timestamp_sec")),
    )
    _set_if_numeric(
        normalized,
        "control_header_sequence_num",
        _first_numeric_value(row.get("control_header_sequence_num"), raw.get("control_header_sequence_num")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_lateral_error_m",
        _first_numeric_value(raw.get("debug_simple_lat_lateral_error")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_heading_error_rad",
        _first_numeric_value(raw.get("debug_simple_lat_heading_error")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_curvature",
        _first_numeric_value(raw.get("debug_simple_lat_curvature")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_kappa",
        _first_numeric_value(raw.get("debug_simple_lat_current_target_point_kappa")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_s",
        _first_numeric_value(raw.get("debug_simple_lat_current_target_point_s")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_x",
        _first_numeric_value(raw.get("debug_simple_lat_current_target_point_x")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_target_point_y",
        _first_numeric_value(raw.get("debug_simple_lat_current_target_point_y")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_current_reference_point_x",
        _first_numeric_value(raw.get("debug_simple_lat_current_reference_point_x")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lat_current_reference_point_y",
        _first_numeric_value(raw.get("debug_simple_lat_current_reference_point_y")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_current_station_m",
        _first_numeric_value(raw.get("debug_simple_lon_current_station")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_station_reference_m",
        _first_numeric_value(raw.get("debug_simple_lon_station_reference")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_matched_point_s",
        _first_numeric_value(raw.get("debug_simple_lon_current_matched_point_s")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_matched_point_x",
        _first_numeric_value(raw.get("debug_simple_lon_current_matched_point_x")),
    )
    _set_if_numeric(
        normalized,
        "apollo_debug_simple_lon_matched_point_y",
        _first_numeric_value(raw.get("debug_simple_lon_current_matched_point_y")),
    )
    _set_if_numeric(
        normalized,
        "debug_simple_lon_current_reference_point_x",
        _first_numeric_value(raw.get("debug_simple_lon_current_reference_point_x")),
    )
    _set_if_numeric(
        normalized,
        "debug_simple_lon_current_reference_point_y",
        _first_numeric_value(raw.get("debug_simple_lon_current_reference_point_y")),
    )
    _set_if_numeric(
        normalized,
        "debug_simple_lon_current_reference_point_kappa",
        _first_numeric_value(raw.get("debug_simple_lon_current_reference_point_kappa")),
    )
    _set_if_numeric(
        normalized,
        "debug_simple_lon_preview_reference_point_x",
        _first_numeric_value(raw.get("debug_simple_lon_preview_reference_point_x")),
    )
    _set_if_numeric(
        normalized,
        "debug_simple_lon_preview_reference_point_y",
        _first_numeric_value(raw.get("debug_simple_lon_preview_reference_point_y")),
    )
    _set_if_numeric(
        normalized,
        "debug_simple_lon_preview_reference_point_kappa",
        _first_numeric_value(raw.get("debug_simple_lon_preview_reference_point_kappa")),
    )
    return normalized


def _first_numeric_value(*values: Any) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _set_if_numeric(payload: dict[str, Any], key: str, value: Any) -> None:
    number = _num(value)
    if number is not None:
        payload[key] = number


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
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


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path in (None, ""):
        return {}
    resolved = Path(str(path)).expanduser()
    if not resolved.exists():
        return {}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    return None


def _find_first_deep(root: Path, relative_paths: Sequence[str]) -> Path | None:
    exact = _find_first(root, relative_paths)
    if exact is not None:
        return exact
    candidates: list[Path] = []
    for relative in relative_paths:
        name = Path(relative).name
        if not name:
            continue
        candidates.extend(path for path in root.rglob(name) if path.is_file())
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (len(path.relative_to(root).parts), str(path)))[0]


def _find_best_route_definition(root: Path, relative_paths: Sequence[str]) -> Path | None:
    best_path: Path | None = None
    best_rank = -1
    for relative in relative_paths:
        path = root / relative
        if not path.exists():
            continue
        rank = _route_definition_evidence_rank(_read_json(path))
        if rank > best_rank:
            best_path = path
            best_rank = rank
    return best_path


def _path_repr(path: str | Path | Sequence[str | Path] | None) -> str | list[str] | None:
    if path is None:
        return None
    if isinstance(path, Sequence) and not isinstance(path, (str, bytes, Path)):
        return [str(Path(str(item))) for item in path]
    return str(Path(str(path)))


def _first_text(rows: Sequence[Mapping[str, Any]], field: str, *, default: str | None = None) -> str | None:
    for row in rows:
        value = row.get(field)
        if value not in {None, ""}:
            return str(value)
    return default


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


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _nested(payload: Mapping[str, Any], dotted: str) -> Any:
    current: Any = payload
    for part in dotted.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _markdown(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    lines = [
        "# Apollo Lateral Semantics Report",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- run_id: `{report.get('run_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- backend: `{report.get('backend')}`",
        f"- status: `{verdict.get('status')}`",
        f"- suspected_layer: `{report.get('suspected_layer')}`",
        f"- confidence: `{report.get('confidence')}`",
        "",
        "## Availability",
        "",
        f"- route_curvature_available: `{report.get('route_curvature_available')}`",
        f"- reference_line_curvature_available: `{report.get('reference_line_curvature_available')}`",
        f"- planning_kappa_available: `{report.get('planning_kappa_available')}`",
        f"- target_point_available: `{report.get('target_point_available')}`",
        f"- source_steer_available: `{report.get('source_steer_available')}`",
        f"- matched_point_available: `{report.get('matched_point_available')}`",
        "",
        "## Anomalies",
        "",
    ]
    anomalies = report.get("anomalies") if isinstance(report.get("anomalies"), list) else []
    if anomalies:
        for item in anomalies:
            if isinstance(item, Mapping):
                lines.append(f"- `{item.get('type')}` -> `{item.get('suspected_layer')}`: {item.get('reason')}")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## HDMap Route Lateral Consistency",
            "",
            f"- status: `{_nested(report, 'hdmap_route_lateral_consistency.status')}`",
            f"- interpretation: `{_nested(report, 'hdmap_route_lateral_consistency.interpretation')}`",
            f"- best_abs_delta_p95_m: `{_nested(report, 'hdmap_route_lateral_consistency.best_abs_delta_p95_m')}`",
            "",
            "## Drift Window",
            "",
            f"- status: `{_nested(report, 'drift_window_summary.status')}`",
            f"- first_high_lateral: `{_nested(report, 'drift_window_summary.first_high_lateral')}`",
            f"- max_abs_lateral: `{_nested(report, 'drift_window_summary.max_abs_lateral')}`",
            f"- phase_samples: `{_nested(report, 'drift_window_summary.phase_samples')}`",
            "",
            "## Critical Same-Window Samples",
            "",
            f"- status: `{_nested(report, 'critical_window_samples.status')}`",
            f"- anchor_reason: `{_nested(report, 'critical_window_samples.anchor_reason')}`",
            f"- anchor: `{_nested(report, 'critical_window_samples.anchor')}`",
            f"- returned_sample_count: `{_nested(report, 'critical_window_samples.returned_sample_count')}`",
            f"- reference_line_context: `{_nested(report, 'critical_window_samples.reference_line_context')}`",
            "",
            "## Lateral Sign Alignment",
            "",
            f"- status: `{_nested(report, 'lateral_sign_alignment.status')}`",
            f"- source_steer_vs_route_lateral_error: `{_nested(report, 'lateral_sign_alignment.source_steer_vs_route_lateral_error')}`",
            f"- source_steer_vs_simple_lat_lateral_error: `{_nested(report, 'lateral_sign_alignment.source_steer_vs_simple_lat_lateral_error')}`",
            f"- applied_steer_vs_route_lateral_error: `{_nested(report, 'lateral_sign_alignment.applied_steer_vs_route_lateral_error')}`",
            f"- route_lateral_provenance: `{_nested(report, 'lateral_sign_alignment.route_lateral_provenance')}`",
            f"- route_simple_lat_magnitude_alignment: `{_nested(report, 'lateral_sign_alignment.route_simple_lat_magnitude_alignment')}`",
            f"- official_hdmap_projection_alignment: `{_nested(report, 'lateral_sign_alignment.official_hdmap_projection_alignment')}`",
            f"- lateral_frame_convention_diagnostic: `{_nested(report, 'lateral_sign_alignment.lateral_frame_convention_diagnostic')}`",
            f"- first_high_lateral_sample: `{_nested(report, 'lateral_sign_alignment.first_high_lateral_sample')}`",
            "",
            "## Missing Fields",
            "",
            f"`{', '.join(report.get('missing_fields') or []) or 'none'}`",
            "",
            "## Interpretation Boundary",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )
    return "\n".join(lines)
