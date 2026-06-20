from __future__ import annotations

import csv
import json
import math
from bisect import bisect_left
from pathlib import Path
from typing import Any, Mapping, Sequence

APOLLO_LATERAL_SEMANTICS_SCHEMA_VERSION = "apollo_lateral_semantics.v1"
CONTROL_TRACE_MERGE_MAX_DT_S = 0.075

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
        "apollo_debug_simple_lat_lateral_error_m",
        "debug_simple_lat_lateral_error_m",
        "e_y_m",
    ],
    "apollo_simple_lon_current_station": [
        "apollo_debug_simple_lon_current_station_m",
        "debug_simple_lon_current_station_m",
    ],
    "apollo_simple_lon_station_reference": [
        "apollo_debug_simple_lon_station_reference_m",
        "debug_simple_lon_station_reference_m",
    ],
    "apollo_simple_lat_target_point_s": [
        "apollo_debug_simple_lat_target_point_s",
        "debug_simple_lat_target_point_s",
        "apollo_target_point_s",
    ],
    "apollo_simple_lon_matched_point_s": [
        "apollo_debug_simple_lon_matched_point_s",
        "debug_simple_lon_matched_point_s",
        "apollo_matched_point_s",
    ],
    "ego_x": ["ego_x", "localization_x", "apollo_localization_x"],
    "ego_y": ["ego_y", "localization_y", "apollo_localization_y"],
    "route_x": ["route_x", "nearest_route_x"],
    "route_y": ["route_y", "nearest_route_y"],
    "apollo_matched_point_x": [
        "apollo_debug_simple_lon_matched_point_x",
        "apollo_debug_simple_mpc_matched_point_x",
        "apollo_matched_point_x",
    ],
    "apollo_matched_point_y": [
        "apollo_debug_simple_lon_matched_point_y",
        "apollo_debug_simple_mpc_matched_point_y",
        "apollo_matched_point_y",
    ],
    "apollo_target_point_x": [
        "apollo_debug_simple_lat_target_point_x",
        "apollo_target_point_x",
    ],
    "apollo_target_point_y": [
        "apollo_debug_simple_lat_target_point_y",
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
    "simple_lat_lateral_error_low_abs_p95_m": 0.05,
    "route_s_station_delta_high_p95_m": 20.0,
    "matched_point_near_ego_p95_m": 0.30,
    "matched_point_far_from_route_p95_m": 0.50,
}

DRIFT_WINDOW_FIELD_ALIASES = {
    "sim_time": ["sim_time", "ts_sec", "timestamp", "time"],
    "route_s": ["route_s", "apollo_debug_simple_lon_current_station_m"],
    **FIELD_ALIASES,
}


def analyze_apollo_lateral_semantics_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    timeseries_paths = [
        path
        for path in (
            _find_first(root, ["artifacts/debug_timeseries.csv"]),
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
        control_trace=_find_first(root, ["artifacts/control_apply_trace.jsonl", "control_apply_trace.jsonl"]),
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
        reference_line_contract=_find_first(
            root,
            [
                "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
                "apollo_reference_line_contract_report.json",
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
    control_trace: str | Path | None = None,
    source_steer_summary: str | Path | None = None,
    kappa_audit_summary: str | Path | None = None,
    localization_contract: str | Path | None = None,
    reference_line_contract: str | Path | None = None,
    run_dir: str | Path | None = None,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})
    timeseries_rows = _read_rows(timeseries)
    planning_rows = _read_rows(planning_debug)
    control_trace_rows = _read_rows(control_trace)
    route_health_payload = _read_json(route_health)
    source_summary = _read_json(source_steer_summary)
    kappa_summary = _read_json(kappa_audit_summary)
    localization_contract_payload = _read_json(localization_contract)
    reference_line_contract_payload = _read_json(reference_line_contract)
    semantic_rows = _merge_control_trace_fields([*timeseries_rows, *planning_rows], control_trace_rows)
    rows = [*semantic_rows, *control_trace_rows]
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
    anomalies = _anomalies(
        values,
        stats,
        active_thresholds,
        rows=rows,
        hdmap_route_lateral_consistency=hdmap_route_lateral_consistency,
        reference_debug_summary=reference_debug_summary,
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
        "drift_window_summary": drift_window_summary,
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
            "control_trace": None if control_trace is None else str(Path(control_trace)),
            "route_health": None if route_health is None else str(Path(route_health)),
            "planning_debug": None if planning_debug is None else str(Path(planning_debug)),
            "source_steer_summary": None if source_steer_summary is None else str(Path(source_steer_summary)),
            "kappa_audit_summary": None if kappa_audit_summary is None else str(Path(kappa_audit_summary)),
            "localization_contract": None if localization_contract is None else str(Path(localization_contract)),
            "reference_line_contract": None if reference_line_contract is None else str(Path(reference_line_contract)),
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
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "apollo_lateral_semantics_report": str(report_path),
        "apollo_lateral_semantics_summary": str(summary_path),
    }


def _anomalies(
    values: Mapping[str, list[float]],
    stats: Mapping[str, Any],
    thresholds: Mapping[str, float],
    *,
    rows: Sequence[Mapping[str, Any]],
    hdmap_route_lateral_consistency: Mapping[str, Any] | None = None,
    reference_debug_summary: Mapping[str, Any] | None = None,
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
        anomalies.append(
            _anomaly(
                "planning_nonempty_but_reference_line_debug_missing",
                (
                    "Planning trajectories are non-empty while reference-line debug reports zero "
                    "reference lines or provider-not-ready state"
                ),
                "reference_line_semantics",
                nonempty_trajectory_ratio=reference_debug_summary.get("nonempty_trajectory_ratio"),
                reference_line_provider_ready_ratio=reference_debug_summary.get("reference_line_provider_ready_ratio"),
                reference_line_count_zero_ratio=reference_debug_summary.get("reference_line_count_zero_ratio"),
                routing_segment_count_zero_ratio=reference_debug_summary.get("routing_segment_count_zero_ratio"),
                debug_gap_classification=reference_debug_summary.get("debug_gap_classification"),
                route_segment_available=reference_debug_summary.get("route_segment_available"),
                control_simple_lat_reference_available=reference_debug_summary.get("control_simple_lat_reference_available"),
                control_reference_join_coverage_ratio=reference_debug_summary.get("control_reference_join_coverage_ratio"),
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
    )
    for row in semantic_rows:
        current = dict(row)
        sim_time = _row_value(current, DRIFT_WINDOW_FIELD_ALIASES["sim_time"])
        nearest = _nearest_control_row(sim_time, times, indexed)
        if nearest is None:
            merged.append(current)
            continue
        nearest_time = _row_value(nearest, DRIFT_WINDOW_FIELD_ALIASES["sim_time"])
        current["_control_trace_merge_dt_s"] = abs(float(sim_time) - float(nearest_time))
        for field in control_fields:
            value = _row_value(nearest, FIELD_ALIASES.get(field, [field]))
            if value is not None:
                current[field] = value
        merged.append(current)
    return merged


def _nearest_control_row(
    sim_time: float | None,
    times: Sequence[float],
    indexed: Sequence[tuple[float, Mapping[str, Any]]],
) -> Mapping[str, Any] | None:
    if sim_time is None or not times:
        return None
    insert_at = bisect_left(times, sim_time)
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    if insert_at < len(indexed):
        candidates.append(indexed[insert_at])
    if insert_at > 0:
        candidates.append(indexed[insert_at - 1])
    if not candidates:
        return None
    nearest_time, nearest_row = min(candidates, key=lambda item: abs(float(item[0]) - float(sim_time)))
    if abs(float(nearest_time) - float(sim_time)) > CONTROL_TRACE_MERGE_MAX_DT_S:
        return None
    return nearest_row


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
    warnings = [str(item) for item in payload.get("warnings", []) if item not in {None, ""}]
    nonempty_ratio = _num(evidence.get("nonempty_trajectory_ratio"))
    ready_ratio = _num(evidence.get("reference_line_provider_ready_ratio"))
    count_zero_ratio = _num(metrics.get("reference_line_count_zero_ratio"))
    routing_segment_zero_ratio = _num(metrics.get("routing_segment_count_zero_ratio"))
    debug_missing = (
        "reference_line_count_zero_debug_counter_with_nonempty_trajectory" in warnings
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
        "route_segment_available": diagnostic.get("route_segment_available"),
        "control_simple_lat_reference_available": diagnostic.get("control_simple_lat_reference_available"),
        "control_reference_join_coverage_ratio": diagnostic.get("control_reference_join_coverage_ratio"),
        "nonempty_planning_with_reference_debug_missing": bool(debug_missing),
    }


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
    _set_if_numeric(normalized, "sim_time", _first_numeric_value(row.get("sim_time"), row.get("timestamp"), gt_state.get("sim_time_sec")))
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
