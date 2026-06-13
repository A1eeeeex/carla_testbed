from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.assist_ledger import read_assist_ledger_from_run_dir
from carla_testbed.analysis.artifact_completeness import check_run_artifact_completeness
from carla_testbed.analysis.natural_driving_evidence import build_natural_driving_evidence
from carla_testbed.record.route_curve_fields import ROUTE_CURVE_P0_FIELDS

NATURAL_DRIVING_REPORT_SCHEMA_VERSION = "town01_natural_driving_report.v1"
TRAFFIC_LIGHT_SCENARIO_CLASSES = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
CLAIM_GRADE_TRAFFIC_LIGHT_STIMULUS_MODES = {"deterministic_gt_control"}
BASE_REQUIRED_CHANNEL_RESULTS = (
    "localization",
    "chassis",
    "planning",
    "control",
)
LOCALIZATION_HARD_GATE_CHECKS = {
    "localization_channel_health": {"pass", "warn"},
    "sim_time_time_base": {"pass"},
    "frame_transform_configured": {"pass"},
    "position_uses_vrp": {"pass"},
    "source_to_published_reference_explained": {"pass", "warn"},
    "heading_from_transformed_forward": {"pass"},
    "rfu_to_enu_orientation": {"pass"},
    "decoded_orientation_consistency": {"pass", "warn"},
    "kinematics_frame_and_units": {"pass", "warn"},
    "chassis_speed_consistency": {"pass", "warn"},
    "lane_projection": {"pass", "warn"},
    "acceleration_semantics": {"pass", "warn"},
    "uncertainty_status_policy": {"pass", "warn"},
}
LOCALIZATION_REFERENCE_LINE_REQUIRED_CLASSES = {
    "curve_diagnostic",
    "junction_turn",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
EXPECTED_TRAFFIC_LIGHT_BEHAVIOR_BY_CLASS = {
    "traffic_light_red_stop": "red_stop",
    "traffic_light_green_go": "green_go",
    "traffic_light_red_to_green_release": "red_to_green_release",
}
TARGET_SCENARIO_CLASSES = (
    "lane_keep",
    "curve_diagnostic",
    "junction_turn",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
)
ROUTE_COMPLETION_REQUIRED_CLASSES = {
    "lane_keep",
    "junction_turn",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
ROUTE_HEALTH_HARD_GATE_SCENARIO_CLASSES = {
    "lane_keep",
    "junction_turn",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
DEFAULT_THRESHOLDS = {
    "min_route_completion": 0.8,
    "min_planning_nonempty_ratio": 0.8,
    "max_lateral_error_p95_m": 1.5,
    "max_heading_error_p95_rad": 0.35,
    "max_control_latency_p95_ms": 500.0,
    "max_red_stop_distance_m": 8.0,
}
APOLLO_CYBERRT_BACKEND = "apollo_cyberrt"
APOLLO_CONTROL_SOURCE = "/apollo/control"
PASS_WARN_STATUSES = {"pass", "warn"}
CONTROL_TRACE_REQUIRED_FIELDS = [
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "throttle_raw",
    "throttle_mapped",
    "throttle_applied",
    "brake_raw",
    "brake_mapped",
    "brake_applied",
    "lateral_guard_applied",
    "trajectory_contract_guard_applied",
]
MISSING_CONTROL_TRACE_FIELDS = [
    field for field in CONTROL_TRACE_REQUIRED_FIELDS if field in ROUTE_CURVE_P0_FIELDS
]

CSV_FIELDS = [
    "run_id",
    "scenario_id",
    "scenario_class",
    "route_id",
    "route_source",
    "route_evidence_level",
    "route_hard_gate_eligible",
    "route_evidence_reason",
    "run_dir",
    "algorithm_variant_id",
    "algorithm_variant_manifest_path",
    "online_config_path",
    "online_config_profile_name",
    "map",
    "transport_mode",
    "transport_mode_source",
    "backend",
    "truth_input",
    "active_assists",
    "blocking_assists",
    "non_blocking_assists",
    "assist_confidence",
    "assist_source_artifact",
    "can_claim_unassisted_natural_driving",
    "why_not_claimable",
    "control_source",
    "routing_success_count",
    "planning_nonempty_ratio",
    "planning_nonempty_ratio_for_claim",
    "planning_nonempty_ratio_source",
    "planning_nonempty_ratio_overall",
    "planning_nonempty_ratio_filtered",
    "planning_nonempty_ratio_filtered_after_routing_segment_available",
    "planning_nonempty_ratio_after_routing_segment_available",
    "planning_materialization_nonempty_ratio",
    "route_established",
    "route_establishment_source",
    "control_rx_count",
    "control_tx_count",
    "control_apply_count",
    "lateral_guard_apply_count",
    "trajectory_contract_guard_apply_count",
    "duration_s",
    "fixed_delta_seconds",
    "ticks",
    "runtime_contract_status",
    "routing_materialized",
    "planning_materialized",
    "control_handoff_status",
    "materialization_status",
    "apollo_route_contract_status",
    "apollo_route_contract_blocking_reasons",
    "apollo_route_contract_routing_phase",
    "apollo_route_contract_report_path",
    "planning_materialization_status",
    "planning_materialization_blocking_reasons",
    "planning_materialization_report_path",
    "apollo_chain_completion_verdict",
    "apollo_chain_completion_failure_stage",
    "apollo_chain_completion_report_path",
    "apollo_channel_health_status",
    "apollo_channel_gap_failures",
    "apollo_channel_low_rate_channels",
    "apollo_channel_missing_required_channels",
    "apollo_channel_failed_channels",
    "localization_contract_status",
    "localization_blocking_reasons",
    "localization_report_path",
    "chassis_gt_contract_status",
    "chassis_gt_contract_claim_grade",
    "chassis_gt_contract_blocking_reasons",
    "chassis_gt_contract_report_path",
    "apollo_reference_line_contract_status",
    "apollo_reference_line_blocking_reasons",
    "apollo_reference_line_report_path",
    "apollo_hdmap_projection_status",
    "apollo_hdmap_projection_claim_grade",
    "apollo_hdmap_projection_report_path",
    "control_health_status",
    "apollo_control_handoff_status",
    "apollo_control_handoff_failure_stage",
    "apollo_control_handoff_report_path",
    "failure_timeline_status",
    "failure_timeline_primary_failure",
    "failure_timeline_anchor_event",
    "failure_timeline_ordering_findings",
    "failure_route_s",
    "failure_before_route_start",
    "failure_near_route_start",
    "route_start_alignment_status",
    "route_start_alignment_reason",
    "initial_rear_axle_offset_compatible",
    "initial_rear_axle_offset_error_m",
    "static_spawn_lateral_offset_m",
    "recommended_ego_offset_y_delta_m",
    "verdict",
    "failure_reason",
    "route_completion",
    "lateral_error_p95",
    "heading_error_p95",
    "collision_count",
    "lane_invasion_count",
    "red_stop_distance_m",
    "stopped_at_red",
    "traffic_light_expected_behavior",
    "traffic_light_expectation_source",
    "traffic_light_stimulus_mode",
    "traffic_light_claim_grade",
    "traffic_light_control_available",
    "traffic_light_control_current_state",
    "traffic_light_control_phase",
    "traffic_light_control_release_observed",
    "green_pass_time_s",
    "red_to_green_release_time_s",
    "control_latency_p95_ms",
    "control_bridge_apply_world_frame_hz",
    "control_bridge_same_frame_drop_ratio",
    "control_bridge_bind_to_first_apply_s",
    "control_bridge_first_watchdog_apply_wall_s",
    "control_bridge_final_rx_count",
    "control_bridge_final_applied_count",
    "control_attribution_status",
    "control_attribution_dominant_breakpoint",
    "autoware_evidence_status",
    "autoware_can_compare_with_apollo",
    "autoware_recording_artifacts_ready",
    "autoware_gate_artifacts_ready",
    "apollo_lateral_semantics_status",
    "apollo_lateral_suspected_layer",
    "apollo_lateral_confidence",
    "matched_point_anomaly_count",
    "target_point_anomaly_count",
    "first_high_steer_seq",
    "control_trace_available",
    "missing_artifacts",
    "missing_fields",
    "missing_manifest_fields",
    "invalid_manifest_source_fields",
    "missing_control_trace_fields",
    "invalid_report_source_fields",
]


def analyze_natural_driving_suite(
    suite_run_root: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
    require_full_target_coverage: bool | None = None,
) -> dict[str, Any]:
    root = Path(suite_run_root).expanduser()
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})
    run_dirs = _discover_run_dirs(root)
    run_results = [_analyze_run_dir(path, root=root, thresholds=active_thresholds) for path in run_dirs]
    suite_plan_scope = _suite_plan_scope(root)
    planned_scenarios = [] if suite_plan_scope.get("filtered") else _planned_suite_scenarios(root)
    capability_coverage = _capability_coverage(
        run_results,
        planned_scenarios=planned_scenarios,
    )
    require_coverage = _has_suite_plan(root) if require_full_target_coverage is None else require_full_target_coverage
    verdict = _suite_verdict(
        run_results,
        capability_coverage=capability_coverage,
        suite_plan_scope=suite_plan_scope,
        require_full_target_coverage=require_coverage,
    )
    return {
        "schema_version": NATURAL_DRIVING_REPORT_SCHEMA_VERSION,
        "suite_run_root": str(root),
        "thresholds": active_thresholds,
        "run_count": len(run_results),
        "run_results": run_results,
        "capability_coverage": capability_coverage,
        "suite_plan_scope": suite_plan_scope,
        "verdict": verdict,
        "summary": {
            "pass_count": sum(1 for run in run_results if run["verdict"] == "pass"),
            "assisted_pass_count": sum(1 for run in run_results if run["verdict"] == "assisted_pass"),
            "diagnostic_only_count": sum(1 for run in run_results if run["verdict"] == "diagnostic_only"),
            "warn_count": sum(1 for run in run_results if run["verdict"] == "warn"),
            "fail_count": sum(1 for run in run_results if run["verdict"] == "fail"),
            "insufficient_data_count": sum(1 for run in run_results if run["verdict"] == "insufficient_data"),
            "missing_manifest_fields_run_count": sum(
                1 for run in run_results if run.get("missing_manifest_fields")
            ),
            "invalid_manifest_source_fields_run_count": sum(
                1 for run in run_results if run.get("invalid_manifest_source_fields")
            ),
            "missing_control_trace_fields_run_count": sum(
                1 for run in run_results if run.get("missing_control_trace_fields")
            ),
            "invalid_report_source_fields_run_count": sum(
                1 for run in run_results if run.get("invalid_report_source_fields")
            ),
            "blocking_assist_run_count": sum(1 for run in run_results if run.get("blocking_assists")),
            "unassisted_claimable_run_count": sum(
                1 for run in run_results if run.get("can_claim_unassisted_natural_driving") is True
            ),
        },
        "problem_run_details": problem_run_details(run_results),
        "interpretation_boundary": (
            "Natural-driving evaluation aggregates run artifacts only. It does not start CARLA/Apollo "
            "and must not treat candidate_positive or vehicle motion alone as natural-driving pass. "
            "Full Town01 natural-driving claims require capability_coverage.can_claim_full_natural_driving=true."
        ),
    }


def write_natural_driving_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "natural_driving_report.json"
    csv_path = output_dir / "natural_driving_report.csv"
    md_path = output_dir / "natural_driving_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(csv_path, report.get("run_results") or [])
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {
        "natural_driving_report": str(json_path),
        "natural_driving_csv": str(csv_path),
        "natural_driving_summary": str(md_path),
    }


def problem_run_details(
    run_results: Sequence[Mapping[str, Any]],
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for run in run_results:
        verdict = str(run.get("verdict") or "")
        if verdict not in {"fail", "warn", "insufficient_data", "assisted_pass", "diagnostic_only"}:
            continue
        details.append(
            {
                "run_id": run.get("run_id"),
                "scenario_class": run.get("scenario_class"),
                "route_id": run.get("route_id"),
                "verdict": verdict,
                "failure_reason": run.get("failure_reason"),
                "missing_artifacts": list(run.get("missing_artifacts") or [])[:10],
                "missing_fields": list(run.get("missing_fields") or [])[:10],
                "why_not_claimable": list(run.get("why_not_claimable") or [])[:10],
                "missing_manifest_fields": list(run.get("missing_manifest_fields") or [])[:10],
                "invalid_manifest_source_fields": list(run.get("invalid_manifest_source_fields") or [])[:10],
                "missing_control_trace_fields": list(run.get("missing_control_trace_fields") or [])[:10],
                "invalid_report_source_fields": list(run.get("invalid_report_source_fields") or [])[:10],
                "failure_route_s": run.get("failure_route_s"),
                "failure_before_route_start": run.get("failure_before_route_start"),
                "failure_near_route_start": run.get("failure_near_route_start"),
                "failure_timeline_ordering_findings": list(run.get("failure_timeline_ordering_findings") or [])[:10],
                "apollo_channel_health_status": run.get("apollo_channel_health_status"),
                "apollo_channel_gap_failures": list(run.get("apollo_channel_gap_failures") or [])[:10],
                "apollo_channel_low_rate_channels": list(run.get("apollo_channel_low_rate_channels") or [])[:10],
                "apollo_channel_missing_required_channels": list(
                    run.get("apollo_channel_missing_required_channels") or []
                )[:10],
                "apollo_channel_failed_channels": list(run.get("apollo_channel_failed_channels") or [])[:10],
                "localization_contract_status": run.get("localization_contract_status"),
                "localization_blocking_reasons": list(run.get("localization_blocking_reasons") or [])[:10],
                "localization_report_path": run.get("localization_report_path"),
                "control_health_status": run.get("control_health_status"),
                "control_attribution_status": run.get("control_attribution_status"),
                "control_attribution_dominant_breakpoint": run.get(
                    "control_attribution_dominant_breakpoint"
                ),
                "apollo_lateral_semantics_status": run.get("apollo_lateral_semantics_status"),
                "apollo_lateral_suspected_layer": run.get("apollo_lateral_suspected_layer"),
                "apollo_lateral_confidence": run.get("apollo_lateral_confidence"),
                "control_health_failure_reason": run.get("control_health_failure_reason"),
                "control_health_warnings": list(run.get("control_health_warnings") or [])[:10],
                "control_apply_observation_delay_s": run.get("control_apply_observation_delay_s"),
                "mapped_applied_throttle_abs_error_p95": run.get("mapped_applied_throttle_abs_error_p95"),
                "route_s_delta_m": run.get("route_s_delta_m"),
                "route_s_after_first_applied_control_delta_m": run.get(
                    "route_s_after_first_applied_control_delta_m"
                ),
                "stopped_ratio_after_first_applied_control": run.get(
                    "stopped_ratio_after_first_applied_control"
                ),
                "control_bridge_apply_world_frame_hz": run.get("control_bridge_apply_world_frame_hz"),
                "control_bridge_same_frame_drop_ratio": run.get("control_bridge_same_frame_drop_ratio"),
                "control_bridge_bind_to_first_apply_s": run.get("control_bridge_bind_to_first_apply_s"),
                "control_bridge_final_rx_count": run.get("control_bridge_final_rx_count"),
                "control_bridge_final_applied_count": run.get("control_bridge_final_applied_count"),
                "route_start_alignment_status": run.get("route_start_alignment_status"),
                "route_start_alignment_reason": run.get("route_start_alignment_reason"),
                "initial_rear_axle_offset_compatible": run.get("initial_rear_axle_offset_compatible"),
                "static_spawn_lateral_offset_m": run.get("static_spawn_lateral_offset_m"),
                "recommended_ego_offset_y_delta_m": run.get("recommended_ego_offset_y_delta_m"),
            }
        )
        if len(details) >= limit:
            break
    return details


def _discover_run_dirs(root: Path) -> list[Path]:
    if (root / "summary.json").exists():
        return [root]
    if not root.exists():
        return []
    summary_dirs = {path.parent for path in root.rglob("summary.json") if path.is_file()}
    matrix_dirs = set(_run_dirs_from_matrix(root, summary_dirs))
    return sorted(summary_dirs | matrix_dirs, key=lambda path: str(path))


def _run_dirs_from_matrix(root: Path, summary_dirs: set[Path]) -> list[Path]:
    matrix_path = root / "run_matrix.csv"
    if not matrix_path.exists():
        return []
    summary_dirs_resolved = {path.expanduser().resolve() for path in summary_dirs}
    run_dirs: list[Path] = []
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            raw = str(row.get("actual_run_dir") or row.get("run_dir") or "").strip()
            if not raw:
                continue
            path = _resolve_matrix_run_dir(root, raw)
            resolved = path.resolve()
            if any(_is_relative_to(summary_dir, resolved) for summary_dir in summary_dirs_resolved):
                continue
            run_dirs.append(resolved)
    return run_dirs


def _resolve_matrix_run_dir(root: Path, raw: str) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    if path.exists():
        return path
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return root / path


def _planned_suite_scenarios(root: Path) -> list[dict[str, str]]:
    matrix_path = root / "run_matrix.csv"
    if not matrix_path.exists():
        return []
    scenarios: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            scenario_id = str(row.get("scenario_id") or "").strip()
            route_id = str(row.get("route_id") or "").strip()
            scenario_class = str(row.get("scenario_class") or "").strip()
            if not scenario_id:
                continue
            key = (scenario_id, route_id, scenario_class)
            if key in seen:
                continue
            seen.add(key)
            scenarios.append(
                {
                    "scenario_id": scenario_id,
                    "route_id": route_id,
                    "scenario_class": scenario_class,
                }
            )
    return scenarios


def _analyze_run_dir(run_dir: Path, *, root: Path, thresholds: Mapping[str, float]) -> dict[str, Any]:
    summary_path = run_dir / "summary.json"
    summary = _read_json(summary_path)
    manifest = _read_json(run_dir / "manifest.json")
    route_health_path = _find_first(
        run_dir,
        [
            "analysis/route_health/route_health.json",
            "route_health.json",
        ],
    )
    route_health_csv_path = _find_first(
        run_dir,
        [
            "analysis/route_health/route_health.csv",
            "route_health.csv",
        ],
    )
    curve_segments_path = _find_first(
        run_dir,
        [
            "analysis/route_health/curve_segments.csv",
            "curve_segments.csv",
        ],
    )
    channel_health_path = _find_first(
        run_dir,
        [
            "analysis/apollo_channel_health/apollo_channel_health_report.json",
            "apollo_channel_health_report.json",
        ],
    )
    localization_contract_path = _find_first(
        run_dir,
        [
            "analysis/localization_contract/localization_contract_report.json",
            "localization_contract_report.json",
        ],
    )
    chassis_gt_contract_path = _find_first(
        run_dir,
        [
            "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
            "chassis_gt_contract_report.json",
        ],
    )
    apollo_reference_line_contract_path = _find_first(
        run_dir,
        [
            "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
            "apollo_reference_line_contract_report.json",
        ],
    )
    apollo_hdmap_projection_path = _find_first(
        run_dir,
        [
            "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
            "apollo_hdmap_projection_report.json",
        ],
    )
    control_health_path = _find_first(
        run_dir,
        [
            "analysis/control_health/control_health_report.json",
            "control_health_report.json",
        ],
    )
    apollo_control_handoff_path = _find_first(
        run_dir,
        [
            "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
            "apollo_control_handoff_report.json",
        ],
    )
    apollo_route_contract_path = _find_first(
        run_dir,
        [
            "analysis/apollo_route_contract/apollo_route_contract_report.json",
            "apollo_route_contract_report.json",
        ],
    )
    planning_materialization_path = _find_first(
        run_dir,
        [
            "analysis/planning_materialization/planning_materialization_report.json",
            "planning_materialization_report.json",
        ],
    )
    apollo_chain_completion_path = _find_first(
        run_dir,
        [
            "analysis/apollo_chain_completion/apollo_chain_completion_report.json",
            "apollo_chain_completion_report.json",
        ],
    )
    control_attribution_path = _find_first(
        run_dir,
        [
            "analysis/control_attribution/control_attribution_report.json",
            "control_attribution_report.json",
        ],
    )
    apollo_lateral_semantics_path = _find_first(
        run_dir,
        [
            "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
            "apollo_lateral_semantics_report.json",
        ],
    )
    autoware_evidence_path = _find_first(
        run_dir,
        [
            "analysis/autoware_evidence/autoware_evidence_report.json",
            "autoware_evidence_report.json",
        ],
    )
    failure_timeline_path = _find_first(
        run_dir,
        [
            "analysis/failure_timeline/failure_timeline_report.json",
            "failure_timeline_report.json",
        ],
    )
    traffic_light_contract_path = _find_first(
        run_dir,
        [
            "analysis/traffic_light_contract/traffic_light_contract_report.json",
            "analysis/traffic_light/traffic_light_contract_report.json",
            "traffic_light_contract_report.json",
        ],
    )
    route_curve_artifact_gap_path = _find_first(
        run_dir,
        [
            "analysis/route_curve_artifact_gap/route_curve_artifact_gap_report.json",
            "route_curve_artifact_gap_report.json",
        ],
    )
    traffic_light_behavior_path = _find_first(
        run_dir,
        [
            "analysis/traffic_light_behavior/traffic_light_behavior_report.json",
            "analysis/traffic_light/traffic_light_behavior_report.json",
            "traffic_light_behavior_report.json",
        ],
    )
    artifact_completeness_path = _find_first(
        run_dir,
        [
            "analysis/artifact_completeness/artifact_completeness_report.json",
            "artifact_completeness_report.json",
        ],
    )
    config_resolved_path = _find_first(run_dir, ["config.resolved.yaml", "effective_config.yaml"])
    route_health_summary_path = _find_first(
        run_dir,
        [
            "analysis/route_health/route_health_summary.md",
            "route_health_summary.md",
        ],
    )
    timeseries_path = _find_first(run_dir, ["timeseries.csv"])
    events_path = _find_first(run_dir, ["events.jsonl"])
    assist_ledger_path = _find_first(
        run_dir,
        [
            "assist_ledger.json",
            "artifacts/assist_ledger.json",
            "analysis/assist_ledger/assist_ledger.json",
        ],
    )

    route_health = _read_json(route_health_path)
    channel_health = _read_json(channel_health_path)
    localization_contract = _read_json(localization_contract_path)
    chassis_gt_contract = _read_json(chassis_gt_contract_path)
    apollo_reference_line_contract = _read_json(apollo_reference_line_contract_path)
    apollo_hdmap_projection = _read_json(apollo_hdmap_projection_path)
    control_health = _read_json(control_health_path)
    apollo_control_handoff = _read_json(apollo_control_handoff_path)
    apollo_route_contract = _read_json(apollo_route_contract_path)
    planning_materialization = _read_json(planning_materialization_path)
    apollo_chain_completion = _read_json(apollo_chain_completion_path)
    control_attribution = _read_json(control_attribution_path)
    apollo_lateral_semantics = _read_json(apollo_lateral_semantics_path)
    autoware_evidence = _read_json(autoware_evidence_path)
    assist_ledger = read_assist_ledger_from_run_dir(run_dir)
    failure_timeline = _read_json(failure_timeline_path)
    route_start_alignment_path = _find_first(
        run_dir,
        [
            "analysis/route_start_alignment/route_start_alignment_report.json",
            "route_start_alignment_report.json",
        ],
    )
    route_start_alignment = _read_json(route_start_alignment_path)
    traffic_light_contract = _read_json(traffic_light_contract_path)
    route_curve_artifact_gap = _read_json(route_curve_artifact_gap_path)
    traffic_light_behavior = _read_json(traffic_light_behavior_path)
    traffic_light_expectation, traffic_light_expectation_source = _traffic_light_expectation(
        manifest,
        traffic_light_behavior,
    )
    apollo_semantics = route_health.get("apollo_semantics")
    if not isinstance(apollo_semantics, Mapping):
        apollo_semantics = {}
    timeseries_rows = _read_csv_rows(timeseries_path)
    metrics = _summary_metrics(summary)
    scenario_class = _first_text(
        summary,
        "scenario_class",
        manifest,
        "scenario_class",
        default=_infer_scenario_class(run_dir.name),
    )
    traffic_light_control = _traffic_light_control(summary, manifest)
    completeness = check_run_artifact_completeness(run_dir, scenario_class=scenario_class)
    missing_artifacts = list(completeness["missing_artifacts"])
    if artifact_completeness_path is None:
        missing_artifacts.append("artifact_completeness_report.json")
    control_metrics = control_health.get("metrics")
    if not isinstance(control_metrics, Mapping):
        control_metrics = {}
    control_bridge_log_metrics = control_metrics.get("control_bridge_log")
    if not isinstance(control_bridge_log_metrics, Mapping):
        control_bridge_log_metrics = {}
    control_source = _control_source(summary, manifest, apollo_control_handoff, control_health)
    routing_success_count = _routing_success_count(summary, manifest, apollo_control_handoff)
    planning_nonempty = _planning_nonempty_metric(
        summary,
        manifest,
        apollo_reference_line_contract,
        apollo_control_handoff,
        planning_materialization,
    )
    planning_nonempty_ratio = planning_nonempty["ratio"]
    control_rx_count = _control_rx_count(summary, manifest, apollo_control_handoff, control_health)
    control_tx_count = _control_tx_count(summary, manifest, apollo_control_handoff, control_health)
    control_apply_count = _control_apply_count(
        summary,
        manifest,
        apollo_control_handoff,
        control_health,
    )
    lateral_guard_apply_count = _guard_apply_count(
        "lateral_guard_apply_count",
        "lateral_guard",
        summary=summary,
        manifest=manifest,
        route_health=route_health,
        control_health=control_health,
    )
    trajectory_guard_apply_count = _guard_apply_count(
        "trajectory_contract_guard_apply_count",
        "trajectory_contract_lateral_guard",
        summary=summary,
        manifest=manifest,
        route_health=route_health,
        control_health=control_health,
    )
    run_result: dict[str, Any] = {
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=run_dir.name),
        "scenario_id": _first_text(summary, "scenario_id", manifest, "scenario_id", default=run_dir.name),
        "scenario_class": scenario_class,
        "gate_role": _first_text(summary, "gate_role", manifest, "gate_role"),
        "route_id": _first_text(summary, "route_id", manifest, "route_id", route_health, "route_id"),
        "route_source": route_health.get("route_source"),
        "route_evidence_level": route_health.get("evidence_level"),
        "route_hard_gate_eligible": route_health.get("hard_gate_eligible"),
        "route_evidence_reason": route_health.get("route_evidence_reason"),
        "run_dir": str(run_dir.relative_to(root) if _is_relative_to(run_dir, root) else run_dir),
        "algorithm_variant_id": _first_text(manifest, "algorithm_variant_id", manifest, "variant_id"),
        "algorithm_variant_manifest_path": _first_text(manifest, "algorithm_variant_manifest_path"),
        "online_config_path": _first_text(manifest, "online_config_path"),
        "online_config_profile_name": _first_text(manifest, "online_config_profile_name"),
        "map": _first_text(manifest, "map", manifest, "town_map"),
        "transport_mode": _first_text(manifest, "transport_mode"),
        "transport_mode_source": _first_text(manifest, "transport_mode_source"),
        "backend": _first_text(manifest, "backend"),
        "truth_input": _truth_input_declared(manifest),
        "assist_ledger": assist_ledger,
        "active_assists": list(assist_ledger.get("active_assists") or []),
        "blocking_assists": list(assist_ledger.get("blocking_assists") or []),
        "non_blocking_assists": list(assist_ledger.get("non_blocking_assists") or []),
        "assist_confidence": assist_ledger.get("assist_confidence"),
        "assist_source_artifact": assist_ledger.get("source_artifact"),
        "can_claim_unassisted_natural_driving": False,
        "why_not_claimable": [],
        "control_source": control_source,
        "routing_success_count": routing_success_count,
        "planning_nonempty_ratio": planning_nonempty_ratio,
        "planning_nonempty_ratio_for_claim": planning_nonempty_ratio,
        "planning_nonempty_ratio_source": planning_nonempty["source"],
        "planning_nonempty_ratio_overall": planning_nonempty["overall_ratio"],
        "planning_nonempty_ratio_filtered": planning_nonempty["filtered_ratio"],
        "planning_nonempty_ratio_filtered_after_routing_segment_available": planning_nonempty[
            "after_routing_segment_available"
        ],
        "planning_nonempty_ratio_after_routing_segment_available": planning_nonempty[
            "after_routing_segment_available"
        ],
        "planning_materialization_nonempty_ratio": planning_nonempty[
            "planning_materialization_nonempty_ratio"
        ],
        "route_established": planning_nonempty["route_established"],
        "route_establishment_source": planning_nonempty["route_establishment_source"],
        "control_rx_count": control_rx_count,
        "control_tx_count": control_tx_count,
        "control_apply_count": control_apply_count,
        "lateral_guard_apply_count": lateral_guard_apply_count,
        "trajectory_contract_guard_apply_count": trajectory_guard_apply_count,
        "duration_s": _num(manifest.get("duration_s")),
        "fixed_delta_seconds": _num(manifest.get("fixed_delta_seconds")),
        "ticks": _num(manifest.get("ticks")),
        "runtime_contract_status": _runtime_contract_status(summary, manifest),
        "routing_materialized": _summary_bool(summary, "routing_materialized"),
        "planning_materialized": _summary_bool(summary, "planning_materialized"),
        "control_handoff_status": _control_handoff_status(summary),
        "materialization_status": _summary_text(summary, "materialization_status"),
        "apollo_route_contract_status": apollo_route_contract.get("status"),
        "apollo_route_contract_blocking_reasons": list(
            apollo_route_contract.get("blocking_reasons") or []
        ),
        "apollo_route_contract_routing_phase": apollo_route_contract.get("routing_phase"),
        "apollo_route_contract_report_path": (
            str(apollo_route_contract_path) if apollo_route_contract_path else None
        ),
        "planning_materialization_status": planning_materialization.get("verdict"),
        "planning_materialization_blocking_reasons": list(
            planning_materialization.get("blocking_reasons") or []
        ),
        "planning_materialization_report_path": (
            str(planning_materialization_path) if planning_materialization_path else None
        ),
        "apollo_chain_completion_verdict": apollo_chain_completion.get("verdict"),
        "apollo_chain_completion_failure_stage": apollo_chain_completion.get("failure_stage"),
        "apollo_chain_completion_report_path": (
            str(apollo_chain_completion_path) if apollo_chain_completion_path else None
        ),
        "control_health_status": control_health.get("status"),
        "control_health_failure_reason": control_health.get("failure_reason"),
        "control_health_warnings": list(control_health.get("warnings") or []),
        "apollo_control_handoff_status": apollo_control_handoff.get("verdict"),
        "apollo_control_handoff_failure_stage": apollo_control_handoff.get("failure_stage"),
        "apollo_control_handoff_report_path": (
            str(apollo_control_handoff_path) if apollo_control_handoff_path else None
        ),
        "apollo_control_handoff_blocking_reasons": list(
            apollo_control_handoff.get("blocking_reasons") or []
        ),
        "control_apply_observation_delay_s": _num(control_metrics.get("control_apply_observation_delay_s")),
        "mapped_applied_throttle_abs_error_p95": _num(
            control_metrics.get("mapped_applied_throttle_abs_error_p95")
        ),
        "route_s_delta_m": _num(control_metrics.get("route_s_delta_m")),
        "route_s_after_first_applied_control_delta_m": _num(
            control_metrics.get("route_s_after_first_applied_control_delta_m")
        ),
        "stopped_ratio_after_first_applied_control": _num(
            control_metrics.get("stopped_ratio_after_first_applied_control")
        ),
        "control_bridge_apply_world_frame_hz": _num(
            control_bridge_log_metrics.get("apply_world_frame_hz")
        ),
        "control_bridge_same_frame_drop_ratio": _num(
            control_bridge_log_metrics.get("same_frame_drop_ratio")
        ),
        "control_bridge_bind_to_first_apply_s": _num(
            control_bridge_log_metrics.get("bind_to_first_apply_s")
        ),
        "control_bridge_first_watchdog_apply_wall_s": _num(
            control_bridge_log_metrics.get("first_watchdog_apply_wall_s")
        ),
        "control_bridge_final_rx_count": _num(control_bridge_log_metrics.get("final_rx_count")),
        "control_bridge_final_applied_count": _num(
            control_bridge_log_metrics.get("final_applied_count")
        ),
        "control_attribution_status": _control_attribution_status(control_attribution),
        "control_attribution_dominant_breakpoint": _control_attribution_dominant_breakpoint(
            control_attribution
        ),
        "autoware_evidence_status": autoware_evidence.get("artifact_completeness_status"),
        "autoware_can_compare_with_apollo": autoware_evidence.get("can_compare_with_apollo"),
        "autoware_recording_artifacts_ready": autoware_evidence.get("recording_artifacts_ready"),
        "autoware_gate_artifacts_ready": autoware_evidence.get("gate_artifacts_ready"),
        "apollo_channel_health_status": channel_health.get("status"),
        "apollo_channel_gap_failures": _string_list(channel_health.get("gap_failures")),
        "apollo_channel_low_rate_channels": _string_list(channel_health.get("low_rate_channels")),
        "apollo_channel_missing_required_channels": _string_list(
            channel_health.get("missing_required_channels")
        ),
        "apollo_channel_failed_channels": _failed_channel_names(channel_health),
        "localization_contract_status": _localization_contract_status(localization_contract),
        "localization_blocking_reasons": _localization_blocking_reasons(localization_contract),
        "localization_report_path": str(localization_contract_path) if localization_contract_path else None,
        "chassis_gt_contract_status": _chassis_gt_contract_status(chassis_gt_contract),
        "chassis_gt_contract_claim_grade": _chassis_gt_contract_claim_grade(chassis_gt_contract),
        "chassis_gt_contract_blocking_reasons": _chassis_gt_contract_blocking_reasons(
            chassis_gt_contract
        ),
        "chassis_gt_contract_report_path": (
            str(chassis_gt_contract_path) if chassis_gt_contract_path else None
        ),
        "apollo_reference_line_contract_status": _apollo_reference_line_contract_status(
            apollo_reference_line_contract
        ),
        "apollo_reference_line_blocking_reasons": _apollo_reference_line_blocking_reasons(
            apollo_reference_line_contract
        ),
        "apollo_reference_line_report_path": (
            str(apollo_reference_line_contract_path) if apollo_reference_line_contract_path else None
        ),
        "apollo_hdmap_projection_status": _apollo_hdmap_projection_status(apollo_hdmap_projection),
        "apollo_hdmap_projection_claim_grade": _apollo_hdmap_projection_claim_grade(apollo_hdmap_projection),
        "apollo_hdmap_projection_report_path": (
            str(apollo_hdmap_projection_path) if apollo_hdmap_projection_path else None
        ),
        "failure_timeline_status": failure_timeline.get("status"),
        "failure_timeline_primary_failure": _failure_timeline_primary_failure(failure_timeline),
        "failure_timeline_anchor_event": _failure_timeline_anchor_event(failure_timeline),
        "failure_timeline_ordering_findings": _failure_timeline_ordering_findings(failure_timeline),
        "failure_route_s": _failure_route_s(failure_timeline),
        "failure_before_route_start": _failure_start_gate_bool(
            failure_timeline,
            "anchor_before_route_start",
        ),
        "failure_near_route_start": _failure_start_gate_bool(
            failure_timeline,
            "anchor_near_route_start",
        ),
        "route_start_alignment_status": route_start_alignment.get("status"),
        "route_start_alignment_reason": route_start_alignment.get("reason"),
        "initial_rear_axle_offset_compatible": _initial_rear_axle_offset_compatible(route_start_alignment),
        "initial_rear_axle_offset_error_m": _initial_rear_axle_offset_error(route_start_alignment),
        "static_spawn_lateral_offset_m": _static_spawn_lateral_offset(route_start_alignment),
        "recommended_ego_offset_y_delta_m": _recommended_ego_offset_y_delta(route_start_alignment),
        "artifacts": {
            "manifest": str(run_dir / "manifest.json") if (run_dir / "manifest.json").exists() else None,
            "summary": str(summary_path) if summary_path.exists() else None,
            "config_resolved": str(config_resolved_path) if config_resolved_path else None,
            "events": str(events_path) if events_path else None,
            "timeseries": str(timeseries_path) if timeseries_path else None,
            "route_health": str(route_health_path) if route_health_path else None,
            "route_health_csv": str(route_health_csv_path) if route_health_csv_path else None,
            "curve_segments": str(curve_segments_path) if curve_segments_path else None,
            "route_health_summary": str(route_health_summary_path) if route_health_summary_path else None,
            "apollo_channel_health": str(channel_health_path) if channel_health_path else None,
            "localization_contract": str(localization_contract_path) if localization_contract_path else None,
            "chassis_gt_contract": str(chassis_gt_contract_path) if chassis_gt_contract_path else None,
            "apollo_reference_line_contract": (
                str(apollo_reference_line_contract_path) if apollo_reference_line_contract_path else None
            ),
            "apollo_hdmap_projection": str(apollo_hdmap_projection_path) if apollo_hdmap_projection_path else None,
            "control_health": str(control_health_path) if control_health_path else None,
            "apollo_control_handoff": str(apollo_control_handoff_path)
            if apollo_control_handoff_path
            else None,
            "apollo_route_contract": (
                str(apollo_route_contract_path) if apollo_route_contract_path else None
            ),
            "planning_materialization": (
                str(planning_materialization_path) if planning_materialization_path else None
            ),
            "apollo_chain_completion": (
                str(apollo_chain_completion_path) if apollo_chain_completion_path else None
            ),
            "control_attribution": str(control_attribution_path) if control_attribution_path else None,
            "autoware_evidence": str(autoware_evidence_path) if autoware_evidence_path else None,
            "apollo_lateral_semantics": str(apollo_lateral_semantics_path)
            if apollo_lateral_semantics_path
            else None,
            "failure_timeline": str(failure_timeline_path) if failure_timeline_path else None,
            "route_start_alignment": str(route_start_alignment_path) if route_start_alignment_path else None,
            "traffic_light_contract": str(traffic_light_contract_path) if traffic_light_contract_path else None,
            "traffic_light_behavior": str(traffic_light_behavior_path) if traffic_light_behavior_path else None,
            "artifact_completeness": str(artifact_completeness_path) if artifact_completeness_path else None,
            "assist_ledger": str(assist_ledger_path) if assist_ledger_path else None,
            "route_curve_artifact_gap": str(route_curve_artifact_gap_path)
            if route_curve_artifact_gap_path
            else None,
        },
        "route_curve_artifact_gap_status": route_curve_artifact_gap.get("status"),
        "route_curve_artifact_gap_failure_reason": route_curve_artifact_gap.get("failure_reason"),
        "route_curve_missing_p1_fields": route_curve_artifact_gap.get("missing_p1_fields") or [],
        "artifact_completeness": completeness,
        "route_completion": _metric(metrics, "route_completion", "route_completion_ratio", rows=timeseries_rows),
        "lateral_error_p95": _route_health_metric(
            route_health,
            "lateral_error_p95_m",
            "lateral_error_p95",
            rows=timeseries_rows,
            row_field="cross_track_error",
            absolute=True,
        ),
        "heading_error_p95": _route_health_metric(
            route_health,
            "heading_error_p95_rad",
            "heading_error_p95",
            rows=timeseries_rows,
            row_field="heading_error",
            absolute=True,
        ),
        "collision_count": _metric(metrics, "collision_count", default=0),
        "lane_invasion_count": _metric(metrics, "lane_invasion_count", default=0),
        "red_stop_distance_m": _traffic_behavior_metric(
            traffic_light_behavior,
            "red_stop_distance_m",
            fallback=_metric(metrics, "red_stop_distance_m", rows=timeseries_rows),
        ),
        "stopped_at_red": _traffic_behavior_bool(
            traffic_light_behavior,
            "stopped_at_red",
            fallback=_bool_metric(summary, "stopped_at_red"),
        ),
        "traffic_light_expectation": traffic_light_expectation,
        "traffic_light_expected_behavior": (
            traffic_light_expectation.get("expected_behavior") if traffic_light_expectation else None
        ),
        "traffic_light_expectation_source": traffic_light_expectation_source,
        "traffic_light_stimulus_mode": (
            traffic_light_expectation.get("stimulus_mode") if traffic_light_expectation else None
        ),
        "traffic_light_claim_grade": _traffic_light_claim_grade(traffic_light_expectation),
        "traffic_light_control": traffic_light_control,
        "traffic_light_control_available": bool(traffic_light_control),
        "traffic_light_control_current_state": traffic_light_control.get("current_state"),
        "traffic_light_control_phase": traffic_light_control.get("current_phase"),
        "traffic_light_control_release_observed": _traffic_light_release_observed(traffic_light_control),
        "green_pass_time_s": _traffic_behavior_metric(
            traffic_light_behavior,
            "green_pass_time_s",
            fallback=_metric(metrics, "green_pass_time_s", rows=timeseries_rows),
        ),
        "red_to_green_release_time_s": _traffic_behavior_metric(
            traffic_light_behavior,
            "red_to_green_release_time_s",
            fallback=_metric(metrics, "red_to_green_release_time_s", rows=timeseries_rows),
        ),
        "matched_point_anomaly_count": len(apollo_semantics.get("matched_point_anomaly_locations") or []),
        "target_point_anomaly_count": len(apollo_semantics.get("target_point_anomaly_locations") or []),
        "first_high_steer_seq": _first_high_steer_seq(apollo_semantics),
        "apollo_lateral_semantics_status": _report_status(apollo_lateral_semantics),
        "apollo_lateral_suspected_layer": apollo_lateral_semantics.get("suspected_layer"),
        "apollo_lateral_confidence": apollo_lateral_semantics.get("confidence"),
        "control_latency_p95_ms": _metric(
            metrics,
            "control_latency_p95_ms",
            rows=timeseries_rows,
            row_field="control_latency_ms",
            percentile=0.95,
        ),
        "control_trace_available": completeness["control_trace_available"],
        "missing_manifest_fields": completeness.get("missing_manifest_fields") or [],
        "invalid_manifest_source_fields": completeness.get("invalid_manifest_source_fields") or [],
        "missing_control_trace_fields": completeness["missing_control_trace_fields"],
        "invalid_report_source_fields": completeness.get("invalid_report_source_fields") or [],
        "missing_artifacts": missing_artifacts,
        "missing_fields": [],
    }
    claimability = _unassisted_claimability(
        run_result,
        summary=summary,
        manifest=manifest,
        localization_contract=localization_contract,
        chassis_gt_contract=chassis_gt_contract,
        apollo_reference_line_contract=apollo_reference_line_contract,
        apollo_hdmap_projection=apollo_hdmap_projection,
        apollo_route_contract=apollo_route_contract,
        planning_materialization=planning_materialization,
        apollo_chain_completion=apollo_chain_completion,
        apollo_control_handoff=apollo_control_handoff,
        control_health=control_health,
        assist_ledger_path=assist_ledger_path,
    )
    run_result["can_claim_unassisted_natural_driving"] = claimability["can_claim_unassisted"]
    run_result["why_not_claimable"] = claimability["why_not_claimable"]
    verdict, failure_reason, missing_fields = _run_verdict(
        run_result,
        summary=summary,
        route_health=route_health,
        channel_health=channel_health,
        localization_contract=localization_contract,
        apollo_reference_line_contract=apollo_reference_line_contract,
        apollo_hdmap_projection=apollo_hdmap_projection,
        apollo_control_handoff=apollo_control_handoff,
        control_health=control_health,
        traffic_light_contract=traffic_light_contract,
        traffic_light_behavior=traffic_light_behavior,
        route_curve_artifact_gap=route_curve_artifact_gap,
        run_dir=run_dir,
        route_health_path=route_health_path,
        channel_health_path=channel_health_path,
        localization_contract_path=localization_contract_path,
        apollo_reference_line_contract_path=apollo_reference_line_contract_path,
        apollo_hdmap_projection_path=apollo_hdmap_projection_path,
        apollo_control_handoff_path=apollo_control_handoff_path,
        control_health_path=control_health_path,
        traffic_light_contract_path=traffic_light_contract_path,
        traffic_light_behavior_path=traffic_light_behavior_path,
        route_curve_artifact_gap_path=route_curve_artifact_gap_path,
        thresholds=thresholds,
    )
    run_result["verdict"] = verdict
    run_result["failure_reason"] = failure_reason
    run_result["missing_fields"] = missing_fields
    run_result["evidence"] = build_natural_driving_evidence(
        run_dir=run_dir,
        summary=summary,
        manifest=manifest,
        route_health=route_health,
        channel_health=channel_health,
        localization_contract=localization_contract,
        apollo_reference_line_contract=apollo_reference_line_contract,
        apollo_control_handoff=apollo_control_handoff,
        control_attribution=control_attribution,
        assist_ledger=assist_ledger,
        route_health_path=route_health_path,
        channel_health_path=channel_health_path,
        localization_contract_path=localization_contract_path,
        apollo_reference_line_contract_path=apollo_reference_line_contract_path,
        apollo_control_handoff_path=apollo_control_handoff_path,
        control_attribution_path=control_attribution_path,
        traffic_light_evidence_path=traffic_light_behavior_path or traffic_light_contract_path,
        assist_ledger_path=assist_ledger_path,
    ).to_dict()
    return run_result


def _unassisted_claimability(
    run: Mapping[str, Any],
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    localization_contract: Mapping[str, Any],
    chassis_gt_contract: Mapping[str, Any],
    apollo_reference_line_contract: Mapping[str, Any],
    apollo_hdmap_projection: Mapping[str, Any],
    apollo_route_contract: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
    apollo_chain_completion: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
    assist_ledger_path: Path | None,
) -> dict[str, Any]:
    reasons: list[str] = []
    scenario_class = str(run.get("scenario_class") or "")

    if str(run.get("backend") or "").strip() != APOLLO_CYBERRT_BACKEND:
        reasons.append("backend_not_apollo_cyberrt")

    algorithm_variant_id = str(run.get("algorithm_variant_id") or "").strip()
    algorithm_variant_manifest_path = str(run.get("algorithm_variant_manifest_path") or "").strip()
    if not algorithm_variant_id:
        reasons.append("algorithm_variant_id_missing")
    if not algorithm_variant_manifest_path:
        reasons.append("algorithm_variant_manifest_path_missing")
    for field in run.get("invalid_manifest_source_fields") or []:
        field_text = str(field)
        if field_text == "algorithm_variant_manifest_path":
            reasons.append("algorithm_variant_manifest_path_invalid")
        elif field_text == "algorithm_variant_manifest_path.variant_id_mismatch":
            reasons.append("algorithm_variant_manifest_id_mismatch")
        elif field_text == "algorithm_variant_manifest_path.variant_type_not_truth_input_closed_loop":
            reasons.append("algorithm_variant_not_truth_input_closed_loop")

    control_source = str(run.get("control_source") or "").strip()
    if not control_source:
        reasons.append("control_source_missing")
    elif control_source != APOLLO_CONTROL_SOURCE:
        reasons.append("control_source_not_apollo_control")

    routing_success_count = _num(run.get("routing_success_count"))
    if routing_success_count is None:
        reasons.append("routing_success_count_missing")
    elif routing_success_count < 1:
        reasons.append("routing_success_count_zero")

    planning_nonempty_ratio = _num(run.get("planning_nonempty_ratio"))
    if planning_nonempty_ratio is None:
        reasons.append("planning_nonempty_ratio_missing")
    elif planning_nonempty_ratio < DEFAULT_THRESHOLDS["min_planning_nonempty_ratio"]:
        reasons.append("planning_nonempty_ratio_low")

    route_contract_status = str(apollo_route_contract.get("status") or "").strip()
    route_contract_blockers = [
        str(item) for item in (apollo_route_contract.get("blocking_reasons") or []) if item
    ]
    claim_route_contract = apollo_route_contract.get("claim_route_contract")
    if not apollo_route_contract:
        reasons.append("apollo_route_contract_missing")
    elif route_contract_status == "fail":
        reasons.append("apollo_route_contract_failed")
    elif route_contract_status != "pass":
        reasons.append("apollo_route_contract_not_claim_grade")
    if isinstance(claim_route_contract, Mapping) and claim_route_contract.get("materialized") is False:
        reasons.append("claim_route_not_materialized")
    for blocker in route_contract_blockers:
        if blocker in {"claim_route_not_materialized", "apollo_routing_length_mismatch"}:
            reasons.append(blocker)

    planning_materialization_status = str(planning_materialization.get("verdict") or "").strip()
    planning_materialization_blockers = [
        str(item) for item in (planning_materialization.get("blocking_reasons") or []) if item
    ]
    if not planning_materialization:
        reasons.append("planning_materialization_missing")
    elif planning_materialization_status == "fail":
        reasons.append("planning_materialization_failed")
    elif planning_materialization_status not in PASS_WARN_STATUSES:
        reasons.append("planning_materialization_not_pass_warn")
    if "route_establishment_not_confirmed" in planning_materialization_blockers:
        reasons.append("route_establishment_not_confirmed")

    chain_verdict = str(apollo_chain_completion.get("verdict") or "").strip()
    chain_stage = str(apollo_chain_completion.get("failure_stage") or "").strip()
    if not apollo_chain_completion:
        reasons.append("apollo_chain_completion_missing")
    elif chain_verdict == "fail":
        if chain_stage == "planning_materialization":
            reasons.append("planning_materialization_failed")
        elif chain_stage in {"route_establishment", "hdmap_or_route_contract"}:
            reasons.append("route_establishment_not_confirmed")
        elif chain_stage in {"control_handoff", "process_health"}:
            reasons.append("control_process_failed")
        else:
            reasons.append(f"apollo_chain_completion_failed:{chain_stage or 'unknown'}")

    for field, reason in (
        ("control_rx_count", "control_rx_count_missing_or_zero"),
        ("control_tx_count", "control_tx_count_missing_or_zero"),
        ("control_apply_count", "control_apply_count_missing_or_zero"),
    ):
        value = _num(run.get(field))
        if value is None or value < 1:
            reasons.append(reason)

    localization_blockers = _localization_blocking_reasons(localization_contract)
    if "duplicate_localization_timestamps_claim_grade_blocked" in localization_blockers:
        reasons.append("stale_gt_republish_claim_blocked")
    if not _localization_claim_grade(localization_contract):
        reasons.append("localization_contract_not_claim_grade")

    if not _chassis_gt_contract_claim_grade(chassis_gt_contract):
        reasons.append("chassis_gt_contract_not_claim_grade")

    reference_status = _apollo_reference_line_contract_status(apollo_reference_line_contract)
    if reference_status != "pass" or _apollo_reference_line_blocking_reasons(
        apollo_reference_line_contract
    ):
        reasons.append("apollo_reference_line_contract_not_pass")
    if not _apollo_hdmap_projection_claim_grade(apollo_hdmap_projection):
        reasons.append("apollo_hdmap_projection_not_claim_grade")

    handoff_status = str(apollo_control_handoff.get("verdict") or "").strip()
    handoff_stage = str(apollo_control_handoff.get("failure_stage") or "").strip()
    if handoff_status != "pass" or handoff_stage not in {"", "none"}:
        reasons.append("control_handoff_not_pass")
    if handoff_stage in {"process_health", "control_process", "process_crash"}:
        reasons.append("control_process_failed")
    handoff_blockers = {str(item) for item in (apollo_control_handoff.get("blocking_reasons") or [])}
    if any("process" in item or "crash" in item for item in handoff_blockers):
        reasons.append("control_process_failed")
    process_health = apollo_control_handoff.get("process_health")
    if isinstance(process_health, Mapping):
        crash_reason = str(
            process_health.get("crash_reason")
            or process_health.get("fatal_signal")
            or process_health.get("failure_reason")
            or ""
        )
        if _parse_bool(process_health.get("crash_detected")) is True or crash_reason:
            reasons.append("control_process_crash_before_control_output")

    if _control_health_blocking(control_health):
        reasons.append("control_health_blocking")

    assist_confidence = str(run.get("assist_confidence") or "").strip()
    active_assists = [str(item) for item in (run.get("active_assists") or []) if item]
    if not _has_assist_ledger_artifact(
        summary=summary,
        manifest=manifest,
        assist_ledger_path=assist_ledger_path,
    ):
        reasons.append("assist_ledger_missing_or_unknown")
    elif assist_confidence not in {"explicit", "inferred"}:
        reasons.append("assist_ledger_missing_or_unknown")
    if active_assists:
        reasons.append("active_assists_present")

    lateral_guard_apply_count = _num(run.get("lateral_guard_apply_count"))
    if lateral_guard_apply_count is None:
        reasons.append("lateral_guard_apply_count_missing")
    elif lateral_guard_apply_count != 0:
        reasons.append("lateral_guard_apply_count_nonzero")

    trajectory_guard_apply_count = _num(run.get("trajectory_contract_guard_apply_count"))
    if trajectory_guard_apply_count is None:
        reasons.append("trajectory_contract_guard_apply_count_missing")
    elif trajectory_guard_apply_count != 0:
        reasons.append("trajectory_contract_guard_apply_count_nonzero")

    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and _force_green_active(summary, manifest, run):
        reasons.append("force_green_traffic_light_active")

    return {
        "can_claim_unassisted": not reasons,
        "why_not_claimable": _unique_ordered(reasons),
    }


def _localization_claim_grade(report: Mapping[str, Any]) -> bool:
    if _localization_contract_status(report) not in {"pass", "warn"}:
        return False
    if report.get("claim_grade") is False:
        return False
    if _localization_blocking_reasons(report):
        return False
    reference = report.get("reference_point")
    if not isinstance(reference, Mapping):
        return False
    if reference.get("position_uses_vrp") is not True:
        return False
    if reference.get("vehicle_reference_hard_gate_eligible") is False:
        return False
    confidence = str(
        reference.get("confidence")
        or reference.get("configured_vehicle_reference_confidence")
        or ""
    ).strip()
    if confidence != "verified":
        return False
    checklist_missing = _localization_checklist_hard_gate_missing(report, scenario_class="lane_keep")
    return not checklist_missing


def _control_health_blocking(report: Mapping[str, Any]) -> bool:
    status = str(report.get("status") or "").strip()
    if status == "fail":
        return True
    if report.get("blocking_failure") is True:
        return True
    if report.get("blocking_failures"):
        return True
    if report.get("blocking_reasons"):
        return True
    return False


def _control_source(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
) -> str | None:
    control_channel = apollo_control_handoff.get("control_channel")
    if isinstance(control_channel, Mapping):
        value = _first_text(
            control_channel,
            "channel",
            control_channel,
            "name",
            control_channel,
            "topic",
        )
        if value:
            return value
        # Older handoff reports only recorded materialized /apollo/control
        # counts. Keep them usable, but require those counts to be positive.
        if (_num(control_channel.get("message_count")) or 0.0) > 0:
            return APOLLO_CONTROL_SOURCE
    claim_grade_source = _first_recursive_text(
        manifest,
        control_health,
        keys={"control_source", "source_control_channel", "control_channel_name"},
    )
    if claim_grade_source == APOLLO_CONTROL_SOURCE:
        return APOLLO_CONTROL_SOURCE
    source = _first_recursive_text(
        summary,
        manifest,
        control_health,
        keys={"control_source", "source_control_channel", "control_channel_name"},
    )
    if source:
        return source
    return None


def _routing_success_count(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
) -> float | None:
    value = _first_recursive_number(
        summary,
        manifest,
        apollo_control_handoff,
        keys={"routing_success_count", "routing_response_success_count"},
    )
    if value is not None:
        return value
    if _summary_bool(summary, "routing_materialized") is True:
        return 1.0
    return None


def _planning_nonempty_metric(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_reference_line_contract: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = apollo_reference_line_contract.get("evidence")
    claim_source = None
    filtered_ratio = None
    after_routing = None
    after_first_nonempty = None
    if isinstance(evidence, Mapping):
        filtered_ratio = _num(evidence.get("planning_claim_window_nonempty_trajectory_ratio"))
        claim_source = str(evidence.get("planning_claim_window_source") or "").strip() or None
        after_routing = _num(evidence.get("nonempty_trajectory_ratio_after_routing_segment_available"))
        after_first_nonempty = _num(evidence.get("nonempty_trajectory_ratio_after_first_nonempty"))
    summary_ratio = _first_recursive_number(
        summary,
        manifest,
        apollo_control_handoff,
        keys={"planning_nonempty_ratio", "planning_non_empty_ratio", "nonempty_trajectory_ratio"},
    )
    planning_metrics = (
        planning_materialization.get("metrics")
        if isinstance(planning_materialization.get("metrics"), Mapping)
        else {}
    )
    planning_materialization_ratio = _first_num(
        planning_materialization.get("nonempty_trajectory_ratio"),
        planning_metrics.get("nonempty_trajectory_ratio"),
    )
    reference_overall = None
    evidence = apollo_reference_line_contract.get("evidence")
    if isinstance(evidence, Mapping):
        reference_overall = _num(evidence.get("nonempty_trajectory_ratio"))
    overall_ratio = _first_num(planning_materialization_ratio, summary_ratio, reference_overall)
    ratio = overall_ratio
    if planning_materialization_ratio is not None:
        source = "planning_materialization.overall"
    elif summary_ratio is not None:
        source = "summary_or_control_handoff"
    elif reference_overall is not None:
        source = "apollo_reference_line_contract.overall"
    else:
        source = None
    route_establishment = (
        planning_materialization.get("route_establishment")
        if isinstance(planning_materialization.get("route_establishment"), Mapping)
        else {}
    )
    return {
        "ratio": ratio,
        "source": source,
        "overall_ratio": overall_ratio,
        "filtered_ratio": filtered_ratio,
        "after_routing_segment_available": after_routing,
        "after_first_nonempty": after_first_nonempty,
        "planning_materialization_nonempty_ratio": planning_materialization_ratio,
        "route_established": route_establishment.get("route_established"),
        "route_establishment_source": "planning_materialization" if planning_materialization else None,
    }


def _planning_nonempty_ratio(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_reference_line_contract: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
) -> float | None:
    return _planning_nonempty_metric(
        summary,
        manifest,
        apollo_reference_line_contract,
        apollo_control_handoff,
        {},
    )["ratio"]


def _control_rx_count(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
) -> float | None:
    return _first_recursive_number(
        apollo_control_handoff,
        control_health,
        summary,
        manifest,
        keys={"control_rx_count", "final_rx_count"},
    )


def _control_tx_count(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
) -> float | None:
    value = _first_recursive_number(
        apollo_control_handoff,
        control_health,
        summary,
        manifest,
        keys={"control_tx_count", "final_tx_count", "control_publish_count"},
    )
    if value is not None:
        return value
    control_channel = apollo_control_handoff.get("control_channel")
    if isinstance(control_channel, Mapping):
        return _num(control_channel.get("message_count"))
    return None


def _control_apply_count(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
) -> float | None:
    return _first_recursive_number(
        apollo_control_handoff,
        control_health,
        summary,
        manifest,
        keys={"control_apply_count", "apply_control_count", "final_applied_count"},
    )


def _guard_apply_count(
    flat_key: str,
    route_health_guard_key: str,
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    route_health: Mapping[str, Any],
    control_health: Mapping[str, Any],
) -> float | None:
    value = _first_recursive_number(summary, manifest, control_health, keys={flat_key})
    if value is not None:
        return value
    control_semantics = route_health.get("control_semantics")
    if isinstance(control_semantics, Mapping):
        guard_counts = control_semantics.get("guard_apply_counts")
        if isinstance(guard_counts, Mapping):
            return _num(guard_counts.get(route_health_guard_key))
    return None


def _force_green_active(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    run: Mapping[str, Any],
) -> bool:
    for payload in (summary, manifest, run):
        for value in _recursive_values(payload, {"force_green", "force_green_enabled"}):
            if _parse_bool(value) is True:
                return True
        for value in _recursive_values(
            payload,
            {"traffic_light_policy", "stimulus_mode", "mode"},
        ):
            if str(value).strip().lower() == "force_green":
                return True
    return False


def _has_assist_ledger_artifact(
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    assist_ledger_path: Path | None,
) -> bool:
    if assist_ledger_path is not None:
        return True
    return isinstance(summary.get("assist_ledger"), Mapping) or isinstance(manifest.get("assist_ledger"), Mapping)


def _first_recursive_text(*payloads: Mapping[str, Any], keys: set[str]) -> str | None:
    for payload in payloads:
        for value in _recursive_values(payload, keys):
            if value not in {None, ""}:
                return str(value)
    return None


def _first_recursive_number(*payloads: Mapping[str, Any], keys: set[str]) -> float | None:
    for payload in payloads:
        for value in _recursive_values(payload, keys):
            number = _num(value)
            if number is not None:
                return number
    return None


def _recursive_values(payload: Any, keys: set[str]) -> list[Any]:
    found: list[Any] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if str(key) in keys:
                found.append(value)
            found.extend(_recursive_values(value, keys))
    elif isinstance(payload, list):
        for value in payload:
            found.extend(_recursive_values(value, keys))
    return found


def _run_verdict(
    run: Mapping[str, Any],
    *,
    summary: Mapping[str, Any],
    route_health: Mapping[str, Any],
    channel_health: Mapping[str, Any],
    localization_contract: Mapping[str, Any],
    apollo_reference_line_contract: Mapping[str, Any],
    apollo_hdmap_projection: Mapping[str, Any],
    apollo_control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
    traffic_light_contract: Mapping[str, Any],
    traffic_light_behavior: Mapping[str, Any],
    route_curve_artifact_gap: Mapping[str, Any],
    run_dir: Path,
    route_health_path: Path | None,
    channel_health_path: Path | None,
    localization_contract_path: Path | None,
    apollo_reference_line_contract_path: Path | None,
    apollo_hdmap_projection_path: Path | None,
    apollo_control_handoff_path: Path | None,
    control_health_path: Path | None,
    traffic_light_contract_path: Path | None,
    traffic_light_behavior_path: Path | None,
    route_curve_artifact_gap_path: Path | None,
    thresholds: Mapping[str, float],
) -> tuple[str, str | None, list[str]]:
    missing_fields: list[str] = []
    scenario_class = str(run.get("scenario_class") or "")
    missing_artifacts = list(run.get("missing_artifacts") or [])
    if missing_artifacts:
        return "insufficient_data", "missing_required_artifacts", missing_fields

    missing_manifest_fields = list(run.get("missing_manifest_fields") or [])
    traffic_control_manifest_reason = _traffic_light_control_manifest_failure_reason(missing_manifest_fields)
    if traffic_control_manifest_reason is not None:
        missing_fields.extend(str(field) for field in missing_manifest_fields if str(field).startswith("traffic_light_control"))
        return "insufficient_data", traffic_control_manifest_reason, missing_fields
    generic_missing_manifest_fields = [
        field
        for field in missing_manifest_fields
        if not str(field).startswith("traffic_light_expectation.")
        and not str(field).startswith("traffic_light_control")
    ]
    if generic_missing_manifest_fields:
        missing_fields.extend(f"manifest.{field}" for field in generic_missing_manifest_fields)
        return "insufficient_data", "missing_manifest_fields", missing_fields

    invalid_manifest_sources = list(run.get("invalid_manifest_source_fields") or [])
    if invalid_manifest_sources:
        missing_fields.extend(f"manifest.{field}" for field in invalid_manifest_sources)
        return "insufficient_data", "invalid_manifest_source_fields", missing_fields

    missing_control_trace = list(run.get("missing_control_trace_fields") or [])
    if missing_control_trace:
        missing_fields.extend(f"control_trace.{field}" for field in missing_control_trace)
        return "insufficient_data", "missing_control_trace_fields", missing_fields

    runtime_contract_status = str(run.get("runtime_contract_status") or "").strip()
    if runtime_contract_status != "aligned":
        missing_fields.append("runtime_contract.status")
        if runtime_contract_status:
            return "fail", "runtime_contract_not_aligned", missing_fields
        return "insufficient_data", "runtime_contract_missing_status", missing_fields

    link_missing: list[str] = []
    if run.get("routing_materialized") is None:
        link_missing.append("routing_materialized")
    if run.get("planning_materialized") is None:
        link_missing.append("planning_materialized")
    if not run.get("control_handoff_status"):
        link_missing.append("control_handoff_status")
    if link_missing:
        missing_fields.extend(f"link_health.{field}" for field in link_missing)
        return "insufficient_data", "missing_link_health_fields", missing_fields
    if run.get("routing_materialized") is False:
        return "fail", "routing_missing", missing_fields
    if run.get("planning_materialized") is False:
        return "fail", "planning_missing", missing_fields
    if run.get("control_handoff_status") != "control_consuming_with_nonzero_planning":
        return "fail", "control_handoff_not_consuming", missing_fields

    route_contract_status = str(run.get("apollo_route_contract_status") or "").strip()
    route_contract_blockers = {str(item) for item in (run.get("apollo_route_contract_blocking_reasons") or [])}
    if route_contract_status == "fail":
        return "fail", "apollo_route_contract_failed", missing_fields
    if "claim_route_not_materialized" in route_contract_blockers:
        return "fail", "claim_route_not_materialized", missing_fields
    planning_materialization_status = str(run.get("planning_materialization_status") or "").strip()
    if planning_materialization_status == "fail":
        return "fail", "planning_materialization_failed", missing_fields
    if run.get("route_established") is False:
        return "fail", "route_establishment_not_confirmed", missing_fields

    channel_status = channel_health.get("status")
    if channel_status == "fail":
        return "fail", "apollo_channel_health_failed", missing_fields
    if channel_status not in {"pass", "warn"}:
        return "insufficient_data", "apollo_channel_health_missing_status", missing_fields
    channel_evidence_status, channel_evidence_reason, channel_evidence_missing = _channel_health_evidence_verdict(
        channel_health,
        scenario_class=scenario_class,
        run_dir=run_dir,
        report_path=channel_health_path,
    )
    if channel_evidence_status != "pass":
        missing_fields.extend(channel_evidence_missing)
        return channel_evidence_status, channel_evidence_reason, missing_fields

    localization_status, localization_reason, localization_missing = _localization_contract_verdict(
        localization_contract,
        scenario_class=scenario_class,
        report_path=localization_contract_path,
    )
    if localization_status != "pass":
        missing_fields.extend(localization_missing)
        return localization_status, localization_reason, missing_fields

    reference_status, reference_reason, reference_missing = _apollo_reference_line_contract_verdict(
        apollo_reference_line_contract,
        scenario_class=scenario_class,
        report_path=apollo_reference_line_contract_path,
    )
    if reference_status != "pass":
        missing_fields.extend(reference_missing)
        return reference_status, reference_reason, missing_fields

    hdmap_status, hdmap_reason, hdmap_missing = _apollo_hdmap_projection_verdict(
        apollo_hdmap_projection,
        scenario_class=scenario_class,
        report_path=apollo_hdmap_projection_path,
    )
    if hdmap_status != "pass":
        missing_fields.extend(hdmap_missing)
        return hdmap_status, hdmap_reason, missing_fields

    handoff_status, handoff_reason, handoff_missing = _apollo_control_handoff_evidence_verdict(
        apollo_control_handoff,
        report_path=apollo_control_handoff_path,
    )
    if handoff_status != "pass":
        missing_fields.extend(handoff_missing)
        return handoff_status, handoff_reason, missing_fields

    control_status = control_health.get("status")
    if control_status == "fail":
        return "fail", str(control_health.get("failure_reason") or "control_health_failed"), missing_fields
    if control_status not in {"pass", "warn"}:
        return "insufficient_data", "control_health_missing_status", missing_fields
    control_evidence_status, control_evidence_reason, control_evidence_missing = _control_health_evidence_verdict(
        control_health,
        scenario_class=scenario_class,
        expected_route_id=str(run.get("route_id") or ""),
        run_dir=run_dir,
        report_path=control_health_path,
    )
    if control_evidence_status != "pass":
        missing_fields.extend(control_evidence_missing)
        return control_evidence_status, control_evidence_reason, missing_fields
    if str(run.get("backend") or "").lower() == "autoware":
        attribution_status = str(run.get("control_attribution_status") or "")
        if attribution_status == "fail":
            return "fail", "control_attribution_failed", missing_fields
        if attribution_status not in {"pass", "warn"}:
            missing_fields.append("control_attribution_report.json")
            return "insufficient_data", "control_attribution_missing_status", missing_fields

    route_health_status = _route_health_status(route_health)
    if route_health_status in {"fail", "failed"}:
        return "fail", "route_health_failed", missing_fields
    if route_health_status not in {"pass", "warn", "diagnostic_ready"}:
        return "insufficient_data", "route_health_missing_status", missing_fields
    route_evidence_status, route_evidence_reason, route_evidence_missing = _route_health_evidence_verdict(
        route_health,
        expected_route_id=str(run.get("route_id") or ""),
        scenario_class=scenario_class,
        run_dir=run_dir,
        report_path=route_health_path,
    )
    route_diagnostic_only = False
    if route_evidence_status != "pass":
        if scenario_class == "curve_diagnostic" and route_evidence_status == "diagnostic_only":
            route_diagnostic_only = True
            missing_fields.extend(route_evidence_missing)
        else:
            missing_fields.extend(route_evidence_missing)
            return route_evidence_status, route_evidence_reason, missing_fields

    invalid_route_start_diagnostics = _invalid_route_start_diagnostic_fields(run)
    if invalid_route_start_diagnostics:
        missing_fields.extend(invalid_route_start_diagnostics)
        return "insufficient_data", "invalid_route_start_diagnostic_artifacts", missing_fields

    if scenario_class == "curve_diagnostic":
        gap_status = route_curve_artifact_gap.get("status")
        if gap_status != "pass":
            missing_fields.extend(
                f"route_curve_artifact_gap.{field}"
                for field in (route_curve_artifact_gap.get("missing_p1_fields") or [])
            )
            if not missing_fields:
                missing_fields.append("route_curve_artifact_gap.status")
            if route_diagnostic_only:
                return "diagnostic_only", "curve_diagnostic_missing_p1_fields", missing_fields
            return "insufficient_data", "route_curve_artifact_gap_insufficient", missing_fields
        gap_evidence_status, gap_evidence_reason, gap_evidence_missing = _route_curve_artifact_gap_evidence_verdict(
            route_curve_artifact_gap,
            run_dir=run_dir,
            report_path=route_curve_artifact_gap_path,
        )
        if gap_evidence_status != "pass":
            missing_fields.extend(gap_evidence_missing)
            if route_diagnostic_only:
                return "diagnostic_only", str(gap_evidence_reason or "curve_diagnostic_gap_diagnostic_only"), missing_fields
            return gap_evidence_status, gap_evidence_reason, missing_fields
        curve_status, curve_reason, curve_missing = _curve_semantic_verdict(run, route_health)
        if curve_status != "pass":
            missing_fields.extend(curve_missing)
            if route_diagnostic_only and curve_status == "fail":
                return "diagnostic_only", "curve_diagnostic_anomaly_with_diagnostic_route_evidence", missing_fields
            return curve_status, curve_reason, missing_fields
        if route_diagnostic_only:
            return "diagnostic_only", str(route_evidence_reason or "route_health_diagnostic_only"), missing_fields

    traffic_status = traffic_light_contract.get("status") if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES else None
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_status == "fail":
        return "fail", "traffic_light_contract_failed", missing_fields
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_status not in {"pass", "warn"}:
        return "insufficient_data", "traffic_light_contract_missing_status", missing_fields
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        contract_status, contract_reason, contract_missing = _traffic_light_contract_evidence_verdict(
            traffic_light_contract,
            scenario_class=scenario_class,
            run_dir=run_dir,
            report_path=traffic_light_contract_path,
        )
        if contract_status != "pass":
            missing_fields.extend(contract_missing)
            return contract_status, contract_reason, missing_fields

    traffic_behavior_status = (
        traffic_light_behavior.get("status") if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES else None
    )
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        behavior_status, behavior_reason, behavior_missing = _traffic_light_behavior_evidence_verdict(
            traffic_light_behavior,
            scenario_class=scenario_class,
            expected_route_id=str(run.get("route_id") or ""),
            run_dir=run_dir,
            report_path=traffic_light_behavior_path,
        )
        if behavior_status != "pass":
            missing_fields.extend(behavior_missing)
            return behavior_status, behavior_reason, missing_fields
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_behavior_status == "fail":
        return "fail", str(traffic_light_behavior.get("failure_reason") or "traffic_light_behavior_failed"), missing_fields
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_behavior_status not in {"pass", "warn"}:
        return "insufficient_data", "traffic_light_behavior_missing_status", missing_fields
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        expected_behavior = EXPECTED_TRAFFIC_LIGHT_BEHAVIOR_BY_CLASS[scenario_class]
        observed_behavior = run.get("traffic_light_expected_behavior")
        expectation_source = run.get("traffic_light_expectation_source")
        if observed_behavior in {None, ""}:
            missing_fields.append("traffic_light_expectation.expected_behavior")
            return "insufficient_data", "traffic_light_expectation_missing", missing_fields
        if observed_behavior != expected_behavior:
            missing_fields.append("traffic_light_expectation.expected_behavior")
            return "fail", "traffic_light_expectation_mismatch", missing_fields
        if expectation_source != "manifest":
            missing_fields.append("traffic_light_expectation.expected_behavior")
            return "insufficient_data", "traffic_light_expectation_missing", missing_fields
        if run.get("traffic_light_claim_grade") is not True:
            missing_fields.append("traffic_light_expectation.stimulus_mode")
            return "insufficient_data", "traffic_light_stimulus_not_claim_grade", missing_fields
        control_status, control_reason, control_missing = _traffic_light_control_evidence_verdict(run)
        if control_status != "pass":
            missing_fields.extend(control_missing)
            return control_status, control_reason, missing_fields

    if _num(run.get("collision_count")) and _num(run.get("collision_count")) > 0:
        return "fail", "collision", missing_fields
    if _num(run.get("lane_invasion_count")) and _num(run.get("lane_invasion_count")) > 0:
        route_start_reason = str(run.get("route_start_alignment_reason") or "")
        if route_start_reason in {"failure_before_route_start", "failure_near_route_start"}:
            return "fail", "route_start_lane_invasion", missing_fields
        return "fail", "lane_invasion", missing_fields

    route_completion = _num(run.get("route_completion"))
    if scenario_class in ROUTE_COMPLETION_REQUIRED_CLASSES:
        if route_completion is None:
            missing_fields.append("route_completion")
            return "insufficient_data", "missing_route_completion", missing_fields
        if route_completion < thresholds["min_route_completion"]:
            return "fail", "route_completion_too_low", missing_fields

    lateral_p95 = _num(run.get("lateral_error_p95"))
    heading_p95 = _num(run.get("heading_error_p95"))
    if lateral_p95 is None:
        missing_fields.append("lateral_error_p95")
    elif lateral_p95 > thresholds["max_lateral_error_p95_m"]:
        return "fail", "lateral_error_too_high", missing_fields
    if heading_p95 is None:
        missing_fields.append("heading_error_p95")
    elif heading_p95 > thresholds["max_heading_error_p95_rad"]:
        return "fail", "heading_error_too_high", missing_fields

    if scenario_class == "traffic_light_red_stop":
        stopped_at_red = run.get("stopped_at_red")
        red_stop_distance = _num(run.get("red_stop_distance_m"))
        if stopped_at_red is False:
            return "fail", "red_light_not_stopped", missing_fields
        if stopped_at_red is None:
            missing_fields.append("stopped_at_red")
            return "insufficient_data", "missing_stopped_at_red", missing_fields
        if red_stop_distance is None:
            missing_fields.append("red_stop_distance_m")
            return "insufficient_data", "missing_red_stop_distance", missing_fields
        if red_stop_distance < 0:
            return "fail", "red_light_stop_line_violation", missing_fields
        if red_stop_distance > thresholds["max_red_stop_distance_m"]:
            return "warn", "red_stop_too_far_from_line", missing_fields
    elif scenario_class == "traffic_light_green_go":
        if _num(run.get("green_pass_time_s")) is None:
            missing_fields.append("green_pass_time_s")
            return "insufficient_data", "missing_green_pass_time", missing_fields
    elif scenario_class == "traffic_light_red_to_green_release":
        if _num(run.get("red_to_green_release_time_s")) is None:
            missing_fields.append("red_to_green_release_time_s")
            return "insufficient_data", "missing_red_to_green_release_time", missing_fields

    latency = _num(run.get("control_latency_p95_ms"))
    latency_missing = False
    if latency is None:
        latency_missing = True
    elif latency > thresholds["max_control_latency_p95_ms"]:
        return "warn", "control_latency_high", missing_fields

    if missing_fields:
        return "insufficient_data", "missing_required_metrics", missing_fields
    blocking_assists = [str(item) for item in (run.get("blocking_assists") or []) if item]
    why_not_claimable = [str(item) for item in (run.get("why_not_claimable") or []) if item]
    if why_not_claimable and _applies_apollo_unassisted_claim_gate(run):
        active_assists = [str(item) for item in (run.get("active_assists") or []) if item]
        if blocking_assists:
            missing_fields.extend(f"assist_ledger.blocking_assists.{assist}" for assist in blocking_assists)
            if _diagnostic_assisted_context(run):
                return "diagnostic_only", "assisted_diagnostic_only_unassisted_claim_blocked", missing_fields
            return "assisted_pass", "assisted_pass_unassisted_claim_blocked", missing_fields
        if active_assists:
            missing_fields.extend(f"assist_ledger.active_assists.{assist}" for assist in active_assists)
        missing_fields.extend(f"claimability.{reason}" for reason in why_not_claimable)
        return "insufficient_data", "unassisted_claim_not_supported", sorted(set(missing_fields))
    if blocking_assists:
        missing_fields.extend(f"assist_ledger.blocking_assists.{assist}" for assist in blocking_assists)
        return "assisted_pass", "assisted_pass_unassisted_claim_blocked", missing_fields
    if latency_missing:
        return "warn", "control_latency_missing", ["control_latency_p95_ms"]
    if (
        channel_status == "warn"
        or control_status == "warn"
        or route_health_status == "warn"
        or traffic_status == "warn"
        or traffic_behavior_status == "warn"
    ):
        return "warn", "upstream_artifact_warn", missing_fields
    return "pass", None, missing_fields


def _applies_apollo_unassisted_claim_gate(run: Mapping[str, Any]) -> bool:
    return str(run.get("backend") or "").strip().lower() != "autoware"


def _diagnostic_assisted_context(run: Mapping[str, Any]) -> bool:
    scenario_id = str(run.get("scenario_id") or "").lower()
    scenario_class = str(run.get("scenario_class") or "").lower()
    gate_role = str(run.get("gate_role") or "").lower()
    if gate_role in {"diagnostic_gate", "informational"}:
        return True
    return any(token in scenario_id or token in scenario_class for token in ("smoke", "debug"))


def _invalid_route_start_diagnostic_fields(run: Mapping[str, Any]) -> list[str]:
    prefixes = ("failure_timeline.", "route_start_alignment.")
    return [
        str(field)
        for field in (run.get("invalid_report_source_fields") or [])
        if any(str(field).startswith(prefix) for prefix in prefixes)
    ]


def _capability_coverage(
    run_results: Sequence[Mapping[str, Any]],
    *,
    planned_scenarios: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    by_class: dict[str, dict[str, Any]] = {}
    by_scenario_id: dict[str, dict[str, Any]] = {}
    for run in run_results:
        scenario_class = str(run.get("scenario_class") or "unknown")
        bucket = by_class.setdefault(
            scenario_class,
            {
                "total": 0,
                "pass": 0,
                "assisted_pass": 0,
                "diagnostic_only": 0,
                "warn": 0,
                "fail": 0,
                "insufficient_data": 0,
                "run_ids": [],
            },
        )
        bucket["total"] += 1
        bucket["run_ids"].append(run.get("run_id"))
        verdict = str(run.get("verdict") or "insufficient_data")
        if verdict in {"pass", "assisted_pass", "diagnostic_only", "warn", "fail", "insufficient_data"}:
            bucket[verdict] += 1
        scenario_id = str(run.get("scenario_id") or "").strip()
        if scenario_id:
            scenario_bucket = by_scenario_id.setdefault(
                scenario_id,
                {
                    "total": 0,
                    "pass": 0,
                    "assisted_pass": 0,
                    "diagnostic_only": 0,
                    "warn": 0,
                    "fail": 0,
                    "insufficient_data": 0,
                    "run_ids": [],
                    "route_ids": set(),
                    "scenario_classes": set(),
                },
            )
            scenario_bucket["total"] += 1
            scenario_bucket["run_ids"].append(run.get("run_id"))
            scenario_bucket["route_ids"].add(str(run.get("route_id") or ""))
            scenario_bucket["scenario_classes"].add(scenario_class)
            if verdict in {"pass", "assisted_pass", "diagnostic_only", "warn", "fail", "insufficient_data"}:
                scenario_bucket[verdict] += 1

    missing_classes = [name for name in TARGET_SCENARIO_CLASSES if name not in by_class]
    unproven_classes = [
        name
        for name in TARGET_SCENARIO_CLASSES
        if name in by_class and by_class[name].get("pass", 0) < 1
    ]
    scenario_identity = _scenario_identity_coverage(by_scenario_id, planned_scenarios or [])
    return {
        "required_scenario_classes": list(TARGET_SCENARIO_CLASSES),
        "by_scenario_class": by_class,
        "missing_required_scenario_classes": missing_classes,
        "unproven_required_scenario_classes": unproven_classes,
        **scenario_identity,
        "can_claim_full_natural_driving": bool(
            not missing_classes
            and not unproven_classes
            and not scenario_identity["missing_required_scenario_ids"]
            and not scenario_identity["unproven_required_scenario_ids"]
            and not scenario_identity["scenario_identity_mismatches"]
        ),
        "interpretation_boundary": (
            "Capability coverage is required for a full Town01 truth-input natural-driving claim. "
            "When a suite run_matrix.csv is available, coverage is scenario-id and route-id "
            "specific; class-only pass evidence is not enough. Subset reports can diagnose "
            "runs, but cannot prove the full objective."
        ),
    }


def _scenario_identity_coverage(
    by_scenario_id: Mapping[str, Mapping[str, Any]],
    planned_scenarios: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    required: list[dict[str, str]] = []
    seen_required_ids: set[str] = set()
    for item in planned_scenarios:
        scenario_id = str(item.get("scenario_id") or "").strip()
        if not scenario_id or scenario_id in seen_required_ids:
            continue
        seen_required_ids.add(scenario_id)
        required.append(
            {
                "scenario_id": scenario_id,
                "route_id": str(item.get("route_id") or "").strip(),
                "scenario_class": str(item.get("scenario_class") or "").strip(),
            }
        )

    missing: list[str] = []
    unproven: list[str] = []
    mismatches: list[dict[str, Any]] = []
    for expected in required:
        scenario_id = expected["scenario_id"]
        observed = by_scenario_id.get(scenario_id)
        if not observed:
            missing.append(scenario_id)
            continue
        observed_routes = sorted(
            str(route_id)
            for route_id in observed.get("route_ids", set())
            if route_id not in {None, ""}
        )
        observed_classes = sorted(
            str(scenario_class)
            for scenario_class in observed.get("scenario_classes", set())
            if scenario_class not in {None, ""}
        )
        expected_route = expected["route_id"]
        expected_class = expected["scenario_class"]
        route_ok = not expected_route or expected_route in observed_routes
        class_ok = not expected_class or expected_class in observed_classes
        if not route_ok or not class_ok:
            mismatches.append(
                {
                    "scenario_id": scenario_id,
                    "expected_route_id": expected_route,
                    "observed_route_ids": observed_routes,
                    "expected_scenario_class": expected_class,
                    "observed_scenario_classes": observed_classes,
                }
            )
            continue
        if int(observed.get("pass", 0) or 0) < 1:
            unproven.append(scenario_id)

    return {
        "required_scenarios": required,
        "required_scenario_ids": [item["scenario_id"] for item in required],
        "observed_scenario_ids": sorted(by_scenario_id),
        "missing_required_scenario_ids": missing,
        "unproven_required_scenario_ids": unproven,
        "scenario_identity_mismatches": mismatches,
    }


def _suite_verdict(
    run_results: Sequence[Mapping[str, Any]],
    *,
    capability_coverage: Mapping[str, Any],
    suite_plan_scope: Mapping[str, Any] | None = None,
    require_full_target_coverage: bool,
) -> dict[str, Any]:
    statuses = [str(run.get("verdict")) for run in run_results]
    if not statuses:
        status = "insufficient_data"
    elif "fail" in statuses:
        status = "fail"
    elif "insufficient_data" in statuses:
        status = "insufficient_data"
    elif "diagnostic_only" in statuses:
        status = "insufficient_data"
    elif "assisted_pass" in statuses:
        status = "warn"
    elif "warn" in statuses:
        status = "warn"
    else:
        status = "pass"
    missing_classes = list(capability_coverage.get("missing_required_scenario_classes") or [])
    unproven_classes = list(capability_coverage.get("unproven_required_scenario_classes") or [])
    missing_scenario_ids = list(capability_coverage.get("missing_required_scenario_ids") or [])
    unproven_scenario_ids = list(capability_coverage.get("unproven_required_scenario_ids") or [])
    scenario_identity_mismatches = list(capability_coverage.get("scenario_identity_mismatches") or [])
    scope = suite_plan_scope if isinstance(suite_plan_scope, Mapping) else {}
    suite_plan_missing = _missing_suite_plan_fields(scope) if require_full_target_coverage else []
    filtered_suite_plan = bool(scope.get("filtered"))
    if require_full_target_coverage and status in {"pass", "warn"} and (
        suite_plan_missing
        or missing_classes
        or unproven_classes
        or missing_scenario_ids
        or unproven_scenario_ids
        or scenario_identity_mismatches
        or filtered_suite_plan
    ):
        status = "insufficient_data"
    return {
        "status": status,
        "failed_runs": [run.get("run_id") for run in run_results if run.get("verdict") == "fail"],
        "insufficient_data_runs": [
            run.get("run_id") for run in run_results if run.get("verdict") == "insufficient_data"
        ],
        "diagnostic_only_runs": [
            run.get("run_id") for run in run_results if run.get("verdict") == "diagnostic_only"
        ],
        "assisted_pass_runs": [
            run.get("run_id") for run in run_results if run.get("verdict") == "assisted_pass"
        ],
        "warning_runs": [run.get("run_id") for run in run_results if run.get("verdict") == "warn"],
        "require_full_target_coverage": require_full_target_coverage,
        "can_claim_full_natural_driving": bool(
            status == "pass"
            and capability_coverage.get("can_claim_full_natural_driving")
            and not suite_plan_missing
            and not filtered_suite_plan
        ),
        "suite_plan_missing": suite_plan_missing,
        "suite_plan_has_manifest": scope.get("has_suite_manifest"),
        "suite_plan_has_run_matrix": scope.get("has_run_matrix"),
        "missing_required_scenario_classes": missing_classes,
        "unproven_required_scenario_classes": unproven_classes,
        "missing_required_scenario_ids": missing_scenario_ids,
        "unproven_required_scenario_ids": unproven_scenario_ids,
        "scenario_identity_mismatches": scenario_identity_mismatches,
        "filtered_suite_plan": filtered_suite_plan,
        "scenario_class_filter": list(scope.get("scenario_class_filter") or []),
        "scenario_id_filter": list(scope.get("scenario_id_filter") or []),
    }


def _has_suite_plan(root: Path) -> bool:
    return (root / "suite_manifest.json").exists() or (root / "run_matrix.csv").exists()


def _missing_suite_plan_fields(scope: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    if scope.get("has_suite_manifest") is not True:
        missing.append("suite_manifest.json")
    if scope.get("has_run_matrix") is not True:
        missing.append("run_matrix.csv")
    return missing


def _suite_plan_scope(root: Path) -> dict[str, Any]:
    manifest = _read_json(root / "suite_manifest.json")
    class_filter = _text_list(manifest.get("scenario_class_filter"))
    scenario_filter = _text_list(manifest.get("scenario_id_filter"))
    return {
        "has_suite_manifest": bool(manifest),
        "has_run_matrix": (root / "run_matrix.csv").exists(),
        "scenario_class_filter": class_filter,
        "scenario_id_filter": scenario_filter,
        "filtered": bool(class_filter or scenario_filter),
        "interpretation_boundary": (
            "Filtered suite plans are targeted probes. They can diagnose selected scenarios, "
            "but cannot support a full Town01 natural-driving claim."
        ),
    }


def _text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {None, ""}]
    if value in {None, ""}:
        return []
    return [str(value)]


def _missing_artifacts(
    *,
    summary_path: Path,
    manifest_path: Path,
    config_resolved_path: Path | None,
    events_path: Path | None,
    timeseries_path: Path | None,
    route_health_path: Path | None,
    route_health_csv_path: Path | None,
    curve_segments_path: Path | None,
    route_health_summary_path: Path | None,
    channel_health_path: Path | None,
    localization_contract_path: Path | None,
    apollo_reference_line_contract_path: Path | None,
    control_health_path: Path | None,
    traffic_light_contract_path: Path | None,
    scenario_class: str,
) -> list[str]:
    missing: list[str] = []
    if not manifest_path.exists():
        missing.append("manifest.json")
    if not summary_path.exists():
        missing.append("summary.json")
    if config_resolved_path is None:
        missing.append("config.resolved.yaml")
    if events_path is None:
        missing.append("events.jsonl")
    if timeseries_path is None:
        missing.append("timeseries.csv")
    if route_health_path is None:
        missing.append("route_health.json")
    if route_health_csv_path is None:
        missing.append("route_health.csv")
    if curve_segments_path is None:
        missing.append("curve_segments.csv")
    if route_health_summary_path is None:
        missing.append("route_health_summary.md")
    if channel_health_path is None:
        missing.append("apollo_channel_health_report.json")
    if localization_contract_path is None:
        missing.append("localization_contract_report.json")
    if apollo_reference_line_contract_path is None:
        missing.append("apollo_reference_line_contract_report.json")
    if control_health_path is None:
        missing.append("control_health_report.json")
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_light_contract_path is None:
        missing.append("traffic_light_contract_report.json")
    return missing


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_csv_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
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


def _summary_metrics(summary: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = summary.get("metrics")
    if not isinstance(metrics, Mapping):
        return summary
    combined = dict(summary)
    combined.update(metrics)
    return combined


def _metric(
    metrics: Mapping[str, Any],
    *names: str,
    rows: Sequence[Mapping[str, Any]] | None = None,
    row_field: str | None = None,
    percentile: float | None = None,
    default: float | None = None,
) -> float | None:
    for name in names:
        value = _num(metrics.get(name))
        if value is not None:
            return value
    fieldnames = [row_field] if row_field else list(names)
    values = _row_values(rows or [], *fieldnames)
    if values:
        if percentile is not None:
            return _percentile(values, percentile)
        return max(values)
    return default


def _route_health_metric(
    route_health: Mapping[str, Any],
    *names: str,
    rows: Sequence[Mapping[str, Any]],
    row_field: str,
    absolute: bool,
) -> float | None:
    run_metrics = route_health.get("run_metrics")
    if isinstance(run_metrics, Mapping):
        for name in names:
            value = _num(run_metrics.get(name))
            if value is not None:
                return value
    return _percentile(_row_values(rows, row_field, absolute=absolute), 0.95)


def _row_values(
    rows: Sequence[Mapping[str, Any]],
    *fields: str,
    absolute: bool = False,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        for field in fields:
            value = _num(row.get(field))
            if value is not None:
                values.append(abs(value) if absolute else value)
                break
    return values


def _route_health_status(route_health: Mapping[str, Any]) -> str | None:
    verdict = route_health.get("verdict")
    if isinstance(verdict, Mapping) and verdict.get("status"):
        return str(verdict.get("status"))
    if route_health.get("status"):
        return str(route_health.get("status"))
    return None


def _failure_timeline_primary_failure(failure_timeline: Mapping[str, Any]) -> str | None:
    primary = failure_timeline.get("primary_failure")
    if isinstance(primary, Mapping) and primary.get("failure_reason") not in {None, ""}:
        return str(primary.get("failure_reason"))
    return None


def _failure_timeline_anchor_event(failure_timeline: Mapping[str, Any]) -> str | None:
    anchor = failure_timeline.get("anchor_event")
    if isinstance(anchor, Mapping) and anchor.get("event_type") not in {None, ""}:
        return str(anchor.get("event_type"))
    return None


def _failure_timeline_ordering_findings(failure_timeline: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in (failure_timeline.get("ordering_findings") or []) if item not in {None, ""}]


def _failure_route_s(failure_timeline: Mapping[str, Any]) -> float | None:
    start_gate = failure_timeline.get("route_start_gate")
    if isinstance(start_gate, Mapping):
        value = _num(start_gate.get("route_s_at_anchor"))
        if value is not None:
            return value
    anchor = failure_timeline.get("anchor_event")
    context = anchor.get("row_context") if isinstance(anchor, Mapping) else None
    if isinstance(context, Mapping):
        return _num(context.get("route_s"))
    return None


def _failure_start_gate_bool(failure_timeline: Mapping[str, Any], field: str) -> bool | None:
    if not _failure_start_gate_actionable(failure_timeline):
        return None
    start_gate = failure_timeline.get("route_start_gate")
    if isinstance(start_gate, Mapping):
        value = start_gate.get(field)
        if isinstance(value, bool):
            return value
    return None


_NON_ACTIONABLE_ROUTE_START_ANCHOR_TYPES = {
    "first_high_steer",
    "matched_point_anomaly",
    "target_point_anomaly",
}


def _failure_start_gate_actionable(failure_timeline: Mapping[str, Any]) -> bool:
    ordering_findings = [
        str(item)
        for item in (failure_timeline.get("ordering_findings") or [])
        if item not in {None, ""}
    ]
    if ordering_findings:
        return True
    anchor = failure_timeline.get("anchor_event")
    event_type = (
        str(anchor.get("event_type") or "")
        if isinstance(anchor, Mapping)
        else ""
    )
    return event_type not in _NON_ACTIONABLE_ROUTE_START_ANCHOR_TYPES


def _initial_rear_axle_offset_compatible(route_start_alignment: Mapping[str, Any]) -> bool | None:
    initial = route_start_alignment.get("initial_ego_alignment")
    if isinstance(initial, Mapping):
        value = initial.get("rear_axle_offset_compatible")
        if isinstance(value, bool):
            return value
    return None


def _report_status(report: Mapping[str, Any]) -> str | None:
    status = report.get("status")
    if _scalar_status_value(status):
        return str(status)
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping):
        value = verdict.get("status")
        if _scalar_status_value(value):
            return str(value)
    return None


def _scalar_status_value(value: Any) -> bool:
    return isinstance(value, str | int | float | bool) and value not in {None, ""}


def _control_attribution_status(control_attribution: Mapping[str, Any]) -> str:
    status = _report_status(control_attribution)
    return status if status is not None else "insufficient_data"


def _control_attribution_dominant_breakpoint(control_attribution: Mapping[str, Any]) -> str | None:
    attribution = control_attribution.get("attribution")
    if isinstance(attribution, Mapping):
        value = attribution.get("dominant_breakpoint")
        if value not in {None, ""}:
            return str(value)
    value = control_attribution.get("dominant_breakpoint")
    return None if value in {None, ""} else str(value)


def _localization_contract_status(localization_contract: Mapping[str, Any]) -> str:
    status = _report_status(localization_contract)
    return status if status is not None else "insufficient_data"


def _chassis_gt_contract_status(report: Mapping[str, Any]) -> str:
    status = _report_status(report)
    return status if status is not None else "insufficient_data"


def _chassis_gt_contract_claim_grade(report: Mapping[str, Any]) -> bool:
    if not isinstance(report, Mapping) or not report:
        return False
    if _chassis_gt_contract_status(report) != "pass":
        return False
    if _chassis_gt_contract_blocking_reasons(report):
        return False
    return report.get("claim_grade") is True


def _chassis_gt_contract_blocking_reasons(report: Mapping[str, Any]) -> list[str]:
    if not isinstance(report, Mapping) or not report:
        return ["chassis_gt_contract_missing"]
    return [str(item) for item in (report.get("blocking_reasons") or []) if item]


def _apollo_reference_line_contract_status(report: Mapping[str, Any]) -> str:
    status = _report_status(report)
    return status if status is not None else "insufficient_data"


def _apollo_hdmap_projection_status(report: Mapping[str, Any]) -> str:
    if not report:
        return "insufficient_data"
    status = _report_status(report)
    if status != "insufficient_data":
        return status
    projection = report.get("projection")
    return _report_status(projection) if isinstance(projection, Mapping) else status


def _apollo_hdmap_projection_claim_grade(report: Mapping[str, Any]) -> bool:
    if not isinstance(report, Mapping) or not report:
        return False
    if _apollo_hdmap_projection_status(report) != "pass":
        return False
    if report.get("claim_grade") is True:
        return True
    projection = report.get("projection")
    return bool(isinstance(projection, Mapping) and projection.get("claim_grade") is True)


def _apollo_reference_line_blocking_reasons(report: Mapping[str, Any]) -> list[str]:
    reasons = [str(item) for item in (report.get("blocking_reasons") or []) if item]
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping):
        reasons.extend(str(item) for item in (verdict.get("blocking_reasons") or []) if item)
    return sorted(set(reasons))


def _localization_blocking_reasons(localization_contract: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    verdict = localization_contract.get("verdict")
    if isinstance(verdict, Mapping):
        reasons.extend(str(item) for item in (verdict.get("blocking_reasons") or []) if item)
    reference = localization_contract.get("reference_point")
    if isinstance(reference, Mapping):
        if reference.get("position_uses_vrp") is False:
            reasons.append("position_uses_vrp_false")
    frame_transform = localization_contract.get("frame_transform")
    if isinstance(frame_transform, Mapping):
        if frame_transform.get("uses_configured_transform") is False:
            reasons.append("frame_transform_missing")
        if frame_transform.get("y_flip_or_axis_mapping_declared") is False:
            reasons.append("axis_mapping_missing")
    pose = localization_contract.get("pose_consistency")
    if isinstance(pose, Mapping):
        lane_error = _num(pose.get("heading_error_to_lane_p95_rad"))
        if lane_error is not None and lane_error >= DEFAULT_THRESHOLDS["max_heading_error_p95_rad"]:
            reasons.append("heading_error_to_lane_high")
    channel = localization_contract.get("channel")
    if isinstance(channel, Mapping):
        if channel.get("timestamp_monotonic") is False or channel.get("timestamp_non_decreasing") is False:
            reasons.append("timestamp_non_monotonic")
        if channel.get("sequence_monotonic") is False:
            reasons.append("sequence_non_monotonic")
    return sorted(set(reasons))


REFERENCE_LINE_REQUIRED_SCENARIO_CLASSES = {
    "lane_keep",
    "curve_diagnostic",
    "junction_turn",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}


def _apollo_reference_line_contract_verdict(
    report: Mapping[str, Any],
    *,
    scenario_class: str,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    if scenario_class not in REFERENCE_LINE_REQUIRED_SCENARIO_CLASSES:
        return "pass", None, []
    if report_path is None or not report:
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_reference_line_contract_missing_for_curve_diagnostic",
                ["apollo_reference_line_contract_report.json"],
            )
        return (
            "insufficient_data",
            "apollo_reference_line_contract_missing",
            ["apollo_reference_line_contract_report.json"],
        )
    raw_status = _report_status(report)
    status = raw_status if raw_status is not None else "insufficient_data"
    blocking = _apollo_reference_line_blocking_reasons(report)
    if status == "fail" or blocking:
        fields = [f"apollo_reference_line_contract.blocking_reasons.{reason}" for reason in blocking]
        if not fields:
            fields.append("apollo_reference_line_contract.status")
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_reference_line_contract_blocking_curve_diagnostic",
                fields,
            )
        return "insufficient_data", "apollo_reference_line_contract_blocking", fields
    if status not in {"pass", "warn"}:
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_reference_line_contract_insufficient_curve_diagnostic",
                ["apollo_reference_line_contract.status"],
            )
        if raw_status is None:
            return (
                "insufficient_data",
                "apollo_reference_line_contract_missing_status",
                ["apollo_reference_line_contract.status"],
            )
        return (
            "insufficient_data",
            "apollo_reference_line_contract_insufficient",
            ["apollo_reference_line_contract.status"],
        )
    evidence = report.get("evidence")
    if not isinstance(evidence, Mapping):
        return (
            "insufficient_data",
            "apollo_reference_line_contract_missing_evidence",
            ["apollo_reference_line_contract.evidence"],
        )
    required = {
        "planning_reference_available": evidence.get("planning_reference_available") is True,
        "control_reference_available": evidence.get("control_reference_available") is True,
    }
    missing = [
        f"apollo_reference_line_contract.evidence.{field}"
        for field, present in required.items()
        if not present
    ]
    if missing:
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_reference_line_contract_diagnostic_only",
                missing,
            )
        return "insufficient_data", "apollo_reference_line_contract_incomplete", missing
    return "pass", None, []


def _apollo_hdmap_projection_verdict(
    report: Mapping[str, Any],
    *,
    scenario_class: str,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    if scenario_class not in REFERENCE_LINE_REQUIRED_SCENARIO_CLASSES:
        return "pass", None, []
    if report_path is None or not report:
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_hdmap_projection_missing_for_curve_diagnostic",
                ["apollo_hdmap_projection_report.json"],
            )
        return (
            "insufficient_data",
            "apollo_hdmap_projection_missing",
            ["apollo_hdmap_projection_report.json"],
        )
    status = _apollo_hdmap_projection_status(report)
    if status == "fail":
        return "fail", "apollo_hdmap_projection_failed", ["apollo_hdmap_projection.status"]
    if status not in PASS_WARN_STATUSES:
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_hdmap_projection_insufficient_curve_diagnostic",
                ["apollo_hdmap_projection.status"],
            )
        return (
            "insufficient_data",
            "apollo_hdmap_projection_insufficient",
            ["apollo_hdmap_projection.status"],
        )
    if not _apollo_hdmap_projection_claim_grade(report):
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "apollo_hdmap_projection_not_claim_grade_curve_diagnostic",
                ["apollo_hdmap_projection.claim_grade"],
            )
        return (
            "insufficient_data",
            "apollo_hdmap_projection_not_claim_grade",
            ["apollo_hdmap_projection.claim_grade"],
        )
    return "pass", None, []


def _initial_rear_axle_offset_error(route_start_alignment: Mapping[str, Any]) -> float | None:
    initial = route_start_alignment.get("initial_ego_alignment")
    if isinstance(initial, Mapping):
        return _num(initial.get("rear_axle_offset_error_m"))
    return None


def _static_spawn_lateral_offset(route_start_alignment: Mapping[str, Any]) -> float | None:
    static = route_start_alignment.get("static_spawn_alignment")
    if isinstance(static, Mapping):
        return _num(static.get("spawn_lateral_offset_m"))
    return None


def _recommended_ego_offset_y_delta(route_start_alignment: Mapping[str, Any]) -> float | None:
    recommendation = route_start_alignment.get("recommendation")
    if isinstance(recommendation, Mapping):
        return _num(recommendation.get("recommended_ego_offset_y_delta_m"))
    return None


def _channel_health_evidence_verdict(
    channel_health: Mapping[str, Any],
    *,
    scenario_class: str,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    missing_inputs = [str(item) for item in (channel_health.get("missing_inputs") or []) if item]
    if missing_inputs:
        return (
            "insufficient_data",
            "apollo_channel_health_missing_inputs",
            [f"apollo_channel_health.missing_inputs.{item}" for item in missing_inputs],
        )
    source = channel_health.get("source")
    if not isinstance(source, Mapping):
        return (
            "insufficient_data",
            "apollo_channel_health_missing_source_evidence",
            ["apollo_channel_health.source"],
        )
    reported_scenario_class = str(channel_health.get("scenario_class") or "").strip()
    if not reported_scenario_class:
        return (
            "insufficient_data",
            "apollo_channel_health_missing_scenario_context",
            ["apollo_channel_health.scenario_class"],
        )
    if reported_scenario_class != scenario_class:
        return (
            "insufficient_data",
            "apollo_channel_health_scenario_mismatch",
            ["apollo_channel_health.scenario_class"],
        )
    required_source_fields = ("config_path", "stats_path")
    missing_source = [
        f"apollo_channel_health.source.{field}"
        for field in required_source_fields
        if source.get(field) in {None, ""}
    ]
    if missing_source:
        return "insufficient_data", "apollo_channel_health_missing_source_evidence", missing_source
    unresolved_source = _missing_channel_health_source_paths(
        source,
        run_dir=run_dir,
        report_path=report_path,
    )
    if unresolved_source:
        return "insufficient_data", "apollo_channel_health_missing_source_artifacts", unresolved_source

    channel_results = channel_health.get("channel_results")
    if not isinstance(channel_results, Mapping):
        return (
            "insufficient_data",
            "apollo_channel_health_missing_channel_results",
            ["apollo_channel_health.channel_results"],
        )
    required = list(BASE_REQUIRED_CHANNEL_RESULTS)
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        required.append("traffic_light")
    missing: list[str] = []
    failed: list[str] = []
    for name in required:
        result = channel_results.get(name)
        if not isinstance(result, Mapping):
            missing.append(f"apollo_channel_health.channel_results.{name}")
            continue
        status = str(result.get("status") or "")
        if status == "fail":
            failed.append(name)
        elif status not in {"pass", "warn"}:
            missing.append(f"apollo_channel_health.channel_results.{name}.status")
        if name == "traffic_light" and result.get("required") is not True:
            missing.append("apollo_channel_health.channel_results.traffic_light.required")
    if failed:
        return "fail", "apollo_channel_health_failed", [
            f"apollo_channel_health.channel_results.{name}.status" for name in failed
        ]
    if missing:
        return "insufficient_data", "apollo_channel_health_missing_channel_evidence", missing
    return "pass", None, []


def _localization_contract_verdict(
    localization_contract: Mapping[str, Any],
    *,
    scenario_class: str,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    if report_path is None or not localization_contract:
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "localization_contract_missing_for_curve_diagnostic",
                ["localization_contract_report.json"],
            )
        return (
            "insufficient_data",
            "localization_contract_missing",
            ["localization_contract_report.json"],
        )

    status = _localization_contract_status(localization_contract)
    blocking_reasons = _localization_blocking_reasons(localization_contract)
    missing_fields = [
        f"localization_contract.blocking_reasons.{reason}"
        for reason in blocking_reasons
    ]
    if status == "fail" or blocking_reasons:
        if not missing_fields:
            missing_fields.append("localization_contract.verdict.status")
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "localization_contract_blocking_curve_diagnostic",
                missing_fields,
            )
        return "insufficient_data", _localization_contract_failure_reason(localization_contract), missing_fields
    if status not in {"pass", "warn"}:
        return (
            "insufficient_data",
            "localization_contract_missing_status",
            ["localization_contract.verdict.status"],
        )
    checklist_missing = _localization_checklist_hard_gate_missing(
        localization_contract,
        scenario_class=scenario_class,
    )
    if checklist_missing:
        missing_fields.extend(checklist_missing)
        if scenario_class == "curve_diagnostic":
            return (
                "diagnostic_only",
                "localization_contract_not_claim_grade_for_curve_diagnostic",
                missing_fields,
            )
        return (
            "insufficient_data",
            "localization_contract_not_claim_grade",
            missing_fields,
        )
    return "pass", None, []


def _localization_contract_failure_reason(localization_contract: Mapping[str, Any]) -> str:
    blocking_reasons = set(_localization_blocking_reasons(localization_contract))
    lateral_consistency = localization_contract.get("hdmap_route_lateral_consistency")
    lateral_consistency = lateral_consistency if isinstance(lateral_consistency, Mapping) else {}
    if (
        "apollo_hdmap_projection_lateral_error_high" in blocking_reasons
        and lateral_consistency.get("status") == "pass"
        and lateral_consistency.get("interpretation")
        == "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"
    ):
        return "actual_lateral_drift_matches_hdmap_projection"
    return "localization_contract_blocking"


def _localization_checklist_hard_gate_missing(
    localization_contract: Mapping[str, Any],
    *,
    scenario_class: str,
) -> list[str]:
    checklist = localization_contract.get("acceptance_checklist")
    if not isinstance(checklist, Mapping):
        return ["localization_contract.acceptance_checklist"]
    missing: list[str] = []
    required_checks = dict(LOCALIZATION_HARD_GATE_CHECKS)
    # Apollo planning/control reference-line closure is checked by
    # apollo_reference_line_contract_report.json. Do not let bridge nearest-lane
    # diagnostics satisfy that claim via the localization checklist.
    for check_id, allowed_statuses in required_checks.items():
        item = checklist.get(check_id)
        if not isinstance(item, Mapping):
            missing.append(f"localization_contract.acceptance_checklist.{check_id}")
            continue
        status = str(item.get("status") or "")
        if status not in allowed_statuses:
            missing.append(f"localization_contract.acceptance_checklist.{check_id}.status")
    return missing


def _missing_channel_health_source_paths(
    source: Mapping[str, Any],
    *,
    run_dir: Path,
    report_path: Path | None,
) -> list[str]:
    missing: list[str] = []
    for field in ("config_path", "stats_path"):
        raw = source.get(field)
        if raw in {None, ""}:
            continue
        if not _source_path_exists(str(raw), run_dir=run_dir, report_path=report_path):
            missing.append(f"apollo_channel_health.source.{field}")
    return missing


def _source_path_exists(raw: str, *, run_dir: Path, report_path: Path | None) -> bool:
    path = Path(raw).expanduser()
    candidates = [path]
    if not path.is_absolute():
        candidates.extend(
            [
                run_dir / path,
                Path.cwd() / path,
            ]
        )
        if report_path is not None:
            candidates.append(report_path.parent / path)
    return any(candidate.exists() for candidate in candidates)


def _traffic_light_control_manifest_failure_reason(fields: Sequence[Any]) -> str | None:
    field_set = {str(field) for field in fields}
    if "traffic_light_control" in field_set:
        return "traffic_light_control_missing"
    if "traffic_light_control.release_frame_id" in field_set:
        return "traffic_light_control_release_not_observed"
    if any(field.startswith("traffic_light_control.") for field in field_set):
        return "traffic_light_control_missing"
    return None


def _traffic_light_contract_evidence_verdict(
    traffic_light_contract: Mapping[str, Any],
    *,
    scenario_class: str,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    missing_inputs = [str(item) for item in (traffic_light_contract.get("missing_inputs") or []) if item]
    if missing_inputs:
        return (
            "insufficient_data",
            "traffic_light_contract_missing_inputs",
            [f"traffic_light_contract.missing_inputs.{item}" for item in missing_inputs],
        )
    reported_scenario_class = str(traffic_light_contract.get("scenario_class") or "").strip()
    if not reported_scenario_class:
        return (
            "insufficient_data",
            "traffic_light_contract_missing_scenario_context",
            ["traffic_light_contract.scenario_class"],
        )
    if reported_scenario_class != scenario_class:
        return (
            "insufficient_data",
            "traffic_light_contract_scenario_mismatch",
            ["traffic_light_contract.scenario_class"],
        )
    source = traffic_light_contract.get("source")
    if not isinstance(source, Mapping):
        return (
            "insufficient_data",
            "traffic_light_contract_missing_source_evidence",
            ["traffic_light_contract.source"],
        )
    required_source_fields = ("town01_contract_path", "traffic_light_mapping_path")
    missing_source = [
        f"traffic_light_contract.source.{field}"
        for field in required_source_fields
        if source.get(field) in {None, ""}
    ]
    if missing_source:
        return "insufficient_data", "traffic_light_contract_missing_source_evidence", missing_source
    missing_source_artifacts = [
        field
        for field in required_source_fields
        if not _source_path_exists(str(source.get(field)), run_dir=run_dir, report_path=report_path)
    ]
    if missing_source_artifacts:
        return (
            "insufficient_data",
            "traffic_light_contract_missing_source_artifacts",
            [f"traffic_light_contract.source.{field}" for field in missing_source_artifacts],
        )
    return "pass", None, []


def _route_curve_artifact_gap_evidence_verdict(
    route_curve_artifact_gap: Mapping[str, Any],
    *,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    missing_inputs = [str(item) for item in (route_curve_artifact_gap.get("missing_inputs") or []) if item]
    if missing_inputs:
        return (
            "insufficient_data",
            "route_curve_artifact_gap_missing_inputs",
            [f"route_curve_artifact_gap.missing_inputs.{item}" for item in missing_inputs],
        )
    source = route_curve_artifact_gap.get("source")
    if not isinstance(source, Mapping):
        return (
            "insufficient_data",
            "route_curve_artifact_gap_missing_source_evidence",
            ["route_curve_artifact_gap.source"],
        )
    required_source_fields = ("timeseries_csv", "summary_json")
    missing_source = [
        f"route_curve_artifact_gap.source.{field}"
        for field in required_source_fields
        if source.get(field) in {None, ""}
    ]
    if missing_source:
        return "insufficient_data", "route_curve_artifact_gap_missing_source_evidence", missing_source
    missing_source_artifacts = [
        field
        for field in required_source_fields
        if not _source_path_exists(str(source.get(field)), run_dir=run_dir, report_path=report_path)
    ]
    if missing_source_artifacts:
        return (
            "insufficient_data",
            "route_curve_artifact_gap_missing_source_artifacts",
            [f"route_curve_artifact_gap.source.{field}" for field in missing_source_artifacts],
        )
    return "pass", None, []


def _traffic_light_behavior_evidence_verdict(
    traffic_light_behavior: Mapping[str, Any],
    *,
    scenario_class: str,
    expected_route_id: str,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    actual_scenario_class = str(traffic_light_behavior.get("scenario_class") or "").strip()
    if not actual_scenario_class:
        return (
            "insufficient_data",
            "traffic_light_behavior_missing_scenario_context",
            ["traffic_light_behavior.scenario_class"],
        )
    if actual_scenario_class != scenario_class:
        return (
            "insufficient_data",
            "traffic_light_behavior_scenario_mismatch",
            ["traffic_light_behavior.scenario_class"],
        )
    actual_route_id = str(traffic_light_behavior.get("route_id") or "").strip()
    if not actual_route_id:
        return (
            "insufficient_data",
            "traffic_light_behavior_missing_route_id",
            ["traffic_light_behavior.route_id"],
        )
    if expected_route_id and actual_route_id != expected_route_id:
        return (
            "insufficient_data",
            "traffic_light_behavior_route_id_mismatch",
            ["traffic_light_behavior.route_id"],
        )
    missing_inputs = [str(item) for item in (traffic_light_behavior.get("missing_inputs") or []) if item]
    if missing_inputs:
        return (
            "insufficient_data",
            "traffic_light_behavior_missing_inputs",
            [f"traffic_light_behavior.missing_inputs.{item}" for item in missing_inputs],
        )
    source = traffic_light_behavior.get("source")
    if not isinstance(source, Mapping):
        return (
            "insufficient_data",
            "traffic_light_behavior_missing_source_evidence",
            ["traffic_light_behavior.source"],
        )
    required_source_fields = (
        "summary_path",
        "manifest_path",
        "timeseries_path",
        "events_path",
        "traffic_light_contract_path",
    )
    missing_source = [
        f"traffic_light_behavior.source.{field}"
        for field in required_source_fields
        if source.get(field) in {None, ""}
    ]
    if missing_source:
        return "insufficient_data", "traffic_light_behavior_missing_source_evidence", missing_source
    missing_source_artifacts = [
        field
        for field in required_source_fields
        if not _source_path_exists(str(source.get(field)), run_dir=run_dir, report_path=report_path)
    ]
    if missing_source_artifacts:
        return (
            "insufficient_data",
            "traffic_light_behavior_missing_source_artifacts",
            [f"traffic_light_behavior.source.{field}" for field in missing_source_artifacts],
        )
    return "pass", None, []


def _control_health_evidence_verdict(
    control_health: Mapping[str, Any],
    *,
    scenario_class: str,
    expected_route_id: str,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    missing_inputs = [str(item) for item in (control_health.get("missing_inputs") or []) if item]
    if missing_inputs:
        return (
            "insufficient_data",
            "control_health_missing_inputs",
            [f"control_health.missing_inputs.{item}" for item in missing_inputs],
        )
    reported_scenario_class = str(control_health.get("scenario_class") or "").strip()
    if not reported_scenario_class:
        return (
            "insufficient_data",
            "control_health_missing_scenario_context",
            ["control_health.scenario_class"],
        )
    if reported_scenario_class != scenario_class:
        return (
            "insufficient_data",
            "control_health_scenario_mismatch",
            ["control_health.scenario_class"],
        )
    reported_route_id = str(control_health.get("route_id") or "").strip()
    if not reported_route_id:
        return "insufficient_data", "control_health_missing_route_id", ["control_health.route_id"]
    if expected_route_id and reported_route_id != expected_route_id:
        return "insufficient_data", "control_health_route_id_mismatch", ["control_health.route_id"]
    if control_health.get("raw_mapped_applied_control_available") is not True:
        missing_fields = control_health.get("missing_fields") or []
        if missing_fields:
            return (
                "insufficient_data",
                "control_health_missing_control_trace",
                [f"control_health.missing_fields.{field}" for field in missing_fields],
            )
        return (
            "insufficient_data",
            "control_health_missing_control_trace",
            ["control_health.raw_mapped_applied_control_available"],
        )
    source = control_health.get("source")
    if not isinstance(source, Mapping):
        return "insufficient_data", "control_health_missing_source_evidence", ["control_health.source"]
    required_source_fields = ("summary_path", "manifest_path", "timeseries_path")
    missing_source = [
        f"control_health.source.{field}"
        for field in required_source_fields
        if source.get(field) in {None, ""}
    ]
    if missing_source:
        return "insufficient_data", "control_health_missing_source_evidence", missing_source
    missing_source_artifacts = [
        field
        for field in required_source_fields
        if not _source_path_exists(str(source.get(field)), run_dir=run_dir, report_path=report_path)
    ]
    if missing_source_artifacts:
        return (
            "insufficient_data",
            "control_health_missing_source_artifacts",
            [f"control_health.source.{field}" for field in missing_source_artifacts],
        )
    return "pass", None, []


def _apollo_control_handoff_evidence_verdict(
    report: Mapping[str, Any],
    *,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    if report_path is None:
        return (
            "insufficient_data",
            "apollo_control_handoff_report_missing",
            ["apollo_control_handoff_report.json"],
        )
    verdict = str(report.get("verdict") or "").strip()
    if verdict == "fail":
        stage = str(report.get("failure_stage") or "unknown")
        return (
            "fail",
            f"apollo_control_handoff_{stage}",
            [f"apollo_control_handoff.failure_stage.{stage}"],
        )
    if verdict != "pass":
        return (
            "insufficient_data",
            "apollo_control_handoff_not_pass",
            ["apollo_control_handoff.verdict"],
        )
    stage = str(report.get("failure_stage") or "").strip()
    if stage and stage != "none":
        status = "insufficient_data" if stage == "insufficient_data" else "fail"
        return (
            status,
            f"apollo_control_handoff_{stage}",
            [f"apollo_control_handoff.failure_stage.{stage}"],
        )
    control_channel = report.get("control_channel")
    if not isinstance(control_channel, Mapping):
        control_channel = {}
    bridge_receive = report.get("bridge_receive")
    if not isinstance(bridge_receive, Mapping):
        bridge_receive = {}
    mapping_and_apply = report.get("mapping_and_apply")
    if not isinstance(mapping_and_apply, Mapping):
        mapping_and_apply = {}
    missing: list[str] = []
    if (_num(control_channel.get("message_count")) or 0.0) <= 0:
        missing.append("apollo_control_handoff.control_channel.message_count")
    if (_num(bridge_receive.get("control_rx_count")) or 0.0) <= 0:
        missing.append("apollo_control_handoff.bridge_receive.control_rx_count")
    if (_num(mapping_and_apply.get("apply_control_count")) or 0.0) <= 0:
        missing.append("apollo_control_handoff.mapping_and_apply.apply_control_count")
    if missing:
        return "insufficient_data", "apollo_control_handoff_missing_materialization", missing
    return "pass", None, []


def _route_health_evidence_verdict(
    route_health: Mapping[str, Any],
    *,
    expected_route_id: str,
    scenario_class: str,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    missing_inputs = [str(item) for item in (route_health.get("missing_inputs") or []) if item]
    if missing_inputs:
        return (
            "insufficient_data",
            "route_health_missing_inputs",
            [f"route_health.missing_inputs.{item}" for item in missing_inputs],
        )
    hard_gate_eligible = route_health.get("hard_gate_eligible")
    evidence_level = str(route_health.get("evidence_level") or "")
    if hard_gate_eligible is not True:
        missing = [
            "route_health.hard_gate_eligible",
            "route_health.evidence_level",
            "route_health.route_evidence_reason",
        ]
        if scenario_class == "curve_diagnostic" and evidence_level == "diagnostic_only":
            return "diagnostic_only", "route_health_diagnostic_only_not_claim_grade", missing
        if scenario_class in ROUTE_HEALTH_HARD_GATE_SCENARIO_CLASSES:
            return "insufficient_data", "route_health_not_hard_gate_eligible", missing
    source_status, source_reason, source_missing = _route_health_source_evidence_verdict(
        route_health,
        expected_route_id=expected_route_id,
        run_dir=run_dir,
        report_path=report_path,
    )
    if source_status != "pass":
        return source_status, source_reason, source_missing
    curve_semantic_missing_fields = {"matched_point", "target_point", "apollo_raw_steer"}
    reported_missing_fields = [
        str(item)
        for item in (route_health.get("missing_fields") or [])
        if item and str(item) not in curve_semantic_missing_fields
    ]
    if reported_missing_fields:
        return (
            "insufficient_data",
            "route_health_reported_missing_fields",
            [f"route_health.missing_fields.{field}" for field in reported_missing_fields],
        )

    missing: list[str] = []
    route_geometry = route_health.get("route_geometry")
    if not isinstance(route_geometry, Mapping):
        missing.append("route_health.route_geometry")
        route_geometry = {}
    _require_numeric(route_geometry, "point_count", missing, "route_health.route_geometry.point_count")
    _require_numeric(route_geometry, "length_m", missing, "route_health.route_geometry.length_m")
    spacing = route_geometry.get("spacing")
    if not isinstance(spacing, Mapping):
        missing.append("route_health.route_geometry.spacing")
        spacing = {}
    for field in ("mean_m", "p95_m", "max_m"):
        _require_numeric(spacing, field, missing, f"route_health.route_geometry.spacing.{field}")
    heading = route_geometry.get("heading")
    if not isinstance(heading, Mapping):
        missing.append("route_health.route_geometry.heading")
        heading = {}
    _require_numeric(heading, "max_jump_rad", missing, "route_health.route_geometry.heading.max_jump_rad")
    curvature = route_geometry.get("curvature")
    if not isinstance(curvature, Mapping):
        missing.append("route_health.route_geometry.curvature")
        curvature = {}
    for field in ("p95_abs", "max_abs"):
        _require_numeric(curvature, field, missing, f"route_health.route_geometry.curvature.{field}")
    spawn_alignment = route_geometry.get("spawn_alignment")
    if not isinstance(spawn_alignment, Mapping):
        missing.append("route_health.route_geometry.spawn_alignment")
        spawn_alignment = {}
    _require_numeric(
        spawn_alignment,
        "distance_m",
        missing,
        "route_health.route_geometry.spawn_alignment.distance_m",
    )
    _require_numeric(
        spawn_alignment,
        "heading_error_rad",
        missing,
        "route_health.route_geometry.spawn_alignment.heading_error_rad",
    )
    if spawn_alignment.get("direction_consistent") is None:
        missing.append("route_health.route_geometry.spawn_alignment.direction_consistent")
    route_direction = route_geometry.get("route_direction_consistency")
    if not isinstance(route_direction, Mapping) or not route_direction.get("status"):
        missing.append("route_health.route_geometry.route_direction_consistency.status")

    run_metrics = route_health.get("run_metrics")
    if not isinstance(run_metrics, Mapping):
        missing.append("route_health.run_metrics")
        run_metrics = {}
    for field in ("lateral_error_p95_m", "heading_error_p95_rad"):
        _require_numeric(run_metrics, field, missing, f"route_health.run_metrics.{field}")

    control_semantics = route_health.get("control_semantics")
    if not isinstance(control_semantics, Mapping):
        missing.append("route_health.control_semantics")
        control_semantics = {}
    if control_semantics.get("raw_mapped_applied_steer_available") is None:
        missing.append("route_health.control_semantics.raw_mapped_applied_steer_available")
    guard_counts = control_semantics.get("guard_apply_counts")
    if not isinstance(guard_counts, Mapping):
        missing.append("route_health.control_semantics.guard_apply_counts")
    else:
        for field in ("lateral_guard", "trajectory_contract_lateral_guard"):
            _require_numeric(
                guard_counts,
                field,
                missing,
                f"route_health.control_semantics.guard_apply_counts.{field}",
            )

    if missing:
        return "insufficient_data", "route_health_missing_evidence", sorted(set(missing))
    return "pass", None, []


def _route_health_source_evidence_verdict(
    route_health: Mapping[str, Any],
    *,
    expected_route_id: str,
    run_dir: Path,
    report_path: Path | None,
) -> tuple[str, str | None, list[str]]:
    actual_route_id = str(route_health.get("route_id") or "").strip()
    if not actual_route_id:
        return "insufficient_data", "route_health_missing_route_id", ["route_health.route_id"]
    if expected_route_id and actual_route_id != expected_route_id:
        return "insufficient_data", "route_health_route_id_mismatch", ["route_health.route_id"]

    source = route_health.get("source")
    if not isinstance(source, Mapping):
        return "insufficient_data", "route_health_missing_source_evidence", ["route_health.source"]

    required_source_fields = ("manifest_path", "summary_path", "timeseries_path")
    missing_source = [
        f"route_health.source.{field}"
        for field in required_source_fields
        if source.get(field) in {None, ""}
    ]
    if missing_source:
        return "insufficient_data", "route_health_missing_source_evidence", missing_source

    missing_source_artifacts = [
        field
        for field in required_source_fields
        if not _source_path_exists(str(source.get(field)), run_dir=run_dir, report_path=report_path)
    ]
    route_path = source.get("route_path")
    route_source = str(route_health.get("route_source") or source.get("route_source") or "")
    evidence_level = str(route_health.get("evidence_level") or "")
    hard_gate_eligible = route_health.get("hard_gate_eligible") is True
    reconstructed = "route reconstructed from timeseries P0 route_curve fields" in {
        str(item) for item in (route_health.get("warnings") or [])
    }
    if route_path in {None, ""}:
        source_is_claim_grade_manifest_route = (
            route_source in {"configured_route_file", "manifest_route", "manifest_route_trace"}
            and evidence_level.startswith("claim_grade")
            and hard_gate_eligible
        )
        if route_source != "inline_route" and not reconstructed and not source_is_claim_grade_manifest_route:
            missing_source.append("route_health.source.route_path")
    elif not _source_path_exists(str(route_path), run_dir=run_dir, report_path=report_path):
        missing_source_artifacts.append("route_path")

    if missing_source:
        return "insufficient_data", "route_health_missing_source_evidence", missing_source
    if missing_source_artifacts:
        return (
            "insufficient_data",
            "route_health_missing_source_artifacts",
            [f"route_health.source.{field}" for field in missing_source_artifacts],
        )
    return "pass", None, []


def _require_numeric(mapping: Mapping[str, Any], field: str, missing: list[str], path: str) -> None:
    if _num(mapping.get(field)) is None:
        missing.append(path)


def _first_high_steer_seq(apollo_semantics: Mapping[str, Any]) -> int | None:
    first = apollo_semantics.get("first_high_steer")
    if not isinstance(first, Mapping):
        return None
    value = first.get("seq")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _curve_semantic_verdict(
    run: Mapping[str, Any],
    route_health: Mapping[str, Any],
) -> tuple[str, str | None, list[str]]:
    apollo_semantics = route_health.get("apollo_semantics")
    if not isinstance(apollo_semantics, Mapping):
        return "insufficient_data", "curve_semantics_missing_fields", ["apollo_semantics"]
    required_apollo_keys = {
        "matched_point_anomaly_locations",
        "target_point_anomaly_locations",
        "first_high_steer",
    }
    missing_apollo_keys = sorted(required_apollo_keys - set(apollo_semantics))
    if missing_apollo_keys:
        return (
            "insufficient_data",
            "curve_semantics_missing_fields",
            [f"apollo_semantics.{key}" for key in missing_apollo_keys],
        )
    missing = {
        str(field)
        for field in route_health.get("missing_fields") or []
        if field in {"matched_point", "target_point", "apollo_raw_steer"}
    }
    if missing:
        return "insufficient_data", "curve_semantics_missing_fields", sorted(missing)
    if _num(run.get("matched_point_anomaly_count")) and _num(run.get("matched_point_anomaly_count")) > 0:
        return "fail", "matched_point_anomaly", []
    if _num(run.get("target_point_anomaly_count")) and _num(run.get("target_point_anomaly_count")) > 0:
        return "fail", "target_point_anomaly", []
    if run.get("first_high_steer_seq") is not None:
        return "fail", "first_high_steer", []
    return "pass", None, []


def _traffic_behavior_metric(
    traffic_light_behavior: Mapping[str, Any],
    name: str,
    *,
    fallback: float | None,
) -> float | None:
    metrics = traffic_light_behavior.get("metrics")
    if isinstance(metrics, Mapping):
        value = _num(metrics.get(name))
        if value is not None:
            return value
    return fallback


def _traffic_behavior_bool(
    traffic_light_behavior: Mapping[str, Any],
    name: str,
    *,
    fallback: bool | None,
) -> bool | None:
    metrics = traffic_light_behavior.get("metrics")
    if isinstance(metrics, Mapping):
        value = _parse_bool(metrics.get(name))
        if value is not None:
            return value
    return fallback


def _traffic_light_expectation(
    manifest: Mapping[str, Any],
    traffic_light_behavior: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    for source_name, source in (
        ("manifest", manifest),
        ("traffic_light_behavior_report", traffic_light_behavior),
    ):
        expectation = source.get("traffic_light_expectation")
        if isinstance(expectation, Mapping):
            return dict(expectation), source_name
    return None, None


def _traffic_light_claim_grade(expectation: Mapping[str, Any] | None) -> bool:
    if not isinstance(expectation, Mapping):
        return False
    if expectation.get("claim_grade") is False:
        return False
    stimulus_mode = str(expectation.get("stimulus_mode") or "").strip()
    return stimulus_mode in CLAIM_GRADE_TRAFFIC_LIGHT_STIMULUS_MODES


def _traffic_light_control(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    for source in (summary, manifest):
        value = source.get("traffic_light_control")
        if isinstance(value, Mapping):
            return dict(value)
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping):
            scenario_metadata = metadata.get("scenario_metadata")
            if isinstance(scenario_metadata, Mapping):
                value = scenario_metadata.get("traffic_light_control")
                if isinstance(value, Mapping):
                    return dict(value)
    return {}


def _traffic_light_release_observed(control: Mapping[str, Any]) -> bool:
    if control.get("release_frame_id") not in {None, ""}:
        return True
    for event in control.get("events") or []:
        if isinstance(event, Mapping) and event.get("phase") == "release":
            return True
    return False


def _traffic_light_control_evidence_verdict(
    run: Mapping[str, Any],
) -> tuple[str, str | None, list[str]]:
    expectation = run.get("traffic_light_expectation")
    control = run.get("traffic_light_control")
    if not isinstance(expectation, Mapping):
        return "insufficient_data", "traffic_light_expectation_missing", ["traffic_light_expectation"]
    if not isinstance(control, Mapping) or not control:
        return "insufficient_data", "traffic_light_control_missing", ["traffic_light_control"]
    if control.get("stimulus_mode") != "deterministic_gt_control":
        return (
            "insufficient_data",
            "traffic_light_control_not_deterministic",
            ["traffic_light_control.stimulus_mode"],
        )
    affected_count = _num(control.get("initial_affected_count"))
    if affected_count is None:
        affected_count = _num(control.get("last_affected_count"))
    if affected_count is None:
        return (
            "insufficient_data",
            "traffic_light_control_missing_affected_count",
            ["traffic_light_control.initial_affected_count"],
        )
    if affected_count <= 0:
        return "fail", "traffic_light_control_no_actor_affected", []
    expected_initial = str(expectation.get("expected_initial_state") or "").strip().upper()
    observed_initial = str(control.get("initial_state") or "").strip().upper()
    if expected_initial and observed_initial and observed_initial != expected_initial:
        return "fail", "traffic_light_control_initial_state_mismatch", ["traffic_light_control.initial_state"]
    expected_release = str(expectation.get("expected_release_state") or "").strip().upper()
    if expected_release:
        observed_release = str(control.get("release_state") or "").strip().upper()
        if observed_release and observed_release != expected_release:
            return "fail", "traffic_light_control_release_state_mismatch", ["traffic_light_control.release_state"]
        if not _traffic_light_release_observed(control):
            return (
                "insufficient_data",
                "traffic_light_control_release_not_observed",
                ["traffic_light_control.release_frame_id"],
            )
    return "pass", None, []


def _control_trace_available(rows: Sequence[Mapping[str, Any]]) -> bool:
    return not _missing_control_trace_fields(rows)


def _missing_control_trace_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return list(MISSING_CONTROL_TRACE_FIELDS)
    missing: list[str] = []
    for field in MISSING_CONTROL_TRACE_FIELDS:
        if not any(row.get(field) not in {None, ""} for row in rows):
            missing.append(field)
    return missing


def _bool_metric(summary: Mapping[str, Any], name: str) -> bool | None:
    metrics = _summary_metrics(summary)
    return _parse_bool(metrics.get(name))


def _summary_bool(summary: Mapping[str, Any], name: str) -> bool | None:
    value = _parse_bool(summary.get(name))
    if value is not None:
        return value
    metrics = summary.get("metrics")
    if isinstance(metrics, Mapping):
        return _parse_bool(metrics.get(name))
    return None


def _summary_text(summary: Mapping[str, Any], name: str) -> str | None:
    value = summary.get(name)
    if value not in {None, ""}:
        return str(value)
    metrics = summary.get("metrics")
    if isinstance(metrics, Mapping):
        value = metrics.get(name)
        if value not in {None, ""}:
            return str(value)
    return None


def _control_handoff_status(summary: Mapping[str, Any]) -> str | None:
    value = _summary_text(summary, "control_handoff_status")
    if value:
        return value
    handoff = summary.get("control_handoff")
    if isinstance(handoff, Mapping) and handoff.get("control_handoff_status") not in {None, ""}:
        return str(handoff.get("control_handoff_status"))
    return None


def _parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _truth_input_declared(manifest: Mapping[str, Any]) -> bool:
    value = _parse_bool(manifest.get("truth_input"))
    if value is not None:
        return value
    input_mode = str(manifest.get("input_mode") or manifest.get("gt_source") or "").strip().lower()
    return input_mode in {"truth_input", "carla_gt", "ground_truth", "gt"}


def _runtime_contract_status(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> str | None:
    for source in (summary, manifest):
        runtime_contract = source.get("runtime_contract")
        if isinstance(runtime_contract, Mapping) and runtime_contract.get("status") not in {None, ""}:
            return str(runtime_contract.get("status"))
        value = source.get("runtime_contract_status")
        if value not in {None, ""}:
            return str(value)
    return None


def _first_text(*args: Any, default: str | None = None) -> str | None:
    pairs = list(zip(args[0::2], args[1::2]))
    for mapping, key in pairs:
        if isinstance(mapping, Mapping):
            value = mapping.get(str(key))
            if value not in {None, ""}:
                return str(value)
    return default


def _infer_scenario_class(name: str) -> str:
    if "traffic_light_red_to_green" in name:
        return "traffic_light_red_to_green_release"
    if "traffic_light_red" in name or "red_stop" in name:
        return "traffic_light_red_stop"
    if "traffic_light_green" in name or "green_go" in name:
        return "traffic_light_green_go"
    if "junction" in name:
        return "junction_turn"
    if "curve" in name:
        return "curve_diagnostic"
    return "lane_keep"


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _first_num(*values: Any) -> float | None:
    for value in values:
        parsed = _num(value)
        if parsed is not None:
            return parsed
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _failed_channel_names(channel_health: Mapping[str, Any]) -> list[str]:
    results = channel_health.get("channel_results")
    if not isinstance(results, Mapping):
        return []
    failed: list[str] = []
    for name, entry in results.items():
        if isinstance(entry, Mapping) and entry.get("status") == "fail":
            failed.append(str(name))
    return sorted(failed)


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


def _write_csv(path: Path, run_results: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for run in run_results:
            row = {field: run.get(field) for field in CSV_FIELDS}
            row["missing_artifacts"] = ";".join(run.get("missing_artifacts") or [])
            row["missing_fields"] = ";".join(run.get("missing_fields") or [])
            row["missing_manifest_fields"] = ";".join(run.get("missing_manifest_fields") or [])
            row["invalid_manifest_source_fields"] = ";".join(run.get("invalid_manifest_source_fields") or [])
            row["missing_control_trace_fields"] = ";".join(run.get("missing_control_trace_fields") or [])
            row["invalid_report_source_fields"] = ";".join(run.get("invalid_report_source_fields") or [])
            row["active_assists"] = ";".join(run.get("active_assists") or [])
            row["blocking_assists"] = ";".join(run.get("blocking_assists") or [])
            row["non_blocking_assists"] = ";".join(run.get("non_blocking_assists") or [])
            row["why_not_claimable"] = ";".join(run.get("why_not_claimable") or [])
            row["apollo_channel_gap_failures"] = ";".join(run.get("apollo_channel_gap_failures") or [])
            row["apollo_channel_low_rate_channels"] = ";".join(
                run.get("apollo_channel_low_rate_channels") or []
            )
            row["apollo_channel_missing_required_channels"] = ";".join(
                run.get("apollo_channel_missing_required_channels") or []
            )
            row["apollo_channel_failed_channels"] = ";".join(
                run.get("apollo_channel_failed_channels") or []
            )
            row["localization_blocking_reasons"] = ";".join(
                run.get("localization_blocking_reasons") or []
            )
            writer.writerow(row)


def _summary_markdown(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    coverage = report.get("capability_coverage") if isinstance(report.get("capability_coverage"), Mapping) else {}
    lines = [
        "# Town01 Natural Driving Summary",
        "",
        f"- status: `{verdict.get('status')}`",
        f"- can_claim_full_natural_driving: `{verdict.get('can_claim_full_natural_driving')}`",
        f"- suite_plan_missing: `{', '.join(verdict.get('suite_plan_missing') or [])}`",
        f"- filtered_suite_plan: `{verdict.get('filtered_suite_plan')}`",
        f"- run_count: `{report.get('run_count')}`",
        f"- pass_count: `{summary.get('pass_count')}`",
        f"- warn_count: `{summary.get('warn_count')}`",
        f"- fail_count: `{summary.get('fail_count')}`",
        f"- insufficient_data_count: `{summary.get('insufficient_data_count')}`",
        f"- missing_manifest_fields_run_count: `{summary.get('missing_manifest_fields_run_count')}`",
        f"- invalid_manifest_source_fields_run_count: `{summary.get('invalid_manifest_source_fields_run_count')}`",
        f"- missing_control_trace_fields_run_count: `{summary.get('missing_control_trace_fields_run_count')}`",
        f"- invalid_report_source_fields_run_count: `{summary.get('invalid_report_source_fields_run_count')}`",
        f"- blocking_assist_run_count: `{summary.get('blocking_assist_run_count')}`",
        f"- unassisted_claimable_run_count: `{summary.get('unassisted_claimable_run_count')}`",
        f"- missing_required_scenario_classes: `{', '.join(coverage.get('missing_required_scenario_classes') or [])}`",
        f"- unproven_required_scenario_classes: `{', '.join(coverage.get('unproven_required_scenario_classes') or [])}`",
        f"- missing_required_scenario_ids: `{', '.join(coverage.get('missing_required_scenario_ids') or [])}`",
        f"- unproven_required_scenario_ids: `{', '.join(coverage.get('unproven_required_scenario_ids') or [])}`",
        f"- scenario_identity_mismatches: `{len(coverage.get('scenario_identity_mismatches') or [])}`",
        "",
        "| run_id | scenario_class | route_id | transport | active_assists | blocking_assists | unassisted_claim | why_not_claimable | runtime | handoff | channel | localization | localization_blocking | verdict | failure_reason | tl_expected | stopped_at_red | matched | target | first_high_steer |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in report.get("run_results") or []:
        lines.append(
            f"| {run.get('run_id')} | {run.get('scenario_class')} | {run.get('route_id')} | "
            f"{run.get('transport_mode')} | "
            f"{';'.join(run.get('active_assists') or [])} | "
            f"{';'.join(run.get('blocking_assists') or [])} | "
            f"{run.get('can_claim_unassisted_natural_driving')} | "
            f"{';'.join(run.get('why_not_claimable') or [])} | "
            f"{run.get('runtime_contract_status')} | {run.get('control_handoff_status')} | "
            f"{run.get('apollo_channel_health_status')} | "
            f"{run.get('localization_contract_status')} | "
            f"{';'.join(run.get('localization_blocking_reasons') or [])} | "
            f"{run.get('verdict')} | {run.get('failure_reason')} | "
            f"{run.get('traffic_light_expected_behavior')} | "
            f"{run.get('stopped_at_red')} | "
            f"{run.get('matched_point_anomaly_count')} | {run.get('target_point_anomaly_count')} | "
            f"{run.get('first_high_steer_seq')} |"
        )

    source_problem_rows = [
        run
        for run in report.get("run_results") or []
        if run.get("missing_manifest_fields")
        or run.get("invalid_manifest_source_fields")
        or run.get("missing_control_trace_fields")
        or run.get("invalid_report_source_fields")
    ]
    if source_problem_rows:
        lines.extend(
            [
                "",
                "## Evidence Source Problems",
                "",
                "| run_id | failure_reason | missing_manifest_fields | invalid_manifest_source_fields | "
                "missing_control_trace_fields | invalid_report_source_fields |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for run in source_problem_rows:
            lines.append(
                f"| {run.get('run_id')} | {run.get('failure_reason')} | "
                f"{_markdown_list(run.get('missing_manifest_fields'))} | "
                f"{_markdown_list(run.get('invalid_manifest_source_fields'))} | "
                f"{_markdown_list(run.get('missing_control_trace_fields'))} | "
                f"{_markdown_list(run.get('invalid_report_source_fields'))} |"
            )
    lines.extend(
        [
            "",
            "This report aggregates artifacts only. It does not start CARLA/Apollo and does not treat "
            "`candidate_positive` as a natural-driving pass.",
            "",
        ]
    )
    return "\n".join(lines)


def _markdown_list(values: Any) -> str:
    if not values:
        return ""
    if isinstance(values, (list, tuple, set)):
        return "<br>".join(str(value) for value in values)
    return str(values)


def _unique_ordered(values: Sequence[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True
