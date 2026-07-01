from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract

PHASE1_STATUS_SCHEMA_VERSION = "phase1_status.v1"

INVALID_REASONS = {
    "setup_failed",
    "missing_target_actor",
    "missing_apollo_obstacle_gt_contract",
    "apollo_obstacle_gt_contract_failed",
    "apollo_obstacle_gt_contract_insufficient_data",
    "missing_required_artifact",
    "backend_not_ready",
    "no_timeseries",
    "fixed_scene_failed",
    "config_invalid",
}
FAILED_REASONS = {
    "collision",
    "control_apply_mismatch",
    "control_process_failed",
    "no_control",
    "planning_control_handoff_missing",
    "stuck",
    "overshoot_target",
    "unsafe_gap",
    "timeout",
    "route_health_failed",
    "off_route",
    "lane_invasion",
    "route_establishment_latency",
    "scenario_phase_trigger_not_reached",
}
DEGRADED_REASONS = {
    "late_response",
    "large_final_gap",
    "small_final_gap",
    "unstable_control",
    "degraded_gap_method",
}


def classify_phase1_run(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    v_t_gap = _read_json(root / "analysis" / "v_t_gap" / "v_t_gap_report.json")
    required_artifacts = _required_artifacts(root)
    missing = [name for name, state in required_artifacts.items() if state == "missing"]

    # Setup/preflight reasons dominate missing raw artifacts. An Apollo scaffold
    # that did not start runtime should remain backend_not_ready, not no_timeseries.
    if _summary_failed_by_setup(summary):
        return _status(root, manifest, summary, "invalid", "setup_failed", [], v_t_gap, required_artifacts)
    if _backend_not_ready(summary, manifest):
        return _status(root, manifest, summary, "invalid", "backend_not_ready", [], v_t_gap, required_artifacts)
    fixed_scene_status = _fixed_scene_status(root, summary)
    if fixed_scene_status is not None:
        status, reason = fixed_scene_status
        return _status(root, manifest, summary, status, reason, [], v_t_gap, required_artifacts)

    base_missing = [name for name in ("manifest", "summary") if required_artifacts[name] == "missing"]
    if base_missing:
        return _status(root, manifest, summary, "invalid", "missing_required_artifact", missing, v_t_gap, required_artifacts)
    if required_artifacts["timeseries"] == "missing":
        return _status(root, manifest, summary, "invalid", "no_timeseries", missing, v_t_gap, required_artifacts)

    target_contract = _target_contract(root, v_t_gap, manifest)
    if target_contract.get("status") == "missing":
        return _status(root, manifest, summary, "invalid", "missing_target_actor", [], v_t_gap, required_artifacts)
    if not v_t_gap and target_contract.get("status") != "not_required":
        return _status(
            root,
            manifest,
            summary,
            "invalid",
            "missing_required_artifact",
            ["analysis/v_t_gap/v_t_gap_report.json"],
            v_t_gap,
            required_artifacts,
        )

    if v_t_gap and v_t_gap.get("status") == "invalid":
        reason = str(v_t_gap.get("invalid_reason") or "missing_required_artifact")
        if reason not in INVALID_REASONS:
            reason = "missing_required_artifact"
        return _status(root, manifest, summary, "invalid", reason, [], v_t_gap, required_artifacts)

    if _apollo_fixed_scene_target_requires_obstacle(manifest, summary, target_contract):
        if _missing_apollo_obstacle_gt_contract(root):
            return _status(
                root,
                manifest,
                summary,
                "invalid",
                "missing_apollo_obstacle_gt_contract",
                ["analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"],
                v_t_gap,
                required_artifacts,
            )
        obstacle_blocker = _apollo_obstacle_gt_contract_blocker(root)
        if obstacle_blocker:
            return _status(root, manifest, summary, "invalid", obstacle_blocker, [], v_t_gap, required_artifacts)

    failed = _failed_reason(root, summary, v_t_gap)
    if failed:
        return _status(root, manifest, summary, "failed", failed, [], v_t_gap, required_artifacts)

    summary_invalid = _unclassified_summary_failure_reason(summary)
    if summary_invalid:
        return _status(root, manifest, summary, "invalid", summary_invalid, [], v_t_gap, required_artifacts)

    degraded = _degraded_reason(summary, v_t_gap)
    if degraded:
        return _status(root, manifest, summary, "degraded", degraded, [], v_t_gap, required_artifacts)

    return _status(root, manifest, summary, "success", None, [], v_t_gap, required_artifacts)


def write_phase1_status(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "phase1_status.json"
    md_path = output / "phase1_status_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_phase1_status_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _phase1_status_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Run Status",
        "",
        f"Status: `{report.get('status')}`",
        "",
        f"Failure reason: `{report.get('failure_reason')}`",
        "",
        f"Run evaluable: `{report.get('run_evaluable')}`",
        f"Scenario interaction evaluable: `{report.get('scenario_interaction_evaluable')}`",
        f"Target metric evaluable: `{report.get('target_metric_evaluable')}`",
        f"Backend-loss eligible for target scenario: `{report.get('counts_as_backend_loss_for_target_scenario')}`",
    ]
    if report.get("primary_behavior_blocker"):
        lines.extend(
            [
                "",
                "## Primary Behavior Blocker",
                "",
                f"- blocker: `{report.get('primary_behavior_blocker')}`",
                f"- layer: `{report.get('behavior_blocker_layer')}`",
                f"- next_action: `{report.get('behavior_next_action')}`",
                (
                    "- claim_boundary: `Phase 1 blocker attribution explains an evaluable "
                    "scenario failure; it is not natural-driving capability evidence.`"
                ),
            ]
    )
    if report.get("primary_setup_blocker"):
        lines.extend(
            [
                "",
                "## Primary Setup Blocker",
                "",
                f"- blocker: `{report.get('primary_setup_blocker')}`",
                f"- layer: `{report.get('setup_blocker_layer')}`",
                f"- next_action: `{report.get('setup_next_action')}`",
                (
                    "- claim_boundary: `Phase 1 setup blocker attribution explains why "
                    "a run is invalid; it is not backend behavior evidence and must not "
                    "be counted as a backend loss.`"
                ),
            ]
        )
    route_lateral_policy = report.get("route_lateral_field_policy")
    if (
        isinstance(route_lateral_policy, Mapping)
        and route_lateral_policy.get("status") not in {None, "not_applicable"}
    ):
        lines.extend(
            [
                "",
                "## Route Lateral Field Policy",
                "",
                f"- policy: `{route_lateral_policy.get('policy')}`",
                f"- source_field: `{route_lateral_policy.get('source_field')}`",
                f"- sign_sensitive_gate_allowed: `{route_lateral_policy.get('sign_sensitive_gate_allowed')}`",
                f"- absolute_magnitude_gate_allowed: `{route_lateral_policy.get('absolute_magnitude_gate_allowed')}`",
                f"- recommended_action: `{route_lateral_policy.get('recommended_action')}`",
            ]
        )
    reference_line_policy = report.get("reference_line_debug_export_policy")
    if (
        isinstance(reference_line_policy, Mapping)
        and reference_line_policy.get("status") not in {None, "not_applicable"}
    ):
        lines.extend(
            [
                "",
                "## Reference-Line Debug Export Policy",
                "",
                f"- policy: `{reference_line_policy.get('policy')}`",
                f"- classification: `{reference_line_policy.get('classification')}`",
                f"- field_gap_classification: `{reference_line_policy.get('field_gap_classification')}`",
                f"- reference_line_count_positive_count: `{reference_line_policy.get('reference_line_count_positive_count')}`",
                f"- claim_window_source: `{reference_line_policy.get('claim_window_source')}`",
                f"- claim_window_reference_line_count_positive_count: `{reference_line_policy.get('claim_window_reference_line_count_positive_count')}`",
                f"- planning_route_segment_count_positive_count: `{reference_line_policy.get('planning_route_segment_count_positive_count')}`",
                f"- routing_segment_count_positive_count: `{reference_line_policy.get('routing_segment_count_positive_count')}`",
                f"- routing_road_count_positive_count: `{reference_line_policy.get('routing_road_count_positive_count')}`",
                f"- trajectory_sample_rows: `{reference_line_policy.get('trajectory_sample_rows')}`",
                f"- control_target_point_rows: `{reference_line_policy.get('control_target_point_rows')}`",
                f"- planning_debug_presence_classification: `{reference_line_policy.get('planning_debug_presence_classification')}`",
                f"- planning_debug_reference_line_nonempty_ratio: `{reference_line_policy.get('planning_debug_reference_line_nonempty_ratio')}`",
                f"- planning_debug_routing_segment_nonempty_ratio: `{reference_line_policy.get('planning_debug_routing_segment_nonempty_ratio')}`",
                f"- planning_materialization_classification: `{reference_line_policy.get('planning_materialization_classification')}`",
                f"- planning_materialization_route_segments_ready_ratio: `{reference_line_policy.get('planning_materialization_route_segments_ready_ratio')}`",
                f"- planning_materialization_reference_line_empty_while_ready_ratio: `{reference_line_policy.get('planning_materialization_reference_line_empty_while_ready_ratio')}`",
                f"- reference_line_debug_field_state: `{reference_line_policy.get('reference_line_debug_field_state')}`",
                f"- planning_info_log_reference_line_available: `{reference_line_policy.get('planning_info_log_reference_line_available')}`",
                f"- planning_info_log_reference_line_claim_grade_allowed: `{reference_line_policy.get('planning_info_log_reference_line_claim_grade_allowed')}`",
                f"- reference_line_debug_claim_grade_allowed: `{reference_line_policy.get('reference_line_debug_claim_grade_allowed')}`",
                f"- local_surrogate_available: `{reference_line_policy.get('local_surrogate_available')}`",
                f"- recommended_action: `{reference_line_policy.get('recommended_action')}`",
            ]
        )
    path_context = report.get("path_candidate_control_context")
    if (
        isinstance(path_context, Mapping)
        and path_context.get("status") not in {None, "not_applicable"}
    ):
        lines.extend(
            [
                "",
                "## Path Candidate / Control Target Context",
                "",
                f"- status: `{path_context.get('status')}`",
                f"- control_target_vs_path_candidate: `{path_context.get('control_target_vs_path_candidate_classification')}`",
                f"- target_inside_path_lateral_envelope: `{path_context.get('target_inside_path_lateral_envelope')}`",
                f"- target_point_to_path_candidate_line_abs_p95_m: `{path_context.get('target_point_to_path_candidate_line_abs_p95_m')}`",
                f"- target_point_lane_l_abs_p95_m: `{path_context.get('target_point_lane_l_abs_p95_m')}`",
                f"- path_candidate_lane_l_abs_p95_m: `{path_context.get('path_candidate_lane_l_abs_p95_m')}`",
                f"- path_candidate_vs_trajectory: `{path_context.get('planning_debug_path_candidate_vs_trajectory_sample_classification')}`",
                f"- path_candidate_to_planning_sample_line_abs_p95_m: `{path_context.get('path_candidate_to_planning_sample_line_abs_p95_m')}`",
                f"- path_candidate_hdmap_projection: `{path_context.get('planning_debug_path_candidate_hdmap_projection_classification')}`",
                f"- path_candidate_routing_lane_window_compatible: `{path_context.get('path_candidate_routing_lane_window_compatible')}`",
                f"- reference_line_claim_grade_allowed: `{path_context.get('reference_line_claim_grade_allowed')}`",
                f"- recommended_evidence_policy: `{path_context.get('recommended_evidence_policy')}`",
                f"- claim_boundary: `{path_context.get('claim_boundary')}`",
            ]
        )
    metrics = report.get("phase1_metrics") if isinstance(report.get("phase1_metrics"), Mapping) else {}
    lane_context = (
        metrics.get("lane_invasion_context")
        if isinstance(metrics.get("lane_invasion_context"), Mapping)
        else {}
    )
    if lane_context.get("available"):
        lines.extend(
            [
                "",
                "## Lane Invasion Context",
                "",
                f"- first_row_index: `{lane_context.get('first_row_index')}`",
                f"- first_sim_time_s: `{lane_context.get('first_sim_time_s')}`",
                f"- ego_speed_mps: `{lane_context.get('ego_speed_mps')}`",
                f"- ego_displacement_from_start_m: `{lane_context.get('ego_displacement_from_start_m')}`",
                f"- cross_track_error_m: `{lane_context.get('cross_track_error_m')}`",
                f"- heading_error_rad: `{lane_context.get('heading_error_rad')}`",
            ]
        )
    initial_condition = (
        metrics.get("initial_condition_materialization")
        if isinstance(metrics.get("initial_condition_materialization"), Mapping)
        else {}
    )
    if initial_condition.get("status") not in {None, "not_applicable"}:
        lines.extend(
            [
                "",
                "## Initial Condition Materialization",
                "",
                f"- status: `{initial_condition.get('status')}`",
                f"- reason: `{initial_condition.get('reason')}`",
                f"- expected_ego_initial_speed_mps: `{initial_condition.get('expected_ego_initial_speed_mps')}`",
                f"- observed_ego_initial_speed_mps: `{initial_condition.get('observed_ego_initial_speed_mps')}`",
                f"- delta_mps: `{initial_condition.get('delta_mps')}`",
                f"- tolerance_mps: `{initial_condition.get('tolerance_mps')}`",
                f"- claim_boundary: `{initial_condition.get('claim_boundary')}`",
            ]
        )
    route_start_alignment = (
        metrics.get("route_start_alignment")
        if isinstance(metrics.get("route_start_alignment"), Mapping)
        else {}
    )
    if route_start_alignment.get("available"):
        lines.extend(
            [
                "",
                "## Route Start Alignment",
                "",
                f"- status: `{route_start_alignment.get('status')}`",
                f"- reason: `{route_start_alignment.get('reason')}`",
                f"- static_spawn_lateral_offset_m: `{route_start_alignment.get('static_spawn_lateral_offset_m')}`",
                f"- initial_cross_track_error_m: `{route_start_alignment.get('initial_cross_track_error_m')}`",
                f"- startup_geometry_localization_to_final_start_distance_m: `{route_start_alignment.get('startup_geometry_localization_to_final_start_distance_m')}`",
                f"- recommended_ego_offset_y_delta_m: `{route_start_alignment.get('recommended_ego_offset_y_delta_m')}`",
                f"- warnings: `{route_start_alignment.get('warnings')}`",
                f"- hypotheses: `{route_start_alignment.get('hypotheses')}`",
                (
                    "- claim_boundary: `Route-start alignment is diagnostic setup evidence. "
                    "It can justify a probe override, but it does not prove backend behavior success.`"
                ),
            ]
        )
    blockers = (
        metrics.get("derived_blocker_evidence")
        if isinstance(metrics.get("derived_blocker_evidence"), Mapping)
        else {}
    )
    if blockers.get("available"):
        control_health = (
            blockers.get("control_health")
            if isinstance(blockers.get("control_health"), Mapping)
            else {}
        )
        link_health = (
            blockers.get("apollo_link_health")
            if isinstance(blockers.get("apollo_link_health"), Mapping)
            else {}
        )
        lane_event = (
            blockers.get("baguang_lane_event_contract")
            if isinstance(blockers.get("baguang_lane_event_contract"), Mapping)
            else {}
        )
        lines.extend(
            [
                "",
                "## Derived Blocker Evidence",
                "",
                f"- control_health_failure_reason: `{control_health.get('failure_reason')}`",
                f"- apollo_link_primary_blocker: `{link_health.get('primary_blocker')}`",
                f"- phase1_relevant_apollo_link_blocker: `{link_health.get('phase1_relevant_primary_blocker')}`",
                f"- lane_event_departure_classification: `{lane_event.get('departure_classification')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _status(
    root: Path,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    status: str,
    reason: str | None,
    missing_artifacts: list[str],
    v_t_gap: Mapping[str, Any],
    required_artifacts: Mapping[str, str],
) -> dict[str, Any]:
    original_reason = summary.get("fail_reason") or summary.get("failure_reason")
    target_contract = _target_contract(root, v_t_gap, manifest)
    scenario_case = _phase1_scenario_case(manifest, summary)
    invalid_reasons = [reason] if status == "invalid" and reason else []
    failed_reasons = [reason] if status == "failed" and reason else []
    degraded_reasons = [reason] if status == "degraded" and reason else []
    evaluability = _phase1_evaluability(
        root=root,
        manifest=manifest,
        summary=summary,
        status=status,
        reason=reason,
        target_contract=target_contract,
        v_t_gap=v_t_gap,
    )
    derived_blocker_evidence = _derived_blocker_evidence(root)
    behavior_blocker = _primary_behavior_blocker(
        status=status,
        reason=reason,
        derived_blocker_evidence=derived_blocker_evidence,
    )
    setup_blocker = _primary_setup_blocker(
        status=status,
        reason=reason,
        derived_blocker_evidence=derived_blocker_evidence,
    )
    route_lateral_field_policy = _route_lateral_field_policy(derived_blocker_evidence)
    reference_line_debug_export_policy = _reference_line_debug_export_policy(
        derived_blocker_evidence
    )
    path_candidate_control_context = _path_candidate_control_context(derived_blocker_evidence)
    return {
        "schema_version": PHASE1_STATUS_SCHEMA_VERSION,
        "run_dir": str(root),
        "run_id": manifest.get("run_id") or summary.get("run_id") or root.name,
        "scenario_id": manifest.get("scenario_id") or summary.get("scenario_id"),
        "scenario_case": scenario_case,
        "backend": manifest.get("backend") or manifest.get("backend_name") or summary.get("backend"),
        "backend_name": manifest.get("backend_name") or manifest.get("backend") or summary.get("backend_name"),
        "backend_type": _backend_type(manifest, summary),
        "status": status,
        "failure_reason": reason,
        "invalid_reasons": invalid_reasons,
        "degraded_reasons": degraded_reasons,
        "failed_reasons": failed_reasons,
        "primary_behavior_blocker": behavior_blocker["primary_behavior_blocker"],
        "behavior_blocker_layer": behavior_blocker["behavior_blocker_layer"],
        "behavior_next_action": behavior_blocker["behavior_next_action"],
        "behavior_blocker_evidence": behavior_blocker["behavior_blocker_evidence"],
        "primary_setup_blocker": setup_blocker["primary_setup_blocker"],
        "setup_blocker_layer": setup_blocker["setup_blocker_layer"],
        "setup_next_action": setup_blocker["setup_next_action"],
        "setup_blocker_evidence": setup_blocker["setup_blocker_evidence"],
        "route_lateral_field_policy": route_lateral_field_policy,
        "reference_line_debug_export_policy": reference_line_debug_export_policy,
        "path_candidate_control_context": path_candidate_control_context,
        "original_failure_reason": original_reason,
        "evaluable": evaluability["run_evaluable"],
        "run_evaluable": evaluability["run_evaluable"],
        "scenario_interaction_evaluable": evaluability["scenario_interaction_evaluable"],
        "scenario_interaction_reason": evaluability["scenario_interaction_reason"],
        "target_metric_evaluable": evaluability["target_metric_evaluable"],
        "target_metric_status": evaluability["target_metric_status"],
        "target_metric_reason": evaluability["target_metric_reason"],
        "safety_event_hard_gate_eligible": evaluability["safety_event_hard_gate_eligible"],
        "safety_event_hard_gate_reason": evaluability["safety_event_hard_gate_reason"],
        "counts_as_backend_loss_for_target_scenario": evaluability[
            "counts_as_backend_loss_for_target_scenario"
        ],
        "target_actor_contract": dict(target_contract) if target_contract else None,
        "artifact_contract_version": manifest.get("artifact_contract_version"),
        "phase1_metrics": {
            **_phase1_metrics(v_t_gap),
            "initial_condition_materialization": _initial_condition_materialization(root, v_t_gap),
            "safety_event_evidence": _safety_event_evidence(root, summary),
            "control_motion": _control_motion_metrics(root),
            "lane_invasion_context": _lane_invasion_context(
                root,
                summary,
                route_lateral_field_policy=route_lateral_field_policy,
            ),
            "route_start_alignment": _route_start_alignment_metrics(root),
            "derived_blocker_evidence": derived_blocker_evidence,
        },
        "evidence_files": _evidence_files(root),
        "missing_artifacts": missing_artifacts,
        "required_artifacts": dict(required_artifacts),
        "v_t_gap_status": v_t_gap.get("status") if v_t_gap else None,
        "summary_status": summary.get("status"),
        "summary_success": summary.get("success"),
        "notes": _status_notes(status, reason),
        "claim_boundary": "phase1_status_is_scenario_evaluation_not_natural_driving_claim",
    }


def _phase1_evaluability(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    status: str,
    reason: str | None,
    target_contract: Mapping[str, Any],
    v_t_gap: Mapping[str, Any],
) -> dict[str, Any]:
    run_evaluable = status != "invalid"
    target_required = _target_required(target_contract, manifest)
    fixed_interaction = _fixed_scene_interaction_evidence(root)
    initial_condition = _initial_condition_materialization(root, v_t_gap)
    if not run_evaluable:
        scenario_interaction_evaluable = False
        scenario_interaction_reason = "run_invalid"
    elif target_required and initial_condition.get("status") == "fail":
        scenario_interaction_evaluable = False
        scenario_interaction_reason = str(
            initial_condition.get("reason") or "ego_initial_speed_not_materialized"
        )
    elif target_required and not fixed_interaction["evaluable"]:
        scenario_interaction_evaluable = False
        scenario_interaction_reason = str(fixed_interaction["reason"] or "required_phase_not_reached")
    else:
        scenario_interaction_evaluable = True
        scenario_interaction_reason = None

    target_metric_status, target_metric_evaluable, target_metric_reason = _target_metric_evidence(
        target_required=target_required,
        v_t_gap=v_t_gap,
        run_evaluable=run_evaluable,
    )
    safety_event_hard_gate_eligible, safety_event_hard_gate_reason = _safety_event_hard_gate_eligibility(
        root=root,
        status=status,
        reason=reason,
    )
    return {
        "run_evaluable": run_evaluable,
        "scenario_interaction_evaluable": scenario_interaction_evaluable,
        "scenario_interaction_reason": scenario_interaction_reason,
        "target_metric_evaluable": target_metric_evaluable,
        "target_metric_status": target_metric_status,
        "target_metric_reason": target_metric_reason,
        "safety_event_hard_gate_eligible": safety_event_hard_gate_eligible,
        "safety_event_hard_gate_reason": safety_event_hard_gate_reason,
        "counts_as_backend_loss_for_target_scenario": bool(
            run_evaluable
            and scenario_interaction_evaluable
            and target_metric_evaluable
            and safety_event_hard_gate_eligible
            and status == "failed"
        ),
    }


def _safety_event_hard_gate_eligibility(
    *,
    root: Path,
    status: str,
    reason: str | None,
) -> tuple[bool, str | None]:
    if status != "failed" or reason != "lane_invasion":
        return True, None
    lane_event = _read_json(root / "analysis" / "baguang_lane_event_contract" / "baguang_lane_event_contract_report.json")
    if not lane_event:
        return True, None
    boundary = lane_event.get("claim_boundary") if isinstance(lane_event.get("claim_boundary"), Mapping) else {}
    if boundary.get("lane_invasion_event_can_be_used_as_hard_gate") is False:
        return False, str(boundary.get("reason") or "lane_event_contract_blocks_hard_gate")
    return True, None


def _target_required(target_contract: Mapping[str, Any], manifest: Mapping[str, Any]) -> bool:
    status = str(target_contract.get("status") or "")
    if status == "not_required":
        return False
    if target_contract.get("required") is False:
        return False
    scenario_class = str(
        manifest.get("scenario_class")
        or manifest.get("scenario_type")
        or manifest.get("scenario_id")
        or ""
    )
    return scenario_class not in {"lane_keep", "lane_keep_straight", "curve_diagnostic", "junction"}


def _fixed_scene_interaction_evidence(root: Path) -> dict[str, Any]:
    fixed_scene_report = _read_json(root / "analysis" / "fixed_scene_contract" / "fixed_scene_contract_report.json")
    scenario_actor_report = _read_json(
        root / "analysis" / "scenario_actor_contract" / "scenario_actor_contract_report.json"
    )
    if not fixed_scene_report and not scenario_actor_report:
        return {"evaluable": True, "reason": None}
    fixed_status = str(fixed_scene_report.get("status") or "")
    actor_status = str(scenario_actor_report.get("status") or "")
    if fixed_status == "pass" and actor_status in {"", "pass", "warn"}:
        return {"evaluable": True, "reason": None}
    if _fixed_scene_duration_only_after_actor_interaction(fixed_scene_report, scenario_actor_report):
        return {"evaluable": True, "reason": None}
    if _fixed_scene_unreached_without_setup_blocker(fixed_scene_report, scenario_actor_report):
        return {"evaluable": False, "reason": "required_phase_not_reached"}
    if fixed_status == "fail" or actor_status == "fail":
        return {"evaluable": False, "reason": "fixed_scene_or_actor_contract_failed"}
    if actor_status == "insufficient_data":
        return {"evaluable": False, "reason": "scenario_actor_contract_insufficient_data"}
    return {"evaluable": True, "reason": None}


def _target_metric_evidence(
    *,
    target_required: bool,
    v_t_gap: Mapping[str, Any],
    run_evaluable: bool,
) -> tuple[str, bool, str | None]:
    if not target_required:
        return ("not_applicable", True, None)
    if not run_evaluable:
        return ("not_evaluable", False, "run_invalid")
    if not v_t_gap:
        return ("missing", False, "missing_v_t_gap")
    raw_status = str(v_t_gap.get("status") or "")
    if raw_status in {"pass", "warn", "fail"}:
        return (raw_status, True, None)
    if raw_status == "invalid":
        return ("invalid", False, str(v_t_gap.get("invalid_reason") or "v_t_gap_invalid"))
    if raw_status == "not_applicable":
        return ("not_applicable", False, "target_metric_not_applicable_for_required_target")
    return (raw_status or "unknown", False, "target_metric_unknown")


def _required_artifacts(root: Path) -> dict[str, str]:
    return {
        "manifest": "present" if (root / "manifest.json").exists() else "missing",
        "summary": "present" if (root / "summary.json").exists() else "missing",
        "timeseries": "present" if (root / "timeseries.csv").exists() or (root / "timeseries.jsonl").exists() else "missing",
        "v_t_gap_report": "present"
        if (root / "analysis" / "v_t_gap" / "v_t_gap_report.json").exists()
        else "missing",
    }


def _evidence_files(root: Path) -> list[str]:
    candidates = [
        "manifest.json",
        "summary.json",
        "timeseries.csv",
        "timeseries.jsonl",
        "artifacts/fixed_scene_resolved.json",
        "analysis/v_t_gap/v_t_gap_report.json",
        "analysis/v_t_gap/v_t_gap.csv",
        "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        "artifacts/obstacle_gt_contract.jsonl",
        "artifacts/obstacle_gt_contract.json",
        "artifacts/startup_geometry_summary.json",
        "analysis/route_start_alignment/route_start_alignment_report.json",
    ]
    return [str(root / rel) for rel in candidates if (root / rel).exists()]


def _status_notes(status: str, reason: str | None) -> list[str]:
    notes = ["phase1_status_is_scenario_evaluation_not_natural_driving_claim"]
    if status == "invalid":
        notes.append("invalid_run_is_setup_or_evidence_failure_not_backend_loss")
    if reason:
        notes.append(f"classified_by_{reason}")
    return notes


def _phase1_metrics(v_t_gap: Mapping[str, Any]) -> dict[str, Any]:
    rows = v_t_gap.get("rows") if isinstance(v_t_gap.get("rows"), list) else []
    rows = [row for row in rows if isinstance(row, Mapping)]
    gaps = [_to_float(row.get("gap_m")) for row in rows]
    gaps = [gap for gap in gaps if gap is not None]
    ego_speeds = [_to_float(row.get("ego_speed_mps")) for row in rows]
    ego_speeds = [speed for speed in ego_speeds if speed is not None]
    target_speeds = [_to_float(row.get("target_speed_mps")) for row in rows]
    target_speeds = [speed for speed in target_speeds if speed is not None]
    return {
        "v_t_gap_row_count": len(rows),
        "initial_gap_m": gaps[0] if gaps else None,
        "final_gap_m": gaps[-1] if gaps else None,
        "min_gap_m": min(gaps) if gaps else None,
        "max_gap_m": max(gaps) if gaps else None,
        "gap_delta_m": gaps[-1] - gaps[0] if len(gaps) >= 2 else None,
        "max_ego_speed_mps": max(ego_speeds) if ego_speeds else None,
        "final_ego_speed_mps": ego_speeds[-1] if ego_speeds else None,
        "max_target_speed_mps": max(target_speeds) if target_speeds else None,
        "final_target_speed_mps": target_speeds[-1] if target_speeds else None,
    }


def _route_start_alignment_metrics(root: Path) -> dict[str, Any]:
    path = root / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    report = _read_json(path)
    if not report:
        return {
            "available": False,
            "status": None,
            "reason": None,
            "path": None,
            "claim_boundary": (
                "Route-start alignment was not evaluated for this run. Missing "
                "route-start evidence must not be interpreted as setup alignment success."
            ),
        }

    static = report.get("static_spawn_alignment") if isinstance(report.get("static_spawn_alignment"), Mapping) else {}
    initial = report.get("initial_ego_alignment") if isinstance(report.get("initial_ego_alignment"), Mapping) else {}
    startup = (
        report.get("startup_geometry_alignment")
        if isinstance(report.get("startup_geometry_alignment"), Mapping)
        else {}
    )
    recommendation = (
        report.get("recommendation") if isinstance(report.get("recommendation"), Mapping) else {}
    )
    source = report.get("source") if isinstance(report.get("source"), Mapping) else {}
    return {
        "available": True,
        "status": report.get("status"),
        "reason": report.get("reason"),
        "warnings": list(report.get("warnings") or []),
        "hypotheses": list(report.get("hypotheses") or []),
        "path": str(path),
        "source_run_dir": source.get("run_dir"),
        "source_startup_geometry_summary_path": source.get("startup_geometry_summary_path"),
        "static_spawn_lateral_offset_m": _to_float(static.get("spawn_lateral_offset_m")),
        "static_spawn_longitudinal_offset_m": _to_float(static.get("spawn_longitudinal_offset_m")),
        "static_spawn_to_route_start_distance_m": _to_float(static.get("spawn_to_route_start_distance_m")),
        "initial_cross_track_error_m": _to_float(initial.get("cross_track_error_m")),
        "initial_heading_error_rad": _to_float(initial.get("heading_error_rad")),
        "initial_route_s_m": _to_float(initial.get("route_s")),
        "initial_rear_axle_offset_compatible": initial.get("rear_axle_offset_compatible"),
        "initial_rear_axle_offset_error_m": _to_float(initial.get("rear_axle_offset_error_m")),
        "startup_geometry_summary_status": startup.get("summary_status"),
        "startup_geometry_record_count": _to_float(startup.get("record_count")),
        "startup_geometry_localization_to_final_start_distance_m": _to_float(
            startup.get("localization_to_final_start_distance_m")
        ),
        "startup_geometry_raw_start_to_snapped_start_distance_m": _to_float(
            startup.get("raw_start_to_snapped_start_distance_m")
        ),
        "startup_geometry_localization_reference_mode": startup.get("localization_reference_mode"),
        "startup_geometry_trusted_lane_centerline": startup.get("trusted_lane_centerline"),
        "recommended_config_field": recommendation.get("recommended_config_field"),
        "recommended_ego_offset_y_delta_m": _to_float(
            recommendation.get("recommended_ego_offset_y_delta_m")
        ),
        "recommended_ego_offset_y_m_if_current_zero": _to_float(
            recommendation.get("recommended_ego_offset_y_m_if_current_zero")
        ),
        "probe_only": recommendation.get("probe_only"),
        "claim_boundary": (
            "Route-start alignment is diagnostic setup evidence for Phase 1. "
            "A cleaner start alignment can reduce one confounder, but it does not "
            "turn a failed backend run into behavior success or natural-driving evidence."
        ),
    }


def _initial_condition_materialization(root: Path, v_t_gap: Mapping[str, Any]) -> dict[str, Any]:
    storyboard = _read_json(root / "artifacts" / "fixed_scene_resolved.json")
    expected = _expected_ego_initial_speed(storyboard)
    if expected is None:
        return {
            "status": "not_applicable",
            "reason": None,
            "expected_ego_initial_speed_mps": None,
            "observed_ego_initial_speed_mps": None,
            "delta_mps": None,
            "tolerance_mps": None,
        }

    rows = v_t_gap.get("rows") if isinstance(v_t_gap.get("rows"), list) else []
    observed_values: list[float] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        value = _to_float(row.get("ego_speed_mps"))
        if value is not None:
            observed_values.append(value)
        if len(observed_values) >= 10:
            break
    if not observed_values:
        return {
            "status": "insufficient_data",
            "reason": "ego_initial_speed_observation_missing",
            "expected_ego_initial_speed_mps": expected,
            "observed_ego_initial_speed_mps": None,
            "delta_mps": None,
            "tolerance_mps": _initial_speed_tolerance(expected),
        }

    tolerance = _initial_speed_tolerance(expected)
    selected_values = observed_values
    observed_source = "v_t_gap_initial_window_median"

    # Initial speed materialization is a setup property. Apollo or another
    # backend may legitimately change speed immediately after ownership, so a
    # later control response must not erase evidence that the requested initial
    # state existed at the start of the target interaction window. Accept the
    # leading short window when at least two of the first three valid samples
    # are close to the requested initial speed; otherwise fall back to the
    # broader median window used historically.
    leading_values = observed_values[:3]
    leading_close_values = [
        value for value in leading_values if abs(float(value) - float(expected)) <= tolerance
    ]
    if len(leading_close_values) >= 2:
        selected_values = leading_values
        observed_source = "v_t_gap_leading_window"

    observed = _median(selected_values)
    delta = abs(float(observed) - float(expected))
    status = "pass" if delta <= tolerance else "fail"
    reason = None if status == "pass" else "ego_initial_speed_not_materialized"
    return {
        "status": status,
        "reason": reason,
        "expected_ego_initial_speed_mps": expected,
        "observed_ego_initial_speed_mps": observed,
        "observed_ego_initial_speed_source": observed_source,
        "observed_ego_initial_speed_window_count": len(observed_values),
        "selected_observed_ego_initial_speed_window_count": len(selected_values),
        "delta_mps": delta,
        "tolerance_mps": tolerance,
        "claim_boundary": (
            "This checks Phase 1 scenario initial-state materialization only. "
            "A mismatch makes target-interaction comparison incomplete; it is "
            "not by itself an Apollo behavior loss."
        ),
    }


def _expected_ego_initial_speed(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles") if isinstance(storyboard.get("roles"), Mapping) else {}
    ego = roles.get("ego") if isinstance(roles.get("ego"), Mapping) else {}
    value = _to_float(ego.get("initial_speed_mps"))
    if value is not None:
        return value
    params = storyboard.get("params") if isinstance(storyboard.get("params"), Mapping) else {}
    return _to_float(params.get("ego_initial_speed_mps"))


def _initial_speed_tolerance(expected_mps: float) -> float:
    return max(2.0, abs(float(expected_mps)) * 0.25)


def _median(values: list[float]) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _derived_blocker_evidence(root: Path) -> dict[str, Any]:
    control_health_path = root / "analysis" / "control_health" / "control_health_report.json"
    link_health_path = root / "analysis" / "apollo_link_health" / "apollo_link_health_report.json"
    lane_event_path = (
        root
        / "analysis"
        / "baguang_lane_event_contract"
        / "baguang_lane_event_contract_report.json"
    )
    control_health = _read_json(control_health_path)
    link_health = _read_json(link_health_path)
    lane_event = _read_json(lane_event_path)
    platform_timeout = _platform_timeout_blocker_summary(root)
    available = bool(control_health or link_health or lane_event or platform_timeout.get("available"))
    return {
        "available": available,
        "control_health": _control_health_blocker_summary(control_health, control_health_path),
        "apollo_link_health": _apollo_link_health_blocker_summary(link_health, link_health_path),
        "baguang_lane_event_contract": _baguang_lane_event_blocker_summary(lane_event, lane_event_path),
        "platform_timeout": platform_timeout,
        "claim_boundary": (
            "Derived blocker evidence explains an evaluable Phase 1 failure. "
            "It does not override phase1 status, and it is not natural-driving capability evidence."
        ),
    }


_ROUTE_HEALTH_LINK_BLOCKER_LAYERS = {
    "apollo_lateral_semantics",
    "planning_reference_line",
    "routing_planning_control_handoff",
    "control_mapping_apply",
    "localization_gt_contract",
}


def _route_health_link_behavior_blocker(link_health: Mapping[str, Any]) -> dict[str, str] | None:
    if not link_health.get("available"):
        return None
    candidates = (
        link_health.get("phase1_relevant_primary_blocker"),
        link_health.get("primary_blocker"),
    )
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate:
            continue
        if ":" in candidate:
            layer, blocker = candidate.split(":", 1)
        else:
            layer, blocker = "apollo_link_health", candidate
        if layer not in _ROUTE_HEALTH_LINK_BLOCKER_LAYERS or not blocker:
            continue
        return {
            "layer": layer,
            "blocker": blocker,
            "source": candidate,
        }
    return None


def _primary_behavior_blocker(
    *,
    status: str,
    reason: str | None,
    derived_blocker_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    empty = {
        "primary_behavior_blocker": None,
        "behavior_blocker_layer": None,
        "behavior_next_action": None,
        "behavior_blocker_evidence": {},
    }
    if status != "failed" or not reason:
        return empty

    lane_event = (
        derived_blocker_evidence.get("baguang_lane_event_contract")
        if isinstance(derived_blocker_evidence.get("baguang_lane_event_contract"), Mapping)
        else {}
    )
    control_health = (
        derived_blocker_evidence.get("control_health")
        if isinstance(derived_blocker_evidence.get("control_health"), Mapping)
        else {}
    )
    link_health = (
        derived_blocker_evidence.get("apollo_link_health")
        if isinstance(derived_blocker_evidence.get("apollo_link_health"), Mapping)
        else {}
    )
    platform_timeout = (
        derived_blocker_evidence.get("platform_timeout")
        if isinstance(derived_blocker_evidence.get("platform_timeout"), Mapping)
        else {}
    )

    if reason == "lane_invasion":
        if lane_event.get("available"):
            lane_event_hard_gate = lane_event.get("lane_invasion_event_can_be_used_as_hard_gate")
            if lane_event_hard_gate is False:
                return {
                    "primary_behavior_blocker": "lane_event_contract_quarantines_lane_invasion",
                    "behavior_blocker_layer": "lane_event_contract",
                    "behavior_next_action": (
                        "Repair or explain the Baguang lane-event contract before counting "
                        "this lane invasion as a backend behavior loss. Current evidence says "
                        "the LaneInvasionSensor trigger is inconsistent with centerline/static "
                        "footprint evidence, so compare spawn pose, OpenDRIVE road marks, "
                        "vehicle footprint, and lane-invasion sensor placement before changing "
                        "Apollo, builtin controller, steer scale, smoothing, or actuation mapping."
                    ),
                    "behavior_blocker_evidence": {
                        "lane_event_reason": lane_event.get("reason"),
                        "departure_classification": lane_event.get("departure_classification"),
                        "departure_interpretation": list(lane_event.get("departure_interpretation") or []),
                        "lane_invasion_event_can_be_used_as_hard_gate": lane_event_hard_gate,
                        "path": lane_event.get("path"),
                    },
                }
            interpretation = list(lane_event.get("departure_interpretation") or [])
            same_sign = "raw_steer_same_sign_as_cross_track_error" in interpretation
            route_simple_lat_sign_convention = (
                link_health.get("primary_blocker")
                == "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
            ) or (
                link_health.get("route_simple_lat_sign_convention_candidate") is True
            )
            projection_route_sign_confirmed = (
                link_health.get("projection_route_sample_sign_contract_classification")
                == "timeseries_route_lateral_sign_inverted_vs_projection_route_samples"
                and link_health.get("projection_route_sample_timeseries_opposite_sign_ratio") == 1.0
                and link_health.get("projection_route_sample_simple_lat_same_sign_ratio") == 1.0
            )
            route_lateral_gate_policy = link_health.get("route_lateral_field_recommended_gate_policy")
            route_lateral_field_action = link_health.get("route_lateral_field_recommended_action")
            if link_health.get("planning_debug_presence_classification") == "routing_present_reference_line_empty":
                reference_line_next_action = (
                    "Planning debug path is present and routing segments are present, but "
                    "debug.planning_data.reference_line is empty; inspect Apollo Planning "
                    "reference-line materialization/config/debug content for this route"
                )
            else:
                reference_line_next_action = "Close the Planning reference-line debug/export gap"
            blocker = (
                "lane_departure_with_route_simple_lat_sign_convention_candidate"
                if same_sign and route_simple_lat_sign_convention
                else
                "lane_departure_with_source_steer_same_sign"
                if same_sign
                else "lane_departure_evidence"
            )
            next_action = (
                (
                    "Projection-route sample evidence already confirms the route-lateral "
                    f"sign inversion candidate for this run. {reference_line_next_action} and "
                    "decide whether the route-lateral field should be renamed, explicitly "
                    "converted, or excluded from behavior gates until a "
                    "canonical sign convention is declared. Current route-lateral gate policy: "
                    f"{route_lateral_gate_policy or 'not_declared'}; recommended field action: "
                    f"{route_lateral_field_action or 'not_declared'}. Do not change steer scale, "
                    "control mapping, smoothing, or controller gains yet."
                )
                if projection_route_sign_confirmed
                else (
                    "Validate the sign convention between CARLA route cross-track error and "
                    "Apollo simple_lat lateral error before changing control mapping, steer "
                    "scale, smoothing, or controller gains."
                )
                if route_simple_lat_sign_convention
                else (
                    "Inspect Apollo lateral/reference-line and raw->mapped->applied steer "
                    "around the first lane event; do not tune PID or relax the lane-event "
                    "gate before this attribution is checked."
                )
            )
            return {
                "primary_behavior_blocker": blocker,
                "behavior_blocker_layer": (
                    "apollo_lateral_semantics"
                    if route_simple_lat_sign_convention
                    else "lane_event_contract"
                ),
                "behavior_next_action": next_action,
                "behavior_blocker_evidence": {
                    "lane_event_reason": lane_event.get("reason"),
                    "departure_classification": lane_event.get("departure_classification"),
                    "departure_interpretation": interpretation,
                    "lane_invasion_event_can_be_used_as_hard_gate": lane_event.get(
                        "lane_invasion_event_can_be_used_as_hard_gate"
                    ),
                    "apollo_link_health_primary_blocker": link_health.get("primary_blocker"),
                    "phase1_relevant_apollo_link_blocker": link_health.get(
                        "phase1_relevant_primary_blocker"
                    ),
                    "route_simple_lat_sign_convention_candidate": link_health.get(
                        "route_simple_lat_sign_convention_candidate"
                    ),
                    "route_lateral_provenance_evidence_level": link_health.get(
                        "route_lateral_provenance_evidence_level"
                    ),
                    "route_lateral_provenance_interpretation": link_health.get(
                        "route_lateral_provenance_interpretation"
                    ),
                    "route_definition_geometry_status": link_health.get(
                        "route_definition_geometry_status"
                    ),
                    "projection_route_sample_sign_contract_status": link_health.get(
                        "projection_route_sample_sign_contract_status"
                    ),
                    "projection_route_sample_sign_contract_classification": link_health.get(
                        "projection_route_sample_sign_contract_classification"
                    ),
                    "projection_route_sample_count": link_health.get(
                        "projection_route_sample_count"
                    ),
                    "projection_route_sample_matched_sample_count": link_health.get(
                        "projection_route_sample_matched_sample_count"
                    ),
                    "projection_route_sample_lateral_source_field": link_health.get(
                        "projection_route_sample_lateral_source_field"
                    ),
                    "projection_route_sample_timeseries_opposite_sign_ratio": link_health.get(
                        "projection_route_sample_timeseries_opposite_sign_ratio"
                    ),
                    "projection_route_sample_simple_lat_same_sign_ratio": link_health.get(
                        "projection_route_sample_simple_lat_same_sign_ratio"
                    ),
                    "projection_route_sample_sign_contract_confirmed": projection_route_sign_confirmed,
                    "route_lateral_field_semantics_classification": link_health.get(
                        "route_lateral_field_semantics_classification"
                    ),
                    "route_lateral_field_recommended_gate_policy": link_health.get(
                        "route_lateral_field_recommended_gate_policy"
                    ),
                    "route_lateral_field_recommended_action": link_health.get(
                        "route_lateral_field_recommended_action"
                    ),
                    "route_lateral_field_sign_sensitive_gate_allowed": link_health.get(
                        "route_lateral_field_sign_sensitive_gate_allowed"
                    ),
                    "route_lateral_field_absolute_magnitude_gate_allowed": link_health.get(
                        "route_lateral_field_absolute_magnitude_gate_allowed"
                    ),
                    "official_hdmap_projection_matched_sample_count": link_health.get(
                        "official_hdmap_projection_matched_sample_count"
                    ),
                    "route_lateral_projection_lateral_opposite_sign_ratio": link_health.get(
                        "route_lateral_projection_lateral_opposite_sign_ratio"
                    ),
                    "simple_lat_projection_lateral_same_sign_ratio": link_health.get(
                        "simple_lat_projection_lateral_same_sign_ratio"
                    ),
                    "route_projection_abs_magnitude_delta_p95_m": link_health.get(
                        "route_projection_abs_magnitude_delta_p95_m"
                    ),
                    "simple_lat_matched_point_projection_line_lateral_abs_p95_m": link_health.get(
                        "simple_lat_matched_point_projection_line_lateral_abs_p95_m"
                    ),
                    "simple_lat_matched_point_projection_line_sample_count": link_health.get(
                        "simple_lat_matched_point_projection_line_sample_count"
                    ),
                    "simple_lat_current_reference_point_projection_line_lateral_abs_p95_m": link_health.get(
                        "simple_lat_current_reference_point_projection_line_lateral_abs_p95_m"
                    ),
                    "simple_lat_current_reference_point_projection_line_sample_count": link_health.get(
                        "simple_lat_current_reference_point_projection_line_sample_count"
                    ),
                    "simple_lat_target_point_projection_line_lateral_abs_p95_m": link_health.get(
                        "simple_lat_target_point_projection_line_lateral_abs_p95_m"
                    ),
                    "simple_lat_target_point_projection_line_sample_count": link_health.get(
                        "simple_lat_target_point_projection_line_sample_count"
                    ),
                    "simple_lat_point_coverage_status": link_health.get(
                        "simple_lat_point_coverage_status"
                    ),
                    "simple_lat_missing_point_fields": link_health.get(
                        "simple_lat_missing_point_fields"
                    ),
                    "simple_lat_station_coverage_status": link_health.get(
                        "simple_lat_station_coverage_status"
                    ),
                    "simple_lat_station_frame_classification": link_health.get(
                        "simple_lat_station_frame_classification"
                    ),
                    "simple_lat_missing_station_fields": link_health.get(
                        "simple_lat_missing_station_fields"
                    ),
                    "simple_lat_current_station_projection_s_delta_p95_m": link_health.get(
                        "simple_lat_current_station_projection_s_delta_p95_m"
                    ),
                    "simple_lat_matched_s_projection_s_delta_p95_m": link_health.get(
                        "simple_lat_matched_s_projection_s_delta_p95_m"
                    ),
                    "simple_lat_target_s_projection_s_delta_p95_m": link_health.get(
                        "simple_lat_target_s_projection_s_delta_p95_m"
                    ),
                    "simple_lat_target_s_current_station_delta_p95_m": link_health.get(
                        "simple_lat_target_s_current_station_delta_p95_m"
                    ),
                    "route_simple_lat_opposite_sign_abs_sum_p95_m": link_health.get(
                        "route_simple_lat_opposite_sign_abs_sum_p95_m"
                    ),
                    "route_simple_lat_abs_magnitude_delta_p95_m": link_health.get(
                        "route_simple_lat_abs_magnitude_delta_p95_m"
                    ),
                    "route_simple_lat_alignment_interpretation": link_health.get(
                        "route_simple_lat_alignment_interpretation"
                    ),
                    "path": lane_event.get("path"),
                },
            }
        return {
            "primary_behavior_blocker": "lane_invasion_without_lane_event_contract",
            "behavior_blocker_layer": "missing_lane_event_contract",
            "behavior_next_action": (
                "Generate baguang_lane_event_contract_report.json before counting this "
                "lane invasion as backend behavior evidence."
            ),
            "behavior_blocker_evidence": {},
        }

    if reason in {
        "unstable_control",
        "control_apply_mismatch",
        "no_control",
        "planning_control_handoff_missing",
    }:
        return {
            "primary_behavior_blocker": control_health.get("failure_reason") or reason,
            "behavior_blocker_layer": "control_health",
            "behavior_next_action": (
                "Inspect control_health, control_handoff, and raw/mapped/applied traces; "
                "do not hide source-command behavior with smoothing or clamps."
            ),
            "behavior_blocker_evidence": {
                "control_health_failure_reason": control_health.get("failure_reason"),
                "control_semantics_primary_factor": control_health.get(
                    "control_semantics_primary_factor"
                ),
                "control_semantics_suspected_factors": control_health.get(
                    "control_semantics_suspected_factors"
                ),
                "path": control_health.get("path"),
            },
        }

    if reason == "timeout" and platform_timeout.get("classification") == (
        "planning_available_control_process_crash_timeout"
    ):
        return {
            "primary_behavior_blocker": "planning_available_control_process_crash_timeout",
            "behavior_blocker_layer": "apollo_control_process_health",
            "behavior_next_action": (
                "Routing succeeded and Planning emitted non-empty trajectories before timeout, "
                "but the deferred Apollo Control process did not stay alive and /apollo/control "
                "never materialized. Inspect Control runtime process health and startup logs "
                "before changing reference-line, steer scale, smoothing, or CARLA actuation."
            ),
            "behavior_blocker_evidence": {
                "classification": platform_timeout.get("classification"),
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "runtime_routing_request_count": platform_timeout.get("runtime_routing_request_count"),
                "runtime_routing_success_count": platform_timeout.get("runtime_routing_success_count"),
                "runtime_control_tx_count": platform_timeout.get("runtime_control_tx_count"),
                "planning_nonempty_trajectory_count": platform_timeout.get(
                    "planning_nonempty_trajectory_count"
                ),
                "control_started_pid_seen": platform_timeout.get("control_started_pid_seen"),
                "control_survived_5s": platform_timeout.get("control_survived_5s"),
                "control_survived_10s": platform_timeout.get("control_survived_10s"),
                "control_present_at_end": platform_timeout.get("control_present_at_end"),
                "control_crash_detected": platform_timeout.get("control_crash_detected"),
                "control_crash_reason": platform_timeout.get("control_crash_reason"),
                "control_crash_signature": platform_timeout.get("control_crash_signature"),
                "control_deferred_survival_path": platform_timeout.get(
                    "control_deferred_survival_path"
                ),
                "control_deferred_mainboard_log_path": platform_timeout.get(
                    "control_deferred_mainboard_log_path"
                ),
                "planning_topic_debug_summary_path": platform_timeout.get(
                    "planning_topic_debug_summary_path"
                ),
                "cyber_bridge_stats_path": platform_timeout.get("cyber_bridge_stats_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    if reason == "timeout" and platform_timeout.get("classification") == (
        "routing_available_reference_line_empty_control_missing_timeout"
    ):
        return {
            "primary_behavior_blocker": "routing_available_reference_line_empty_control_missing_timeout",
            "behavior_blocker_layer": "apollo_planning_reference_line",
            "behavior_next_action": (
                "Routing was sent and succeeded before timeout, and Planning debug shows "
                "routing segments while debug.planning_data.reference_line remains empty. "
                "Inspect Apollo Planning reference-line materialization, route/lane compatibility, "
                "and Planning logs before changing Control, steer scale, smoothing, or CARLA actuation."
            ),
            "behavior_blocker_evidence": {
                "classification": platform_timeout.get("classification"),
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "runtime_routing_request_count": platform_timeout.get("runtime_routing_request_count"),
                "runtime_routing_success_count": platform_timeout.get("runtime_routing_success_count"),
                "runtime_control_tx_count": platform_timeout.get("runtime_control_tx_count"),
                "planning_debug_presence_classification": platform_timeout.get(
                    "planning_debug_presence_classification"
                ),
                "planning_debug_reference_line_nonempty_ratio": platform_timeout.get(
                    "planning_debug_reference_line_nonempty_ratio"
                ),
                "planning_debug_routing_segment_nonempty_ratio": platform_timeout.get(
                    "planning_debug_routing_segment_nonempty_ratio"
                ),
                "command_last_blocking_reason": platform_timeout.get("command_last_blocking_reason"),
                "command_last_phase": platform_timeout.get("command_last_phase"),
                "command_first_ready_to_send_ts_sec": platform_timeout.get(
                    "command_first_ready_to_send_ts_sec"
                ),
                "cyber_bridge_stats_path": platform_timeout.get("cyber_bridge_stats_path"),
                "planning_topic_debug_summary_path": platform_timeout.get(
                    "planning_topic_debug_summary_path"
                ),
                "command_materialization_summary_path": platform_timeout.get(
                    "command_materialization_summary_path"
                ),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    if reason == "timeout" and platform_timeout.get("classification") == (
        "control_output_present_route_chain_not_finalized_timeout"
    ):
        return {
            "primary_behavior_blocker": "control_output_present_route_chain_not_finalized_timeout",
            "behavior_blocker_layer": "phase1_runtime_finalization",
            "behavior_next_action": (
                "Routing, Planning, and /apollo/control all materialized before timeout, "
                "and timeseries shows route progress. Inspect route-chain completion, "
                "Phase 1 exit criteria, finalized summary materialization, and why the "
                "online progress log still reports a non-final route/summary state before "
                "changing Control, steer scale, smoothing, or CARLA actuation."
            ),
            "behavior_blocker_evidence": {
                "classification": platform_timeout.get("classification"),
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "max_routing_success_count": platform_timeout.get("max_routing_success_count"),
                "max_planning_message_count": platform_timeout.get("max_planning_message_count"),
                "max_control_tx_count": platform_timeout.get("max_control_tx_count"),
                "runtime_routing_success_count": platform_timeout.get("runtime_routing_success_count"),
                "planning_nonempty_trajectory_count": platform_timeout.get(
                    "planning_nonempty_trajectory_count"
                ),
                "runtime_control_tx_count": platform_timeout.get("runtime_control_tx_count"),
                "last_phase": platform_timeout.get("last_phase"),
                "last_route_status": platform_timeout.get("last_route_status"),
                "last_summary_status": platform_timeout.get("last_summary_status"),
                "last_control_status": platform_timeout.get("last_control_status"),
                "last_speed_mps": platform_timeout.get("last_speed_mps"),
                "phase_counts": platform_timeout.get("phase_counts"),
                "timeseries_rows": platform_timeout.get("timeseries_rows"),
                "route_s_delta_m": platform_timeout.get("route_s_delta_m"),
                "final_ego_speed_mps": platform_timeout.get("final_ego_speed_mps"),
                "command_materialization_summary_path": platform_timeout.get(
                    "command_materialization_summary_path"
                ),
                "cyber_bridge_stats_path": platform_timeout.get("cyber_bridge_stats_path"),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    if (
        reason == "route_health_failed"
        or (
            reason == "timeout"
            and platform_timeout.get("classification")
            == "finalized_route_health_failure_platform_wrapper_timeout"
        )
    ):
        link_route_health_blocker = (
            _route_health_link_behavior_blocker(link_health)
            if reason == "route_health_failed"
            else None
        )
        primary_behavior_blocker = (
            link_route_health_blocker["blocker"]
            if link_route_health_blocker
            else (
                "finalized_route_health_failure_platform_wrapper_timeout"
                if reason == "timeout"
                else "finalized_route_health_failure"
            )
        )
        behavior_blocker_layer = (
            link_route_health_blocker["layer"]
            if link_route_health_blocker
            else "apollo_route_health_behavior"
        )
        behavior_next_action = _route_health_failure_next_action(
            platform_timeout.get("nested_failure_codes"),
            nested_exit_reason=platform_timeout.get("nested_exit_reason"),
            timed_out_after_summary=(reason == "timeout"),
        )
        if link_route_health_blocker:
            behavior_next_action = (
                "Route-health finalized as failed, and Apollo link-health provides a more "
                f"specific behavior blocker at `{behavior_blocker_layer}`. Inspect "
                f"`{primary_behavior_blocker}` evidence before changing control mapping, "
                "steer scale, smoothing, or CARLA actuation. " + behavior_next_action
            )
        return {
            "primary_behavior_blocker": primary_behavior_blocker,
            "behavior_blocker_layer": behavior_blocker_layer,
            "behavior_next_action": behavior_next_action,
            "behavior_blocker_evidence": {
                "classification": platform_timeout.get("classification"),
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "nested_summary_path": platform_timeout.get("nested_summary_path"),
                "nested_summary_status": platform_timeout.get("nested_summary_status"),
                "nested_summary_success": platform_timeout.get("nested_summary_success"),
                "nested_exit_reason": platform_timeout.get("nested_exit_reason"),
                "nested_fail_reason": platform_timeout.get("nested_fail_reason"),
                "nested_failure_codes": platform_timeout.get("nested_failure_codes"),
                "nested_route_health_label": platform_timeout.get("nested_route_health_label"),
                "nested_route_distance_achieved_m": platform_timeout.get(
                    "nested_route_distance_achieved_m"
                ),
                "nested_route_completion_ratio": platform_timeout.get(
                    "nested_route_completion_ratio"
                ),
                "max_routing_success_count": platform_timeout.get("max_routing_success_count"),
                "max_planning_message_count": platform_timeout.get("max_planning_message_count"),
                "max_control_tx_count": platform_timeout.get("max_control_tx_count"),
                "runtime_routing_success_count": platform_timeout.get("runtime_routing_success_count"),
                "planning_nonempty_trajectory_count": platform_timeout.get(
                    "planning_nonempty_trajectory_count"
                ),
                "runtime_control_tx_count": platform_timeout.get("runtime_control_tx_count"),
                "last_phase": platform_timeout.get("last_phase"),
                "last_route_status": platform_timeout.get("last_route_status"),
                "last_summary_status": platform_timeout.get("last_summary_status"),
                "last_control_status": platform_timeout.get("last_control_status"),
                "last_speed_mps": platform_timeout.get("last_speed_mps"),
                "timeseries_rows": platform_timeout.get("timeseries_rows"),
                "route_s_delta_m": platform_timeout.get("route_s_delta_m"),
                "final_ego_speed_mps": platform_timeout.get("final_ego_speed_mps"),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
                "apollo_link_health_primary_blocker": link_health.get("primary_blocker"),
                "phase1_relevant_apollo_link_blocker": link_health.get(
                    "phase1_relevant_primary_blocker"
                ),
                "apollo_link_health_path": link_health.get("path"),
                "route_simple_lat_sign_convention_candidate": link_health.get(
                    "route_simple_lat_sign_convention_candidate"
                ),
                "route_lateral_field_semantics_classification": link_health.get(
                    "route_lateral_field_semantics_classification"
                ),
                "route_lateral_field_recommended_gate_policy": link_health.get(
                    "route_lateral_field_recommended_gate_policy"
                ),
                "simple_lat_station_frame_classification": link_health.get(
                    "simple_lat_station_frame_classification"
                ),
            },
        }

    if reason == "timeout" and platform_timeout.get("classification") == (
        "routing_missing_timeout"
    ):
        return {
            "primary_behavior_blocker": "routing_missing_timeout",
            "behavior_blocker_layer": "apollo_routing_materialization",
            "behavior_next_action": (
                "The timed-out Apollo run never showed routing success in online-chain "
                "progress logs. Inspect routing request send/materialization, map/route "
                "compatibility, Dreamview/Apollo routing status, and route goal validity "
                "before debugging Planning, Control, or CARLA actuation."
            ),
            "behavior_blocker_evidence": {
                "classification": platform_timeout.get("classification"),
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "max_routing_success_count": platform_timeout.get("max_routing_success_count"),
                "max_planning_message_count": platform_timeout.get("max_planning_message_count"),
                "max_control_tx_count": platform_timeout.get("max_control_tx_count"),
                "max_tick_count": platform_timeout.get("max_tick_count"),
                "last_phase": platform_timeout.get("last_phase"),
                "phase_counts": platform_timeout.get("phase_counts"),
                "timeseries_rows": platform_timeout.get("timeseries_rows"),
                "route_s_delta_m": platform_timeout.get("route_s_delta_m"),
                "final_ego_speed_mps": platform_timeout.get("final_ego_speed_mps"),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    if reason == "timeout" and platform_timeout.get("classification") == (
        "routing_available_planning_missing_timeout"
    ):
        return {
            "primary_behavior_blocker": "routing_available_planning_missing_timeout",
            "behavior_blocker_layer": "apollo_planning_materialization",
            "behavior_next_action": (
                "Routing materialized during the timed-out Apollo run, but progress logs "
                "never showed Planning output or control_tx. Inspect Planning readiness, "
                "reference-line materialization, route/lane compatibility, and Planning logs "
                "before changing control mapping, smoothing, steer scale, or CARLA actuation."
            ),
            "behavior_blocker_evidence": {
                "classification": platform_timeout.get("classification"),
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "max_routing_success_count": platform_timeout.get("max_routing_success_count"),
                "max_planning_message_count": platform_timeout.get("max_planning_message_count"),
                "max_control_tx_count": platform_timeout.get("max_control_tx_count"),
                "max_tick_count": platform_timeout.get("max_tick_count"),
                "last_phase": platform_timeout.get("last_phase"),
                "phase_counts": platform_timeout.get("phase_counts"),
                "timeseries_rows": platform_timeout.get("timeseries_rows"),
                "route_s_delta_m": platform_timeout.get("route_s_delta_m"),
                "final_ego_speed_mps": platform_timeout.get("final_ego_speed_mps"),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    return {
        "primary_behavior_blocker": reason,
        "behavior_blocker_layer": "phase1_status",
        "behavior_next_action": "Inspect scenario-specific evidence before changing runtime behavior.",
        "behavior_blocker_evidence": {},
    }


def _route_health_failure_next_action(
    nested_failure_codes: Any,
    *,
    nested_exit_reason: Any = None,
    timed_out_after_summary: bool = False,
) -> str:
    codes = {
        str(item)
        for item in (nested_failure_codes or [])
        if str(item).strip()
    }
    parts = [
        "The nested Apollo route-health run wrote a finalized failure summary.",
    ]
    if "PLANNING_NONZERO_RATIO" in codes:
        parts.append(
            "Inspect Planning non-empty trajectory ratio, path fallback, matched-point evidence, and route completion."
        )
    elif {"ROUTE_DISTANCE_ACHIEVED_M", "ROUTE_COMPLETION_RATIO"} & codes:
        parts.append(
            "Planning/control materialized; inspect why route distance or completion stayed below threshold, "
            "including mid-route stop behavior, lane-invasion timing, path fallback/matched-point evidence, "
            "route target length, and evaluation window before changing control gains or steer scale."
        )
    else:
        parts.append(
            "Inspect the finalized route-health acceptance failures, path fallback/matched-point evidence, and route completion."
        )
    if str(nested_exit_reason or "").upper() == "LANE_INVASION":
        parts.append(
            "Because the nested exit reason is LANE_INVASION, check lane-event timing, cross-track error, "
            "route/lane sign convention, and footprint/sensor evidence before treating it as a pure longitudinal failure."
        )
    if timed_out_after_summary:
        parts.append("If the platform wrapper still timed out after this summary, fix lifecycle propagation separately.")
    parts.append("Do not treat this as startup, missing Control, or natural-driving success.")
    return " ".join(parts)


def _primary_setup_blocker(
    *,
    status: str,
    reason: str | None,
    derived_blocker_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    empty = {
        "primary_setup_blocker": None,
        "setup_blocker_layer": None,
        "setup_next_action": None,
        "setup_blocker_evidence": {},
    }
    if status != "invalid" or not reason:
        return empty

    platform_timeout = (
        derived_blocker_evidence.get("platform_timeout")
        if isinstance(derived_blocker_evidence.get("platform_timeout"), Mapping)
        else {}
    )
    classification = platform_timeout.get("classification")
    if reason == "no_timeseries" and classification == (
        "apollo_startup_command_materialization_missing_timeout"
    ):
        return {
            "primary_setup_blocker": "apollo_startup_command_materialization_missing_timeout",
            "setup_blocker_layer": "apollo_runtime_startup_materialization",
            "setup_next_action": (
                "The Apollo diagnostic run timed out before root timeseries or command "
                "materialization artifacts were produced. Inspect typed runtime startup, "
                "Dreamview/adapter startup stage, routing request eligibility, and the "
                "selected diagnostic config before treating this as backend behavior. "
                "A comparable Phase 1 run needs timeseries plus routing/planning/control "
                "materialization evidence."
            ),
            "setup_blocker_evidence": {
                "classification": classification,
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "last_phase": platform_timeout.get("last_phase"),
                "phase_counts": platform_timeout.get("phase_counts"),
                "max_routing_success_count": platform_timeout.get("max_routing_success_count"),
                "max_planning_message_count": platform_timeout.get("max_planning_message_count"),
                "max_control_tx_count": platform_timeout.get("max_control_tx_count"),
                "timeseries_rows": platform_timeout.get("timeseries_rows"),
                "startup_stage": platform_timeout.get("startup_stage"),
                "startup_stage_path": platform_timeout.get("startup_stage_path"),
                "typed_transition_entrypoint": platform_timeout.get(
                    "typed_transition_entrypoint"
                ),
                "typed_transition_path": platform_timeout.get("typed_transition_path"),
                "overlay_active": platform_timeout.get("overlay_active"),
                "overlay_selected_count": platform_timeout.get("overlay_selected_count"),
                "overlay_missing_source_dirs": platform_timeout.get(
                    "overlay_missing_source_dirs"
                ),
                "overlay_manifest_path": platform_timeout.get("overlay_manifest_path"),
                "routing_process_seen": platform_timeout.get("routing_process_seen"),
                "prediction_process_seen": platform_timeout.get("prediction_process_seen"),
                "planning_process_seen": platform_timeout.get("planning_process_seen"),
                "control_process_seen": platform_timeout.get("control_process_seen"),
                "apollo_modules_start_log_path": platform_timeout.get(
                    "apollo_modules_start_log_path"
                ),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    if reason == "no_timeseries" and platform_timeout.get("available"):
        return {
            "primary_setup_blocker": classification or "apollo_runtime_no_timeseries",
            "setup_blocker_layer": "apollo_runtime_artifact_materialization",
            "setup_next_action": (
                "The run is invalid because root timeseries is missing, but Apollo "
                "runtime evidence exists. Inspect runtime_stdout, nested startup artifacts, "
                "and artifact normalization before counting this as backend behavior."
            ),
            "setup_blocker_evidence": {
                "classification": classification,
                "platform_status": platform_timeout.get("platform_status"),
                "exit_code": platform_timeout.get("exit_code"),
                "runtime_exit_code": platform_timeout.get("runtime_exit_code"),
                "last_phase": platform_timeout.get("last_phase"),
                "max_routing_success_count": platform_timeout.get("max_routing_success_count"),
                "max_planning_message_count": platform_timeout.get("max_planning_message_count"),
                "max_control_tx_count": platform_timeout.get("max_control_tx_count"),
                "runtime_stdout_path": platform_timeout.get("runtime_stdout_path"),
                "platform_execution_path": platform_timeout.get("platform_execution_path"),
            },
        }

    return empty


def collect_derived_blocker_evidence(root: str | Path) -> dict[str, Any]:
    """Return current blocker evidence from optional Phase 1 analysis reports."""

    return _derived_blocker_evidence(Path(root))


def _route_lateral_field_policy(derived_blocker_evidence: Mapping[str, Any]) -> dict[str, Any]:
    link_health = (
        derived_blocker_evidence.get("apollo_link_health")
        if isinstance(derived_blocker_evidence.get("apollo_link_health"), Mapping)
        else {}
    )
    semantics_status = link_health.get("route_lateral_field_semantics_status")
    classification = link_health.get("route_lateral_field_semantics_classification")
    if not semantics_status and not classification:
        return {
            "status": "not_applicable",
            "policy": "route_lateral_field_policy_not_available",
            "source_field": None,
            "sign_sensitive_gate_allowed": None,
            "absolute_magnitude_gate_allowed": None,
            "recommended_action": "generate_apollo_lateral_semantics_and_link_health_reports",
            "claim_boundary": (
                "Route-lateral field policy is an analysis/gating contract only. "
                "Missing policy must not be interpreted as behavior success."
            ),
        }

    sign_sensitive_allowed = link_health.get("route_lateral_field_sign_sensitive_gate_allowed")
    magnitude_allowed = link_health.get("route_lateral_field_absolute_magnitude_gate_allowed")
    if sign_sensitive_allowed is False and magnitude_allowed is True:
        policy = "exclude_from_sign_sensitive_behavior_gates"
    elif sign_sensitive_allowed is True:
        policy = "signed_and_absolute_route_lateral_allowed"
    elif magnitude_allowed is True:
        policy = "absolute_magnitude_only"
    else:
        policy = "do_not_use_route_lateral_field_for_behavior_gate"

    return {
        "status": semantics_status or "unknown",
        "policy": policy,
        "source_field": link_health.get("route_lateral_field_semantics_source_field")
        or link_health.get("projection_route_sample_lateral_source_field"),
        "classification": classification,
        "sign_sensitive_gate_allowed": sign_sensitive_allowed,
        "absolute_magnitude_gate_allowed": magnitude_allowed,
        "recommended_gate_policy": link_health.get("route_lateral_field_recommended_gate_policy"),
        "recommended_action": link_health.get("route_lateral_field_recommended_action"),
        "projection_route_sample_sign_contract_classification": link_health.get(
            "projection_route_sample_sign_contract_classification"
        ),
        "projection_route_sample_timeseries_opposite_sign_ratio": link_health.get(
            "projection_route_sample_timeseries_opposite_sign_ratio"
        ),
        "projection_route_sample_simple_lat_same_sign_ratio": link_health.get(
            "projection_route_sample_simple_lat_same_sign_ratio"
        ),
        "claim_boundary": (
            "This policy applies only to Phase 1 analysis/gating semantics. It does not "
            "change runtime control, does not prove Apollo behavior success, and must not "
            "be used to flip control signs."
        ),
    }


def _reference_line_debug_export_policy(
    derived_blocker_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    link_health = (
        derived_blocker_evidence.get("apollo_link_health")
        if isinstance(derived_blocker_evidence.get("apollo_link_health"), Mapping)
        else {}
    )
    status = link_health.get("reference_line_debug_export_policy_status")
    classification = link_health.get("reference_line_debug_export_policy_classification")
    if not status and not classification:
        return {
            "status": "not_applicable",
            "policy": "reference_line_debug_export_policy_not_available",
            "classification": None,
            "reference_line_debug_claim_grade_allowed": None,
            "local_surrogate_available": None,
            "recommended_action": "generate_apollo_reference_line_contract_and_link_health_reports",
            "claim_boundary": (
                "Missing reference-line export policy must not be interpreted as "
                "reference-line success or behavior success."
            ),
        }

    claim_grade_allowed = link_health.get("reference_line_debug_claim_grade_allowed")
    planning_local = link_health.get("planning_first_point_local_alignment_available")
    trajectory_surrogate = link_health.get("planning_trajectory_sample_surrogate_available")
    control_reference = link_health.get("control_simple_lat_reference_available")
    local_surrogate_available = bool(planning_local or trajectory_surrogate or control_reference)
    presence_classification = link_health.get("planning_debug_presence_classification")
    default_recommended_action = link_health.get("reference_line_recommended_next_action")
    if presence_classification == "routing_present_reference_line_empty":
        if link_health.get("planning_info_log_reference_line_available") is True:
            recommended_action = (
                "Planning debug path is present and routing segments are present, but "
                "debug.planning_data.reference_line is empty while apollo_planning.INFO "
                "contains internal reference-line text traces. Inspect ADCTrajectory debug "
                "reference-line population/export and Planning ReferenceLineInfo debug content "
                "before changing steer scale, control mapping, smoothing, PID, or actuation mapping."
            )
        else:
            recommended_action = (
                "Planning debug path is present and routing segments are present, but "
                "debug.planning_data.reference_line is empty. Inspect Apollo Planning "
                "reference-line materialization/config/debug content for this route before "
                "changing steer scale, control mapping, smoothing, PID, or actuation mapping."
            )
    else:
        recommended_action = default_recommended_action
    if claim_grade_allowed is True:
        policy = "reference_line_debug_can_support_claim_if_other_gates_pass"
    elif local_surrogate_available:
        policy = "local_surrogate_only_until_reference_line_debug_exported"
    else:
        policy = "insufficient_reference_line_debug_evidence"

    return {
        "status": status or "unknown",
        "policy": policy,
        "classification": classification,
        "field_gap_classification": link_health.get("reference_line_field_gap_classification"),
        "reference_line_count_positive_count": link_health.get(
            "reference_line_count_positive_count"
        ),
        "claim_window_source": link_health.get("reference_line_claim_window_source"),
        "claim_window_reference_line_count_positive_count": link_health.get(
            "reference_line_claim_window_reference_line_count_positive_count"
        ),
        "claim_window_reference_line_provider_status_topk": link_health.get(
            "reference_line_claim_window_provider_status_topk"
        ),
        "planning_route_segment_count_positive_count": link_health.get(
            "planning_route_segment_count_positive_count"
        ),
        "routing_segment_count_positive_count": link_health.get(
            "routing_segment_count_positive_count"
        ),
        "routing_road_count_positive_count": link_health.get(
            "routing_road_count_positive_count"
        ),
        "trajectory_sample_rows": link_health.get("reference_line_trajectory_sample_rows"),
        "control_target_point_rows": link_health.get("reference_line_control_target_point_rows"),
        "planning_debug_presence_classification": presence_classification,
        "planning_debug_reference_line_path": link_health.get(
            "planning_debug_presence_last_reference_line_path"
        ),
        "planning_debug_routing_path": link_health.get("planning_debug_presence_last_routing_path"),
        "planning_debug_reference_line_nonempty_ratio": link_health.get(
            "planning_debug_presence_reference_line_nonempty_ratio"
        ),
        "planning_debug_routing_segment_nonempty_ratio": link_health.get(
            "planning_debug_presence_routing_segment_nonempty_ratio"
        ),
        "planning_materialization_classification": link_health.get(
            "planning_materialization_classification"
        ),
        "planning_materialization_claim_window_source": link_health.get(
            "planning_materialization_claim_window_source"
        ),
        "planning_materialization_route_segments_ready_ratio": link_health.get(
            "planning_materialization_route_segments_ready_ratio"
        ),
        "planning_materialization_reference_line_empty_while_ready_ratio": link_health.get(
            "planning_materialization_reference_line_empty_while_ready_ratio"
        ),
        "planning_materialization_lane_follow_stage_ratio": link_health.get(
            "planning_materialization_lane_follow_stage_ratio"
        ),
        "reference_line_debug_field_state": link_health.get("reference_line_debug_field_state"),
        "planning_info_log_reference_line_classification": link_health.get(
            "planning_info_log_reference_line_classification"
        ),
        "planning_info_log_reference_line_available": link_health.get(
            "planning_info_log_reference_line_available"
        ),
        "planning_info_log_reference_line_claim_grade_allowed": link_health.get(
            "planning_info_log_reference_line_claim_grade_allowed"
        ),
        "reference_line_debug_claim_grade_allowed": claim_grade_allowed,
        "local_surrogate_available": local_surrogate_available,
        "planning_first_point_local_alignment_available": planning_local,
        "planning_trajectory_sample_surrogate_available": trajectory_surrogate,
        "control_simple_lat_reference_available": control_reference,
        "route_segment_available": link_health.get("reference_line_route_segment_available"),
        "recommended_evidence_policy": link_health.get("reference_line_recommended_evidence_policy"),
        "recommended_action": recommended_action,
        "reason": link_health.get("reference_line_debug_export_policy_reason"),
        "claim_boundary": (
            "This policy is Phase 1 reference-line attribution evidence. Local Planning/control "
            "surrogates can narrow diagnosis, but they do not replace exported Planning "
            "reference-line debug and do not prove behavior success."
        ),
    }


def _path_candidate_control_context(
    derived_blocker_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    link_health = (
        derived_blocker_evidence.get("apollo_link_health")
        if isinstance(derived_blocker_evidence.get("apollo_link_health"), Mapping)
        else {}
    )
    target_classification = link_health.get("control_target_point_vs_path_candidate_classification")
    trajectory_classification = link_health.get(
        "planning_debug_path_candidate_vs_trajectory_sample_classification"
    )
    hdmap_classification = link_health.get(
        "planning_debug_path_candidate_hdmap_projection_classification"
    )
    if not any((target_classification, trajectory_classification, hdmap_classification)):
        return {
            "status": "not_applicable",
            "recommended_action": "generate_apollo_reference_line_contract_and_link_health_reports",
            "claim_boundary": (
                "Missing path-candidate context must not be interpreted as Planning "
                "path/reference-line success."
            ),
        }

    claim_flags = [
        link_health.get("control_target_point_vs_path_candidate_reference_line_claim_grade_allowed"),
        link_health.get("planning_debug_path_candidate_vs_trajectory_sample_reference_line_claim_grade_allowed"),
        link_health.get("planning_debug_path_candidate_hdmap_projection_reference_line_claim_grade_allowed"),
    ]
    explicit_false = any(item is False for item in claim_flags)
    explicit_true = any(item is True for item in claim_flags)
    if explicit_false:
        claim_grade_allowed = False
    elif explicit_true:
        claim_grade_allowed = True
    else:
        claim_grade_allowed = None

    status = "diagnostic_only" if claim_grade_allowed is False else "available"
    recommended_policy = (
        "path_candidate_surrogate_only_until_reference_line_debug_exported"
        if claim_grade_allowed is False
        else "path_candidate_context_available"
    )
    return {
        "status": status,
        "control_target_vs_path_candidate_classification": target_classification,
        "target_point_to_path_candidate_line_abs_p95_m": link_health.get(
            "control_target_point_vs_path_candidate_p95_m"
        ),
        "target_point_lane_l_abs_p95_m": link_health.get(
            "control_target_point_vs_path_candidate_target_lane_l_abs_p95_m"
        ),
        "target_inside_path_lateral_envelope": link_health.get(
            "control_target_point_inside_path_candidate_lateral_envelope"
        ),
        "path_candidate_lane_l_abs_p95_m": link_health.get(
            "planning_debug_path_candidate_hdmap_projection_lane_l_abs_p95_m"
        ),
        "path_candidate_lane_l_min_m": link_health.get("path_candidate_lane_l_min_m"),
        "path_candidate_lane_l_max_m": link_health.get("path_candidate_lane_l_max_m"),
        "planning_debug_path_candidate_vs_trajectory_sample_classification": (
            trajectory_classification
        ),
        "path_candidate_to_planning_sample_line_abs_p95_m": link_health.get(
            "planning_debug_path_candidate_vs_trajectory_sample_p95_m"
        ),
        "path_candidate_vs_trajectory_sample_coverage_ratio": link_health.get(
            "planning_debug_path_candidate_vs_trajectory_sample_coverage_ratio"
        ),
        "planning_debug_path_candidate_hdmap_projection_classification": hdmap_classification,
        "path_candidate_routing_lane_window_compatible": link_health.get(
            "planning_debug_path_candidate_hdmap_projection_routing_lane_window_compatible"
        ),
        "reference_line_claim_grade_allowed": claim_grade_allowed,
        "recommended_evidence_policy": recommended_policy,
        "recommended_action": (
            "Use this context to decide whether Planning path candidates and Control target "
            "points are locally consistent. It is not a substitute for exported Planning "
            "reference-line debug."
        ),
        "claim_boundary": (
            "Phase 1 path-candidate context is diagnostic-only unless exported Planning "
            "reference-line debug is present and claim-grade."
        ),
    }


def _platform_timeout_blocker_summary(root: Path) -> dict[str, Any]:
    platform_path = root / "platform_execution_result.json"
    platform = _read_json(platform_path)
    progress = _online_chain_progress_summary(root)
    runtime_artifacts = _apollo_timeout_runtime_artifact_summary(root)
    motion = _timeseries_timeout_motion_summary(root)
    status = str(platform.get("status") or "").strip()
    exit_code = platform.get("exit_code")
    runtime_exit_code = platform.get("runtime_exit_code")
    timeout_observed = status == "timeout" or exit_code == -2 or runtime_exit_code == -2
    available = bool(
        platform
        or progress.get("available")
        or runtime_artifacts.get("available")
        or motion.get("timeseries_rows")
    )

    max_routing = progress.get("max_routing_success_count")
    max_planning = progress.get("max_planning_message_count")
    max_control_tx = progress.get("max_control_tx_count")
    nested_summary_finalized = runtime_artifacts.get("nested_summary_status") == "finalized"
    if (
        timeout_observed
        and not motion.get("timeseries_rows")
        and progress.get("available")
        and _zero_number(max_routing)
        and not runtime_artifacts.get("command_materialization_summary_path")
        and (
            runtime_artifacts.get("startup_stage_path")
            or runtime_artifacts.get("typed_transition_path")
            or runtime_artifacts.get("overlay_manifest_path")
        )
    ):
        classification = "apollo_startup_command_materialization_missing_timeout"
    elif timeout_observed and (
        _positive_number(runtime_artifacts.get("runtime_routing_success_count"))
        and _positive_number(runtime_artifacts.get("planning_nonempty_trajectory_count"))
        and not _positive_number(runtime_artifacts.get("runtime_control_tx_count"))
        and runtime_artifacts.get("control_crash_detected") is True
    ):
        classification = "planning_available_control_process_crash_timeout"
    elif timeout_observed and (
        _positive_number(runtime_artifacts.get("runtime_routing_success_count"))
        and not _positive_number(runtime_artifacts.get("runtime_control_tx_count"))
        and runtime_artifacts.get("planning_debug_presence_classification")
        == "routing_present_reference_line_empty"
    ):
        classification = "routing_available_reference_line_empty_control_missing_timeout"
    elif (
        timeout_observed
        and _zero_number(max_routing)
        and not _positive_number(max_planning)
        and not _positive_number(max_control_tx)
    ):
        classification = "routing_missing_timeout"
    elif timeout_observed and _positive_number(max_routing) and not _positive_number(max_planning) and not _positive_number(max_control_tx):
        classification = "routing_available_planning_missing_timeout"
    elif timeout_observed and nested_summary_finalized:
        classification = "finalized_route_health_failure_platform_wrapper_timeout"
    elif nested_summary_finalized and runtime_artifacts.get("nested_summary_success") is False:
        classification = "finalized_route_health_failure"
    elif (
        timeout_observed
        and _positive_number(max_routing)
        and _positive_number(max_planning)
        and _positive_number(max_control_tx)
    ):
        classification = "control_output_present_route_chain_not_finalized_timeout"
    elif timeout_observed and progress.get("available"):
        classification = "timeout_with_progress_log"
    elif timeout_observed:
        classification = "timeout_without_progress_log"
    else:
        classification = "not_applicable"

    return {
        "available": available,
        "classification": classification,
        "platform_status": status or None,
        "exit_code": exit_code,
        "runtime_exit_code": runtime_exit_code,
        "platform_execution_path": str(platform_path) if platform_path.exists() else None,
        **progress,
        **runtime_artifacts,
        **motion,
        "claim_boundary": (
            "Platform-timeout blocker evidence narrows Phase 1 attribution only. It does not "
            "prove backend behavior success and does not override failed/invalid run status."
        ),
    }


def _apollo_timeout_runtime_artifact_summary(root: Path) -> dict[str, Any]:
    cyber_path = _first_nested_artifact(root, "cyber_bridge_stats.json")
    planning_path = _first_nested_artifact(root, "planning_topic_debug_summary.json")
    command_path = _first_nested_artifact(root, "command_materialization_summary.json")
    survival_path = _first_nested_artifact(root, "apollo_control_deferred_survival.json")
    control_log_path = _first_nested_artifact(root, "apollo_control_deferred_mainboard.log")
    startup_stage_path = _first_nested_artifact(root, "startup_stage.json")
    overlay_path = _first_nested_artifact(root, "apollo_control_runtime_overlay_manifest.json")
    typed_transition_path = _first_nested_artifact(root, "typed_transition_runtime.json")
    modules_start_log_path = _first_nested_artifact(root, "apollo_modules_start.log")
    nested_summary_path = _first_nested_summary(root)
    cyber = _read_json(cyber_path) if cyber_path else {}
    planning = _read_json(planning_path) if planning_path else {}
    command = _read_json(command_path) if command_path else {}
    survival = _read_json(survival_path) if survival_path else {}
    startup_stage = _read_json(startup_stage_path) if startup_stage_path else {}
    overlay = _read_json(overlay_path) if overlay_path else {}
    typed_transition = _read_json(typed_transition_path) if typed_transition_path else {}
    nested_summary = _read_json(nested_summary_path) if nested_summary_path else {}
    control_crash = _control_deferred_log_crash_summary(control_log_path)
    module_processes = _apollo_modules_start_process_summary(modules_start_log_path)
    planning_presence = (
        planning.get("planning_debug_presence")
        if isinstance(planning.get("planning_debug_presence"), Mapping)
        else {}
    )
    gate_state = command.get("gate_state") if isinstance(command.get("gate_state"), Mapping) else {}
    observed = (
        command.get("observed_counters")
        if isinstance(command.get("observed_counters"), Mapping)
        else {}
    )
    routing_request_count = cyber.get("routing_request_count", observed.get("routing_request_count"))
    routing_success_count = cyber.get("routing_success_count", observed.get("routing_success_count"))
    control_tx_count = cyber.get("control_tx_count", observed.get("control_tx_count"))
    return {
        "available": bool(cyber or planning or command),
        "cyber_bridge_stats_path": str(cyber_path) if cyber_path else None,
        "planning_topic_debug_summary_path": str(planning_path) if planning_path else None,
        "command_materialization_summary_path": str(command_path) if command_path else None,
        "control_deferred_survival_path": str(survival_path) if survival_path else None,
        "control_deferred_mainboard_log_path": str(control_log_path) if control_log_path else None,
        "startup_stage_path": str(startup_stage_path) if startup_stage_path else None,
        "startup_stage": startup_stage.get("stage"),
        "startup_stage_step": startup_stage.get("step"),
        "overlay_manifest_path": str(overlay_path) if overlay_path else None,
        "overlay_active": overlay.get("overlay_active"),
        "overlay_selected_count": overlay.get("selected_count"),
        "overlay_missing_source_dirs": overlay.get("missing_source_dirs"),
        "typed_transition_path": str(typed_transition_path) if typed_transition_path else None,
        "typed_transition_entrypoint": typed_transition.get("transition_entrypoint"),
        "typed_transition_command": typed_transition.get("command"),
        "apollo_modules_start_log_path": str(modules_start_log_path) if modules_start_log_path else None,
        **module_processes,
        "nested_summary_path": str(nested_summary_path) if nested_summary_path else None,
        "nested_summary_status": nested_summary.get("summary_status"),
        "nested_summary_success": nested_summary.get("success"),
        "nested_finalized_from_event_stream": nested_summary.get("finalized_from_event_stream"),
        "nested_exit_reason": nested_summary.get("exit_reason"),
        "nested_fail_reason": nested_summary.get("fail_reason"),
        "nested_failure_codes": _nested_failure_codes(nested_summary),
        "nested_route_health_label": nested_summary.get("route_health_label"),
        "nested_route_distance_achieved_m": _acceptance_check_actual(
            nested_summary, "route_distance_achieved_m"
        ),
        "nested_route_completion_ratio": _acceptance_check_actual(
            nested_summary, "route_completion_ratio"
        ),
        "runtime_routing_request_count": routing_request_count,
        "runtime_routing_success_count": routing_success_count,
        "runtime_routing_response_count": cyber.get("routing_response_count"),
        "runtime_control_rx_count": cyber.get("control_rx_count", observed.get("control_rx_count")),
        "runtime_control_tx_count": control_tx_count,
        "planning_nonempty_trajectory_count": planning.get(
            "messages_with_nonzero_trajectory_points"
        ),
        "planning_debug_presence_classification": planning_presence.get("last_diagnosis"),
        "planning_debug_reference_line_nonempty_ratio": planning_presence.get(
            "reference_line_nonempty_ratio"
        ),
        "planning_debug_routing_segment_nonempty_ratio": planning_presence.get(
            "routing_segment_nonempty_ratio"
        ),
        "command_last_blocking_reason": gate_state.get("last_blocking_reason"),
        "command_last_phase": gate_state.get("last_phase"),
        "command_first_ready_to_send_ts_sec": gate_state.get("first_ready_to_send_ts_sec"),
        "command_last_ready_to_send_ts_sec": gate_state.get("last_ready_to_send_ts_sec"),
        "command_last_error": command.get("last_error"),
        "control_started_pid_seen": survival.get("control_started_pid_seen"),
        "control_survived_5s": survival.get("control_survived_5s"),
        "control_survived_10s": survival.get("control_survived_10s"),
        "control_present_after_first_nonzero_planning": survival.get(
            "control_present_after_first_nonzero_planning"
        ),
        "control_present_at_end": survival.get("control_present_at_end"),
        **control_crash,
    }


def _control_deferred_log_crash_summary(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {
            "control_crash_detected": False,
            "control_crash_reason": None,
            "control_crash_signature": None,
        }
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {
            "control_crash_detected": False,
            "control_crash_reason": None,
            "control_crash_signature": None,
        }
    lowered = text.lower()
    if "attempt to free invalid pointer" in lowered:
        return {
            "control_crash_detected": True,
            "control_crash_reason": "tcmalloc_invalid_free",
            "control_crash_signature": _first_matching_line(
                text,
                ("Attempt to free invalid pointer", "attempt to free invalid pointer"),
            ),
        }
    if "segmentation fault" in lowered or "check failed" in lowered or "fatal" in lowered:
        return {
            "control_crash_detected": True,
            "control_crash_reason": "control_process_crash",
            "control_crash_signature": _first_matching_line(
                text,
                ("Segmentation fault", "Check failed", "FATAL", "fatal"),
            ),
        }
    return {
        "control_crash_detected": False,
        "control_crash_reason": None,
        "control_crash_signature": None,
    }


def _apollo_modules_start_process_summary(path: Path | None) -> dict[str, Any]:
    empty = {
        "routing_process_seen": None,
        "prediction_process_seen": None,
        "planning_process_seen": None,
        "control_process_seen": None,
    }
    if path is None or not path.exists():
        return empty
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return empty
    if "--- stdout ---" in text:
        text = text.split("--- stdout ---", 1)[1]
        if "--- stderr ---" in text:
            text = text.split("--- stderr ---", 1)[0]
    return {
        "routing_process_seen": "modules/routing/dag/routing.dag" in text,
        "prediction_process_seen": "modules/prediction/dag/prediction.dag" in text,
        "planning_process_seen": "modules/planning/planning_component/dag/planning.dag" in text,
        "control_process_seen": "modules/control/control_component/dag/control.dag" in text,
    }


def _first_matching_line(text: str, needles: tuple[str, ...]) -> str | None:
    lowered_needles = tuple(needle.lower() for needle in needles)
    for line in text.splitlines():
        lowered = line.lower()
        if any(needle in lowered for needle in lowered_needles):
            return line.strip()
    return None


def _first_nested_artifact(root: Path, name: str) -> Path | None:
    preferred = root / "artifacts" / name
    if preferred.exists():
        return preferred
    matches = sorted(
        path for path in root.rglob(name) if "analysis" not in path.parts and "execution" not in path.parts
    )
    return matches[0] if matches else None


def _first_nested_summary(root: Path) -> Path | None:
    matches = sorted(
        path
        for path in root.rglob("summary.json")
        if path != root / "summary.json"
        and "analysis" not in path.parts
        and "execution" not in path.parts
    )
    finalized: list[Path] = []
    for path in matches:
        payload = _read_json(path)
        if payload.get("summary_status") == "finalized":
            finalized.append(path)
    if finalized:
        return finalized[0]
    return matches[0] if matches else None


def _nested_failure_codes(summary: Mapping[str, Any]) -> list[str]:
    acceptance = summary.get("acceptance") if isinstance(summary.get("acceptance"), Mapping) else {}
    codes = acceptance.get("failure_codes")
    if isinstance(codes, list):
        return [str(code) for code in codes]
    return []


def _acceptance_check_actual(summary: Mapping[str, Any], check_name: str) -> Any:
    acceptance = summary.get("acceptance") if isinstance(summary.get("acceptance"), Mapping) else {}
    checks = acceptance.get("checks") if isinstance(acceptance.get("checks"), Mapping) else {}
    check = checks.get(check_name) if isinstance(checks.get(check_name), Mapping) else {}
    return check.get("actual")


def _online_chain_progress_summary(root: Path) -> dict[str, Any]:
    path = _first_existing_path(
        (
            root / "execution" / "runtime_stdout.log",
            root / "runtime_stdout.log",
        )
    )
    if path is None:
        return {"available": False, "runtime_stdout_path": None}
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return {"available": False, "runtime_stdout_path": str(path)}

    phase_counts: dict[str, int] = {}
    max_routing: int | None = None
    max_planning: int | None = None
    max_control_tx: int | None = None
    max_ticks: int | None = None
    last_phase: str | None = None
    last_route_status: str | None = None
    last_summary_status: str | None = None
    last_control_status: str | None = None
    last_speed_mps: float | None = None
    progress_line_count = 0
    for line in text.splitlines():
        if "[online-chain][progress]" not in line:
            continue
        progress_line_count += 1
        tokens = _key_value_tokens(line)
        phase = tokens.get("phase")
        if phase:
            last_phase = phase
            phase_counts[phase] = phase_counts.get(phase, 0) + 1
        max_routing = _max_optional_int(max_routing, _int_token(tokens.get("routing")))
        max_planning = _max_optional_int(max_planning, _int_token(tokens.get("planning")))
        max_control_tx = _max_optional_int(max_control_tx, _int_token(tokens.get("control_tx")))
        max_ticks = _max_optional_int(max_ticks, _ticks_token(tokens.get("ticks")))
        last_route_status = tokens.get("route") or last_route_status
        last_summary_status = tokens.get("summary") or last_summary_status
        last_control_status = tokens.get("control") or last_control_status
        speed_mps = _float_token(tokens.get("speed"), suffix="mps")
        if speed_mps is not None:
            last_speed_mps = speed_mps

    return {
        "available": progress_line_count > 0,
        "runtime_stdout_path": str(path),
        "progress_line_count": progress_line_count,
        "phase_counts": phase_counts,
        "last_phase": last_phase,
        "last_route_status": last_route_status,
        "last_summary_status": last_summary_status,
        "last_control_status": last_control_status,
        "last_speed_mps": last_speed_mps,
        "max_routing_success_count": max_routing,
        "max_planning_message_count": max_planning,
        "max_control_tx_count": max_control_tx,
        "max_tick_count": max_ticks,
    }


def _timeseries_timeout_motion_summary(root: Path) -> dict[str, Any]:
    rows = _read_timeseries_rows(root)
    if not rows:
        return {
            "timeseries_rows": 0,
            "route_s_delta_m": None,
            "final_ego_speed_mps": None,
            "control_source_counts": {},
        }
    route_s = _series_from_aliases(rows, ("route_s", "ego_route_s", "route_progress_m"))[1]
    speed = _series_from_aliases(rows, ("ego_speed_mps", "ego_speed", "v_mps"))[1]
    route_s_delta = None
    if len(route_s) >= 2:
        route_s_delta = route_s[-1] - route_s[0]
    return {
        "timeseries_rows": len(rows),
        "route_s_delta_m": route_s_delta,
        "final_ego_speed_mps": speed[-1] if speed else None,
        "control_source_counts": _value_counts(rows, "control_source"),
    }


def _first_existing_path(candidates: tuple[Path, ...]) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return None


def _key_value_tokens(line: str) -> dict[str, str]:
    tokens: dict[str, str] = {}
    for item in line.split():
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        tokens[key.strip()] = value.strip()
    return tokens


def _int_token(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _ticks_token(value: str | None) -> int | None:
    if not value:
        return None
    return _int_token(value.split("/", 1)[0])


def _float_token(value: str | None, *, suffix: str = "") -> float | None:
    if value is None:
        return None
    text = value
    if suffix and text.endswith(suffix):
        text = text[: -len(suffix)]
    try:
        return float(text)
    except ValueError:
        return None


def _max_optional_int(left: int | None, right: int | None) -> int | None:
    if right is None:
        return left
    if left is None:
        return right
    return max(left, right)


def _positive_number(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _zero_number(value: Any) -> bool:
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _control_health_blocker_summary(report: Mapping[str, Any], path: Path) -> dict[str, Any]:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    materialization = (
        metrics.get("materialization_evidence")
        if isinstance(metrics.get("materialization_evidence"), Mapping)
        else {}
    )
    control_semantics = (
        report.get("control_semantics_evidence")
        if isinstance(report.get("control_semantics_evidence"), Mapping)
        else {}
    )
    return {
        "available": bool(report),
        "path": str(path) if path.exists() else None,
        "status": report.get("status"),
        "failure_reason": report.get("failure_reason"),
        "control_semantics_primary_factor": report.get("control_semantics_primary_factor")
        or control_semantics.get("primary_factor"),
        "control_semantics_suspected_factors": list(
            report.get("control_semantics_suspected_factors")
            or control_semantics.get("suspected_factors")
            or []
        ),
        "routing_materialized": report.get("routing_materialized"),
        "planning_materialized": report.get("planning_materialized"),
        "control_handoff_status": report.get("control_handoff_status"),
        "materialization_warnings": list(materialization.get("warnings") or []),
        "warnings": list(report.get("warnings") or []),
    }


def _apollo_link_health_blocker_summary(report: Mapping[str, Any], path: Path) -> dict[str, Any]:
    lateral_metrics = _lookup_path(report, ("layers", "apollo_lateral_semantics", "key_metrics"))
    lateral_metrics = lateral_metrics if isinstance(lateral_metrics, Mapping) else {}
    reference_metrics = _lookup_path(report, ("layers", "planning_reference_line", "key_metrics"))
    reference_metrics = reference_metrics if isinstance(reference_metrics, Mapping) else {}
    export_policy = (
        reference_metrics.get("reference_line_debug_export_policy")
        if isinstance(reference_metrics.get("reference_line_debug_export_policy"), Mapping)
        else {}
    )
    reference_debug_diagnostic = (
        reference_metrics.get("reference_debug_diagnostic")
        if isinstance(reference_metrics.get("reference_debug_diagnostic"), Mapping)
        else {}
    )
    materialization_summary = (
        reference_metrics.get("planning_materialization_summary")
        if isinstance(reference_metrics.get("planning_materialization_summary"), Mapping)
        else {}
    )
    planning_info_log_evidence = (
        reference_metrics.get("planning_info_log_reference_line_evidence")
        if isinstance(reference_metrics.get("planning_info_log_reference_line_evidence"), Mapping)
        else {}
    )
    materialization_claim_window = (
        materialization_summary.get("claim_window")
        if isinstance(materialization_summary.get("claim_window"), Mapping)
        else {}
    )
    reference_field_inventory = (
        reference_debug_diagnostic.get("field_inventory")
        if isinstance(reference_debug_diagnostic.get("field_inventory"), Mapping)
        else {}
    )
    reference_claim_window_inventory = (
        reference_field_inventory.get("claim_window")
        if isinstance(reference_field_inventory.get("claim_window"), Mapping)
        else {}
    )
    control_target_path_candidate = (
        reference_metrics.get("control_target_point_vs_planning_path_candidate_sample")
        if isinstance(reference_metrics.get("control_target_point_vs_planning_path_candidate_sample"), Mapping)
        else {}
    )
    path_candidate_trajectory_surrogate = (
        reference_metrics.get("planning_debug_path_candidate_vs_trajectory_sample")
        if isinstance(reference_metrics.get("planning_debug_path_candidate_vs_trajectory_sample"), Mapping)
        else {}
    )
    path_candidate_hdmap_alignment = (
        reference_metrics.get("planning_debug_path_candidate_hdmap_projection_alignment")
        if isinstance(reference_metrics.get("planning_debug_path_candidate_hdmap_projection_alignment"), Mapping)
        else {}
    )
    phase1_relevant_primary_blocker = _phase1_relevant_apollo_link_blocker(
        report=report,
        lateral_metrics=lateral_metrics,
        reference_metrics=reference_metrics,
        export_policy=export_policy,
    )
    return {
        "available": bool(report),
        "path": str(path) if path.exists() else None,
        "primary_blocker": report.get("primary_blocker"),
        "phase1_relevant_primary_blocker": phase1_relevant_primary_blocker,
        "secondary_blockers": list(report.get("secondary_blockers") or []),
        "can_claim_unassisted_natural_driving": report.get("can_claim_unassisted_natural_driving"),
        "reference_line_debug_export_policy_status": export_policy.get("status"),
        "reference_line_debug_export_policy_classification": export_policy.get("classification"),
        "reference_line_field_gap_classification": reference_field_inventory.get(
            "field_gap_classification"
        ),
        "reference_line_count_positive_count": reference_field_inventory.get(
            "reference_line_count_positive_count"
        ),
        "reference_line_claim_window_source": reference_field_inventory.get(
            "claim_window_source"
        ),
        "reference_line_claim_window_reference_line_count_positive_count": reference_claim_window_inventory.get(
            "reference_line_count_positive_count"
        ),
        "reference_line_claim_window_provider_status_topk": reference_claim_window_inventory.get(
            "reference_line_provider_status_topk"
        ),
        "planning_route_segment_count_positive_count": reference_field_inventory.get(
            "planning_route_segment_count_positive_count"
        ),
        "routing_segment_count_positive_count": reference_field_inventory.get(
            "routing_segment_count_positive_count"
        ),
        "routing_road_count_positive_count": reference_field_inventory.get(
            "routing_road_count_positive_count"
        ),
        "reference_line_trajectory_sample_rows": reference_field_inventory.get(
            "trajectory_sample_rows"
        ),
        "reference_line_control_target_point_rows": reference_field_inventory.get(
            "control_target_point_rows"
        ),
        "reference_line_debug_claim_grade_allowed": export_policy.get(
            "reference_line_debug_claim_grade_allowed"
        ),
        "planning_first_point_local_alignment_available": export_policy.get(
            "planning_first_point_local_alignment_available"
        ),
        "planning_trajectory_sample_surrogate_available": export_policy.get(
            "planning_trajectory_sample_surrogate_available"
        ),
        "control_simple_lat_reference_available": export_policy.get(
            "control_simple_lat_reference_available"
        ),
        "reference_line_route_segment_available": export_policy.get("route_segment_available"),
        "reference_line_recommended_evidence_policy": export_policy.get(
            "recommended_evidence_policy"
        ),
        "reference_line_recommended_next_action": export_policy.get("recommended_next_action"),
        "reference_line_debug_export_policy_reason": export_policy.get("reason"),
        "reference_line_debug_field_state": export_policy.get("reference_line_debug_field_state"),
        "planning_info_log_reference_line_classification": planning_info_log_evidence.get(
            "classification"
        ),
        "planning_info_log_reference_line_available": planning_info_log_evidence.get(
            "reference_line_text_prints_present"
        ),
        "planning_info_log_reference_line_claim_grade_allowed": planning_info_log_evidence.get(
            "text_log_reference_line_claim_grade_allowed"
        ),
        "planning_debug_presence_classification": (
            reference_metrics.get("planning_debug_presence_classification")
            or reference_metrics.get("planning_debug_presence_last_diagnosis")
        ),
        "planning_debug_presence_last_reference_line_path": reference_metrics.get(
            "planning_debug_presence_last_reference_line_path"
        ),
        "planning_debug_presence_last_routing_path": reference_metrics.get(
            "planning_debug_presence_last_routing_path"
        ),
        "planning_debug_presence_reference_line_nonempty_ratio": reference_metrics.get(
            "planning_debug_presence_reference_line_nonempty_ratio"
        ),
        "planning_debug_presence_routing_segment_nonempty_ratio": reference_metrics.get(
            "planning_debug_presence_routing_segment_nonempty_ratio"
        ),
        "planning_materialization_classification": materialization_summary.get("classification"),
        "planning_materialization_claim_window_source": materialization_summary.get(
            "claim_window_source"
        ),
        "planning_materialization_route_segments_ready_ratio": materialization_claim_window.get(
            "route_segments_ready_ratio"
        ),
        "planning_materialization_reference_line_empty_while_ready_ratio": materialization_claim_window.get(
            "reference_line_empty_with_route_segments_ready_ratio"
        ),
        "planning_materialization_lane_follow_stage_ratio": materialization_claim_window.get(
            "lane_follow_stage_ratio"
        ),
        "control_target_point_vs_path_candidate_classification": control_target_path_candidate.get(
            "classification"
        ),
        "control_target_point_vs_path_candidate_reference_line_claim_grade_allowed": (
            control_target_path_candidate.get("reference_line_claim_grade_allowed")
        ),
        "control_target_point_vs_path_candidate_p95_m": control_target_path_candidate.get(
            "target_point_to_path_candidate_line_abs_p95_m"
        ),
        "control_target_point_vs_path_candidate_target_lane_l_abs_p95_m": (
            control_target_path_candidate.get("target_point_lane_l_abs_p95_m")
        ),
        "control_target_point_inside_path_candidate_lateral_envelope": (
            control_target_path_candidate.get("target_inside_path_lateral_envelope")
        ),
        "path_candidate_lane_l_min_m": (
            control_target_path_candidate.get("path_candidate_lane_l_min_m")
            if control_target_path_candidate.get("path_candidate_lane_l_min_m") is not None
            else path_candidate_hdmap_alignment.get("path_candidate_lane_l_min_m")
        ),
        "path_candidate_lane_l_max_m": (
            control_target_path_candidate.get("path_candidate_lane_l_max_m")
            if control_target_path_candidate.get("path_candidate_lane_l_max_m") is not None
            else path_candidate_hdmap_alignment.get("path_candidate_lane_l_max_m")
        ),
        "planning_debug_path_candidate_vs_trajectory_sample_classification": (
            path_candidate_trajectory_surrogate.get("classification")
        ),
        "planning_debug_path_candidate_vs_trajectory_sample_reference_line_claim_grade_allowed": (
            path_candidate_trajectory_surrogate.get("reference_line_claim_grade_allowed")
        ),
        "planning_debug_path_candidate_vs_trajectory_sample_p95_m": (
            path_candidate_trajectory_surrogate.get(
                "path_candidate_to_planning_sample_line_abs_p95_m"
            )
        ),
        "planning_debug_path_candidate_vs_trajectory_sample_coverage_ratio": (
            path_candidate_trajectory_surrogate.get("sample_coverage_ratio")
        ),
        "planning_debug_path_candidate_hdmap_projection_classification": (
            path_candidate_hdmap_alignment.get("classification")
        ),
        "planning_debug_path_candidate_hdmap_projection_reference_line_claim_grade_allowed": (
            path_candidate_hdmap_alignment.get("reference_line_claim_grade_allowed")
        ),
        "planning_debug_path_candidate_hdmap_projection_lane_l_abs_p95_m": (
            path_candidate_hdmap_alignment.get("path_candidate_lane_l_abs_p95_m")
        ),
        "planning_debug_path_candidate_hdmap_projection_routing_lane_window_compatible": (
            path_candidate_hdmap_alignment.get("routing_lane_window_compatible")
        ),
        "route_simple_lat_sign_convention_candidate": lateral_metrics.get(
            "route_simple_lat_sign_convention_candidate"
        ),
        "route_lateral_provenance_evidence_level": lateral_metrics.get(
            "route_lateral_provenance_evidence_level"
        ),
        "route_lateral_provenance_interpretation": lateral_metrics.get(
            "route_lateral_provenance_interpretation"
        ),
        "route_definition_geometry_status": lateral_metrics.get(
            "route_definition_geometry_status"
        ),
        "projection_route_sample_sign_contract_status": lateral_metrics.get(
            "projection_route_sample_sign_contract_status"
        ),
        "projection_route_sample_sign_contract_classification": lateral_metrics.get(
            "projection_route_sample_sign_contract_classification"
        ),
        "projection_route_sample_count": lateral_metrics.get(
            "projection_route_sample_count"
        ),
        "projection_route_sample_matched_sample_count": lateral_metrics.get(
            "projection_route_sample_matched_sample_count"
        ),
        "projection_route_sample_lateral_source_field": lateral_metrics.get(
            "projection_route_sample_lateral_source_field"
        ),
        "projection_route_sample_timeseries_opposite_sign_ratio": lateral_metrics.get(
            "projection_route_sample_timeseries_opposite_sign_ratio"
        ),
        "projection_route_sample_simple_lat_same_sign_ratio": lateral_metrics.get(
            "projection_route_sample_simple_lat_same_sign_ratio"
        ),
        "route_lateral_field_semantics_status": lateral_metrics.get(
            "route_lateral_field_semantics_status"
        ),
        "route_lateral_field_semantics_classification": lateral_metrics.get(
            "route_lateral_field_semantics_classification"
        ),
        "route_lateral_field_semantics_source_field": lateral_metrics.get(
            "route_lateral_field_semantics_source_field"
        ),
        "route_lateral_field_sign_sensitive_gate_allowed": lateral_metrics.get(
            "route_lateral_field_sign_sensitive_gate_allowed"
        ),
        "route_lateral_field_absolute_magnitude_gate_allowed": lateral_metrics.get(
            "route_lateral_field_absolute_magnitude_gate_allowed"
        ),
        "route_lateral_field_recommended_gate_policy": lateral_metrics.get(
            "route_lateral_field_recommended_gate_policy"
        ),
        "route_lateral_field_recommended_action": lateral_metrics.get(
            "route_lateral_field_recommended_action"
        ),
        "official_hdmap_projection_matched_sample_count": lateral_metrics.get(
            "official_hdmap_projection_matched_sample_count"
        ),
        "route_lateral_projection_lateral_opposite_sign_ratio": lateral_metrics.get(
            "route_lateral_projection_lateral_opposite_sign_ratio"
        ),
        "simple_lat_projection_lateral_same_sign_ratio": lateral_metrics.get(
            "simple_lat_projection_lateral_same_sign_ratio"
        ),
        "route_projection_abs_magnitude_delta_p95_m": lateral_metrics.get(
            "route_projection_abs_magnitude_delta_p95_m"
        ),
        "simple_lat_matched_point_projection_line_lateral_abs_p95_m": lateral_metrics.get(
            "simple_lat_matched_point_projection_line_lateral_abs_p95_m"
        ),
        "simple_lat_matched_point_projection_line_sample_count": lateral_metrics.get(
            "simple_lat_matched_point_projection_line_sample_count"
        ),
        "simple_lat_current_reference_point_projection_line_lateral_abs_p95_m": lateral_metrics.get(
            "simple_lat_current_reference_point_projection_line_lateral_abs_p95_m"
        ),
        "simple_lat_current_reference_point_projection_line_sample_count": lateral_metrics.get(
            "simple_lat_current_reference_point_projection_line_sample_count"
        ),
        "simple_lat_target_point_projection_line_lateral_abs_p95_m": lateral_metrics.get(
            "simple_lat_target_point_projection_line_lateral_abs_p95_m"
        ),
        "simple_lat_target_point_projection_line_sample_count": lateral_metrics.get(
            "simple_lat_target_point_projection_line_sample_count"
        ),
        "simple_lat_point_coverage_status": lateral_metrics.get(
            "simple_lat_point_coverage_status"
        ),
        "simple_lat_missing_point_fields": lateral_metrics.get(
            "simple_lat_missing_point_fields"
        ),
        "simple_lat_station_coverage_status": lateral_metrics.get(
            "simple_lat_station_coverage_status"
        ),
        "simple_lat_station_frame_classification": lateral_metrics.get(
            "simple_lat_station_frame_classification"
        ),
        "simple_lat_missing_station_fields": lateral_metrics.get(
            "simple_lat_missing_station_fields"
        ),
        "simple_lat_current_station_projection_s_delta_p95_m": lateral_metrics.get(
            "simple_lat_current_station_projection_s_delta_p95_m"
        ),
        "simple_lat_matched_s_projection_s_delta_p95_m": lateral_metrics.get(
            "simple_lat_matched_s_projection_s_delta_p95_m"
        ),
        "simple_lat_target_s_projection_s_delta_p95_m": lateral_metrics.get(
            "simple_lat_target_s_projection_s_delta_p95_m"
        ),
        "simple_lat_target_s_current_station_delta_p95_m": lateral_metrics.get(
            "simple_lat_target_s_current_station_delta_p95_m"
        ),
        "route_simple_lat_opposite_sign_abs_sum_p95_m": lateral_metrics.get(
            "route_simple_lat_opposite_sign_abs_sum_p95_m"
        ),
        "route_simple_lat_abs_magnitude_delta_p95_m": lateral_metrics.get(
            "route_simple_lat_abs_magnitude_delta_p95_m"
        ),
        "route_simple_lat_alignment_interpretation": lateral_metrics.get(
            "route_simple_lat_alignment_interpretation"
        ),
    }


_PHASE1_CLAIM_ONLY_APOLLO_LINK_PREFIXES = (
    "no_assist_claim_boundary:",
    "natural_driving_outcome:",
)


def _phase1_relevant_apollo_link_blocker(
    *,
    report: Mapping[str, Any],
    lateral_metrics: Mapping[str, Any],
    reference_metrics: Mapping[str, Any],
    export_policy: Mapping[str, Any],
) -> str | None:
    """Return the Apollo link blocker that is actionable for Phase 1 behavior."""

    candidates = [report.get("primary_blocker")]
    candidates.extend(list(report.get("secondary_blockers") or []))
    for blocker in candidates:
        if not isinstance(blocker, str) or not blocker:
            continue
        if blocker.startswith(_PHASE1_CLAIM_ONLY_APOLLO_LINK_PREFIXES):
            continue
        return blocker

    if lateral_metrics.get("route_simple_lat_sign_convention_candidate") is True:
        return "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"

    lateral_classification = lateral_metrics.get("route_lateral_field_semantics_classification")
    if isinstance(lateral_classification, str) and lateral_classification:
        return f"apollo_lateral_semantics:{lateral_classification}"

    export_classification = export_policy.get("classification")
    if isinstance(export_classification, str) and export_classification:
        return f"planning_reference_line:{export_classification}"

    reference_classification = (
        reference_metrics.get("planning_debug_presence_classification")
        or reference_metrics.get("planning_debug_presence_last_diagnosis")
    )
    if isinstance(reference_classification, str) and reference_classification:
        return f"planning_reference_line:{reference_classification}"

    return None


def _baguang_lane_event_blocker_summary(report: Mapping[str, Any], path: Path) -> dict[str, Any]:
    run_reports = report.get("run_reports") if isinstance(report.get("run_reports"), list) else []
    first = next((item for item in run_reports if isinstance(item, Mapping)), {})
    departure = (
        first.get("departure_diagnostics")
        if isinstance(first.get("departure_diagnostics"), Mapping)
        else {}
    )
    return {
        "available": bool(report),
        "path": str(path) if path.exists() else None,
        "status": report.get("status"),
        "reason": first.get("reason"),
        "lane_invasion_event_can_be_used_as_hard_gate": (
            first.get("lane_invasion_event_can_be_used_as_hard_gate")
            if "lane_invasion_event_can_be_used_as_hard_gate" in first
            else (report.get("claim_boundary") or {}).get("lane_invasion_event_can_be_used_as_hard_gate")
            if isinstance(report.get("claim_boundary"), Mapping)
            else None
        ),
        "departure_classification": departure.get("classification"),
        "departure_interpretation": list(departure.get("interpretation") or []),
    }


def _control_motion_metrics(root: Path) -> dict[str, Any]:
    rows = _read_timeseries_rows(root)
    if not rows:
        return {
            "timeseries_rows": 0,
            "applied_control_available": False,
            "command_control_available": False,
            "command_source": "unavailable",
            "stuck_control_signature": "timeseries_missing",
        }
    control_source_counts = _value_counts(rows, "control_source")
    command_source, command_throttle, command_brake = _source_control_series(root, rows)
    applied_throttle = _series_from_aliases(
        rows,
        ("applied_throttle", "throttle_applied", "carla_throttle_applied", "throttle"),
    )
    applied_brake = _series_from_aliases(rows, ("applied_brake", "brake_applied", "carla_brake_applied", "brake"))
    ego_speed = _series_from_aliases(rows, ("ego_speed_mps", "ego_speed", "v_mps"))
    command_control_available = bool(command_throttle[1] or command_brake[1])
    applied_control_available = bool(applied_throttle[1] or applied_brake[1])
    metrics = {
        "timeseries_rows": len(rows),
        "control_source_counts": control_source_counts,
        "timeseries_command_fields_role": _timeseries_command_fields_role(control_source_counts),
        "command_source": command_source,
        "command_control_available": command_control_available,
        "applied_control_available": applied_control_available,
        "command_throttle": _series_stats(command_throttle),
        "command_brake": _series_stats(command_brake),
        "applied_throttle": _series_stats(applied_throttle),
        "applied_brake": _series_stats(applied_brake),
        "ego_speed": _series_stats(ego_speed),
    }
    metrics["stuck_control_signature"] = _stuck_control_signature(metrics)
    metrics["source_control_context"] = _source_control_context(root)
    metrics["source_brake_interpretation"] = _source_brake_interpretation(metrics)
    return metrics


def _lane_invasion_context(
    root: Path,
    summary: Mapping[str, Any],
    *,
    route_lateral_field_policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = _read_timeseries_rows(root)
    summary_count = _to_float(summary.get("lane_invasion_count"))
    context: dict[str, Any] = {
        "available": False,
        "summary_lane_invasion_count": summary_count,
        "summary_exit_reason": summary.get("exit_reason") or summary.get("fail_reason") or summary.get("failure_reason"),
        "timeseries_rows": len(rows),
    }
    if not rows:
        context["missing_reason"] = "timeseries_missing"
        return context

    first_index: int | None = None
    first_row: Mapping[str, Any] | None = None
    for index, row in enumerate(rows):
        count = _to_float(row.get("lane_invasion_count"))
        if count is not None and count > 0:
            first_index = index
            first_row = row
            break
    if first_row is None or first_index is None:
        if summary_count and summary_count > 0:
            context["missing_reason"] = "summary_reports_lane_invasion_but_timeseries_has_no_count"
        else:
            context["missing_reason"] = "no_lane_invasion_rows"
        return context

    first_x = _first_row_float(first_row, ("ego_x", "x", "vehicle_x"))
    first_y = _first_row_float(first_row, ("ego_y", "y", "vehicle_y"))
    first_z = _first_row_float(first_row, ("ego_z", "z", "vehicle_z"))
    start = rows[0] if rows else {}
    start_x = _first_row_float(start, ("ego_x", "x", "vehicle_x"))
    start_y = _first_row_float(start, ("ego_y", "y", "vehicle_y"))
    start_z = _first_row_float(start, ("ego_z", "z", "vehicle_z"))
    displacement = None
    if None not in (first_x, first_y, start_x, start_y):
        dz = (first_z or 0.0) - (start_z or 0.0)
        displacement = (
            ((first_x or 0.0) - (start_x or 0.0)) ** 2
            + ((first_y or 0.0) - (start_y or 0.0)) ** 2
            + dz**2
        ) ** 0.5
    first_route_s = _first_row_float(first_row, ("route_s", "ego_route_s"))
    start_route_s = _first_row_float(start, ("route_s", "ego_route_s"))
    route_s_delta = None
    if first_route_s is not None and start_route_s is not None:
        route_s_delta = first_route_s - start_route_s
    context.update(
        {
            "available": True,
            "first_row_index": first_index,
            "first_frame": _first_row_text(first_row, ("frame", "frame_id")),
            "first_sim_time_s": _first_row_float(first_row, ("sim_time", "t", "timestamp")),
            "lane_invasion_count_at_first": _to_float(first_row.get("lane_invasion_count")),
            "ego_x": first_x,
            "ego_y": first_y,
            "ego_z": first_z,
            "ego_speed_mps": _first_row_float(first_row, ("ego_speed", "ego_speed_mps", "v_mps")),
            "ego_heading_rad": _first_row_float(first_row, ("ego_heading", "heading", "yaw")),
            "ego_displacement_from_start_m": displacement,
            "route_s": first_route_s,
            "route_s_delta_from_start_m": route_s_delta,
            "cross_track_error_m": _first_row_float(first_row, ("cross_track_error", "cross_track_error_m")),
            "heading_error_rad": _first_row_float(first_row, ("heading_error", "heading_error_rad")),
            "route_heading_rad": _first_row_float(first_row, ("route_heading",)),
            "route_curvature": _first_row_float(first_row, ("route_curvature", "curvature_at_nearest")),
            "apollo_steer_raw": _first_row_float(first_row, ("apollo_steer_raw", "steering_target")),
            "applied_steer": _first_row_float(first_row, ("applied_steer", "carla_steer_applied", "steer")),
            "applied_throttle": _first_row_float(first_row, ("applied_throttle", "throttle_applied", "throttle")),
            "applied_brake": _first_row_float(first_row, ("applied_brake", "brake_applied", "brake")),
            "control_source": _first_row_text(first_row, ("control_source",)),
            "near_start": bool((first_index <= 200) or (displacement is not None and displacement <= 5.0)),
        }
    )
    if route_lateral_field_policy:
        context["route_lateral_field_policy"] = dict(route_lateral_field_policy)
        context["route_lateral_source_field"] = route_lateral_field_policy.get("source_field")
        context["route_lateral_sign_sensitive_gate_allowed"] = route_lateral_field_policy.get(
            "sign_sensitive_gate_allowed"
        )
        context["route_lateral_absolute_magnitude_gate_allowed"] = route_lateral_field_policy.get(
            "absolute_magnitude_gate_allowed"
        )
        context["route_lateral_recommended_gate_policy"] = route_lateral_field_policy.get(
            "recommended_gate_policy"
        )
        context["route_lateral_recommended_action"] = route_lateral_field_policy.get(
            "recommended_action"
        )
    return context


def _safety_event_evidence(root: Path, summary: Mapping[str, Any]) -> dict[str, Any]:
    rows = _read_timeseries_rows(root)
    return {
        "collision": _safety_event_metric(
            summary=summary,
            rows=rows,
            event_name="collision",
            summary_key="collision_count",
            timeseries_aliases=("collision_count", "collisions"),
            sensor_key="collision_sensor_available",
            timeseries_sensor_aliases=("collision_sensor_available",),
        ),
        "lane_invasion": _safety_event_metric(
            summary=summary,
            rows=rows,
            event_name="lane_invasion",
            summary_key="lane_invasion_count",
            timeseries_aliases=("lane_invasion_count", "lane_invasions"),
            sensor_key="lane_invasion_sensor_available",
            timeseries_sensor_aliases=("lane_invasion_sensor_available",),
        ),
    }


def _safety_event_metric(
    *,
    summary: Mapping[str, Any],
    rows: list[Mapping[str, Any]],
    event_name: str,
    summary_key: str,
    timeseries_aliases: tuple[str, ...],
    sensor_key: str,
    timeseries_sensor_aliases: tuple[str, ...],
) -> dict[str, Any]:
    summary_present = summary_key in summary
    summary_count = _to_float(summary.get(summary_key)) if summary_present else None
    summary_sensor_available = _optional_bool(summary.get(sensor_key)) if sensor_key in summary else None
    source_fields: list[str] = []
    if summary_present:
        source_fields.append(f"summary.{summary_key}")
    if summary_sensor_available is not None:
        source_fields.append(f"summary.{sensor_key}")

    timeseries_field = _first_present_field(rows, timeseries_aliases)
    timeseries_sensor_field = _first_present_field(rows, timeseries_sensor_aliases)
    timeseries_values: list[float] = []
    if timeseries_field:
        source_fields.append(f"timeseries.{timeseries_field}")
        timeseries_values = [_to_float(row.get(timeseries_field)) for row in rows]
        timeseries_values = [value for value in timeseries_values if value is not None]
    timeseries_sensor_values: list[bool] = []
    if timeseries_sensor_field:
        source_fields.append(f"timeseries.{timeseries_sensor_field}")
        timeseries_sensor_values = [
            value
            for value in (_optional_bool(row.get(timeseries_sensor_field)) for row in rows)
            if value is not None
        ]

    max_timeseries_count = max(timeseries_values) if timeseries_values else None
    count = summary_count if summary_count is not None else max_timeseries_count
    first_event_row_index = None
    if timeseries_field:
        for index, row in enumerate(rows):
            value = _to_float(row.get(timeseries_field))
            if value is not None and value > 0:
                first_event_row_index = index
                break

    explicit_sensor_available = _combine_sensor_available(summary_sensor_available, timeseries_sensor_values)
    counter_available = summary_present or bool(timeseries_field)
    available = counter_available and explicit_sensor_available is not False
    missing_reason = None
    if counter_available and explicit_sensor_available is False:
        missing_reason = f"{event_name}_sensor_unavailable"
    elif not available:
        missing_reason = f"{event_name}_event_counter_missing"
    return {
        "available": available,
        "event_name": event_name,
        "summary_field_present": summary_present,
        "summary_count": summary_count,
        "summary_sensor_available": summary_sensor_available,
        "timeseries_field_present": bool(timeseries_field),
        "timeseries_field": timeseries_field,
        "timeseries_sample_count": len(timeseries_values),
        "timeseries_sensor_field_present": bool(timeseries_sensor_field),
        "timeseries_sensor_field": timeseries_sensor_field,
        "timeseries_sensor_sample_count": len(timeseries_sensor_values),
        "timeseries_sensor_available_all": all(timeseries_sensor_values) if timeseries_sensor_values else None,
        "max_timeseries_count": max_timeseries_count,
        "first_event_row_index": first_event_row_index,
        "count": count,
        "sensor_available": explicit_sensor_available,
        "source_fields": source_fields,
        "missing_reason": missing_reason,
    }


def _first_present_field(rows: list[Mapping[str, Any]], aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        if any(alias in row for row in rows):
            return alias
    return None


def _optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
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


def _combine_sensor_available(summary_value: bool | None, timeseries_values: list[bool]) -> bool | None:
    values: list[bool] = []
    if summary_value is not None:
        values.append(summary_value)
    values.extend(timeseries_values)
    if not values:
        return None
    return all(values)


def _first_row_float(row: Mapping[str, Any], aliases: tuple[str, ...]) -> float | None:
    for alias in aliases:
        value = _to_float(row.get(alias))
        if value is not None:
            return value
    return None


def _first_row_text(row: Mapping[str, Any], aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        value = row.get(alias)
        if value not in (None, ""):
            return str(value)
    return None


def _source_control_series(
    root: Path,
    timeseries_rows: list[Mapping[str, Any]],
) -> tuple[str, tuple[str | None, list[float]], tuple[str | None, list[float]]]:
    bridge_rows = _read_jsonl_rows(root / "artifacts" / "bridge_control_decode.jsonl")
    if bridge_rows:
        throttle = _series_from_aliases(
            bridge_rows,
            ("commanded_throttle", "raw_throttle", "throttle_before_mutual_exclusion"),
        )
        brake = _series_from_aliases(bridge_rows, ("commanded_brake", "raw_brake", "brake_before_mutual_exclusion"))
        if throttle[1] or brake[1]:
            return "bridge_control_decode", throttle, brake

    apollo_rows = _read_jsonl_rows(root / "artifacts" / "apollo_control_raw.jsonl")
    if apollo_rows:
        throttle = _series_from_paths(apollo_rows, (("apollo_control_raw", "throttle"),))
        brake = _series_from_paths(apollo_rows, (("apollo_control_raw", "brake"),))
        if throttle[1] or brake[1]:
            return "apollo_control_raw", throttle, brake

    return (
        "timeseries_command_fields",
        _series_from_aliases(timeseries_rows, ("cmd_throttle", "throttle_raw", "apollo_throttle_raw")),
        _series_from_aliases(timeseries_rows, ("cmd_brake", "brake_raw", "apollo_brake_raw")),
    )


def _read_timeseries_rows(root: Path) -> list[dict[str, Any]]:
    csv_path = root / "timeseries.csv"
    if csv_path.exists():
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                return [dict(row) for row in csv.DictReader(handle)]
        except Exception:
            return []
    jsonl_path = root / "timeseries.jsonl"
    rows: list[dict[str, Any]] = []
    if jsonl_path.exists():
        try:
            with jsonl_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    if isinstance(payload, Mapping):
                        rows.append(dict(payload))
        except Exception:
            return []
    return rows


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if isinstance(payload, Mapping):
                    rows.append(dict(payload))
    except Exception:
        return []
    return rows


def _series_from_aliases(rows: list[Mapping[str, Any]], aliases: tuple[str, ...]) -> tuple[str | None, list[float]]:
    for alias in aliases:
        values = [_to_float(row.get(alias)) for row in rows]
        values = [value for value in values if value is not None]
        if values:
            return alias, values
    return None, []


def _series_from_paths(
    rows: list[Mapping[str, Any]],
    paths: tuple[tuple[str, ...], ...],
) -> tuple[str | None, list[float]]:
    for path in paths:
        values = [_to_float(_lookup_path(row, path)) for row in rows]
        values = [value for value in values if value is not None]
        if values:
            return ".".join(path), values
    return None, []


def _series_stats(series: tuple[str | None, list[float]]) -> dict[str, Any]:
    source, values = series
    nonzero_count = sum(1 for value in values if abs(value) > 1e-6)
    return {
        "source_field": source,
        "sample_count": len(values),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "mean": (sum(values) / len(values)) if values else None,
        "first": values[0] if values else None,
        "final": values[-1] if values else None,
        "nonzero_count": nonzero_count,
        "nonzero_ratio": (nonzero_count / len(values)) if values else None,
    }


def _source_control_context(root: Path) -> dict[str, Any]:
    bridge_rows = _read_jsonl_rows(root / "artifacts" / "bridge_control_decode.jsonl")
    apollo_rows = _read_jsonl_rows(root / "artifacts" / "apollo_control_raw.jsonl")
    planning_rows = _read_jsonl_rows(root / "artifacts" / "planning_topic_debug.jsonl")
    route_segment_rows = _read_jsonl_rows(root / "artifacts" / "planning_route_segment_debug.jsonl")
    handoff_report = _read_json(root / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json")
    return {
        "bridge_control_decode": _bridge_control_context(bridge_rows),
        "apollo_control_raw": _apollo_control_context(apollo_rows),
        "planning_topic_debug": _planning_topic_context(planning_rows),
        "planning_route_segment_debug": _planning_route_segment_context(route_segment_rows),
        "apollo_control_handoff": _apollo_control_handoff_context(handoff_report),
    }


def _bridge_control_context(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "planning_lateral_contract_reason_counts": _path_counts(rows, ("planning_lateral_contract_reason",)),
        "planning_lateral_contract_valid": _bool_stats(rows, ("planning_lateral_contract_valid",)),
        "planning_lateral_latest_point_count": _path_numeric_stats(rows, ("planning_lateral_latest_point_count",)),
        "planning_lateral_latest_sequence_num": _path_numeric_stats(rows, ("planning_lateral_latest_sequence_num",)),
        "throttle_brake_state_counts": _path_counts(rows, ("throttle_brake_mutual_exclusion_state",)),
    }


def _apollo_control_context(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "brake": _path_numeric_stats(rows, ("apollo_control_raw", "brake")),
        "throttle": _path_numeric_stats(rows, ("apollo_control_raw", "throttle")),
        "input_trajectory_sequence": _path_numeric_stats(
            rows,
            ("apollo_control_raw", "debug_input_trajectory_header_sequence_num"),
        ),
        "latest_replan_trajectory_sequence": _path_numeric_stats(
            rows,
            ("apollo_control_raw", "debug_input_latest_replan_trajectory_header_sequence_num"),
        ),
        "input_trajectory_timestamp": _path_numeric_stats(
            rows,
            ("apollo_control_raw", "debug_input_trajectory_header_timestamp_sec"),
        ),
        "latest_replan_trajectory_timestamp": _path_numeric_stats(
            rows,
            ("apollo_control_raw", "debug_input_latest_replan_trajectory_header_timestamp_sec"),
        ),
        "speed_reference": _path_numeric_stats(rows, ("apollo_control_raw", "debug_simple_lon_speed_reference")),
        "preview_speed_reference": _path_numeric_stats(
            rows,
            ("apollo_control_raw", "debug_simple_lon_preview_speed_reference"),
        ),
    }


def _planning_topic_context(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "trajectory_point_count": _path_numeric_stats(rows, ("trajectory_point_count",)),
        "reference_line_count": _path_numeric_stats(rows, ("reference_line_count",)),
        "routing_lane_window_signature_counts": _path_counts(rows, ("routing_lane_window_signature",)),
        "engage_advice_counts": _path_counts(rows, ("engage_advice",)),
    }


def _planning_route_segment_context(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "route_segment_count": _path_numeric_stats(rows, ("route_segment_count",)),
        "reference_line_count": _path_numeric_stats(rows, ("reference_line_count",)),
        "reference_line_debug_missing_but_trajectory_nonzero": _bool_stats(
            rows,
            ("reference_line_debug_missing_but_trajectory_nonzero",),
        ),
        "lane_follow_map_status_counts": _path_counts(rows, ("lane_follow_map_status",)),
    }


def _apollo_control_handoff_context(report: Mapping[str, Any]) -> dict[str, Any]:
    planning_handoff = report.get("planning_control_handoff")
    if not isinstance(planning_handoff, Mapping):
        planning_handoff = {}
    failure_reasons = [
        str(reason)
        for reason in planning_handoff.get("failure_reasons") or report.get("failure_reasons") or []
        if reason
    ]
    return {
        "status": report.get("verdict") or report.get("status"),
        "failure_stage": report.get("failure_stage"),
        "failure_reasons": failure_reasons,
        "first_nonzero_planning_timestamp_sec": planning_handoff.get("first_nonzero_planning_timestamp_sec"),
        "last_control_raw_timestamp_sec": planning_handoff.get("last_control_raw_timestamp_sec"),
        "last_control_consume_timestamp_sec": planning_handoff.get("last_control_consume_timestamp_sec"),
        "control_rows_after_first_nonzero_planning": planning_handoff.get(
            "control_rows_after_first_nonzero_planning"
        ),
        "control_stream_end_before_first_nonzero_planning_delta_ms": planning_handoff.get(
            "control_stream_end_before_first_nonzero_planning_delta_ms"
        ),
    }


def _source_brake_interpretation(metrics: Mapping[str, Any]) -> str | None:
    if metrics.get("stuck_control_signature") != "source_brake_applied":
        return None
    context = metrics.get("source_control_context")
    if not isinstance(context, Mapping):
        return None
    handoff_context = (
        context.get("apollo_control_handoff")
        if isinstance(context.get("apollo_control_handoff"), Mapping)
        else {}
    )
    handoff_failure_reasons = {
        str(reason) for reason in handoff_context.get("failure_reasons") or [] if reason
    }
    if "control_stream_ended_before_first_nonzero_planning" in handoff_failure_reasons:
        return "control_stream_ended_before_first_nonzero_planning"
    if "planning_nonzero_stale_before_control_consume" in handoff_failure_reasons:
        return "planning_nonzero_stale_before_control_consume"
    apollo_context = context.get("apollo_control_raw") if isinstance(context.get("apollo_control_raw"), Mapping) else {}
    planning_context = (
        context.get("planning_topic_debug") if isinstance(context.get("planning_topic_debug"), Mapping) else {}
    )
    bridge_context = (
        context.get("bridge_control_decode") if isinstance(context.get("bridge_control_decode"), Mapping) else {}
    )
    planning_nonzero_ratio = _nested_float(planning_context, "trajectory_point_count", "nonzero_ratio") or 0.0
    control_input_nonzero_ratio = _nested_float(apollo_context, "input_trajectory_sequence", "nonzero_ratio") or 0.0
    replan_input_nonzero_ratio = _nested_float(apollo_context, "latest_replan_trajectory_sequence", "nonzero_ratio") or 0.0
    bridge_latest_point_max = _nested_float(bridge_context, "planning_lateral_latest_point_count", "max")
    reason_counts = bridge_context.get("planning_lateral_contract_reason_counts")
    zero_contract_rows = 0
    bridge_rows = int(bridge_context.get("row_count") or 0)
    if isinstance(reason_counts, Mapping):
        zero_contract_rows = int(reason_counts.get("zero_trajectory_points") or 0)
    if planning_nonzero_ratio > 0.0 and control_input_nonzero_ratio == 0.0 and replan_input_nonzero_ratio == 0.0:
        return "planning_topic_nonzero_but_control_input_trajectory_zero"
    if bridge_rows > 0 and zero_contract_rows == bridge_rows:
        return "bridge_planning_lateral_contract_zero_trajectory_points"
    if bridge_latest_point_max == 0.0:
        return "bridge_latest_planning_points_zero"
    return "apollo_source_brake_applied_without_motion"


def _path_numeric_stats(rows: list[Mapping[str, Any]], path: tuple[str, ...]) -> dict[str, Any]:
    values = [_to_float(_lookup_path(row, path)) for row in rows]
    values = [value for value in values if value is not None]
    return _numeric_stats(values)


def _numeric_stats(values: list[float]) -> dict[str, Any]:
    nonzero_count = sum(1 for value in values if abs(value) > 1e-6)
    return {
        "sample_count": len(values),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "mean": (sum(values) / len(values)) if values else None,
        "first": values[0] if values else None,
        "final": values[-1] if values else None,
        "nonzero_count": nonzero_count,
        "nonzero_ratio": (nonzero_count / len(values)) if values else None,
    }


def _path_counts(rows: list[Mapping[str, Any]], path: tuple[str, ...], *, limit: int = 10) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = _lookup_path(row, path)
        if value in {None, ""}:
            continue
        text = str(value)
        counts[text] = counts.get(text, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit])


def _bool_stats(rows: list[Mapping[str, Any]], path: tuple[str, ...]) -> dict[str, Any]:
    values: list[bool] = []
    for row in rows:
        value = _lookup_path(row, path)
        if isinstance(value, bool):
            values.append(value)
        elif isinstance(value, str) and value.lower() in {"true", "false"}:
            values.append(value.lower() == "true")
    true_count = sum(1 for value in values if value)
    return {
        "sample_count": len(values),
        "true_count": true_count,
        "true_ratio": (true_count / len(values)) if values else None,
    }


def _value_counts(rows: list[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        if value in {None, ""}:
            continue
        text = str(value)
        counts[text] = counts.get(text, 0) + 1
    return counts


def _timeseries_command_fields_role(control_source_counts: Mapping[str, int]) -> str:
    total = sum(int(value) for value in control_source_counts.values())
    external = int(control_source_counts.get("external_stack") or 0)
    if total > 0 and external / total >= 0.8:
        return "harness_placeholder"
    return "runtime_command"


def _stuck_control_signature(metrics: Mapping[str, Any]) -> str:
    command_throttle = _nested_float(metrics, "command_throttle", "nonzero_ratio") or 0.0
    command_brake = _nested_float(metrics, "command_brake", "nonzero_ratio") or 0.0
    applied_throttle = _nested_float(metrics, "applied_throttle", "nonzero_ratio") or 0.0
    applied_brake = _nested_float(metrics, "applied_brake", "nonzero_ratio") or 0.0
    max_ego_speed = _nested_float(metrics, "ego_speed", "max")
    final_ego_speed = _nested_float(metrics, "ego_speed", "final")
    if not metrics.get("applied_control_available"):
        return "applied_control_fields_missing"
    if (
        metrics.get("command_source") == "timeseries_command_fields"
        and metrics.get("timeseries_command_fields_role") == "harness_placeholder"
    ):
        if applied_brake >= 0.5 and applied_throttle <= 0.05:
            return "external_stack_applied_brake_source_unknown"
        return "external_stack_applied_control_source_unknown"
    if command_throttle >= 0.5 and command_brake <= 0.05 and applied_brake >= 0.5 and applied_throttle <= 0.05:
        return "command_throttle_but_applied_brake"
    if command_brake >= 0.5 and applied_brake >= 0.5:
        return "source_brake_applied"
    if applied_brake >= 0.5 and applied_throttle <= 0.05:
        return "applied_brake_no_throttle"
    if max_ego_speed is not None and max_ego_speed < 0.5:
        return "ego_never_moved"
    if final_ego_speed is not None and final_ego_speed < 0.1:
        return "ego_finally_stopped"
    return "no_clear_control_blocker"


def _nested_float(mapping: Mapping[str, Any], key: str, nested_key: str) -> float | None:
    nested = mapping.get(key)
    if not isinstance(nested, Mapping):
        return None
    return _to_float(nested.get(nested_key))


def _lookup_path(row: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = row
    for part in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _summary_failed_by_setup(summary: Mapping[str, Any]) -> bool:
    reason = _normalized_reason(summary)
    return reason in {"setup_failed", "bridge_runtime_import_failed", "config_invalid"}


def _backend_not_ready(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> bool:
    reason = _normalized_reason(summary)
    if reason == "backend_not_ready":
        return True
    return manifest.get("backend_ready") is False


def _fixed_scene_status(root: Path, summary: Mapping[str, Any]) -> tuple[str, str] | None:
    fixed_scene_report = _read_json(root / "analysis" / "fixed_scene_contract" / "fixed_scene_contract_report.json")
    scenario_actor_report = _read_json(
        root / "analysis" / "scenario_actor_contract" / "scenario_actor_contract_report.json"
    )
    fixed_failed = str(summary.get("fixed_scene_contract_status") or "") == "fail" or str(
        fixed_scene_report.get("status") or ""
    ) == "fail"
    actor_failed = str(summary.get("scenario_actor_contract_status") or "") == "fail" or str(
        scenario_actor_report.get("status") or ""
    ) == "fail"
    if not fixed_failed and not actor_failed:
        return None
    early_behavior_failure = _summary_behavior_failure_reason(summary)
    if early_behavior_failure and _fixed_scene_unreached_without_setup_blocker(
        fixed_scene_report,
        scenario_actor_report,
    ):
        return ("failed", early_behavior_failure)
    if _fixed_scene_trigger_not_reached(fixed_scene_report, scenario_actor_report):
        return ("failed", "scenario_phase_trigger_not_reached")
    return ("invalid", "fixed_scene_failed")


def _fixed_scene_trigger_not_reached(
    fixed_scene_report: Mapping[str, Any], scenario_actor_report: Mapping[str, Any]
) -> bool:
    fixed_blockers = {str(item) for item in fixed_scene_report.get("blocking_reasons") or []}
    actor_blockers = {str(item) for item in scenario_actor_report.get("blocking_reasons") or []}
    spawn_feasibility = fixed_scene_report.get("spawn_feasibility")
    if _has_spawn_or_setup_blocker(spawn_feasibility):
        return False
    missing_phase_blockers = {
        "fixed_scene_required_phase_not_started",
        "fixed_scene_required_phase_not_completed",
        "stop_before_required_phase_completed",
    }
    actor_trigger_blockers = {"lead_vehicle_never_observed_stopped", "lead_hold_stop_phase_not_observed"}
    if fixed_blockers and fixed_blockers.issubset(missing_phase_blockers):
        if not actor_blockers or actor_blockers.issubset(actor_trigger_blockers):
            return True
    return False


def _fixed_scene_duration_only_after_actor_interaction(
    fixed_scene_report: Mapping[str, Any], scenario_actor_report: Mapping[str, Any]
) -> bool:
    fixed_blockers = {str(item) for item in fixed_scene_report.get("blocking_reasons") or []}
    actor_status = str(scenario_actor_report.get("status") or "")
    actor_blockers = {str(item) for item in scenario_actor_report.get("blocking_reasons") or []}
    duration_after_actor_blockers = {
        "duration_policy_route_end_not_reached",
        "fixed_scene_required_phase_not_completed",
    }
    if not fixed_blockers or not fixed_blockers.issubset(duration_after_actor_blockers):
        return False
    if actor_status not in {"pass", "warn"} or actor_blockers:
        return False
    metrics = scenario_actor_report.get("metrics") if isinstance(scenario_actor_report.get("metrics"), Mapping) else {}
    try:
        if float(metrics.get("phase_completion_ratio")) >= 1.0:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if float(metrics.get("speed_profile_error_p95_mps")) >= 0.0:
            return True
    except (TypeError, ValueError):
        pass
    return True


def _fixed_scene_unreached_without_setup_blocker(
    fixed_scene_report: Mapping[str, Any], scenario_actor_report: Mapping[str, Any]
) -> bool:
    fixed_blockers = {str(item) for item in fixed_scene_report.get("blocking_reasons") or []}
    actor_blockers = {str(item) for item in scenario_actor_report.get("blocking_reasons") or []}
    spawn_feasibility = fixed_scene_report.get("spawn_feasibility")
    if _has_spawn_or_setup_blocker(spawn_feasibility):
        return False
    phase_or_duration_blockers = {
        "fixed_scene_required_phase_not_started",
        "fixed_scene_required_phase_not_completed",
        "stop_before_required_phase_completed",
        "duration_policy_route_end_not_reached",
    }
    actor_trigger_blockers = {"lead_vehicle_never_observed_stopped", "lead_hold_stop_phase_not_observed"}
    if fixed_blockers and fixed_blockers.issubset(phase_or_duration_blockers):
        return not actor_blockers or actor_blockers.issubset(actor_trigger_blockers)
    return False


def _summary_behavior_failure_reason(summary: Mapping[str, Any]) -> str | None:
    for key, reason in (("collision_count", "collision"), ("lane_invasion_count", "lane_invasion")):
        try:
            if float(summary.get(key) or 0.0) > 0.0:
                return reason
        except (TypeError, ValueError):
            pass
    reason = _normalized_reason(summary)
    if reason in FAILED_REASONS:
        return reason
    return None


def _has_spawn_or_setup_blocker(spawn_feasibility: Any) -> bool:
    if not isinstance(spawn_feasibility, Mapping):
        return False
    for report in spawn_feasibility.values():
        if isinstance(report, Mapping) and report.get("blocking_reasons"):
            return True
        if isinstance(report, Mapping) and str(report.get("status") or "") == "fail":
            return True
    return False


def _failed_reason(root: Path, summary: Mapping[str, Any], v_t_gap: Mapping[str, Any]) -> str | None:
    for key in ("collision_count", "lane_invasion_count"):
        try:
            if float(summary.get(key) or 0.0) > 0.0:
                return "collision" if key == "collision_count" else "lane_invasion"
        except (TypeError, ValueError):
            pass
    handoff_failure = _apollo_handoff_failed_reason(root)
    if handoff_failure:
        return handoff_failure
    reason = _normalized_reason(summary)
    if reason == "timeout":
        return reason
    if _nested_route_health_failed(root):
        return "route_health_failed"
    if reason in FAILED_REASONS:
        return reason
    if _summary_has_failure_code(summary, "EGO_NOT_MOVING"):
        control_metrics = _control_motion_metrics(root)
        if control_metrics.get("stuck_control_signature") == "command_throttle_but_applied_brake":
            return "control_apply_mismatch"
        if _source_brake_indicates_handoff_missing(control_metrics.get("source_brake_interpretation")):
            return "planning_control_handoff_missing"
        return "stuck"
    rows = v_t_gap.get("rows") if isinstance(v_t_gap.get("rows"), list) else []
    gaps = [
        _to_float(row.get("gap_m"))
        for row in rows
        if isinstance(row, Mapping) and _row_gap_is_claim_valid(row)
    ]
    gaps = [gap for gap in gaps if gap is not None]
    if gaps and min(gaps) < 0.0:
        return "unsafe_gap"
    return None


def _apollo_handoff_failed_reason(root: Path) -> str | None:
    report = _read_json(root / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json")
    if not report:
        return None
    status = str(report.get("verdict") or report.get("status") or "").strip()
    stage = str(report.get("failure_stage") or "").strip()
    if status != "fail":
        return None
    if stage == "process_health":
        return "control_process_failed"
    if stage == "planning_control_handoff":
        return "planning_control_handoff_missing"
    return None


def _nested_route_health_failed(root: Path) -> bool:
    nested_summary_path = _first_nested_summary(root)
    if nested_summary_path is None:
        return False
    nested_summary = _read_json(nested_summary_path)
    if nested_summary.get("summary_status") != "finalized":
        return False
    return nested_summary.get("success") is False or bool(_nested_failure_codes(nested_summary))


def _source_brake_indicates_handoff_missing(reason: Any) -> bool:
    return str(reason or "") in {
        "planning_topic_nonzero_but_control_input_trajectory_zero",
        "control_stream_ended_before_first_nonzero_planning",
    }


def _degraded_reason(summary: Mapping[str, Any], v_t_gap: Mapping[str, Any]) -> str | None:
    reason = _normalize_reason_value(summary.get("degraded_reason"))
    if reason in DEGRADED_REASONS:
        return reason
    if v_t_gap.get("status") == "warn" and (
        v_t_gap.get("degraded_reason_counts") or v_t_gap.get("gap_method_counts")
    ):
        return "degraded_gap_method"
    rows = v_t_gap.get("rows") if isinstance(v_t_gap.get("rows"), list) else []
    final_gap = _final_gap(rows)
    if final_gap is not None and final_gap > 60.0:
        return "large_final_gap"
    if v_t_gap.get("status") == "warn":
        return "degraded_gap_method"
    return None


def _unclassified_summary_failure_reason(summary: Mapping[str, Any]) -> str | None:
    if summary.get("success") is not False and _normalize_reason_value(summary.get("status")) not in {"fail", "failed"}:
        return None
    reason = _normalized_reason(summary)
    if not reason:
        return None
    if reason in FAILED_REASONS or reason in DEGRADED_REASONS:
        return None
    if reason in INVALID_REASONS:
        return reason
    return "missing_required_artifact"


def _final_gap(rows: Any) -> float | None:
    if not rows:
        return None
    for row in reversed(rows):
        if isinstance(row, Mapping):
            gap = _to_float(row.get("gap_m"))
            if gap is not None:
                return gap
    return None


def _row_gap_is_claim_valid(row: Mapping[str, Any]) -> bool:
    if _truthy(row.get("gap_degraded")):
        return False
    if str(row.get("validity") or "").lower() == "degraded":
        return False
    method = str(row.get("gap_method") or "")
    return not method.endswith("_degraded")


def _target_contract(root: Path, v_t_gap: Mapping[str, Any], manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(manifest.get("target_actor_contract"), Mapping):
        merged.update(dict(manifest["target_actor_contract"]))
    resolved = _read_json(root / "artifacts" / "fixed_scene_resolved.json")
    contract = resolved.get("target_actor_contract")
    if isinstance(contract, Mapping):
        merged.update(dict(contract))
    if isinstance(v_t_gap.get("target_actor_contract"), Mapping):
        merged.update(dict(v_t_gap["target_actor_contract"]))
    if not merged and manifest:
        merged.update(resolve_target_actor_contract(manifest))
    return merged


def _phase1_scenario_case(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str | None:
    for source in (manifest, summary):
        explicit = source.get("scenario_case") or source.get("fixed_scene_case")
        if explicit:
            return str(explicit)

    route_id = str(manifest.get("route_id") or summary.get("route_id") or "")
    scenario_class = str(manifest.get("scenario_class") or summary.get("scenario_class") or "")
    scenario_id = str(manifest.get("scenario_id") or summary.get("scenario_id") or "")
    if route_id == "town01_rh_spawn068_goal079" and scenario_class in {"curve_diagnostic", "lane_keep", ""}:
        return "town01_curve217_diagnostic"
    if "town01_rh_spawn068_goal079" in scenario_id and (
        "curve" in scenario_id or scenario_class == "curve_diagnostic"
    ):
        return "town01_curve217_diagnostic"
    if route_id == "town01_rh_spawn097_goal046" and scenario_class in {"lane_keep", ""}:
        return "town01_lane_keep_097"
    if "town01_rh_spawn097_goal046" in scenario_id and "lane_keep" in scenario_id:
        return "town01_lane_keep_097"

    return str(manifest.get("scenario_id") or summary.get("scenario_id") or "") or None


def _backend_type(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str | None:
    explicit = manifest.get("backend_type") or summary.get("backend_type")
    if explicit:
        return str(explicit)
    backend = str(
        manifest.get("backend")
        or manifest.get("backend_name")
        or summary.get("backend")
        or summary.get("backend_name")
        or ""
    )
    if backend == "apollo_cyberrt":
        return "apollo_reference_backend"
    if backend == "carla_builtin":
        return "planning_control_backend"
    return None


def _apollo_fixed_scene_target_requires_obstacle(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    target_contract: Mapping[str, Any],
) -> bool:
    if _backend_type(manifest, summary) != "apollo_reference_backend":
        return False
    if target_contract.get("status") != "resolved":
        return False
    if target_contract.get("required") is False:
        return False
    return True


def _missing_apollo_obstacle_gt_contract(root: Path) -> bool:
    return not _apollo_obstacle_gt_contract_present(root)


def _apollo_obstacle_gt_contract_present(root: Path) -> bool:
    candidates = [
        root / "analysis" / "obstacle_gt_contract" / "obstacle_gt_contract_report.json",
        root / "artifacts" / "obstacle_gt_contract.jsonl",
        root / "artifacts" / "obstacle_gt_contract.json",
    ]
    return any(path.exists() for path in candidates)


def _apollo_obstacle_gt_contract_blocker(root: Path) -> str | None:
    report = _read_json(root / "analysis" / "obstacle_gt_contract" / "obstacle_gt_contract_report.json")
    if not report:
        return None
    status = str(report.get("status") or "")
    if status == "fail":
        return "apollo_obstacle_gt_contract_failed"
    if status in {"insufficient_data", "missing"}:
        return "apollo_obstacle_gt_contract_insufficient_data"
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalized_reason(summary: Mapping[str, Any]) -> str:
    for key in ("fail_reason", "failure_reason", "exit_reason"):
        reason = _normalize_reason_value(summary.get(key))
        if reason:
            return reason
    return ""


def _summary_has_failure_code(summary: Mapping[str, Any], code: str) -> bool:
    wanted = code.strip().upper()
    codes: list[Any] = []
    if isinstance(summary.get("failure_codes"), list):
        codes.extend(summary["failure_codes"])
    acceptance = summary.get("acceptance")
    if isinstance(acceptance, Mapping) and isinstance(acceptance.get("failure_codes"), list):
        codes.extend(acceptance["failure_codes"])
    return any(str(item).strip().upper() == wanted for item in codes)


def _normalize_reason_value(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "platform_timeout": "timeout",
        "runtime_timeout": "timeout",
        "route_establishment_latency_sec": "route_establishment_latency",
    }
    return aliases.get(normalized, normalized)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
