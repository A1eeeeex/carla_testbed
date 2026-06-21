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
            "safety_event_evidence": _safety_event_evidence(root, summary),
            "control_motion": _control_motion_metrics(root),
            "lane_invasion_context": _lane_invasion_context(root, summary),
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
    if not run_evaluable:
        scenario_interaction_evaluable = False
        scenario_interaction_reason = "run_invalid"
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
    available = bool(control_health or link_health or lane_event)
    return {
        "available": available,
        "control_health": _control_health_blocker_summary(control_health, control_health_path),
        "apollo_link_health": _apollo_link_health_blocker_summary(link_health, link_health_path),
        "baguang_lane_event_contract": _baguang_lane_event_blocker_summary(lane_event, lane_event_path),
        "claim_boundary": (
            "Derived blocker evidence explains an evaluable Phase 1 failure. "
            "It does not override phase1 status, and it is not natural-driving capability evidence."
        ),
    }


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

    if reason == "lane_invasion":
        if lane_event.get("available"):
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
                    "sign inversion candidate for this run. Close the Planning reference-line "
                    "debug/export gap and decide whether the route-lateral field should be "
                    "renamed, explicitly converted, or excluded from behavior gates until a "
                    "canonical sign convention is declared; do not change steer scale, "
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

    return {
        "primary_behavior_blocker": reason,
        "behavior_blocker_layer": "phase1_status",
        "behavior_next_action": "Inspect scenario-specific evidence before changing runtime behavior.",
        "behavior_blocker_evidence": {},
    }


def collect_derived_blocker_evidence(root: str | Path) -> dict[str, Any]:
    """Return current blocker evidence from optional Phase 1 analysis reports."""

    return _derived_blocker_evidence(Path(root))


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
    return {
        "available": bool(report),
        "path": str(path) if path.exists() else None,
        "primary_blocker": report.get("primary_blocker"),
        "secondary_blockers": list(report.get("secondary_blockers") or []),
        "can_claim_unassisted_natural_driving": report.get("can_claim_unassisted_natural_driving"),
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


def _lane_invasion_context(root: Path, summary: Mapping[str, Any]) -> dict[str, Any]:
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
    if fixed_blockers != {"duration_policy_route_end_not_reached"}:
        return False
    if actor_status not in {"pass", "warn"} or actor_blockers:
        return False
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
        "route_establishment_latency_sec": "route_establishment_latency",
    }
    return aliases.get(normalized, normalized)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
