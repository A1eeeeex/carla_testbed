from __future__ import annotations

import csv
import json

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs, write_scenario_comparison


def test_two_evaluable_runs_are_comparable(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "success",
        handoff_status="pass",
        handoff_failure_stage="none",
    )
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "degraded")

    report = compare_scenario_runs([run_a, run_b])

    assert report["schema_version"] == "phase1_comparison.v1"
    assert report["comparison_id"] == "follow_stop_static__apollo_cyberrt_vs_carla_builtin"
    assert report["scenario_case"] == "follow_stop_static"
    assert report["backends"] == ["apollo_cyberrt", "carla_builtin"]
    assert report["comparison_status"] == "comparable"
    assert report["comparison_target_status"] == "apollo_vs_planning_control_evaluable"
    assert report["backend_coverage"]["evaluable_apollo_reference_backend"] == 1
    assert report["backend_coverage"]["evaluable_planning_control_backend"] == 1
    assert any(path.endswith("analysis/v_t_gap/v_t_gap_report.json") for path in report["v_t_gap_files"])
    assert any(path.endswith("manifest.json") for path in report["evidence_files"])
    assert "phase1_comparison_not_natural_driving_claim" in report["summary_notes"]
    assert all(item["counts_as_backend_loss"] is False for item in report["backend_results"])
    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["phase1_metrics"]["control_motion"]["stuck_control_signature"] == "no_clear_control_blocker"
    assert apollo["apollo_control_handoff_status"] == "pass"
    assert apollo["apollo_control_handoff_failure_stage"] == "none"
    assert apollo["apollo_control_handoff_failure_reasons"] == []
    assert any(path.endswith("analysis/apollo_control_handoff/apollo_control_handoff_report.json") for path in report["evidence_files"])
    assert sum(path.endswith("analysis/apollo_control_handoff/apollo_control_handoff_report.json") for path in report["evidence_files"]) == 1


def test_invalid_run_does_not_count_as_backend_loss(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "invalid",
        "backend_not_ready",
        missing_expected_artifacts=["analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"],
    )
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "missing_evaluable_apollo_reference_backend"
    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["counts_as_backend_loss"] is False
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in apollo["missing_expected_artifacts"]
    participating_apollo = [item for item in report["participating_runs"] if item["backend"] == "apollo_cyberrt"][0]
    assert "backend_not_ready" in participating_apollo["invalid_reasons"]
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in participating_apollo[
        "missing_expected_artifacts"
    ]
    assert not any(
        path.endswith("analysis/apollo_control_handoff/apollo_control_handoff_report.json")
        for path in report["evidence_files"]
    )


def test_scenario_mismatch_is_invalid(tmp_path) -> None:
    run_a = _write_run(tmp_path, "a", "apollo_cyberrt", "apollo_reference_backend", "success", scenario_id="s1")
    run_b = _write_run(tmp_path, "b", "carla_builtin", "planning_control_backend", "success", scenario_id="s2")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "invalid"
    assert report["reason"] == "scenario_case_mismatch"


def test_matching_scenario_case_allows_legacy_scenario_id_alias(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "lane_invasion",
        scenario_id="carla_town01_route_health",
        scenario_case="town01_lane_keep_097",
    )
    run_b = _write_run(
        tmp_path,
        "builtin",
        "carla_builtin",
        "planning_control_backend",
        "success",
        scenario_id="town01_lane_keep_097",
        scenario_case="town01_lane_keep_097",
    )

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "comparable"
    assert report["comparison_target_status"] == "apollo_vs_planning_control_evaluable"
    assert report["scenario_case"] == "town01_lane_keep_097"
    assert report["scenario_id"] is None
    assert report["scenario_ids"] == ["carla_town01_route_health", "town01_lane_keep_097"]
    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["counts_as_backend_loss"] is True


def test_safety_event_evidence_mismatch_blocks_backend_loss(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "lane_invasion",
        scenario_id="town01_lane_keep_097",
        scenario_case="town01_lane_keep_097",
    )
    run_b = _write_run(
        tmp_path,
        "builtin",
        "carla_builtin",
        "planning_control_backend",
        "success",
        scenario_id="town01_lane_keep_097",
        scenario_case="town01_lane_keep_097",
        safety_event_evidence="missing",
    )

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "safety_event_evidence_mismatch"
    assert report["reason"] == "safety_event_evidence_mismatch"
    safety = report["safety_event_evidence"]
    assert safety["blocking_events"] == ["lane_invasion"]
    lane_check = safety["event_checks"]["lane_invasion"]
    assert lane_check["triggered_run_ids"] == ["apollo"]
    assert lane_check["missing_evidence_run_ids"] == ["builtin"]
    assert all(item["counts_as_backend_loss"] is False for item in report["backend_results"])


def test_shared_near_start_lane_invasion_context_blocks_backend_loss(tmp_path) -> None:
    lane_context = {
        "available": True,
        "near_start": True,
        "first_row_index": 25,
        "first_sim_time_s": 1.3,
        "ego_displacement_from_start_m": 0.85,
        "cross_track_error_m": 0.001,
        "heading_error_rad": 0.001,
        "control_source": "external_stack",
    }
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "lane_invasion",
        lane_invasion_context=lane_context,
    )
    run_b = _write_run(
        tmp_path,
        "builtin",
        "carla_builtin",
        "planning_control_backend",
        "failed",
        "lane_invasion",
        lane_invasion_context={**lane_context, "control_source": "carla_testbed_builtin_controller"},
    )

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "shared_safety_event_context_issue"
    assert report["reason"] == "shared_near_start_lane_invasion_context_issue"
    safety_context = report["safety_event_context"]
    assert safety_context["comparison_blocking"] is True
    assert safety_context["event_checks"]["lane_invasion"]["reason"] == "shared_near_start_low_error_lane_invasion"
    assert all(item["counts_as_backend_loss"] is False for item in report["backend_results"])


def test_high_error_shared_lane_invasion_remains_backend_failure(tmp_path) -> None:
    lane_context = {
        "available": True,
        "near_start": False,
        "first_row_index": 300,
        "first_sim_time_s": 15.0,
        "ego_displacement_from_start_m": 42.0,
        "cross_track_error_m": 1.2,
        "heading_error_rad": 0.08,
        "control_source": "external_stack",
    }
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "lane_invasion",
        lane_invasion_context=lane_context,
    )
    run_b = _write_run(
        tmp_path,
        "builtin",
        "carla_builtin",
        "planning_control_backend",
        "failed",
        "lane_invasion",
        lane_invasion_context={**lane_context, "control_source": "carla_testbed_builtin_controller"},
    )

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "comparable"
    assert report["comparison_target_status"] == "apollo_vs_planning_control_evaluable"
    assert report["safety_event_context"]["comparison_blocking"] is False
    assert all(item["counts_as_backend_loss"] is True for item in report["backend_results"])


def test_blocking_assist_keeps_comparison_out_of_phase1_reference_target(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "unsafe_gap",
        active_assists=["straight_acc_override"],
    )
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "blocking_assists_present_not_phase1_reference"
    assert report["reason"] == "blocking_assists_present_not_phase1_reference"
    assert report["validity_gates"]["blocking_assists_absent"] is False
    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["blocking_assists"] == ["straight_acc_override"]
    assert apollo["counts_as_backend_loss"] is False


def test_target_interaction_not_exercised_blocks_comparable_cut_in(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "lane_invasion",
        scenario_id="baguang_cut_in_35kph_left_to_right_10m",
        scenario_case="baguang_cut_in_35kph_left_to_right_10m",
        scenario_interaction_evaluable=False,
        scenario_interaction_reason="required_phase_not_reached",
        target_metric_evaluable=False,
        target_metric_status="invalid",
        target_metric_reason="missing_target_activation_phase",
    )
    run_b = _write_run(
        tmp_path,
        "builtin",
        "carla_builtin",
        "planning_control_backend",
        "failed",
        "lane_invasion",
        scenario_id="baguang_cut_in_35kph_left_to_right_10m",
        scenario_case="baguang_cut_in_35kph_left_to_right_10m",
        scenario_interaction_evaluable=False,
        scenario_interaction_reason="required_phase_not_reached",
        target_metric_evaluable=False,
        target_metric_status="invalid",
        target_metric_reason="missing_target_activation_phase",
    )

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "target_interaction_not_exercised"
    assert report["reason"] == "target_interaction_not_exercised"
    assert report["validity_gates"]["interaction_complete"] is False
    assert report["validity_gates"]["target_metric_valid"] is False
    assert all(item["counts_as_backend_loss"] is False for item in report["backend_results"])


def test_scenario_comparison_writer(tmp_path) -> None:
    run_a = _write_run(tmp_path, "apollo", "apollo_cyberrt", "apollo_reference_backend", "success")
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")
    report = compare_scenario_runs([run_a, run_b])

    outputs = write_scenario_comparison(report, tmp_path / "comparison")

    assert outputs["manifest"].endswith("comparison_manifest.json")
    assert outputs["summary"].endswith("comparison_summary.json")
    assert outputs["v_t_gap_csv"].endswith("comparison_curves/v_t_gap_combined.csv")
    written = json.loads((tmp_path / "comparison" / "comparison_summary.json").read_text(encoding="utf-8"))
    assert written["comparison_target_status"] == "apollo_vs_planning_control_evaluable"
    manifest = json.loads((tmp_path / "comparison" / "comparison_manifest.json").read_text(encoding="utf-8"))
    markdown = (tmp_path / "comparison" / "comparison_summary.md").read_text(encoding="utf-8")
    assert manifest["comparison_id"] == "follow_stop_static__apollo_cyberrt_vs_carla_builtin"
    assert manifest["scenario_case"] == "follow_stop_static"
    assert manifest["backends"] == ["apollo_cyberrt", "carla_builtin"]
    assert manifest["evaluable_run_dirs"]
    assert manifest["v_t_gap_files"]
    assert manifest["evidence_files"]
    assert manifest["claim_boundary"]
    assert "Handoff stage" in markdown
    assert "Handoff reason" in markdown
    assert "Control signature" in markdown
    assert "Control diagnosis" in markdown
    assert "Link blocker" in markdown
    assert "Control-health reason" in markdown
    assert "Lane-event class" in markdown
    assert "no_clear_control_blocker" in markdown


def test_scenario_comparison_curve_excludes_invalid_runs(tmp_path) -> None:
    run_a = _write_run(tmp_path, "apollo", "apollo_cyberrt", "apollo_reference_backend", "invalid", "backend_not_ready")
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")
    report = compare_scenario_runs([run_a, run_b])

    outputs = write_scenario_comparison(report, tmp_path / "comparison")

    with (tmp_path / "comparison" / "comparison_curves" / "v_t_gap_combined.csv").open(
        "r", encoding="utf-8"
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert {row["run_id"] for row in rows} == {"builtin"}


def test_same_backend_comparison_is_not_phase1_target_complete(tmp_path) -> None:
    run_a = _write_run(tmp_path, "builtin_a", "carla_builtin", "planning_control_backend", "success")
    run_b = _write_run(tmp_path, "builtin_b", "carla_builtin", "planning_control_backend", "degraded")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "comparable"
    assert report["comparison_target_status"] == "same_backend_regression"
    assert report["backend_coverage"]["evaluable_apollo_reference_backend"] == 0


def test_unknown_backend_type_keeps_comparison_target_unknown(tmp_path) -> None:
    run_a = _write_run(tmp_path, "legacy_a", "legacy_backend", None, "success")
    run_b = _write_run(tmp_path, "legacy_b", "legacy_backend", None, "success")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "comparable"
    assert report["comparison_target_status"] == "unknown_backend_coverage"


def test_scenario_comparison_propagates_handoff_failure_reasons(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "planning_control_handoff_missing",
        handoff_status="fail",
        handoff_failure_stage="planning_control_handoff",
        handoff_failure_reasons=["control_stream_ended_before_first_nonzero_planning"],
    )
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")

    report = compare_scenario_runs([run_a, run_b])

    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["apollo_control_handoff_status"] == "fail"
    assert apollo["apollo_control_handoff_failure_stage"] == "planning_control_handoff"
    assert apollo["apollo_control_handoff_failure_reasons"] == [
        "control_stream_ended_before_first_nonzero_planning"
    ]
    assert apollo["apollo_control_handoff_failure_reason"] == "control_stream_ended_before_first_nonzero_planning"


def test_scenario_comparison_prefers_process_health_crash_reason(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "control_process_failed",
        handoff_status="fail",
        handoff_failure_stage="process_health",
        handoff_failure_reasons=["planning_topic_nonzero_but_control_input_trajectory_zero"],
        handoff_process_crash_reason="tcmalloc_invalid_free",
    )
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")

    report = compare_scenario_runs([run_a, run_b])

    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["apollo_control_handoff_failure_stage"] == "process_health"
    assert apollo["apollo_control_handoff_failure_reason"] == "tcmalloc_invalid_free"


def test_scenario_comparison_refreshes_stale_derived_blocker_evidence(tmp_path) -> None:
    run_a = _write_run(
        tmp_path,
        "apollo",
        "apollo_cyberrt",
        "apollo_reference_backend",
        "failed",
        "control_process_failed",
        handoff_status="fail",
        handoff_failure_stage="process_health",
        handoff_process_crash_reason="tcmalloc_invalid_free",
    )
    status_path = run_a / "analysis" / "phase1_status" / "phase1_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["phase1_metrics"]["derived_blocker_evidence"] = {
        "available": True,
        "apollo_link_health": {
            "available": True,
            "primary_blocker": "planning_reference_line:stale_insufficient_data",
        },
        "control_health": {
            "available": True,
            "failure_reason": "stale_control_reason",
        },
    }
    status_path.write_text(json.dumps(status), encoding="utf-8")
    link_dir = run_a / "analysis" / "apollo_link_health"
    link_dir.mkdir(parents=True, exist_ok=True)
    (link_dir / "apollo_link_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_link_health.v1",
                "primary_blocker": "routing_planning_control_handoff:control_process_crash_before_control_output",
                "secondary_blockers": ["planning_reference_line:insufficient_data"],
            }
        ),
        encoding="utf-8",
    )
    control_dir = run_a / "analysis" / "control_health"
    control_dir.mkdir(parents=True, exist_ok=True)
    (control_dir / "control_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "control_health.v1",
                "status": "fail",
                "failure_reason": "control_process_crash_before_control_output",
            }
        ),
        encoding="utf-8",
    )
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")

    report = compare_scenario_runs([run_a, run_b])

    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    derived = apollo["phase1_metrics"]["derived_blocker_evidence"]
    assert (
        derived["apollo_link_health"]["primary_blocker"]
        == "routing_planning_control_handoff:control_process_crash_before_control_output"
    )
    assert derived["control_health"]["failure_reason"] == "control_process_crash_before_control_output"


def _write_run(
    tmp_path,
    name,
    backend,
    backend_type,
    status,
    reason=None,
    scenario_id="follow_stop_static",
    scenario_case=None,
    active_assists=None,
    missing_expected_artifacts=None,
    handoff_status=None,
    handoff_failure_stage=None,
    handoff_failure_reasons=None,
    handoff_process_crash_reason=None,
    safety_event_evidence=None,
    lane_invasion_context=None,
    scenario_interaction_evaluable=None,
    scenario_interaction_reason=None,
    target_metric_evaluable=None,
    target_metric_status=None,
    target_metric_reason=None,
):
    run = tmp_path / name
    analysis = run / "analysis" / "phase1_status"
    analysis.mkdir(parents=True)
    scenario_case = scenario_case or scenario_id
    manifest = {"run_id": name, "scenario_id": scenario_id, "scenario_case": scenario_case, "backend": backend}
    if backend_type is not None:
        manifest["backend_type"] = backend_type
    if active_assists is not None:
        manifest["assist_ledger"] = {
            "schema_version": "assist_ledger.v1",
            "active_assists": active_assists,
            "blocking_assists": [item for item in active_assists if item != "carla_direct_transport"],
            "non_blocking_assists": [item for item in active_assists if item == "carla_direct_transport"],
            "assist_confidence": "explicit",
            "source_artifact": "manifest",
            "can_claim_unassisted_natural_driving": not [
                item for item in active_assists if item != "carla_direct_transport"
            ],
        }
    (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (analysis / "phase1_status.json").write_text(
        json.dumps(
            {
                "run_id": name,
                "scenario_id": scenario_id,
                "scenario_case": scenario_case,
                "backend": backend,
                **({"backend_type": backend_type} if backend_type is not None else {}),
                "status": status,
                "failure_reason": reason,
                "evaluable": status != "invalid",
                **(
                    {"run_evaluable": status != "invalid"}
                    if status != "invalid"
                    else {"run_evaluable": False}
                ),
                **(
                    {"scenario_interaction_evaluable": scenario_interaction_evaluable}
                    if scenario_interaction_evaluable is not None
                    else {}
                ),
                **(
                    {"scenario_interaction_reason": scenario_interaction_reason}
                    if scenario_interaction_reason is not None
                    else {}
                ),
                **(
                    {"target_metric_evaluable": target_metric_evaluable}
                    if target_metric_evaluable is not None
                    else {}
                ),
                **(
                    {"target_metric_status": target_metric_status}
                    if target_metric_status is not None
                    else {}
                ),
                **(
                    {"target_metric_reason": target_metric_reason}
                    if target_metric_reason is not None
                    else {}
                ),
                "counts_as_backend_loss_for_target_scenario": (
                    status == "failed"
                    and scenario_interaction_evaluable is not False
                    and target_metric_evaluable is not False
                ),
                "invalid_reasons": [reason] if status == "invalid" and reason else [],
                "missing_expected_artifacts": missing_expected_artifacts or [],
                "phase1_metrics": {
                    "safety_event_evidence": _safety_event_evidence_payload(
                        status=status,
                        reason=reason,
                        safety_event_evidence=safety_event_evidence,
                    ),
                    **(
                        {"lane_invasion_context": lane_invasion_context}
                        if lane_invasion_context is not None
                        else {}
                    ),
                    "control_motion": {
                        "stuck_control_signature": "no_clear_control_blocker",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    if handoff_status is not None or handoff_failure_stage is not None:
        handoff_dir = run / "analysis" / "apollo_control_handoff"
        handoff_dir.mkdir(parents=True)
        (handoff_dir / "apollo_control_handoff_report.json").write_text(
            json.dumps(
                {
                    "schema_version": "apollo_control_handoff.v1",
                    "verdict": handoff_status or "pass",
                    "status": handoff_status or "pass",
                    "failure_stage": handoff_failure_stage or "none",
                    "process_health": {
                        "crash_reason": handoff_process_crash_reason,
                    },
                    "planning_control_handoff": {
                        "failure_reasons": handoff_failure_reasons or [],
                    },
                    "blocking_reasons": []
                    if (handoff_status or "pass") in {"pass", "warn"}
                    else [f"{handoff_failure_stage or 'unknown'}_failed"],
                }
            ),
            encoding="utf-8",
        )
    v_t_gap_dir = run / "analysis" / "v_t_gap"
    v_t_gap_dir.mkdir(parents=True)
    (v_t_gap_dir / "v_t_gap_report.json").write_text(json.dumps({"status": "pass", "rows": []}), encoding="utf-8")
    with (v_t_gap_dir / "v_t_gap.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time_s",
                "ego_speed_mps",
                "target_speed_mps",
                "gap_m",
                "relative_speed_mps",
                "target_actor_id",
                "target_actor_role",
                "gap_method",
                "gap_degraded",
                "gap_degraded_reason",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time_s": 0.0,
                "ego_speed_mps": 1.0,
                "target_speed_mps": 0.0,
                "gap_m": 10.0,
                "relative_speed_mps": -1.0,
                "target_actor_id": "lead-1",
                "target_actor_role": "lead_vehicle",
                "gap_method": "bumper_to_bumper_longitudinal_projection",
                "gap_degraded": "False",
                "gap_degraded_reason": "",
            }
        )
    return run


def _safety_event_evidence_payload(status, reason, safety_event_evidence):
    if isinstance(safety_event_evidence, dict):
        return safety_event_evidence
    if safety_event_evidence == "missing":
        return {
            "lane_invasion": {
                "available": False,
                "missing_reason": "lane_invasion_event_counter_missing",
            },
            "collision": {
                "available": False,
                "missing_reason": "collision_event_counter_missing",
            },
        }
    lane_count = 1.0 if status == "failed" and reason == "lane_invasion" else 0.0
    collision_count = 1.0 if status == "failed" and reason == "collision" else 0.0
    return {
        "lane_invasion": {
            "available": True,
            "summary_field_present": True,
            "summary_count": lane_count,
            "timeseries_field_present": True,
            "timeseries_field": "lane_invasion_count",
            "max_timeseries_count": lane_count,
            "count": lane_count,
            "source_fields": ["summary.lane_invasion_count", "timeseries.lane_invasion_count"],
        },
        "collision": {
            "available": True,
            "summary_field_present": True,
            "summary_count": collision_count,
            "timeseries_field_present": True,
            "timeseries_field": "collision_count",
            "max_timeseries_count": collision_count,
            "count": collision_count,
            "source_fields": ["summary.collision_count", "timeseries.collision_count"],
        },
    }
