from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status


def test_missing_timeseries_is_invalid_not_backend_failure(tmp_path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(json.dumps({"run_id": "r1", "backend": "apollo_cyberrt"}), encoding="utf-8")
    (run / "summary.json").write_text(json.dumps({"success": False}), encoding="utf-8")

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "no_timeseries"
    assert report["invalid_reasons"] == ["no_timeseries"]
    assert report["failed_reasons"] == []
    assert report["degraded_reasons"] == []
    assert report["evaluable"] is False
    assert report["run_evaluable"] is False
    assert report["scenario_interaction_evaluable"] is False
    assert report["scenario_interaction_reason"] == "run_invalid"
    assert report["target_metric_evaluable"] is False
    assert report["target_metric_reason"] == "run_invalid"
    assert report["counts_as_backend_loss_for_target_scenario"] is False
    assert any(path.endswith("manifest.json") for path in report["evidence_files"])
    assert "invalid_run_is_setup_or_evidence_failure_not_backend_loss" in report["notes"]


def test_platform_timeout_with_timeseries_is_evaluable_backend_failure(tmp_path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "platform_timeout"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "timeout"
    assert report["failed_reasons"] == ["timeout"]
    assert report["invalid_reasons"] == []
    assert report["run_evaluable"] is True
    assert report["required_artifacts"]["timeseries"] == "present"


def test_backend_not_ready_takes_priority_over_missing_timeseries(tmp_path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps({"run_id": "r1", "backend": "apollo_cyberrt", "backend_ready": False}),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "failure_reason": "backend_not_ready"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "backend_not_ready"
    assert report["invalid_reasons"] == ["backend_not_ready"]
    assert report["evaluable"] is False


def test_missing_target_actor_is_invalid(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "artifacts").mkdir()
    (run / "artifacts" / "fixed_scene_resolved.json").write_text(
        json.dumps({"target_actor_contract": {"status": "missing", "invalid_reason": "missing_target_actor"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "missing_target_actor"


def test_negative_gap_is_failed_unsafe_gap(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": -0.5}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "unsafe_gap"
    assert report["failed_reasons"] == ["unsafe_gap"]
    assert report["invalid_reasons"] == []
    assert report["degraded_reasons"] == []
    assert report["evaluable"] is True


def test_degraded_negative_gap_is_not_backend_unsafe_gap(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "warn",
                "gap_method_counts": {"bumper_to_bumper_longitudinal_projection_lateral_degraded": 1},
                "rows": [
                    {
                        "gap_m": -10.0,
                        "gap_degraded": True,
                        "gap_method": "bumper_to_bumper_longitudinal_projection_lateral_degraded",
                        "validity": "degraded",
                    }
                ],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "pass"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "degraded"
    assert report["failure_reason"] == "degraded_gap_method"
    assert report["failed_reasons"] == []
    assert report["degraded_reasons"] == ["degraded_gap_method"]
    assert report["evaluable"] is True


def test_scenario_actor_contract_failure_is_invalid_before_unsafe_gap(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": -0.5}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    actor_out = run / "analysis" / "scenario_actor_contract"
    actor_out.mkdir(parents=True)
    (actor_out / "scenario_actor_contract_report.json").write_text(
        json.dumps({"schema_version": "scenario_actor_contract.v1", "status": "fail"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "fixed_scene_failed"
    assert report["invalid_reasons"] == ["fixed_scene_failed"]
    assert report["failed_reasons"] == []
    assert report["evaluable"] is False


def test_fixed_scene_phase_trigger_not_reached_is_evaluable_backend_failure(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 200.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    fixed_out = run / "analysis" / "fixed_scene_contract"
    fixed_out.mkdir(parents=True)
    (fixed_out / "fixed_scene_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_contract.v1",
                "status": "fail",
                "blocking_reasons": [
                    "fixed_scene_required_phase_not_started",
                    "fixed_scene_required_phase_not_completed",
                    "stop_before_required_phase_completed",
                ],
                "spawn_feasibility": {"lead_vehicle": {"status": "warn", "blocking_reasons": []}},
            }
        ),
        encoding="utf-8",
    )
    actor_out = run / "analysis" / "scenario_actor_contract"
    actor_out.mkdir(parents=True)
    (actor_out / "scenario_actor_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "scenario_actor_contract.v1",
                "status": "fail",
                "blocking_reasons": ["lead_vehicle_never_observed_stopped"],
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "scenario_phase_trigger_not_reached"
    assert report["failed_reasons"] == ["scenario_phase_trigger_not_reached"]
    assert report["invalid_reasons"] == []
    assert report["evaluable"] is True


def test_early_lane_invasion_before_phase_activation_is_evaluable_failure(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "LANE_INVASION_HARD",
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "invalid",
                "invalid_reason": "missing_target_activation_phase",
                "target_actor_contract": {
                    "status": "resolved",
                    "target_actor_role": "cutin_vehicle",
                    "activation": {
                        "activation_semantics": "active_after_phase",
                        "active_after_phase": "cut_in_lane_change",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    fixed_out = run / "analysis" / "fixed_scene_contract"
    fixed_out.mkdir(parents=True)
    (fixed_out / "fixed_scene_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_contract.v1",
                "status": "fail",
                "blocking_reasons": [
                    "fixed_scene_required_phase_not_started",
                    "fixed_scene_required_phase_not_completed",
                    "duration_policy_route_end_not_reached",
                ],
                "spawn_feasibility": {"lead_vehicle": {"status": "pass", "blocking_reasons": []}},
            }
        ),
        encoding="utf-8",
    )
    actor_out = run / "analysis" / "scenario_actor_contract"
    actor_out.mkdir(parents=True)
    (actor_out / "scenario_actor_contract_report.json").write_text(
        json.dumps({"schema_version": "scenario_actor_contract.v1", "status": "insufficient_data"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["failed_reasons"] == ["lane_invasion"]
    assert report["invalid_reasons"] == []
    assert report["evaluable"] is True
    assert report["run_evaluable"] is True
    assert report["scenario_interaction_evaluable"] is False
    assert report["scenario_interaction_reason"] == "required_phase_not_reached"
    assert report["target_metric_evaluable"] is False
    assert report["target_metric_status"] == "invalid"
    assert report["target_metric_reason"] == "missing_target_activation_phase"
    assert report["counts_as_backend_loss_for_target_scenario"] is False


def test_lane_event_contract_can_block_backend_loss_attribution(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "LANE_INVASION_HARD",
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 20.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    lane_event = run / "analysis" / "baguang_lane_event_contract"
    lane_event.mkdir(parents=True)
    (lane_event / "baguang_lane_event_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "baguang_lane_event_contract.v1",
                "status": "warn",
                "claim_boundary": {
                    "lane_invasion_event_can_be_used_as_hard_gate": False,
                    "reason": "lane_invasion_trigger_before_static_footprint_crossing",
                },
                "run_reports": [
                    {
                        "reason": "lane_invasion_trigger_before_static_footprint_crossing",
                        "lane_invasion_event_can_be_used_as_hard_gate": False,
                        "departure_diagnostics": {
                            "classification": "downstream_progressive_lane_departure",
                            "interpretation": ["trigger_geometrically_implausible"],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["run_evaluable"] is True
    assert report["scenario_interaction_evaluable"] is True
    assert report["target_metric_evaluable"] is True
    assert report["safety_event_hard_gate_eligible"] is False
    assert report["safety_event_hard_gate_reason"] == "lane_invasion_trigger_before_static_footprint_crossing"
    assert report["counts_as_backend_loss_for_target_scenario"] is False
    assert report["primary_behavior_blocker"] == "lane_event_contract_quarantines_lane_invasion"
    assert report["behavior_blocker_layer"] == "lane_event_contract"
    assert (
        report["behavior_blocker_evidence"]["lane_invasion_event_can_be_used_as_hard_gate"]
        is False
    )


def test_duration_policy_not_reached_after_target_interaction_is_evaluable_failure(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "LANE_INVASION",
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "warn",
                "rows": [{"gap_m": 20.0}],
                "target_actor_contract": {
                    "status": "resolved",
                    "target_actor_role": "lead_vehicle",
                    "activation": {
                        "activation_semantics": "active_after_phase",
                        "active_after_phase": "cut_in_lane_change",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    fixed_out = run / "analysis" / "fixed_scene_contract"
    fixed_out.mkdir(parents=True)
    (fixed_out / "fixed_scene_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_contract.v1",
                "status": "fail",
                "blocking_reasons": ["duration_policy_route_end_not_reached"],
                "metrics": {
                    "required_phase_start_ratio": 1.0,
                    "required_phase_completion_ratio": 1.0,
                },
                "spawn_feasibility": {"lead_vehicle": {"status": "pass", "blocking_reasons": []}},
            }
        ),
        encoding="utf-8",
    )
    actor_out = run / "analysis" / "scenario_actor_contract"
    actor_out.mkdir(parents=True)
    (actor_out / "scenario_actor_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "scenario_actor_contract.v1",
                "status": "pass",
                "blocking_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["run_evaluable"] is True
    assert report["scenario_interaction_evaluable"] is True
    assert report["target_metric_evaluable"] is True
    assert report["counts_as_backend_loss_for_target_scenario"] is True


def test_duration_and_phase_completion_blockers_after_actor_contract_pass_are_evaluable_failure(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "COLLISION",
                "lane_invasion_count": 0,
                "collision_count": 1,
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [{"gap_m": 16.0, "ego_speed_mps": 19.4, "target_speed_mps": 19.4}],
                "target_actor_contract": {"status": "resolved", "target_actor_role": "lead_vehicle"},
            }
        ),
        encoding="utf-8",
    )
    fixed_out = run / "analysis" / "fixed_scene_contract"
    fixed_out.mkdir(parents=True)
    (fixed_out / "fixed_scene_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_contract.v1",
                "status": "fail",
                "blocking_reasons": [
                    "fixed_scene_required_phase_not_completed",
                    "duration_policy_route_end_not_reached",
                ],
                "spawn_feasibility": {"lead_vehicle": {"status": "pass", "blocking_reasons": []}},
            }
        ),
        encoding="utf-8",
    )
    actor_out = run / "analysis" / "scenario_actor_contract"
    actor_out.mkdir(parents=True)
    (actor_out / "scenario_actor_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "scenario_actor_contract.v1",
                "status": "pass",
                "blocking_reasons": [],
                "metrics": {
                    "phase_completion_ratio": 1.0,
                    "speed_profile_error_p95_mps": 0.15,
                },
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "collision"
    assert report["run_evaluable"] is True
    assert report["scenario_interaction_evaluable"] is True
    assert report["target_metric_evaluable"] is True
    assert report["counts_as_backend_loss_for_target_scenario"] is True


def test_ego_initial_speed_not_materialized_blocks_target_interaction_loss_claim(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "LANE_INVASION",
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        ),
        encoding="utf-8",
    )
    artifacts = run / "artifacts"
    artifacts.mkdir(exist_ok=True)
    (artifacts / "fixed_scene_resolved.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_storyboard.v1",
                "roles": {"ego": {"initial_speed_mps": 19.44}},
                "params": {"ego_initial_speed_mps": 19.44},
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "warn",
                "rows": [
                    {"gap_m": 20.0, "ego_speed_mps": 2.4, "target_speed_mps": 19.4},
                    {"gap_m": 40.0, "ego_speed_mps": 2.6, "target_speed_mps": 19.3},
                ],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["run_evaluable"] is True
    assert report["scenario_interaction_evaluable"] is False
    assert report["scenario_interaction_reason"] == "ego_initial_speed_not_materialized"
    assert report["target_metric_evaluable"] is True
    assert report["counts_as_backend_loss_for_target_scenario"] is False
    initial = report["phase1_metrics"]["initial_condition_materialization"]
    assert initial["status"] == "fail"
    assert initial["expected_ego_initial_speed_mps"] == 19.44
    assert initial["observed_ego_initial_speed_mps"] == 2.5
    assert initial["delta_mps"] > initial["tolerance_mps"]

    write_phase1_status(report, run / "analysis" / "phase1_status")
    summary_text = (run / "analysis" / "phase1_status" / "phase1_status_summary.md").read_text(
        encoding="utf-8"
    )
    assert "Initial Condition Materialization" in summary_text
    assert "ego_initial_speed_not_materialized" in summary_text


def test_ego_initial_speed_leading_window_materialized_allows_behavior_loss_claim(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "exit_reason": "LANE_INVASION",
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        ),
        encoding="utf-8",
    )
    artifacts = run / "artifacts"
    artifacts.mkdir(exist_ok=True)
    (artifacts / "fixed_scene_resolved.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_storyboard.v1",
                "roles": {"ego": {"initial_speed_mps": 19.44}},
                "params": {"ego_initial_speed_mps": 19.44},
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "ego_initial_state_materialization.json").write_text(
        json.dumps(
            {
                "schema_version": "ego_initial_state_materialization.v1",
                "status": "pass",
                "expected_ego_initial_speed_mps": 19.44,
                "observed_ego_speed_mps": 0.98,
                "warnings": ["observed_speed_may_lag_until_next_carla_tick"],
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "warn",
                "rows": [
                    {"gap_m": 16.0, "ego_speed_mps": 19.48, "target_speed_mps": 19.52},
                    {"gap_m": 16.1, "ego_speed_mps": 19.52, "target_speed_mps": 19.44},
                    {"gap_m": 16.1, "ego_speed_mps": 19.57, "target_speed_mps": 19.44},
                    {"gap_m": 16.2, "ego_speed_mps": 18.0, "target_speed_mps": 19.44},
                    {"gap_m": 16.3, "ego_speed_mps": 15.5, "target_speed_mps": 19.44},
                    {"gap_m": 16.5, "ego_speed_mps": 13.5, "target_speed_mps": 19.44},
                    {"gap_m": 16.9, "ego_speed_mps": 11.9, "target_speed_mps": 19.44},
                    {"gap_m": 17.3, "ego_speed_mps": 10.1, "target_speed_mps": 19.44},
                    {"gap_m": 17.8, "ego_speed_mps": 9.2, "target_speed_mps": 19.44},
                    {"gap_m": 18.3, "ego_speed_mps": 8.1, "target_speed_mps": 19.44},
                ],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["run_evaluable"] is True
    assert report["scenario_interaction_evaluable"] is True
    assert report["scenario_interaction_reason"] is None
    assert report["target_metric_evaluable"] is True
    assert report["counts_as_backend_loss_for_target_scenario"] is True
    initial = report["phase1_metrics"]["initial_condition_materialization"]
    assert initial["status"] == "pass"
    assert initial["observed_ego_initial_speed_source"] == "v_t_gap_leading_window"
    assert initial["selected_observed_ego_initial_speed_window_count"] == 3
    assert initial["observed_ego_initial_speed_mps"] == 19.52


def test_large_final_gap_is_degraded(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [{"gap_m": 100.0, "ego_speed_mps": 1.0}, {"gap_m": 120.0, "ego_speed_mps": 2.0}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "degraded"
    assert report["failure_reason"] == "large_final_gap"
    assert report["degraded_reasons"] == ["large_final_gap"]
    assert report["failed_reasons"] == []
    assert report["invalid_reasons"] == []
    assert report["evaluable"] is True
    assert report["scenario_case"] == "baguang_follow_stop_static_300m"
    assert report["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert report["target_actor_contract"]["status"] == "resolved"
    assert report["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert report["phase1_metrics"]["initial_gap_m"] == 100.0
    assert report["phase1_metrics"]["final_gap_m"] == 120.0
    assert report["phase1_metrics"]["gap_delta_m"] == 20.0
    assert report["phase1_metrics"]["max_ego_speed_mps"] == 2.0


def test_ego_not_moving_with_applied_brake_is_control_apply_mismatch(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "status": "fail",
                "fail_reason": "NO_SENSOR_DATA",
                "acceptance": {"failure_codes": ["NO_SENSOR_DATA", "EGO_NOT_MOVING"]},
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps,cmd_throttle,cmd_brake,applied_throttle,applied_brake\n"
        "0.0,1.0,1.0,0.0,0.0,1.0\n"
        "1.0,0.0,1.0,0.0,0.0,1.0\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [
                    {"gap_m": 295.0, "ego_speed_mps": 1.0, "target_speed_mps": 0.0},
                    {"gap_m": 295.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0},
                ],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "control_apply_mismatch"
    assert report["failed_reasons"] == ["control_apply_mismatch"]
    assert report["degraded_reasons"] == []
    assert report["evaluable"] is True
    assert report["original_failure_reason"] == "NO_SENSOR_DATA"
    assert report["phase1_metrics"]["final_gap_m"] == 295.0
    assert report["phase1_metrics"]["final_ego_speed_mps"] == 0.0
    control = report["phase1_metrics"]["control_motion"]
    assert control["stuck_control_signature"] == "command_throttle_but_applied_brake"
    assert control["command_throttle"]["source_field"] == "cmd_throttle"
    assert control["command_throttle"]["nonzero_ratio"] == 1.0
    assert control["applied_brake"]["source_field"] == "applied_brake"
    assert control["applied_brake"]["nonzero_ratio"] == 1.0
    assert control["applied_throttle"]["nonzero_ratio"] == 0.0


def test_external_stack_placeholder_commands_do_not_override_bridge_control_trace(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "status": "fail",
                "fail_reason": "NO_SENSOR_DATA",
                "acceptance": {"failure_codes": ["NO_SENSOR_DATA", "EGO_NOT_MOVING"]},
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "sim_time,control_source,ego_speed_mps,cmd_throttle,cmd_brake,applied_throttle,applied_brake\n"
        "0.0,external_stack,0.0,1.0,0.0,0.0,1.0\n"
        "1.0,external_stack,0.0,1.0,0.0,0.0,1.0\n",
        encoding="utf-8",
    )
    artifacts = run / "artifacts"
    artifacts.mkdir()
    (artifacts / "bridge_control_decode.jsonl").write_text(
        json.dumps(
            {
                "commanded_throttle": 0.0,
                "commanded_brake": 0.15,
                "planning_lateral_contract_reason": "zero_trajectory_points",
                "planning_lateral_contract_valid": False,
                "planning_lateral_latest_point_count": 0,
            }
        )
        + "\n"
        + json.dumps(
            {
                "commanded_throttle": 0.0,
                "commanded_brake": 0.15,
                "planning_lateral_contract_reason": "zero_trajectory_points",
                "planning_lateral_contract_valid": False,
                "planning_lateral_latest_point_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "apollo_control_raw.jsonl").write_text(
        json.dumps(
            {
                "apollo_control_raw": {
                    "throttle": 0.0,
                    "brake": 15.0,
                    "debug_input_trajectory_header_sequence_num": 0,
                    "debug_input_latest_replan_trajectory_header_sequence_num": 0,
                    "debug_input_trajectory_header_timestamp_sec": 0.0,
                    "debug_input_latest_replan_trajectory_header_timestamp_sec": 0.0,
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "planning_topic_debug.jsonl").write_text(
        json.dumps({"trajectory_point_count": 0, "reference_line_count": 0}) + "\n"
        + json.dumps({"trajectory_point_count": 128, "reference_line_count": 0}) + "\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [
                    {"gap_m": 295.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0},
                    {"gap_m": 295.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0},
                ],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "pass"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "planning_control_handoff_missing"
    assert report["failed_reasons"] == ["planning_control_handoff_missing"]
    control = report["phase1_metrics"]["control_motion"]
    assert control["command_source"] == "bridge_control_decode"
    assert control["timeseries_command_fields_role"] == "harness_placeholder"
    assert control["stuck_control_signature"] == "source_brake_applied"
    assert control["source_brake_interpretation"] == "planning_topic_nonzero_but_control_input_trajectory_zero"
    assert control["command_brake"]["source_field"] == "commanded_brake"
    assert control["command_brake"]["nonzero_ratio"] == 1.0
    context = control["source_control_context"]
    assert context["bridge_control_decode"]["planning_lateral_contract_reason_counts"] == {
        "zero_trajectory_points": 2
    }
    assert context["apollo_control_raw"]["input_trajectory_sequence"]["nonzero_ratio"] == 0.0
    assert context["planning_topic_debug"]["trajectory_point_count"]["nonzero_ratio"] == 0.5


def test_phase1_status_prefers_precise_apollo_handoff_failure_reason(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "status": "fail",
                "fail_reason": "NO_SENSOR_DATA",
                "acceptance": {"failure_codes": ["NO_SENSOR_DATA", "EGO_NOT_MOVING"]},
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "sim_time,control_source,ego_speed_mps,cmd_throttle,cmd_brake,applied_throttle,applied_brake\n"
        "0.0,external_stack,0.0,1.0,0.0,0.0,1.0\n"
        "1.0,external_stack,0.0,1.0,0.0,0.0,1.0\n",
        encoding="utf-8",
    )
    artifacts = run / "artifacts"
    artifacts.mkdir()
    (artifacts / "bridge_control_decode.jsonl").write_text(
        json.dumps(
            {
                "commanded_throttle": 0.0,
                "commanded_brake": 0.15,
                "planning_lateral_contract_reason": "zero_trajectory_points",
                "planning_lateral_contract_valid": False,
                "planning_lateral_latest_point_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "apollo_control_raw.jsonl").write_text(
        json.dumps(
            {
                "apollo_control_raw": {
                    "throttle": 0.0,
                    "brake": 15.0,
                    "debug_input_trajectory_header_sequence_num": 0,
                    "debug_input_latest_replan_trajectory_header_sequence_num": 0,
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "planning_topic_debug.jsonl").write_text(
        json.dumps({"trajectory_point_count": 128, "reference_line_count": 1}) + "\n",
        encoding="utf-8",
    )
    handoff_out = run / "analysis" / "apollo_control_handoff"
    handoff_out.mkdir(parents=True)
    (handoff_out / "apollo_control_handoff_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_control_handoff.v1",
                "verdict": "fail",
                "failure_stage": "planning_control_handoff",
                "planning_control_handoff": {
                    "failure_reasons": ["control_stream_ended_before_first_nonzero_planning"],
                    "first_nonzero_planning_timestamp_sec": 10.0,
                    "last_control_raw_timestamp_sec": 9.9,
                    "last_control_consume_timestamp_sec": 9.9,
                    "control_rows_after_first_nonzero_planning": 0,
                    "control_stream_end_before_first_nonzero_planning_delta_ms": 100.0,
                },
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [{"gap_m": 295.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "pass"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "planning_control_handoff_missing"
    control = report["phase1_metrics"]["control_motion"]
    assert control["source_brake_interpretation"] == "control_stream_ended_before_first_nonzero_planning"
    handoff_context = control["source_control_context"]["apollo_control_handoff"]
    assert handoff_context["failure_reasons"] == ["control_stream_ended_before_first_nonzero_planning"]
    assert handoff_context["control_rows_after_first_nonzero_planning"] == 0


def test_phase1_status_prefers_stale_planning_handoff_reason(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "status": "fail",
                "fail_reason": "NO_SENSOR_DATA",
                "acceptance": {"failure_codes": ["NO_SENSOR_DATA", "EGO_NOT_MOVING"]},
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps,throttle_applied,brake_applied\n"
        "0.0,0.0,0.0,0.15\n",
        encoding="utf-8",
    )
    artifacts = run / "artifacts"
    artifacts.mkdir(exist_ok=True)
    (artifacts / "bridge_control_decode.jsonl").write_text(
        json.dumps({"commanded_throttle": 0.0, "commanded_brake": 0.15}) + "\n",
        encoding="utf-8",
    )
    (artifacts / "apollo_control_raw.jsonl").write_text(
        json.dumps(
            {
                "apollo_control_raw": {
                    "throttle": 0.0,
                    "brake": 15.0,
                    "debug_input_trajectory_header_sequence_num": 0,
                    "debug_input_latest_replan_trajectory_header_sequence_num": 0,
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "planning_topic_debug.jsonl").write_text(
        json.dumps({"trajectory_point_count": 1, "reference_line_count": 0}) + "\n",
        encoding="utf-8",
    )
    handoff_out = run / "analysis" / "apollo_control_handoff"
    handoff_out.mkdir(parents=True)
    (handoff_out / "apollo_control_handoff_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_control_handoff.v1",
                "verdict": "fail",
                "failure_stage": "planning_control_handoff",
                "planning_control_handoff": {
                    "failure_reasons": ["planning_nonzero_stale_before_control_consume"],
                    "first_nonzero_planning_timestamp_sec": 11.0,
                    "last_control_raw_timestamp_sec": 13.0,
                    "last_control_consume_timestamp_sec": 13.0,
                    "control_rows_after_first_nonzero_planning": 1,
                    "planning_nonzero_to_first_control_gap_ms": 2000.0,
                },
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [{"gap_m": 20.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "pass"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "planning_control_handoff_missing"
    control = report["phase1_metrics"]["control_motion"]
    assert control["source_brake_interpretation"] == "planning_nonzero_stale_before_control_consume"
    handoff_context = control["source_control_context"]["apollo_control_handoff"]
    assert handoff_context["failure_reasons"] == ["planning_nonzero_stale_before_control_consume"]


def test_phase1_status_reports_apollo_control_process_failed_from_handoff(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "status": "fail",
                "fail_reason": "NO_CONTROL_LOG",
                "acceptance": {"failure_codes": ["NO_CONTROL_LOG"]},
            }
        ),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [{"gap_m": 295.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "pass"}),
        encoding="utf-8",
    )
    handoff_out = run / "analysis" / "apollo_control_handoff"
    handoff_out.mkdir(parents=True)
    (handoff_out / "apollo_control_handoff_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_control_handoff.v1",
                "verdict": "fail",
                "failure_stage": "process_health",
                "blocking_reasons": ["process_health_failed"],
                "process_health": {
                    "status": "fail",
                    "started": True,
                    "alive_after_5s": False,
                    "alive_at_end": False,
                    "crash_reason": "module_exited",
                },
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "control_process_failed"
    assert report["failed_reasons"] == ["control_process_failed"]


def test_ego_not_moving_without_control_mismatch_remains_stuck(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "status": "fail",
                "fail_reason": "NO_SENSOR_DATA",
                "acceptance": {"failure_codes": ["EGO_NOT_MOVING"]},
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps,cmd_throttle,cmd_brake,applied_throttle,applied_brake\n"
        "0.0,0.0,0.0,0.0,0.0,0.0\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [{"gap_m": 100.0, "ego_speed_mps": 0.0, "target_speed_mps": 0.0}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "stuck"
    assert report["phase1_metrics"]["control_motion"]["stuck_control_signature"] == "ego_never_moved"


def test_degraded_gap_method_is_not_rewritten_as_large_final_gap(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "warn",
                "degraded_reason_counts": {"existing_lead_gap_m_degraded": 2},
                "rows": [{"gap_m": 120.0, "gap_degraded": True}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "degraded"
    assert report["failure_reason"] == "degraded_gap_method"
    assert report["degraded_reasons"] == ["degraded_gap_method"]


def test_missing_v_t_gap_report_is_invalid_missing_required_artifact(tmp_path) -> None:
    run = _base_run(tmp_path)

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "missing_required_artifact"
    assert report["invalid_reasons"] == ["missing_required_artifact"]
    assert report["evaluable"] is False


def test_unclassified_summary_failure_is_invalid_not_success(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps({"success": False, "status": "fail", "fail_reason": "NO_SENSOR_DATA"}),
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "rows": [
                    {"gap_m": 300.0, "ego_speed_mps": 1.0, "target_speed_mps": 0.0},
                    {"gap_m": 295.0, "ego_speed_mps": 2.0, "target_speed_mps": 0.0},
                ],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "missing_required_artifact"
    assert report["invalid_reasons"] == ["missing_required_artifact"]
    assert report["failed_reasons"] == []
    assert report["evaluable"] is False
    assert report["original_failure_reason"] == "NO_SENSOR_DATA"
    assert "invalid_run_is_setup_or_evidence_failure_not_backend_loss" in report["notes"]


def test_route_establishment_latency_is_evaluable_route_only_failure(tmp_path) -> None:
    run = tmp_path / "apollo_curve"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_curve",
                "scenario_id": "town01_curve217_diagnostic",
                "scenario_class": "curve_diagnostic",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
                "target_actor_contract": {
                    "status": "not_required",
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "fail_reason": "ROUTE_ESTABLISHMENT_LATENCY_SEC"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "not_applicable",
                "target_actor_contract": {
                    "status": "not_required",
                    "scenario_class": "curve_diagnostic",
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "route_establishment_latency"
    assert report["failed_reasons"] == ["route_establishment_latency"]
    assert report["invalid_reasons"] == []
    assert report["evaluable"] is True
    assert report["original_failure_reason"] == "ROUTE_ESTABLISHMENT_LATENCY_SEC"
    assert report["scenario_case"] == "town01_curve217_diagnostic"
    assert report["target_actor_contract"]["status"] == "not_required"


def test_legacy_apollo_lane_invasion_exit_reason_is_evaluable_failure(tmp_path) -> None:
    run = tmp_path / "apollo_lane"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_lane",
                "scenario_case": "town01_lane_keep_097",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "LANE_INVASION"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame,sim_time,ego_x,ego_y,ego_speed_mps,lane_invasion_count,cross_track_error,heading_error,applied_throttle,applied_brake,applied_steer,control_source",
                "0,10.0,100.0,5.0,1.0,0,0.01,0.001,0.0,0.0,0.0,external_stack",
                "1,10.05,101.2,5.1,3.5,1,0.02,0.002,0.4,0.0,-0.01,external_stack",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "not_applicable",
                "target_actor_contract": {
                    "status": "not_required",
                    "scenario_class": "lane_keep",
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["evaluable"] is True
    assert report["target_actor_contract"]["status"] == "not_required"
    context = report["phase1_metrics"]["lane_invasion_context"]
    assert context["available"] is True
    assert context["first_row_index"] == 1
    assert context["ego_speed_mps"] == 3.5
    assert context["ego_displacement_from_start_m"] > 1.0
    assert context["cross_track_error_m"] == 0.02
    assert context["control_source"] == "external_stack"
    safety = report["phase1_metrics"]["safety_event_evidence"]
    assert safety["lane_invasion"]["available"] is True
    assert safety["lane_invasion"]["timeseries_field"] == "lane_invasion_count"
    assert safety["lane_invasion"]["max_timeseries_count"] == 1.0
    assert safety["collision"]["available"] is False
    assert safety["collision"]["missing_reason"] == "collision_event_counter_missing"

    paths = write_phase1_status(report, run / "analysis" / "phase1_status")
    summary_text = (run / "analysis" / "phase1_status" / "phase1_status_summary.md").read_text(encoding="utf-8")
    assert paths["summary"].endswith("phase1_status_summary.md")
    assert "Lane Invasion Context" in summary_text
    assert "ego_speed_mps: `3.5`" in summary_text


def test_phase1_status_echoes_derived_blocker_evidence_without_reclassifying(tmp_path) -> None:
    run = tmp_path / "apollo_lane"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_lane",
                "scenario_case": "baguang_follow_stop_static_300m_spawn2m",
                "scenario_id": "baguang_follow_stop_static_300m_spawn2m",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "LANE_INVASION"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame,sim_time,ego_x,ego_y,ego_speed_mps,lane_invasion_count,cross_track_error,heading_error,applied_throttle,applied_brake,applied_steer,control_source",
                "0,10.0,100.0,5.0,1.0,0,-0.10,-0.01,0.2,0.0,-0.01,external_stack",
                "1,10.05,101.2,5.1,3.5,1,-0.85,-0.05,0.0,0.3,-0.04,external_stack",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "not_applicable",
                "target_actor_contract": {
                    "status": "not_required",
                    "scenario_class": "lane_keep",
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )
    control_health = run / "analysis" / "control_health"
    control_health.mkdir(parents=True)
    (control_health / "control_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "control_health_report.v1",
                "status": "fail",
                "failure_reason": "apollo_raw_command_oscillation",
                "routing_materialized": True,
                "planning_materialized": True,
                "control_handoff_status": "control_consuming_with_nonzero_planning",
                "control_semantics_primary_factor": "same_trajectory_longitudinal_control_switching",
                "control_semantics_suspected_factors": ["same_trajectory_longitudinal_control_switching"],
                "metrics": {
                    "materialization_evidence": {
                        "warnings": ["summary_routing_materialized_false_overridden_by_artifact"]
                    }
                },
                "warnings": ["external_control_trace_from_bridge_artifacts"],
            }
        ),
        encoding="utf-8",
    )
    link_health = run / "analysis" / "apollo_link_health"
    link_health.mkdir(parents=True)
    (link_health / "apollo_link_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_link_health.v1",
                "primary_blocker": "route_establishment:long_goal_not_compatible_with_scenario_route",
                "secondary_blockers": ["control_mapping_apply:apollo_raw_command_oscillation"],
                "can_claim_unassisted_natural_driving": False,
            }
        ),
        encoding="utf-8",
    )
    lane_event = run / "analysis" / "baguang_lane_event_contract"
    lane_event.mkdir(parents=True)
    (lane_event / "baguang_lane_event_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "baguang_lane_event_contract.v1",
                "status": "fail",
                "run_reports": [
                    {
                        "reason": "possible_real_lane_departure_or_unclassified_lane_event",
                        "departure_diagnostics": {
                            "classification": "downstream_progressive_lane_departure",
                            "interpretation": ["raw_steer_same_sign_as_cross_track_error"],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert report["primary_behavior_blocker"] == "lane_departure_with_source_steer_same_sign"
    assert report["behavior_blocker_layer"] == "lane_event_contract"
    assert "raw->mapped->applied steer" in report["behavior_next_action"]
    assert (
        report["behavior_blocker_evidence"]["departure_classification"]
        == "downstream_progressive_lane_departure"
    )
    derived = report["phase1_metrics"]["derived_blocker_evidence"]
    assert derived["available"] is True
    assert derived["control_health"]["failure_reason"] == "apollo_raw_command_oscillation"
    assert derived["control_health"]["routing_materialized"] is True
    assert derived["control_health"]["materialization_warnings"] == [
        "summary_routing_materialized_false_overridden_by_artifact"
    ]
    assert (
        derived["apollo_link_health"]["primary_blocker"]
        == "route_establishment:long_goal_not_compatible_with_scenario_route"
    )
    assert (
        derived["baguang_lane_event_contract"]["departure_classification"]
        == "downstream_progressive_lane_departure"
    )

    paths = write_phase1_status(report, run / "analysis" / "phase1_status")
    summary_text = (run / "analysis" / "phase1_status" / "phase1_status_summary.md").read_text(encoding="utf-8")
    assert paths["summary"].endswith("phase1_status_summary.md")
    assert "Derived Blocker Evidence" in summary_text
    assert "Primary Behavior Blocker" in summary_text
    assert "lane_departure_with_source_steer_same_sign" in summary_text
    assert "apollo_raw_command_oscillation" in summary_text


def test_phase1_status_uses_lateral_sign_convention_caveat_for_lane_departure(
    tmp_path,
) -> None:
    run = tmp_path / "apollo_lane_sign_convention"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_lane_sign_convention",
                "scenario_case": "baguang_cut_in_35kph_left_to_right_10m",
                "scenario_id": "baguang_cut_in_35kph_left_to_right_10m",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "LANE_INVASION"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame,sim_time,ego_x,ego_y,ego_speed_mps,lane_invasion_count,cross_track_error,heading_error,applied_throttle,applied_brake,applied_steer,control_source",
                "0,10.0,100.0,5.0,1.0,0,-0.10,-0.01,0.2,0.0,-0.01,external_stack",
                "1,10.05,101.2,5.1,3.5,1,0.85,-0.05,0.0,0.3,0.04,external_stack",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "target_actor_contract": {"status": "pass"},
                "rows": [{"gap_m": 10.0, "ego_speed_mps": 3.5, "target_speed_mps": 3.0}],
            }
        ),
        encoding="utf-8",
    )
    link_health = run / "analysis" / "apollo_link_health"
    link_health.mkdir(parents=True)
    (link_health / "apollo_link_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_link_health.v1",
                "primary_blocker": "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate",
                "secondary_blockers": ["natural_driving_outcome:insufficient_data"],
                "can_claim_unassisted_natural_driving": False,
                "layers": {
                    "apollo_lateral_semantics": {
                        "key_metrics": {
                            "route_simple_lat_sign_convention_candidate": True,
                            "route_lateral_provenance_evidence_level": "hdmap_projection_consistency",
                            "route_lateral_provenance_interpretation": (
                                "route_lateral_sign_supported_by_hdmap_projection_consistency"
                            ),
                            "route_simple_lat_opposite_sign_abs_sum_p95_m": 0.028,
                            "route_simple_lat_abs_magnitude_delta_p95_m": 0.028,
                            "route_simple_lat_alignment_interpretation": (
                                "opposite_sign_matching_magnitude_suggests_route_simple_lat_sign_convention_mismatch"
                            ),
                            "simple_lat_station_coverage_status": (
                                "projection_current_matched_target_s_available"
                            ),
                            "simple_lat_station_frame_classification": (
                                "local_station_frame_offset_candidate"
                            ),
                            "simple_lat_missing_station_fields": [],
                            "simple_lat_current_station_projection_s_delta_p95_m": 25.08,
                            "simple_lat_matched_s_projection_s_delta_p95_m": 23.64,
                            "simple_lat_target_s_projection_s_delta_p95_m": 25.12,
                            "simple_lat_target_s_current_station_delta_p95_m": 0.043,
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    lane_event = run / "analysis" / "baguang_lane_event_contract"
    lane_event.mkdir(parents=True)
    (lane_event / "baguang_lane_event_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "baguang_lane_event_contract.v1",
                "status": "fail",
                "run_reports": [
                    {
                        "reason": "possible_real_lane_departure_or_unclassified_lane_event",
                        "lane_invasion_event_can_be_used_as_hard_gate": True,
                        "departure_diagnostics": {
                            "classification": "downstream_progressive_lane_departure",
                            "interpretation": ["raw_steer_same_sign_as_cross_track_error"],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert (
        report["primary_behavior_blocker"]
        == "lane_departure_with_route_simple_lat_sign_convention_candidate"
    )
    assert report["behavior_blocker_layer"] == "apollo_lateral_semantics"
    assert "Validate the sign convention" in report["behavior_next_action"]
    evidence = report["behavior_blocker_evidence"]
    assert evidence["apollo_link_health_primary_blocker"] == (
        "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
    )
    assert evidence["phase1_relevant_apollo_link_blocker"] == (
        "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
    )
    assert evidence["route_simple_lat_sign_convention_candidate"] is True
    assert evidence["route_lateral_provenance_evidence_level"] == "hdmap_projection_consistency"
    assert evidence["route_lateral_provenance_interpretation"] == (
        "route_lateral_sign_supported_by_hdmap_projection_consistency"
    )
    assert evidence["route_simple_lat_opposite_sign_abs_sum_p95_m"] == 0.028
    assert evidence["simple_lat_station_coverage_status"] == (
        "projection_current_matched_target_s_available"
    )
    assert evidence["simple_lat_station_frame_classification"] == (
        "local_station_frame_offset_candidate"
    )
    assert evidence["simple_lat_missing_station_fields"] == []
    assert evidence["simple_lat_current_station_projection_s_delta_p95_m"] == 25.08
    assert evidence["simple_lat_matched_s_projection_s_delta_p95_m"] == 23.64
    assert evidence["simple_lat_target_s_projection_s_delta_p95_m"] == 25.12
    assert evidence["simple_lat_target_s_current_station_delta_p95_m"] == 0.043

    link_report_path = link_health / "apollo_link_health_report.json"
    link_report = json.loads(link_report_path.read_text(encoding="utf-8"))
    metrics = link_report["layers"]["apollo_lateral_semantics"]["key_metrics"]
    metrics.update(
        {
            "projection_route_sample_sign_contract_status": "available",
            "projection_route_sample_sign_contract_classification": (
                "timeseries_route_lateral_sign_inverted_vs_projection_route_samples"
            ),
            "projection_route_sample_count": 61,
            "projection_route_sample_matched_sample_count": 335,
            "projection_route_sample_lateral_source_field": "cross_track_error",
            "projection_route_sample_timeseries_opposite_sign_ratio": 1.0,
            "projection_route_sample_simple_lat_same_sign_ratio": 1.0,
            "route_lateral_field_semantics_status": "available",
            "route_lateral_field_semantics_classification": (
                "route_lateral_field_opposite_signed_to_apollo_projection"
            ),
            "route_lateral_field_sign_sensitive_gate_allowed": False,
            "route_lateral_field_absolute_magnitude_gate_allowed": True,
            "route_lateral_field_recommended_gate_policy": (
                "absolute_magnitude_only_until_canonical_sign_declared"
            ),
            "route_lateral_field_recommended_action": (
                "relabel_or_explicitly_convert_before_sign_sensitive_gate"
            ),
            }
        )
    link_report["layers"]["planning_reference_line"] = {
        "key_metrics": {
            "planning_debug_presence": {
                "classification": "routing_present_reference_line_empty",
                "last_diagnosis": "routing_present_reference_line_empty",
                "last_reference_line_path": "debug.planning_data.reference_line",
                "last_routing_path": "debug.planning_data.routing",
                "reference_line_nonempty_ratio": 0.0,
                "routing_segment_nonempty_ratio": 0.52,
            },
            "planning_debug_presence_classification": "routing_present_reference_line_empty",
            "planning_debug_presence_last_diagnosis": "routing_present_reference_line_empty",
            "planning_debug_presence_last_reference_line_path": "debug.planning_data.reference_line",
            "planning_debug_presence_last_routing_path": "debug.planning_data.routing",
            "planning_debug_presence_reference_line_nonempty_ratio": 0.0,
            "planning_debug_presence_routing_segment_nonempty_ratio": 0.52,
            "planning_materialization_summary": {
                "classification": "route_segments_ready_trajectory_nonzero_reference_line_empty",
                "claim_window_source": "after_routing_segment_available",
                "claim_window": {
                    "route_segments_ready_ratio": 1.0,
                    "reference_line_empty_with_route_segments_ready_ratio": 1.0,
                    "lane_follow_stage_ratio": 1.0,
                },
            },
            "reference_debug_diagnostic": {
                "field_inventory": {
                    "field_gap_classification": (
                        "reference_line_counter_missing_but_planning_control_surrogates_present"
                    ),
                    "reference_line_count_positive_count": 0,
                    "claim_window_source": "after_routing_segment_available",
                    "claim_window": {
                        "reference_line_count_positive_count": 0,
                        "reference_line_provider_status_topk": [
                            "trajectory_nonzero_debug_missing"
                        ],
                    },
                    "planning_route_segment_count_positive_count": 171,
                    "routing_segment_count_positive_count": 171,
                    "routing_road_count_positive_count": 171,
                    "trajectory_sample_rows": 170,
                    "control_target_point_rows": 126,
                }
            },
            "planning_info_log_reference_line_evidence": {
                "classification": "planning_info_log_reference_line_text_prints_present",
                "reference_line_text_prints_present": True,
                "text_log_reference_line_claim_grade_allowed": False,
            },
            "reference_line_debug_export_policy": {
                "status": "warn",
                "classification": (
                    "reference_line_debug_export_gap_with_local_planning_and_control_reference_evidence"
                ),
                "reference_line_debug_claim_grade_allowed": False,
                "planning_first_point_local_alignment_available": True,
                "planning_trajectory_sample_surrogate_available": True,
                "control_simple_lat_reference_available": True,
                "route_segment_available": True,
                "reference_line_debug_field_state": (
                    "field_present_but_empty_after_lane_follow_materialization"
                ),
                "planning_info_log_reference_line_available": True,
                "planning_info_log_reference_line_classification": (
                    "planning_info_log_reference_line_text_prints_present"
                ),
                "planning_info_log_reference_line_claim_grade_allowed": False,
                "recommended_evidence_policy": (
                    "local_surrogate_only_until_reference_line_debug_exported"
                ),
                "recommended_next_action": (
                    "Export or decode Planning reference-line debug before changing control mapping."
                ),
                "reason": (
                    "Planning first trajectory points align locally with official HDMap projection "
                    "and Control simple_lat reference evidence is visible, but Planning reference-line "
                    "debug counters are not exported."
                ),
            },
            "control_target_point_vs_planning_path_candidate_sample": {
                "classification": "control_target_between_planning_path_candidate_lateral_bounds",
                "reference_line_claim_grade_allowed": False,
                "target_inside_path_lateral_envelope": True,
                "target_point_to_path_candidate_line_abs_p95_m": 0.82,
                "target_point_lane_l_abs_p95_m": 0.000001,
                "path_candidate_lane_l_min_m": -1.25,
                "path_candidate_lane_l_max_m": 0.82,
            },
            "planning_debug_path_candidate_vs_trajectory_sample": {
                "classification": "planning_debug_path_candidate_offset_from_planning_trajectory_sample_support",
                "reference_line_claim_grade_allowed": False,
                "path_candidate_to_planning_sample_line_abs_p95_m": 3.25,
                "sample_coverage_ratio": 1.0,
            },
            "planning_debug_path_candidate_hdmap_projection_alignment": {
                "classification": "planning_debug_path_candidate_lateral_offset_from_hdmap_lane_center",
                "reference_line_claim_grade_allowed": False,
                "path_candidate_lane_l_abs_p95_m": 1.25,
                "path_candidate_lane_l_min_m": -1.25,
                "path_candidate_lane_l_max_m": 0.82,
                "routing_lane_window_compatible": True,
            },
        }
    }
    link_report_path.write_text(json.dumps(link_report), encoding="utf-8")

    refreshed = classify_phase1_run(run)
    refreshed_evidence = refreshed["behavior_blocker_evidence"]

    assert "Projection-route sample evidence already confirms" in refreshed["behavior_next_action"]
    assert "debug.planning_data.reference_line is empty" in refreshed["behavior_next_action"]
    assert "absolute_magnitude_only_until_canonical_sign_declared" in refreshed["behavior_next_action"]
    assert "relabel_or_explicitly_convert_before_sign_sensitive_gate" in refreshed["behavior_next_action"]
    assert "Do not change steer scale" in refreshed["behavior_next_action"]
    assert (
        refreshed_evidence["projection_route_sample_sign_contract_classification"]
        == "timeseries_route_lateral_sign_inverted_vs_projection_route_samples"
    )
    assert refreshed_evidence["projection_route_sample_sign_contract_confirmed"] is True
    assert refreshed_evidence["projection_route_sample_count"] == 61
    assert refreshed_evidence["projection_route_sample_matched_sample_count"] == 335
    assert refreshed_evidence["projection_route_sample_lateral_source_field"] == "cross_track_error"
    assert refreshed_evidence["projection_route_sample_timeseries_opposite_sign_ratio"] == 1.0
    assert refreshed_evidence["projection_route_sample_simple_lat_same_sign_ratio"] == 1.0
    assert refreshed_evidence["route_lateral_field_semantics_classification"] == (
        "route_lateral_field_opposite_signed_to_apollo_projection"
    )
    assert refreshed_evidence["route_lateral_field_recommended_gate_policy"] == (
        "absolute_magnitude_only_until_canonical_sign_declared"
    )
    assert refreshed_evidence["route_lateral_field_recommended_action"] == (
        "relabel_or_explicitly_convert_before_sign_sensitive_gate"
    )
    assert refreshed_evidence["route_lateral_field_sign_sensitive_gate_allowed"] is False
    assert refreshed_evidence["route_lateral_field_absolute_magnitude_gate_allowed"] is True
    assert refreshed_evidence["phase1_relevant_apollo_link_blocker"] == (
        "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
    )
    policy = refreshed["route_lateral_field_policy"]
    assert policy["status"] == "available"
    assert policy["policy"] == "exclude_from_sign_sensitive_behavior_gates"
    assert policy["source_field"] == "cross_track_error"
    assert policy["classification"] == "route_lateral_field_opposite_signed_to_apollo_projection"
    assert policy["sign_sensitive_gate_allowed"] is False
    assert policy["absolute_magnitude_gate_allowed"] is True
    assert policy["recommended_gate_policy"] == "absolute_magnitude_only_until_canonical_sign_declared"
    assert policy["recommended_action"] == "relabel_or_explicitly_convert_before_sign_sensitive_gate"
    lane_context = refreshed["phase1_metrics"]["lane_invasion_context"]
    assert lane_context["route_lateral_source_field"] == "cross_track_error"
    assert lane_context["route_lateral_sign_sensitive_gate_allowed"] is False
    assert lane_context["route_lateral_absolute_magnitude_gate_allowed"] is True
    assert lane_context["route_lateral_recommended_gate_policy"] == (
        "absolute_magnitude_only_until_canonical_sign_declared"
    )
    assert lane_context["route_lateral_recommended_action"] == (
        "relabel_or_explicitly_convert_before_sign_sensitive_gate"
    )
    assert lane_context["route_lateral_field_policy"]["policy"] == (
        "exclude_from_sign_sensitive_behavior_gates"
    )
    reference_policy = refreshed["reference_line_debug_export_policy"]
    assert reference_policy["status"] == "warn"
    assert reference_policy["policy"] == "local_surrogate_only_until_reference_line_debug_exported"
    assert reference_policy["classification"] == (
        "reference_line_debug_export_gap_with_local_planning_and_control_reference_evidence"
    )
    assert reference_policy["reference_line_debug_claim_grade_allowed"] is False
    assert reference_policy["local_surrogate_available"] is True
    assert reference_policy["field_gap_classification"] == (
        "reference_line_counter_missing_but_planning_control_surrogates_present"
    )
    assert reference_policy["reference_line_count_positive_count"] == 0
    assert reference_policy["claim_window_source"] == "after_routing_segment_available"
    assert reference_policy["claim_window_reference_line_count_positive_count"] == 0
    assert reference_policy["claim_window_reference_line_provider_status_topk"] == [
        "trajectory_nonzero_debug_missing"
    ]
    assert reference_policy["planning_route_segment_count_positive_count"] == 171
    assert reference_policy["routing_segment_count_positive_count"] == 171
    assert reference_policy["routing_road_count_positive_count"] == 171
    assert reference_policy["trajectory_sample_rows"] == 170
    assert reference_policy["control_target_point_rows"] == 126
    assert reference_policy["planning_debug_presence_classification"] == (
        "routing_present_reference_line_empty"
    )
    assert reference_policy["planning_debug_reference_line_path"] == (
        "debug.planning_data.reference_line"
    )
    assert reference_policy["planning_debug_routing_path"] == "debug.planning_data.routing"
    assert reference_policy["planning_debug_reference_line_nonempty_ratio"] == 0.0
    assert reference_policy["planning_debug_routing_segment_nonempty_ratio"] == 0.52
    assert reference_policy["planning_materialization_classification"] == (
        "route_segments_ready_trajectory_nonzero_reference_line_empty"
    )
    assert reference_policy["planning_materialization_route_segments_ready_ratio"] == 1.0
    assert (
        reference_policy["planning_materialization_reference_line_empty_while_ready_ratio"]
        == 1.0
    )
    assert reference_policy["reference_line_debug_field_state"] == (
        "field_present_but_empty_after_lane_follow_materialization"
    )
    assert reference_policy["planning_info_log_reference_line_available"] is True
    assert reference_policy["planning_info_log_reference_line_claim_grade_allowed"] is False
    assert reference_policy["planning_first_point_local_alignment_available"] is True
    assert reference_policy["planning_trajectory_sample_surrogate_available"] is True
    assert reference_policy["control_simple_lat_reference_available"] is True
    assert "debug.planning_data.reference_line is empty" in reference_policy["recommended_action"]
    path_context = refreshed["path_candidate_control_context"]
    assert path_context["status"] == "diagnostic_only"
    assert path_context["control_target_vs_path_candidate_classification"] == (
        "control_target_between_planning_path_candidate_lateral_bounds"
    )
    assert path_context["target_inside_path_lateral_envelope"] is True
    assert path_context["target_point_to_path_candidate_line_abs_p95_m"] == 0.82
    assert path_context["target_point_lane_l_abs_p95_m"] == 0.000001
    assert path_context["path_candidate_lane_l_abs_p95_m"] == 1.25
    assert path_context["path_candidate_lane_l_min_m"] == -1.25
    assert path_context["path_candidate_lane_l_max_m"] == 0.82
    assert path_context["planning_debug_path_candidate_vs_trajectory_sample_classification"] == (
        "planning_debug_path_candidate_offset_from_planning_trajectory_sample_support"
    )
    assert path_context["path_candidate_to_planning_sample_line_abs_p95_m"] == 3.25
    assert path_context["planning_debug_path_candidate_hdmap_projection_classification"] == (
        "planning_debug_path_candidate_lateral_offset_from_hdmap_lane_center"
    )
    assert path_context["path_candidate_routing_lane_window_compatible"] is True
    assert path_context["reference_line_claim_grade_allowed"] is False
    assert path_context["recommended_evidence_policy"] == (
        "path_candidate_surrogate_only_until_reference_line_debug_exported"
    )

    paths = write_phase1_status(refreshed, run / "analysis" / "phase1_status")
    summary_text = Path(paths["summary"]).read_text(encoding="utf-8")
    assert "## Route Lateral Field Policy" in summary_text
    assert "exclude_from_sign_sensitive_behavior_gates" in summary_text
    assert "sign_sensitive_gate_allowed: `False`" in summary_text
    assert "## Reference-Line Debug Export Policy" in summary_text
    assert "local_surrogate_only_until_reference_line_debug_exported" in summary_text
    assert "reference_line_counter_missing_but_planning_control_surrogates_present" in summary_text
    assert "routing_present_reference_line_empty" in summary_text
    assert "planning_debug_reference_line_nonempty_ratio: `0.0`" in summary_text
    assert "route_segments_ready_trajectory_nonzero_reference_line_empty" in summary_text
    assert "field_present_but_empty_after_lane_follow_materialization" in summary_text
    assert "planning_info_log_reference_line_available: `True`" in summary_text
    assert "planning_info_log_reference_line_claim_grade_allowed: `False`" in summary_text
    assert "reference_line_debug_claim_grade_allowed: `False`" in summary_text
    assert "## Path Candidate / Control Target Context" in summary_text
    assert "control_target_between_planning_path_candidate_lateral_bounds" in summary_text
    assert "planning_debug_path_candidate_lateral_offset_from_hdmap_lane_center" in summary_text
    assert "path_candidate_surrogate_only_until_reference_line_debug_exported" in summary_text


def test_phase1_status_filters_claim_only_apollo_link_primary_for_phase1_behavior(
    tmp_path,
) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "run_id": "apollo_claim_boundary_primary",
            "scenario_case": "baguang_cut_in_35kph_left_to_right_10m",
            "scenario_id": "baguang_cut_in_35kph_left_to_right_10m",
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "LANE_INVASION"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame,sim_time,ego_speed_mps,lane_invasion_count,cross_track_error,applied_steer,control_source",
                "0,10.0,1.0,0,-0.10,-0.01,external_stack",
                "1,10.05,3.5,1,0.85,0.04,external_stack",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "target_actor_contract": {"status": "pass"},
                "rows": [{"gap_m": 10.0, "ego_speed_mps": 3.5, "target_speed_mps": 3.0}],
            }
        ),
        encoding="utf-8",
    )
    link_health = run / "analysis" / "apollo_link_health"
    link_health.mkdir(parents=True)
    (link_health / "apollo_link_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_link_health.v1",
                "primary_blocker": "no_assist_claim_boundary:blocking_assists_active",
                "secondary_blockers": ["natural_driving_outcome:insufficient_data"],
                "can_claim_unassisted_natural_driving": False,
                "layers": {
                    "apollo_lateral_semantics": {
                        "key_metrics": {
                            "route_simple_lat_sign_convention_candidate": True,
                            "route_lateral_field_semantics_status": "available",
                            "route_lateral_field_semantics_classification": (
                                "route_lateral_field_opposite_signed_to_apollo_projection"
                            ),
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    lane_event = run / "analysis" / "baguang_lane_event_contract"
    lane_event.mkdir(parents=True)
    (lane_event / "baguang_lane_event_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "baguang_lane_event_contract.v1",
                "status": "fail",
                "run_reports": [
                    {
                        "reason": "possible_real_lane_departure_or_unclassified_lane_event",
                        "lane_invasion_event_can_be_used_as_hard_gate": True,
                        "departure_diagnostics": {
                            "classification": "downstream_progressive_lane_departure",
                            "interpretation": ["raw_steer_same_sign_as_cross_track_error"],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"
    assert (
        report["primary_behavior_blocker"]
        == "lane_departure_with_route_simple_lat_sign_convention_candidate"
    )
    evidence = report["behavior_blocker_evidence"]
    assert evidence["apollo_link_health_primary_blocker"] == (
        "no_assist_claim_boundary:blocking_assists_active"
    )
    assert evidence["phase1_relevant_apollo_link_blocker"] == (
        "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
    )
    link_summary = report["phase1_metrics"]["derived_blocker_evidence"]["apollo_link_health"]
    assert link_summary["primary_blocker"] == "no_assist_claim_boundary:blocking_assists_active"
    assert link_summary["phase1_relevant_primary_blocker"] == (
        "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
    )

    paths = write_phase1_status(report, run / "analysis" / "phase1_status")
    summary_text = Path(paths["summary"]).read_text(encoding="utf-8")
    assert "apollo_link_primary_blocker: `no_assist_claim_boundary:blocking_assists_active`" in summary_text
    assert (
        "phase1_relevant_apollo_link_blocker: "
        "`apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate`"
    ) in summary_text


def test_legacy_apollo_route_id_normalizes_to_phase1_scenario_case(tmp_path) -> None:
    run = tmp_path / "apollo_lane"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_lane",
                "scenario_id": "carla_town01_route_health",
                "scenario_class": "lane_keep",
                "route_id": "town01_rh_spawn097_goal046",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "LANE_INVASION"}),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "not_applicable",
                "target_actor_contract": {
                    "status": "not_required",
                    "scenario_class": "lane_keep",
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["scenario_case"] == "town01_lane_keep_097"
    assert report["backend_type"] == "apollo_reference_backend"
    assert report["status"] == "failed"
    assert report["failure_reason"] == "lane_invasion"


def test_phase1_status_records_missing_safety_event_evidence_surface(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 12.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    safety = report["phase1_metrics"]["safety_event_evidence"]
    assert safety["lane_invasion"]["available"] is False
    assert safety["lane_invasion"]["missing_reason"] == "lane_invasion_event_counter_missing"
    assert safety["collision"]["available"] is False
    assert safety["collision"]["missing_reason"] == "collision_event_counter_missing"


def test_phase1_status_marks_explicitly_unavailable_safety_sensor_as_not_comparable(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "success": True,
                "status": "pass",
                "lane_invasion_count": 0,
                "collision_count": 0,
                "lane_invasion_sensor_available": False,
                "collision_sensor_available": True,
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps,lane_invasion_count,collision_count,lane_invasion_sensor_available,collision_sensor_available\n"
        "0.0,1.0,0,0,False,True\n",
        encoding="utf-8",
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 12.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    safety = report["phase1_metrics"]["safety_event_evidence"]
    assert safety["lane_invasion"]["available"] is False
    assert safety["lane_invasion"]["sensor_available"] is False
    assert safety["lane_invasion"]["missing_reason"] == "lane_invasion_sensor_unavailable"
    assert safety["collision"]["available"] is True
    assert safety["collision"]["sensor_available"] is True


def test_apollo_fixed_scene_target_requires_obstacle_gt_contract(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 12.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "missing_apollo_obstacle_gt_contract"
    assert report["invalid_reasons"] == ["missing_apollo_obstacle_gt_contract"]
    assert report["failed_reasons"] == []
    assert report["evaluable"] is False


def test_apollo_fixed_scene_target_with_obstacle_gt_contract_is_evaluable(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 12.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "pass"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "success"
    assert report["evaluable"] is True


def test_apollo_fixed_scene_target_with_failed_obstacle_gt_contract_is_invalid(tmp_path) -> None:
    run = _base_run(tmp_path)
    _write_manifest(
        run,
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
        },
    )
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 12.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "fail"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "apollo_obstacle_gt_contract_failed"
    assert report["evaluable"] is False


def test_carla_builtin_ignores_apollo_obstacle_contract_failure_gate(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 12.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    obstacle_out = run / "analysis" / "obstacle_gt_contract"
    obstacle_out.mkdir(parents=True)
    (obstacle_out / "obstacle_gt_contract_report.json").write_text(
        json.dumps({"schema_version": "obstacle_gt_contract.v1", "status": "fail"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "success"
    assert report["evaluable"] is True


def test_route_only_apollo_not_required_skips_obstacle_gt_contract(tmp_path) -> None:
    run = tmp_path / "apollo_lane"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_lane",
                "scenario_case": "town01_lane_keep_097",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(json.dumps({"success": True, "status": "pass"}), encoding="utf-8")
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "not_applicable",
                "target_actor_contract": {
                    "status": "not_required",
                    "scenario_class": "lane_keep",
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "success"
    assert report["target_actor_contract"]["status"] == "not_required"


def test_phase1_status_writer(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 10.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    report = classify_phase1_run(run)

    outputs = write_phase1_status(report, tmp_path / "out")

    assert outputs["report"].endswith("phase1_status.json")
    written = json.loads((tmp_path / "out" / "phase1_status.json").read_text(encoding="utf-8"))
    assert written["scenario_case"] == "baguang_follow_stop_static_300m"
    assert written["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert written["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert any(path.endswith("analysis/v_t_gap/v_t_gap_report.json") for path in written["evidence_files"])


def _write_manifest(run, overrides):
    path = run / "manifest.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data.update(overrides)
    path.write_text(json.dumps(data), encoding="utf-8")


def _base_run(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "scenario_case": "baguang_follow_stop_static_300m",
                "scenario_id": "baguang_follow_stop_static_300m",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
                "artifact_contract_version": "phase1_scenario_run_artifacts.v1",
                "target_actor_contract": {
                    "status": "resolved",
                    "target_actor_role": "lead_vehicle",
                    "source": "scenario_case_explicit",
                },
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(json.dumps({"success": True, "status": "pass"}), encoding="utf-8")
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    return run
