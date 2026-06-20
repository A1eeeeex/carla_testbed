from __future__ import annotations

import json

from carla_testbed.analysis.phase1_scenario_catalog import analyze_phase1_scenario_catalog


def test_phase1_catalog_lists_p0_p1_readiness_without_claiming_done(tmp_path) -> None:
    report = analyze_phase1_scenario_catalog(".", evidence_root=tmp_path / "empty_evidence")

    scenarios = {item["scenario"]: item for item in report["scenarios"]}

    assert scenarios["follow_stop_static"]["case_yaml_status"] == "DONE"
    assert scenarios["follow_stop_static"]["target_actor_status"] == "resolved"
    assert "baguang_follow_stop_static_300m" in scenarios["follow_stop_static"]["scenario_case_ids"]
    assert scenarios["follow_stop_static"]["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert scenarios["follow_stop_static"]["target_actor_contract"]["source"] == "scenario_case_explicit"
    assert scenarios["follow_stop_static"]["overall_status"] == "PARTIAL"
    assert scenarios["follow_stop_static"]["template_status"] == "DONE"
    assert scenarios["follow_stop_static"]["status"] == scenarios["follow_stop_static"]["overall_status"]
    assert "missing" in scenarios["follow_stop_static"]
    assert "carla_online_evidence" in scenarios["follow_stop_static"]["missing"]
    assert "apollo_online_evidence" in scenarios["follow_stop_static"]["missing"]
    assert "scenario_comparison_report" in scenarios["follow_stop_static"]["missing"]
    assert scenarios["follow_stop_static"]["comparison_status"] == "NOT_YET"
    assert scenarios["follow_stop_static"]["comparison_target_status"] == "NOT_YET"
    assert scenarios["follow_stop_static"]["accepted_bundle_status"] == "NOT_YET"
    assert any("CARLA builtin" in action for action in scenarios["follow_stop_static"]["next_action"])
    assert any(
        "Apollo fixed-scene runtime dispatch" in action
        for action in scenarios["follow_stop_static"]["next_action"]
    )
    assert all("path" in item and "evidence_type" in item for item in scenarios["follow_stop_static"]["evidence"])
    assert scenarios["lead_hard_brake"]["case_yaml_status"] == "DONE"
    assert scenarios["lead_hard_brake"]["template_status"] == "DONE"
    assert scenarios["lead_hard_brake"]["target_actor_status"] == "resolved"
    assert scenarios["lead_hard_brake"]["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert scenarios["lead_hard_brake"]["overall_status"] == "PARTIAL"
    assert "apollo_online_evidence" in scenarios["lead_hard_brake"]["missing_items"]
    assert scenarios["cut_in_simple"]["target_actor_status"] == "resolved"
    assert scenarios["cut_in_simple"]["target_actor_contract"]["role_aliases"]["cutin_vehicle"] == "lead_vehicle"
    assert scenarios["cut_in_simple"]["target_actor_contract"]["activation"]["active_after_phase"] == "cut_in_lane_change"
    assert scenarios["cut_in_simple"]["overall_status"] != "DONE"


def test_phase1_catalog_ingests_explicit_evidence_root_without_overclaiming(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_builtin_follow_stop"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "analysis" / "v_t_gap").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "phase1_builtin_follow_stop",
                "scenario_id": "baguang_follow_stop_static_300m",
                "scenario_class": "follow_stop_static",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps({"status": "success", "scenario_id": "baguang_follow_stop_static_300m"}),
        encoding="utf-8",
    )
    (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass"}),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["carla_online_status"] == "DONE"
    assert follow_stop["v_t_gap_readiness"] == "DONE"
    assert follow_stop["comparison_readiness"] == "NOT_YET"
    assert follow_stop["overall_status"] == "PARTIAL"
    assert any(item["evidence_type"] == "CARLA_online" for item in follow_stop["evidence"])


def test_phase1_catalog_prioritizes_best_representative_online_evidence(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    stale = evidence_root / "phase1_builtin_lead_accel_20260617_010000"
    stale.mkdir(parents=True)
    (stale / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": stale.name,
                "scenario_id": "baguang_lead_accel_40_to_70_20m",
                "scenario_class": "lead_accel",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    success = evidence_root / "phase1_builtin_lead_accel_decel_20260618_010000"
    _write_catalog_run(
        success,
        backend="carla_builtin",
        backend_type="planning_control_backend",
        scenario_case="baguang_lead_accel_40_to_70_20m",
        status="success",
    )
    failed = evidence_root / "phase1_builtin_lead_accel_decel_20260618_020000"
    _write_catalog_run(
        failed,
        backend="carla_builtin",
        backend_type="planning_control_backend",
        scenario_case="baguang_lead_accel_40_to_70_20m",
        status="failed",
    )
    (failed / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "fail"}),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    lead = {item["scenario"]: item for item in report["scenarios"]}["lead_decel_accel"]

    assert lead["carla_online_status"] == "DONE"
    assert lead["v_t_gap_status"] == "not_applicable"
    assert lead["overall_status"] == "PARTIAL"
    representative = lead["representative_evidence"]["CARLA_online"]
    assert representative["path"].endswith(success.name)
    assert representative["phase1_run_status"] == "success"
    carla_evidence = [item for item in lead["evidence"] if item["evidence_type"] == "CARLA_online"]
    assert carla_evidence[0]["path"].endswith(success.name)
    assert "phase1_status=missing" in carla_evidence[-1]["note"]


def test_phase1_catalog_maps_legacy_follow_stop_097_to_static_follow_stop_without_claiming_pass(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_apollo_followstop_20260618_010000"
    _write_catalog_run(
        run,
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_case="follow_stop_097",
        status="failed",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "status": "failed",
                "scenario_id": "follow_stop_097",
                "scenario_case": "follow_stop_097",
                "scenario_class": "follow_stop",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
                "failure_reason": "unsafe_gap",
                "failed_reasons": ["unsafe_gap"],
                "evaluable": True,
            }
        ),
        encoding="utf-8",
    )
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run.name,
                "scenario_id": "follow_stop_097",
                "scenario_case": "follow_stop_097",
                "scenario_class": "follow_stop",
                "backend": "apollo_cyberrt",
                "backend_name": "harness",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert "follow_stop_097" in follow_stop["scenario_case_ids"]
    assert follow_stop["apollo_online_status"] == "DONE"
    assert follow_stop["overall_status"] == "PARTIAL"
    assert "scenario_comparison_report" in follow_stop["missing_items"]
    representative = follow_stop["representative_evidence"]["Apollo_online"]
    assert representative["path"].endswith(run.name)
    assert representative["phase1_run_status"] == "failed"
    assert representative["failure_reason"] == "unsafe_gap"


def test_phase1_catalog_does_not_count_invalid_apollo_scaffold_as_online_done(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_apollo_follow_stop"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "phase1_apollo_follow_stop",
                "scenario_id": "baguang_follow_stop_static_300m",
                "scenario_class": "follow_stop_static",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "status": "invalid",
                "failure_reason": "backend_not_ready",
                "invalid_reasons": ["backend_not_ready"],
                "evaluable": False,
                "scenario_id": "baguang_follow_stop_static_300m",
                "missing_expected_artifacts": [
                    "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"
                ],
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_apollo_fixed_scene_readiness").mkdir(parents=True)
    (run / "analysis" / "phase1_apollo_fixed_scene_readiness" / "phase1_apollo_fixed_scene_readiness_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_apollo_fixed_scene_readiness.v1",
                "status": "fail",
                "target_actor_role": "lead_vehicle",
                "actor_probe_enabled_effective": False,
                "target_role_covered_by_bridge_roles": False,
                "front_obstacle_behavior": {"role_names": ["front"]},
                "blocking_reasons": [
                    "front_obstacle_actor_probe_disabled",
                    "target_role_not_in_front_obstacle_role_names",
                ],
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_apollo_fixed_scene_dispatch").mkdir(parents=True)
    (run / "analysis" / "phase1_apollo_fixed_scene_dispatch" / "phase1_apollo_fixed_scene_dispatch_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_apollo_fixed_scene_dispatch.v1",
                "status": "pass",
                "dispatch_mode": "guarded_legacy_transition_available",
                "starts_runtime": True,
                "commands_present": True,
                "blocking_reasons": [],
                "warnings": ["guarded_static_follow_stop_transition_not_generic_fixed_scene_runtime"],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["apollo_online_status"] == "PARTIAL"
    assert follow_stop["apollo_fixed_scene_readiness_status"] == "fail"
    assert follow_stop["apollo_fixed_scene_readiness"] == "PARTIAL"
    assert follow_stop["apollo_fixed_scene_dispatch_contract_status"] == "pass"
    assert follow_stop["apollo_fixed_scene_dispatch_contract"] == "DONE"
    assert follow_stop["apollo_fixed_scene_dispatch_mode"] == "guarded_legacy_transition_available"
    assert follow_stop["apollo_fixed_scene_runtime_dispatch_status"] == "PARTIAL"
    assert follow_stop["apollo_fixed_scene_runtime_dispatch_reason"] == "backend_not_ready"
    assert follow_stop["overall_status"] == "PARTIAL"
    assert "apollo_fixed_scene_not_ready" in follow_stop["missing_items"]
    assert "apollo_fixed_scene_runtime_dispatch" in follow_stop["missing_items"]
    assert any(
        item["evidence_type"] == "Apollo_online"
        and item["status"] == "PARTIAL"
        and "phase1_status=invalid" in item.get("note", "")
        and "failure_reason=backend_not_ready" in item.get("note", "")
        and "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in item.get("note", "")
        and item.get("failure_reason") == "backend_not_ready"
        and item.get("evaluable") is False
        and "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"
        in item.get("missing_expected_artifacts", [])
        for item in follow_stop["evidence"]
    )
    assert any(
        item["evidence_type"] == "apollo_fixed_scene_readiness"
        and item["status"] == "PARTIAL"
        and "front_obstacle_actor_probe_disabled" in item.get("blocking_reasons", [])
        and item.get("actor_probe_enabled_effective") is False
        for item in follow_stop["evidence"]
    )
    assert any(
        item["evidence_type"] == "apollo_fixed_scene_dispatch_contract"
        and item["status"] == "DONE"
        and item.get("dispatch_mode") == "guarded_legacy_transition_available"
        for item in follow_stop["evidence"]
    )


def test_phase1_catalog_surfaces_dynamic_dispatch_contract_without_online_done(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_apollo_lead_decel_scaffold"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "analysis" / "phase1_apollo_fixed_scene_dispatch").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run.name,
                "scenario_id": "baguang_lead_decel_70_to_40_20m",
                "scenario_class": "lead_decel",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "status": "invalid",
                "failure_reason": "backend_not_ready",
                "preflight_reasons": ["apollo_fixed_scene_runtime_not_migrated"],
                "evaluable": False,
                "scenario_id": "baguang_lead_decel_70_to_40_20m",
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_apollo_fixed_scene_dispatch" / "phase1_apollo_fixed_scene_dispatch_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_apollo_fixed_scene_dispatch.v1",
                "status": "partial",
                "dispatch_mode": "runtime_migration_required",
                "starts_runtime": False,
                "commands_present": False,
                "blocking_reasons": ["apollo_fixed_scene_runtime_migration_required"],
                "warnings": ["dynamic_fixed_scene_runtime_not_migrated"],
                "runtime_migration_requirements": [
                    "apollo_online_runner_starts_carla_fixed_scene_runtime",
                    "speed_profile_non_ego_actor_control",
                    "v_t_gap_from_target_actor_trace_and_timeseries",
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    lead = {item["scenario"]: item for item in report["scenarios"]}["lead_decel_accel"]

    assert lead["apollo_online_status"] == "PARTIAL"
    assert lead["apollo_fixed_scene_dispatch_contract_status"] == "partial"
    assert lead["apollo_fixed_scene_dispatch_contract"] == "PARTIAL"
    assert lead["apollo_fixed_scene_dispatch_mode"] == "runtime_migration_required"
    assert lead["apollo_fixed_scene_runtime_dispatch_status"] == "PARTIAL"
    assert lead["apollo_fixed_scene_runtime_dispatch_reason"] == "apollo_fixed_scene_runtime_not_migrated"
    assert lead["overall_status"] == "PARTIAL"
    assert "apollo_fixed_scene_runtime_dispatch" in lead["missing_items"]
    assert any(
        "Apollo runtime migration requirements: apollo_online_runner_starts_carla_fixed_scene_runtime"
        in action
        for action in lead["next_action"]
    )
    dispatch_evidence = [
        item for item in lead["evidence"] if item["evidence_type"] == "apollo_fixed_scene_dispatch_contract"
    ][0]
    assert "speed_profile_non_ego_actor_control" in dispatch_evidence["runtime_migration_requirements"]
    assert "runtime_requirements=apollo_online_runner_starts_carla_fixed_scene_runtime" in dispatch_evidence["note"]


def test_phase1_catalog_requires_dispatch_contract_for_fixed_scene_done(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    _write_catalog_run(
        evidence_root / "apollo_follow_stop",
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_case="baguang_follow_stop_static_300m",
        status="success",
    )
    readiness_dir = (
        evidence_root
        / "apollo_follow_stop"
        / "analysis"
        / "phase1_apollo_fixed_scene_readiness"
    )
    readiness_dir.mkdir(parents=True)
    (readiness_dir / "phase1_apollo_fixed_scene_readiness_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_apollo_fixed_scene_readiness.v1",
                "status": "pass",
                "blocking_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    _write_catalog_run(
        evidence_root / "builtin_follow_stop",
        backend="carla_builtin",
        backend_type="planning_control_backend",
        scenario_case="baguang_follow_stop_static_300m",
        status="success",
    )
    comparison = evidence_root / "comparisons" / "follow_stop_missing_dispatch_contract"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "baguang_follow_stop_static_300m",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["carla_online_status"] == "DONE"
    assert follow_stop["apollo_online_status"] == "DONE"
    assert follow_stop["apollo_fixed_scene_readiness"] == "DONE"
    assert follow_stop["apollo_fixed_scene_dispatch_contract"] == "NOT_YET"
    assert follow_stop["apollo_fixed_scene_runtime_dispatch_status"] == "DONE"
    assert follow_stop["comparison_readiness"] == "DONE"
    assert follow_stop["overall_status"] == "PARTIAL"
    assert "apollo_fixed_scene_dispatch_contract_report" in follow_stop["missing_items"]
    assert any(
        "generate Apollo fixed-scene dispatch contract evidence" in action
        for action in follow_stop["next_action"]
    )


def test_phase1_catalog_uses_preflight_missing_expected_artifacts_when_status_is_legacy(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_apollo_follow_stop"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "phase1_apollo_follow_stop",
                "scenario_id": "baguang_follow_stop_static_300m",
                "scenario_class": "follow_stop_static",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "preflight.json").write_text(
        json.dumps(
            {
                "status": "backend_not_ready",
                "missing_expected_artifacts": [
                    "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"
                ],
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "status": "invalid",
                "failure_reason": "backend_not_ready",
                "evaluable": False,
                "scenario_id": "baguang_follow_stop_static_300m",
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert any(
        item["evidence_type"] == "Apollo_online"
        and "preflight_missing_expected_artifacts=analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"
        in item.get("note", "")
        for item in follow_stop["evidence"]
    )


def test_phase1_catalog_readiness_pass_does_not_count_as_apollo_online_done(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    run = evidence_root / "phase1_apollo_follow_stop"
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "analysis" / "phase1_apollo_fixed_scene_readiness").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "phase1_apollo_follow_stop",
                "scenario_id": "baguang_follow_stop_static_300m",
                "scenario_class": "follow_stop_static",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "status": "invalid",
                "failure_reason": "backend_not_ready",
                "preflight_reasons": ["apollo_fixed_scene_runtime_not_migrated"],
                "evaluable": False,
                "scenario_id": "baguang_follow_stop_static_300m",
            }
        ),
        encoding="utf-8",
    )
    (run / "preflight.json").write_text(
        json.dumps(
            {
                "status": "backend_not_ready",
                "reasons": ["apollo_fixed_scene_runtime_not_migrated"],
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_apollo_fixed_scene_readiness" / "phase1_apollo_fixed_scene_readiness_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_apollo_fixed_scene_readiness.v1",
                "status": "pass",
                "target_actor_role": "lead_vehicle",
                "actor_probe_enabled_effective": True,
                "target_role_covered_by_bridge_roles": True,
                "blocking_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["apollo_fixed_scene_readiness_status"] == "pass"
    assert follow_stop["apollo_fixed_scene_readiness"] == "DONE"
    assert follow_stop["apollo_fixed_scene_runtime_dispatch_status"] == "PARTIAL"
    assert (
        follow_stop["apollo_fixed_scene_runtime_dispatch_reason"]
        == "apollo_fixed_scene_runtime_not_migrated"
    )
    assert follow_stop["apollo_online_status"] == "PARTIAL"
    assert "apollo_online_evidence" in follow_stop["missing_items"]
    assert "apollo_fixed_scene_runtime_dispatch" in follow_stop["missing_items"]
    assert "apollo_fixed_scene_not_ready" not in follow_stop["missing_items"]
    assert follow_stop["overall_status"] == "PARTIAL"


def test_phase1_catalog_matches_comparison_by_scenario_case(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    comparison = evidence_root / "comparisons" / "lead_accel"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "baguang_lead_accel_40_to_70_20m",
                "comparison_status": "partially_evaluable",
                "comparison_target_status": "missing_evaluable_apollo_reference_backend",
                "participating_runs": [
                    {
                        "backend": "apollo_cyberrt",
                        "missing_expected_artifacts": [
                            "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json"
                        ],
                    },
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    lead = {item["scenario"]: item for item in report["scenarios"]}["lead_decel_accel"]

    assert lead["comparison_status"] == "partially_evaluable"
    assert lead["comparison_target_status"] == "missing_evaluable_apollo_reference_backend"
    assert any(item["evidence_type"] == "comparison_online" for item in lead["evidence"])
    assert any(
        "apollo_cyberrt:analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in item.get("note", "")
        for item in lead["evidence"]
    )


def test_phase1_catalog_uses_best_comparison_summary_when_multiple_exist(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    old_comparison = evidence_root / "comparisons" / "follow_stop_old"
    old_comparison.mkdir(parents=True)
    (old_comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "follow_stop_097",
                "comparison_status": "partially_evaluable",
                "comparison_target_status": "missing_evaluable_apollo_reference_backend",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )
    better_comparison = evidence_root / "comparisons" / "follow_stop_20260618_010000"
    better_comparison.mkdir(parents=True)
    (better_comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "follow_stop_097",
                "comparison_status": "partially_evaluable",
                "comparison_target_status": "missing_evaluable_planning_control_backend",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["comparison_status"] == "partially_evaluable"
    assert follow_stop["comparison_target_status"] == "missing_evaluable_planning_control_backend"
    representative = follow_stop["representative_evidence"]["comparison_online"]
    assert representative["path"].endswith("follow_stop_20260618_010000/comparison_summary.json")


def test_phase1_catalog_downgrades_stale_safety_failure_comparison_without_event_surface(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    comparison = evidence_root / "comparisons" / "follow_stop_20260618_030000"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "follow_stop_097",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "carla_builtin"},
                    {"backend": "apollo_cyberrt"},
                ],
                "backend_results": [
                    {
                        "backend": "carla_builtin",
                        "backend_type": "planning_control_backend",
                        "phase1_status": "success",
                        "counts_as_backend_loss": False,
                    },
                    {
                        "backend": "apollo_cyberrt",
                        "backend_type": "apollo_reference_backend",
                        "phase1_status": "failed",
                        "failure_reason": "lane_invasion",
                        "counts_as_backend_loss": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    assert follow_stop["comparison_status"] == "partially_evaluable"
    assert follow_stop["comparison_target_status"] == "safety_event_evidence_mismatch"
    assert follow_stop["comparison_readiness"] == "PARTIAL"
    assert "cross_backend_scenario_comparison" in follow_stop["missing_items"]
    representative = follow_stop["representative_evidence"]["comparison_online"]
    assert representative["status"] == "PARTIAL"
    assert representative["safety_event_evidence_issue"] == "safety_event_evidence_missing_for_safety_failure"
    assert "safety_event_evidence_missing_for_safety_failure" in representative["note"]


def test_phase1_catalog_surfaces_comparison_backend_failure_context(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    comparison = evidence_root / "comparisons" / "follow_stop_20260618_020000"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "follow_stop_097",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "carla_builtin"},
                    {"backend": "apollo_cyberrt"},
                ],
                "backend_results": [
                    {
                        "backend": "carla_builtin",
                        "backend_type": "planning_control_backend",
                        "phase1_status": "success",
                        "counts_as_backend_loss": False,
                    },
                    {
                        "backend": "apollo_cyberrt",
                        "backend_type": "apollo_reference_backend",
                        "phase1_status": "failed",
                        "failure_reason": "planning_control_handoff_missing",
                        "apollo_control_handoff_status": "fail",
                        "apollo_control_handoff_failure_stage": "planning_control_handoff",
                        "apollo_control_handoff_failure_reason": "control_stream_ended_before_first_nonzero_planning",
                        "apollo_control_handoff_failure_reasons": [
                            "control_stream_ended_before_first_nonzero_planning"
                        ],
                        "counts_as_backend_loss": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    representative = follow_stop["representative_evidence"]["comparison_online"]
    assert (
        "apollo_cyberrt:failed/planning_control_handoff_missing@planning_control_handoff"
        "[control_stream_ended_before_first_nonzero_planning]"
        in representative["note"]
    )
    assert representative["backend_results"] == [
        {
            "backend": "carla_builtin",
            "backend_type": "planning_control_backend",
            "phase1_status": "success",
            "counts_as_backend_loss": False,
        },
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
            "phase1_status": "failed",
            "failure_reason": "planning_control_handoff_missing",
            "apollo_control_handoff_status": "fail",
            "apollo_control_handoff_failure_stage": "planning_control_handoff",
            "apollo_control_handoff_failure_reason": "control_stream_ended_before_first_nonzero_planning",
            "apollo_control_handoff_failure_reasons": [
                "control_stream_ended_before_first_nonzero_planning"
            ],
            "counts_as_backend_loss": True,
        },
    ]


def test_phase1_catalog_surfaces_comparison_derived_blocker_context(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    comparison = evidence_root / "comparisons" / "follow_stop_20260618_030000"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "follow_stop_097",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "carla_builtin"},
                    {"backend": "apollo_cyberrt"},
                ],
                "backend_results": [
                    {
                        "backend": "carla_builtin",
                        "backend_type": "planning_control_backend",
                        "phase1_status": "success",
                        "counts_as_backend_loss": False,
                    },
                    {
                        "backend": "apollo_cyberrt",
                        "backend_type": "apollo_reference_backend",
                        "phase1_status": "failed",
                        "failure_reason": "lane_invasion",
                        "counts_as_backend_loss": True,
                        "phase1_metrics": {
                            "derived_blocker_evidence": {
                                "available": True,
                                "control_health": {
                                    "failure_reason": "apollo_raw_command_oscillation"
                                },
                                "apollo_link_health": {
                                    "primary_blocker": "route_establishment:long_goal_not_compatible_with_scenario_route"
                                },
                                "baguang_lane_event_contract": {
                                    "departure_classification": "downstream_progressive_lane_departure"
                                },
                            }
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    follow_stop = {item["scenario"]: item for item in report["scenarios"]}["follow_stop_static"]

    representative = follow_stop["representative_evidence"]["comparison_online"]
    assert "link=route_establishment:long_goal_not_compatible_with_scenario_route" in representative["note"]
    assert "control=apollo_raw_command_oscillation" in representative["note"]
    assert "lane_event=downstream_progressive_lane_departure" in representative["note"]
    assert any(
        "current representative Apollo blocker: route_establishment:long_goal_not_compatible_with_scenario_route"
        == action
        for action in follow_stop["next_action"]
    )
    apollo_row = [
        row for row in representative["backend_results"] if row["backend"] == "apollo_cyberrt"
    ][0]
    assert (
        apollo_row["apollo_link_primary_blocker"]
        == "route_establishment:long_goal_not_compatible_with_scenario_route"
    )
    assert apollo_row["control_health_failure_reason"] == "apollo_raw_command_oscillation"
    assert apollo_row["lane_event_departure_classification"] == "downstream_progressive_lane_departure"


def test_phase1_catalog_matches_numbered_curve_diagnostic_comparison(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    comparison = evidence_root / "comparisons" / "curve217"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "town01_curve217_diagnostic",
                "comparison_status": "partially_evaluable",
                "comparison_target_status": "missing_evaluable_apollo_reference_backend",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    curve = {item["scenario"]: item for item in report["scenarios"]}["lane_keep_curve"]

    assert curve["comparison_status"] == "partially_evaluable"
    assert curve["comparison_target_status"] == "missing_evaluable_apollo_reference_backend"
    assert any(item["evidence_type"] == "comparison_online" for item in curve["evidence"])


def test_route_only_lane_keep_needs_phase1_acceptance_bundle_for_done(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    _write_catalog_run(
        evidence_root / "apollo_lane_keep",
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_case="town01_lane_keep_097",
        status="failed",
    )
    _write_catalog_run(
        evidence_root / "builtin_lane_keep",
        backend="carla_builtin",
        backend_type="planning_control_backend",
        scenario_case="town01_lane_keep_097",
        status="success",
    )
    comparison = evidence_root / "comparisons" / "lane_keep"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "town01_lane_keep_097",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    lane = {item["scenario"]: item for item in report["scenarios"]}["lane_keep_straight"]

    assert lane["template_status"] == "DONE"
    assert lane["target_actor_status"] == "not_required"
    assert lane["carla_online_status"] == "DONE"
    assert lane["apollo_online_status"] == "DONE"
    assert lane["v_t_gap_readiness"] == "DONE"
    assert lane["comparison_readiness"] == "DONE"
    assert lane["accepted_bundle_status"] == "NOT_YET"
    assert lane["overall_status"] == "PARTIAL"
    assert "accepted_phase1_comparison_bundle" in lane["missing_items"]
    assert "route_only_scenario_has_no_fixed_scene_compile_requirement" in lane["notes"]


def test_route_only_lane_keep_done_requires_self_contained_accepted_bundle(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    _write_catalog_run(
        evidence_root / "apollo_lane_keep",
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_case="town01_lane_keep_097",
        status="failed",
    )
    _write_catalog_run(
        evidence_root / "builtin_lane_keep",
        backend="carla_builtin",
        backend_type="planning_control_backend",
        scenario_case="town01_lane_keep_097",
        status="success",
    )
    comparison = evidence_root / "comparisons" / "lane_keep"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "town01_lane_keep_097",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )
    acceptance = evidence_root / "comparisons" / "lane_keep" / "acceptance"
    acceptance.mkdir(parents=True)
    (acceptance / "phase1_acceptance_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_acceptance.v1",
                "status": "DONE",
                "scenario_case": "town01_lane_keep_097",
                "comparison_id": "lane_keep",
                "apollo_run_id": "apollo_lane_keep",
                "planning_control_run_id": "builtin_lane_keep",
                "blocking_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    lane = {item["scenario"]: item for item in report["scenarios"]}["lane_keep_straight"]

    assert lane["accepted_bundle_status"] == "PARTIAL"
    assert lane["overall_status"] == "PARTIAL"
    assert "accepted_phase1_comparison_bundle" in lane["missing_items"]
    assert lane["representative_evidence"]["phase1_acceptance"]["status"] == "PARTIAL"
    assert lane["representative_evidence"]["phase1_acceptance"]["bundle_self_contained"] is False
    assert "bundle_self_contained=False" in lane["representative_evidence"]["phase1_acceptance"]["note"]


def test_route_only_lane_keep_done_with_self_contained_accepted_bundle(tmp_path) -> None:
    evidence_root = tmp_path / "evidence"
    _write_catalog_run(
        evidence_root / "apollo_lane_keep",
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_case="town01_lane_keep_097",
        status="failed",
    )
    _write_catalog_run(
        evidence_root / "builtin_lane_keep",
        backend="carla_builtin",
        backend_type="planning_control_backend",
        scenario_case="town01_lane_keep_097",
        status="success",
    )
    comparison = evidence_root / "comparisons" / "lane_keep"
    comparison.mkdir(parents=True)
    (comparison / "comparison_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_comparison.v1",
                "scenario_case": "town01_lane_keep_097",
                "comparison_status": "comparable",
                "comparison_target_status": "apollo_vs_planning_control_evaluable",
                "participating_runs": [
                    {"backend": "apollo_cyberrt"},
                    {"backend": "carla_builtin"},
                ],
            }
        ),
        encoding="utf-8",
    )
    acceptance = evidence_root / "comparisons" / "lane_keep" / "acceptance"
    acceptance.mkdir(parents=True)
    (acceptance / "phase1_acceptance_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_acceptance.v1",
                "status": "DONE",
                "scenario_case": "town01_lane_keep_097",
                "comparison_id": "lane_keep",
                "apollo_run_id": "apollo_lane_keep",
                "planning_control_run_id": "builtin_lane_keep",
                "blocking_reasons": [],
                "gates": {"bundle_self_contained": True},
                "bundle_materialization": {
                    "self_contained": True,
                    "missing_required_files": [],
                },
            }
        ),
        encoding="utf-8",
    )

    report = analyze_phase1_scenario_catalog(".", evidence_root=evidence_root)
    lane = {item["scenario"]: item for item in report["scenarios"]}["lane_keep_straight"]

    assert lane["accepted_bundle_status"] == "DONE"
    assert lane["overall_status"] == "DONE"
    assert "accepted_phase1_comparison_bundle" not in lane["missing_items"]
    assert lane["representative_evidence"]["phase1_acceptance"]["status"] == "DONE"
    assert lane["representative_evidence"]["phase1_acceptance"]["bundle_self_contained"] is True


def _write_catalog_run(run, *, backend, backend_type, scenario_case, status):
    (run / "analysis" / "phase1_status").mkdir(parents=True)
    (run / "analysis" / "v_t_gap").mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run.name,
                "scenario_id": scenario_case,
                "scenario_case": scenario_case,
                "scenario_class": "lane_keep",
                "backend": backend,
                "backend_type": backend_type,
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "status": status,
                "scenario_id": scenario_case,
                "scenario_case": scenario_case,
                "backend": backend,
                "backend_type": backend_type,
                "evaluable": True,
            }
        ),
        encoding="utf-8",
    )
    (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "not_applicable"}),
        encoding="utf-8",
    )
