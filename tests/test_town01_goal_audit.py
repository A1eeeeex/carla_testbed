from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.town01_goal_audit import (
    build_goal_audit,
    find_goal_ab_report_paths,
    merge_ab_reports,
    render_goal_audit_markdown,
)


SCRIPT = Path("tools/audit_town01_goal.py")


def _complete_ab_report() -> dict:
    run_results = []
    comparisons = []
    route_ids = (
        "lane097",
        "lane217",
        "junction031",
        "curve217",
        "curve213",
        "random_lane_183_044",
        "random_lane_213_048",
        "random_junction_176_063",
        "random_junction_071_063",
        "random_curve_219_048",
        "random_curve_177_051",
    )
    for route_id in route_ids:
        baseline_id = f"baseline_{route_id}"
        candidate_id = f"candidate_{route_id}"
        run_results.extend(
            [
                {
                    "run_id": baseline_id,
                    "route_id": route_id,
                    "backend": "ros2_gt",
                    "run_status": "success",
                    "artifact_complete": True,
                    "route_health_source": "route_health_file",
                    "route_completion": 0.9,
                    "lateral_error_p95_m": 0.1,
                    "heading_error_p95_rad": 0.01,
                    "planning_hz": 10.0,
                    "carla_applied_control_hz": 20.0,
                    "localization_hz": 20.0,
                    "failure_reason": "success",
                    "direct_transport_contract_status": "not_applicable",
                },
                {
                    "run_id": candidate_id,
                    "route_id": route_id,
                    "backend": "carla_direct",
                    "run_status": "success",
                    "artifact_complete": True,
                    "route_health_source": "route_health_file",
                    "route_completion": 0.9,
                    "lateral_error_p95_m": 0.1,
                    "heading_error_p95_rad": 0.01,
                    "planning_hz": 10.0,
                    "carla_applied_control_hz": 20.0,
                    "localization_hz": 20.0,
                    "failure_reason": "success",
                    "direct_transport_contract_status": "aligned",
                    "direct_stale_world_frame_policy": "always_republish",
                    "direct_stale_world_frame_policy_source": "direct_bridge_stats",
                },
            ]
        )
        comparisons.append(
            {
                "route_id": route_id,
                "duration_s": 30.0,
                "baseline_run_id": baseline_id,
                "candidate_run_id": candidate_id,
                "status": "candidate_positive",
                "reasons": ["multi_metric_gate_passed"],
                "cadence_comparison": {
                    "bridge_loc_hz_ratio": 0.95,
                    "bridge_chassis_hz_ratio": 0.95,
                },
            }
        )
    return {
        "schema_version": "ab_report.v1",
        "verdict": {
            "status": "candidate_positive",
            "hard_gate_summary": {
                "status": "hard_gate_pass",
                "pass": True,
                "positive_routes": ["lane097", "lane217", "junction031"],
                "degraded_routes": [],
                "insufficient_routes": [],
                "missing_routes": [],
            },
        },
        "run_results": run_results,
        "comparisons": comparisons,
    }


def _complete_natural_driving_report() -> dict:
    traffic_light_behaviors = {
        "traffic_light_red_stop": "red_stop",
        "traffic_light_green_go": "green_go",
        "traffic_light_red_to_green_release": "red_to_green_release",
    }
    classes = (
        "lane_keep",
        "curve_diagnostic",
        "junction_turn",
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    )
    route_ids = {
        "lane_keep": "lane097",
        "curve_diagnostic": "curve217",
        "junction_turn": "junction031",
        "traffic_light_red_stop": "traffic_light_red_stop_tbd",
        "traffic_light_green_go": "traffic_light_green_go_tbd",
        "traffic_light_red_to_green_release": "traffic_light_red_to_green_release_tbd",
    }
    scenario_ids = {
        "lane_keep": "lane_keep_097",
        "curve_diagnostic": "curve_diagnostic_217",
        "junction_turn": "junction_turn_031",
        "traffic_light_red_stop": "traffic_light_red_stop_placeholder",
        "traffic_light_green_go": "traffic_light_green_go_placeholder",
        "traffic_light_red_to_green_release": "traffic_light_red_to_green_release_placeholder",
    }
    return {
        "schema_version": "town01_natural_driving_report.v1",
        "run_count": len(classes),
        "summary": {
            "pass_count": len(classes),
            "warn_count": 0,
            "fail_count": 0,
            "insufficient_data_count": 0,
        },
        "verdict": {
            "status": "pass",
            "can_claim_full_natural_driving": True,
            "missing_required_scenario_classes": [],
            "unproven_required_scenario_classes": [],
            "failed_runs": [],
            "insufficient_data_runs": [],
            "warning_runs": [],
            "filtered_suite_plan": False,
            "scenario_class_filter": [],
            "scenario_id_filter": [],
            "suite_plan_missing": [],
            "missing_required_scenario_ids": [],
            "unproven_required_scenario_ids": [],
            "scenario_identity_mismatches": [],
        },
        "capability_coverage": {
            "required_scenario_classes": list(classes),
            "observed_scenario_classes": list(classes),
            "missing_required_scenario_classes": [],
            "unproven_required_scenario_classes": [],
            "missing_required_scenario_ids": [],
            "unproven_required_scenario_ids": [],
            "scenario_identity_mismatches": [],
            "can_claim_full_natural_driving": True,
        },
        "suite_plan_scope": {
            "has_suite_manifest": True,
            "has_run_matrix": True,
            "filtered": False,
            "scenario_class_filter": [],
            "scenario_id_filter": [],
        },
        "run_results": [
            {
                "run_id": f"{scenario_class}_run",
                "scenario_id": scenario_ids[scenario_class],
                "scenario_class": scenario_class,
                "route_id": route_ids[scenario_class],
                "run_dir": f"runs/natural/{scenario_class}_run",
                "verdict": "pass",
                "artifacts": {
                    "artifact_completeness": (
                        f"runs/natural/{scenario_class}_run/analysis/artifact_completeness/"
                        "artifact_completeness_report.json"
                    ),
                },
                "artifact_completeness": {
                    "status": "pass",
                    "artifact_complete": True,
                    "missing_artifacts": [],
                    "missing_manifest_fields": [],
                    "missing_control_trace_fields": [],
                },
                "traffic_light_expected_behavior": traffic_light_behaviors.get(scenario_class),
                "traffic_light_expectation_source": "manifest"
                if scenario_class in traffic_light_behaviors
                else None,
                "traffic_light_stimulus_mode": "deterministic_gt_control"
                if scenario_class in traffic_light_behaviors
                else None,
                "traffic_light_claim_grade": True if scenario_class in traffic_light_behaviors else None,
            }
            for scenario_class in classes
        ],
    }


def _write_natural_report(path: Path, report: dict) -> None:
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_artifact_completeness_files(report: dict, root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    report["suite_run_root"] = str(root)
    with (root / "suite_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "schema_version": "natural_driving_suite_manifest.v1",
                "dry_run": False,
                "run_count": len(report["run_results"]),
                "scenario_class_filter": [],
                "scenario_id_filter": [],
                "runs": [
                    {
                        "run_id": run["run_id"],
                        "scenario_id": run["scenario_id"],
                        "route_id": run["route_id"],
                        "scenario_class": run["scenario_class"],
                        "run_dir": str(root / str(run["run_id"])),
                        "status": "success",
                        "return_code": 0,
                        "artifact_index_status": "found",
                    }
                    for run in report["run_results"]
                ],
            },
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")
    with (root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "scenario_id",
                "route_id",
                "scenario_class",
                "run_dir",
                "status",
                "return_code",
                "artifact_index_status",
            ],
        )
        writer.writeheader()
        for run in report["run_results"]:
            writer.writerow(
                {
                    "run_id": run["run_id"],
                    "scenario_id": run["scenario_id"],
                    "route_id": run["route_id"],
                    "scenario_class": run["scenario_class"],
                    "run_dir": str(root / str(run["run_id"])),
                    "status": "success",
                    "return_code": 0,
                    "artifact_index_status": "found",
                }
            )
    for run in report["run_results"]:
        actual_run_dir = root / str(run["run_id"])
        run["run_dir"] = str(actual_run_dir)
        output = actual_run_dir / "analysis" / "artifact_completeness"
        output.mkdir(parents=True, exist_ok=True)
        path = output / "artifact_completeness_report.json"
        payload = {
            "schema_version": "run_artifact_completeness.v1",
            "status": "pass",
            "artifact_complete": True,
            "run_dir": str(actual_run_dir),
            "run_id": run["run_id"],
            "scenario_id": run["scenario_id"],
            "route_id": run["route_id"],
            "scenario_class": run["scenario_class"],
            "missing_artifacts": [],
            "missing_manifest_fields": [],
            "invalid_manifest_source_fields": [],
            "missing_control_trace_fields": [],
            "invalid_report_source_fields": [],
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        run["artifacts"]["artifact_completeness"] = str(path)


def test_goal_audit_marks_complete_only_with_all_evidence(tmp_path: Path) -> None:
    natural_report = _complete_natural_driving_report()
    _write_artifact_completeness_files(natural_report, tmp_path / "runs")
    natural_report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(natural_report_path, natural_report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report_path=natural_report_path,
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    assert audit["status"] == "complete"
    assert audit["missing_evidence"] == []
    assert audit["sections"]["ab"]["direct_cadence"]["status"] == "pass"
    assert audit["sections"]["ab"]["direct_cadence"]["evidence_scope"] == "transport_materialization_cadence"
    assert audit["sections"]["ab"]["direct_cadence"]["strict_gate_duration_basis"] == "manifest_duration_s"
    assert "does not prove curve health" in audit["sections"]["ab"]["direct_cadence"]["claim_not_supported"]
    assert audit["sections"]["random_regression"]["status"] == "pass"
    assert audit["sections"]["natural_driving"]["status"] == "pass"
    assert audit["sections"]["natural_driving"]["report_file_backed"] is True
    assert audit["sections"]["ab_evidence_coherence"]["status"] == "single_report"
    assert audit["sections"]["demo_recording"]["accepted_for_goal"] is True
    assert "do not claim carla_direct is default" in audit["guardrails"]


def test_goal_audit_rejects_in_memory_natural_driving_report_for_completion() -> None:
    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report=_complete_natural_driving_report(),
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    natural = audit["sections"]["natural_driving"]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert natural["report_file_backed"] is False
    assert "natural_driving_report_path_missing" in natural["claim_blockers"]


def test_goal_audit_report_path_requires_suite_plan_files(tmp_path: Path) -> None:
    report = _complete_natural_driving_report()
    suite_root = tmp_path / "runs"
    _write_artifact_completeness_files(report, suite_root)
    (suite_root / "run_matrix.csv").unlink()
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report_path=report_path,
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    natural = audit["sections"]["natural_driving"]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert natural["suite_plan_file_missing"] == ["run_matrix.csv"]
    assert "suite_plan_files_missing" in natural["claim_blockers"]


def test_goal_audit_report_path_requires_run_matrix_identity_match(tmp_path: Path) -> None:
    report = _complete_natural_driving_report()
    suite_root = tmp_path / "runs"
    _write_artifact_completeness_files(report, suite_root)
    matrix_path = suite_root / "run_matrix.csv"
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    for row in rows:
        if row["run_id"] == "lane_keep_run":
            row["route_id"] = "curve217"
            row["run_dir"] = str(suite_root / "traffic_light_red_stop_run")
    with matrix_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report_path=report_path,
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    natural = audit["sections"]["natural_driving"]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "run_matrix.route_id:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "run_matrix.run_dir:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "suite_plan_files_mismatch" in natural["claim_blockers"]


def test_goal_audit_report_path_requires_suite_manifest_to_match_full_suite(
    tmp_path: Path,
) -> None:
    report = _complete_natural_driving_report()
    suite_root = tmp_path / "runs"
    _write_artifact_completeness_files(report, suite_root)
    manifest_path = suite_root / "suite_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["dry_run"] = True
    manifest["scenario_class_filter"] = ["lane_keep"]
    manifest["run_count"] = 1
    manifest["runs"][0]["route_id"] = "curve217"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report_path=report_path,
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    natural = audit["sections"]["natural_driving"]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "suite_manifest.dry_run" in natural["suite_plan_file_mismatches"]
    assert "suite_manifest.scenario_class_filter" in natural["suite_plan_file_mismatches"]
    assert "suite_manifest.run_count" in natural["suite_plan_file_mismatches"]
    assert "suite_manifest.route_id:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "suite_plan_files_mismatch" in natural["claim_blockers"]


def test_goal_audit_report_path_requires_successful_suite_execution_index(
    tmp_path: Path,
) -> None:
    report = _complete_natural_driving_report()
    suite_root = tmp_path / "runs"
    _write_artifact_completeness_files(report, suite_root)
    manifest_path = suite_root / "suite_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["runs"][0]["status"] = "planned"
    manifest["runs"][0]["return_code"] = None
    manifest["runs"][0]["artifact_index_status"] = "not_checked"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    matrix_path = suite_root / "run_matrix.csv"
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    rows[0]["status"] = "dry_run"
    rows[0]["return_code"] = ""
    rows[0]["artifact_index_status"] = "not_checked"
    with matrix_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report_path=report_path,
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    natural = audit["sections"]["natural_driving"]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "suite_manifest.status:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "suite_manifest.return_code:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "suite_manifest.artifact_index_status:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "run_matrix.status:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "run_matrix.return_code:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "run_matrix.artifact_index_status:lane_keep_run" in natural["suite_plan_file_mismatches"]
    assert "suite_plan_files_mismatch" in natural["claim_blockers"]


def test_goal_audit_requires_natural_driving_report_for_completion() -> None:
    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        demo_recording={
            "status": "ready",
            "route_count": 3,
            "carla_ok_count": 3,
            "dreamview_ok_count": 3,
        },
    )

    assert audit["status"] == "incomplete"
    assert audit["sections"]["natural_driving"]["status"] == "missing"
    assert any("natural_driving_report.json pass" in item for item in audit["missing_evidence"])
    assert audit["sections"]["demo_recording"]["accepted_for_goal"] is False


def test_goal_audit_rejects_natural_driving_missing_required_scenario_classes() -> None:
    report = _complete_natural_driving_report()
    report["run_results"] = [
        item
        for item in report["run_results"]
        if item["scenario_class"] != "traffic_light_red_to_green_release"
    ]

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    assert audit["status"] == "incomplete"
    assert audit["sections"]["natural_driving"]["status"] == "incomplete"
    assert audit["sections"]["natural_driving"]["missing_scenario_classes"] == [
        "traffic_light_red_to_green_release"
    ]


def test_goal_audit_rejects_natural_driving_pass_without_full_claim_flag() -> None:
    report = _complete_natural_driving_report()
    report.pop("capability_coverage")
    report["verdict"].pop("can_claim_full_natural_driving")

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert natural["can_claim_full_natural_driving"] is False
    assert "can_claim_full_natural_driving_not_true" in natural["claim_blockers"]
    assert any("can_claim_full_natural_driving=true" in item for item in audit["missing_evidence"])


def test_goal_audit_rejects_filtered_natural_driving_suite_plan() -> None:
    report = _complete_natural_driving_report()
    report["verdict"]["filtered_suite_plan"] = True
    report["verdict"]["scenario_class_filter"] = ["lane_keep", "traffic_light_red_stop"]
    report["suite_plan_scope"] = {
        "has_suite_manifest": True,
        "has_run_matrix": True,
        "filtered": True,
        "scenario_class_filter": ["lane_keep", "traffic_light_red_stop"],
        "scenario_id_filter": [],
    }

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    markdown = render_goal_audit_markdown(audit)

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert natural["can_claim_full_natural_driving"] is False
    assert natural["filtered_suite_plan"] is True
    assert natural["scenario_class_filter"] == ["lane_keep", "traffic_light_red_stop"]
    assert "filtered_suite_plan" in natural["claim_blockers"]
    assert "natural driving filtered suite: `True`" in markdown


def test_goal_audit_rejects_natural_driving_without_suite_plan_evidence() -> None:
    report = _complete_natural_driving_report()
    report["verdict"]["suite_plan_missing"] = ["suite_manifest.json", "run_matrix.csv"]
    report["suite_plan_scope"] = {
        "has_suite_manifest": False,
        "has_run_matrix": False,
        "filtered": False,
        "scenario_class_filter": [],
        "scenario_id_filter": [],
    }

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    markdown = render_goal_audit_markdown(audit)

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert natural["can_claim_full_natural_driving"] is False
    assert natural["suite_plan_missing"] == ["suite_manifest.json", "run_matrix.csv"]
    assert "suite_plan_missing" in natural["claim_blockers"]
    assert "natural driving suite plan missing: `suite_manifest.json, run_matrix.csv`" in markdown


def test_goal_audit_rejects_missing_required_natural_driving_scenario_ids() -> None:
    report = _complete_natural_driving_report()
    report["verdict"]["missing_required_scenario_ids"] = ["curve_diagnostic_213"]
    report["capability_coverage"]["missing_required_scenario_ids"] = ["curve_diagnostic_213"]

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    markdown = render_goal_audit_markdown(audit)

    assert audit["status"] == "incomplete"
    assert natural["status"] == "incomplete"
    assert natural["can_claim_full_natural_driving"] is False
    assert natural["missing_required_scenario_ids"] == ["curve_diagnostic_213"]
    assert "missing_required_scenario_ids" in natural["claim_blockers"]
    assert "natural driving missing scenario ids: `1`" in markdown


def test_goal_audit_rejects_natural_driving_scenario_identity_mismatch() -> None:
    report = _complete_natural_driving_report()
    mismatch = {
        "scenario_id": "lane_keep_097",
        "expected_route_id": "lane097",
        "observed_route_ids": ["curve217"],
    }
    report["capability_coverage"]["scenario_identity_mismatches"] = [mismatch]

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert natural["can_claim_full_natural_driving"] is False
    assert natural["scenario_identity_mismatches"] == [mismatch]
    assert "scenario_identity_mismatch" in natural["claim_blockers"]


def test_goal_audit_rejects_natural_driving_without_artifact_completeness_evidence() -> None:
    report = _complete_natural_driving_report()
    for run in report["run_results"]:
        if run["scenario_class"] == "lane_keep":
            run.pop("artifact_completeness", None)
            run["artifacts"].pop("artifact_completeness", None)
            break

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "artifact_completeness_not_proven" in natural["claim_blockers"]
    assert natural["artifact_completeness_blockers"][0]["reason"] == "artifact_completeness_missing"
    markdown = render_goal_audit_markdown(audit)
    assert "Natural Driving Artifact Completeness Blockers" in markdown
    assert "artifact_completeness_missing" in markdown


def test_goal_audit_rejects_natural_driving_failed_artifact_completeness() -> None:
    report = _complete_natural_driving_report()
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_red_stop":
            run["artifact_completeness"] = {
                "status": "insufficient_data",
                "artifact_complete": False,
                "missing_artifacts": ["traffic_light_behavior_report.json"],
                "missing_manifest_fields": [],
                "missing_control_trace_fields": [],
            }
            break

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    blocker = natural["artifact_completeness_blockers"][0]
    assert blocker["reason"] == "artifact_completeness_not_pass"
    assert "traffic_light_behavior_report.json" in blocker["missing_artifacts"]
    markdown = render_goal_audit_markdown(audit)
    assert "traffic_light_behavior_report.json" in markdown


def test_goal_audit_surfaces_invalid_artifact_source_fields() -> None:
    report = _complete_natural_driving_report()
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_red_stop":
            run["artifact_completeness"] = {
                "status": "insufficient_data",
                "artifact_complete": False,
                "missing_artifacts": [],
                "missing_manifest_fields": [],
                "invalid_manifest_source_fields": ["algorithm_variant_manifest_path"],
                "missing_control_trace_fields": [],
                "invalid_report_source_fields": [
                    "traffic_light_behavior.scenario_class",
                    "traffic_light_behavior.route_id",
                ],
            }
            break

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    blocker = natural["artifact_completeness_blockers"][0]
    markdown = render_goal_audit_markdown(audit)

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "artifact_completeness_not_proven" in natural["claim_blockers"]
    assert blocker["invalid_manifest_source_fields"] == ["algorithm_variant_manifest_path"]
    assert "traffic_light_behavior.scenario_class" in blocker["invalid_report_source_fields"]
    assert "traffic_light_behavior.route_id" in blocker["invalid_report_source_fields"]
    assert "invalid_report_source_fields" in markdown
    assert "traffic_light_behavior.scenario_class" in markdown


def test_goal_audit_rejects_artifact_complete_true_with_invalid_report_context() -> None:
    report = _complete_natural_driving_report()
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_green_go":
            run["artifact_completeness"] = {
                "status": "pass",
                "artifact_complete": True,
                "missing_artifacts": [],
                "missing_manifest_fields": [],
                "invalid_manifest_source_fields": [],
                "missing_control_trace_fields": [],
                "invalid_report_source_fields": ["traffic_light_behavior.route_id"],
            }
            break

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    blocker = audit["sections"]["natural_driving"]["artifact_completeness_blockers"][0]

    assert audit["status"] == "incomplete"
    assert blocker["status"] == "pass"
    assert blocker["artifact_complete"] is True
    assert blocker["invalid_report_source_fields"] == ["traffic_light_behavior.route_id"]


def test_goal_audit_report_path_requires_artifact_completeness_file_exists(tmp_path: Path) -> None:
    report = _complete_natural_driving_report()
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report_path=report_path,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    blocker = natural["artifact_completeness_blockers"][0]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "artifact_completeness_not_proven" in natural["claim_blockers"]
    assert blocker["reason"] == "artifact_completeness_report_path_not_found"


def test_goal_audit_report_path_rejects_stale_artifact_completeness_file(tmp_path: Path) -> None:
    report = _complete_natural_driving_report()
    _write_artifact_completeness_files(report, tmp_path / "runs")
    bad_path = Path(report["run_results"][0]["artifacts"]["artifact_completeness"])
    payload = json.loads(bad_path.read_text(encoding="utf-8"))
    payload["status"] = "insufficient_data"
    payload["artifact_complete"] = False
    payload["missing_control_trace_fields"] = ["apollo_steer_raw"]
    bad_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report_path=report_path,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    blocker = natural["artifact_completeness_blockers"][0]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert blocker["reason"] == "artifact_completeness_report_file_not_pass"
    assert blocker["status"] == "insufficient_data"
    assert blocker["artifact_complete"] is False
    assert blocker["missing_control_trace_fields"] == ["apollo_steer_raw"]


def test_goal_audit_report_path_rejects_misattached_artifact_completeness_file(
    tmp_path: Path,
) -> None:
    report = _complete_natural_driving_report()
    _write_artifact_completeness_files(report, tmp_path / "runs")
    lane_run = next(run for run in report["run_results"] if run["scenario_class"] == "lane_keep")
    traffic_run = next(
        run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop"
    )
    lane_run["artifacts"]["artifact_completeness"] = traffic_run["artifacts"]["artifact_completeness"]
    report_path = tmp_path / "natural_driving_report.json"
    _write_natural_report(report_path, report)

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report_path=report_path,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    blocker = natural["artifact_completeness_blockers"][0]

    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert blocker["reason"] == "artifact_completeness_report_file_not_pass"
    assert "run_id" in blocker["context_mismatches"]
    assert "scenario_id" in blocker["context_mismatches"]
    assert "scenario_class" in blocker["context_mismatches"]
    assert "route_id" in blocker["context_mismatches"]
    assert "run_dir" in blocker["context_mismatches"]


def test_goal_audit_rejects_natural_driving_without_manifest_backed_traffic_expectation() -> None:
    report = _complete_natural_driving_report()
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_red_stop":
            run["traffic_light_expected_behavior"] = None
            run["traffic_light_expectation_source"] = None

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert "traffic_light_expectation_not_manifest_backed" in natural["claim_blockers"]
    assert natural["traffic_light_expectation_blockers"][0]["reason"] == "traffic_light_expectation_missing"


def test_goal_audit_rejects_natural_driving_traffic_expectation_mismatch() -> None:
    report = _complete_natural_driving_report()
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_green_go":
            run["traffic_light_expected_behavior"] = "red_stop"

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    assert audit["status"] == "incomplete"
    assert natural["status"] == "fail"
    assert "traffic_light_expectation_mismatch" in natural["claim_blockers"]


def test_goal_audit_keeps_natural_driving_problem_run_details() -> None:
    report = _complete_natural_driving_report()
    report["summary"] = {
        "pass_count": 5,
        "warn_count": 0,
        "fail_count": 0,
        "insufficient_data_count": 1,
    }
    report["verdict"] = {
        "status": "insufficient_data",
        "failed_runs": [],
        "insufficient_data_runs": ["traffic_light_red_stop_run"],
        "warning_runs": [],
    }
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_red_stop":
            run.update(
                {
                    "verdict": "insufficient_data",
                    "failure_reason": "missing_required_artifacts",
                    "route_id": "traffic_light_red_stop",
                    "missing_artifacts": [
                        "traffic_light_contract_report.json",
                        "traffic_light_behavior_report.json",
                    ],
                    "missing_fields": ["stopped_at_red"],
                    "runtime_contract_status": "aligned",
                    "control_handoff_status": "control_consuming_with_nonzero_planning",
                    "control_health_status": "pass",
                }
            )

    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=report,
        demo_recording={"status": "ready", "route_count": 3},
    )

    natural = audit["sections"]["natural_driving"]
    details = natural["problem_run_details"]
    assert audit["status"] == "incomplete"
    assert natural["status"] == "insufficient_data"
    assert details[0]["failure_reason"] == "missing_required_artifacts"
    assert "traffic_light_contract_report.json" in details[0]["missing_artifacts"]
    assert "stopped_at_red" in details[0]["missing_fields"]
    md = render_goal_audit_markdown(audit)
    assert "Natural Driving Problem Runs" in md
    assert "traffic_light_contract_report.json" in md


def test_goal_audit_keeps_missing_curve_and_calibration_explicit() -> None:
    ab = _complete_ab_report()
    ab["comparisons"] = [item for item in ab["comparisons"] if item["route_id"] not in {"curve217", "curve213"}]
    audit = build_goal_audit(
        ab_report=ab,
        natural_driving_report=_complete_natural_driving_report(),
        demo_recording={"status": "ready"},
    )

    assert audit["status"] == "incomplete"
    assert any("curve217" in item for item in audit["missing_evidence"])
    assert any("curve213" in item for item in audit["missing_evidence"])
    assert any("calibration report" in item for item in audit["missing_evidence"])
    assert audit["sections"]["demo_recording"]["accepted_for_goal"] is False


def test_goal_audit_keeps_hard_gate_problem_details_for_degraded_lane217() -> None:
    ab = _complete_ab_report()
    for comparison in ab["comparisons"]:
        if comparison["route_id"] == "lane217":
            comparison["status"] = "candidate_degraded"
            comparison["reasons"] = ["lateral_error_p95_m degraded", "candidate failure_reason=lane_invasion"]
    for row in ab["run_results"]:
        if row["route_id"] == "lane217" and row["backend"] == "ros2_gt":
            row.update(
                {
                    "lateral_error_p95_m": 0.68,
                    "heading_error_p95_rad": 0.06,
                    "failure_reason": "success",
                }
            )
        if row["route_id"] == "lane217" and row["backend"] == "carla_direct":
            row.update(
                {
                    "lateral_error_p95_m": 0.98,
                    "heading_error_p95_rad": 0.01,
                    "failure_reason": "lane_invasion",
                    "lane_invasion_count": 1,
                    "first_safety_event_type": "lane_invasion",
                    "first_safety_event_time_s": 43.33,
                    "first_safety_event_frame_id": "11314",
                    "first_safety_event_route_s": 170.0,
                    "first_safety_event_cross_track_error_m": 1.05,
                    "first_safety_event_heading_error_rad": 0.01,
                    "first_safety_event_ego_speed_mps": 6.89,
                    "first_safety_event_apollo_steer_raw": 0.0,
                    "first_safety_event_bridge_steer_mapped": 0.0,
                    "first_safety_event_carla_steer_applied": 0.0001,
                    "first_safety_event_brake_applied": 0.38,
                    "first_safety_event_control_context": "no_lateral_command_at_safety_event",
                    "first_safety_event_source": "timeseries:lane_invasion_count",
                    "safety_window_sample_count": 40,
                    "safety_window_cross_track_error_delta_m": 0.15,
                    "safety_window_apollo_steer_raw_abs_p95": 0.0,
                    "safety_window_bridge_steer_mapped_abs_p95": 0.0,
                    "safety_window_carla_steer_applied_abs_p95": 0.00015,
                    "safety_window_control_context": "sustained_no_lateral_command_before_safety_event",
                    "artifact_dir": "runs/synthetic_lane217_candidate",
                    "timeseries_path": "runs/synthetic_lane217_candidate/timeseries.csv",
                    "summary_path": "runs/synthetic_lane217_candidate/summary.json",
                }
            )

    audit = build_goal_audit(
        ab_report=ab,
        natural_driving_report=_complete_natural_driving_report(),
    )

    details = audit["sections"]["ab"]["hard_gate_problem_details"]
    lane217 = next(item for item in details if item["route_id"] == "lane217")
    blockers = audit["sections"]["ab"]["hard_gate_control_context_blockers"]
    assert audit["status"] == "incomplete"
    assert lane217["status"] == "candidate_degraded"
    assert "lateral_error_p95_m degraded" in lane217["reasons"]
    assert lane217["candidate_metrics"]["failure_reason"] == "lane_invasion"
    assert lane217["candidate_first_safety_event"]["first_safety_event_route_s"] == 170.0
    assert lane217["candidate_first_safety_event"]["first_safety_event_cross_track_error_m"] == 1.05
    assert lane217["candidate_first_safety_event"]["first_safety_event_apollo_steer_raw"] == 0.0
    assert lane217["candidate_first_safety_event"]["first_safety_event_bridge_steer_mapped"] == 0.0
    assert lane217["candidate_first_safety_event"]["first_safety_event_control_context"] == "no_lateral_command_at_safety_event"
    assert lane217["candidate_first_safety_event"]["safety_window_control_context"] == "sustained_no_lateral_command_before_safety_event"
    assert blockers[0]["route_id"] == "lane217"
    assert blockers[0]["window_control_context"] == "sustained_no_lateral_command_before_safety_event"
    assert blockers[0]["candidate_artifacts"]["timeseries_path"] == "runs/synthetic_lane217_candidate/timeseries.csv"
    assert "tools/analyze_apollo_shadow_mode.py" in blockers[0]["shadow_mode_command"]
    assert "runs/synthetic_lane217_candidate/timeseries.csv" in blockers[0]["shadow_mode_command"]
    assert "--summary runs/synthetic_lane217_candidate/summary.json" in blockers[0]["shadow_mode_command"]
    assert blockers[0]["route_curve_artifact_gap"]["status"] == "insufficient_data"
    assert blockers[0]["route_curve_artifact_gap"]["failure_reason"] == "missing_timeseries"
    assert "tools/check_route_curve_artifact_gap.py" in blockers[0]["route_curve_artifact_gap_command"]
    assert "--summary runs/synthetic_lane217_candidate/summary.json" in blockers[0]["route_curve_artifact_gap_command"]
    assert any("Apollo lateral/shadow-mode semantics" in item for item in audit["next_actions"])
    assert any("per-frame P1 matched/target/trajectory" in item for item in audit["next_actions"])
    assert "apollo_shadow_mode_from_timeseries" in audit["next_action_commands"]
    assert "route_curve_artifact_gap_from_timeseries" in audit["next_action_commands"]
    markdown = render_goal_audit_markdown(audit)
    assert "A/B Hard-Gate Problem Details" in markdown
    assert "Hard-Gate Control Context Blockers" in markdown
    assert "lane_invasion" in markdown
    assert "170.0" in markdown
    assert "no_lateral_command_at_safety_event" in markdown
    assert "sustained_no_lateral_command_before_safety_event" in markdown
    assert "Suggested shadow-mode commands" in markdown
    assert "Suggested route-curve artifact-gap commands" in markdown
    assert "runs/synthetic_lane217_candidate/timeseries.csv" in markdown


def test_goal_audit_does_not_accept_historical_demo_before_route_evidence() -> None:
    audit = build_goal_audit(
        ab_report={"verdict": {}, "run_results": [], "comparisons": []},
        natural_driving_report=_complete_natural_driving_report(),
        demo_recording={
            "status": "ready",
            "route_count": 1,
            "carla_ok_count": 1,
            "dreamview_ok_count": 1,
        },
    )

    assert audit["status"] == "incomplete"
    assert audit["sections"]["demo_recording"]["status"] == "ready"
    assert audit["sections"]["demo_recording"]["accepted_for_goal"] is False
    assert any("demo recording after accepted Town01 evidence" in item for item in audit["missing_evidence"])


def test_goal_audit_does_not_accept_curve_positive_without_route_health_evidence() -> None:
    ab = _complete_ab_report()
    for row in ab["run_results"]:
        if row["route_id"] in {"curve217", "curve213"} and row["backend"] == "carla_direct":
            row["artifact_complete"] = False
            row["route_health_source"] = None
            row["lateral_error_p95_m"] = None
            row["heading_error_p95_rad"] = None

    audit = build_goal_audit(
        ab_report=ab,
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {
                "keep_legacy_steer_scale_025": True,
                "enable_physical_mapping": False,
            },
        },
        natural_driving_report=_complete_natural_driving_report(),
        demo_recording={"status": "ready", "route_count": 3},
    )

    assert audit["status"] == "incomplete"
    curve217 = audit["sections"]["ab"]["curve_diagnostics"]["curve217"]
    assert curve217["comparison_status"] == "candidate_positive"
    assert curve217["status"] == "insufficient_route_health_evidence"
    assert "artifact_complete" in curve217["route_health_evidence"]["missing"]
    assert any("curve217" in item for item in audit["missing_evidence"])


def test_goal_audit_reports_direct_contract_mismatch_before_cadence_threshold() -> None:
    ab = _complete_ab_report()
    for row in ab["run_results"]:
        if row["route_id"] == "lane097" and row["backend"] == "carla_direct":
            row["direct_transport_contract_status"] = "mismatch"
    for comparison in ab["comparisons"]:
        if comparison["route_id"] == "lane097":
            comparison["cadence_comparison"] = {
                "bridge_loc_hz_ratio": 0.2,
                "bridge_chassis_hz_ratio": 0.2,
            }

    audit = build_goal_audit(
        ab_report=ab,
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"enable_physical_mapping": False},
        },
        natural_driving_report=_complete_natural_driving_report(),
    )

    direct = audit["sections"]["ab"]["direct_cadence"]
    assert direct["status"] == "contract_not_aligned"
    assert direct["contract_not_aligned"][0]["route_id"] == "lane097"
    assert direct["below_threshold"][0]["route_id"] == "lane097"


def test_goal_audit_markdown_lists_missing_evidence() -> None:
    audit = build_goal_audit(ab_report={"verdict": {}, "run_results": [], "comparisons": []})

    markdown = render_goal_audit_markdown(audit)

    assert "# Town01 Goal Audit" in markdown
    assert "Missing Evidence" in markdown
    assert "A/B evidence coherence" in markdown
    assert "Next Actions" in markdown
    assert "Recommended Commands" in markdown
    assert "Direct Cadence Evidence" in markdown
    assert "does not prove curve health" in markdown
    assert "strict_gate_duration_basis" in markdown
    assert "097/217/031 hard-gate" in markdown
    assert "rerun or inspect 097/217/031 hard-gate A/B" in markdown
    assert "run_town01_natural_driving_suite.py" in markdown


def test_goal_audit_next_actions_include_natural_driving_when_report_missing() -> None:
    audit = build_goal_audit(
        ab_report=_complete_ab_report(),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"keep_legacy_steer_scale_025": True, "enable_physical_mapping": False},
        },
        demo_recording={"status": "ready", "route_count": 3},
    )

    assert any("natural-driving suite" in item for item in audit["next_actions"])
    assert "natural_driving_online" in audit["next_action_commands"]
    assert "--fail-on-postprocess-status fail,warn,insufficient_data" in audit["next_action_commands"]["natural_driving_online"]
    assert "hard_gate_ab_online" in audit["next_action_commands"]
    assert "--routes 097,217,031" in audit["next_action_commands"]["hard_gate_ab_online"]
    assert "random_regression_ab_online" in audit["next_action_commands"]
    assert "random_regression_pool_20260416.yaml" in audit["next_action_commands"]["random_regression_ab_online"]
    assert "calibration_gate_results_from_ab" in audit["next_action_commands"]
    assert "build_calibration_gate_results.py" in audit["next_action_commands"]["calibration_gate_results_from_ab"]
    assert "calibration_report_from_trials" in audit["next_action_commands"]
    assert "analyze_calibration_report.py" in audit["next_action_commands"]["calibration_report_from_trials"]


def test_goal_audit_cli_writes_outputs(tmp_path: Path) -> None:
    ab_report = tmp_path / "ab_report.json"
    demo = tmp_path / "town01_demo_recording_inspection.json"
    natural = tmp_path / "natural_driving_report.json"
    ab_report.write_text(json.dumps(_complete_ab_report()), encoding="utf-8")
    demo.write_text(json.dumps({"status": "ready", "route_count": 3}), encoding="utf-8")
    natural.write_text(json.dumps(_complete_natural_driving_report()), encoding="utf-8")
    out = tmp_path / "audit"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--ab-report",
            str(ab_report),
            "--demo-recording",
            str(demo),
            "--natural-driving-report",
            str(natural),
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["audit"]["status"] == "incomplete"
    assert (out / "town01_goal_audit.json").is_file()
    assert (out / "town01_goal_audit.md").is_file()


def test_goal_audit_cli_can_fail_on_incomplete_status(tmp_path: Path) -> None:
    ab_report = tmp_path / "ab_report.json"
    ab_report.write_text(json.dumps({"verdict": {}, "run_results": [], "comparisons": []}), encoding="utf-8")
    out = tmp_path / "audit"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--ab-report",
            str(ab_report),
            "--out",
            str(out),
            "--fail-on-status",
            "incomplete",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["audit"]["status"] == "incomplete"
    assert (out / "town01_goal_audit.json").is_file()


def test_find_goal_ab_report_paths_collects_latest_goal_route_coverage(tmp_path: Path) -> None:
    root = tmp_path / "runs" / "ab"
    older_hard = root / "older_hard" / "analysis" / "ab_report.json"
    curve = root / "curve" / "analysis" / "ab_report.json"
    latest_lane = root / "latest_lane" / "analysis" / "ab_report.json"
    random = root / "random" / "analysis" / "ab_report.json"
    reports = [
        (
            older_hard,
            {
                "comparisons": [
                    {"route_id": "lane097", "status": "candidate_positive"},
                    {"route_id": "lane217", "status": "candidate_positive"},
                    {"route_id": "junction031", "status": "candidate_positive"},
                ]
            },
            1,
        ),
        (
            curve,
            {
                "comparisons": [
                    {"route_id": "curve217", "status": "candidate_positive"},
                    {"route_id": "curve213", "status": "candidate_positive"},
                ]
            },
            2,
        ),
        (
            random,
            {
                "comparisons": [
                    {"route_id": "random_lane_183_044", "status": "candidate_positive"},
                ]
            },
            3,
        ),
        (
            latest_lane,
            {
                "comparisons": [
                    {"route_id": "lane097", "status": "candidate_degraded"},
                ]
            },
            4,
        ),
    ]
    for path, payload, mtime in reports:
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        path.touch()
        import os

        os.utime(path, (mtime, mtime))

    selected = find_goal_ab_report_paths(root)

    assert selected[0] == latest_lane
    assert curve in selected
    assert older_hard in selected
    assert random in selected


def test_goal_audit_does_not_mark_mixed_batch_ab_evidence_complete() -> None:
    hard = _complete_ab_report()
    hard["batch_id"] = "hard_batch"
    hard["comparisons"] = [item for item in hard["comparisons"] if item["route_id"] in {"lane097", "lane217", "junction031"}]
    hard["run_results"] = [item for item in hard["run_results"] if item["route_id"] in {"lane097", "lane217", "junction031"}]

    curve_random = _complete_ab_report()
    curve_random["batch_id"] = "curve_random_batch"
    curve_random["comparisons"] = [
        item
        for item in curve_random["comparisons"]
        if item["route_id"] not in {"lane097", "lane217", "junction031"}
    ]
    curve_random["run_results"] = [
        item
        for item in curve_random["run_results"]
        if item["route_id"] not in {"lane097", "lane217", "junction031"}
    ]

    # Build directly from merged reports so this test focuses on evidence coherence
    # rather than filesystem discovery.
    audit = build_goal_audit(
        ab_report=merge_ab_reports([hard, curve_random]),
        calibration_report={
            "schema_version": "calibration_report.v1",
            "no_regression": {"promotion_allowed": True, "failed_gates": [], "missing_gates": []},
            "recommendation": {"keep_legacy_steer_scale_025": True, "enable_physical_mapping": False},
        },
        demo_recording={"status": "ready", "route_count": 3, "carla_ok_count": 3, "dreamview_ok_count": 3},
    )

    assert audit["sections"]["ab"]["status"] == "ab_hard_gate_pass"
    assert audit["sections"]["ab_evidence_coherence"]["status"] == "mixed_batch"
    assert audit["status"] == "incomplete"
    assert any("single coherent A/B evidence batch" in item for item in audit["missing_evidence"])


def test_goal_audit_can_refresh_ab_report_from_source_manifest(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    baseline_dir = batch / "runs" / "baseline_lane097"
    run_dir = batch / "runs" / "candidate_lane097"
    stale_report = batch / "analysis" / "ab_report.json"
    manifest = batch / "ab_manifest.json"

    def write_run(root: Path, *, direct: bool) -> tuple[Path, Path]:
        artifacts = root / "artifacts"
        artifacts.mkdir(parents=True)
        summary = root / "summary.json"
        route_health = root / "analysis" / "route_health" / "route_health.json"
        timeseries = root / "timeseries.csv"
        route_health.parent.mkdir(parents=True)
        summary.write_text(
            json.dumps(
                {
                    "route_completion_ratio": 0.9,
                    "collision_count": 0,
                    "lane_invasion_count": 0,
                    "planning_materialized": True,
                    "direct_control_apply_count": 30 if direct else 0,
                }
            ),
            encoding="utf-8",
        )
        route_health.write_text(
            json.dumps(
                {
                    "run_metrics": {
                        "lateral_error_p95_m": 0.1,
                        "heading_error_p95_rad": 0.01,
                    }
                }
            ),
            encoding="utf-8",
        )
        timeseries.write_text(
            "sim_time,ego_x,cross_track_error,heading_error,carla_steer_applied\n"
            "0.0,1.0,0.1,0.01,0.0\n"
            "1.0,2.0,0.1,0.01,0.0\n",
            encoding="utf-8",
        )
        (artifacts / "cyber_bridge_stats.json").write_text(
            json.dumps({"loc_count": 10, "chassis_count": 10}),
            encoding="utf-8",
        )
        if direct:
            (artifacts / "direct_bridge_stats.json").write_text(
                json.dumps(
                    {
                        "control_apply_mode": "frame_flush_only",
                        "stale_world_frame_policy": "always_republish",
                        "snapshot_count": 10,
                    }
                ),
                encoding="utf-8",
            )
        return summary, route_health

    baseline_summary, baseline_route_health = write_run(baseline_dir, direct=False)
    summary, route_health = write_run(run_dir, direct=True)
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "ab_manifest.v1",
                "batch_id": "refresh_test",
                "created_at": "2026-05-23T00:00:00Z",
                "route_set": "test",
                "baseline_backend": "ros2_gt",
                "candidate_backend": "carla_direct",
                "durations_s": [30],
                "fixed_variables": {},
                "allowed_differences": [],
                "missing_fixed_variables": [],
                "consistency_status": "pass",
                "notes": [],
                "runs": [
                    {
                        "run_id": "baseline_lane097",
                        "route_id": "lane097",
                        "backend": "ros2_gt",
                        "transport_mode": "ros2_gt",
                        "duration_s": 30,
                        "run_dir": str(baseline_dir),
                        "summary_path": str(baseline_summary),
                        "route_health_path": str(baseline_route_health),
                        "status": "success",
                    },
                    {
                        "run_id": "candidate_lane097",
                        "route_id": "lane097",
                        "backend": "carla_direct",
                        "transport_mode": "carla_direct",
                        "duration_s": 30,
                        "run_dir": str(run_dir),
                        "summary_path": str(summary),
                        "route_health_path": str(route_health),
                        "status": "success",
                        "direct_control_apply_mode": "frame_flush_only",
                        "direct_stale_world_frame_policy": "always_republish",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    stale_report.parent.mkdir(parents=True)
    stale_report.write_text(
        json.dumps(
            {
                "schema_version": "ab_report.v1",
                "source_manifest": str(manifest),
                "run_results": [
                    {
                        "run_id": "candidate_lane097",
                        "route_id": "lane097",
                        "backend": "carla_direct",
                        "direct_transport_contract_status": None,
                    }
                ],
                "comparisons": [],
                "verdict": {},
            }
        ),
        encoding="utf-8",
    )

    audit = build_goal_audit(
        ab_report_path=stale_report,
        refresh_ab_from_manifest=True,
    )

    observed = audit["sections"]["ab"]["direct_cadence"]["observed"]
    assert observed[0]["contract_status"] == "aligned"
    assert audit["sources"]["ab_refresh_from_manifest"] is True
