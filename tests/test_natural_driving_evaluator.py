from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.natural_driving import (
    NATURAL_DRIVING_REPORT_SCHEMA_VERSION,
    _capability_coverage,
    _suite_verdict,
    _traffic_light_control_evidence_verdict,
    analyze_natural_driving_suite,
    write_natural_driving_report,
)

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def copy_fixture(tmp_path: Path) -> Path:
    target = tmp_path / "simple_suite"
    shutil.copytree(FIXTURE_ROOT, target)
    return target


def test_synthetic_suite_passes_and_writes_reports(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)

    report = analyze_natural_driving_suite(suite_root)
    outputs = write_natural_driving_report(report, tmp_path / "out")

    assert report["schema_version"] == NATURAL_DRIVING_REPORT_SCHEMA_VERSION
    assert report["verdict"]["status"] == "pass"
    assert report["verdict"]["can_claim_full_natural_driving"] is False
    assert "curve_diagnostic" in report["capability_coverage"]["missing_required_scenario_classes"]
    assert "traffic_light_green_go" in report["capability_coverage"]["missing_required_scenario_classes"]
    assert report["run_count"] == 3
    assert report["summary"]["invalid_manifest_source_fields_run_count"] == 0
    assert report["summary"]["invalid_report_source_fields_run_count"] == 0
    assert report["problem_run_details"] == []
    assert all(run["control_trace_available"] is True for run in report["run_results"])
    assert all(run["invalid_manifest_source_fields"] == [] for run in report["run_results"])
    assert all(run["invalid_report_source_fields"] == [] for run in report["run_results"])
    assert all(run["routing_materialized"] is True for run in report["run_results"])
    assert all(run["planning_materialized"] is True for run in report["run_results"])
    assert all(
        run["control_handoff_status"] == "control_consuming_with_nonzero_planning"
        for run in report["run_results"]
    )
    assert all(run["artifacts"]["config_resolved"] for run in report["run_results"])
    assert all(run["artifacts"]["route_health_csv"] for run in report["run_results"])
    assert all(run["artifacts"]["curve_segments"] for run in report["run_results"])
    assert all(run["artifacts"]["route_health_summary"] for run in report["run_results"])
    assert all(run["artifacts"]["control_health"] for run in report["run_results"])
    assert all(run["control_health_status"] == "pass" for run in report["run_results"])
    assert all("evidence" in run for run in report["run_results"])
    lane_run = next(run for run in report["run_results"] if run["scenario_class"] == "lane_keep")
    assert lane_run["evidence"]["scenario_id"] == "lane_keep_097"
    assert lane_run["evidence"]["route_hard_gate_eligible"] is True
    assert "control_attribution_report.json" in lane_run["evidence"]["missing_artifacts"]
    assert {run["scenario_class"] for run in report["run_results"]} >= {
        "lane_keep",
        "junction_turn",
        "traffic_light_red_stop",
    }
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")
    assert red_run["evidence"]["traffic_light_evidence_status"] == "pass"
    assert red_run["traffic_light_expected_behavior"] == "red_stop"
    assert red_run["traffic_light_expectation_source"] == "manifest"
    assert red_run["traffic_light_stimulus_mode"] == "deterministic_gt_control"
    assert red_run["traffic_light_claim_grade"] is True
    assert red_run["traffic_light_control_available"] is True
    assert red_run["traffic_light_control_current_state"] == "RED"
    assert red_run["traffic_light_control_phase"] == "initial"
    assert Path(outputs["natural_driving_report"]).exists()
    assert Path(outputs["natural_driving_csv"]).exists()
    assert Path(outputs["natural_driving_summary"]).exists()
    csv_header = Path(outputs["natural_driving_csv"]).read_text(encoding="utf-8").splitlines()[0]
    assert "invalid_report_source_fields" in csv_header
    assert "invalid_manifest_source_fields" in csv_header
    assert "online_config_path" in csv_header
    assert "transport_mode_source" in csv_header


def test_natural_report_preserves_online_config_transport_provenance(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    manifest_path = lane_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["online_config_path"] = (
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
    )
    manifest["online_config_profile_name"] = (
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate"
    )
    manifest["transport_mode"] = "carla_direct"
    manifest["transport_mode_source"] = "online_config.algo.apollo.transport_mode"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    outputs = write_natural_driving_report(report, tmp_path / "out")
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert lane_run["transport_mode"] == "carla_direct"
    assert lane_run["transport_mode_source"] == "online_config.algo.apollo.transport_mode"
    assert lane_run["online_config_profile_name"].endswith("_direct_candidate")
    assert lane_run["online_config_path"].endswith("_direct_candidate.yaml")

    with Path(outputs["natural_driving_csv"]).open(encoding="utf-8", newline="") as handle:
        rows = {row["run_id"]: row for row in csv.DictReader(handle)}
    assert rows["lane_keep_097"]["transport_mode"] == "carla_direct"
    assert (
        rows["lane_keep_097"]["transport_mode_source"]
        == "online_config.algo.apollo.transport_mode"
    )
    assert rows["lane_keep_097"]["online_config_profile_name"].endswith("_direct_candidate")

    md_text = Path(outputs["natural_driving_summary"]).read_text(encoding="utf-8")
    assert "| lane_keep_097 | lane_keep | lane097 | carla_direct |" in md_text


def test_missing_control_latency_is_warning_not_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    summary_path = lane_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"].pop("control_latency_p95_ms", None)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    timeseries_path = lane_dir / "timeseries.csv"
    lines = timeseries_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    keep_indexes = [index for index, field in enumerate(header) if field != "control_latency_ms"]
    rewritten = [
        ",".join(header[index] for index in keep_indexes),
        *(
            ",".join(row.split(",")[index] for index in keep_indexes)
            for row in lines[1:]
        ),
    ]
    timeseries_path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "warn"
    assert lane_run["failure_reason"] == "control_latency_missing"
    assert lane_run["missing_fields"] == ["control_latency_p95_ms"]
    assert report["verdict"]["status"] == "warn"


def test_reconstructed_route_cannot_pass_natural_driving_hard_gate(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["route_source"] = "reconstructed_from_timeseries"
    route_health["evidence_level"] = "diagnostic_only"
    route_health["hard_gate_eligible"] = False
    route_health["route_evidence_reason"] = "reconstructed_from_timeseries_cannot_support_hard_gate"
    route_health["source"]["route_path"] = None
    route_health_path.write_text(json.dumps(route_health, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_not_hard_gate_eligible"
    assert lane_run["route_source"] == "reconstructed_from_timeseries"
    assert lane_run["route_evidence_level"] == "diagnostic_only"
    assert lane_run["route_hard_gate_eligible"] is False
    assert "route_health.hard_gate_eligible" in lane_run["missing_fields"]


def test_claim_grade_manifest_route_does_not_require_standalone_route_path(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["route_source"] = "manifest_route_trace"
    route_health["evidence_level"] = "claim_grade_for_carla_route_geometry"
    route_health["hard_gate_eligible"] = True
    route_health["source"]["route_path"] = None
    route_health_path.write_text(json.dumps(route_health, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert "route_health.source.route_path" not in lane_run["invalid_report_source_fields"]
    assert lane_run["failure_reason"] != "route_health_missing_source_evidence"


def test_reference_line_insufficient_status_is_not_reported_as_missing(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    report_path = (
        suite_root
        / "lane_keep_097"
        / "analysis"
        / "apollo_reference_line_contract"
        / "apollo_reference_line_contract_report.json"
    )
    reference_report = json.loads(report_path.read_text(encoding="utf-8"))
    reference_report["status"] = "insufficient_data"
    reference_report["warnings"] = ["apollo_hdmap_projection_missing"]
    report_path.write_text(json.dumps(reference_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "apollo_reference_line_contract_insufficient"
    assert lane_run["failure_reason"] != "apollo_reference_line_contract_missing_status"


def test_terminal_stop_hold_blocks_unassisted_natural_driving_claim(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    manifest_path = lane_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["active_assists"] = ["terminal_stop_hold"]
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "assisted_pass"
    assert lane_run["failure_reason"] == "assisted_pass_unassisted_claim_blocked"
    assert lane_run["active_assists"] == ["terminal_stop_hold"]
    assert lane_run["blocking_assists"] == ["terminal_stop_hold"]
    assert lane_run["can_claim_unassisted_natural_driving"] is False
    assert "assist_ledger.blocking_assists.terminal_stop_hold" in lane_run["missing_fields"]
    assert report["summary"]["blocking_assist_run_count"] == 1


def test_apollo_control_apply_without_assists_can_claim_unassisted(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "pass"
    assert lane_run["backend"] == "apollo_cyberrt"
    assert lane_run["control_source"] == "/apollo/control"
    assert lane_run["routing_success_count"] >= 1
    assert lane_run["planning_nonempty_ratio"] >= 0.8
    assert lane_run["control_rx_count"] > 0
    assert lane_run["control_tx_count"] > 0
    assert lane_run["control_apply_count"] > 0
    assert lane_run["lateral_guard_apply_count"] == 0
    assert lane_run["trajectory_contract_guard_apply_count"] == 0
    assert lane_run["active_assists"] == []
    assert lane_run["can_claim_unassisted_natural_driving"] is True
    assert lane_run["why_not_claimable"] == []


def test_planning_claim_window_ratio_prevents_startup_empty_messages_from_blocking_claim(
    tmp_path: Path,
) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    summary_path = lane_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.setdefault("metrics", {})["planning_nonempty_ratio"] = 0.41
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    ref_path = lane_dir / "analysis" / "apollo_reference_line_contract" / "apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["evidence"]["nonempty_trajectory_ratio"] = 0.41
    ref["evidence"]["nonempty_trajectory_ratio_after_routing_segment_available"] = 0.95
    ref["evidence"]["nonempty_trajectory_ratio_after_first_nonempty"] = 1.0
    ref["evidence"]["planning_claim_window_nonempty_trajectory_ratio"] = 0.95
    ref["evidence"]["planning_claim_window_source"] = "after_routing_segment_available"
    ref_path.write_text(json.dumps(ref, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["planning_nonempty_ratio"] == 0.95
    assert "planning_nonempty_ratio_low" not in lane_run["why_not_claimable"]
    assert lane_run["verdict"] == "pass"
    assert lane_run["can_claim_unassisted_natural_driving"] is True


def test_apollo_control_source_prefers_handoff_artifact_over_summary_label(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    summary_path = lane_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["control_source"] = "external_stack"
    summary["controller"] = "external_stack"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["control_source"] == "/apollo/control"
    assert "control_source_not_apollo_control" not in lane_run["why_not_claimable"]
    assert lane_run["can_claim_unassisted_natural_driving"] is True


def test_missing_assist_evidence_is_insufficient_data_not_default_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    manifest_path = lane_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("assist_ledger", None)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "unassisted_claim_not_supported"
    assert lane_run["can_claim_unassisted_natural_driving"] is False
    assert "assist_ledger_missing_or_unknown" in lane_run["why_not_claimable"]
    assert "claimability.assist_ledger_missing_or_unknown" in lane_run["missing_fields"]


def test_dummy_lateral_cannot_pass_unassisted_claim_gate(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    manifest_path = lane_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["assist_ledger"] = {
        "schema_version": "assist_ledger.v1",
        "active_assists": ["dummy_lateral"],
        "blocking_assists": ["dummy_lateral"],
        "non_blocking_assists": [],
        "assist_sources": {"dummy_lateral": "manifest"},
        "assist_confidence": "explicit",
        "source_artifact": "manifest",
        "can_claim_unassisted_natural_driving": False,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "assisted_pass"
    assert lane_run["failure_reason"] == "assisted_pass_unassisted_claim_blocked"
    assert lane_run["blocking_assists"] == ["dummy_lateral"]
    assert lane_run["can_claim_unassisted_natural_driving"] is False
    assert "active_assists_present" in lane_run["why_not_claimable"]


def test_legacy_followstop_cannot_pass_unassisted_claim_gate(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    manifest_path = lane_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["active_assists"] = ["legacy_followstop"]
    manifest.pop("assist_ledger", None)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["scenario_id"] == "lane_keep_097")

    assert lane_run["verdict"] == "assisted_pass"
    assert lane_run["failure_reason"] == "assisted_pass_unassisted_claim_blocked"
    assert lane_run["blocking_assists"] == ["legacy_followstop"]
    assert lane_run["can_claim_unassisted_natural_driving"] is False


def test_force_green_traffic_light_scenario_cannot_pass_claim_grade(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    red_dir = suite_root / "traffic_light_red_stop"
    manifest_path = red_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_control"]["stimulus_mode"] = "force_green"
    manifest["traffic_light_control"]["mode"] = "force_green"
    manifest["traffic_light_expectation"]["stimulus_mode"] = "force_green"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] in {
        "traffic_light_control_missing",
        "traffic_light_stimulus_not_claim_grade",
        "traffic_light_control_not_deterministic",
        "unassisted_claim_not_supported",
    }
    assert red_run["can_claim_unassisted_natural_driving"] is False
    assert "force_green_traffic_light_active" in red_run["why_not_claimable"]


def test_planned_suite_requires_full_target_capability_coverage(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    with (suite_root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_id", "scenario_class", "run_dir"])
        writer.writeheader()
        for run_dir in sorted(path for path in suite_root.iterdir() if path.is_dir()):
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            writer.writerow(
                {
                    "run_id": manifest["run_id"],
                    "scenario_class": manifest["scenario_class"],
                    "run_dir": str(run_dir),
                }
            )

    report = analyze_natural_driving_suite(suite_root)

    assert report["verdict"]["status"] == "insufficient_data"
    assert report["verdict"]["require_full_target_coverage"] is True
    assert report["verdict"]["can_claim_full_natural_driving"] is False
    assert report["verdict"]["missing_required_scenario_classes"] == [
        "curve_diagnostic",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    ]


def test_warn_suite_keeps_scenario_class_unproven_for_full_natural_driving() -> None:
    run_results = [
        {
            "run_id": scenario_class,
            "scenario_class": scenario_class,
            "verdict": "warn" if scenario_class == "traffic_light_red_stop" else "pass",
        }
        for scenario_class in (
            "lane_keep",
            "curve_diagnostic",
            "junction_turn",
            "traffic_light_red_stop",
            "traffic_light_green_go",
            "traffic_light_red_to_green_release",
        )
    ]
    coverage = _capability_coverage(run_results)

    verdict = _suite_verdict(
        run_results,
        capability_coverage=coverage,
        require_full_target_coverage=True,
    )

    assert coverage["can_claim_full_natural_driving"] is False
    assert coverage["unproven_required_scenario_classes"] == ["traffic_light_red_stop"]
    assert verdict["status"] == "insufficient_data"
    assert verdict["can_claim_full_natural_driving"] is False
    assert verdict["unproven_required_scenario_classes"] == ["traffic_light_red_stop"]


def test_filtered_suite_plan_cannot_claim_full_natural_driving() -> None:
    run_results = [
        {
            "run_id": scenario_class,
            "scenario_class": scenario_class,
            "verdict": "pass",
        }
        for scenario_class in (
            "lane_keep",
            "curve_diagnostic",
            "junction_turn",
            "traffic_light_red_stop",
            "traffic_light_green_go",
            "traffic_light_red_to_green_release",
        )
    ]
    coverage = _capability_coverage(run_results)

    verdict = _suite_verdict(
        run_results,
        capability_coverage=coverage,
        suite_plan_scope={
            "filtered": True,
            "scenario_class_filter": ["lane_keep"],
            "scenario_id_filter": [],
        },
        require_full_target_coverage=True,
    )

    assert coverage["can_claim_full_natural_driving"] is True
    assert verdict["status"] == "insufficient_data"
    assert verdict["can_claim_full_natural_driving"] is False
    assert verdict["filtered_suite_plan"] is True
    assert verdict["scenario_class_filter"] == ["lane_keep"]


def test_full_coverage_requires_suite_manifest_and_run_matrix() -> None:
    run_results = [
        {
            "run_id": scenario_class,
            "scenario_class": scenario_class,
            "verdict": "pass",
        }
        for scenario_class in (
            "lane_keep",
            "curve_diagnostic",
            "junction_turn",
            "traffic_light_red_stop",
            "traffic_light_green_go",
            "traffic_light_red_to_green_release",
        )
    ]
    coverage = _capability_coverage(run_results)

    verdict = _suite_verdict(
        run_results,
        capability_coverage=coverage,
        suite_plan_scope={
            "has_suite_manifest": False,
            "has_run_matrix": False,
            "filtered": False,
            "scenario_class_filter": [],
            "scenario_id_filter": [],
        },
        require_full_target_coverage=True,
    )

    assert coverage["can_claim_full_natural_driving"] is True
    assert verdict["status"] == "insufficient_data"
    assert verdict["can_claim_full_natural_driving"] is False
    assert verdict["suite_plan_missing"] == ["suite_manifest.json", "run_matrix.csv"]


def test_full_coverage_requires_planned_scenario_id_and_route_identity() -> None:
    run_results = [
        {
            "run_id": scenario_class,
            "scenario_id": f"ad_hoc_{scenario_class}",
            "route_id": f"ad_hoc_{scenario_class}",
            "scenario_class": scenario_class,
            "verdict": "pass",
        }
        for scenario_class in (
            "lane_keep",
            "curve_diagnostic",
            "junction_turn",
            "traffic_light_red_stop",
            "traffic_light_green_go",
            "traffic_light_red_to_green_release",
        )
    ]
    planned_scenarios = [
        {"scenario_id": "lane_keep_097", "route_id": "lane097", "scenario_class": "lane_keep"},
        {
            "scenario_id": "lane_keep_or_mild_curve_217",
            "route_id": "lane217",
            "scenario_class": "lane_keep",
        },
        {"scenario_id": "junction_turn_031", "route_id": "junction031", "scenario_class": "junction_turn"},
        {
            "scenario_id": "curve_diagnostic_217",
            "route_id": "curve217",
            "scenario_class": "curve_diagnostic",
        },
        {
            "scenario_id": "curve_diagnostic_213",
            "route_id": "curve213",
            "scenario_class": "curve_diagnostic",
        },
        {
            "scenario_id": "traffic_light_red_stop_129051_probe",
            "route_id": "town01_rh_spawn129_goal051",
            "scenario_class": "traffic_light_red_stop",
        },
        {
            "scenario_id": "traffic_light_green_go_219063_probe",
            "route_id": "town01_rh_spawn219_goal063",
            "scenario_class": "traffic_light_green_go",
        },
        {
            "scenario_id": "traffic_light_red_to_green_release_219063_probe",
            "route_id": "town01_rh_spawn219_goal063",
            "scenario_class": "traffic_light_red_to_green_release",
        },
    ]

    coverage = _capability_coverage(run_results, planned_scenarios=planned_scenarios)
    verdict = _suite_verdict(
        run_results,
        capability_coverage=coverage,
        require_full_target_coverage=True,
    )

    assert coverage["missing_required_scenario_classes"] == []
    assert coverage["unproven_required_scenario_classes"] == []
    assert "lane_keep_097" in coverage["missing_required_scenario_ids"]
    assert "curve_diagnostic_213" in coverage["missing_required_scenario_ids"]
    assert coverage["can_claim_full_natural_driving"] is False
    assert verdict["status"] == "insufficient_data"
    assert verdict["can_claim_full_natural_driving"] is False
    assert "traffic_light_red_to_green_release_219063_probe" in verdict["missing_required_scenario_ids"]


def test_planned_scenario_route_mismatch_blocks_full_coverage() -> None:
    run_results = [
        {
            "run_id": "lane_keep_097",
            "scenario_id": "lane_keep_097",
            "route_id": "curve217",
            "scenario_class": "lane_keep",
            "verdict": "pass",
        }
    ]

    coverage = _capability_coverage(
        run_results,
        planned_scenarios=[
            {"scenario_id": "lane_keep_097", "route_id": "lane097", "scenario_class": "lane_keep"}
        ],
    )

    assert coverage["missing_required_scenario_ids"] == []
    assert coverage["scenario_identity_mismatches"][0]["scenario_id"] == "lane_keep_097"
    assert coverage["scenario_identity_mismatches"][0]["expected_route_id"] == "lane097"
    assert coverage["scenario_identity_mismatches"][0]["observed_route_ids"] == ["curve217"]
    assert coverage["can_claim_full_natural_driving"] is False


def test_red_light_not_stopped_fails(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    summary_path = suite_root / "traffic_light_red_stop" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["stopped_at_red"] = False
    summary["metrics"]["red_stop_distance_m"] = -1.0
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["status"] = "fail"
    behavior["failure_reason"] = "red_light_not_stopped"
    behavior["metrics"]["stopped_at_red"] = False
    behavior["metrics"]["red_stop_distance_m"] = -1.0
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "fail"
    assert red_run["verdict"] == "fail"
    assert red_run["failure_reason"] == "red_light_not_stopped"


def test_red_stop_uses_behavior_report_stopped_evidence_when_summary_is_missing(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    summary_path = suite_root / "traffic_light_red_stop" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"].pop("stopped_at_red")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "pass"
    assert red_run["verdict"] == "pass"
    assert red_run["stopped_at_red"] is True


def test_red_stop_behavior_report_without_stopped_evidence_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    summary_path = suite_root / "traffic_light_red_stop" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"].pop("stopped_at_red")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["metrics"].pop("stopped_at_red")
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "missing_stopped_at_red"
    assert "stopped_at_red" in red_run["missing_fields"]


def test_missing_traffic_light_contract_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "traffic_light_red_stop" / "traffic_light_contract_report.json").unlink()

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "missing_required_artifacts"
    assert "traffic_light_contract_report.json" in red_run["missing_artifacts"]


def test_thin_traffic_light_contract_report_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    contract_path = suite_root / "traffic_light_red_stop" / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract.pop("source", None)
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_contract_missing_source_evidence"
    assert "traffic_light_contract.source" in red_run["missing_fields"]
    assert "traffic_light_contract.source" in red_run["invalid_report_source_fields"]


def test_traffic_light_contract_source_paths_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    contract_path = suite_root / "traffic_light_red_stop" / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["source"]["traffic_light_mapping_path"] = "missing_traffic_light_mapping.yaml"
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_contract_missing_source_artifacts"
    assert "traffic_light_contract.source.traffic_light_mapping_path" in red_run["missing_fields"]
    assert "traffic_light_contract.source.traffic_light_mapping_path" in red_run["invalid_report_source_fields"]


def test_traffic_light_contract_report_must_match_run_scenario_class(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    contract_path = suite_root / "traffic_light_red_stop" / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["scenario_class"] = "traffic_light_green_go"
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_contract_scenario_mismatch"
    assert "traffic_light_contract.scenario_class" in red_run["missing_fields"]


def test_traffic_light_contract_report_requires_scenario_context(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    contract_path = suite_root / "traffic_light_red_stop" / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract.pop("scenario_class", None)
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_contract_missing_scenario_context"
    assert "traffic_light_contract.scenario_class" in red_run["missing_fields"]


def test_missing_traffic_light_behavior_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json").unlink()

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "missing_required_artifacts"
    assert "traffic_light_behavior_report.json" in red_run["missing_artifacts"]


def test_thin_traffic_light_behavior_report_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior.pop("source", None)
    behavior.pop("missing_inputs", None)
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_missing_source_evidence"
    assert "traffic_light_behavior.source" in red_run["missing_fields"]


def test_traffic_light_behavior_missing_input_metadata_blocks_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["missing_inputs"] = ["events"]
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_missing_inputs"
    assert "traffic_light_behavior.missing_inputs.events" in red_run["missing_fields"]


def test_traffic_light_behavior_source_paths_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["source"]["events_path"] = "missing_events.jsonl"
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_missing_source_artifacts"
    assert "traffic_light_behavior.source.events_path" in red_run["missing_fields"]
    assert "traffic_light_behavior.source.events_path" in red_run["invalid_report_source_fields"]


def test_traffic_light_behavior_manifest_source_path_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["source"]["manifest_path"] = "missing_manifest.json"
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_missing_source_artifacts"
    assert "traffic_light_behavior.source.manifest_path" in red_run["missing_fields"]
    assert "traffic_light_behavior.source.manifest_path" in red_run["invalid_report_source_fields"]


def test_traffic_light_behavior_report_must_match_run_scenario_class(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["scenario_class"] = "traffic_light_green_go"
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_scenario_mismatch"
    assert "traffic_light_behavior.scenario_class" in red_run["missing_fields"]


def test_traffic_light_behavior_report_requires_scenario_context(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior.pop("scenario_class", None)
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_missing_scenario_context"
    assert "traffic_light_behavior.scenario_class" in red_run["missing_fields"]


def test_traffic_light_behavior_report_must_match_run_route_id(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["route_id"] = "traffic_light_green_go_tbd"
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_route_id_mismatch"
    assert "traffic_light_behavior.route_id" in red_run["missing_fields"]


def test_traffic_light_behavior_report_requires_route_id(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior.pop("route_id", None)
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_behavior_missing_route_id"
    assert "traffic_light_behavior.route_id" in red_run["missing_fields"]


def test_missing_traffic_light_expectation_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "traffic_light_red_stop" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("traffic_light_expectation", None)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_expectation_missing"
    assert "traffic_light_expectation.expected_behavior" in red_run["missing_fields"]


def test_traffic_light_expectation_mismatch_fails(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "traffic_light_red_stop" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_expectation"]["expected_behavior"] = "green_go"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "fail"
    assert red_run["verdict"] == "fail"
    assert red_run["failure_reason"] == "traffic_light_expectation_mismatch"


def test_traffic_light_actual_observed_stimulus_is_not_claim_grade(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "traffic_light_red_stop" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_expectation"]["stimulus_mode"] = "carla_actual_observed"
    manifest["traffic_light_expectation"]["claim_grade"] = False
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_stimulus_not_claim_grade"
    assert red_run["traffic_light_stimulus_mode"] == "carla_actual_observed"
    assert red_run["traffic_light_claim_grade"] is False
    assert "traffic_light_expectation.stimulus_mode" in red_run["missing_fields"]


def test_claim_grade_traffic_light_requires_actual_control_metadata(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "traffic_light_red_stop" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("traffic_light_control", None)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "traffic_light_control_missing"
    assert red_run["traffic_light_control_available"] is False
    assert "traffic_light_control" in red_run["missing_fields"]


def test_red_to_green_control_evidence_requires_release_event() -> None:
    run = {
        "traffic_light_expectation": {
            "expected_behavior": "red_to_green_release",
            "expected_initial_state": "RED",
            "expected_release_state": "GREEN",
            "stimulus_mode": "deterministic_gt_control",
            "claim_grade": True,
        },
        "traffic_light_control": {
            "stimulus_mode": "deterministic_gt_control",
            "initial_state": "RED",
            "release_state": "GREEN",
            "initial_affected_count": 1,
            "events": [{"phase": "initial", "state": "RED", "affected_count": 1}],
        },
    }

    status, reason, missing = _traffic_light_control_evidence_verdict(run)
    assert status == "insufficient_data"
    assert reason == "traffic_light_control_release_not_observed"
    assert "traffic_light_control.release_frame_id" in missing

    run["traffic_light_control"]["events"].append(
        {"phase": "release", "state": "GREEN", "affected_count": 1, "frame_id": 42}
    )
    assert _traffic_light_control_evidence_verdict(run) == ("pass", None, [])


def test_manifest_traffic_light_expectation_overrides_stale_behavior_echo(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    behavior_path = suite_root / "traffic_light_red_stop" / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["traffic_light_expectation"] = {
        "expected_behavior": "green_go",
        "expected_initial_state": "GREEN",
        "required_report_fields": ["green_pass_time_s"],
    }
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert red_run["traffic_light_expected_behavior"] == "red_stop"
    assert red_run["traffic_light_expectation_source"] == "manifest"
    assert red_run["verdict"] == "pass"


def test_missing_route_health_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert "route_health.json" in lane_run["missing_artifacts"]


def test_missing_failure_timeline_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "analysis" / "failure_timeline" / "failure_timeline_report.json").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_required_artifacts"
    assert "failure_timeline_report.json" in lane_run["missing_artifacts"]


def test_missing_route_start_alignment_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (
        suite_root
        / "lane_keep_097"
        / "analysis"
        / "route_start_alignment"
        / "route_start_alignment_report.json"
    ).unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_required_artifacts"
    assert "route_start_alignment_report.json" in lane_run["missing_artifacts"]


def test_invalid_failure_timeline_or_route_start_alignment_source_is_insufficient_data(
    tmp_path: Path,
) -> None:
    suite_root = copy_fixture(tmp_path)
    lane = suite_root / "lane_keep_097"
    failure_path = lane / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    alignment_path = lane / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    for path in (failure_path, alignment_path):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.pop("source", None)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "invalid_route_start_diagnostic_artifacts"
    assert "failure_timeline.source" in lane_run["missing_fields"]
    assert "route_start_alignment.source" in lane_run["missing_fields"]


def test_thin_route_health_report_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health.pop("route_geometry", None)
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_evidence"
    assert "route_health.route_geometry" in lane_run["missing_fields"]


def test_route_health_missing_inputs_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["missing_inputs"] = ["route"]
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_inputs"
    assert "route_health.missing_inputs.route" in lane_run["missing_fields"]


def test_route_health_source_evidence_is_required(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["source"] = "synthetic_fixture"
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_source_evidence"
    assert "route_health.source" in lane_run["missing_fields"]


def test_route_health_source_paths_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["source"]["timeseries_path"] = "missing_timeseries.csv"
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_source_artifacts"
    assert "route_health.source.timeseries_path" in lane_run["missing_fields"]
    assert "route_health.source.timeseries_path" in lane_run["invalid_report_source_fields"]


def test_route_health_manifest_source_path_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["source"]["manifest_path"] = "missing_manifest.json"
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_source_artifacts"
    assert "route_health.source.manifest_path" in lane_run["missing_fields"]
    assert "route_health.source.manifest_path" in lane_run["invalid_report_source_fields"]


def test_route_health_route_id_must_match_run_route_id(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["route_id"] = "curve217"
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_route_id_mismatch"
    assert "route_health.route_id" in lane_run["missing_fields"]


def test_route_health_route_id_is_required(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health.pop("route_id", None)
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_route_id"
    assert "route_health.route_id" in lane_run["missing_fields"]


def test_route_health_requires_run_metrics_and_guard_semantics(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["run_metrics"].pop("heading_error_p95_rad")
    route_health["control_semantics"]["guard_apply_counts"].pop("lateral_guard")
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "route_health_missing_evidence"
    assert "route_health.run_metrics.heading_error_p95_rad" in lane_run["missing_fields"]
    assert (
        "route_health.control_semantics.guard_apply_counts.lateral_guard"
        in lane_run["missing_fields"]
    )


def test_missing_control_health_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert "control_health_report.json" in lane_run["missing_artifacts"]


def test_thin_control_health_report_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control.pop("source", None)
    control.pop("missing_inputs", None)
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_source_evidence"
    assert "control_health.source" in lane_run["missing_fields"]


def test_control_health_missing_inputs_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["missing_inputs"] = ["timeseries"]
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_inputs"
    assert "control_health.missing_inputs.timeseries" in lane_run["missing_fields"]


def test_control_health_source_paths_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["source"]["timeseries_path"] = "missing_timeseries.csv"
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_source_artifacts"
    assert "control_health.source.timeseries_path" in lane_run["missing_fields"]
    assert "control_health.source.timeseries_path" in lane_run["invalid_report_source_fields"]


def test_control_health_manifest_source_path_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["source"]["manifest_path"] = "missing_manifest.json"
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_source_artifacts"
    assert "control_health.source.manifest_path" in lane_run["missing_fields"]
    assert "control_health.source.manifest_path" in lane_run["invalid_report_source_fields"]


def test_control_health_report_must_match_run_scenario_class(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["scenario_class"] = "traffic_light_red_stop"
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_scenario_mismatch"
    assert "control_health.scenario_class" in lane_run["missing_fields"]


def test_control_health_report_requires_scenario_context(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control.pop("scenario_class", None)
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_scenario_context"
    assert "control_health.scenario_class" in lane_run["missing_fields"]


def test_control_health_report_must_match_run_route_id(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["route_id"] = "curve217"
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_route_id_mismatch"
    assert "control_health.route_id" in lane_run["missing_fields"]


def test_control_health_report_requires_route_id(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control.pop("route_id", None)
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_route_id"
    assert "control_health.route_id" in lane_run["missing_fields"]


def test_control_health_requires_raw_mapped_applied_trace_evidence(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["raw_mapped_applied_control_available"] = False
    control["missing_fields"] = ["apollo_steer_raw"]
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "control_health_missing_control_trace"
    assert "control_health.missing_fields.apollo_steer_raw" in lane_run["missing_fields"]


def test_control_health_failure_propagates_to_natural_driving_verdict(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    control_health_path = suite_root / "lane_keep_097" / "analysis" / "control_health" / "control_health_report.json"
    control_health = json.loads(control_health_path.read_text(encoding="utf-8"))
    control_health["status"] = "fail"
    control_health["failure_reason"] = "actuation_mismatch"
    control_health_path.write_text(json.dumps(control_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "fail"
    assert lane_run["verdict"] == "fail"
    assert lane_run["failure_reason"] == "actuation_mismatch"


def test_missing_events_artifact_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "events.jsonl").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert "events.jsonl" in lane_run["missing_artifacts"]


def test_missing_resolved_config_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "config.resolved.yaml").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert "config.resolved.yaml" in lane_run["missing_artifacts"]


def test_missing_artifact_completeness_report_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (
        suite_root
        / "lane_keep_097"
        / "analysis"
        / "artifact_completeness"
        / "artifact_completeness_report.json"
    ).unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_required_artifacts"
    assert "artifact_completeness_report.json" in lane_run["missing_artifacts"]


def test_run_matrix_rows_without_summary_are_reported_as_insufficient_data(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    run_dir = suite_root / "lane_keep" / "lane_keep_097"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "lane_keep_097",
                "scenario_class": "lane_keep",
                "route_id": "lane097",
                "algorithm_variant_id": "apollo_10_0_carla_gt_town01_reference",
                "map": "Town01",
                "transport_mode": "ros2_gt",
                "backend": "apollo_cyberrt",
                "truth_input": True,
                "duration_s": 70,
                "fixed_delta_seconds": 0.05,
                "ticks": 1400,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    with (suite_root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_id", "scenario_class", "run_dir"])
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "lane_keep_097",
                "scenario_class": "lane_keep",
                "run_dir": str(run_dir),
            }
        )

    report = analyze_natural_driving_suite(suite_root)
    lane_run = report["run_results"][0]

    assert report["run_count"] == 1
    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["run_id"] == "lane_keep_097"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_required_artifacts"
    assert "summary.json" in lane_run["missing_artifacts"]


def test_missing_algorithm_variant_in_manifest_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("algorithm_variant_id")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_manifest_fields"
    assert "manifest.algorithm_variant_id" in lane_run["missing_fields"]


def test_missing_algorithm_variant_manifest_path_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("algorithm_variant_manifest_path")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_manifest_fields"
    assert "manifest.algorithm_variant_manifest_path" in lane_run["missing_fields"]


def test_truth_input_false_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["truth_input"] = False
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["truth_input"] is False
    assert lane_run["failure_reason"] == "missing_manifest_fields"
    assert "manifest.truth_input" in lane_run["missing_fields"]


def test_missing_truth_input_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("truth_input")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["truth_input"] is False
    assert lane_run["failure_reason"] == "missing_manifest_fields"
    assert "manifest.truth_input" in lane_run["missing_fields"]


def test_truth_input_input_mode_alias_is_accepted(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("truth_input")
    manifest["input_mode"] = "carla_gt"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "pass"
    assert lane_run["verdict"] == "pass"
    assert lane_run["truth_input"] is True
    assert "truth_input" not in lane_run["missing_manifest_fields"]


def test_algorithm_variant_manifest_path_must_match_variant_id(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["algorithm_variant_id"] = "apollo_upstream_10_0_reference"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "invalid_manifest_source_fields"
    assert "manifest.algorithm_variant_manifest_path.variant_id_mismatch" in lane_run["missing_fields"]
    assert "algorithm_variant_manifest_path.variant_id_mismatch" in lane_run["invalid_manifest_source_fields"]


def test_truth_input_natural_driving_rejects_upstream_algorithm_variant(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["algorithm_variant_id"] = "apollo_upstream_10_0_reference"
    manifest["algorithm_variant_manifest_path"] = "configs/algorithms/apollo_variant.upstream.example.yaml"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "invalid_manifest_source_fields"
    assert (
        "manifest.algorithm_variant_manifest_path.variant_type_not_truth_input_closed_loop"
        in lane_run["missing_fields"]
    )
    assert (
        "algorithm_variant_manifest_path.variant_type_not_truth_input_closed_loop"
        in lane_run["invalid_manifest_source_fields"]
    )


def test_runtime_contract_missing_or_not_aligned_blocks_evidence(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    summary_path = suite_root / "lane_keep_097" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["runtime_contract"]["status"] = "not_aligned"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "fail"
    assert lane_run["verdict"] == "fail"
    assert lane_run["failure_reason"] == "runtime_contract_not_aligned"
    assert "runtime_contract.status" in lane_run["missing_fields"]


def test_missing_link_health_fields_are_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    summary_path = suite_root / "lane_keep_097" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.pop("routing_materialized")
    summary.pop("control_handoff_status")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_link_health_fields"
    assert "link_health.routing_materialized" in lane_run["missing_fields"]


def test_thin_channel_health_report_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "lane_keep_097" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel.pop("source", None)
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "apollo_channel_health_missing_source_evidence"
    assert "apollo_channel_health.source" in lane_run["missing_fields"]


def test_channel_health_missing_inputs_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "lane_keep_097" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["missing_inputs"] = ["channel_stats"]
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "apollo_channel_health_missing_inputs"
    assert "apollo_channel_health.missing_inputs.channel_stats" in lane_run["missing_fields"]


def test_channel_health_source_paths_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "lane_keep_097" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["source"]["stats_path"] = "missing_channel_stats.json"
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "apollo_channel_health_missing_source_artifacts"
    assert "apollo_channel_health.source.stats_path" in lane_run["missing_fields"]
    assert "apollo_channel_health.source.stats_path" in lane_run["invalid_report_source_fields"]


def test_channel_health_report_must_match_run_scenario_class(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "traffic_light_red_stop" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["scenario_class"] = "lane_keep"
    channel["channel_results"]["traffic_light"]["required"] = False
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["run_id"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "apollo_channel_health_scenario_mismatch"
    assert "apollo_channel_health.scenario_class" in red_run["missing_fields"]


def test_channel_health_report_requires_scenario_context(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "lane_keep_097" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel.pop("scenario_class", None)
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "apollo_channel_health_missing_scenario_context"
    assert "apollo_channel_health.scenario_class" in lane_run["missing_fields"]


def test_summary_markdown_surfaces_manifest_and_report_source_problems(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["algorithm_variant_id"] = "apollo_upstream_10_0_reference"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    channel_path = suite_root / "junction_031" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["source"]["stats_path"] = "missing_channel_stats.json"
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    outputs = write_natural_driving_report(report, tmp_path / "out")
    md_text = Path(outputs["natural_driving_summary"]).read_text(encoding="utf-8")

    assert report["summary"]["invalid_manifest_source_fields_run_count"] == 1
    assert report["summary"]["invalid_report_source_fields_run_count"] == 1
    assert {item["run_id"] for item in report["problem_run_details"]} >= {
        "lane_keep_097",
        "junction_031",
    }
    assert any(
        "algorithm_variant_manifest_path.variant_id_mismatch"
        in item["invalid_manifest_source_fields"]
        for item in report["problem_run_details"]
    )
    assert any(
        "apollo_channel_health.source.stats_path" in item["invalid_report_source_fields"]
        for item in report["problem_run_details"]
    )
    assert "Evidence Source Problems" in md_text
    assert "invalid_manifest_source_fields_run_count: `1`" in md_text
    assert "invalid_report_source_fields_run_count: `1`" in md_text
    assert "algorithm_variant_manifest_path.variant_id_mismatch" in md_text
    assert "apollo_channel_health.source.stats_path" in md_text


def test_link_health_failures_are_not_behavior_evidence(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    summary_path = suite_root / "lane_keep_097" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["planning_materialized"] = False
    summary["control_handoff_status"] = "planning_ready_control_not_consuming"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "fail"
    assert lane_run["verdict"] == "fail"
    assert lane_run["failure_reason"] == "planning_missing"


def test_missing_route_health_summary_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health_summary.md").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert "route_health_summary.md" in lane_run["missing_artifacts"]


def test_missing_route_health_csv_outputs_are_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.csv").unlink()
    (suite_root / "lane_keep_097" / "analysis" / "route_health" / "curve_segments.csv").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert "route_health.csv" in lane_run["missing_artifacts"]
    assert "curve_segments.csv" in lane_run["missing_artifacts"]


def test_lane_invasion_near_route_start_gets_specific_failure_reason(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    summary_path = lane_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["lane_invasion_count"] = 1
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    failure_dir = lane_dir / "analysis" / "failure_timeline"
    failure_dir.mkdir(parents=True, exist_ok=True)
    (failure_dir / "failure_timeline_report.json").write_text(
        json.dumps(
            {
                "schema_version": "town01_failure_timeline.v1",
                "run_id": "lane_keep_097",
                "route_id": "lane097",
                "scenario_class": "lane_keep",
                "status": "pass",
                "route_start_gate": {
                    "route_s_at_anchor": -0.5,
                    "anchor_before_route_start": True,
                    "anchor_near_route_start": True,
                },
                "anchor_event": {"event_type": "lane_invasion"},
                "ordering_findings": ["safety_event_before_route_start"],
                "source": {
                    "manifest_path": "manifest.json",
                    "summary_path": "summary.json",
                    "events_path": "events.jsonl",
                    "timeseries_path": "timeseries.csv",
                    "route_health_path": "analysis/route_health/route_health.json",
                    "control_health_path": "analysis/control_health/control_health_report.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    alignment_dir = lane_dir / "analysis" / "route_start_alignment"
    alignment_dir.mkdir(parents=True, exist_ok=True)
    (alignment_dir / "route_start_alignment_report.json").write_text(
        json.dumps(
            {
                "schema_version": "route_start_alignment_report.v1",
                "run_id": "lane_keep_097",
                "route_id": "lane097",
                "scenario_class": "lane_keep",
                "status": "warn",
                "reason": "failure_before_route_start",
                "static_spawn_alignment": {"spawn_lateral_offset_m": -0.5},
                "initial_ego_alignment": {"rear_axle_offset_compatible": True},
                "failure_anchor_alignment": {"route_s": -0.5},
                "recommendation": {
                    "available": True,
                    "recommended_ego_offset_y_delta_m": 0.5,
                },
                "source": {
                    "manifest_path": "manifest.json",
                    "summary_path": "summary.json",
                    "timeseries_path": "timeseries.csv",
                    "failure_timeline_path": "analysis/failure_timeline/failure_timeline_report.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "fail"
    assert lane_run["failure_reason"] == "route_start_lane_invasion"
    assert lane_run["failure_before_route_start"] is True
    assert lane_run["recommended_ego_offset_y_delta_m"] == 0.5


def test_non_actionable_semantic_anchor_is_not_reported_as_route_start_failure(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    failure_dir = lane_dir / "analysis" / "failure_timeline"
    failure_dir.mkdir(parents=True, exist_ok=True)
    (failure_dir / "failure_timeline_report.json").write_text(
        json.dumps(
            {
                "schema_version": "town01_failure_timeline.v1",
                "run_id": "lane_keep_097",
                "route_id": "lane097",
                "scenario_class": "lane_keep",
                "status": "pass",
                "route_start_gate": {
                    "route_s_at_anchor": -0.5,
                    "anchor_before_route_start": True,
                    "anchor_near_route_start": True,
                },
                "anchor_event": {"event_type": "first_high_steer"},
                "ordering_findings": [],
                "source": {
                    "manifest_path": "manifest.json",
                    "summary_path": "summary.json",
                    "events_path": "events.jsonl",
                    "timeseries_path": "timeseries.csv",
                    "route_health_path": "analysis/route_health/route_health.json",
                    "control_health_path": "analysis/control_health/control_health_report.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert lane_run["failure_before_route_start"] is None
    assert lane_run["failure_near_route_start"] is None
    assert lane_run["failure_timeline_anchor_event"] == "first_high_steer"


def test_missing_control_trace_fields_are_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    timeseries_path = suite_root / "lane_keep_097" / "timeseries.csv"
    text = timeseries_path.read_text(encoding="utf-8")
    header, *rows = text.splitlines()
    keep_indexes = [
        index
        for index, name in enumerate(header.split(","))
        if name not in {"apollo_steer_raw", "bridge_steer_mapped", "carla_steer_applied"}
    ]
    new_lines = [
        ",".join(header.split(",")[index] for index in keep_indexes),
        *(",".join(row.split(",")[index] for index in keep_indexes) for row in rows),
    ]
    timeseries_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "missing_control_trace_fields"
    assert lane_run["control_trace_available"] is False
    assert {"apollo_steer_raw", "bridge_steer_mapped", "carla_steer_applied"}.issubset(
        set(lane_run["missing_control_trace_fields"])
    )


def test_channel_health_failure_fails(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "junction_031" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["status"] = "fail"
    channel["missing_required_channels"] = ["control"]
    channel.setdefault("channel_results", {}).setdefault("control", {})["status"] = "fail"
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    junction_run = next(run for run in report["run_results"] if run["scenario_class"] == "junction_turn")

    assert report["verdict"]["status"] == "fail"
    assert junction_run["verdict"] == "fail"
    assert junction_run["failure_reason"] == "apollo_channel_health_failed"
    assert junction_run["apollo_channel_health_status"] == "fail"
    assert "control" in junction_run["apollo_channel_missing_required_channels"]
    assert "control" in junction_run["apollo_channel_failed_channels"]


def test_channel_health_without_channel_results_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "lane_keep_097" / "apollo_channel_health_report.json"
    channel_path.write_text(
        json.dumps(
            {
                "schema_version": "apollo_channel_health_report.v1",
                "status": "pass",
                "scenario_class": "lane_keep",
                "missing_required_channels": [],
                "source": {
                    "config_path": "configs/algorithms/apollo_natural_driving_channels.yaml",
                    "stats_path": "tests/fixtures/apollo/channel_stats_natural_valid.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "insufficient_data"
    assert lane_run["verdict"] == "insufficient_data"
    assert lane_run["failure_reason"] == "apollo_channel_health_missing_channel_results"
    assert "apollo_channel_health.channel_results" in lane_run["missing_fields"]


def test_traffic_light_scenario_requires_traffic_light_channel_result(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    channel_path = suite_root / "traffic_light_red_stop" / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["channel_results"]["traffic_light"]["required"] = False
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "insufficient_data"
    assert red_run["verdict"] == "insufficient_data"
    assert red_run["failure_reason"] == "apollo_channel_health_missing_channel_evidence"
    assert "apollo_channel_health.channel_results.traffic_light.required" in red_run["missing_fields"]


def test_analysis_channel_health_overrides_stale_root_report(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    lane = suite_root / "lane_keep_097"
    analysis_dir = lane / "analysis" / "apollo_channel_health"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    (analysis_dir / "apollo_channel_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_channel_health_report.v1",
                "status": "fail",
                "missing_required_channels": ["control"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_natural_driving_suite(suite_root)
    lane_run = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert report["verdict"]["status"] == "fail"
    assert lane_run["artifacts"]["apollo_channel_health"].endswith(
        "analysis/apollo_channel_health/apollo_channel_health_report.json"
    )
    assert lane_run["verdict"] == "fail"
    assert lane_run["failure_reason"] == "apollo_channel_health_failed"


def test_analysis_traffic_light_contract_overrides_stale_root_report(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    analysis_dir = traffic / "analysis" / "traffic_light"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    (analysis_dir / "traffic_light_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "traffic_light_contract_report.v1",
                "status": "fail",
                "warnings": ["synthetic_analysis_override"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "fail"
    assert red_run["artifacts"]["traffic_light_contract"].endswith(
        "analysis/traffic_light/traffic_light_contract_report.json"
    )
    assert red_run["verdict"] == "fail"
    assert red_run["failure_reason"] == "traffic_light_contract_failed"


def test_analysis_traffic_light_behavior_overrides_stale_root_report(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    analysis_dir = traffic / "analysis" / "traffic_light"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    behavior_path = traffic / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["status"] = "fail"
    behavior["failure_reason"] = "red_light_not_stopped"
    (analysis_dir / "traffic_light_behavior_report.json").write_text(
        json.dumps(behavior, indent=2) + "\n",
        encoding="utf-8",
    )

    report = analyze_natural_driving_suite(suite_root)
    red_run = next(run for run in report["run_results"] if run["scenario_class"] == "traffic_light_red_stop")

    assert report["verdict"]["status"] == "fail"
    assert red_run["artifacts"]["traffic_light_behavior"].endswith(
        "analysis/traffic_light/traffic_light_behavior_report.json"
    )
    assert red_run["verdict"] == "fail"
    assert red_run["failure_reason"] == "red_light_not_stopped"


def _make_curve_run(suite_root: Path) -> Path:
    curve = suite_root / "curve_diagnostic_217"
    shutil.copytree(suite_root / "lane_keep_097", curve)
    summary_path = curve / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "run_id": "curve_diagnostic_217",
            "scenario_id": "curve_diagnostic_217",
            "scenario_class": "curve_diagnostic",
            "route_id": "curve217",
        }
    )
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    route_health_path = curve / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["route_id"] = "curve217"
    route_health["route_geometry"]["curve_segments_count"] = 1
    route_health["route_geometry"]["curve_segments"] = [
        {
            "curve_segment_id": 0,
            "start_index": 1,
            "end_index": 3,
            "length_m": 25.0,
            "direction": "left",
        }
    ]
    route_health["apollo_semantics"] = {
        "matched_point_anomaly_locations": [],
        "target_point_anomaly_locations": [],
        "first_high_steer": None,
    }
    route_health["missing_fields"] = []
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")
    channel_path = curve / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["scenario_class"] = "curve_diagnostic"
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")
    control_path = curve / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["scenario_class"] = "curve_diagnostic"
    control["route_id"] = "curve217"
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")
    failure_path = curve / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    failure["run_id"] = "curve_diagnostic_217"
    failure["route_id"] = "curve217"
    failure["scenario_class"] = "curve_diagnostic"
    failure["source"] = {
        "manifest_path": "manifest.json",
        "summary_path": "summary.json",
        "events_path": "events.jsonl",
        "timeseries_path": "timeseries.csv",
        "route_health_path": "analysis/route_health/route_health.json",
        "control_health_path": "analysis/control_health/control_health_report.json",
    }
    failure_path.write_text(json.dumps(failure, indent=2) + "\n", encoding="utf-8")
    alignment_path = curve / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    alignment = json.loads(alignment_path.read_text(encoding="utf-8"))
    alignment["run_id"] = "curve_diagnostic_217"
    alignment["route_id"] = "curve217"
    alignment["scenario_class"] = "curve_diagnostic"
    alignment["source"] = {
        "manifest_path": "manifest.json",
        "summary_path": "summary.json",
        "timeseries_path": "timeseries.csv",
        "failure_timeline_path": "analysis/failure_timeline/failure_timeline_report.json",
    }
    alignment_path.write_text(json.dumps(alignment, indent=2) + "\n", encoding="utf-8")
    gap_dir = curve / "analysis" / "route_curve_artifact_gap"
    gap_dir.mkdir(parents=True, exist_ok=True)
    (gap_dir / "route_curve_artifact_gap_report.json").write_text(
        json.dumps(
            {
                "schema_version": "route_curve_artifact_gap.v1",
                "status": "pass",
                "failure_reason": None,
                "source": {
                    "timeseries_csv": "timeseries.csv",
                    "summary_json": "summary.json",
                },
                "per_frame_p1_complete": True,
                "missing_p1_fields": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return curve


def test_curve_diagnostic_requires_matched_target_semantic_fields(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    route_health_path = curve / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["missing_fields"] = ["matched_point", "target_point", "apollo_raw_steer"]
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "insufficient_data"
    assert curve_run["verdict"] == "insufficient_data"
    assert curve_run["failure_reason"] == "curve_semantics_missing_fields"
    assert {"matched_point", "target_point", "apollo_raw_steer"}.issubset(set(curve_run["missing_fields"]))


def test_curve_diagnostic_thin_route_curve_artifact_gap_cannot_pass(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    gap_path = curve / "analysis" / "route_curve_artifact_gap" / "route_curve_artifact_gap_report.json"
    gap = json.loads(gap_path.read_text(encoding="utf-8"))
    gap.pop("source", None)
    gap_path.write_text(json.dumps(gap, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "insufficient_data"
    assert curve_run["verdict"] == "insufficient_data"
    assert curve_run["failure_reason"] == "route_curve_artifact_gap_missing_source_evidence"
    assert "route_curve_artifact_gap.source" in curve_run["missing_fields"]
    assert "route_curve_artifact_gap.source" in curve_run["invalid_report_source_fields"]


def test_curve_diagnostic_route_curve_artifact_gap_sources_must_exist(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    gap_path = curve / "analysis" / "route_curve_artifact_gap" / "route_curve_artifact_gap_report.json"
    gap = json.loads(gap_path.read_text(encoding="utf-8"))
    gap["source"]["timeseries_csv"] = "missing_timeseries.csv"
    gap_path.write_text(json.dumps(gap, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "insufficient_data"
    assert curve_run["verdict"] == "insufficient_data"
    assert curve_run["failure_reason"] == "route_curve_artifact_gap_missing_source_artifacts"
    assert "route_curve_artifact_gap.source.timeseries_csv" in curve_run["missing_fields"]
    assert "route_curve_artifact_gap.source.timeseries_csv" in curve_run["invalid_report_source_fields"]


def test_curve_diagnostic_requires_apollo_semantics_object(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    route_health_path = curve / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health.pop("apollo_semantics")
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "insufficient_data"
    assert curve_run["verdict"] == "insufficient_data"
    assert curve_run["failure_reason"] == "curve_semantics_missing_fields"
    assert "apollo_semantics" in curve_run["missing_fields"]


def test_curve_diagnostic_requires_apollo_semantics_keys(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    route_health_path = curve / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["apollo_semantics"].pop("target_point_anomaly_locations")
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "insufficient_data"
    assert curve_run["verdict"] == "insufficient_data"
    assert curve_run["failure_reason"] == "curve_semantics_missing_fields"
    assert "apollo_semantics.target_point_anomaly_locations" in curve_run["missing_fields"]


def test_curve_diagnostic_matched_point_anomaly_fails(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    route_health_path = curve / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["apollo_semantics"]["matched_point_anomaly_locations"] = [12]
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "fail"
    assert curve_run["verdict"] == "fail"
    assert curve_run["failure_reason"] == "matched_point_anomaly"


def test_curve_diagnostic_first_high_steer_fails(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    curve = _make_curve_run(suite_root)
    route_health_path = curve / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["apollo_semantics"]["first_high_steer"] = {"seq": 16, "value": 0.98}
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = analyze_natural_driving_suite(suite_root)
    curve_run = next(run for run in report["run_results"] if run["run_id"] == "curve_diagnostic_217")

    assert report["verdict"]["status"] == "fail"
    assert curve_run["verdict"] == "fail"
    assert curve_run["failure_reason"] == "first_high_steer"


def test_cli_generates_json_csv_and_markdown(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    out_dir = tmp_path / "cli_out"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = json.loads(result.stdout)
    report = json.loads((out_dir / "natural_driving_report.json").read_text(encoding="utf-8"))
    csv_text = (out_dir / "natural_driving_report.csv").read_text(encoding="utf-8")
    md_text = (out_dir / "natural_driving_summary.md").read_text(encoding="utf-8")

    assert stdout["status"] == "pass"
    assert report["verdict"]["status"] == "pass"
    assert "route_completion" in csv_text.splitlines()[0]
    assert "apollo_channel_failed_channels" in csv_text.splitlines()[0]
    assert "control_trace_available" in csv_text.splitlines()[0]
    assert "traffic_light_expected_behavior" in csv_text.splitlines()[0]
    assert "candidate_positive" in md_text


def test_cli_full_target_coverage_gate_blocks_subset_reports(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    out_dir = tmp_path / "cli_full_coverage_out"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
            "--require-full-target-coverage",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    stdout = json.loads(result.stdout)
    assert result.returncode == 2
    assert stdout["status"] == "insufficient_data"
    assert stdout["coverage_check"]["required"] is True
    assert stdout["coverage_check"]["passed"] is False
    assert stdout["coverage_check"]["suite_plan_missing"] == ["suite_manifest.json", "run_matrix.csv"]
    assert "curve_diagnostic" in stdout["coverage_check"]["missing_required_scenario_classes"]
    assert "traffic_light_green_go" in stdout["coverage_check"]["missing_required_scenario_classes"]
    assert "missing_required_scenario_ids" in stdout["coverage_check"]
    assert "unproven_required_scenario_ids" in stdout["coverage_check"]
    assert "scenario_identity_mismatches" in stdout["coverage_check"]


def test_cli_can_fail_on_insufficient_data_status(tmp_path: Path) -> None:
    suite_root = copy_fixture(tmp_path)
    (suite_root / "lane_keep_097" / "events.jsonl").unlink()
    out_dir = tmp_path / "cli_strict_out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
            "--fail-on-status",
            "fail,warn,insufficient_data",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    stdout = json.loads(result.stdout)
    assert result.returncode == 1
    assert stdout["status"] == "insufficient_data"
    assert stdout["problem_run_count"] == 1
    assert "lane_keep_097" in stdout["verdict"]["insufficient_data_runs"]
    problem = stdout["problem_runs"][0]
    assert problem["run_id"] == "lane_keep_097"
    assert problem["failure_reason"] == "missing_required_artifacts"
    assert "events.jsonl" in problem["missing_artifacts"]
