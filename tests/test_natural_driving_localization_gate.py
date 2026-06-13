from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from carla_testbed.analysis.natural_driving import analyze_natural_driving_suite
from carla_testbed.analysis.route_health_report import analyze_route_health_run_dir

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def test_lane_keep_with_localization_pass_can_pass(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "pass"
    assert lane["localization_contract_status"] == "pass"
    assert lane["localization_blocking_reasons"] == []
    assert lane["localization_report_path"]


def test_lane_keep_missing_localization_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    _localization_path(suite_root / "lane_keep_097").unlink()

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "missing_required_artifacts"
    assert lane["localization_contract_status"] == "insufficient_data"
    assert lane["localization_report_path"] is None
    assert "localization_contract_report.json" in lane["missing_artifacts"]


def test_lane_keep_assumed_vrp_warning_cannot_hard_pass(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    loc_path = _localization_path(suite_root / "lane_keep_097")
    loc = _read_json(loc_path)
    loc["verdict"]["status"] = "warn"
    loc["warnings"] = ["vehicle_reference_confidence_assumed"]
    loc["reference_point"]["configured_vehicle_reference_confidence"] = "assumed"
    loc["reference_point"]["confidence"] = "assumed"
    loc["acceptance_checklist"]["position_uses_vrp"]["status"] = "warn"
    loc["acceptance_checklist"]["position_uses_vrp"]["reason"] = "position_uses_vrp=true; confidence=assumed"
    _write_json(loc_path, loc)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "localization_contract_not_claim_grade"
    assert "localization_contract.acceptance_checklist.position_uses_vrp.status" in lane["missing_fields"]


def test_lane_keep_missing_measurement_time_cannot_hard_pass(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    loc_path = _localization_path(suite_root / "lane_keep_097")
    loc = _read_json(loc_path)
    loc["verdict"]["status"] = "warn"
    loc["warnings"] = ["measurement_time_field_missing"]
    loc["status"]["measurement_time_available"] = False
    loc["time"]["measurement_time_source"] = None
    loc["acceptance_checklist"]["sim_time_time_base"]["status"] = "warn"
    loc["acceptance_checklist"]["sim_time_time_base"]["reason"] = "measurement_time_available=False"
    _write_json(loc_path, loc)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "localization_contract_not_claim_grade"
    assert "localization_contract.acceptance_checklist.sim_time_time_base.status" in lane["missing_fields"]


def test_lane_keep_stale_republish_blocks_unassisted_claim(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    loc_path = _localization_path(suite_root / "lane_keep_097")
    loc = _read_json(loc_path)
    loc["verdict"]["status"] = "fail"
    loc["verdict"]["blocking_reasons"] = [
        "duplicate_localization_timestamps_claim_grade_blocked"
    ]
    loc["channel"]["duplicate_timestamp_ratio"] = 0.25
    loc["channel"]["stale_republish_detected"] = True
    loc["channel"]["stale_republish_status"] = "claim_grade_blocked"
    _write_json(loc_path, loc)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "localization_contract_blocking"
    assert "duplicate_localization_timestamps_claim_grade_blocked" in lane["localization_blocking_reasons"]
    assert "stale_gt_republish_claim_blocked" in lane["why_not_claimable"]


def test_lane_keep_hdmap_lateral_matching_route_drift_gets_specific_failure_reason(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    loc_path = _localization_path(suite_root / "lane_keep_097")
    loc = _read_json(loc_path)
    loc["verdict"]["status"] = "fail"
    loc["verdict"]["blocking_reasons"] = ["apollo_hdmap_projection_lateral_error_high"]
    loc["hdmap_route_lateral_consistency"] = {
        "status": "pass",
        "alignment_mode": "negated_projection_l_matches_cross_track",
        "best_abs_delta_p95_m": 0.015,
        "interpretation": "hdmap_lateral_matches_route_cross_track_actual_lateral_drift",
    }
    _write_json(loc_path, loc)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "actual_lateral_drift_matches_hdmap_projection"
    assert lane["can_claim_unassisted_natural_driving"] is False
    assert "apollo_hdmap_projection_lateral_error_high" in lane["localization_blocking_reasons"]


def test_curve_with_localization_heading_block_is_diagnostic_not_algorithm_fail(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _make_curve_run(suite_root)
    loc_path = _localization_path(suite_root / "curve217_diagnostic")
    loc = _read_json(loc_path)
    loc["pose_consistency"]["heading_error_to_lane_p95_rad"] = 0.8
    _write_json(loc_path, loc)

    report = analyze_natural_driving_suite(suite_root)
    curve = _run(report, "curve217_diagnostic")

    assert curve["verdict"] == "diagnostic_only"
    assert curve["failure_reason"] == "localization_contract_blocking_curve_diagnostic"
    assert curve["localization_contract_status"] == "pass"
    assert "heading_error_to_lane_high" in curve["localization_blocking_reasons"]
    assert "localization_contract.blocking_reasons.heading_error_to_lane_high" in curve["missing_fields"]


def test_traffic_light_missing_localization_cannot_pass_behavior_claim(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    _localization_path(suite_root / "traffic_light_red_stop").unlink()

    report = analyze_natural_driving_suite(suite_root)
    red = _run(report, "traffic_light_red_stop")

    assert red["verdict"] == "insufficient_data"
    assert red["failure_reason"] == "missing_required_artifacts"
    assert red["localization_contract_status"] == "insufficient_data"
    assert "localization_contract_report.json" in red["missing_artifacts"]


def test_missing_localization_report_does_not_crash(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    for path in suite_root.glob("*/analysis/localization_contract/localization_contract_report.json"):
        path.unlink()

    report = analyze_natural_driving_suite(suite_root)

    assert report["verdict"]["status"] == "insufficient_data"
    assert all(run["localization_contract_status"] == "insufficient_data" for run in report["run_results"])


def test_route_health_summary_surfaces_localization_contract(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    out_dir = tmp_path / "route_health_out"

    result = analyze_route_health_run_dir(suite_root / "lane_keep_097", out_dir=out_dir)
    summary = Path(result["outputs"]["route_health_summary_md"]).read_text(encoding="utf-8")

    assert "localization_contract_status: `pass`" in summary
    assert "localization warnings: `none`" in summary


def test_route_health_summary_marks_missing_localization_not_evaluated(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    _localization_path(suite_root / "lane_keep_097").unlink()
    out_dir = tmp_path / "route_health_out"

    result = analyze_route_health_run_dir(suite_root / "lane_keep_097", out_dir=out_dir)
    summary = Path(result["outputs"]["route_health_summary_md"]).read_text(encoding="utf-8")

    assert "localization_contract_status: `not evaluated`" in summary


def _copy_suite(tmp_path: Path) -> Path:
    target = tmp_path / "simple_suite"
    shutil.copytree(FIXTURE_ROOT, target)
    return target


def _make_curve_run(suite_root: Path) -> Path:
    source = FIXTURE_ROOT / "lane_keep_097"
    run_dir = suite_root / "curve217_diagnostic"
    shutil.copytree(source, run_dir)
    replacements = {
        "run_id": "curve217_diagnostic",
        "scenario_id": "curve217_diagnostic",
        "scenario_class": "curve_diagnostic",
        "route_id": "curve217",
    }
    for path in [
        run_dir / "manifest.json",
        run_dir / "summary.json",
        run_dir / "apollo_channel_health_report.json",
        run_dir / "analysis" / "control_health" / "control_health_report.json",
        run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json",
        run_dir / "analysis" / "route_start_alignment" / "route_start_alignment_report.json",
        run_dir / "analysis" / "localization_contract" / "localization_contract_report.json",
    ]:
        _patch_json(path, replacements)
    route_health_path = run_dir / "analysis" / "route_health" / "route_health.json"
    route_health = _read_json(route_health_path)
    route_health.update(
        {
            "route_id": "curve217",
            "route_source": "reconstructed_from_timeseries",
            "evidence_level": "diagnostic_only",
            "hard_gate_eligible": False,
            "route_evidence_reason": "reconstructed_from_timeseries_cannot_support_hard_gate",
        }
    )
    route_health["source"]["route_path"] = None
    _write_json(route_health_path, route_health)
    gap_dir = run_dir / "analysis" / "route_curve_artifact_gap"
    gap_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        gap_dir / "route_curve_artifact_gap_report.json",
        {
            "schema_version": "route_curve_artifact_gap.v1",
            "status": "pass",
            "missing_p1_fields": [],
            "source": {
                "timeseries_csv": "timeseries.csv",
                "summary_json": "summary.json",
            },
            "missing_inputs": [],
        },
    )
    return run_dir


def _localization_path(run_dir: Path) -> Path:
    return run_dir / "analysis" / "localization_contract" / "localization_contract_report.json"


def _patch_json(path: Path, replacements: dict[str, Any]) -> None:
    payload = _read_json(path)
    payload.update(replacements)
    _write_json(path, payload)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run(report: dict[str, Any], run_id: str) -> dict[str, Any]:
    return next(run for run in report["run_results"] if run["run_id"] == run_id)
