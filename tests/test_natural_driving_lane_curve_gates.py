from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from carla_testbed.analysis.natural_driving import analyze_natural_driving_suite

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def test_lane_keep_full_evidence_passes(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "pass"
    assert lane["route_hard_gate_eligible"] is True
    assert lane["apollo_channel_health_status"] == "pass"
    assert lane["control_health_status"] == "pass"
    assert lane["control_attribution_status"] == "insufficient_data"


def test_lane_keep_reconstructed_route_is_insufficient_data(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    route_health_path = suite_root / "lane_keep_097" / "analysis" / "route_health" / "route_health.json"
    route_health = _read_json(route_health_path)
    route_health["route_source"] = "reconstructed_from_timeseries"
    route_health["evidence_level"] = "diagnostic_only"
    route_health["hard_gate_eligible"] = False
    route_health["route_evidence_reason"] = "reconstructed_from_timeseries_cannot_support_hard_gate"
    route_health["source"]["route_path"] = None
    route_health["warnings"] = ["route reconstructed from timeseries P0 route_curve fields"]
    _write_json(route_health_path, route_health)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "route_health_not_hard_gate_eligible"
    assert lane["route_source"] == "reconstructed_from_timeseries"


def test_lane_keep_blocking_assist_is_assisted_pass(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    manifest_path = suite_root / "lane_keep_097" / "manifest.json"
    manifest = _read_json(manifest_path)
    manifest["active_assists"] = ["straight_lane_lateral_stabilizer"]
    _write_json(manifest_path, manifest)

    report = analyze_natural_driving_suite(suite_root)
    lane = _run(report, "lane_keep_097")

    assert lane["verdict"] == "assisted_pass"
    assert lane["failure_reason"] == "assisted_pass_unassisted_claim_blocked"
    assert lane["blocking_assists"] == ["straight_lane_lateral_stabilizer"]
    assert lane["can_claim_unassisted_natural_driving"] is False


def test_curve_missing_matched_target_is_diagnostic_only(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _make_curve_run(suite_root, missing_p1=True)

    report = analyze_natural_driving_suite(suite_root)
    curve = _run(report, "curve217_diagnostic")

    assert curve["verdict"] == "diagnostic_only"
    assert curve["failure_reason"] == "curve_diagnostic_missing_p1_fields"
    assert "route_curve_artifact_gap.apollo_matched_point_distance" in curve["missing_fields"]
    assert "route_curve_artifact_gap.apollo_target_point_distance" in curve["missing_fields"]


def test_curve_lateral_semantics_report_is_echoed_without_root_cause_claim(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    _make_curve_run(suite_root, missing_p1=False, lateral_semantics=True)

    report = analyze_natural_driving_suite(suite_root)
    curve = _run(report, "curve217_diagnostic")

    assert curve["verdict"] == "diagnostic_only"
    assert curve["failure_reason"] == "route_health_diagnostic_only_not_claim_grade"
    assert curve["apollo_lateral_suspected_layer"] == "target_point_semantics"
    assert curve["apollo_lateral_confidence"] == "medium"


def _copy_suite(tmp_path: Path) -> Path:
    target = tmp_path / "simple_suite"
    shutil.copytree(FIXTURE_ROOT, target)
    return target


def _make_curve_run(
    suite_root: Path,
    *,
    missing_p1: bool,
    lateral_semantics: bool = False,
) -> Path:
    source = FIXTURE_ROOT / "lane_keep_097"
    run_dir = suite_root / "curve217_diagnostic"
    shutil.copytree(source, run_dir)
    replacements = {
        "run_id": "curve217_diagnostic",
        "scenario_id": "curve217_diagnostic",
        "scenario_class": "curve_diagnostic",
        "route_id": "curve217",
    }
    _patch_json(run_dir / "manifest.json", replacements)
    _patch_json(run_dir / "summary.json", replacements)
    for path in [
        run_dir / "apollo_channel_health_report.json",
        run_dir / "analysis" / "control_health" / "control_health_report.json",
        run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json",
        run_dir / "analysis" / "route_start_alignment" / "route_start_alignment_report.json",
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
            "warnings": ["route reconstructed from timeseries P0 route_curve fields"],
        }
    )
    route_health["source"]["route_path"] = None
    route_health["apollo_semantics"] = {
        "matched_point_anomaly_locations": [],
        "target_point_anomaly_locations": [],
        "first_high_steer": None,
    }
    if not missing_p1:
        route_health["missing_fields"] = []
    _write_json(route_health_path, route_health)
    gap_dir = run_dir / "analysis" / "route_curve_artifact_gap"
    gap_dir.mkdir(parents=True, exist_ok=True)
    gap = {
        "schema_version": "route_curve_artifact_gap.v1",
        "status": "insufficient_data" if missing_p1 else "pass",
        "missing_p1_fields": [
            "apollo_matched_point_distance",
            "apollo_target_point_distance",
        ]
        if missing_p1
        else [],
        "source": {
            "timeseries_csv": "timeseries.csv",
            "summary_json": "summary.json",
        },
        "missing_inputs": [],
    }
    _write_json(gap_dir / "route_curve_artifact_gap_report.json", gap)
    if lateral_semantics:
        lateral_dir = run_dir / "analysis" / "apollo_lateral_semantics"
        lateral_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            lateral_dir / "apollo_lateral_semantics_report.json",
            {
                "schema_version": "apollo_lateral_semantics.v1",
                "status": "warn",
                "suspected_layer": "target_point_semantics",
                "confidence": "medium",
                "verdict": {"status": "warn", "reason": "target_kappa_spike"},
            },
        )
    return run_dir


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
