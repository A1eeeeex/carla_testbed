from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_reference_line_contract import (
    REPORT_SCHEMA_VERSION,
    analyze_apollo_reference_line_contract_run_dir,
    ensure_apollo_reference_line_contract_report,
    write_apollo_reference_line_contract_report,
)
from carla_testbed.analysis.natural_driving import analyze_natural_driving_suite

FIXTURE_ROOT = Path("tests/fixtures/apollo_reference_line_contract")
NATURAL_FIXTURE = Path("tests/fixtures/natural_driving/simple_suite")
SCRIPT = Path("tools/analyze_apollo_reference_line_contract.py")


def test_reference_line_contract_pass_fixture(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path, "pass")

    report = analyze_apollo_reference_line_contract_run_dir(run_dir)
    outputs = write_apollo_reference_line_contract_report(report, tmp_path / "out")

    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["blocking_reasons"] == []
    assert report["evidence"]["planning_reference_available"] is True
    assert report["evidence"]["control_reference_available"] is True
    assert report["metrics"]["planning_ref_heading_error_p95_rad"] < 0.05
    assert report["metrics"]["control_ref_heading_error_p95_rad"] < 0.05
    assert Path(outputs["apollo_reference_line_contract_report"]).is_file()
    assert Path(outputs["apollo_reference_line_contract_summary"]).is_file()


def test_reference_line_heading_mismatch_fails() -> None:
    report = analyze_apollo_reference_line_contract_run_dir(FIXTURE_ROOT / "lane_heading_mismatch")

    assert report["status"] == "fail"
    assert "reference_line_heading_error_high" in report["blocking_reasons"]
    assert report["metrics"]["control_ref_heading_error_p95_rad"] >= 0.20
    assert report["metrics"]["planning_ref_heading_error_p95_rad"] < 0.05


def test_reference_line_missing_is_insufficient_data() -> None:
    report = analyze_apollo_reference_line_contract_run_dir(FIXTURE_ROOT / "reference_line_missing")

    assert report["status"] == "insufficient_data"
    assert "planning_control_reference_evidence_missing" in report["warnings"]
    assert report["evidence"]["planning_reference_available"] is False
    assert report["evidence"]["control_reference_available"] is False


def test_apollo_hdmap_projection_pass_lifts_claim_grade_evidence(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path, "pass")
    _write_hdmap_projection(
        run_dir / "artifacts" / "apollo_hdmap_projection.jsonl",
        heading_error_rad=0.01,
        lateral_error_m=0.05,
    )

    report = analyze_apollo_reference_line_contract_run_dir(run_dir)

    assert report["status"] == "pass"
    assert report["evidence"]["apollo_hdmap_projection_available"] is True
    assert report["evidence"]["apollo_hdmap_projection_claim_grade"] is True
    assert report["apollo_hdmap_projection"]["status"] == "pass"
    assert report["apollo_hdmap_projection"]["claim_grade"] is True


def test_apollo_hdmap_projection_missing_is_explicit_insufficient_data() -> None:
    report = analyze_apollo_reference_line_contract_run_dir(FIXTURE_ROOT / "pass")

    assert report["status"] == "pass"
    assert report["evidence"]["apollo_hdmap_projection_available"] is False
    assert report["evidence"]["apollo_hdmap_projection_claim_grade"] is False
    assert report["apollo_hdmap_projection"]["status"] == "insufficient_data"


def test_apollo_hdmap_projection_high_heading_error_fails(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path, "pass")
    _write_hdmap_projection(
        run_dir / "artifacts" / "apollo_hdmap_projection.jsonl",
        heading_error_rad=0.35,
        lateral_error_m=0.05,
    )

    report = analyze_apollo_reference_line_contract_run_dir(run_dir)

    assert report["status"] == "fail"
    assert "apollo_hdmap_projection_heading_error_high" in report["blocking_reasons"]
    assert "map_alignment" in report["apollo_hdmap_projection"]["suspected_failure_layers"]


def test_control_debug_only_distinguishes_apollo_reference_from_lane_projection() -> None:
    report = analyze_apollo_reference_line_contract_run_dir(FIXTURE_ROOT / "control_debug_only")

    assert report["status"] == "warn"
    assert "planning_reference_missing_control_debug_only" in report["warnings"]
    assert report["evidence"]["control_reference_available"] is True
    assert report["evidence"]["planning_reference_available"] is False
    assert report["metrics"]["control_ref_heading_error_p95_rad"] < 0.05
    assert report["localization_contract_status"] == "pass"
    assert "reference_line_heading_error_high" not in report["blocking_reasons"]


def test_cli_writes_report(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--run-dir",
            str(FIXTURE_ROOT / "pass"),
            "--out",
            str(out_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    report = json.loads((out_dir / "apollo_reference_line_contract_report.json").read_text(encoding="utf-8"))

    assert payload["status"] == "pass"
    assert report["status"] == "pass"


def test_reference_line_mismatch_blocks_natural_driving_hard_pass(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    shutil.copytree(NATURAL_FIXTURE, suite_root)
    lane_report = (
        suite_root
        / "lane_keep_097"
        / "analysis"
        / "apollo_reference_line_contract"
        / "apollo_reference_line_contract_report.json"
    )
    payload = _read_json(lane_report)
    payload["status"] = "fail"
    payload["blocking_reasons"] = ["reference_line_heading_error_high"]
    payload["metrics"]["control_ref_heading_error_p95_rad"] = 0.35
    _write_json(lane_report, payload)

    report = analyze_natural_driving_suite(suite_root)
    lane = next(run for run in report["run_results"] if run["run_id"] == "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "apollo_reference_line_contract_blocking"
    assert lane["apollo_reference_line_contract_status"] == "fail"
    assert "reference_line_heading_error_high" in lane["apollo_reference_line_blocking_reasons"]
    assert (
        "apollo_reference_line_contract.blocking_reasons.reference_line_heading_error_high"
        in lane["missing_fields"]
    )


def test_refresh_preserves_existing_report_when_raw_inputs_are_absent(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    shutil.copytree(NATURAL_FIXTURE / "lane_keep_097", run_dir)

    result = ensure_apollo_reference_line_contract_report(run_dir, refresh=True)
    report = _read_json(Path(result["path"]))

    assert result["status"] == "existing_report_copied"
    assert result["report_status"] == "pass"
    assert result["source_report"] == result["path"]
    assert report["status"] == "pass"


def test_refresh_regenerates_when_raw_reference_line_inputs_exist(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path, "lane_heading_mismatch")
    existing = run_dir / "analysis" / "apollo_reference_line_contract" / "apollo_reference_line_contract_report.json"
    existing.parent.mkdir(parents=True, exist_ok=True)
    _write_json(
        existing,
        {
            "schema_version": REPORT_SCHEMA_VERSION,
            "status": "pass",
            "blocking_reasons": [],
            "source": {"run_dir": "."},
        },
    )

    result = ensure_apollo_reference_line_contract_report(run_dir, refresh=True)
    report = _read_json(Path(result["path"]))

    assert result["status"] == "generated"
    assert result["report_status"] == "fail"
    assert report["status"] == "fail"
    assert "reference_line_heading_error_high" in report["blocking_reasons"]


def _copy_case(tmp_path: Path, name: str) -> Path:
    target = tmp_path / name
    shutil.copytree(FIXTURE_ROOT / name, target)
    return target


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_hdmap_projection(path: Path, *, heading_error_rad: float, lateral_error_m: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "timestamp": 0.0,
            "localization_x": 10.0,
            "localization_y": 2.0,
            "localization_heading": 0.0,
            "nearest_lane_id": "lane_097",
            "projection_s": 3.0,
            "projection_l": lateral_error_m,
            "lane_heading_at_s": 0.0,
            "heading_error_rad": heading_error_rad,
            "lateral_error_m": lateral_error_m,
            "road_id": "road_1",
            "junction_id": None,
            "source": "apollo_hdmap_api",
            "map_name": "Town01",
            "map_dir": "/apollo/modules/map/data/town01",
            "status": "ok",
        },
        {
            "timestamp": 0.05,
            "localization_x": 10.5,
            "localization_y": 2.0,
            "localization_heading": 0.0,
            "nearest_lane_id": "lane_097",
            "projection_s": 3.5,
            "projection_l": lateral_error_m,
            "lane_heading_at_s": 0.0,
            "heading_error_rad": heading_error_rad,
            "lateral_error_m": lateral_error_m,
            "road_id": "road_1",
            "junction_id": None,
            "source": "apollo_hdmap_api",
            "map_name": "Town01",
            "map_dir": "/apollo/modules/map/data/town01",
            "status": "ok",
        },
    ]
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
