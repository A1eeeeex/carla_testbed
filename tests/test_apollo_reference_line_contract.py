from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from carla_testbed.analysis.apollo_reference_line_contract import (
    REPORT_SCHEMA_VERSION,
    analyze_apollo_reference_line_contract,
    analyze_apollo_reference_line_contract_files,
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
    assert report["status"] == "insufficient_data"
    assert report["blocking_reasons"] == []
    assert report["contracts"]["planning_trajectory"]["status"] == "pass"
    assert report["contracts"]["control_reference"]["status"] == "pass"
    assert report["contracts"]["apollo_hdmap_projection"]["status"] == "insufficient_data"
    assert report["planning_trajectory_contract"] == report["contracts"]["planning_trajectory"]
    assert report["control_reference_contract"] == report["contracts"]["control_reference"]
    assert report["apollo_hdmap_projection_contract"] == report["contracts"]["apollo_hdmap_projection"]
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

    assert report["status"] == "insufficient_data"
    assert report["evidence"]["apollo_hdmap_projection_available"] is False
    assert report["evidence"]["apollo_hdmap_projection_claim_grade"] is False
    assert report["apollo_hdmap_projection"]["status"] == "insufficient_data"
    assert report["contracts"]["planning_trajectory"]["status"] == "pass"
    assert report["contracts"]["control_reference"]["status"] == "pass"
    assert report["contracts"]["apollo_hdmap_projection"]["status"] == "insufficient_data"
    assert report["apollo_hdmap_projection_contract"]["status"] == "insufficient_data"


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


def test_nonempty_trajectory_with_zero_reference_line_count_is_not_old_empty_blocker() -> None:
    rows = [
        {
            "localization": {"heading": 0.0},
            "planning": {
                "trajectory_point_count": 20,
                "first_trajectory_point_theta": 0.01,
                "reference_line_count": 0,
                "reference_line_provider_status": "failed",
            },
            "computed": {
                "localization_to_planning_first_heading_error_rad": 0.01,
                "planning_reference_available": True,
                "control_reference_available": False,
            },
        }
    ]

    report = analyze_apollo_reference_line_contract(rows)

    assert report["contracts"]["planning_trajectory"]["status"] == "warn"
    assert "reference_line_count_zero_debug_counter_with_nonempty_trajectory" in report["warnings"]
    assert "reference_line_count_zero_with_empty_trajectory" not in report["blocking_reasons"]
    assert "planning_trajectory_empty_with_zero_reference_line_count" not in report["blocking_reasons"]


def test_nonempty_ratio_claim_window_excludes_pre_routing_startup_empty_messages() -> None:
    rows = []
    for _index in range(5):
        rows.append(
            {
                "routing": {"routing_segment_count": None},
                "planning": {"trajectory_point_count": 0, "routing_segment_count": None},
                "computed": {"planning_reference_available": False, "control_reference_available": False},
            }
        )
    rows.extend(
        [
            {
                "routing": {"routing_segment_count": 1},
                "planning": {"trajectory_point_count": 0, "routing_segment_count": 1},
                "computed": {"planning_reference_available": False, "control_reference_available": False},
            },
            {
                "routing": {"routing_segment_count": 1},
                "planning": {"trajectory_point_count": 20, "routing_segment_count": 1, "first_trajectory_point_theta": 0.0},
                "computed": {"planning_reference_available": True, "control_reference_available": False},
            },
            {
                "routing": {"routing_segment_count": 1},
                "planning": {"trajectory_point_count": 20, "routing_segment_count": 1, "first_trajectory_point_theta": 0.0},
                "computed": {"planning_reference_available": True, "control_reference_available": False},
            },
        ]
    )

    report = analyze_apollo_reference_line_contract(rows)
    evidence = report["evidence"]
    planning_contract = report["contracts"]["planning_trajectory"]

    assert evidence["nonempty_trajectory_ratio"] == 0.25
    assert evidence["nonempty_trajectory_ratio_after_routing_segment_available"] == pytest.approx(2 / 3)
    assert evidence["nonempty_trajectory_ratio_after_first_nonempty"] == 1.0
    assert evidence["planning_claim_window_nonempty_trajectory_ratio"] == pytest.approx(2 / 3)
    assert evidence["planning_claim_window_source"] == "after_routing_segment_available"
    assert planning_contract["key_metrics"]["planning_claim_window_nonempty_trajectory_ratio"] == pytest.approx(2 / 3)


def test_missing_control_reference_is_separate_contract_insufficient_data() -> None:
    rows = [
        {
            "localization": {"heading": 0.0},
            "planning": {
                "trajectory_point_count": 20,
                "first_trajectory_point_theta": 0.01,
                "reference_line_count": 1,
                "reference_line_provider_status": "ready",
            },
            "computed": {
                "localization_to_planning_first_heading_error_rad": 0.01,
                "planning_reference_available": True,
                "control_reference_available": False,
            },
        }
    ]

    report = analyze_apollo_reference_line_contract(rows)

    assert report["contracts"]["planning_trajectory"]["status"] == "pass"
    assert report["contracts"]["control_reference"]["status"] == "insufficient_data"
    assert "control_reference.control_reference_debug" in report["warnings"]


def test_control_debug_only_distinguishes_apollo_reference_from_lane_projection() -> None:
    report = analyze_apollo_reference_line_contract_run_dir(FIXTURE_ROOT / "control_debug_only")

    assert report["status"] == "insufficient_data"
    assert report["contracts"]["planning_trajectory"]["status"] == "insufficient_data"
    assert report["contracts"]["control_reference"]["status"] == "pass"
    assert report["evidence"]["control_reference_available"] is True
    assert report["evidence"]["planning_reference_available"] is False
    assert report["metrics"]["control_ref_heading_error_p95_rad"] < 0.05
    assert report["localization_contract_status"] == "pass"
    assert "reference_line_heading_error_high" not in report["blocking_reasons"]


def test_existing_contract_rows_are_augmented_with_control_decode_debug(tmp_path: Path) -> None:
    contract = tmp_path / "apollo_reference_line_contract.jsonl"
    control = tmp_path / "bridge_control_decode.jsonl"
    _write_jsonl(
        contract,
        [
            {
                "timestamp": 0.0,
                "localization": {"heading": 0.0},
                "planning": {
                    "trajectory_point_count": 20,
                    "first_trajectory_point_theta": 0.01,
                    "reference_line_count": 1,
                    "reference_line_provider_status": "ready",
                },
                "control": {},
                "computed": {
                    "planning_reference_available": True,
                    "control_reference_available": False,
                    "localization_to_planning_first_heading_error_rad": 0.01,
                },
            },
            {
                "timestamp": 0.1,
                "localization": {"heading": 0.0},
                "planning": {
                    "trajectory_point_count": 20,
                    "first_trajectory_point_theta": 0.02,
                    "reference_line_count": 1,
                    "reference_line_provider_status": "ready",
                },
                "control": {},
                "computed": {
                    "planning_reference_available": True,
                    "control_reference_available": False,
                    "localization_to_planning_first_heading_error_rad": 0.02,
                },
            },
        ],
    )
    _write_jsonl(
        control,
        [
            {
                "ts_sec": 0.0,
                "debug_simple_lat_heading_error": 0.01,
                "debug_simple_lat_lateral_error": 0.10,
            },
            {
                "ts_sec": 0.1,
                "debug_simple_lat_heading_error": 0.02,
                "debug_simple_lat_lateral_error": 0.12,
            },
        ],
    )

    report = analyze_apollo_reference_line_contract_files(
        contract_path=contract,
        control_decode_debug_path=control,
    )

    assert report["contracts"]["planning_trajectory"]["status"] == "pass"
    assert report["contracts"]["control_reference"]["status"] == "pass"
    assert report["evidence"]["control_reference_available"] is True
    assert report["metrics"]["control_ref_heading_error_p95_rad"] == 0.0195
    assert report["evidence"]["nonempty_trajectory_ratio"] == 1.0


def test_nested_apollo_control_raw_can_supply_control_reference(tmp_path: Path) -> None:
    contract = tmp_path / "apollo_reference_line_contract.jsonl"
    control = tmp_path / "apollo_control_raw.jsonl"
    _write_jsonl(
        contract,
        [
            {
                "timestamp": 0.0,
                "localization": {"heading": 0.0},
                "planning": {
                    "trajectory_point_count": 20,
                    "first_trajectory_point_theta": 0.01,
                    "reference_line_count": 1,
                    "reference_line_provider_status": "ready",
                },
                "computed": {
                    "planning_reference_available": True,
                    "control_reference_available": False,
                    "localization_to_planning_first_heading_error_rad": 0.01,
                },
            },
        ],
    )
    _write_jsonl(
        control,
        [
            {
                "ts_sec": 0.0,
                "apollo_control_raw": {
                    "debug_simple_lat_ref_heading": 0.01,
                    "debug_simple_lat_heading": 0.0,
                    "debug_simple_lat_heading_error": 0.01,
                    "debug_simple_lat_lateral_error": 0.10,
                },
            }
        ],
    )

    report = analyze_apollo_reference_line_contract_files(
        contract_path=contract,
        control_decode_debug_path=control,
    )

    assert report["contracts"]["control_reference"]["status"] == "pass"
    assert report["evidence"]["control_reference_available"] is True


def test_run_dir_prefers_control_decode_file_with_reference_fields(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(
        run_dir / "artifacts" / "apollo_reference_line_contract.jsonl",
        [
            {
                "timestamp": 0.0,
                "localization": {"heading": 0.0},
                "planning": {
                    "trajectory_point_count": 20,
                    "first_trajectory_point_theta": 0.01,
                    "reference_line_count": 1,
                    "reference_line_provider_status": "ready",
                },
                "computed": {
                    "planning_reference_available": True,
                    "control_reference_available": False,
                    "localization_to_planning_first_heading_error_rad": 0.01,
                },
            },
        ],
    )
    _write_jsonl(
        run_dir / "artifacts" / "control_decode_debug.jsonl",
        [{"ts_sec": 10.0, "raw_throttle": 0.0}],
    )
    _write_jsonl(
        run_dir / "artifacts" / "bridge_control_decode.jsonl",
        [
            {
                "ts_sec": 0.0,
                "debug_simple_lat_heading_error": 0.01,
                "debug_simple_lat_lateral_error": 0.10,
            }
        ],
    )

    report = analyze_apollo_reference_line_contract_run_dir(run_dir)

    assert report["source"]["control_decode_debug_path"].endswith("bridge_control_decode.jsonl")
    assert report["contracts"]["control_reference"]["status"] == "pass"
    assert report["evidence"]["control_reference_available"] is True


def test_unaligned_control_debug_is_not_index_joined_into_contract_rows(tmp_path: Path) -> None:
    contract = tmp_path / "apollo_reference_line_contract.jsonl"
    control = tmp_path / "bridge_control_decode.jsonl"
    _write_jsonl(
        contract,
        [
            {
                "timestamp": 0.0,
                "localization": {"heading": 0.0},
                "planning": {
                    "trajectory_point_count": 20,
                    "first_trajectory_point_theta": 0.01,
                    "reference_line_count": 1,
                    "reference_line_provider_status": "ready",
                },
                "computed": {
                    "planning_reference_available": True,
                    "control_reference_available": False,
                    "localization_to_planning_first_heading_error_rad": 0.01,
                },
            }
        ],
    )
    _write_jsonl(
        control,
        [
            {
                "ts_sec": 10.0,
                "debug_simple_lat_heading_error": 0.01,
                "debug_simple_lat_lateral_error": 0.10,
            }
        ],
    )

    report = analyze_apollo_reference_line_contract_files(
        contract_path=contract,
        control_decode_debug_path=control,
    )

    assert report["contracts"]["planning_trajectory"]["status"] == "pass"
    assert report["contracts"]["control_reference"]["status"] == "insufficient_data"
    assert report["metrics"]["control_ref_heading_error_p95_rad"] is None
    assert report["metrics"]["fallback_join_dropped_unaligned_rows"] == 1
    assert report["metrics"]["fallback_join_tolerance_ms"] == 50.0


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

    assert payload["status"] == "insufficient_data"
    assert report["status"] == "insufficient_data"


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


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


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
