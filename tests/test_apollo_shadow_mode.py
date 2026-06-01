from __future__ import annotations

import csv
from pathlib import Path

from carla_testbed.analysis.apollo_shadow_mode import (
    analyze_apollo_shadow_mode_timeseries as analyze_town01_shadow_timeseries,
    write_apollo_shadow_mode_report,
)
from carla_testbed.algorithms.shadow_mode import (
    analyze_shadow_mode_timeseries,
    write_shadow_mode_report,
)

CONFIG_PATH = Path("configs/algorithms/apollo_shadow_mode_check.yaml")
TIMESERIES_PATH = Path("tests/fixtures/apollo/shadow_mode_timeseries.csv")
TOWN01_TIMESERIES_PATH = Path("tests/fixtures/apollo_shadow_mode/timeseries.csv")


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_fixture_rows() -> list[dict[str, str]]:
    with TIMESERIES_PATH.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_valid_fixture_passes() -> None:
    report = analyze_shadow_mode_timeseries(TIMESERIES_PATH, CONFIG_PATH)

    assert report["schema_version"] == "apollo_shadow_mode_report.v1"
    assert report["status"] == "pass"
    assert report["failure_reason"] is None
    assert report["planning_available"] is True
    assert report["control_available"] is True
    assert report["trajectory_route_alignment"]["p95_lateral_error_m"] < 1.5


def test_missing_apollo_fields_is_insufficient_data(tmp_path: Path) -> None:
    rows = [
        {
            "run_id": "missing_apollo",
            "variant_id": "apollo_10_0_carla_gt_town01_reference",
            "route_id": "route",
            "sim_time": i * 0.1,
            "cross_track_error": 0.1,
            "heading_error": 0.01,
        }
        for i in range(10)
    ]
    path = tmp_path / "missing_apollo.csv"
    _write_rows(path, rows)

    report = analyze_shadow_mode_timeseries(path, CONFIG_PATH)

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "planning_missing"
    assert "planning_available" in report["missing_fields"]
    assert "control_available" in report["missing_fields"]


def test_planning_rate_zero_fails(tmp_path: Path) -> None:
    rows = _read_fixture_rows()
    for row in rows:
        row["planning_available"] = "false"
    path = tmp_path / "no_planning.csv"
    _write_rows(path, rows)

    report = analyze_shadow_mode_timeseries(path, CONFIG_PATH)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "planning_missing"
    assert report["planning_available"] is False


def test_matched_point_anomaly_fails(tmp_path: Path) -> None:
    rows = _read_fixture_rows()
    rows[3]["apollo_matched_point_distance"] = "7.5"
    path = tmp_path / "matched_anomaly.csv"
    _write_rows(path, rows)

    report = analyze_shadow_mode_timeseries(path, CONFIG_PATH)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "matched_point_anomaly"
    assert report["matched_target_anomalies"]["matched_point_too_large_count"] == 1


def test_target_point_anomaly_fails(tmp_path: Path) -> None:
    rows = _read_fixture_rows()
    rows[4]["apollo_target_point_distance"] = "9.5"
    path = tmp_path / "target_anomaly.csv"
    _write_rows(path, rows)

    report = analyze_shadow_mode_timeseries(path, CONFIG_PATH)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "target_point_anomaly"
    assert report["matched_target_anomalies"]["target_point_jump_count"] == 1


def test_writer_creates_shadow_report(tmp_path: Path) -> None:
    report = analyze_shadow_mode_timeseries(TIMESERIES_PATH, CONFIG_PATH)
    outputs = write_shadow_mode_report(report, tmp_path)

    assert Path(outputs["shadow_mode_report"]).exists()


def test_town01_shadow_fixture_generates_report(tmp_path: Path) -> None:
    report = analyze_town01_shadow_timeseries(TOWN01_TIMESERIES_PATH)
    outputs = write_apollo_shadow_mode_report(report, tmp_path)

    assert report["schema_version"] == "apollo_town01_shadow_mode_report.v1"
    assert report["status"] == "pass"
    assert report["planning_available"] is True
    assert report["control_available"] is True
    assert report["trajectory_heading_error_p95"] < 0.35
    assert report["matched_point_anomaly_count"] == 0
    assert report["target_point_jump_count"] == 0
    assert Path(outputs["apollo_shadow_mode_report"]).exists()
    assert Path(outputs["summary"]).exists()


def test_town01_missing_matched_target_fields_graceful_degrade(tmp_path: Path) -> None:
    rows = _read_town01_fixture_rows()
    for row in rows:
        row.pop("matched_point_distance")
        row.pop("target_point_distance")
    path = tmp_path / "missing_matched_target.csv"
    _write_rows(path, rows)

    report = analyze_town01_shadow_timeseries(path)

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_matched_or_target_point_fields"
    assert "matched_point_distance" in report["missing_fields"]
    assert "target_point_distance" in report["missing_fields"]
    assert report["matched_point_anomaly_count"] is None
    assert report["target_point_jump_count"] is None


def test_town01_summary_semantics_do_not_replace_per_frame_p1(tmp_path: Path) -> None:
    import json

    rows = _read_town01_fixture_rows()
    for row in rows:
        row.pop("apollo_trajectory_heading")
        row.pop("matched_point_distance")
        row.pop("target_point_distance")
    timeseries = tmp_path / "missing_p1.csv"
    summary = tmp_path / "summary.json"
    _write_rows(timeseries, rows)
    summary.write_text(
        json.dumps(
            {
                "semantic_window_anchor_kind": "matched_point_too_large",
                "first_high_steer_seq": 374,
                "first_matched_point_too_large_seq": 381,
                "high_steer_before_first_matched_point_too_large": True,
                "apollo_simple_lat_lateral_error_abs_p95": 0.086,
                "target_point_kappa_abs_p95_before_anchor": 0.197,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_town01_shadow_timeseries(timeseries, summary_json=summary)

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "per_frame_p1_missing_summary_semantics_available"
    assert report["per_frame_p1_complete"] is False
    assert report["summary_semantics_available"] is True
    assert report["summary_semantics"]["semantic_window_anchor_kind"] == "matched_point_too_large"
    assert report["summary_semantics"]["evidence_level"] == "summary_derived_not_per_frame_p1"
    assert report["required_next_fields"] == [
        "apollo_trajectory_heading",
        "matched_point_distance",
        "target_point_distance",
    ]
    assert "summary_semantics_available_but_per_frame_p1_missing" in report["warnings"]


def test_town01_first_high_steer_locates_route_s(tmp_path: Path) -> None:
    rows = _read_town01_fixture_rows()
    rows[4]["apollo_steer_raw"] = "0.91"
    path = tmp_path / "high_steer.csv"
    _write_rows(path, rows)

    report = analyze_town01_shadow_timeseries(path)

    assert report["status"] == "warn"
    assert report["failure_reason"] == "high_steer_observed"
    assert report["first_high_steer_s"] == 0.4
    assert report["first_high_steer_route_s"] == 4.0


def test_town01_target_point_jump_fails(tmp_path: Path) -> None:
    rows = _read_town01_fixture_rows()
    rows[5]["target_point_distance"] = "8.5"
    path = tmp_path / "target_jump.csv"
    _write_rows(path, rows)

    report = analyze_town01_shadow_timeseries(path)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "target_point_jump"
    assert report["target_point_jump_count"] >= 1


def test_town01_shadow_cli_writes_json_and_summary(tmp_path: Path) -> None:
    import json
    import subprocess
    import sys

    out_dir = tmp_path / "shadow_cli"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_shadow_mode.py",
            "--timeseries",
            str(TOWN01_TIMESERIES_PATH),
            "--out",
            str(out_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = json.loads(result.stdout)
    report_path = out_dir / "apollo_shadow_mode_report.json"
    summary_path = out_dir / "summary.md"

    assert stdout["status"] == "pass"
    assert report_path.exists()
    assert summary_path.exists()
    assert "closed-loop success" in summary_path.read_text(encoding="utf-8")


def _read_town01_fixture_rows() -> list[dict[str, str]]:
    with TOWN01_TIMESERIES_PATH.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))
