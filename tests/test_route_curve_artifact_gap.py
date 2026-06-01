from __future__ import annotations

import csv
import json
from pathlib import Path

from carla_testbed.analysis.route_curve_artifact_gap import (
    analyze_route_curve_artifact_gap,
    write_route_curve_artifact_gap_report,
)

FIXTURE = Path("tests/fixtures/apollo_shadow_mode/timeseries.csv")


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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


def test_route_curve_artifact_gap_passes_when_required_p1_fields_exist() -> None:
    report = analyze_route_curve_artifact_gap(FIXTURE)

    assert report["schema_version"] == "route_curve_artifact_gap.v1"
    assert report["status"] == "pass"
    assert report["per_frame_p1_complete"] is True
    assert report["missing_p1_fields"] == []
    assert report["p1_field_presence"]["matched_point_distance"]["has_value"] is True
    assert report["p1_field_presence"]["target_point_distance"]["has_value"] is True
    assert report["p1_field_presence"]["apollo_trajectory_heading"]["has_value"] is True


def test_summary_semantics_do_not_replace_missing_per_frame_p1(tmp_path: Path) -> None:
    rows = _read_rows(FIXTURE)
    for row in rows:
        row.pop("apollo_trajectory_heading")
        row.pop("matched_point_distance")
        row.pop("target_point_distance")
    timeseries = tmp_path / "timeseries.csv"
    summary = tmp_path / "summary.json"
    _write_rows(timeseries, rows)
    summary.write_text(
        json.dumps(
            {
                "semantic_window_anchor_kind": "matched_point_too_large",
                "first_matched_point_too_large_seq": 381,
                "first_high_steer_seq": 374,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_route_curve_artifact_gap(timeseries, summary_json=summary)

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "per_frame_p1_missing_summary_semantics_available"
    assert report["per_frame_p1_complete"] is False
    assert report["summary_semantics_available"] is True
    assert report["summary_semantics"]["evidence_level"] == "summary_derived_not_per_frame_p1"
    assert report["missing_p1_fields"] == [
        "apollo_trajectory_heading",
        "matched_point_distance",
        "target_point_distance",
    ]
    assert "summary_semantics_available_but_per_frame_p1_missing" in report["warnings"]


def test_prefixed_matched_target_aliases_count_as_p1(tmp_path: Path) -> None:
    rows = _read_rows(FIXTURE)
    for row in rows:
        row["apollo_matched_point_distance"] = row.pop("matched_point_distance")
        row["apollo_target_point_distance"] = row.pop("target_point_distance")
    timeseries = tmp_path / "prefixed.csv"
    _write_rows(timeseries, rows)

    report = analyze_route_curve_artifact_gap(timeseries)

    assert report["status"] == "pass"
    assert report["missing_p1_fields"] == []
    assert report["p1_field_presence"]["matched_point_distance"]["observed_columns"] == [
        "apollo_matched_point_distance"
    ]


def test_legacy_simple_lat_target_fields_partially_close_p1_gap(tmp_path: Path) -> None:
    rows = [
        {
            "run_id": "legacy",
            "route_id": "lane217",
            "sim_time": "1.0",
            "apollo_debug_simple_lat_target_point_theta_rad": "1.57",
            "apollo_debug_simple_lat_target_point_kappa": "0.01",
            "apollo_debug_simple_lat_target_point_s": "12.5",
        }
    ]
    timeseries = tmp_path / "legacy.csv"
    summary = tmp_path / "summary.json"
    _write_rows(timeseries, rows)
    summary.write_text(
        json.dumps({"semantic_window_anchor_kind": "matched_point_too_large"}),
        encoding="utf-8",
    )

    report = analyze_route_curve_artifact_gap(timeseries, summary_json=summary)

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "per_frame_p1_missing_summary_semantics_available"
    assert report["p1_field_presence"]["apollo_trajectory_heading"]["has_value"] is True
    assert report["p1_field_presence"]["apollo_trajectory_curvature"]["has_value"] is True
    assert report["p1_supporting_field_presence"]["apollo_target_point_s"]["has_value"] is True
    assert report["missing_p1_fields"] == ["matched_point_distance", "target_point_distance"]


def test_computed_matched_target_distance_fields_close_required_p1_gap(tmp_path: Path) -> None:
    rows = [
        {
            "run_id": "legacy",
            "route_id": "lane217",
            "sim_time": "1.0",
            "apollo_trajectory_heading": "1.57",
            "apollo_matched_point_distance": "0.5",
            "apollo_target_point_distance": "5.0",
        }
    ]
    timeseries = tmp_path / "distances.csv"
    _write_rows(timeseries, rows)

    report = analyze_route_curve_artifact_gap(timeseries)

    assert report["status"] == "pass"
    assert report["missing_p1_fields"] == []
    assert report["per_frame_p1_complete"] is True


def test_missing_timeseries_gracefully_reports_artifact_gap(tmp_path: Path) -> None:
    report = analyze_route_curve_artifact_gap(tmp_path / "missing.csv")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_timeseries"
    assert "timeseries" in report["missing_inputs"]
    assert "timeseries.csv or timeseries.jsonl" in report["required_next_artifacts"]


def test_route_curve_artifact_gap_writer_creates_json_and_summary(tmp_path: Path) -> None:
    report = analyze_route_curve_artifact_gap(FIXTURE)
    outputs = write_route_curve_artifact_gap_report(report, tmp_path)

    assert Path(outputs["route_curve_artifact_gap_report"]).exists()
    assert Path(outputs["summary"]).exists()
    assert "per-frame P1" in Path(outputs["summary"]).read_text(encoding="utf-8")
