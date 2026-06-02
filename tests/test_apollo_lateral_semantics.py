from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_lateral_semantics import (
    FIELD_ALIASES,
    analyze_apollo_lateral_semantics,
    analyze_apollo_lateral_semantics_run_dir,
)

FIXTURE_RUN = Path("tests/fixtures/apollo_lateral/straight_high_kappa")


def _write_rows(path: Path, rows: list[dict[str, object]]) -> Path:
    fieldnames = [
        "run_id",
        "route_id",
        "backend",
        "sim_time",
        "route_curvature",
        "reference_lane_curvature",
        "apollo_planning_first_kappa",
        "apollo_target_point_kappa",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "ego_yaw_rate",
        "cross_track_error",
        "heading_error",
        "apollo_matched_point_distance",
        "apollo_target_point_distance",
        "steer_scale",
        "steering_sign",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _base_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(4):
        raw = 0.20 - index * 0.02
        rows.append(
            {
                "run_id": "fixture_run",
                "route_id": "lane_keep_097",
                "backend": "carla_direct",
                "sim_time": index * 0.05,
                "route_curvature": 0.0,
                "reference_lane_curvature": 0.0,
                "apollo_planning_first_kappa": 0.0,
                "apollo_target_point_kappa": 0.0,
                "apollo_steer_raw": raw,
                "bridge_steer_mapped": raw * 0.25,
                "carla_steer_applied": raw * 0.25,
                "ego_yaw_rate": 0.01,
                "cross_track_error": 0.02,
                "heading_error": 0.01,
                "apollo_matched_point_distance": 0.20,
                "apollo_target_point_distance": 0.50 + index * 0.05,
                "steer_scale": 0.25,
                "steering_sign": 1,
            }
        )
    return rows


def _analyze(tmp_path: Path, rows: list[dict[str, object]]) -> dict:
    path = _write_rows(tmp_path / "timeseries.csv", rows)
    return analyze_apollo_lateral_semantics(timeseries=path)


def _types(report: dict) -> set[str]:
    return {item["type"] for item in report["anomalies"]}


def test_straight_route_high_planning_kappa_suspects_reference_line_semantics() -> None:
    report = analyze_apollo_lateral_semantics_run_dir(FIXTURE_RUN)
    assert report["schema_version"] == "apollo_lateral_semantics.v1"
    assert report["suspected_layer"] == "reference_line_semantics"
    assert report["confidence"] in {"medium", "high"}
    assert "route_straight_but_planning_kappa_high" in _types(report)
    assert report["resolved_fields"]["apollo_planning_first_kappa"] == "apollo_planning_first_kappa"


def test_high_target_kappa_suspects_target_point_semantics(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["apollo_target_point_kappa"] = 0.12
    report = _analyze(tmp_path, rows)
    assert report["suspected_layer"] == "target_point_semantics"
    assert "target_kappa_spike" in _types(report)


def test_raw_mapped_mismatch_suspects_control_mapping(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["bridge_steer_mapped"] = 0.50
        row["carla_steer_applied"] = 0.50
    report = _analyze(tmp_path, rows)
    assert report["suspected_layer"] == "control_mapping"
    assert "raw_mapped_applied_mismatch" in _types(report)


def test_applied_steer_no_yaw_response_suspects_vehicle_response(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["apollo_steer_raw"] = 0.40
        row["bridge_steer_mapped"] = 0.10
        row["carla_steer_applied"] = 0.10
        row["ego_yaw_rate"] = 0.0
    report = _analyze(tmp_path, rows)
    assert report["suspected_layer"] == "vehicle_response"
    assert "applied_steer_no_yaw_response" in _types(report)


def test_missing_fields_gracefully_degrade(tmp_path: Path) -> None:
    path = _write_rows(tmp_path / "timeseries.csv", [{"run_id": "missing"}])
    report = analyze_apollo_lateral_semantics(timeseries=path)
    assert report["verdict"]["status"] == "insufficient_data"
    assert report["suspected_layer"] == "insufficient_data"
    assert "apollo_planning_first_kappa" in report["missing_fields"]
    assert "insufficient_data" in _types(report)


def test_alias_fields_are_resolved(tmp_path: Path) -> None:
    path = tmp_path / "alias.csv"
    fieldnames = [
        "run_id",
        "route_id",
        "source_steer",
        "mapped_steer",
        "applied_steer",
        "target_point_kappa",
        "first_point_kappa",
        "reference_line_curvature",
        "curvature_at_nearest",
        "matched_point_distance",
        "target_point_distance",
        "yaw_rate",
        "lateral_error",
        "heading_error",
        "steer_scale",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "alias",
                "route_id": "lane_keep_097",
                "source_steer": 0.2,
                "mapped_steer": 0.05,
                "applied_steer": 0.05,
                "target_point_kappa": 0.0,
                "first_point_kappa": 0.12,
                "reference_line_curvature": 0.0,
                "curvature_at_nearest": 0.0,
                "matched_point_distance": 0.2,
                "target_point_distance": 0.5,
                "yaw_rate": 0.01,
                "lateral_error": 0.02,
                "heading_error": 0.01,
                "steer_scale": 0.25,
            }
        )
    report = analyze_apollo_lateral_semantics(timeseries=path)
    assert report["resolved_fields"]["apollo_steer_raw"] == "source_steer"
    assert report["resolved_fields"]["apollo_planning_first_kappa"] == "first_point_kappa"
    assert report["resolved_fields"]["reference_lane_curvature"] == "reference_line_curvature"
    assert "route_straight_but_planning_kappa_high" in _types(report)
    assert "source_steer" in FIELD_ALIASES["apollo_steer_raw"]


def test_cli_run_dir_writes_report(tmp_path: Path) -> None:
    out = tmp_path / "out"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_lateral_semantics.py",
            "--run-dir",
            str(FIXTURE_RUN),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    payload = json.loads(result.stdout)
    assert payload["suspected_layer"] == "reference_line_semantics"
    assert (out / "apollo_lateral_semantics_report.json").is_file()
    report = json.loads((out / "apollo_lateral_semantics_report.json").read_text(encoding="utf-8"))
    assert report["verdict"]["status"] == "warn"
