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
        "route_s",
        "route_curvature",
        "reference_lane_curvature",
        "apollo_planning_first_kappa",
        "apollo_target_point_kappa",
        "apollo_steer_raw",
        "apollo_debug_simple_lon_current_station_m",
        "apollo_debug_simple_lon_station_reference_m",
        "apollo_debug_simple_lat_target_point_s",
        "apollo_debug_simple_lon_matched_point_s",
        "ego_x",
        "ego_y",
        "route_x",
        "route_y",
        "route_heading",
        "apollo_debug_simple_lat_target_point_x",
        "apollo_debug_simple_lat_target_point_y",
        "apollo_debug_simple_lon_matched_point_x",
        "apollo_debug_simple_lon_matched_point_y",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "ego_yaw_rate",
        "cross_track_error",
        "apollo_debug_simple_lat_lateral_error_m",
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
                "route_s": index * 2.0,
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


def test_high_lateral_drift_with_low_source_steer_suspects_target_point_semantics(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["apollo_steer_raw"] = 0.0
        row["bridge_steer_mapped"] = 0.0
        row["carla_steer_applied"] = 0.0
        row["cross_track_error"] = 0.85
        row["heading_error"] = 0.01

    report = _analyze(tmp_path, rows)

    assert report["verdict"]["status"] == "warn"
    assert report["suspected_layer"] == "target_point_semantics"
    assert "high_lateral_drift_with_low_source_steer" in _types(report)


def test_simple_lat_near_zero_while_route_lateral_high_is_anomaly(tmp_path: Path) -> None:
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["route_s"] = 100.0 + index * 10.0
        row["ego_x"] = 1.0
        row["ego_y"] = float(index)
        row["route_x"] = 1.85
        row["route_y"] = float(index)
        row["apollo_debug_simple_lon_matched_point_x"] = 1.01
        row["apollo_debug_simple_lon_matched_point_y"] = float(index)
        row["apollo_debug_simple_lat_target_point_x"] = 1.01
        row["apollo_debug_simple_lat_target_point_y"] = float(index) + 1.4
        row["cross_track_error"] = 0.85
        row["apollo_debug_simple_lat_lateral_error_m"] = 0.002
        row["apollo_debug_simple_lon_current_station_m"] = 0.5 + index * 0.1
        row["apollo_debug_simple_lat_target_point_s"] = 1.0 + index * 0.1
        row["apollo_debug_simple_lon_matched_point_s"] = 0.0
        row["apollo_steer_raw"] = 0.0
        row["bridge_steer_mapped"] = 0.0
        row["carla_steer_applied"] = 0.0

    report = _analyze(tmp_path, rows)

    assert report["verdict"]["status"] == "warn"
    assert report["suspected_layer"] == "target_point_semantics"
    assert "route_lateral_high_but_simple_lat_and_source_steer_near_zero" in _types(report)
    assert "simple_lat_station_frame_not_route_s_aligned" in _types(report)
    assert "matched_point_tracks_ego_not_route_centerline" in _types(report)
    assert report["correlation_summary"]["apollo_simple_lat_lateral_error_abs"]["p95"] == 0.002
    assert report["correlation_summary"]["route_s_vs_apollo_current_station_abs_delta"]["p95"] > 90.0
    assert report["correlation_summary"]["ego_to_apollo_matched_point_xy_distance"]["p95"] < 0.02
    assert report["correlation_summary"]["route_to_apollo_matched_point_xy_distance"]["p95"] > 0.8


def test_reference_debug_missing_with_nonempty_planning_is_context_anomaly(tmp_path: Path) -> None:
    timeseries = _write_rows(tmp_path / "timeseries.csv", _base_rows())
    reference_contract = tmp_path / "apollo_reference_line_contract_report.json"
    reference_contract.write_text(
        json.dumps(
            {
                "status": "warn",
                "warnings": [
                    "reference_line_count_zero_debug_counter_with_nonempty_trajectory",
                    "reference_line_provider_ready_ratio_low",
                ],
                "evidence": {
                    "nonempty_trajectory_ratio": 0.91,
                    "reference_line_provider_ready_ratio": 0.0,
                },
                "metrics": {
                    "reference_line_count_zero_ratio": 1.0,
                    "routing_segment_count_zero_ratio": 0.08,
                },
                "reference_debug_diagnostic": {
                    "classification": "planning_reference_line_debug_export_gap",
                    "route_segment_available": True,
                    "control_simple_lat_reference_available": True,
                    "control_reference_join_coverage_ratio": 0.95,
                },
            }
        ),
        encoding="utf-8",
    )

    report = analyze_apollo_lateral_semantics(
        timeseries=timeseries,
        reference_line_contract=reference_contract,
    )

    assert report["verdict"]["status"] == "warn"
    assert "planning_nonempty_but_reference_line_debug_missing" in _types(report)
    assert report["reference_debug_summary"]["nonempty_planning_with_reference_debug_missing"] is True
    assert report["reference_debug_summary"]["reference_line_provider_ready_ratio"] == 0.0
    assert report["reference_debug_summary"]["reference_line_count_zero_ratio"] == 1.0
    assert report["reference_debug_summary"]["debug_gap_classification"] == "planning_reference_line_debug_export_gap"
    assert report["reference_debug_summary"]["control_simple_lat_reference_available"] is True
    assert report["anomalies"][0]["evidence"]["control_reference_join_coverage_ratio"] == 0.95


def test_lateral_drift_window_records_route_s_and_target_context(tmp_path: Path) -> None:
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["route_s"] = 10.0 + index * 5.0
        row["cross_track_error"] = [0.05, 0.25, 0.55, 0.85][index]
        row["apollo_debug_simple_lat_lateral_error_m"] = 0.002
        row["apollo_debug_simple_lon_current_station_m"] = -1.0 + index * 0.1
        row["apollo_debug_simple_lat_target_point_s"] = -1.5 + index * 0.1
        row["apollo_debug_simple_lon_matched_point_s"] = 0.0
        row["apollo_steer_raw"] = 0.0
        row["bridge_steer_mapped"] = 0.0
        row["carla_steer_applied"] = 0.0
        row["apollo_target_point_distance"] = 1.2 + index * 0.1
        row["apollo_matched_point_distance"] = 0.1 + index * 0.05

    report = _analyze(tmp_path, rows)
    drift = report["drift_window_summary"]
    first_context = drift["first_high_lateral_context"]["stats"]

    assert drift["status"] == "available"
    assert drift["first_high_lateral"]["route_s"] == 20.0
    assert drift["first_high_lateral"]["cross_track_error"] == 0.55
    assert drift["max_abs_lateral"]["route_s"] == 25.0
    assert drift["phase_samples"]["first_high_lateral"]["route_s"] == 20.0
    assert drift["phase_samples"]["first_high_lateral"]["apollo_simple_lat_lateral_error"] == 0.002
    assert drift["phase_samples"]["first_high_lateral"]["apollo_simple_lon_current_station"] == -0.8
    assert drift["phase_samples"]["first_high_lateral"]["apollo_simple_lat_target_point_s"] == -1.3
    assert drift["phase_samples"]["first_high_lateral"]["apollo_simple_lon_matched_point_s"] == 0.0
    assert drift["phase_samples"]["first_high_lateral"]["apollo_steer_raw"] == 0.0
    assert drift["phase_samples"]["first_high_lateral"]["target_point_distance"] == 1.4
    assert drift["phase_samples"]["end"]["route_s"] == 25.0
    assert first_context["apollo_steer_raw"]["p95"] == 0.0
    assert first_context["carla_steer_applied"]["p95"] == 0.0
    assert first_context["target_point_distance"]["count"] > 0
    assert first_context["matched_point_distance"]["count"] > 0
    assert first_context["apollo_simple_lon_current_station"]["count"] > 0


def test_lateral_sign_alignment_records_source_steer_same_sign_evidence(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["cross_track_error"] = 0.65
        row["apollo_debug_simple_lat_lateral_error_m"] = 0.60
        row["apollo_steer_raw"] = 0.30
        row["bridge_steer_mapped"] = 0.075
        row["carla_steer_applied"] = 0.075
        row["ego_yaw_rate"] = 0.04

    report = _analyze(tmp_path, rows)

    assert report["verdict"]["status"] == "warn"
    assert report["suspected_layer"] == "reference_line_semantics"
    assert "source_steer_same_sign_as_lateral_error" in _types(report)
    alignment = report["lateral_sign_alignment"]
    assert alignment["source_steer_vs_route_lateral_error"]["same_sign_ratio"] == 1.0
    assert alignment["source_steer_vs_simple_lat_lateral_error"]["same_sign_ratio"] == 1.0
    assert alignment["applied_steer_vs_route_lateral_error"]["same_sign_ratio"] == 1.0


def test_lateral_sign_alignment_opposite_sign_is_not_an_anomaly(tmp_path: Path) -> None:
    rows = _base_rows()
    for row in rows:
        row["cross_track_error"] = 0.65
        row["apollo_debug_simple_lat_lateral_error_m"] = 0.60
        row["apollo_steer_raw"] = -0.30
        row["bridge_steer_mapped"] = -0.075
        row["carla_steer_applied"] = -0.075
        row["ego_yaw_rate"] = 0.04

    report = _analyze(tmp_path, rows)

    assert "source_steer_same_sign_as_lateral_error" not in _types(report)
    alignment = report["lateral_sign_alignment"]
    assert alignment["source_steer_vs_route_lateral_error"]["opposite_sign_ratio"] == 1.0
    assert alignment["source_steer_vs_simple_lat_lateral_error"]["opposite_sign_ratio"] == 1.0


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


def test_debug_timeseries_alias_fields_are_resolved(tmp_path: Path) -> None:
    path = tmp_path / "debug_timeseries.csv"
    fieldnames = [
        "run_id",
        "route_id",
        "route_curvature",
        "apollo_debug_simple_lat_curvature",
        "first_trajectory_point_kappa",
        "apollo_debug_simple_lat_target_point_kappa",
        "apollo_desired_steer",
        "mapped_carla_steer_cmd",
        "measured_steer",
        "ego_yaw_rate_rad_s",
        "apollo_debug_simple_lat_lateral_error_m",
        "apollo_debug_simple_lat_heading_error_rad",
        "apollo_matched_point_distance",
        "apollo_target_point_distance",
        "steer_scale",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "debug_alias",
                "route_id": "town01_rh_spawn068_goal079",
                "route_curvature": 0.0,
                "apollo_debug_simple_lat_curvature": 0.0,
                "first_trajectory_point_kappa": 0.0,
                "apollo_debug_simple_lat_target_point_kappa": 0.001,
                "apollo_desired_steer": 0.2,
                "mapped_carla_steer_cmd": 0.05,
                "measured_steer": 0.05,
                "ego_yaw_rate_rad_s": 0.02,
                "apollo_debug_simple_lat_lateral_error_m": 0.1,
                "apollo_debug_simple_lat_heading_error_rad": 0.01,
                "apollo_matched_point_distance": 0.2,
                "apollo_target_point_distance": 1.5,
                "steer_scale": 0.25,
            }
        )

    report = analyze_apollo_lateral_semantics(timeseries=path)

    assert report["missing_fields"] == []
    assert report["verdict"]["status"] == "pass"
    assert report["suspected_layer"] == "none"
    assert report["resolved_fields"]["reference_lane_curvature"] == "apollo_debug_simple_lat_curvature"
    assert report["resolved_fields"]["apollo_target_point_kappa"] == "apollo_debug_simple_lat_target_point_kappa"
    assert report["resolved_fields"]["bridge_steer_mapped"] == "mapped_carla_steer_cmd"
    assert report["resolved_fields"]["matched_point_distance"] == "apollo_matched_point_distance"


def test_run_dir_uses_debug_timeseries_for_apollo_semantic_fields(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    _write_rows(run_dir / "timeseries.csv", [{"run_id": "plain_timeseries_only"}])
    debug_path = run_dir / "artifacts" / "debug_timeseries.csv"
    fieldnames = [
        "run_id",
        "route_id",
        "route_curvature",
        "apollo_debug_simple_lat_curvature",
        "first_trajectory_point_kappa",
        "apollo_debug_simple_lat_target_point_kappa",
        "apollo_desired_steer",
        "mapped_carla_steer_cmd",
        "measured_steer",
        "ego_yaw_rate_rad_s",
        "apollo_debug_simple_lat_lateral_error_m",
        "apollo_debug_simple_lat_heading_error_rad",
        "apollo_matched_point_distance",
        "apollo_target_point_distance",
        "steer_scale",
    ]
    with debug_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "debug_timeseries",
                "route_id": "lane_keep_097",
                "route_curvature": 0.0,
                "apollo_debug_simple_lat_curvature": 0.0,
                "first_trajectory_point_kappa": 0.0,
                "apollo_debug_simple_lat_target_point_kappa": 0.001,
                "apollo_desired_steer": 0.2,
                "mapped_carla_steer_cmd": 0.05,
                "measured_steer": 0.05,
                "ego_yaw_rate_rad_s": 0.02,
                "apollo_debug_simple_lat_lateral_error_m": 0.1,
                "apollo_debug_simple_lat_heading_error_rad": 0.01,
                "apollo_matched_point_distance": 0.2,
                "apollo_target_point_distance": 1.5,
                "steer_scale": 0.25,
            }
        )

    report = analyze_apollo_lateral_semantics_run_dir(run_dir)

    assert report["missing_fields"] == []
    assert report["verdict"]["status"] == "pass"
    assert report["suspected_layer"] == "none"
    assert any(str(path).endswith("artifacts/debug_timeseries.csv") for path in report["source"]["timeseries"])
    assert report["resolved_fields"]["apollo_target_point_kappa"] == "apollo_debug_simple_lat_target_point_kappa"


def test_run_dir_merges_control_apply_trace_over_sparse_timeseries_control_fields(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["sim_time"] = 10.0 + index * 0.05
        row["route_s"] = index * 2.0
        row["cross_track_error"] = 0.60 + index * 0.05
        row["heading_error"] = 0.10 + index * 0.01
        row["apollo_steer_raw"] = 0.0
        row["bridge_steer_mapped"] = 0.0
        row["carla_steer_applied"] = 0.0
        row["ego_yaw_rate"] = 0.0
    _write_rows(run_dir / "timeseries.csv", rows)
    trace_rows = []
    for index, raw in enumerate((0.30, 0.35, 0.40, 0.45)):
        trace_rows.append(
            {
                "schema_version": "control_apply_trace.v1",
                "sim_time": 10.0 + index * 0.05,
                "apollo_raw": {"steer": raw},
                "bridge_mapped": {"mapped_carla_steer_cmd": raw * 0.25},
                "carla_applied": {"steer": raw * 0.25},
                "vehicle_response": {"yaw_rate_rad_s": raw * 0.10},
            }
        )
    (run_dir / "artifacts" / "control_apply_trace.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in trace_rows),
        encoding="utf-8",
    )

    report = analyze_apollo_lateral_semantics_run_dir(run_dir)

    assert any(str(path).endswith("artifacts/control_apply_trace.jsonl") for path in report["source"]["control_trace"])
    assert report["correlation_summary"]["apollo_steer_raw_abs"]["max"] == 0.45
    assert report["correlation_summary"]["bridge_steer_mapped_abs"]["max"] == 0.1125
    assert report["correlation_summary"]["carla_steer_applied_abs"]["max"] == 0.1125
    first_high = report["drift_window_summary"]["phase_samples"]["first_high_lateral"]
    assert first_high["apollo_steer_raw"] == 0.30
    assert first_high["bridge_steer_mapped"] == 0.075
    assert first_high["carla_steer_applied"] == 0.075
    assert report["resolved_fields"]["apollo_steer_raw"] == "apollo_steer_raw"


def test_run_dir_merges_control_decode_debug_simple_lat_fields(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["sim_time"] = 20.0 + index * 0.05
        row["route_s"] = index * 2.0
        row["cross_track_error"] = [0.05, 0.25, 0.60, 0.70][index]
        row["heading_error"] = [0.01, 0.03, 0.12, 0.14][index]
        row["apollo_steer_raw"] = 0.0
        row["bridge_steer_mapped"] = 0.0
        row["carla_steer_applied"] = 0.0
    _write_rows(run_dir / "timeseries.csv", rows)
    decode_rows = []
    for index, steer in enumerate((0.0, 0.10, 0.30, 0.35)):
        sim_time = 20.0 + index * 0.05
        decode_rows.append(
            {
                "ts_sec": 1000.0 + index,
                "parsed_control": {
                    "steer": steer,
                    "debug_simple_lat_lateral_error_m": -rows[index]["cross_track_error"],
                    "debug_simple_lat_heading_error_rad": -rows[index]["heading_error"],
                    "debug_simple_lat_curvature": 0.0,
                    "debug_simple_lat_target_point_kappa": 0.0,
                    "debug_simple_lat_target_point_s": -1.0 - index * 0.1,
                    "debug_simple_lat_target_point_x": 10.0 + index,
                    "debug_simple_lat_target_point_y": 5.0,
                    "debug_simple_lon_current_station_m": -1.0,
                    "debug_simple_lon_station_reference_m": 0.0,
                    "debug_simple_lon_matched_point_s": 0.0,
                    "debug_simple_lon_matched_point_x": 11.0 + index,
                    "debug_simple_lon_matched_point_y": 5.0,
                },
                "output_to_carla": {
                    "gt_state_sim_time_sec": sim_time,
                    "mapped_carla_steer_cmd": steer * 0.25,
                    "steer_sign": 1.0,
                },
            }
        )
    (run_dir / "artifacts" / "control_decode_debug.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in decode_rows),
        encoding="utf-8",
    )

    report = analyze_apollo_lateral_semantics_run_dir(run_dir)

    assert any(str(path).endswith("artifacts/control_decode_debug.jsonl") for path in report["source"]["control_trace"])
    first_high = report["drift_window_summary"]["phase_samples"]["first_high_lateral"]
    assert first_high["cross_track_error"] == 0.60
    assert first_high["apollo_simple_lat_lateral_error"] == -0.60
    assert first_high["apollo_steer_raw"] == 0.30
    assert first_high["apollo_simple_lat_target_point_s"] == -1.2
    alignment = report["lateral_sign_alignment"]["first_high_lateral_sample"]
    assert alignment["source_steer_vs_route_lateral_error"] == "same_sign"
    assert alignment["source_steer_vs_simple_lat_lateral_error"] == "opposite_sign"
    assert alignment["route_lateral_error_vs_simple_lat_lateral_error"] == "opposite_sign"
    assert "route_lateral_error_opposes_simple_lat_lateral_error" in _types(report)
    assert report["correlation_summary"]["apollo_simple_lat_lateral_error_abs"]["p95"] > 0.60


def test_route_simple_lat_opposite_sign_matching_magnitude_is_convention_candidate(
    tmp_path: Path,
) -> None:
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["sim_time"] = 30.0 + index * 0.05
        row["cross_track_error"] = 0.60 + index * 0.02
        row["apollo_debug_simple_lat_lateral_error_m"] = -row["cross_track_error"]
        row["apollo_steer_raw"] = 0.30
        row["bridge_steer_mapped"] = 0.075
        row["carla_steer_applied"] = 0.075
        row["ego_yaw_rate"] = 0.04

    report = _analyze(tmp_path, rows)
    alignment = report["lateral_sign_alignment"]["route_simple_lat_magnitude_alignment"]
    provenance = report["lateral_sign_alignment"]["route_lateral_provenance"]

    assert alignment["magnitude_agreement_candidate"] is True
    assert alignment["opposite_sign_ratio"] == 1.0
    assert alignment["opposite_sign_abs_sum_p95_m"] == 0.0
    assert alignment["abs_magnitude_delta_p95_m"] == 0.0
    assert provenance["evidence_level"] == "alias_only"
    assert provenance["route_lateral_source_field"] == "cross_track_error"
    assert "route_simple_lat_sign_convention_mismatch_candidate" in _types(report)
    assert "route_lateral_error_opposes_simple_lat_lateral_error" in _types(report)


def test_route_lateral_provenance_records_recomputed_route_geometry_cte(
    tmp_path: Path,
) -> None:
    rows = _base_rows()
    for index, row in enumerate(rows):
        row["sim_time"] = 40.0 + index * 0.05
        row["route_x"] = float(index)
        row["route_y"] = 0.0
        row["route_heading"] = 0.0
        row["ego_x"] = float(index)
        row["ego_y"] = 0.60
        row["cross_track_error"] = 0.60
        row["apollo_debug_simple_lat_lateral_error_m"] = -0.60
        row["apollo_steer_raw"] = 0.30
        row["bridge_steer_mapped"] = 0.075
        row["carla_steer_applied"] = 0.075

    report = _analyze(tmp_path, rows)
    provenance = report["lateral_sign_alignment"]["route_lateral_provenance"]

    assert provenance["evidence_level"] == "route_geometry_available"
    assert provenance["route_geometry_fields_available"] is True
    assert provenance["route_geometry_sample_count"] == 4
    assert provenance["route_geometry_recomputed_cte_abs_delta_p95_m"] == 0.0
    assert provenance["interpretation"] == "route_geometry_fields_allow_signed_cte_recompute"


def test_run_dir_consumes_localization_hdmap_lateral_consistency(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    _write_rows(run_dir / "artifacts" / "debug_timeseries.csv", _base_rows())
    loc_dir = run_dir / "analysis" / "localization_contract"
    loc_dir.mkdir(parents=True)
    (loc_dir / "localization_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_localization_contract.v1",
                "verdict": {
                    "status": "fail",
                    "blocking_reasons": ["apollo_hdmap_projection_lateral_error_high"],
                },
                "hdmap_route_lateral_consistency": {
                    "available": True,
                    "status": "pass",
                    "alignment_mode": "opposite_sign",
                    "best_abs_delta_p95_m": 0.015,
                    "projection_lateral_p95_m": 0.82,
                    "route_cross_track_p95_m": 0.81,
                    "sample_count": 60,
                    "interpretation": "hdmap_lateral_matches_route_cross_track_actual_lateral_drift",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_lateral_semantics_run_dir(run_dir)

    assert report["verdict"]["status"] == "warn"
    assert report["suspected_layer"] == "target_point_semantics"
    assert "actual_lateral_drift_matches_hdmap_projection" in _types(report)
    assert report["hdmap_route_lateral_consistency"]["status"] == "pass"
    assert report["source"]["localization_contract"].endswith("localization_contract_report.json")


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
