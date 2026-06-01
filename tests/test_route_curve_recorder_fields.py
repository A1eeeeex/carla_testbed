from __future__ import annotations

import csv
from pathlib import Path

from carla_testbed.analysis.route_health import analyze_route_health
from carla_testbed.record import TimeseriesRecorder
from carla_testbed.record.artifact_store import build_manifest, build_summary
from carla_testbed.record.route_curve_fields import (
    ROUTE_CURVE_FIELDS_SCHEMA_VERSION,
    ROUTE_CURVE_P0_FIELDS,
    ROUTE_CURVE_P1_FIELDS,
    ROUTE_CURVE_P2_FIELDS,
    build_route_curve_row,
    ensure_route_curve_p0_fields,
)
from carla_testbed.record.route_curve_context import route_definition_from_metadata
from carla_testbed.routes.io import load_route_json


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_route_curve_p0_field_list_is_fixed() -> None:
    assert ROUTE_CURVE_P0_FIELDS == [
        "sim_time",
        "frame_id",
        "route_id",
        "route_index",
        "nearest_route_index",
        "route_s",
        "route_x",
        "route_y",
        "route_z",
        "route_heading",
        "route_curvature",
        "ego_x",
        "ego_y",
        "ego_z",
        "ego_heading",
        "ego_speed",
        "ego_yaw_rate",
        "cross_track_error",
        "heading_error",
        "curvature_at_nearest",
        "curve_segment_id",
        "is_curve_segment",
        "is_junction_segment",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "throttle_raw",
        "throttle_mapped",
        "throttle_applied",
        "brake_raw",
        "brake_mapped",
        "brake_applied",
        "lateral_guard_applied",
        "trajectory_contract_guard_applied",
    ]


def test_build_route_curve_row_returns_all_p0_fields() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_curve.json")
    point = route.points[1]

    row = build_route_curve_row(
        sim_time=1.2,
        frame_id=7,
        route=route,
        ego_pose={"x": point.x, "y": point.y, "z": point.z, "heading": point.heading},
        ego_speed=3.4,
        ego_yaw_rate=0.05,
        apollo_control={"raw_steer": 0.8},
        raw_control={"throttle": 0.2, "brake": 0.0, "steer": 0.8},
        mapped_control={"throttle": 0.18, "brake": 0.0, "steer": 0.2},
        applied_control={"throttle": 0.17, "brake": 0.0, "steer": 0.19},
        guard_flags={"lateral_guard_applied": True},
    )

    assert set(ROUTE_CURVE_P0_FIELDS).issubset(row)
    assert row["route_id"] == "simple_curve"
    assert row["nearest_route_index"] == point.index
    assert row["route_s"] == point.s
    assert row["is_curve_segment"] is True
    assert row["curve_segment_id"] == 0
    assert row["cross_track_error"] == 0.0
    assert row["heading_error"] == 0.0
    assert row["apollo_steer_raw"] == 0.8
    assert row["bridge_steer_mapped"] == 0.2
    assert row["carla_steer_applied"] == 0.19
    assert row["throttle_raw"] == 0.2
    assert row["throttle_mapped"] == 0.18
    assert row["throttle_applied"] == 0.17
    assert row["lateral_guard_applied"] is True
    assert row["trajectory_contract_guard_applied"] is None


def test_missing_route_apollo_and_guard_fields_are_none() -> None:
    row = build_route_curve_row(sim_time=0.1, frame_id=1)

    assert set(ROUTE_CURVE_P0_FIELDS).issubset(row)
    assert row["route_id"] is None
    assert row["route_x"] is None
    assert row["nearest_route_index"] is None
    assert row["cross_track_error"] is None
    assert row["heading_error"] is None
    assert row["apollo_steer_raw"] is None
    assert row["bridge_steer_mapped"] is None
    assert row["carla_steer_applied"] is None
    assert row["lateral_guard_applied"] is None
    assert row["trajectory_contract_guard_applied"] is None


def test_debug_semantics_fill_lateral_heading_without_route_geometry() -> None:
    row = build_route_curve_row(
        sim_time=0.1,
        frame_id=1,
        ego_pose={"x": 1.0, "y": 2.0, "z": 0.0, "heading": 0.5},
        apollo_control={"route_id": "town01_rh_spawn217_goal046", "e_y": -0.42, "e_psi": 0.08, "kappa": 0.04},
    )

    assert row["route_id"] == "town01_rh_spawn217_goal046"
    assert row["route_x"] is None
    assert row["cross_track_error"] == -0.42
    assert row["heading_error"] == 0.08
    assert row["route_curvature"] == 0.04
    assert row["curvature_at_nearest"] == 0.04
    assert row["is_curve_segment"] is True


def test_route_definition_from_scenario_metadata_feeds_route_geometry() -> None:
    route = route_definition_from_metadata(
        {
            "route_id": "town01_rh_spawn217_goal046",
            "map": "Town01",
            "route_trace": [
                {"x": 0.0, "y": 0.0, "z": 0.0, "heading": 0.0, "lane_id": "1:0:-1"},
                {"x": 10.0, "y": 0.0, "z": 0.0, "heading": 0.0, "lane_id": "1:0:-1"},
                {"x": 20.0, "y": 0.0, "z": 0.0, "heading": 0.0, "lane_id": "1:0:-1"},
            ],
        }
    )

    assert route is not None
    row = build_route_curve_row(
        sim_time=0.1,
        frame_id=1,
        route=route,
        ego_pose={"x": 10.0, "y": 0.25, "z": 0.0, "heading": 0.0},
    )

    assert row["route_id"] == "town01_rh_spawn217_goal046"
    assert row["route_x"] == 10.0
    assert row["nearest_route_index"] == 1
    assert row["cross_track_error"] == 0.25
    assert row["heading_error"] == 0.0


def test_p1_p2_constants_exist_but_are_not_forced_into_p0_row() -> None:
    assert "apollo_matched_point_index" in ROUTE_CURVE_P1_FIELDS
    assert "control_latency_ms" in ROUTE_CURVE_P2_FIELDS

    row = build_route_curve_row(sim_time=0.1, frame_id=1)

    assert "apollo_matched_point_index" not in row
    assert "control_latency_ms" not in row


def test_timeseries_recorder_adds_p0_columns_for_plain_rows(tmp_path: Path) -> None:
    path = tmp_path / "timeseries.csv"
    recorder = TimeseriesRecorder(path)
    recorder.write_row({"frame": 1, "t": 0.05})
    recorder.close()

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert set(ROUTE_CURVE_P0_FIELDS).issubset(rows[0])
    assert rows[0]["frame_id"] == ""
    assert rows[0]["route_id"] == ""
    assert rows[0]["apollo_steer_raw"] == ""


def test_artifact_manifest_and_summary_include_route_curve_schema_version() -> None:
    manifest = build_manifest(run_id="case")
    summary = build_summary(success=True, exit_reason="ok", frames=1, sim_duration_s=0.1, wall_duration_s=0.2)

    assert manifest["route_curve_fields_schema_version"] == ROUTE_CURVE_FIELDS_SCHEMA_VERSION
    assert summary["route_curve_fields_schema_version"] == ROUTE_CURVE_FIELDS_SCHEMA_VERSION


def test_route_health_analyzer_reads_p0_lateral_and_heading_fields() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    rows = [
        ensure_route_curve_p0_fields(
            {
                "frame_id": 0,
                "ego_x": 0.0,
                "ego_y": 0.1,
                "cross_track_error": 0.1,
                "heading_error": 0.01,
            }
        ),
        ensure_route_curve_p0_fields(
            {
                "frame_id": 1,
                "ego_x": 10.0,
                "ego_y": -0.2,
                "cross_track_error": -0.2,
                "heading_error": -0.02,
            }
        ),
    ]

    report = analyze_route_health(route, rows)

    assert report["missing_inputs"] == []
    assert report["run_metrics"]["lateral_error_mean_m"] == 0.15000000000000002
    assert report["run_metrics"]["heading_error_mean_rad"] == 0.015
