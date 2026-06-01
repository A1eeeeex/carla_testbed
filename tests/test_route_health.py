from __future__ import annotations

import csv
from pathlib import Path

from carla_testbed.analysis.route_health import analyze_route_health
from carla_testbed.routes.geometry import project_onto_route
from carla_testbed.routes.io import load_route_json


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _read_timeseries(name: str) -> list[dict[str, str]]:
    with (FIXTURES / "timeseries" / name).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_simple_straight_route_geometry_length_and_spacing() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    report = analyze_route_health(route)

    geometry = report["route_geometry"]

    assert report["schema_version"] == "route_health.v1"
    assert geometry["point_count"] == 4
    assert geometry["length_m"] == 30.0
    assert geometry["spacing"]["mean_m"] == 10.0
    assert geometry["spacing"]["p95_m"] == 10.0
    assert geometry["spacing"]["max_m"] == 10.0
    assert report["missing_inputs"] == ["timeseries"]
    assert report["verdict"]["status"] == "diagnostic_incomplete"


def test_simple_curve_route_geometry_has_curvature() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_curve.json")
    report = analyze_route_health(route)

    curvature = report["route_geometry"]["curvature"]

    assert curvature["mean_abs"] is not None
    assert curvature["p95_abs"] is not None
    assert curvature["max_abs"] > 0.01


def test_route_projection_interpolates_segment_s_and_allows_negative_start_extension() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")

    middle = project_onto_route(route.points, 5.0, 1.0)
    before_start = project_onto_route(route.points, -2.0, 1.0, extend_ends=True)

    assert middle is not None
    assert middle.segment_start_index == 0
    assert middle.segment_end_index == 1
    assert middle.s == 5.0
    assert middle.x == 5.0
    assert middle.y == 0.0
    assert middle.cross_track_error == 1.0
    assert before_start is not None
    assert before_start.s == -2.0
    assert before_start.x == -2.0
    assert before_start.cross_track_error == 1.0


def test_minimal_timeseries_lateral_and_heading_stats() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    report = analyze_route_health(route, _read_timeseries("route_health_minimal.csv"))

    metrics = report["run_metrics"]
    apollo = report["apollo_semantics"]
    control = report["control_semantics"]

    assert metrics["lateral_error_mean_m"] == 0.25
    assert metrics["lateral_error_max_m"] == 0.4
    assert round(metrics["lateral_error_p95_m"], 3) == 0.385
    assert round(metrics["heading_error_mean_rad"], 3) == 0.025
    assert metrics["heading_error_max_rad"] == 0.04
    assert metrics["ego_speed_mean_mps"] == 1.5
    assert round(metrics["ego_speed_p95_mps"], 3) == 2.85
    assert metrics["ego_speed_max_mps"] == 3.0
    assert metrics["stopped_ratio"] == 0.25
    assert round(metrics["ego_yaw_rate_abs_p95_rad_s"], 3) == 0.037
    assert round(metrics["throttle_applied_p95"], 3) == 0.285
    assert round(metrics["brake_applied_p95"], 3) == 0.355
    assert metrics["brake_throttle_conflict_frames"] == 1
    assert "straight" in metrics["lateral_error_by_curvature_bucket"]
    assert apollo["matched_point_anomaly_locations"] == [2]
    assert apollo["target_point_anomaly_locations"] == [3]
    assert apollo["first_high_steer"] == {"seq": 2, "value": 0.96}
    assert control["raw_mapped_applied_steer_available"] is True
    assert control["guard_apply_counts"]["lateral_guard"] == 1
    assert report["missing_inputs"] == []
    assert report["verdict"]["status"] == "diagnostic_ready"


def test_spawn_alignment_accepts_yaw_degrees() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    route.spawn_pose = {"x": 0.0, "y": 0.0, "yaw_deg": 0.0}

    report = analyze_route_health(route)

    alignment = report["route_geometry"]["spawn_alignment"]
    assert alignment["heading_error_rad"] == 0.0
    assert "spawn_pose.yaw" not in report["missing_fields"]


def test_missing_yaw_rate_can_be_derived_from_ego_heading() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    rows = [
        {
            "sim_time": "0.0",
            "ego_heading": "0.0",
            "lateral_error": "0.0",
            "heading_error": "0.0",
            "ego_speed": "1.0",
            "throttle_raw": "0.2",
            "throttle_mapped": "0.2",
            "throttle_applied": "0.2",
            "brake_raw": "0.0",
            "brake_mapped": "0.0",
            "brake_applied": "0.0",
        },
        {
            "sim_time": "1.0",
            "ego_heading": "0.1",
            "lateral_error": "0.1",
            "heading_error": "0.01",
            "ego_speed": "1.0",
            "throttle_raw": "0.2",
            "throttle_mapped": "0.2",
            "throttle_applied": "0.2",
            "brake_raw": "0.0",
            "brake_mapped": "0.0",
            "brake_applied": "0.0",
        },
    ]

    report = analyze_route_health(route, rows)

    assert round(report["run_metrics"]["ego_yaw_rate_abs_p95_rad_s"], 6) == 0.1
    assert report["run_metrics"]["ego_yaw_rate_source"] == "derived_from_ego_heading"
    assert "ego_yaw_rate" not in report["missing_fields"]


def test_curvature_bucket_prefers_timeseries_route_curvature() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    rows = [
        {
            "sim_time": "0.0",
            "ego_x": "999.0",
            "ego_y": "999.0",
            "ego_heading": "0.0",
            "route_curvature": "0.0",
            "lateral_error": "0.1",
            "heading_error": "0.01",
            "ego_speed": "1.0",
            "ego_yaw_rate": "0.0",
            "throttle_raw": "0.2",
            "throttle_mapped": "0.2",
            "throttle_applied": "0.2",
            "brake_raw": "0.0",
            "brake_mapped": "0.0",
            "brake_applied": "0.0",
        }
    ]

    report = analyze_route_health(route, rows)

    assert "straight" in report["run_metrics"]["lateral_error_by_curvature_bucket"]
    assert "unknown" not in report["run_metrics"]["lateral_error_by_curvature_bucket"]


def test_missing_apollo_fields_graceful_degrade() -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_straight.json")
    rows = [
        {"frame": "0", "x": "0.0", "y": "0.0"},
        {"frame": "1", "x": "1.0", "y": "0.0"},
    ]

    report = analyze_route_health(route, rows)

    assert "lateral_error" in report["missing_fields"]
    assert "heading_error" in report["missing_fields"]
    assert "matched_point" in report["missing_fields"]
    assert "target_point" in report["missing_fields"]
    assert "apollo_raw_steer" in report["missing_fields"]
    assert "ego_speed" in report["missing_fields"]
    assert "throttle_applied" in report["missing_fields"]
    assert report["run_metrics"]["lateral_error_mean_m"] is None
    assert report["run_metrics"]["ego_speed_mean_mps"] is None
    assert report["run_metrics"]["brake_throttle_conflict_frames"] is None
    assert report["apollo_semantics"]["matched_point_anomaly_locations"] == []
    assert report["apollo_semantics"]["target_point_anomaly_locations"] == []
    assert report["apollo_semantics"]["first_high_steer"] is None
    assert report["verdict"]["status"] == "diagnostic_ready"
