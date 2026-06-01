from __future__ import annotations

import csv
import json
from pathlib import Path

import yaml

from carla_testbed.analysis.artifact_completeness import check_run_artifact_completeness
from carla_testbed.analysis.run_artifact_standardizer import standardize_legacy_run_artifacts


def _write_legacy_route_health_run(root: Path) -> None:
    (root / "artifacts").mkdir(parents=True)
    (root / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "lane_keep_097",
                "route_id": "lane097",
                "runtime_contract": {"status": "aligned"},
                "routing_materialized": True,
                "planning_materialized": True,
                "control_handoff_status": "control_consuming_with_nonzero_planning",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "effective.yaml").write_text(
        yaml.safe_dump(
            {
                "run": {
                    "map": "Town01",
                    "capability_profile": "lane_keep",
                    "profile_name": "lane_keep_097",
                },
                "scenario": {
                    "publish_ros2_gt": True,
                    "gt": {"publish_odom": True},
                },
                "algo": {
                    "stack": "apollo",
                    "apollo": {
                        "algorithm_variant_id": "apollo_10_0_carla_gt_town01_reference",
                        "algorithm_variant_manifest_path": "configs/algorithms/apollo_variant.carla_gt.example.yaml",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (root / "artifacts" / "scenario_metadata.json").write_text(
        json.dumps(
            {
                "route_id": "lane097",
                "map": "Town01",
                "capability_profile": "lane_keep",
                "route_trace_source": "town01_forward_waypoint_trace",
                "route_trace": [
                    {"index": 0, "x": 0.0, "y": 0.0, "z": 0.0, "s": 0.0, "heading": 0.0},
                    {"index": 1, "x": 10.0, "y": 0.0, "z": 0.0, "s": 10.0, "heading": 0.0},
                    {"index": 2, "x": 20.0, "y": 0.0, "z": 0.0, "s": 20.0, "heading": 0.0},
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "artifacts" / "town01_route_health_state_timeline.jsonl").write_text(
        json.dumps({"state": "ROUTING_READY", "entered_ts_sec": 1.0, "reason": "routing_request_sent"})
        + "\n",
        encoding="utf-8",
    )
    with (root / "artifacts" / "debug_timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ts_sec",
                "map_x",
                "map_y",
                "map_yaw_deg",
                "speed_mps",
                "e_y_m",
                "e_psi_deg",
                "target_curvature",
                "apollo_desired_throttle",
                "apollo_desired_brake",
                "apollo_desired_steer",
                "mapped_throttle_cmd",
                "mapped_brake_cmd",
                "mapped_carla_steer_cmd",
                "measured_throttle",
                "measured_brake",
                "measured_steer",
                "sustained_lateral_guard_applied",
                "trajectory_contract_lateral_guard_applied",
                "apollo_debug_simple_lat_target_point_s",
                "apollo_debug_simple_lat_target_point_x",
                "apollo_debug_simple_lat_target_point_y",
                "apollo_debug_simple_lat_target_point_theta_rad",
                "apollo_debug_simple_lat_target_point_kappa",
                "apollo_debug_simple_lon_matched_point_s",
                "apollo_debug_simple_lon_matched_point_x",
                "apollo_debug_simple_lon_matched_point_y",
                "localization_timestamp",
                "chassis_timestamp",
                "planning_header_timestamp_sec_used",
                "control_rx_timestamp",
                "control_timestamp",
                "control_latency_ms",
                "latest_planning_msg_age_ms",
                "control_message_age_ms",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ts_sec": "1.0",
                "map_x": "10.0",
                "map_y": "20.0",
                "map_yaw_deg": "90.0",
                "speed_mps": "3.0",
                "e_y_m": "0.15",
                "e_psi_deg": "2.0",
                "target_curvature": "0.01",
                "apollo_desired_throttle": "0.2",
                "apollo_desired_brake": "0.0",
                "apollo_desired_steer": "0.8",
                "mapped_throttle_cmd": "0.2",
                "mapped_brake_cmd": "0.0",
                "mapped_carla_steer_cmd": "0.2",
                "measured_throttle": "0.19",
                "measured_brake": "0.0",
                "measured_steer": "0.18",
                "sustained_lateral_guard_applied": "false",
                "trajectory_contract_lateral_guard_applied": "false",
                "apollo_debug_simple_lat_target_point_s": "12.5",
                "apollo_debug_simple_lat_target_point_x": "13.0",
                "apollo_debug_simple_lat_target_point_y": "24.0",
                "apollo_debug_simple_lat_target_point_theta_rad": "1.57",
                "apollo_debug_simple_lat_target_point_kappa": "0.012",
                "apollo_debug_simple_lon_matched_point_s": "9.8",
                "apollo_debug_simple_lon_matched_point_x": "10.5",
                "apollo_debug_simple_lon_matched_point_y": "20.0",
                "localization_timestamp": "1.0",
                "chassis_timestamp": "1.0",
                "planning_header_timestamp_sec_used": "0.92",
                "control_rx_timestamp": "1.01",
                "control_timestamp": "1.02",
                "control_latency_ms": "10.0",
                "latest_planning_msg_age_ms": "100.0",
                "control_message_age_ms": "40.0",
            }
        )


def test_standardizer_derives_standard_aliases_from_legacy_route_health_run(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_lane_keep_097"
    _write_legacy_route_health_run(run_dir)

    report = standardize_legacy_run_artifacts(run_dir)

    assert report["status"] == "updated"
    assert (run_dir / "manifest.json").is_file()
    assert (run_dir / "route.json").is_file()
    assert (run_dir / "config.resolved.yaml").is_file()
    assert (run_dir / "events.jsonl").is_file()
    assert (run_dir / "timeseries.csv").is_file()

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["scenario_class"] == "lane_keep"
    assert manifest["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert manifest["algorithm_variant_manifest_path"] == "configs/algorithms/apollo_variant.carla_gt.example.yaml"
    assert manifest["transport_mode"] == "ros2_gt"
    assert manifest["backend"] == "apollo_cyberrt"
    assert manifest["truth_input"] is True
    route = json.loads((run_dir / "route.json").read_text(encoding="utf-8"))
    assert route["route_id"] == "lane097"
    assert route["map"] == "Town01"
    assert len(route["points"]) == 3

    with (run_dir / "timeseries.csv").open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["sim_time"] == "1.0"
    assert row["ego_x"] == "10.0"
    assert row["nearest_route_index"] == "1"
    assert row["route_s"] == "10.0"
    assert row["route_x"] == "10.0"
    assert row["route_y"] == "0.0"
    assert row["heading_error"]
    assert row["apollo_steer_raw"] == "0.8"
    assert row["bridge_steer_mapped"] == "0.2"
    assert row["carla_steer_applied"] == "0.18"
    assert row["throttle_raw"] == "0.2"
    assert row["brake_applied"] == "0.0"
    assert row["apollo_trajectory_heading"] == "1.57"
    assert row["apollo_trajectory_curvature"] == "0.012"
    assert row["apollo_target_point_s"] == "12.5"
    assert row["apollo_target_point_distance"] == "5.0"
    assert row["apollo_matched_point_distance"] == "0.5"
    assert row["apollo_matched_point_s"] == "9.8"
    assert row["localization_timestamp"] == "1.0"
    assert row["chassis_timestamp"] == "1.0"
    assert row["planning_timestamp"] == "0.92"
    assert row["control_rx_timestamp"] == "1.01"
    assert row["control_timestamp"] == "1.02"
    assert row["control_latency_ms"] == "10.0"
    assert row["planning_message_age_ms"] == "100.0"
    assert row["control_message_age_ms"] == "40.0"


def test_standardizer_mirrors_route_context_for_apollo_debug_y_axis(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_lane_keep_097"
    _write_legacy_route_health_run(run_dir)
    meta_path = run_dir / "artifacts" / "scenario_metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["spawn"] = {"x": 0.0, "y": 10.0, "yaw_deg": -90.0}
    meta["route_trace"] = [
        {"index": 0, "x": 0.0, "y": 10.0, "z": 0.0, "s": 0.0, "heading": -1.57079632679},
        {"index": 1, "x": 0.0, "y": 5.0, "z": 0.0, "s": 5.0, "heading": -1.57079632679},
        {"index": 2, "x": 0.0, "y": 0.0, "z": 0.0, "s": 10.0, "heading": -1.57079632679},
    ]
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    debug_path = run_dir / "artifacts" / "debug_timeseries.csv"
    rows = []
    with debug_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["map_x"] = "0.0"
    rows[0]["map_y"] = "-5.1"
    with debug_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    standardize_legacy_run_artifacts(run_dir)

    with (run_dir / "timeseries.csv").open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["nearest_route_index"] == "1"
    assert round(float(row["route_s"]), 3) == 4.9
    assert round(float(row["route_y"]), 3) == -5.1
    assert round(float(row["route_heading"]), 3) == 1.571


def test_standardizer_uses_segment_projection_for_route_s_before_first_point(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_lane_keep_097"
    _write_legacy_route_health_run(run_dir)
    debug_path = run_dir / "artifacts" / "debug_timeseries.csv"
    rows = []
    with debug_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["map_x"] = "-2.0"
    rows[0]["map_y"] = "1.0"
    rows[0]["map_yaw_deg"] = "0.0"
    rows[0]["e_y_m"] = ""
    with debug_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    standardize_legacy_run_artifacts(run_dir)

    with (run_dir / "timeseries.csv").open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["nearest_route_index"] == "0"
    assert round(float(row["route_s"]), 3) == -2.0
    assert round(float(row["route_x"]), 3) == -2.0
    assert round(float(row["route_y"]), 3) == 0.0
    assert round(float(row["cross_track_error"]), 3) == 1.0
    assert row["route_projection_t"] == "-0.2"


def test_standardized_run_no_longer_reports_basic_artifact_aliases_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_lane_keep_097"
    _write_legacy_route_health_run(run_dir)
    standardize_legacy_run_artifacts(run_dir)

    report = check_run_artifact_completeness(run_dir, scenario_class="lane_keep")

    assert "config.resolved.yaml" not in report["missing_artifacts"]
    assert "events.jsonl" not in report["missing_artifacts"]
    assert "timeseries.csv" not in report["missing_artifacts"]
    assert report["missing_control_trace_fields"] == []


def test_standardizer_derives_summary_safety_events(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_lane_keep_097"
    _write_legacy_route_health_run(run_dir)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "exit_reason": "LANE_INVASION",
            "first_failure_step": 0,
            "lane_invasion_count": 1,
        }
    )
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    standardize_legacy_run_artifacts(run_dir)

    events = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    lane_event = next(event for event in events if event["event_type"] == "lane_invasion")
    assert lane_event["step"] == 0
    assert lane_event["context_source"] == "timeseries.csv:row_index_from_summary_step"
    assert lane_event["lane_invasion_count"] == 1
    assert lane_event["ego_speed"] == 3.0
    assert lane_event["route_s"] == 10.0
    assert lane_event["nearest_route_index"] == 1
    assert lane_event["cross_track_error"] == 0.15


def test_standardized_legacy_run_can_generate_route_health_from_metadata_route(tmp_path: Path) -> None:
    from carla_testbed.analysis.route_health_report import analyze_route_health_run_dir

    run_dir = tmp_path / "legacy_lane_keep_097"
    _write_legacy_route_health_run(run_dir)
    standardize_legacy_run_artifacts(run_dir)

    result = analyze_route_health_run_dir(run_dir)
    report = result["report"]

    assert report["route_id"] == "lane097"
    assert report["map_name"] == "Town01"
    assert "route" not in report["missing_inputs"]
    assert Path(result["outputs"]["route_health_json"]).is_file()
    assert Path(result["outputs"]["route_health_csv"]).is_file()
    assert Path(result["outputs"]["curve_segments_csv"]).is_file()
