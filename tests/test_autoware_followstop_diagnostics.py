import csv
import json
from pathlib import Path

from carla_testbed.analysis.autoware_followstop import (
    analyze_followstop_run,
    write_followstop_report,
)


def _write_run(
    root: Path,
    *,
    ego_spawn: dict,
    front_spawn: dict,
    min_lead_distance_m: float,
    final_speed_mps: float,
) -> Path:
    run = root / "run"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "scenario_metadata.json").write_text(
        json.dumps(
            {
                "ego_idx": 1,
                "front_idx": 2,
                "spawn": ego_spawn,
                "front_spawn": front_spawn,
            }
        )
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "frames": 2,
                "exit_reason": "max_steps_reached",
                "metrics": {
                    "max_speed_mps": 12.0,
                    "min_lead_distance_m": min_lead_distance_m,
                },
            }
        )
    )
    with (run / "timeseries.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ego_x", "ego_y", "ego_z", "ego_heading", "ego_speed"],
        )
        writer.writeheader()
        writer.writerow({"ego_x": ego_spawn["x"], "ego_y": ego_spawn["y"], "ego_z": 0, "ego_heading": 0, "ego_speed": 0})
        writer.writerow({"ego_x": 95, "ego_y": 0, "ego_z": 0, "ego_heading": 0, "ego_speed": final_speed_mps})
    return run


def test_followstop_diagnostics_flags_front_lateral_misalignment(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 0, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 0, "y": 100, "z": 0, "yaw_deg": 90},
        min_lead_distance_m=100.0,
        final_speed_mps=10.0,
    )

    report = analyze_followstop_run(run)

    assert report["status"] == "fail"
    assert "front_not_ahead_of_ego" in report["failure_reasons"]
    assert "front_lateral_misaligned" in report["failure_reasons"]
    assert report["verdict"]["is_followstop_evidence"] is False


def test_followstop_diagnostics_passes_when_front_ahead_and_stopped(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 0, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 100, "y": 0.5, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=8.0,
        final_speed_mps=0.2,
    )

    report = analyze_followstop_run(run)

    assert report["status"] == "pass"
    assert report["failure_reasons"] == []
    assert report["verdict"]["is_followstop_evidence"] is True


def test_followstop_diagnostics_warns_when_stop_zone_not_reached(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 0, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 100, "y": 0, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=60.0,
        final_speed_mps=8.0,
    )

    report = analyze_followstop_run(run)

    assert report["status"] == "warn"
    assert "front_stop_zone_not_reached" in report["failure_reasons"]
    assert report["verdict"]["is_followstop_evidence"] is False


def test_followstop_diagnostics_classifies_near_ego_planning_stop(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 100, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 300, "y": 0, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=190.0,
        final_speed_mps=0.1,
    )
    artifacts = run / "artifacts"
    (artifacts / "ros2_topic__planning__scenario_planning__lane_driving__behavior_planning__path_with_lane_id.jsonl").write_text(
        json.dumps(
            {
                "stamp": {"sec": 1, "nanosec": 0},
                "trajectory_point_count": 10,
                "trajectory_velocity_mps": {
                    "min": 0.0,
                    "max": 22.22,
                    "mean": 4.4,
                    "first": 22.22,
                    "last": 0.0,
                },
                "first_stop_velocity_indices": [3],
                "trajectory_points_sample": [
                    {
                        "index": 3,
                        "pose": {"x": 108.0, "y": 0.0, "z": 0.0},
                        "longitudinal_velocity_mps": 0.0,
                    }
                ],
            }
        )
        + "\n"
    )
    with (run / "timeseries.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["sim_time", "ego_x", "ego_y", "ego_z", "ego_heading", "ego_speed"],
        )
        writer.writeheader()
        writer.writerow({"sim_time": 1.0, "ego_x": 100, "ego_y": 0, "ego_z": 0, "ego_heading": 0, "ego_speed": 3.0})
        writer.writerow({"sim_time": 2.0, "ego_x": 110, "ego_y": 0, "ego_z": 0, "ego_heading": 0, "ego_speed": 0.1})

    report = analyze_followstop_run(run)

    assert report["status"] == "warn"
    assert "front_stop_zone_not_reached" in report["failure_reasons"]
    assert "planning_stop_near_ego_before_front_stop_zone" in report["failure_reasons"]
    profile = report["planning_speed_profile"]
    assert profile["classification"] == "planning_stop_near_ego"
    assert profile["first_near_ego_stop_event"]["topic"] == "behavior_path_with_lane_id"
    assert profile["first_near_ego_stop_event"]["ego_to_stop_abs_dx_m"] == 8.0
    assert "causal_stop_reason_topics_missing_or_unusable" in report["warnings"]


def test_followstop_diagnostics_keeps_obstacle_stop_factor_separate_from_near_stop(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 100, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 300, "y": 0, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=190.0,
        final_speed_mps=0.1,
    )
    artifacts = run / "artifacts"
    (artifacts / "ros2_topic__planning__scenario_planning__lane_driving__behavior_planning__path_with_lane_id.jsonl").write_text(
        json.dumps(
            {
                "stamp": {"sec": 1, "nanosec": 0},
                "trajectory_point_count": 10,
                "trajectory_velocity_mps": {"min": 0.0, "max": 22.22, "mean": 4.4, "first": 22.22, "last": 0.0},
                "first_stop_velocity_indices": [3],
                "trajectory_points_sample": [
                    {"index": 3, "pose": {"x": 108.0, "y": 0.0, "z": 0.0}, "longitudinal_velocity_mps": 0.0}
                ],
            }
        )
        + "\n"
    )
    (artifacts / "ros2_topic__planning__planning_factors__obstacle_stop.jsonl").write_text(
        json.dumps(
            {
                "stamp": {"sec": 1, "nanosec": 0},
                "planning_factor_count": 1,
                "planning_factors_sample": [
                    {
                        "module": "obstacle_stop",
                        "behavior": 3,
                        "control_points_sample": [
                            {"distance": 188.0, "pose": {"x": 300.0, "y": 0.0, "z": 0.0}}
                        ],
                    }
                ],
            }
        )
        + "\n"
    )
    with (run / "timeseries.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["sim_time", "ego_x", "ego_y", "ego_z", "ego_heading", "ego_speed"],
        )
        writer.writeheader()
        writer.writerow({"sim_time": 1.0, "ego_x": 100, "ego_y": 0, "ego_z": 0, "ego_heading": 0, "ego_speed": 3.0})

    report = analyze_followstop_run(run)

    assert "planning_stop_near_ego_before_front_stop_zone" in report["failure_reasons"]
    assert "near_ego_stop_not_explained_by_obstacle_stop_factor" in report["warnings"]
    factor_profile = report["planning_factor_distance"]
    assert factor_profile["obstacle_stop_near_ego"] is False
    assert factor_profile["topics"]["obstacle_stop"]["min_control_point_distance_m"] == 188.0


def test_followstop_diagnostics_uses_first_timeseries_row_for_initial_pose(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 95, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 100, "y": 0, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=5.0,
        final_speed_mps=0.2,
    )
    with (run / "timeseries.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ego_x", "ego_y", "ego_z", "ego_heading", "ego_speed"],
        )
        writer.writeheader()
        writer.writerow({"ego_x": 0, "ego_y": 0, "ego_z": 0, "ego_heading": 0, "ego_speed": 0})
        writer.writerow({"ego_x": 95, "ego_y": 0, "ego_z": 0, "ego_heading": 0, "ego_speed": 0.2})

    report = analyze_followstop_run(run)

    assert report["scene"]["initial_ego_pose_source"] == "timeseries.first_row"
    assert report["scene"]["initial_front_relative_to_ego"]["longitudinal_m"] == 100
    assert report["scene"]["final_front_relative_to_ego"]["longitudinal_m"] == 5
    assert report["status"] == "pass"


def test_followstop_diagnostics_rejects_origin_front_spawn_placeholder(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 95, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 0, "y": 0, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=5.0,
        final_speed_mps=0.2,
    )

    report = analyze_followstop_run(run)

    assert report["status"] == "insufficient_data"
    assert "front_spawn_origin_placeholder" in report["failure_reasons"]
    assert report["verdict"] == {}


def test_followstop_report_writer_creates_json_and_markdown(tmp_path):
    run = _write_run(
        tmp_path,
        ego_spawn={"x": 0, "y": 0, "z": 0, "yaw_deg": 0},
        front_spawn={"x": 100, "y": 0, "z": 0, "yaw_deg": 0},
        min_lead_distance_m=60.0,
        final_speed_mps=8.0,
    )
    report = analyze_followstop_run(run)

    paths = write_followstop_report(report, tmp_path / "out")

    assert Path(paths["json"]).exists()
    assert "Autoware Follow-Stop Diagnostics" in Path(paths["markdown"]).read_text()
