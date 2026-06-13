from __future__ import annotations

import csv
import json
import math
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

from carla_testbed.analysis.localization_contract import (
    REPORT_SCHEMA_VERSION,
    analyze_localization_contract,
    analyze_localization_contract_files,
    write_localization_contract_report,
)
from carla_testbed.adapters.apollo.frame_transform import load_frame_transform
from carla_testbed.adapters.apollo.vehicle_reference import load_vehicle_reference

FIXTURE_DIR = Path("tests/fixtures/localization_contract")
TIMESERIES = FIXTURE_DIR / "complete_timeseries.csv"
CHANNEL_STATS = FIXTURE_DIR / "channel_stats.json"
ROUTE_HEALTH = FIXTURE_DIR / "route_health.json"
CYBER_BRIDGE_STATS = FIXTURE_DIR / "cyber_bridge_stats.json"
ROS2_GT_LIVE_STATS = FIXTURE_DIR / "ros2_gt_live_stats.json"
FRAME_TRANSFORM = Path("configs/town01/apollo_frame_transform.example.yaml")
VEHICLE_REFERENCE = FIXTURE_DIR / "vehicle_reference_verified.yaml"


def test_complete_fixture_passes_and_writes_report(tmp_path: Path) -> None:
    report = _analyze_fixture()
    outputs = write_localization_contract_report(report, tmp_path)

    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["verdict"]["status"] == "pass"
    assert report["channel"]["status"] == "pass"
    assert report["reference_point"]["position_uses_vrp"] is True
    assert report["reference_point"]["confidence"] == "verified"
    assert report["pose_consistency"]["heading_error_to_route_p95_rad"] < 0.03
    assert Path(outputs["localization_contract_report"]).is_file()
    assert Path(outputs["localization_contract_summary"]).read_text(encoding="utf-8").startswith(
        "# Apollo Localization Contract Summary"
    )


def test_acceptance_checklist_answers_final_validation_questions() -> None:
    report = _analyze_fixture()
    checklist = report["acceptance_checklist"]

    assert set(checklist) == {
        "localization_channel_health",
        "sim_time_time_base",
        "frame_transform_configured",
        "position_uses_vrp",
        "source_to_published_reference_explained",
        "heading_from_transformed_forward",
        "rfu_to_enu_orientation",
        "decoded_orientation_consistency",
        "kinematics_frame_and_units",
        "chassis_speed_consistency",
        "lane_projection",
        "reference_line_projection",
        "acceleration_semantics",
        "uncertainty_status_policy",
        "natural_driving_gate_ready",
    }
    assert checklist["reference_line_projection"]["status"] == "insufficient_data"
    assert all(
        item["status"] == "pass"
        for key, item in checklist.items()
        if key != "reference_line_projection"
    )
    assert all(item["question"] for item in checklist.values())
    assert all(item["evidence_fields"] for item in checklist.values())


def test_apollo_hdmap_projection_passes_reference_line_projection_check() -> None:
    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        hdmap_projection_rows=_hdmap_projection_rows(heading_error_rad=0.01, lateral_error_m=0.05),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "pass"
    assert report["apollo_hdmap_projection"]["status"] == "pass"
    assert report["apollo_hdmap_projection"]["claim_grade"] is True
    assert report["acceptance_checklist"]["reference_line_projection"]["status"] == "pass"
    assert report["pose_consistency"]["heading_error_to_apollo_hdmap_lane_p95_rad"] < 0.05


def test_apollo_hdmap_projection_missing_keeps_reference_line_insufficient() -> None:
    report = _analyze_fixture()

    assert report["apollo_hdmap_projection"]["status"] == "insufficient_data"
    assert report["apollo_hdmap_projection"]["available"] is False
    assert report["acceptance_checklist"]["reference_line_projection"]["status"] == "insufficient_data"


def test_apollo_hdmap_projection_high_heading_error_blocks_claim() -> None:
    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        hdmap_projection_rows=_hdmap_projection_rows(heading_error_rad=0.35, lateral_error_m=0.05),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "fail"
    assert "apollo_hdmap_projection_heading_error_high" in report["verdict"]["blocking_reasons"]
    assert report["acceptance_checklist"]["reference_line_projection"]["status"] == "fail"
    assert "lane_direction" in report["apollo_hdmap_projection"]["suspected_failure_layers"]


def test_hdmap_lateral_error_can_be_attributed_to_route_cross_track_drift() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["cross_track_error"] = "-0.80"
        row["route_s"] = str(float(row["sim_time"]) * 10.0)

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        hdmap_projection_rows=_hdmap_projection_rows(heading_error_rad=0.01, lateral_error_m=0.80),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    consistency = report["hdmap_route_lateral_consistency"]
    assert report["verdict"]["status"] == "fail"
    assert "apollo_hdmap_projection_lateral_error_high" in report["verdict"]["blocking_reasons"]
    assert consistency["status"] == "pass"
    assert consistency["alignment_mode"] == "opposite_sign"
    assert consistency["best_abs_delta_p95_m"] == pytest.approx(0.0)
    assert consistency["interpretation"] == "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"


def test_checklist_missing_fields_do_not_fabricate_pass() -> None:
    report = analyze_localization_contract(
        timeseries_rows=[],
        channel_stats=None,
        route_health=None,
        frame_transform=None,
        vehicle_reference=None,
    )
    checklist = report["acceptance_checklist"]

    assert report["verdict"]["status"] == "insufficient_data"
    assert checklist["localization_channel_health"]["status"] == "insufficient_data"
    assert checklist["frame_transform_configured"]["status"] == "insufficient_data"
    assert checklist["position_uses_vrp"]["status"] == "insufficient_data"
    assert checklist["lane_projection"]["status"] == "insufficient_data"
    assert checklist["reference_line_projection"]["status"] == "insufficient_data"
    assert checklist["natural_driving_gate_ready"]["status"] == "insufficient_data"


def test_checklist_flags_chassis_speed_mismatch() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["chassis_speed_mps"] = "12.0"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "fail"
    assert "velocity_chassis_speed_mismatch_high" in report["verdict"]["blocking_reasons"]
    assert report["acceptance_checklist"]["chassis_speed_consistency"]["status"] == "fail"


def test_missing_channel_stats_is_insufficient_data() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        for key in [
            "localization_header_timestamp_sec",
            "localization_measurement_time",
            "measurement_time",
            "localization_timestamp",
        ]:
            row.pop(key, None)
    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=None,
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["channel"]["status"] == "insufficient_data"
    assert report["verdict"]["status"] == "insufficient_data"
    assert "channel_stats" in report["missing_fields"]


def test_debug_timeseries_channel_stats_distinguish_publish_and_fresh_samples() -> None:
    rows = []
    for sequence, timestamp in enumerate([0.0, 0.0, 0.05, 0.05, 0.10]):
        rows.append(
            {
                "localization_header_timestamp_sec": str(timestamp),
                "localization_measurement_time": str(timestamp),
                "localization_sequence_num": str(sequence),
                "localization_time_base": "sim_time",
                "measurement_time": str(timestamp),
                "localization_heading": "0.0",
                "localization_orientation_qx": "0.0",
                "localization_orientation_qy": "0.0",
                "localization_orientation_qz": "-0.7071067811865475",
                "localization_orientation_qw": "0.7071067811865476",
                "heading_source": "transformed_forward_vector",
                "orientation_convention": "RFU_to_ENU",
                "localization_speed_mps": "5.0",
                "chassis_speed_mps": "5.0",
                "lane_inside": "true",
                "lane_dist_m": "0.1",
                "e_y_m": "0.02",
                "e_psi_deg": "0.5",
                "angular_velocity_unit": "rad_per_s",
                "linear_acceleration_available": "true",
                "linear_acceleration_vrf_available": "true",
                "angular_velocity_vrf_available": "true",
                "acceleration_source": "carla_actor",
                "uncertainty": "available",
                "msf_status": "OK",
                "sensor_status": "OK",
            }
        )

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=None,
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    channel = report["channel"]
    assert channel["publish_message_count"] == 5
    assert channel["fresh_sample_count"] == 3
    assert channel["unique_timestamp_count"] == 3
    assert channel["duplicate_timestamp_count"] == 2
    assert channel["timestamp_non_decreasing"] is True
    assert channel["timestamp_strictly_increasing"] is False
    assert channel["stale_republish_detected"] is True
    assert channel["stale_republish_status"] == "policy_ok"
    assert channel["republish_policy"] == "stale_republish_allowed_when_fresh_sample_rate_ok"
    assert "duplicate_localization_timestamps_detected" in report["warnings"]
    assert report["verdict"]["status"] == "fail"
    assert "duplicate_localization_timestamps_claim_grade_blocked" in report["verdict"]["blocking_reasons"]


def test_duplicate_timestamps_allowed_for_non_claim_debug_summary() -> None:
    rows = []
    for sequence, timestamp in enumerate([0.0, 0.0, 0.05, 0.05, 0.10]):
        rows.append(
            {
                "localization_header_timestamp_sec": str(timestamp),
                "localization_measurement_time": str(timestamp),
                "localization_sequence_num": str(sequence),
                "localization_time_base": "sim_time",
                "measurement_time": str(timestamp),
                "localization_heading": "0.0",
                "localization_frame_id": "map",
                "localization_orientation_qx": "0.0",
                "localization_orientation_qy": "0.0",
                "localization_orientation_qz": "-0.7071067811865475",
                "localization_orientation_qw": "0.7071067811865476",
                "heading_source": "transformed_forward_vector",
                "orientation_convention": "RFU_to_ENU",
                "localization_speed_mps": "5.0",
                "chassis_speed_mps": "5.0",
                "lane_inside": "true",
                "lane_dist_m": "0.1",
                "e_y_m": "0.02",
                "e_psi_deg": "0.5",
                "angular_velocity_unit": "rad_per_s",
                "linear_acceleration_available": "true",
                "linear_acceleration_vrf_available": "true",
                "angular_velocity_vrf_available": "true",
                "acceleration_source": "carla_actor",
                "uncertainty": "available",
                "msf_status": "OK",
                "sensor_status": "OK",
            }
        )

    source = _runtime_reference_source()
    source["claim_grade"] = False
    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=None,
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=source,
    )

    assert report["channel"]["stale_republish_status"] == "policy_ok"
    assert report["verdict"]["status"] == "warn"
    assert "duplicate_localization_timestamps_claim_grade_blocked" not in report["verdict"]["blocking_reasons"]


def test_yaw_rate_can_be_checked_against_heading_finite_difference() -> None:
    rows = []
    for sequence, (timestamp, heading) in enumerate([(0.0, 0.0), (0.05, 0.01), (0.10, 0.02)]):
        rows.append(
            {
                "localization_header_timestamp_sec": str(timestamp),
                "localization_measurement_time": str(timestamp),
                "localization_sequence_num": str(sequence),
                "localization_time_base": "sim_time",
                "measurement_time": str(timestamp),
                "localization_heading": str(heading),
                "localization_orientation_qx": "0.0",
                "localization_orientation_qy": "0.0",
                "localization_orientation_qz": "-0.7071067811865475",
                "localization_orientation_qw": "0.7071067811865476",
                "heading_source": "transformed_forward_vector",
                "orientation_convention": "RFU_to_ENU",
                "ego_yaw_rate_rad_s": "0.2",
                "localization_speed_mps": "5.0",
                "chassis_speed_mps": "5.0",
                "lane_inside": "true",
                "lane_dist_m": "0.1",
                "e_y_m": "0.02",
                "e_psi_deg": "0.5",
                "angular_velocity_unit": "rad_per_s",
                "linear_acceleration_available": "true",
                "linear_acceleration_vrf_available": "true",
                "angular_velocity_vrf_available": "true",
                "acceleration_source": "carla_actor",
                "uncertainty": "available",
                "msf_status": "OK",
                "sensor_status": "OK",
            }
        )

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=None,
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    kinematics = report["kinematics"]
    assert "ego_yaw_rate_or_heading_fd_yaw_rate" not in report["missing_fields"]
    assert kinematics["ego_yaw_rate_available"] is True
    assert kinematics["heading_finite_difference_yaw_rate_available"] is True
    assert kinematics["yaw_rate_vs_heading_fd_p95_rad_s"] == pytest.approx(0.0, abs=1e-9)


def test_uncertainty_status_policy_gaps_are_explicit_not_missing() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row.pop("uncertainty", None)
        row.pop("msf_status", None)
        row.pop("sensor_status", None)
        row["localization_uncertainty_policy"] = "not_modelled_gt_truth"
        row["localization_msf_status_policy"] = "not_applicable_gt_truth"
        row["localization_sensor_status_policy"] = "not_applicable_gt_truth"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert "uncertainty" not in report["missing_fields"]
    assert "msf_status" not in report["missing_fields"]
    assert "sensor_status" not in report["missing_fields"]
    assert report["status"]["uncertainty_policy"] == "not_modelled_gt_truth"
    assert report["status"]["msf_status_policy"] == "not_applicable_gt_truth"
    assert report["status"]["sensor_status_policy"] == "not_applicable_gt_truth"
    assert report["acceptance_checklist"]["uncertainty_status_policy"]["status"] == "warn"


def test_timestamp_non_monotonic_fails() -> None:
    stats = _read_json(CHANNEL_STATS)
    stats["channels"]["/apollo/localization/pose"]["timestamp_monotonic"] = False

    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=stats,
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "fail"
    assert "localization_timestamp_non_monotonic" in report["verdict"]["blocking_reasons"]


def test_measurement_time_must_match_header_timestamp() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["measurement_time"] = str(float(row["localization_timestamp"]) + 0.010)

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["time"]["measurement_header_delta_ms_p95"] == pytest.approx(10.0)
    assert report["verdict"]["status"] == "fail"
    assert "measurement_header_timestamp_mismatch" in report["verdict"]["blocking_reasons"]


def test_header_frame_id_must_be_map_for_claim_grade() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["localization_frame_id"] = "odom"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["channel"]["header_frame_id"] == "odom"
    assert report["verdict"]["status"] == "fail"
    assert "localization_header_frame_id_not_map" in report["verdict"]["blocking_reasons"]


def test_high_heading_error_warns_or_fails() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["heading_error"] = "0.35"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "warn"
    assert "heading_error_to_route_elevated" in report["warnings"]


def test_yaw_rate_unit_unknown_warns() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["angular_velocity_unit"] = "unknown"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "warn"
    assert report["kinematics"]["angular_velocity_unit"] == "unknown"
    assert "angular_velocity_unit_unknown" in report["warnings"]


def test_lane_projection_aliases_are_reported() -> None:
    report = _analyze_fixture()
    lane = report["lane_projection"]

    assert lane["lane_projection_available"] is True
    assert lane["lane_projection_inside_ratio"] == 1.0
    assert lane["lane_projection_distance_p95_m"] == 0.08
    assert lane["lane_lateral_error_p95_m"] == 0.02
    assert lane["lane_heading_error_p95_rad"] == report["pose_consistency"]["heading_error_to_lane_p95_rad"]
    assert lane["reference_line_verified"] is False


def test_lane_heading_error_high_is_report_blocking() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["lane_heading_error"] = "0.40"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "fail"
    assert "heading_error_to_lane_high" in report["verdict"]["blocking_reasons"]
    assert report["acceptance_checklist"]["natural_driving_gate_ready"]["status"] == "fail"


def test_quaternion_decode_is_independent_of_self_reported_diff() -> None:
    report = _analyze_fixture()

    assert report["pose_consistency"]["orientation_quaternion_available"] is True
    assert report["pose_consistency"]["orientation_heading_diff_source"] == "decoded_quaternion"
    assert report["pose_consistency"]["orientation_heading_diff_p95_rad"] == pytest.approx(0.0)
    assert report["acceptance_checklist"]["decoded_orientation_consistency"]["status"] == "pass"


def test_wrong_quaternion_warns_or_fails_orientation_consistency() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["localization_orientation_qz"] = "0.0"
        row["localization_orientation_qw"] = "1.0"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["pose_consistency"]["orientation_heading_diff_p95_rad"] == math.pi / 2
    assert report["verdict"]["status"] == "fail"
    assert "orientation_heading_diff_high" in report["verdict"]["blocking_reasons"]
    assert report["acceptance_checklist"]["decoded_orientation_consistency"]["status"] == "fail"


def test_zero_filled_acceleration_is_not_claim_grade() -> None:
    rows = _read_csv(TIMESERIES)
    for row in rows:
        row["acceleration_source"] = "zero_filled"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["kinematics"]["physical_linear_acceleration_available"] is False
    assert report["kinematics"]["linear_acceleration_claim_grade"] is False
    assert report["acceptance_checklist"]["acceleration_semantics"]["status"] == "warn"
    assert "acceleration_zero_filled" in report["warnings"]


def test_finite_difference_acceleration_source_is_preferred_over_initial_sample() -> None:
    rows = _read_csv(TIMESERIES)
    for index, row in enumerate(rows):
        row["acceleration_source"] = "initial_sample_missing_previous_velocity" if index == 0 else "finite_difference"
        row["linear_acceleration_x"] = "0.0"
        row["linear_acceleration_vrf_x"] = "0.0"

    report = analyze_localization_contract(
        timeseries_rows=rows,
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["kinematics"]["linear_acceleration_source"] == "finite_difference"
    assert report["kinematics"]["physical_linear_acceleration_available"] is True
    assert report["kinematics"]["linear_acceleration_claim_grade"] is True
    assert report["acceptance_checklist"]["acceleration_semantics"]["status"] == "pass"


def test_vehicle_reference_confidence_assumed_blocks_claim_grade() -> None:
    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference("configs/vehicles/ego_vehicle_reference.example.yaml"),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "insufficient_data"
    assert report["reference_point"]["configured_vehicle_reference_confidence"] == "assumed"
    assert report["reference_point"]["vehicle_reference_hard_gate_eligible"] is False
    assert "vehicle_reference_confidence_assumed" in report["warnings"]


def test_missing_route_health_does_not_crash() -> None:
    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=None,
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )

    assert report["verdict"]["status"] == "warn"
    assert "route_health" in report["missing_fields"]
    assert "route_health_missing" in report["warnings"]


def test_cli_generates_report_and_summary(tmp_path: Path) -> None:
    out = tmp_path / "loc"
    run_dir = _make_run_dir(tmp_path / "run")
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_localization_contract.py",
            "--run-dir",
            str(run_dir),
            "--frame-transform",
            str(FRAME_TRANSFORM),
            "--vehicle-reference",
            str(VEHICLE_REFERENCE),
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = json.loads(result.stdout)
    payload = json.loads((out / "localization_contract_report.json").read_text(encoding="utf-8"))
    summary = (out / "localization_contract_summary.md").read_text(encoding="utf-8")

    assert stdout["status"] == "pass"
    assert payload["verdict"]["status"] == "pass"
    assert "Blocking reasons" in summary


def test_run_dir_auto_discovers_inputs(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path / "run")

    report = analyze_localization_contract_files(
        run_dir=run_dir,
        frame_transform_path=FRAME_TRANSFORM,
        vehicle_reference_path=VEHICLE_REFERENCE,
    )

    assert report["run_id"] == "run_a"
    assert report["route_id"] == "lane097"
    assert report["backend"] == "carla_direct"
    assert report["verdict"]["status"] == "pass"


def test_run_dir_auto_discovers_apollo_hdmap_projection(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path / "run")
    projection_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    projection_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in _hdmap_projection_rows(heading_error_rad=0.01, lateral_error_m=0.05))
        + "\n",
        encoding="utf-8",
    )

    report = analyze_localization_contract_files(
        run_dir=run_dir,
        frame_transform_path=FRAME_TRANSFORM,
        vehicle_reference_path=VEHICLE_REFERENCE,
    )

    assert report["source"]["hdmap_projection_path"].endswith("artifacts/apollo_hdmap_projection.jsonl")
    assert report["apollo_hdmap_projection"]["claim_grade"] is True
    assert report["acceptance_checklist"]["reference_line_projection"]["status"] == "pass"


def test_debug_timeseries_merges_primary_route_context_for_hdmap_lateral_attribution(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path / "run")
    route_rows = [
        {"sim_time": f"{index * 0.05:.2f}", "route_id": "lane097", "route_s": str(index), "cross_track_error": "-0.80"}
        for index in range(8)
    ]
    _write_csv(run_dir / "timeseries.csv", route_rows)
    _copy(TIMESERIES, run_dir / "artifacts" / "debug_timeseries.csv")
    projection_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    projection_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in _hdmap_projection_rows(heading_error_rad=0.01, lateral_error_m=0.80))
        + "\n",
        encoding="utf-8",
    )

    report = analyze_localization_contract_files(
        run_dir=run_dir,
        frame_transform_path=FRAME_TRANSFORM,
        vehicle_reference_path=VEHICLE_REFERENCE,
    )

    consistency = report["hdmap_route_lateral_consistency"]
    assert report["source"]["timeseries_path"].endswith("artifacts/debug_timeseries.csv")
    assert report["verdict"]["status"] == "fail"
    assert consistency["status"] == "pass"
    assert consistency["alignment_mode"] == "opposite_sign"
    assert consistency["interpretation"] == "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"


def test_run_dir_falls_back_to_debug_timeseries_for_localization_fields(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path / "run")
    (run_dir / "timeseries.csv").write_text("sim_time,route_id\n1.0,lane097\n", encoding="utf-8")
    _copy(TIMESERIES, run_dir / "artifacts" / "debug_timeseries.csv")

    report = analyze_localization_contract_files(
        run_dir=run_dir,
        frame_transform_path=FRAME_TRANSFORM,
        vehicle_reference_path=VEHICLE_REFERENCE,
    )

    assert report["verdict"]["status"] == "pass"
    assert report["source"]["timeseries_path"].endswith("artifacts/debug_timeseries.csv")
    assert report["source"]["timeseries_selection_reason"] == "debug_timeseries_has_stronger_localization_contract_fields"


def test_run_dir_prefers_debug_timeseries_over_generic_ego_fields(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path / "run")
    (run_dir / "timeseries.csv").write_text(
        "sim_time,route_id,ego_speed_mps,ego_heading\n1.0,lane097,5.0,0.0\n",
        encoding="utf-8",
    )
    _copy(TIMESERIES, run_dir / "artifacts" / "debug_timeseries.csv")

    report = analyze_localization_contract_files(
        run_dir=run_dir,
        frame_transform_path=FRAME_TRANSFORM,
        vehicle_reference_path=VEHICLE_REFERENCE,
    )

    assert report["verdict"]["status"] == "pass"
    assert report["source"]["timeseries_path"].endswith("artifacts/debug_timeseries.csv")
    assert report["channel"]["fresh_sample_count"] is not None


def test_runtime_reference_conversion_supports_vrp_claim() -> None:
    report = _analyze_fixture()
    reference = report["reference_point"]

    assert reference["source_reference_mode"] == "vehicle_origin"
    assert reference["published_localization_reference_mode"] == "rear_axle"
    assert reference["position_uses_vrp"] is True
    assert reference["reference_conversion_detected"] is True
    assert reference["reference_conversion_method"] == "source_vehicle_origin_to_published_rear_axle"


def test_config_only_reference_does_not_fabricate_vrp_claim() -> None:
    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
    )

    assert report["verdict"]["status"] == "warn"
    assert report["reference_point"]["position_uses_vrp"] is None
    assert report["reference_point"]["reference_path_status"] == "insufficient_data"
    assert report["acceptance_checklist"]["position_uses_vrp"]["status"] == "insufficient_data"


def test_runtime_vehicle_origin_to_vehicle_origin_blocks_vrp_claim() -> None:
    source = _runtime_reference_source()
    source["cyber_bridge_stats"]["last_pose_debug"]["localization_reference_mode"] = "vehicle_origin"
    source["cyber_bridge_stats"]["localization"]["reference_mode"] = "vehicle_origin"

    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=source,
    )

    assert report["verdict"]["status"] == "fail"
    assert report["reference_point"]["position_uses_vrp"] is False
    assert "localization_position_not_vrp" in report["verdict"]["blocking_reasons"]


def test_published_rear_axle_without_back_offset_warns_not_passes() -> None:
    source = _runtime_reference_source()
    source["cyber_bridge_stats"]["last_pose_debug"].pop("localization_back_offset_m", None)
    source["cyber_bridge_stats"]["localization"].pop("back_offset_m", None)

    report = analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=source,
    )

    assert report["verdict"]["status"] == "warn"
    assert report["reference_point"]["position_uses_vrp"] is None
    assert "reference_conversion_back_offset_missing" in report["warnings"]


def _analyze_fixture() -> dict:
    return analyze_localization_contract(
        timeseries_rows=_read_csv(TIMESERIES),
        channel_stats=_read_json(CHANNEL_STATS),
        route_health=_read_json(ROUTE_HEALTH),
        frame_transform=load_frame_transform(FRAME_TRANSFORM),
        vehicle_reference=load_vehicle_reference(VEHICLE_REFERENCE),
        source=_runtime_reference_source(),
    )


def _runtime_reference_source() -> dict:
    return {
        "cyber_bridge_stats": _read_json(CYBER_BRIDGE_STATS),
        "ros2_gt_live_stats": _read_json(ROS2_GT_LIVE_STATS),
    }


def _hdmap_projection_rows(*, heading_error_rad: float, lateral_error_m: float) -> list[dict]:
    rows = []
    for index in range(60):
        rows.append(
            {
                "timestamp": index * 0.05,
                "localization_x": 10.0 + index * 0.5,
                "localization_y": 2.0,
                "localization_heading": 0.0,
                "nearest_lane_id": "lane_097",
                "projection_s": 3.0 + index * 0.6,
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
            }
        )
    return rows


def _make_run_dir(run_dir: Path) -> Path:
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(json.dumps({"run_id": "run_a", "backend": "carla_direct"}), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps({"route_id": "lane097"}), encoding="utf-8")
    _copy(TIMESERIES, run_dir / "timeseries.csv")
    _copy(CHANNEL_STATS, run_dir / "channel_stats.json")
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True)
    _copy(CYBER_BRIDGE_STATS, artifacts_dir / "cyber_bridge_stats.json")
    _copy(ROS2_GT_LIVE_STATS, artifacts_dir / "ros2_gt_live_stats.json")
    rh_dir = run_dir / "analysis" / "route_health"
    rh_dir.mkdir(parents=True)
    _copy(ROUTE_HEALTH, rh_dir / "route_health.json")
    return run_dir


def _read_json(path: Path) -> dict:
    return deepcopy(json.loads(path.read_text(encoding="utf-8")))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _copy(source: Path, dest: Path) -> None:
    dest.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
