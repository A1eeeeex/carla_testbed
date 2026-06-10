from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pytest

from carla_testbed.analysis.control_health import (
    CONTROL_HEALTH_REPORT_SCHEMA_VERSION,
    analyze_control_health_run_dir,
    write_control_health_report,
)

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def _copy_run(tmp_path: Path, name: str = "lane_keep_097") -> Path:
    target = tmp_path / name
    shutil.copytree(FIXTURE_ROOT / name, target)
    return target


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_control_health_passes_and_writes_report(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)

    report = analyze_control_health_run_dir(run_dir)
    outputs = write_control_health_report(report, tmp_path / "out")

    assert report["schema_version"] == CONTROL_HEALTH_REPORT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["failure_reason"] is None
    assert report["raw_mapped_applied_control_available"] is True
    assert report["metrics"]["brake_throttle_conflict_frames"] == 0
    boundary = report["metrics"]["control_mapping_claim_boundary"]
    assert boundary["claim_grade_control_mapping"] is False
    assert boundary["steering_parameters_source"] == "runtime_config_or_unknown"
    assert "calibration_profile_missing" in boundary["warnings"]
    assert "steering_parameters_not_backed_by_calibration_profile" in boundary["warnings"]
    preconditions = report["metrics"]["upstream_contract_preconditions"]
    assert preconditions["control_oscillation_analysis_eligible"] is True
    assert preconditions["localization_contract"]["status"] == "pass"
    assert preconditions["apollo_reference_line_contract"]["status"] == "pass"
    assert Path(outputs["control_health_report"]).is_file()
    assert Path(outputs["control_health_summary"]).is_file()


def test_control_health_bad_handoff_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["control_handoff_status"] = "control_missing"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "control_handoff_not_consuming"


def test_control_health_reports_control_process_crash_before_actuation(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    handoff_path = run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    handoff_path.write_text(
        json.dumps(
            {
                "schema_version": "apollo_control_handoff.v1",
                "verdict": "fail",
                "failure_stage": "process_health",
                "blocking_reasons": ["process_health_failed"],
                "process_health": {
                    "status": "fail",
                    "crash_detected": True,
                    "crash_reason": "tcmalloc_invalid_free",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "control_process_crash_before_control_output"
    process = report["metrics"]["control_process_health"]
    assert process["crash_detected"] is True
    assert process["crash_reason"] == "tcmalloc_invalid_free"


def test_control_health_missing_control_trace_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        row.pop("apollo_steer_raw")
        row.pop("bridge_steer_mapped")
        row.pop("carla_steer_applied")
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_control_trace_fields"
    assert {"apollo_steer_raw", "bridge_steer_mapped", "carla_steer_applied"}.issubset(
        set(report["missing_fields"])
    )


def test_control_apply_trace_without_command_payload_is_not_control_evidence(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        for field in (
            "apollo_steer_raw",
            "bridge_steer_mapped",
            "throttle_raw",
            "throttle_mapped",
            "brake_raw",
            "brake_mapped",
            "carla_steer_applied",
            "throttle_applied",
            "brake_applied",
        ):
            row[field] = ""
    _write_csv(csv_path, rows)
    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "control_apply_trace.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "event_type": "control_apply",
                    "timestamp": index * 0.05,
                    "apollo_raw": {"throttle": None, "brake": None, "steer": None},
                    "bridge_mapped": {"throttle": None, "brake": None, "steer": None},
                    "carla_applied": {"throttle": None, "brake": None, "steer": None},
                }
            )
            for index in range(3)
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "[INFO] [1002.000000000] [carla_control_bridge]: apply frame=10 source=pending "
        "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
        "clamped=False throttle=0.000 brake=0.000 rx=10 applied=3 drop_same_frame=0\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)

    assert report["raw_mapped_applied_control_available"] is False
    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_control_trace_fields"
    decode = report["metrics"]["control_decode_debug"]
    assert decode["available"] is True
    assert decode["trace_row_count"] == 3
    assert decode["command_payload_row_count"] == 0
    assert decode["no_command_placeholder_count"] == 3
    assert decode["command_payload_available"] is False
    assert "control_row_level_trace_no_command_payload" in report["warnings"]


def test_control_health_uses_external_bridge_evidence_when_p0_raw_mapped_is_null(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["apollo_steer_raw"] = ""
        row["bridge_steer_mapped"] = ""
        row["throttle_raw"] = ""
        row["throttle_mapped"] = ""
        row["brake_raw"] = ""
        row["brake_mapped"] = ""
        row["carla_steer_applied"] = "0.0"
        row["throttle_applied"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_applied"] = "0.0" if index % 2 == 0 else "0.7"
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[INFO] [1000.000000000] [carla_control_bridge]: "
                    "Bridge listening on /tb/ego/control_cmd (direct), carla=127.0.0.1:2000, "
                    "ego_role=hero, dryrun=False, apply_hz=20.0, sync_to_world_tick=True"
                ),
                (
                    "[INFO] [1001.000000000] [carla_control_bridge]: "
                    "control target bound actor_id=197 role=hero"
                ),
                (
                    "[INFO] [1002.000000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.800 brake=0.000 rx=10 applied=1 drop_same_frame=0"
            ),
            (
                "[INFO] [1002.050000000] [carla_control_bridge]: apply frame=11 source=pending "
                "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                "clamped=False throttle=0.000 brake=0.700 rx=20 applied=2 drop_same_frame=0"
            ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "control_decode_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "parsed_control": {
                        "control_latency_ms": 1.0,
                        "throttle": 0.2,
                        "brake": 0.0,
                        "steer": 0.0,
                    },
                    "output_to_carla": {
                        "mapped_throttle_cmd": 0.2,
                        "mapped_brake_cmd": 0.0,
                        "mapped_carla_steer_cmd": 0.0,
                    },
                }
            )
            for index in range(12)
        )
        + "\n",
        encoding="utf-8",
    )
    report = analyze_control_health_run_dir(run_dir)

    assert report["raw_mapped_applied_control_available"] is True
    assert report["raw_mapped_applied_control_source"] == "bridge_decode_plus_timeseries"
    assert report["status"] == "fail"
    assert report["failure_reason"] == "applied_actuation_oscillation"
    assert "external_control_trace_from_bridge_artifacts" in report["warnings"]
    layers = report["metrics"]["oscillation_decomposition"]["layers"]
    assert layers["apollo_raw_command"]["source"] == "control_decode_debug.jsonl"
    assert layers["bridge_mapped_command"]["source"] == "control_decode_debug.jsonl"
    assert layers["bridge_mapped_command"]["status"] == "pass"


def test_control_health_detects_bridge_decode_mapped_oscillation_when_p0_raw_mapped_is_null(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["apollo_steer_raw"] = ""
        row["bridge_steer_mapped"] = ""
        row["throttle_raw"] = ""
        row["throttle_mapped"] = ""
        row["brake_raw"] = ""
        row["brake_mapped"] = ""
        row["carla_steer_applied"] = "0.0"
        row["throttle_applied"] = "0.2"
        row["brake_applied"] = "0.0"
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[INFO] [1000.000000000] [carla_control_bridge]: "
                    "Bridge listening on /tb/ego/control_cmd (direct), carla=127.0.0.1:2000, "
                    "ego_role=hero, dryrun=False, apply_hz=20.0, sync_to_world_tick=True"
                ),
                (
                    "[INFO] [1001.000000000] [carla_control_bridge]: "
                    "control target bound actor_id=197 role=hero"
                ),
                (
                    "[INFO] [1002.000000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.200 brake=0.000 rx=10 applied=1 drop_same_frame=0"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "control_decode_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "parsed_control": {
                        "control_latency_ms": 1.0,
                        "throttle": 0.2,
                        "brake": 0.0,
                        "steer": 0.0,
                    },
                    "output_to_carla": {
                        "mapped_throttle_cmd": 0.8 if index % 2 == 0 else 0.0,
                        "mapped_brake_cmd": 0.0 if index % 2 == 0 else 0.7,
                        "mapped_carla_steer_cmd": 0.0,
                    },
                }
            )
            for index in range(12)
        )
        + "\n",
        encoding="utf-8",
    )
    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "bridge_mapped_command_oscillation"
    layers = report["metrics"]["oscillation_decomposition"]["layers"]
    assert layers["bridge_mapped_command"]["source"] == "control_decode_debug.jsonl"
    assert layers["bridge_mapped_command"]["status"] == "fail"


def test_control_health_summarizes_apollo_simple_lon_debug_context(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "control_decode_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "parsed_control": {
                        "control_latency_ms": 1.0,
                        "throttle": 0.12,
                        "brake": 0.0,
                        "steer": 0.0,
                        "debug_simple_lon_current_speed_mps": 3.0 + index,
                        "debug_simple_lon_speed_reference_mps": 8.0,
                        "debug_simple_lon_speed_error_mps": 5.0 - index,
                        "debug_simple_lon_acceleration_cmd_mps2": 0.4,
                        "debug_simple_lon_acceleration_cmd_closeloop_mps2": 0.3,
                        "debug_simple_lon_acceleration_lookup_mps2": 0.2,
                        "debug_simple_lon_acceleration_reference_mps2": 0.5,
                        "debug_simple_lon_current_acceleration_mps2": 0.1,
                        "debug_simple_lon_acceleration_error_mps2": 0.4,
                        "debug_simple_lon_throttle_cmd_pct": 12.0,
                        "debug_simple_lon_brake_cmd_pct": 0.0,
                        "debug_simple_lon_path_remain_m": 40.0 - index,
                        "debug_simple_lon_station_error_m": -0.5,
                        "debug_simple_lon_preview_station_error_m": -0.2,
                        "debug_simple_lon_is_full_stop": index == 2,
                    },
                    "output_to_carla": {
                        "mapped_throttle_cmd": 0.12,
                        "mapped_brake_cmd": 0.0,
                        "mapped_carla_steer_cmd": 0.0,
                    },
                }
            )
            for index in range(3)
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    longitudinal = report["metrics"]["control_decode_debug"]["longitudinal_debug"]

    assert longitudinal["available"] is True
    assert longitudinal["sample_count"] == 3
    assert longitudinal["missing_context_fields"] == []
    assert longitudinal["full_stop_count"] == 1
    assert longitudinal["available_counts"]["debug_simple_lon_current_speed_mps"] == 3
    assert longitudinal["stats"]["debug_simple_lon_speed_reference_mps"]["p50"] == 8.0
    assert longitudinal["stats"]["debug_simple_lon_throttle_cmd_pct"]["max"] == 12.0


def test_control_health_reads_simple_lon_debug_from_raw_control_dump(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "control_decode_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "raw_control_msg_dump": {
                        "control_header_sequence_num": index + 10,
                        "throttle": 100.0 if index == 0 else 0.0,
                        "brake": 0.0 if index == 0 else 50.0,
                        "debug_simple_lon_current_speed": 2.0,
                        "debug_simple_lon_speed_reference": 6.0 if index == 0 else 1.0,
                        "debug_simple_lon_speed_error": 4.0 if index == 0 else -1.0,
                        "debug_simple_lon_current_acceleration": 0.0 if index == 0 else -12.0,
                        "debug_simple_lon_acceleration_cmd": 0.8 if index == 0 else -0.8,
                        "debug_simple_lon_throttle_cmd": 100.0 if index == 0 else 0.0,
                        "debug_simple_lon_brake_cmd": 0.0 if index == 0 else 50.0,
                        "debug_simple_lon_path_remain": 30.0 if index == 0 else 0.5,
                        "debug_simple_lon_station_error": 3.0 if index == 0 else 0.1,
                        "debug_simple_lon_is_full_stop": False,
                    },
                    "mapped_throttle_cmd": 1.0,
                    "mapped_brake_cmd": 0.0,
                    "mapped_carla_steer_cmd": 0.0,
                }
            )
            for index in range(2)
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "control_trajectory_consume_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "planning_header_sequence_num_used": 20 + index,
                    "latest_planning_trajectory_point_count": 120,
                    "effective_planning_source": "matched_seq" if index == 0 else "latest_known_fallback",
                    "control_input_candidate_source": "primary",
                    "latest_planning_msg_age_ms": 100.0 + index * 10.0,
                    "trajectory_first_point_relative_time": -1.0 if index == 0 else -0.2,
                    "trajectory_last_point_relative_time": 7.0,
                }
            )
            for index in range(2)
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "planning_topic_debug.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "planning_header_sequence_num": 20,
                        "trajectory_point_count": 240,
                        "trajectory_total_path_length": 30.0,
                        "trajectory_total_time": 7.1,
                        "trajectory_relative_time_min_sec": -1.0,
                        "trajectory_relative_time_max_sec": 7.0,
                        "first_trajectory_point_x": 10.0,
                        "first_trajectory_point_y": 1.0,
                        "last_trajectory_point_x": 40.0,
                        "last_trajectory_point_y": 1.0,
                        "first_trajectory_point_v": 1.0,
                        "trajectory_type": "NORMAL",
                        "scenario_plugin_type": "LANE_FOLLOW",
                        "stage_plugin_type": "LANE_FOLLOW_STAGE",
                        "reference_line_count": 0,
                        "routing_lane_window_signature": "15_1_1@0->30",
                    }
                ),
                json.dumps(
                    {
                        "planning_header_sequence_num": 21,
                        "trajectory_point_count": 69,
                        "trajectory_total_path_length": 0.4,
                        "trajectory_total_time": 2.9,
                        "trajectory_relative_time_min_sec": 0.1,
                        "trajectory_relative_time_max_sec": 2.9,
                        "first_trajectory_point_x": 10.1,
                        "first_trajectory_point_y": 1.0,
                        "last_trajectory_point_x": 10.4,
                        "last_trajectory_point_y": 1.0,
                        "first_trajectory_point_v": 1.5,
                        "trajectory_type": "SPEED_FALLBACK",
                        "scenario_plugin_type": "LANE_FOLLOW",
                        "stage_plugin_type": "LANE_FOLLOW_STAGE",
                        "reference_line_count": 0,
                        "routing_lane_window_signature": "15_1_1@0->30",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "planning_route_segment_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "planning_header_sequence_num": 20 + index,
                    "reference_line_provider_status": "trajectory_nonzero_debug_missing",
                    "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                    "create_route_segments_status": "ok",
                    "planning_empty_reason_guess": "",
                }
            )
            for index in range(2)
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "apollo_planning.INFO").write_text(
        "\n".join(
            [
                "E0605 trajectory_stitcher.cc:213] the distance between matched point and actual position is too large. Replan is triggered. lon_diff = 2.75",
                "E0605 piecewise_jerk_speed_optimizer.cc:191] Piecewise jerk speed optimizer failed!.try to fallback.",
                "E0605 lane_follow_stage.cc:173] Failed to run tasks[PIECEWISE_JERK_SPEED], Error message: Piecewise jerk speed optimizer failed!",
                "E0605 trajectory_fallback_task.cc:42] Speed fallback due to algorithm failure",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "publish_elapsed_wall_sec": 10.0,
                "loc_count": 20,
                "chassis_count": 20,
                "control_rx_count": 200,
                "control_tx_count": 200,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    longitudinal = report["metrics"]["control_decode_debug"]["longitudinal_debug"]

    assert report["source"]["control_row_level_trace_path"].endswith("control_decode_debug.jsonl")
    assert longitudinal["available"] is True
    assert longitudinal["available_counts"]["debug_simple_lon_current_speed_mps"] == 2
    assert longitudinal["stats"]["debug_simple_lon_speed_reference_mps"]["p50"] == 3.5
    assert longitudinal["stats"]["debug_simple_lon_acceleration_cmd_mps2"]["max"] == 0.8
    attribution = report["metrics"]["control_decode_debug"]["longitudinal_oscillation_attribution"]
    assert attribution["transition_count"] == 1
    assert attribution["transition_buckets"]["acceleration_cmd_sign_flip"] == 1
    assert attribution["transition_buckets"]["path_remain_large_drop"] == 1
    assert attribution["dominant_suspected_factor"] == "apollo_simple_lon_acceleration_cmd_sign_switching"
    correlation = report["metrics"]["control_decode_debug"]["trajectory_consume_correlation"]
    assert correlation["available"] is True
    assert correlation["transition_count"] == 1
    assert correlation["transition_buckets"]["planning_sequence_changed"] == 1
    assert correlation["transition_buckets"]["effective_planning_source_changed"] == 1
    assert correlation["transition_buckets"]["trajectory_window_changed"] == 1
    assert correlation["dominant_suspected_factor"] == "planning_sequence_update_correlates_with_switches"
    planning = report["metrics"]["control_decode_debug"]["planning_trajectory_correlation"]
    assert planning["available"] is True
    assert planning["transition_count"] == 1
    assert planning["transition_buckets"]["trajectory_path_length_delta_gt_5m"] == 1
    assert planning["transition_buckets"]["trajectory_last_point_jump_gt_5m"] == 1
    assert planning["transition_buckets"]["speed_fallback_involved"] == 1
    assert planning["dominant_suspected_factor"] == "planning_trajectory_length_switching"
    assert (
        planning["examples"][0]["planning_trajectory"]["trajectory_type"]
        == ["NORMAL", "SPEED_FALLBACK"]
    )
    fallback = report["metrics"]["planning_log_fallback_diagnostics"]
    assert fallback["available"] is True
    assert fallback["matched_point_lon_diff_replan_count"] == 1
    assert fallback["matched_point_lon_diff_p95_m"] == 2.75
    assert fallback["piecewise_jerk_speed_optimizer_fail_count"] == 2
    assert fallback["speed_fallback_due_to_algorithm_failure_count"] == 1
    assert (
        fallback["dominant_suspected_factor"]
        == "trajectory_stitcher_matched_point_lon_diff_replans"
    )
    cadence = report["metrics"]["gt_state_sampling_cadence"]
    assert cadence["available"] is True
    assert cadence["control_to_chassis_count_ratio"] == 10.0
    assert cadence["control_rx_wall_hz"] == 20.0
    assert cadence["chassis_wall_hz"] == 2.0
    assert "apollo_control_oversamples_gt_state" in report["warnings"]
    assert "gt_state_wall_rate_low_relative_to_control" in report["warnings"]
    assert "apollo_control_current_acceleration_spiky" in report["warnings"]


def test_control_health_brake_throttle_conflict_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    rows[0]["throttle_applied"] = "0.40"
    rows[0]["brake_applied"] = "0.30"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "brake_throttle_conflict"
    assert report["metrics"]["brake_throttle_conflict_frames"] == 1


def test_control_health_applied_throttle_brake_switching_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["throttle_mapped"] = "0.2"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_applied"] = "0.0" if index % 2 == 0 else "0.7"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "applied_actuation_oscillation"
    assert report["metrics"]["applied_throttle_brake_switch_count"] == 11
    assert report["metrics"]["applied_throttle_frames"] == 6
    assert report["metrics"]["applied_brake_frames"] == 6
    layers = report["metrics"]["oscillation_decomposition"]["layers"]
    assert layers["carla_applied_command"]["status"] == "fail"
    assert report["metrics"]["oscillation_decomposition"]["dominant_oscillation_layer"] == "carla_applied_command"


def test_control_health_defers_applied_oscillation_until_upstream_contracts_nonblocking(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["throttle_raw"] = "0.2"
        row["brake_raw"] = "0.0"
        row["throttle_mapped"] = "0.2"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_applied"] = "0.0" if index % 2 == 0 else "0.7"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    localization_path = (
        run_dir / "analysis/localization_contract/localization_contract_report.json"
    )
    localization = json.loads(localization_path.read_text(encoding="utf-8"))
    localization["verdict"] = {
        "status": "fail",
        "blocking_reasons": ["heading_error_to_lane_high"],
    }
    localization_path.write_text(json.dumps(localization, indent=2) + "\n", encoding="utf-8")

    report = analyze_control_health_run_dir(run_dir)
    preconditions = report["metrics"]["upstream_contract_preconditions"]
    layers = report["metrics"]["oscillation_decomposition"]["layers"]

    assert report["status"] == "warn"
    assert report["failure_reason"] == "control_health_warn"
    assert "applied_actuation_oscillation_deferred_until_upstream_contracts_nonblocking" in report[
        "warnings"
    ]
    assert preconditions["control_oscillation_analysis_eligible"] is False
    assert "heading_error_to_lane_high" in preconditions["blocking_reasons"]
    assert layers["carla_applied_command"]["status"] == "fail"
    assert report["metrics"]["oscillation_decomposition"]["dominant_oscillation_layer"] == "carla_applied_command"


def test_control_health_records_calibration_profile_for_steering_parameters(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["calibration_profile_id"] = "town01_control_actuation_legacy_draft"
    manifest["actuator_mapping_mode"] = "legacy"
    manifest["steer_scale"] = 0.25
    manifest["steering_sign"] = 1.0
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_control_health_run_dir(run_dir)
    boundary = report["metrics"]["control_mapping_claim_boundary"]

    assert boundary["calibration_profile_id"] == "town01_control_actuation_legacy_draft"
    assert boundary["steer_scale"] == 0.25
    assert boundary["steering_sign"] == 1.0
    assert boundary["steering_parameters_source"] == "calibration_profile"
    assert boundary["claim_grade_control_mapping"] is False
    assert "legacy_mapping_smoke_only" in boundary["warnings"]


def test_control_health_bridge_mapped_command_oscillation_is_not_mislabelled_as_applied(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["throttle_raw"] = "0.2"
        row["brake_raw"] = "0.0"
        row["throttle_mapped"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_mapped"] = "0.0" if index % 2 == 0 else "0.7"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = row["throttle_mapped"]
        row["brake_applied"] = row["brake_mapped"]
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "bridge_mapped_command_oscillation"
    layers = report["metrics"]["oscillation_decomposition"]["layers"]
    assert layers["apollo_raw_command"]["status"] == "pass"
    assert layers["bridge_mapped_command"]["status"] == "fail"


def test_control_health_apollo_raw_command_oscillation_is_separate_layer(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["throttle_raw"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_raw"] = "0.0" if index % 2 == 0 else "0.7"
        row["throttle_mapped"] = "0.2"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.2"
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "apollo_raw_command_oscillation"
    layers = report["metrics"]["oscillation_decomposition"]["layers"]
    assert layers["apollo_raw_command"]["status"] == "fail"
    assert layers["bridge_mapped_command"]["status"] == "pass"


def test_control_health_prioritizes_apply_cadence_before_actuation_tuning(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["throttle_raw"] = "0.2"
        row["brake_raw"] = "0.0"
        row["throttle_mapped"] = "0.2"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_applied"] = "0.0" if index % 2 == 0 else "0.7"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir()
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[INFO] [100.000000000] [carla_control_bridge]: "
                    "Bridge listening on /tb/ego/control_cmd (direct), carla=127.0.0.1:2000, "
                    "ego_role=hero, dryrun=False, apply_hz=20.0, sync_to_world_tick=False"
                ),
                "[INFO] [100.000000000] [carla_control_bridge]: control target bound actor_id=197 role=hero",
                (
                    "[INFO] [101.000000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.800 brake=0.000 rx=10 applied=1 drop_same_frame=0"
                ),
                (
                    "[INFO] [111.000000000] [carla_control_bridge]: apply frame=20 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.000 brake=0.700 rx=20 applied=2 drop_same_frame=0"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "control_bridge_world_frame_cadence_low"
    assert report["metrics"]["oscillation_decomposition"]["layers"]["bridge_apply_cadence"]["status"] == "fail"


def test_control_health_prioritizes_apply_cadence_before_raw_command_tuning(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    while len(rows) < 12:
        rows.append(dict(rows[-1]))
    rows = rows[:12]
    for index, row in enumerate(rows):
        row["sim_time"] = str(index * 0.05)
        row["throttle_raw"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_raw"] = "0.0" if index % 2 == 0 else "0.7"
        row["throttle_mapped"] = row["throttle_raw"]
        row["brake_mapped"] = row["brake_raw"]
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = row["throttle_raw"]
        row["brake_applied"] = row["brake_raw"]
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir()
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[INFO] [100.000000000] [carla_control_bridge]: "
                    "Bridge listening on /tb/ego/control_cmd (direct), carla=127.0.0.1:2000, "
                    "ego_role=hero, dryrun=False, apply_hz=20.0, sync_to_world_tick=False"
                ),
                "[INFO] [100.000000000] [carla_control_bridge]: control target bound actor_id=197 role=hero",
                (
                    "[INFO] [101.000000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.800 brake=0.000 rx=10 applied=1 drop_same_frame=0"
                ),
                (
                    "[INFO] [111.000000000] [carla_control_bridge]: apply frame=20 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.000 brake=0.700 rx=20 applied=2 drop_same_frame=0"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "control_bridge_world_frame_cadence_low"
    layers = report["metrics"]["oscillation_decomposition"]["layers"]
    assert layers["bridge_apply_cadence"]["status"] == "fail"
    assert layers["apollo_raw_command"]["status"] == "fail"


def test_sync_tick_low_wall_cadence_is_explained_by_frame_coverage(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[INFO] [100.000000000] [carla_control_bridge]: "
                    "Bridge listening on /tb/ego/control_cmd (direct), carla=127.0.0.1:2000, "
                    "ego_role=hero, dryrun=False, apply_hz=20.0, sync_to_world_tick=True"
                ),
                "[INFO] [100.000000000] [carla_control_bridge]: control target bound actor_id=197 role=hero",
                (
                    "[INFO] [101.000000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.200 brake=0.000 rx=100 applied=1 drop_same_frame=0"
                ),
                (
                    "[INFO] [111.000000000] [carla_control_bridge]: apply frame=20 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.200 brake=0.000 rx=100 applied=10 drop_same_frame=90"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    cadence = report["metrics"]["oscillation_decomposition"]["layers"]["bridge_apply_cadence"]

    assert report["status"] == "warn"
    assert report["failure_reason"] == "control_health_warn"
    assert "control_bridge_world_frame_cadence_low" not in report["warnings"]
    assert "control_bridge_drop_same_frame_high" not in report["warnings"]
    assert "control_bridge_sync_tick_wall_cadence_low_explained" in report["warnings"]
    assert cadence["status"] == "pass"
    assert cadence["wall_cadence_low_explained_by_sync_tick"] is True
    assert cadence["same_frame_drop_expected_from_sync_tick"] is True


def test_control_health_throttle_sampling_mismatch_warns_when_apply_is_observed(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        row["throttle_mapped"] = "1.0"
        row["throttle_applied"] = "0.25"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "warn"
    assert report["failure_reason"] == "control_health_warn"
    assert "mapped_applied_throttle_mismatch" in report["warnings"]


def test_control_health_accepts_one_frame_sampling_lag_between_mapped_and_applied(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    mapped_values = ["0.10", "0.60", "0.20", "0.45"]
    while len(rows) < len(mapped_values):
        rows.append(dict(rows[-1]))
    rows = rows[: len(mapped_values)]
    for index, row in enumerate(rows):
        row["sim_time"] = str(float(index))
        row["throttle_mapped"] = mapped_values[index]
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.0" if index == 0 else mapped_values[index - 1]
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "pass"
    assert report["metrics"]["mapped_applied_throttle_abs_error_p95_zero_lag"] > 0.10
    assert report["metrics"]["mapped_applied_throttle_abs_error_p95"] == 0.0
    assert report["metrics"]["mapped_applied_throttle_best_lag_frames"] == 1
    assert "mapped_applied_throttle_mismatch" not in report["warnings"]


def test_control_health_does_not_double_count_startup_delay_as_throttle_mismatch(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for index, row in enumerate(rows):
        row["throttle_mapped"] = "0.30"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.0" if index == 0 else "0.30"
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(
        run_dir,
        thresholds={"max_control_apply_observation_delay_s": 0.5},
    )

    assert report["status"] == "warn"
    assert "control_apply_observation_delay_high" in report["warnings"]
    assert "mapped_applied_throttle_mismatch" not in report["warnings"]
    assert report["metrics"]["mapped_applied_throttle_abs_error_p95"] == 0.0


def test_control_health_uses_direct_apply_log_for_apply_delay(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for index, row in enumerate(rows):
        row["sim_time"] = str(float(index))
        row["throttle_mapped"] = "0.30"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.0" if index == 0 else "0.30"
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir()
    (artifact_dir / "direct_bridge_control_apply.jsonl").write_text(
        json.dumps(
            {
                "ts_sec": 0.0,
                "source": "pending",
                "frame_id": 10,
                "actor_id": 197,
                "throttle": 0.30,
                "brake": 0.0,
                "steer": 0.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(
        run_dir,
        thresholds={"max_control_apply_observation_delay_s": 0.5},
    )
    direct_apply = report["metrics"]["direct_control_apply_log"]

    assert report["status"] == "pass"
    assert report["metrics"]["control_apply_observation_delay_s"] == 0.0
    assert report["metrics"]["control_apply_observation_delay_timeseries_s"] == 1.0
    assert report["metrics"]["control_apply_observation_delay_source"] == "direct_bridge_control_apply.jsonl"
    assert "control_apply_observation_delay_high" not in report["warnings"]
    assert report["source"]["direct_control_apply_path"].endswith("direct_bridge_control_apply.jsonl")
    assert direct_apply["available"] is True
    assert direct_apply["apply_count"] == 1
    assert direct_apply["actor_id"] == 197


def test_control_health_no_applied_control_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        row["throttle_mapped"] = "0.40"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.0"
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "control_apply_missing"
    assert report["metrics"]["nonzero_mapped_control_frames"] == len(rows)
    assert report["metrics"]["nonzero_applied_control_frames"] == 0


def test_control_health_warns_when_applied_control_has_no_route_progress(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        row["route_s"] = "10.0"
        row["ego_speed"] = "0.0"
        row["throttle_mapped"] = "0.40"
        row["brake_mapped"] = "0.0"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = "0.40"
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "warn"
    assert report["failure_reason"] == "control_health_warn"
    assert "route_progress_stalled_after_control" in report["warnings"]
    assert report["metrics"]["route_s_after_first_applied_control_delta_m"] == 0.0


def test_control_health_parses_control_bridge_log_warnings(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    log_dir = run_dir / "artifacts"
    log_dir.mkdir()
    (log_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[WARN] [1000.000000000] [carla_control_bridge]: "
                    "ego vehicle not found; control commands will be buffered"
                ),
                "[WARN] [1001.000000000] [carla_control_bridge]: no ego found yet; skip control",
                "[INFO] [1008.000000000] [carla_control_bridge]: control target bound actor_id=197 role=hero",
                (
                    "[INFO] [1020.000000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.250 brake=0.000 rx=3 applied=0 drop_same_frame=0"
                ),
                (
                    "[INFO] [1030.000000000] [carla_control_bridge]: apply frame=20 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=1.000 brake=0.000 rx=100 applied=5 drop_same_frame=90"
                ),
                (
                    "[INFO] [1040.000000000] [carla_control_bridge]: apply frame=30 source=watchdog "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.000 brake=1.000 rx=100 applied=10 drop_same_frame=300"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    log_metrics = report["metrics"]["control_bridge_log"]

    assert report["status"] == "warn"
    assert report["failure_reason"] == "control_health_warn"
    assert report["source"]["control_bridge_log_path"].endswith("cyber_control_bridge.err.log")
    assert log_metrics["available"] is True
    assert log_metrics["ego_bind_actor_id"] == 197
    assert log_metrics["ego_bind_delay_s"] == 8.0
    assert log_metrics["bind_to_first_apply_s"] == 12.0
    assert log_metrics["apply_world_frame_hz"] == 1.0
    assert log_metrics["same_frame_drop_ratio"] > 0.9
    assert "control_bridge_world_frame_cadence_low" in report["warnings"]
    assert "control_bridge_drop_same_frame_high" in report["warnings"]
    assert "control_bridge_ego_bind_delay_high" in report["warnings"]
    assert "control_bridge_first_apply_delay_high" in report["warnings"]
    assert "control_bridge_watchdog_after_rx_stop" in report["warnings"]


def test_control_health_decomposes_first_apply_delay(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["routing_first_request_at"] = 125.0
    summary["planning_first_message_at"] = 100.5
    summary["planning_first_nonempty_at"] = 126.0
    summary["control_first_consume_at"] = 128.0
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir()
    (artifact_dir / "control_handoff_summary.json").write_text(
        json.dumps(
            {
                "planning_first_nonempty_at": 126.0,
                "control_first_consume_at": 128.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "planning_topic_debug_summary.json").write_text(
        json.dumps({"first_nonzero_trajectory_timestamp": 126.0}) + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "command_materialization": {
                    "gate_state": {
                        "first_eval_ts_sec": 100.0,
                        "first_eligible_ts_sec": 103.0,
                        "first_ready_to_send_ts_sec": 125.0,
                        "first_blocking_reason": "startup_delay",
                        "last_blocking_reason": "routing_phase_already_sent",
                        "last_error_snapshot": "waiting_for_apollo_startup_warmup",
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "cyber_control_bridge.err.log").write_text(
        "\n".join(
            [
                (
                    "[INFO] [100.000000000] [carla_control_bridge]: "
                    "Bridge listening on /tb/ego/control_cmd (direct), carla=127.0.0.1:2000, "
                    "ego_role=hero, dryrun=False, apply_hz=20.0, sync_to_world_tick=True"
                ),
                "[INFO] [100.000000000] [carla_control_bridge]: control target bound actor_id=197 role=hero",
                (
                    "[INFO] [128.100000000] [carla_control_bridge]: apply frame=10 source=pending "
                    "actor_id=197 role=hero src_steer=0.000 norm_steer=0.000 carla_steer=0.000 "
                    "clamped=False throttle=0.250 brake=0.000 rx=3 applied=1 drop_same_frame=0"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    delay = report["metrics"]["link_delay_decomposition"]

    assert delay["bridge_bind_to_first_apply_s"] == pytest.approx(28.1)
    assert delay["bridge_bind_to_routing_first_request_s"] == pytest.approx(25.0)
    assert delay["command_gate_startup_delay_observed_s"] == pytest.approx(3.0)
    assert delay["command_gate_apollo_warmup_delay_observed_s"] == pytest.approx(22.0)
    assert delay["command_gate_ready_to_routing_first_request_s"] == pytest.approx(0.0)
    assert delay["command_gate_first_blocking_reason"] == "startup_delay"
    assert delay["command_gate_last_error_snapshot"] == "waiting_for_apollo_startup_warmup"
    assert delay["routing_first_request_to_planning_nonempty_s"] == pytest.approx(1.0)
    assert delay["planning_nonempty_to_control_first_consume_s"] == pytest.approx(2.0)
    assert delay["control_first_consume_to_bridge_first_apply_s"] == pytest.approx(0.1)
    assert delay["primary_delay_stage"] == "bridge_bind_to_routing_first_request_s"
    assert report["source"]["control_handoff_path"].endswith("control_handoff_summary.json")
    assert report["source"]["planning_summary_path"].endswith("planning_topic_debug_summary.json")
    assert report["source"]["cyber_bridge_stats_path"].endswith("cyber_bridge_stats.json")


def test_control_health_uses_control_decode_debug_latency_when_timeseries_missing(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"].pop("control_latency_p95_ms")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        row["control_latency_ms"] = ""
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir()
    (artifact_dir / "control_decode_debug.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "parsed_control": {
                            "control_latency_ms": 10,
                            "control_message_age_ms": 12,
                        },
                        "output_to_carla": {
                            "control_latency_ms": 20,
                            "control_message_age_ms": 22,
                            "planning_message_age_ms": 30,
                        },
                    }
                ),
                json.dumps(
                    {
                        "parsed_control": {"control_latency_ms": 30},
                        "output_to_carla": {
                            "control_latency_ms": 40,
                            "control_message_age_ms": 42,
                            "planning_message_age_ms": 60,
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    decode = report["metrics"]["control_decode_debug"]

    assert report["status"] == "pass"
    assert report["metrics"]["control_latency_p95_ms"] == 39.0
    assert report["metrics"]["control_latency_source"] == "control_decode_debug_jsonl.control_latency_ms"
    assert "control_latency_missing" not in report["warnings"]
    assert report["source"]["control_decode_debug_path"].endswith("control_decode_debug.jsonl")
    assert decode["available"] is True
    assert decode["control_latency_sample_count"] == 2
    assert decode["control_message_age_p95_ms"] == 41.0
    assert decode["planning_message_age_p95_ms"] == 58.5


def test_control_health_prefers_control_apply_trace_jsonl_for_row_level_evidence(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    rows = _read_csv(csv_path)
    for row in rows:
        row["throttle_raw"] = ""
        row["brake_raw"] = ""
        row["throttle_mapped"] = ""
        row["brake_mapped"] = ""
        row["bridge_steer_mapped"] = ""
    _write_csv(csv_path, rows)

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "control_apply_trace.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "schema_version": "control_apply_trace.v1",
                    "timestamp": 10.0 + index,
                    "control_latency_ms": 10.0 * (index + 1),
                    "control_message_age_ms": 5.0,
                    "planning_message_age_ms": 7.0,
                    "apollo_raw": {"throttle": 0.2, "brake": 0.0, "steer": 0.0},
                    "bridge_mapped": {
                        "throttle": 0.2,
                        "brake": 0.0,
                        "steer": 0.0,
                        "throttle_brake_mutual_exclusion_applied": index == 1,
                    },
                    "carla_applied": {"throttle": 0.2, "brake": 0.0, "steer": 0.0},
                    "vehicle_response": {"speed_mps": 2.0 + index, "yaw_rate_rad_s": 0.0},
                }
            )
            for index in range(2)
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_control_health_run_dir(run_dir)
    decode = report["metrics"]["control_decode_debug"]

    assert report["source"]["control_apply_trace_path"].endswith("control_apply_trace.jsonl")
    assert report["source"]["control_row_level_trace_path"].endswith("control_apply_trace.jsonl")
    assert decode["available"] is True
    assert decode["path"].endswith("control_apply_trace.jsonl")
    assert decode["trace_row_count"] == 2
    assert decode["nonzero_mapped_control_frames"] == 2
    assert decode["control_latency_p95_ms"] == 19.5
    assert decode["throttle_brake_mutual_exclusion_applied_count"] == 1
    assert decode["apollo_raw_command_layer"]["status"] == "pass"
    assert decode["bridge_mapped_command_layer"]["status"] == "pass"
