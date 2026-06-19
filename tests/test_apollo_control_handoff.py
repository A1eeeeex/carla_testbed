from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_control_handoff import (
    APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION,
    analyze_apollo_control_handoff,
    ensure_apollo_control_handoff_report,
    write_apollo_control_handoff_report,
)
from carla_testbed.analysis.natural_driving import analyze_natural_driving_suite
from carla_testbed.analysis.transport_ab import _compare_pair

FIXTURE = Path("tests/fixtures/apollo_control_handoff/full_pass")
SCRIPT = Path("tools/analyze_apollo_control_handoff.py")
NATURAL_FIXTURE = Path("tests/fixtures/natural_driving/simple_suite")


def _copy_case(tmp_path: Path) -> Path:
    target = tmp_path / "case"
    shutil.copytree(FIXTURE, target)
    return target


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_full_pass_writes_report(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)

    report = analyze_apollo_control_handoff(run_dir=run_dir)
    outputs = write_apollo_control_handoff_report(report, tmp_path / "out")

    assert report["schema_version"] == APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION
    assert report["verdict"] == "pass"
    assert report["failure_stage"] == "none"
    assert report["evidence_level"] == "mixed"
    assert report["control_channel"]["message_count"] == 10
    assert report["bridge_receive"]["control_rx_count"] == 10
    assert report["mapping_and_apply"]["apply_control_count"] == 10
    assert Path(outputs["apollo_control_handoff_report"]).is_file()
    assert Path(outputs["apollo_control_handoff_summary"]).is_file()


def test_route_completion_delta_counts_as_vehicle_response(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    rows = _read_csv(run_dir / "timeseries.csv")
    for index, row in enumerate(rows):
        row["ego_speed"] = ""
        row["speed_mps"] = ""
        row["v_mps"] = ""
        row["route_s"] = ""
        row["route_completion"] = str(0.1 + 0.2 * index)
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["vehicle_response"]["status"] == "pass"
    assert report["vehicle_response"]["route_completion_delta"] > 0.01
    assert report["failure_stage"] == "none"


def test_refresh_preserves_existing_report_without_raw_handoff_inputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    shutil.copytree(NATURAL_FIXTURE / "lane_keep_097", run_dir)
    expected_path = run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"

    result = ensure_apollo_control_handoff_report(run_dir, refresh=True)
    report = _json(expected_path)

    assert result["status"] == "existing_report_copied"
    assert result["path"] == str(expected_path)
    assert result["source_report"] == str(expected_path)
    assert result["report_status"] == "pass"
    assert report["verdict"] == "pass"
    assert report["failure_stage"] == "none"


def test_refresh_regenerates_handoff_when_raw_inputs_exist(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    existing_path = run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_text(
        json.dumps(
            {
                "schema_version": APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION,
                "verdict": "pass",
                "failure_stage": "none",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    stats = _json(run_dir / "artifacts" / "channel_stats.json")
    stats["channels"]["/apollo/control"]["message_count"] = 0
    _write_json(run_dir / "artifacts" / "channel_stats.json", stats)
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_rx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()

    result = ensure_apollo_control_handoff_report(run_dir, refresh=True)
    regenerated = _json(existing_path)

    assert result["status"] == "generated"
    assert result["report_status"] == "fail"
    assert result["failure_stage"] == "control_channel"
    assert regenerated["verdict"] == "fail"
    assert regenerated["failure_stage"] == "control_channel"


def test_non_refresh_regenerates_stale_handoff_when_raw_inputs_exist(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    existing_path = run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_text(
        json.dumps(
            {
                "schema_version": APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION,
                "verdict": "insufficient_data",
                "failure_stage": "insufficient_data",
                "blocking_reasons": ["control_runtime_messages_missing"],
                "stale_marker": "must_be_regenerated",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    result = ensure_apollo_control_handoff_report(run_dir, refresh=False)
    regenerated = _json(existing_path)

    assert result["status"] == "generated"
    assert result["report_status"] == "pass"
    assert result["failure_stage"] == "none"
    assert "stale_marker" not in regenerated
    assert regenerated["control_channel"]["message_count"] == 10


def test_process_crash_tcmalloc_is_process_health_failure(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "control.err.log").write_text(
        "FATAL tcmalloc invalid free detected\ncore dumped\n",
        encoding="utf-8",
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "process_health"
    assert report["process_health"]["crash_reason"] == "tcmalloc_invalid_free"


def test_deferred_mainboard_invalid_pointer_is_process_health_failure(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "control.out.log").unlink()
    (run_dir / "artifacts" / "apollo_control_deferred_mainboard.log").write_text(
        "src/tcmalloc.cc:333] Attempt to free invalid pointer 0x4020f38fcaf2932d\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts" / "apollo_control_deferred_survival.json").write_text(
        json.dumps(
            {
                "probe_window_sec": 10.0,
                "control_started_pid_seen": True,
                "control_survived_5s": False,
                "control_survived_10s": False,
                "control_present_at_end": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "process_health"
    assert report["process_health"]["status"] == "fail"
    assert report["process_health"]["crash_detected"] is True
    assert report["process_health"]["crash_reason"] == "tcmalloc_invalid_free"
    assert "invalid pointer" in report["process_health"]["crash_signature"].lower()


def test_deferred_survival_artifact_satisfies_process_health(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    summary = _json(run_dir / "summary.json")
    summary.pop("control_process_started", None)
    _write_json(run_dir / "summary.json", summary)
    (run_dir / "artifacts" / "control.out.log").unlink()
    (run_dir / "artifacts" / "apollo_control_deferred_survival.json").write_text(
        json.dumps(
            {
                "probe_window_sec": 10.0,
                "control_started_pid_seen": True,
                "control_survived_5s": True,
                "control_survived_10s": True,
                "control_present_after_first_nonzero_planning": True,
                "control_present_at_end": True,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "pass"
    assert report["failure_stage"] == "none"
    assert report["process_health"]["status"] == "pass"
    assert report["process_health"]["started"] is True
    assert report["process_health"]["alive_after_5s"] is True
    assert report["process_health"]["alive_at_end"] is True
    assert report["process_health"]["survival_probe_available"] is True


def test_planning_ready_control_missing_fails_control_channel(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    stats = _json(run_dir / "artifacts" / "channel_stats.json")
    stats["channels"]["/apollo/control"]["message_count"] = 0
    _write_json(run_dir / "artifacts" / "channel_stats.json", stats)
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_rx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "control_channel"
    assert report["input_readiness"]["status"] in {"pass", "warn"}


def test_input_readiness_accepts_online_planning_topic_debug_field_names(
    tmp_path: Path,
) -> None:
    run_dir = _copy_case(tmp_path)
    summary = _json(run_dir / "summary.json")
    summary.pop("planning_nonempty_count", None)
    summary["planning_materialized"] = False
    _write_json(run_dir / "summary.json", summary)
    _write_json(
        run_dir / "artifacts" / "planning_topic_debug_summary.json",
        {
            "total_messages_received": 112,
            "messages_with_nonzero_trajectory_points": 73,
        },
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["input_readiness"]["planning_message_count"] == 112
    assert report["input_readiness"]["planning_nonempty_count"] == 73
    assert "planning_empty" not in report["input_readiness"]["failure_reasons"]
    assert report["failure_stage"] == "none"


def test_planning_topic_nonzero_but_control_input_trajectory_zero_fails_handoff(
    tmp_path: Path,
) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "planning_topic_debug.jsonl").write_text(
        json.dumps({"trajectory_point_count": 0, "reference_line_count": 0}) + "\n"
        + json.dumps({"trajectory_point_count": 120, "reference_line_count": 0}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts" / "planning_route_segment_debug.jsonl").write_text(
        json.dumps(
            {
                "route_segment_count": 1,
                "reference_line_count": 0,
                "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts" / "apollo_control_raw.jsonl").write_text(
        json.dumps(
            {
                "apollo_control_raw": {
                    "throttle": 0.0,
                    "brake": 15.0,
                    "debug_input_trajectory_header_sequence_num": 0,
                    "debug_input_latest_replan_trajectory_header_sequence_num": 0,
                    "debug_input_trajectory_header_timestamp_sec": 0.0,
                    "debug_input_latest_replan_trajectory_header_timestamp_sec": 0.0,
                }
            }
        )
        + "\n"
        + json.dumps(
            {
                "apollo_control_raw": {
                    "throttle": 0.0,
                    "brake": 15.0,
                    "debug_input_trajectory_header_sequence_num": 0,
                    "debug_input_latest_replan_trajectory_header_sequence_num": 0,
                    "debug_input_trajectory_header_timestamp_sec": 0.0,
                    "debug_input_latest_replan_trajectory_header_timestamp_sec": 0.0,
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "planning_control_handoff"
    handoff = report["planning_control_handoff"]
    assert handoff["status"] == "fail"
    assert handoff["planning_nonzero_trajectory_rows"] == 1
    assert handoff["control_input_trajectory_nonzero_rows"] == 0
    assert handoff["latest_replan_trajectory_nonzero_rows"] == 0
    assert handoff["failure_reasons"] == ["planning_topic_nonzero_but_control_input_trajectory_zero"]
    assert handoff["lane_follow_map_status_counts"] == {
        "trajectory_nonzero_reference_line_debug_missing": 1
    }


def test_control_stream_ended_before_first_nonzero_planning_gets_specific_handoff_reason(
    tmp_path: Path,
) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "planning_topic_debug.jsonl").write_text(
        json.dumps(
            {
                "timestamp": 10.0,
                "planning_header_timestamp_sec": 10.0,
                "trajectory_point_count": 0,
                "reference_line_count": 0,
            }
        )
        + "\n"
        + json.dumps(
            {
                "timestamp": 20.0,
                "planning_header_timestamp_sec": 20.0,
                "trajectory_point_count": 120,
                "reference_line_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts" / "apollo_control_raw.jsonl").write_text(
        json.dumps(
            {
                "ts_sec": 19.0,
                "apollo_control_raw": {
                    "throttle": 0.0,
                    "brake": 15.0,
                    "control_header_timestamp_sec": 19.0,
                    "debug_input_trajectory_header_sequence_num": 0,
                    "debug_input_latest_replan_trajectory_header_sequence_num": 0,
                    "debug_input_trajectory_header_timestamp_sec": 0.0,
                    "debug_input_latest_replan_trajectory_header_timestamp_sec": 0.0,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts" / "control_trajectory_consume_debug_live.jsonl").write_text(
        json.dumps(
            {
                "timestamp": 19.0,
                "control_rx_timestamp": 19.0,
                "latest_planning_msg_sequence_num": 5,
                "latest_planning_trajectory_point_count": 0,
                "control_input_candidate_source": "missing",
                "control_input_candidate_trajectory_header_sequence_num": None,
                "effective_planning_source": "latest_known_fallback",
                "control_used_planning_trajectory": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)
    handoff = report["planning_control_handoff"]

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "planning_control_handoff"
    assert handoff["failure_reasons"] == ["control_stream_ended_before_first_nonzero_planning"]
    assert handoff["first_planning_timestamp_sec"] == 10.0
    assert handoff["last_planning_timestamp_sec"] == 20.0
    assert handoff["first_nonzero_planning_timestamp_sec"] == 20.0
    assert handoff["last_nonzero_planning_timestamp_sec"] == 20.0
    assert handoff["first_control_raw_timestamp_sec"] == 19.0
    assert handoff["last_control_raw_timestamp_sec"] == 19.0
    assert handoff["first_control_consume_timestamp_sec"] == 19.0
    assert handoff["last_control_consume_timestamp_sec"] == 19.0
    assert handoff["first_control_timestamp_sec"] == 19.0
    assert handoff["last_control_timestamp_sec"] == 19.0
    assert handoff["planning_stream_span_sec"] == 10.0
    assert handoff["planning_nonzero_stream_span_sec"] == 0.0
    assert handoff["control_stream_span_sec"] == 0.0
    assert handoff["control_rows_after_first_nonzero_planning"] == 0
    assert handoff["planning_rows_after_control_stream_end"] == 1
    assert handoff["planning_nonzero_rows_after_control_stream_end"] == 1
    assert handoff["planning_continued_after_control_stream_end"] is True
    assert handoff["control_stream_ended_before_first_nonzero_planning"] is True
    assert handoff["control_stream_end_before_first_nonzero_planning_delta_ms"] == 1000.0
    assert handoff["control_consume_candidate_source_counts"] == {"missing": 1}
    assert handoff["control_consume_effective_planning_source_counts"] == {
        "latest_known_fallback": 1
    }


def test_control_channel_present_but_bridge_missing_fails_bridge_receive(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_rx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "bridge_receive"


def test_bridge_receive_but_decode_missing_fails_raw_decode(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        for field in ("apollo_steer_raw", "throttle_raw", "brake_raw"):
            row[field] = ""
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "raw_decode"


def test_mapped_control_without_applied_control_fails_mapping_and_apply(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "direct_bridge_control_apply.jsonl").unlink()
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_tx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        for field in ("carla_steer_applied", "throttle_applied", "brake_applied"):
            row[field] = "0.0"
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "mapping_and_apply"
    assert report["mapping_and_apply"]["apply_control_count"] == 0


def test_bridge_decode_mapped_control_counts_when_timeseries_raw_mapped_missing(
    tmp_path: Path,
) -> None:
    run_dir = _copy_case(tmp_path)
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        for field in ("bridge_steer_mapped", "throttle_mapped", "brake_mapped"):
            row[field] = ""
        row["throttle_applied"] = "0.2"
        row["brake_applied"] = "0.0"
        row["carla_steer_applied"] = "0.0"
    _write_csv(run_dir / "timeseries.csv", rows)

    (run_dir / "artifacts" / "control_decode_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "parsed_control": {"throttle": 0.2, "brake": 0.0, "steer": 0.0},
                    "output_to_carla": {
                        "mapped_throttle_cmd": 0.2,
                        "mapped_brake_cmd": 0.0,
                        "mapped_carla_steer_cmd": 0.0,
                    },
                }
            )
            for _ in rows
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink(missing_ok=True)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["mapping_and_apply"]["status"] == "pass"
    assert report["mapping_and_apply"]["nonzero_mapped_frames"] == len(rows)
    assert report["mapping_and_apply"]["nonzero_mapped_frames_source"] == "control_decode_debug.jsonl"
    assert "mapping_and_apply.mapped_control" not in report["missing_fields"]


def test_applied_control_without_vehicle_response_fails_vehicle_response(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        row["route_s"] = "0.0"
        row["ego_speed"] = "0.0"
        row["ego_yaw_rate"] = "0.0"
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "vehicle_response"


def test_cli_generates_report(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    assert "failure_stage" in result.stdout
    assert (out / "apollo_control_handoff_report.json").is_file()
    assert (out / "apollo_control_handoff_summary.md").is_file()


def test_natural_driving_missing_handoff_report_cannot_pass(tmp_path: Path) -> None:
    suite = tmp_path / "suite"
    shutil.copytree(NATURAL_FIXTURE, suite)
    handoff_path = (
        suite
        / "lane_keep_097"
        / "analysis"
        / "apollo_control_handoff"
        / "apollo_control_handoff_report.json"
    )
    handoff_path.unlink()

    report = analyze_natural_driving_suite(suite)
    lane = next(row for row in report["run_results"] if row["scenario_id"] == "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "apollo_control_handoff_report_missing"
    assert "apollo_control_handoff_report.json" in lane["missing_fields"]


def test_natural_driving_failed_handoff_report_blocks_hard_pass(tmp_path: Path) -> None:
    suite = tmp_path / "suite"
    shutil.copytree(NATURAL_FIXTURE, suite)
    handoff_path = (
        suite
        / "lane_keep_097"
        / "analysis"
        / "apollo_control_handoff"
        / "apollo_control_handoff_report.json"
    )
    payload = _json(handoff_path)
    payload["verdict"] = "fail"
    payload["failure_stage"] = "control_channel"
    payload["blocking_reasons"] = ["control_channel_failed"]
    payload["control_channel"]["message_count"] = 0
    _write_json(handoff_path, payload)

    report = analyze_natural_driving_suite(suite)
    lane = next(row for row in report["run_results"] if row["scenario_id"] == "lane_keep_097")

    assert lane["verdict"] == "fail"
    assert lane["failure_reason"] == "apollo_control_handoff_control_channel"
    assert lane["apollo_control_handoff_status"] == "fail"
    assert lane["apollo_control_handoff_failure_stage"] == "control_channel"


def test_natural_driving_accepts_nonblocking_handoff_warn(tmp_path: Path) -> None:
    suite = tmp_path / "suite"
    shutil.copytree(NATURAL_FIXTURE, suite)
    handoff_path = (
        suite
        / "lane_keep_097"
        / "analysis"
        / "apollo_control_handoff"
        / "apollo_control_handoff_report.json"
    )
    payload = _json(handoff_path)
    payload["verdict"] = "warn"
    payload["failure_stage"] = "none"
    payload["warnings"] = ["control_pad_requirement_unknown"]
    payload["blocking_reasons"] = []
    _write_json(handoff_path, payload)

    report = analyze_natural_driving_suite(suite)
    lane = next(row for row in report["run_results"] if row["scenario_id"] == "lane_keep_097")

    assert lane["apollo_control_handoff_status"] == "warn"
    assert lane["apollo_control_handoff_failure_stage"] == "none"
    assert "apollo_control_handoff.verdict" not in lane["missing_fields"]
    assert "control_handoff_not_pass" not in lane["why_not_claimable"]


def test_ab_candidate_positive_blocked_when_handoff_fails() -> None:
    baseline = {
        "run_id": "baseline_lane097",
        "route_id": "lane097",
        "backend": "ros2_gt",
        "duration_s": 30.0,
        "run_status": "success",
        "return_code": None,
        "artifact_complete": True,
        "route_completion": 0.7,
        "lateral_error_p95_m": 0.5,
        "heading_error_p95_rad": 0.02,
        "planning_hz": 10.0,
        "carla_applied_control_hz": 20.0,
        "localization_hz": 20.0,
        "chassis_hz": 20.0,
        "route_hard_gate_eligible": True,
        "failure_reason": "success",
        "apollo_control_handoff_status": "pass",
        "apollo_control_handoff_failure_stage": "none",
    }
    candidate = {
        **baseline,
        "run_id": "candidate_lane097",
        "backend": "carla_direct",
        "direct_transport_contract_status": "aligned",
        "apollo_control_handoff_status": "fail",
        "apollo_control_handoff_failure_stage": "bridge_receive",
    }

    comparison = _compare_pair(baseline, candidate)

    assert comparison["status"] == "insufficient_data"
    assert "candidate apollo_control_handoff failed: bridge_receive" in comparison["reasons"]
