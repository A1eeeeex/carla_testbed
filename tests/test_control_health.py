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
                    "[INFO] [1003.000000000] [carla_control_bridge]: apply frame=11 source=pending "
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
                    "parsed_control": {"control_latency_ms": 1.0},
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

    assert report["raw_mapped_applied_control_available"] is True
    assert report["raw_mapped_applied_control_source"] == "bridge_decode_plus_timeseries"
    assert report["status"] == "fail"
    assert report["failure_reason"] == "applied_actuation_oscillation"
    assert "external_control_trace_from_bridge_artifacts" in report["warnings"]


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
        row["throttle_mapped"] = "0.8" if index % 2 == 0 else "0.0"
        row["brake_mapped"] = "0.0" if index % 2 == 0 else "0.7"
        row["bridge_steer_mapped"] = "0.0"
        row["throttle_applied"] = row["throttle_mapped"]
        row["brake_applied"] = row["brake_mapped"]
        row["carla_steer_applied"] = "0.0"
    _write_csv(csv_path, rows)

    report = analyze_control_health_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["failure_reason"] == "applied_actuation_oscillation"
    assert report["metrics"]["applied_throttle_brake_switch_count"] == 11
    assert report["metrics"]["applied_throttle_frames"] == 6
    assert report["metrics"]["applied_brake_frames"] == 6


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
