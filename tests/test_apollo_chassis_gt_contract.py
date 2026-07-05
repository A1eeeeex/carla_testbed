from __future__ import annotations

import csv
import json
from pathlib import Path

from carla_testbed.analysis.chassis_gt_contract import (
    analyze_chassis_gt_contract_files,
    write_chassis_gt_contract_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_timeseries(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted({key for row in rows for key in row}))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_channel_stats(path: Path, **overrides: object) -> None:
    payload = {
        "schema_version": "channel_stats.v1",
        "channels": {
            "/apollo/canbus/chassis": {
                "message_count": 100,
                "hz": 20.0,
                "max_gap_ms": 80.0,
                "timestamp_monotonic": True,
                "sequence_monotonic": True,
                "stale_count": 0,
            }
        },
    }
    payload["channels"]["/apollo/canbus/chassis"].update(overrides)
    _write_json(path, payload)


def _write_complete_run(run_dir: Path) -> None:
    _write_json(run_dir / "summary.json", {"run_id": "run", "route_id": "097", "backend": "apollo_cyberrt"})
    _write_channel_stats(run_dir / "channel_stats.json")
    _write_timeseries(
        run_dir / "timeseries.csv",
        [
            {
                "sim_time": index * 0.05,
                "chassis_speed_mps": 10.0 + index * 0.01,
                "ego_speed_mps": 10.0 + index * 0.01,
                "driving_mode": "COMPLETE_AUTO_DRIVE",
                "gear_location": "GEAR_DRIVE",
                "chassis_error_code": "NO_ERROR",
                "throttle_applied": 0.20,
                "throttle_feedback": 0.21,
                "brake_applied": 0.0,
                "brake_feedback": 0.0,
                "carla_steer_applied": 0.01,
                "chassis_steering_percentage": 0.01,
                "steering_sign": 1.0,
            }
            for index in range(20)
        ],
    )


def test_chassis_gt_contract_complete_fixture_passes(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "pass"
    assert report["claim_grade"] is True
    assert report["channel"]["message_count"] == 100
    assert report["speed_consistency"]["speed_delta_p95_mps"] == 0.0
    assert report["blocking_reasons"] == []


def test_chassis_gt_contract_missing_channel_stats_is_insufficient(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    (run_dir / "channel_stats.json").unlink()

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert "channel_stats.chassis" in report["missing_fields"]


def test_chassis_gt_contract_run_dir_prefers_debug_timeseries_with_chassis_fields(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(run_dir / "summary.json", {"run_id": "run", "route_id": "097", "backend": "apollo_cyberrt"})
    _write_channel_stats(run_dir / "channel_stats.json")
    _write_timeseries(
        run_dir / "timeseries.csv",
        [{"sim_time": index * 0.05, "ego_x": float(index)} for index in range(5)],
    )
    _write_timeseries(
        run_dir / "artifacts/debug_timeseries.csv",
        [
            {
                "sim_time": index * 0.05,
                "chassis_speed_mps": 7.0,
                "localization_speed_mps": 7.0,
            }
            for index in range(5)
        ],
    )

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "warn"
    assert report["claim_grade"] is False
    assert report["speed_consistency"]["sample_count"] == 5
    assert report["speed_consistency"]["speed_delta_p95_mps"] == 0.0
    assert "driving_mode" in report["missing_fields"]


def test_chassis_gt_contract_accepts_bridge_debug_feedback_aliases(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(run_dir / "summary.json", {"run_id": "run", "route_id": "097", "backend": "apollo_cyberrt"})
    _write_channel_stats(run_dir / "channel_stats.json")
    _write_timeseries(
        run_dir / "artifacts/debug_timeseries.csv",
        [
            {
                "sim_time": index * 0.05,
                "chassis_speed_mps": 7.0,
                "localization_speed_mps": 7.0,
                "driving_mode": "COMPLETE_AUTO_DRIVE",
                "gear_location_value": 1,
                "chassis_error_code_value": 0,
                "commanded_throttle": 0.20,
                "measured_throttle": 0.20,
                "commanded_brake": 0.0,
                "measured_brake": 0.0,
                "carla_steer_applied": -0.12,
                "measured_steer_pct": -12.0,
            }
            for index in range(5)
        ],
    )

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "pass"
    assert report["claim_grade"] is True
    assert report["control_feedback"]["throttle_feedback_available"] is True
    assert report["control_feedback"]["steer_applied_feedback_delta_p95"] == 0.0
    assert report["state"]["gear_locations"] == ["1"]
    assert report["state"]["error_codes"] == ["0"]


def test_chassis_gt_contract_non_monotonic_channel_fails(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    _write_channel_stats(run_dir / "channel_stats.json", timestamp_monotonic=False)

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "fail"
    assert "chassis_timestamp_non_monotonic" in report["blocking_reasons"]


def test_chassis_gt_contract_speed_mismatch_fails(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    _write_timeseries(
        run_dir / "timeseries.csv",
        [
            {
                "chassis_speed_mps": 4.0,
                "ego_speed_mps": 8.0,
                "driving_mode": "COMPLETE_AUTO_DRIVE",
                "gear_location": "GEAR_DRIVE",
                "chassis_error_code": "NO_ERROR",
            }
            for _ in range(10)
        ],
    )

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "fail"
    assert "chassis_speed_mismatch_high" in report["blocking_reasons"]


def test_chassis_gt_contract_steer_feedback_mismatch_fails(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    _write_timeseries(
        run_dir / "artifacts/debug_timeseries.csv",
        [
            {
                "sim_time": index * 0.05,
                "chassis_speed_mps": 7.0,
                "localization_speed_mps": 7.0,
                "driving_mode": "COMPLETE_AUTO_DRIVE",
                "gear_location": "GEAR_DRIVE",
                "chassis_error_code": "NO_ERROR",
                "commanded_throttle": 0.20,
                "measured_throttle": 0.20,
                "commanded_brake": 0.0,
                "measured_brake": 0.0,
                "carla_steer_applied": 1.0,
                "chassis_steering_percentage": -80.0,
            }
            for index in range(20)
        ],
    )

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "fail"
    assert report["claim_grade"] is False
    assert "chassis_steer_feedback_mismatch_high" in report["blocking_reasons"]
    assert report["control_feedback"]["steer_applied_feedback_delta_p95"] == 1.8


def test_chassis_gt_contract_converts_apollo_frame_steer_feedback_with_steering_sign(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    _write_timeseries(
        run_dir / "artifacts/debug_timeseries.csv",
        [
            {
                "sim_time": index * 0.05,
                "chassis_speed_mps": 7.0,
                "localization_speed_mps": 7.0,
                "driving_mode": "COMPLETE_AUTO_DRIVE",
                "gear_location": "GEAR_DRIVE",
                "chassis_error_code": "NO_ERROR",
                "commanded_throttle": 0.20,
                "measured_throttle": 0.20,
                "commanded_brake": 0.0,
                "measured_brake": 0.0,
                "carla_steer_applied": -0.50,
                "chassis_steering_percentage": 50.0,
                "steering_sign": -1.0,
            }
            for index in range(20)
        ],
    )

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "pass"
    assert report["claim_grade"] is True
    assert report["control_feedback"]["steer_applied_feedback_delta_p95"] == 0.0


def test_chassis_gt_contract_prefers_explicit_carla_steer_feedback_over_apollo_frame(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    _write_timeseries(
        run_dir / "artifacts/debug_timeseries.csv",
        [
            {
                "sim_time": index * 0.05,
                "chassis_speed_mps": 7.0,
                "localization_speed_mps": 7.0,
                "driving_mode": "COMPLETE_AUTO_DRIVE",
                "gear_location": "GEAR_DRIVE",
                "chassis_error_code": "NO_ERROR",
                "commanded_throttle": 0.20,
                "measured_throttle": 0.20,
                "commanded_brake": 0.0,
                "measured_brake": 0.0,
                "carla_steer_applied": -0.50,
                "chassis_steering_percentage": 50.0,
                "chassis_carla_steering_percentage": -50.0,
            }
            for index in range(20)
        ],
    )

    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    assert report["status"] == "pass"
    assert report["claim_grade"] is True
    assert report["control_feedback"]["steer_applied_feedback_delta_p95"] == 0.0


def test_chassis_gt_contract_report_writer(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_complete_run(run_dir)
    report = analyze_chassis_gt_contract_files(run_dir=run_dir)

    outputs = write_chassis_gt_contract_report(report, tmp_path / "out")

    assert Path(outputs["chassis_gt_contract_report"]).is_file()
    assert Path(outputs["chassis_gt_contract_summary"]).is_file()
