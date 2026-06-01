from __future__ import annotations

import csv
import json
from pathlib import Path

from carla_testbed.calibration.control_actuation import CALIBRATION_TRIAL_FIELDS
from carla_testbed.calibration.trial_extraction import (
    discover_timeseries_paths,
    extract_control_actuation_trials,
    load_timeseries_rows,
    write_calibration_trials_csv,
)
from tools.extract_calibration_trials import main as extract_trials_main


def _rows() -> list[dict[str, object]]:
    return [
        {
            "sim_time": 0.0,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.2,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.0,
            "ego_yaw_rate": 0.0,
            "control_latency_ms": 20,
        },
        {
            "sim_time": 0.1,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.2,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.1,
            "ego_yaw_rate": 0.0,
            "control_latency_ms": 30,
        },
        {
            "sim_time": 0.2,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.2,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.2,
            "ego_yaw_rate": 0.0,
            "control_latency_ms": 40,
        },
        {
            "sim_time": 0.3,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.2,
            "ego_yaw_rate": 0.0,
        },
        {
            "sim_time": 0.4,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.3,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.2,
            "ego_yaw_rate": 0.0,
        },
        {
            "sim_time": 0.5,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.3,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.1,
            "ego_yaw_rate": 0.0,
        },
        {
            "sim_time": 0.6,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.3,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.0,
            "ego_yaw_rate": 0.0,
        },
        {
            "sim_time": 0.7,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.0,
            "ego_speed": 1.0,
            "ego_yaw_rate": 0.0,
        },
        {
            "sim_time": 0.8,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.2,
            "ego_speed": 1.0,
            "ego_yaw_rate": 0.03,
        },
        {
            "sim_time": 0.9,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.2,
            "ego_speed": 1.0,
            "ego_yaw_rate": 0.04,
        },
        {
            "sim_time": 1.0,
            "route_id": "097",
            "backend": "carla_direct",
            "throttle_applied": 0.0,
            "brake_applied": 0.0,
            "carla_steer_applied": 0.2,
            "ego_speed": 1.0,
            "ego_yaw_rate": 0.05,
        },
    ]


def test_extract_trials_from_p0_timeseries_segments() -> None:
    trials = extract_control_actuation_trials(_rows(), min_duration_s=0.1)
    assert [row["command_type"] for row in trials] == ["throttle", "brake", "steer"]
    assert {field for row in trials for field in row}.issuperset(CALIBRATION_TRIAL_FIELDS)

    throttle = trials[0]
    assert throttle["route_id"] == "097"
    assert throttle["backend"] == "carla_direct"
    assert abs(float(throttle["command_value"]) - 0.2) < 1e-9
    assert throttle["accel_mean_mps2"] is not None
    assert throttle["latency_ms"] is not None

    brake = trials[1]
    assert brake["decel_mean_mps2"] is not None

    steer = trials[2]
    assert steer["yaw_rate_mean_rad_s"] is not None
    assert "latency_missing" in str(steer["notes"])


def test_missing_yaw_and_latency_degrades_to_empty_fields(tmp_path: Path) -> None:
    rows = [
        {"sim_time": 0.0, "frame_id": 0, "carla_steer_applied": 0.3, "ego_speed": 1.0},
        {"sim_time": 0.1, "frame_id": 1, "carla_steer_applied": 0.3, "ego_speed": 1.0},
        {"sim_time": 0.2, "frame_id": 2, "carla_steer_applied": 0.3, "ego_speed": 1.0},
    ]
    trials = extract_control_actuation_trials(rows, route_id="curve217", backend="ros2_gt")
    assert len(trials) == 1
    assert trials[0]["yaw_rate_mean_rad_s"] is None
    assert trials[0]["latency_ms"] is None
    assert "yaw_rate_missing" in str(trials[0]["notes"])

    out = tmp_path / "calibration_trials.csv"
    write_calibration_trials_csv(out, trials)
    with out.open(encoding="utf-8", newline="") as handle:
        loaded = list(csv.DictReader(handle))
    assert loaded[0]["yaw_rate_mean_rad_s"] == ""
    assert loaded[0]["latency_ms"] == ""
    assert loaded[0]["route_id"] == "curve217"


def test_jsonl_loading_and_fixed_dt_fallback(tmp_path: Path) -> None:
    path = tmp_path / "timeseries.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"frame_id": 0, "throttle_applied": 0.1, "ego_speed": 0.0}),
                json.dumps({"frame_id": 1, "throttle_applied": 0.1, "ego_speed": 0.1}),
                json.dumps({"frame_id": 2, "throttle_applied": 0.1, "ego_speed": 0.2}),
            ]
        ),
        encoding="utf-8",
    )
    rows = load_timeseries_rows(path)
    trials = extract_control_actuation_trials(rows, fixed_dt_s=0.05, min_duration_s=0.09)
    assert len(trials) == 1
    assert trials[0]["duration_s"] == 0.1


def test_cli_discovers_run_dir_timeseries_and_writes_csv(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    ts_path = run_dir / "timeseries.csv"
    with ts_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_rows()[0]))
        writer.writeheader()
        writer.writerows(_rows())

    assert discover_timeseries_paths(run_dir) == [ts_path]
    out = tmp_path / "calibration_trials.csv"
    rc = extract_trials_main(["--run-dir", str(run_dir), "--out", str(out), "--min-duration-s", "0.1"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["trial_count"] == 3
    with out.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert set(rows[0]) == set(CALIBRATION_TRIAL_FIELDS)
