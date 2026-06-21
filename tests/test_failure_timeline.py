from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.failure_timeline import (
    FAILURE_TIMELINE_SCHEMA_VERSION,
    analyze_failure_timeline_run_dir,
    write_failure_timeline_report,
)
from carla_testbed.analysis.natural_driving_postprocess import postprocess_natural_driving_runs


FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_timeseries(path: Path) -> None:
    rows = [
        {
            "sim_time": "0.0",
            "route_s": "0.0",
            "cross_track_error": "0.05",
            "heading_error": "0.01",
            "ego_speed": "1.0",
            "apollo_steer_raw": "0.02",
            "bridge_steer_mapped": "0.005",
            "carla_steer_applied": "0.005",
            "apollo_matched_point_distance": "0.10",
            "apollo_target_point_distance": "0.50",
            "lateral_guard_applied": "false",
            "trajectory_contract_guard_applied": "false",
        },
        {
            "sim_time": "1.0",
            "route_s": "4.0",
            "cross_track_error": "0.22",
            "heading_error": "0.04",
            "ego_speed": "1.3",
            "apollo_steer_raw": "0.80",
            "bridge_steer_mapped": "0.20",
            "carla_steer_applied": "0.20",
            "apollo_matched_point_distance": "0.20",
            "apollo_target_point_distance": "0.70",
            "lateral_guard_applied": "false",
            "trajectory_contract_guard_applied": "false",
        },
        {
            "sim_time": "2.0",
            "route_s": "8.0",
            "cross_track_error": "1.20",
            "heading_error": "0.12",
            "ego_speed": "1.1",
            "apollo_steer_raw": "0.35",
            "bridge_steer_mapped": "0.09",
            "carla_steer_applied": "0.09",
            "apollo_matched_point_distance": "0.30",
            "apollo_target_point_distance": "1.20",
            "lateral_guard_applied": "false",
            "trajectory_contract_guard_applied": "false",
        },
        {
            "sim_time": "3.0",
            "route_s": "12.0",
            "cross_track_error": "1.40",
            "heading_error": "0.18",
            "ego_speed": "0.9",
            "apollo_steer_raw": "0.30",
            "bridge_steer_mapped": "0.08",
            "carla_steer_applied": "0.08",
            "apollo_matched_point_distance": "5.40",
            "apollo_target_point_distance": "1.40",
            "lateral_guard_applied": "false",
            "trajectory_contract_guard_applied": "false",
        },
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _make_failed_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "failed_lane"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "failed_lane",
            "route_id": "lane097",
            "scenario_class": "lane_keep",
            "exit_reason": "LANE_INVASION",
            "first_failure_step": 2,
            "metrics": {
                "lane_invasion_count": 1,
                "collision_count": 0,
            },
        },
    )
    _write_json(run_dir / "manifest.json", {"run_id": "failed_lane", "route_id": "lane097"})
    _write_timeseries(run_dir / "timeseries.csv")
    _write_jsonl(
        run_dir / "events.jsonl",
        [
            {
                "event_type": "lane_invasion",
                "source": "summary.json",
                "step": 2,
                "reason": "LANE_INVASION",
            }
        ],
    )
    _write_json(
        run_dir / "analysis" / "route_health" / "route_health.json",
        {
            "route_id": "lane097",
            "verdict": {"status": "fail"},
            "apollo_semantics": {
                "first_high_steer": {"seq": 1, "at": 1.0, "value": 0.8},
                "first_matched_point_too_large": {"seq": 3, "at": 3.0, "distance_m": 5.4},
                "matched_point_anomaly_locations": [3],
                "target_point_anomaly_locations": [],
            },
        },
    )
    _write_json(
        run_dir / "analysis" / "control_health" / "control_health_report.json",
        {
            "status": "warn",
            "failure_reason": "control_health_warn",
            "warnings": ["control_latency_missing"],
        },
    )
    return run_dir


def test_failure_timeline_orders_key_events_and_writes_report(tmp_path: Path) -> None:
    run_dir = _make_failed_run(tmp_path)

    report = analyze_failure_timeline_run_dir(run_dir, window_rows=1)
    outputs = write_failure_timeline_report(report, tmp_path / "out")

    assert report["schema_version"] == FAILURE_TIMELINE_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["primary_failure"]["failure_reason"] == "lane_invasion"
    assert report["anchor_event"]["event_type"] == "lane_invasion"
    assert "first_high_steer_before_safety_event" in report["ordering_findings"]
    assert "safety_event_before_matched_point_anomaly" in report["ordering_findings"]
    assert "control_health_nonpass_present" in report["ordering_findings"]
    assert report["route_start_gate"]["route_s_at_anchor"] == 8.0
    assert report["route_start_gate"]["anchor_near_route_start"] is False
    assert report["window_summary"]["route_s_min"] == 4.0
    assert report["window_summary"]["route_s_max"] == 12.0
    assert Path(outputs["failure_timeline_report"]).is_file()
    assert Path(outputs["failure_timeline_summary"]).is_file()


def test_failure_timeline_uses_event_t_timestamp(tmp_path: Path) -> None:
    run_dir = _make_failed_run(tmp_path)
    _write_jsonl(
        run_dir / "events.jsonl",
        [
            {
                "event_type": "lane_invasion",
                "source": "events.jsonl",
                "step": 2,
                "t": 12.34,
                "crossed_lane_marking_types": ["Broken"],
            }
        ],
    )

    report = analyze_failure_timeline_run_dir(run_dir, window_rows=1)

    assert report["anchor_event"]["event_type"] == "lane_invasion"
    assert report["anchor_event"]["timestamp_sec"] == 12.34


def test_failure_timeline_missing_timeseries_graceful_degrades(tmp_path: Path) -> None:
    run_dir = _make_failed_run(tmp_path)
    (run_dir / "timeseries.csv").unlink()

    report = analyze_failure_timeline_run_dir(run_dir)

    assert report["status"] == "warn"
    assert "timeseries" in report["missing_inputs"]
    assert report["window_summary"] is None
    assert "timeseries_context_missing" in report["warnings"]


def test_failure_timeline_flags_safety_event_near_route_start(tmp_path: Path) -> None:
    run_dir = _make_failed_run(tmp_path)
    rows = []
    with (run_dir / "timeseries.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[2]["route_s"] = "-0.5"
    with (run_dir / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    report = analyze_failure_timeline_run_dir(run_dir)

    assert report["route_start_gate"]["status"] == "warn"
    assert report["route_start_gate"]["reason"] == "anchor_before_route_start"
    assert report["route_start_gate"]["anchor_before_route_start"] is True
    assert "safety_event_before_route_start" in report["ordering_findings"]
    assert "safety_event_near_route_start" in report["ordering_findings"]


def test_failure_timeline_cli_writes_outputs(tmp_path: Path) -> None:
    run_dir = _make_failed_run(tmp_path)
    out_dir = tmp_path / "timeline"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_town01_failure_timeline.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert Path(payload["outputs"]["failure_timeline_report"]).is_file()


def test_natural_driving_postprocess_generates_failure_timeline(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    shutil.copytree(FIXTURE_ROOT, suite_root)
    lane = suite_root / "lane_keep_097"
    shutil.rmtree(lane / "analysis" / "failure_timeline", ignore_errors=True)

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")

    assert lane_result["failure_timeline"]["status"] == "generated"
    assert Path(lane_result["failure_timeline"]["path"]).is_file()
