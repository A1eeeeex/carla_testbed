from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.natural_driving_postprocess import postprocess_natural_driving_runs
from carla_testbed.analysis.route_start_alignment import (
    ROUTE_START_ALIGNMENT_SCHEMA_VERSION,
    analyze_route_start_alignment_run_dir,
    write_route_start_alignment_report,
)


FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "route_start_run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "route_start_run",
            "route_id": "lane097",
            "scenario_class": "lane_keep",
            "metrics": {"lane_invasion_count": 1},
            "exit_reason": "LANE_INVASION",
        },
    )
    _write_json(run_dir / "manifest.json", {"run_id": "route_start_run", "route_id": "lane097"})
    _write_json(
        run_dir / "route.json",
        {
            "route_id": "lane097",
            "map": "Town01",
            "spawn_pose": {"x": 0.0, "y": 0.45, "yaw": 0.0},
            "points": [
                {"index": 0, "x": 0.0, "y": 0.0, "z": 0.0, "s": 0.0, "heading": 0.0},
                {"index": 1, "x": 10.0, "y": 0.0, "z": 0.0, "s": 10.0, "heading": 0.0},
            ],
        },
    )
    with (run_dir / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time",
                "route_s",
                "cross_track_error",
                "heading_error",
                "ego_speed",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": "0.0",
                "route_s": "-1.4235",
                "cross_track_error": "0.45",
                "heading_error": "0.0",
                "ego_speed": "0.0",
            }
        )
    _write_json(
        run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json",
        {
            "schema_version": "town01_failure_timeline.v1",
            "status": "pass",
            "ordering_findings": ["safety_event_before_route_start"],
            "route_start_gate": {
                "route_s_at_anchor": -0.5,
                "anchor_before_route_start": True,
                "anchor_near_route_start": True,
                "window_route_s_min": -1.0,
                "window_route_s_max": 0.2,
            },
            "anchor_event": {
                "event_type": "lane_invasion",
                "row_context": {
                    "route_s": -0.5,
                    "cross_track_error": 0.46,
                    "heading_error": 0.01,
                },
            },
        },
    )
    return run_dir


def test_route_start_alignment_detects_start_gate_and_rear_axle_offset(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)

    report = analyze_route_start_alignment_run_dir(run_dir)

    assert report["schema_version"] == ROUTE_START_ALIGNMENT_SCHEMA_VERSION
    assert report["status"] == "warn"
    assert report["reason"] == "failure_before_route_start"
    assert report["static_spawn_alignment"]["spawn_lateral_offset_m"] == 0.45
    assert report["initial_ego_alignment"]["rear_axle_offset_compatible"] is True
    assert report["initial_ego_alignment"]["rear_axle_offset_error_m"] == 0.0
    assert report["failure_anchor_alignment"]["route_s"] == -0.5
    assert report["recommendation"]["available"] is True
    assert report["recommendation"]["action"] == "probe_spawn_lateral_alignment"
    assert report["recommendation"]["recommended_config_field"] == "scenario.route_health.ego_offset_y_m"
    assert report["recommendation"]["recommended_ego_offset_y_delta_m"] == -0.45
    assert "rear_axle_localization_offset_compatible" in report["hypotheses"]


def test_route_start_alignment_does_not_repeat_compensated_static_spawn_probe(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    timeline_path = run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
    timeline["route_start_gate"] = {
        "route_s_at_anchor": 21.6,
        "anchor_before_route_start": False,
        "anchor_near_route_start": False,
        "window_route_s_min": 20.0,
        "window_route_s_max": 23.0,
    }
    timeline["anchor_event"] = {
        "event_type": "lane_invasion",
        "row_context": {
            "route_s": 21.6,
            "cross_track_error": 0.02,
            "heading_error": 0.006,
        },
    }
    timeline_path.write_text(json.dumps(timeline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    timeseries_path = run_dir / "timeseries.csv"
    with timeseries_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time",
                "route_s",
                "cross_track_error",
                "heading_error",
                "ego_speed",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": "0.0",
                "route_s": "-1.4235",
                "cross_track_error": "0.0",
                "heading_error": "0.0",
                "ego_speed": "0.0",
            }
        )

    report = analyze_route_start_alignment_run_dir(run_dir)

    assert report["status"] == "warn"
    assert report["reason"] == "spawn_lateral_offset_high"
    assert report["initial_ego_alignment"]["rear_axle_offset_compatible"] is True
    assert report["recommendation"]["available"] is False
    assert (
        report["recommendation"]["reason"]
        == "spawn_lateral_offset_already_compensated_by_initial_alignment"
    )


def test_route_start_alignment_recommends_probe_when_initial_lateral_error_remains_high(
    tmp_path: Path,
) -> None:
    run_dir = _write_run(tmp_path)
    timeline_path = run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
    timeline["route_start_gate"] = {
        "route_s_at_anchor": 21.6,
        "anchor_before_route_start": False,
        "anchor_near_route_start": False,
        "window_route_s_min": 20.0,
        "window_route_s_max": 23.0,
    }
    timeline["anchor_event"] = {
        "event_type": "lane_invasion",
        "row_context": {
            "route_s": 21.6,
            "cross_track_error": 0.46,
            "heading_error": 0.006,
        },
    }
    timeline_path.write_text(json.dumps(timeline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    timeseries_path = run_dir / "timeseries.csv"
    with timeseries_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time",
                "route_s",
                "cross_track_error",
                "heading_error",
                "ego_speed",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": "0.0",
                "route_s": "-1.4235",
                "cross_track_error": "0.46",
                "heading_error": "0.0",
                "ego_speed": "0.0",
            }
        )

    report = analyze_route_start_alignment_run_dir(run_dir)

    assert report["status"] == "warn"
    assert report["reason"] == "spawn_lateral_offset_high"
    assert report["initial_ego_alignment"]["rear_axle_offset_compatible"] is True
    assert report["recommendation"]["available"] is True
    assert report["recommendation"]["action"] == "probe_spawn_lateral_alignment"
    assert report["recommendation"]["recommended_ego_offset_y_delta_m"] == -0.45


def test_route_start_alignment_ignores_non_actionable_semantic_anchor_without_ordering_findings(
    tmp_path: Path,
) -> None:
    run_dir = _write_run(tmp_path)
    timeline_path = run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    timeline = json.loads(timeline_path.read_text(encoding="utf-8"))
    timeline["ordering_findings"] = []
    timeline["anchor_event"] = {
        "event_type": "first_high_steer",
        "row_context": {
            "route_s": -1.0,
            "cross_track_error": 0.45,
            "heading_error": 0.001,
        },
    }
    timeline_path.write_text(json.dumps(timeline, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = analyze_route_start_alignment_run_dir(run_dir)

    assert report["failure_anchor_alignment"]["anchor_event_type"] == "first_high_steer"
    assert report["status"] == "warn"
    assert report["reason"] == "spawn_lateral_offset_high"
    assert "failure_anchor_before_route_start" not in report["warnings"]
    assert report["recommendation"]["available"] is False
    assert (
        report["recommendation"]["reason"]
        == "spawn_lateral_offset_already_compensated_by_initial_alignment"
    )


def test_route_start_alignment_writes_report_and_cli(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    outputs = write_route_start_alignment_report(
        analyze_route_start_alignment_run_dir(run_dir),
        tmp_path / "out",
    )

    assert Path(outputs["route_start_alignment_report"]).is_file()
    assert Path(outputs["route_start_alignment_summary"]).is_file()

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_route_start_alignment.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(tmp_path / "cli_out"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["reason"] == "failure_before_route_start"


def test_route_start_alignment_missing_inputs_graceful_degrade(tmp_path: Path) -> None:
    run_dir = tmp_path / "empty_run"
    run_dir.mkdir()

    report = analyze_route_start_alignment_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert "route" in report["missing_inputs"]
    assert "timeseries" in report["missing_inputs"]


def test_postprocess_generates_route_start_alignment(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    shutil.copytree(FIXTURE_ROOT, suite_root)
    lane = suite_root / "lane_keep_097"
    shutil.rmtree(lane / "analysis" / "route_start_alignment", ignore_errors=True)

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")

    assert lane_result["route_start_alignment"]["status"] == "generated"
    assert Path(lane_result["route_start_alignment"]["path"]).is_file()
