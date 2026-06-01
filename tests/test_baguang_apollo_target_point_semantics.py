from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_apollo_target_point_semantics import (
    analyze_baguang_apollo_target_point_semantics,
    analyze_baguang_apollo_target_point_semantics_run,
    write_baguang_apollo_target_point_semantics_report,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_lateral_geometry(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["reference_lane_curvature"])
        writer.writeheader()
        writer.writerow({"reference_lane_curvature": 0.0})


def _write_run(root: Path, *, success: bool) -> None:
    _write_json(root / "summary.json", {"success": success, "exit_reason": "success" if success else "COLLISION"})
    _write_jsonl(
        root / "artifacts" / "planning_topic_debug.jsonl",
        [
            {
                "planning_header_sequence_num": 7,
                "trajectory_point_count": 111,
                "trajectory_header_status": "",
                "reference_line_count": 0,
                "first_trajectory_point_kappa": 0.18,
                "first_trajectory_point_theta": 3.13,
            }
        ],
    )
    _write_jsonl(
        root / "artifacts" / "apollo_control_raw.jsonl",
        [
            {
                "apollo_control_raw": {
                    "control_header_sequence_num": 3,
                    "steering_target": 45.0,
                    "debug_input_trajectory_header_sequence_num": 7,
                    "debug_simple_lat_current_target_point_kappa": 0.18,
                    "debug_simple_lat_current_target_point_theta": 3.13,
                    "debug_simple_lat_current_target_point_s": 0.0,
                    "debug_simple_lat_current_target_point_relative_time": 0.08,
                    "debug_simple_lat_lateral_error": 0.01,
                    "debug_simple_lat_heading_error": 0.01,
                }
            }
        ],
    )
    _write_jsonl(
        root / "artifacts" / "control_trajectory_consume_debug_live.jsonl",
        [
            {
                "control_cycle_index": 3,
                "matched_planning_event_found_exactly": True,
                "control_used_planning_trajectory": True,
                "control_used_cached_trajectory": False,
                "trajectory_time_window_valid": True,
                "trajectory_not_started_yet": False,
                "latest_planning_msg_age_ms": 12.0,
                "control_message_age_ms": 2.0,
            }
        ],
    )
    _write_lateral_geometry(root / "artifacts" / "lateral_geometry_debug.csv")


def test_target_point_semantics_links_control_target_to_planning_first_point(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    _write_run(no_lateral, success=False)
    _write_run(stabilizer, success=True)

    report = analyze_baguang_apollo_target_point_semantics(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )

    assert report["status"] == "warn"
    assert "target_point_kappa_semantics_suspect" in report["findings"]
    assert "high_steer_tracks_planning_first_point_kappa" in report["findings"]
    assert "reference_line_debug_gap_blocks_full_attribution" in report["findings"]
    run = report["runs"]["no_lateral"]
    assert run["planning"]["reference_line_nonzero_rows"] == 0
    assert run["high_steer_alignment"]["target_kappa_matches_planning_first_point_count"] == 1
    assert run["high_steer_alignment"]["consume_used_planning_trajectory_count"] == 1
    assert run["samples"][0]["planning_trajectory_point_count"] == 111
    assert report["claim_boundary"]["proves_apollo_algorithm_limitation"] is False


def test_missing_artifacts_gracefully_degrade(tmp_path: Path) -> None:
    run = tmp_path / "missing"
    _write_json(run / "summary.json", {"success": False})

    report = analyze_baguang_apollo_target_point_semantics_run(run)

    assert report["planning"]["rows"] == 0
    assert report["control"]["high_steer_count"] == 0
    assert "artifacts/planning_topic_debug.jsonl" in report["missing_inputs"]
    assert "artifacts/apollo_control_raw.jsonl" in report["missing_inputs"]


def test_writer_and_cli(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_run(no_lateral, success=False)
    _write_run(stabilizer, success=True)

    report = analyze_baguang_apollo_target_point_semantics(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )
    outputs = write_baguang_apollo_target_point_semantics_report(report, out)

    assert Path(outputs["report"]).exists()
    assert "Baguang Apollo Target Point Semantics" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_apollo_target_point_semantics.py",
            "--no-lateral-run",
            str(no_lateral),
            "--stabilizer-run",
            str(stabilizer),
            "--out",
            str(cli_out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    assert '"status": "warn"' in result.stdout
    assert (cli_out / "baguang_apollo_target_point_semantics_report.json").exists()
