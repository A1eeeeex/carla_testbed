from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_apollo_lateral_blocker import (
    analyze_baguang_apollo_lateral_blocker,
    write_baguang_apollo_lateral_blocker_report,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_timeseries(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame_id",
                "sim_time",
                "ego_x",
                "ego_y",
                "ego_speed",
                "cross_track_error",
                "heading_error",
                "apollo_steer_raw",
                "carla_steer_applied",
                "collision_count",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "frame_id": 1,
                "sim_time": 1.0,
                "ego_x": 0.0,
                "ego_y": 0.0,
                "ego_speed": 1.0,
                "cross_track_error": 0.0,
                "heading_error": 0.0,
                "apollo_steer_raw": 0.0,
                "carla_steer_applied": 0.0,
                "collision_count": 0,
            }
        )
        writer.writerow(
            {
                "frame_id": 2,
                "sim_time": 2.0,
                "ego_x": 10.0,
                "ego_y": 7.0,
                "ego_speed": 12.0,
                "cross_track_error": 7.0,
                "heading_error": 0.2,
                "apollo_steer_raw": 0.0,
                "carla_steer_applied": 0.02,
                "collision_count": 1,
            }
        )


def _write_synthetic_run(root: Path) -> None:
    _write_json(root / "summary.json", {"success": False, "exit_reason": "COLLISION", "collision_count": 1})
    _write_json(
        root / "artifacts" / "direct_bridge_stats.json",
        {
            "control_apply_count": 2,
            "control_apply_fail_count": 0,
            "straight_lane_lateral_stabilizer_enabled": False,
            "straight_lane_lateral_stabilizer_apply_count": 0,
        },
    )
    _write_json(
        root / "artifacts" / "cyber_bridge_stats.json",
        {"routing_success_count": 1, "routing_request_count": 1, "control_rx_count": 2, "control_tx_count": 2},
    )
    _write_json(
        root / "artifacts" / "planning_topic_debug_summary.json",
        {"total_messages_received": 2, "messages_with_nonzero_trajectory_points": 2},
    )
    _write_json(
        root / "artifacts" / "map_contract_guard.json",
        {"effective_bridge_map_complete": True, "container_runtime_map_complete": True},
    )
    _write_timeseries(root / "timeseries.csv")
    _write_jsonl(
        root / "artifacts" / "bridge_control_decode.jsonl",
        [
            {"raw_steer": 0.2, "commanded_steer": 0.05, "straight_lane_zero_steer_applied": False},
            {"raw_steer": -0.1, "commanded_steer": -0.025, "trajectory_contract_lateral_guard_applied": False},
        ],
    )
    _write_jsonl(
        root / "artifacts" / "direct_bridge_control_apply.jsonl",
        [
            {"source_steer": 0.05, "steer": 0.05},
            {"source_steer": -0.025, "steer": -0.025},
        ],
    )
    _write_jsonl(
        root / "artifacts" / "planning_topic_debug.jsonl",
        [
            {"trajectory_header_status": "", "trajectory_point_count": 10, "trajectory_total_path_length": 30},
            {
                "trajectory_header_status": "planner failed to make a driving plan",
                "trajectory_point_count": 10,
                "trajectory_total_path_length": 20,
            },
        ],
    )


def test_lateral_blocker_report_detects_divergence_and_p0_mismatch(tmp_path: Path) -> None:
    run = tmp_path / "run"
    _write_synthetic_run(run)

    report = analyze_baguang_apollo_lateral_blocker(run)

    assert report["status"] == "fail"
    assert report["reason"] == "lateral_divergence_after_stabilizer_removed"
    assert report["runtime_chain"]["routing_success_count"] == 1.0
    assert report["lateral_divergence"]["cross_track_error"]["max_abs"] == 7.0
    assert report["lateral_divergence"]["first_cross_track_thresholds"]["5.0"]["frame_id"] == 2.0
    assert report["control_semantics"]["p0_timeseries_raw_steer_mismatch"] is True
    assert report["control_semantics"]["bridge_decode_raw_steer"]["max_abs"] == 0.2
    assert "planning_reported_driving_plan_failures" in report["findings"]
    assert report["claim_boundary"]["proves_apollo_algorithm_lateral_limitation"] is False


def test_missing_artifacts_gracefully_degrade(tmp_path: Path) -> None:
    run = tmp_path / "missing"
    _write_json(run / "summary.json", {"success": False, "exit_reason": "COLLISION"})

    report = analyze_baguang_apollo_lateral_blocker(run)

    assert report["status"] == "insufficient_data"
    assert "timeseries.csv" in report["missing_inputs"]
    assert report["control_semantics"]["bridge_decode_raw_steer"]["count"] == 0


def test_writer_and_cli(tmp_path: Path) -> None:
    run = tmp_path / "run"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_synthetic_run(run)
    report = analyze_baguang_apollo_lateral_blocker(run)
    outputs = write_baguang_apollo_lateral_blocker_report(report, out)

    assert Path(outputs["report"]).exists()
    assert "Baguang Apollo Lateral Blocker" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_apollo_lateral_blocker.py",
            "--run-dir",
            str(run),
            "--out",
            str(cli_out),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    assert '"status": "fail"' in result.stdout
    assert (cli_out / "baguang_apollo_lateral_blocker_report.json").exists()
