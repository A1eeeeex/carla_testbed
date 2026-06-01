from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_apollo_lateral_stabilizer_ab import (
    analyze_baguang_apollo_lateral_stabilizer_ab,
    write_baguang_apollo_lateral_stabilizer_ab_report,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_timeseries(path: Path, *, success: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame_id",
                "sim_time",
                "ego_speed",
                "cross_track_error",
                "heading_error",
                "apollo_steer_raw",
                "carla_steer_applied",
                "collision_count",
            ],
        )
        writer.writeheader()
        if success:
            writer.writerow(
                {
                    "frame_id": 1,
                    "sim_time": 1.0,
                    "ego_speed": 10.0,
                    "cross_track_error": 0.001,
                    "heading_error": 0.001,
                    "apollo_steer_raw": 0.0,
                    "carla_steer_applied": 0.0002,
                    "collision_count": 0,
                }
            )
        else:
            writer.writerow(
                {
                    "frame_id": 1,
                    "sim_time": 1.0,
                    "ego_speed": 10.0,
                    "cross_track_error": 6.0,
                    "heading_error": 0.2,
                    "apollo_steer_raw": 0.0,
                    "carla_steer_applied": 0.25,
                    "collision_count": 1,
                }
            )


def _write_run(root: Path, *, success: bool, stabilizer_enabled: bool) -> None:
    _write_json(root / "summary.json", {"success": success, "exit_reason": "success" if success else "COLLISION"})
    _write_json(
        root / "artifacts" / "direct_bridge_stats.json",
        {
            "control_apply_count": 1,
            "straight_lane_lateral_stabilizer_enabled": stabilizer_enabled,
            "straight_lane_lateral_stabilizer_apply_count": 1 if stabilizer_enabled else 0,
        },
    )
    _write_json(
        root / "artifacts" / "cyber_bridge_stats.json",
        {"routing_success_count": 1, "control_tx_count": 1},
    )
    _write_json(
        root / "artifacts" / "planning_topic_debug_summary.json",
        {"messages_with_nonzero_trajectory_points": 1, "total_messages_received": 1},
    )
    _write_json(root / "artifacts" / "map_contract_guard.json", {"effective_bridge_map_complete": True})
    _write_timeseries(root / "timeseries.csv", success=success)
    _write_jsonl(
        root / "artifacts" / "bridge_control_decode.jsonl",
        [{"raw_steer": 1.0, "commanded_steer": 0.25}],
    )
    _write_jsonl(
        root / "artifacts" / "direct_bridge_control_apply.jsonl",
        [
            {
                "source_steer": 0.25,
                "steer": 0.0002 if stabilizer_enabled else 0.25,
                "straight_lane_lateral_stabilizer": {
                    "enabled": stabilizer_enabled,
                    "applied": stabilizer_enabled,
                },
            }
        ],
    )
    _write_jsonl(root / "artifacts" / "planning_topic_debug.jsonl", [{"trajectory_point_count": 10}])


def test_stabilizer_ab_detects_steer_suppression(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    _write_run(no_lateral, success=False, stabilizer_enabled=False)
    _write_run(stabilizer, success=True, stabilizer_enabled=True)

    report = analyze_baguang_apollo_lateral_stabilizer_ab(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )

    assert report["status"] == "warn"
    assert report["reason"] == "stabilizer_suppresses_harmful_direct_steer_on_straight_route"
    assert report["contrast"]["no_lateral_diverged"] is True
    assert report["contrast"]["stabilizer_kept_route"] is True
    assert report["contrast"]["stabilizer_reduced_applied_steer"] is True
    assert report["contrast"]["stabilizer_source_to_applied_suppression_ratio"] == 1250.0
    assert "stabilizer_suppressed_direct_applied_steer" in report["findings"]


def test_writer_and_cli(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_run(no_lateral, success=False, stabilizer_enabled=False)
    _write_run(stabilizer, success=True, stabilizer_enabled=True)

    report = analyze_baguang_apollo_lateral_stabilizer_ab(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )
    outputs = write_baguang_apollo_lateral_stabilizer_ab_report(report, out)

    assert Path(outputs["report"]).exists()
    assert "Baguang Apollo Lateral Stabilizer A/B" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_apollo_lateral_stabilizer_ab.py",
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
    assert (cli_out / "baguang_apollo_lateral_stabilizer_ab_report.json").exists()
