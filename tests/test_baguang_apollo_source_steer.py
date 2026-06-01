from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_apollo_source_steer import (
    analyze_baguang_apollo_source_steer,
    analyze_baguang_apollo_source_steer_run,
    write_baguang_apollo_source_steer_report,
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
        writer = csv.DictWriter(
            handle,
            fieldnames=["reference_lane_curvature", "e_y_m", "e_psi_deg", "lane_dist_m"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "reference_lane_curvature": 0.0,
                "e_y_m": 0.001,
                "e_psi_deg": 0.01,
                "lane_dist_m": 0.001,
            }
        )


def _write_run(root: Path, *, success: bool, stabilizer_enabled: bool) -> None:
    _write_json(
        root / "summary.json",
        {
            "success": success,
            "exit_reason": "success" if success else "COLLISION",
            "fail_reason": None if success else "COLLISION",
            "collision_count": 0 if success else 1,
            "max_speed_mps": 20.0,
        },
    )
    _write_json(
        root / "artifacts" / "direct_bridge_stats.json",
        {
            "straight_lane_lateral_stabilizer_enabled": stabilizer_enabled,
            "straight_lane_lateral_stabilizer_apply_count": 1 if stabilizer_enabled else 0,
        },
    )
    _write_jsonl(
        root / "artifacts" / "apollo_control_raw.jsonl",
        [
            {
                "apollo_control_raw": {
                    "control_header_sequence_num": 1,
                    "steering_target": 100.0,
                    "debug_simple_lat_lateral_error": 0.005,
                    "debug_simple_lat_heading_error": 0.005,
                    "debug_simple_lat_current_target_point_kappa": 0.19,
                    "debug_simple_lat_current_target_point_s": 0.0,
                    "debug_simple_lat_current_target_point_relative_time": 0.08,
                    "debug_simple_lat_steering_position": 0.0,
                    "debug_simple_lon_current_steer_interval": 0.02,
                    "debug_simple_lon_current_speed": 0.5,
                    "debug_simple_lon_path_remain": 30.0,
                    "debug_simple_lat_ref_speed": 0.5,
                    "debug_simple_lat_steer_angle_limited": 100.0,
                }
            },
            {
                "apollo_control_raw": {
                    "control_header_sequence_num": 2,
                    "steering_target": 0.0,
                    "debug_simple_lat_lateral_error": 0.0,
                    "debug_simple_lat_heading_error": 0.0,
                    "debug_simple_lat_current_target_point_kappa": 0.0,
                }
            },
        ],
    )
    _write_jsonl(root / "artifacts" / "bridge_control_decode.jsonl", [{"raw_steer": 1.0, "commanded_steer": 0.25}])
    _write_jsonl(
        root / "artifacts" / "direct_bridge_control_apply.jsonl",
        [
            {
                "source_steer": 0.25,
                "steer": 0.0002 if stabilizer_enabled else 0.25,
            }
        ],
    )
    _write_lateral_geometry(root / "artifacts" / "lateral_geometry_debug.csv")


def test_source_steer_detects_target_kappa_spike_and_stabilizer_masking(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    _write_run(no_lateral, success=False, stabilizer_enabled=False)
    _write_run(stabilizer, success=True, stabilizer_enabled=True)

    report = analyze_baguang_apollo_source_steer(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )

    assert report["status"] == "warn"
    assert report["reason"] == "source_steer_semantics_require_audit"
    assert report["runs"]["no_lateral"]["high_steer_count"] == 1
    assert report["runs"]["no_lateral"]["simple_lat"]["target_point_kappa"]["max_abs"] == 0.19
    assert report["runs"]["no_lateral"]["high_steer_context"]["current_speed_mps"]["max_abs"] == 0.5
    assert report["runs"]["no_lateral"]["high_steer_context"]["path_remain_m"]["max_abs"] == 30.0
    assert report["runs"]["no_lateral"]["high_steer_context"]["small_error_count"] == 1
    assert report["runs"]["no_lateral"]["lateral_geometry"]["reference_lane_curvature"]["max_abs"] == 0.0
    assert "target_point_curvature_semantics_suspect" in report["findings"]
    assert "stabilizer_masks_source_steer_anomaly" in report["findings"]
    assert "high_source_steer_is_low_speed_terminal_semantics" in report["findings"]
    assert "high_source_steer_with_small_simple_lat_error" in report["runs"]["stabilizer_enabled"]["findings"]
    assert (
        "high_source_steer_concentrates_in_low_speed_terminal_context"
        in report["runs"]["stabilizer_enabled"]["findings"]
    )
    assert report["claim_boundary"]["proves_apollo_algorithm_limitation"] is False


def test_missing_artifacts_gracefully_degrade(tmp_path: Path) -> None:
    run = tmp_path / "missing"
    _write_json(run / "summary.json", {"success": False})

    report = analyze_baguang_apollo_source_steer_run(run)

    assert report["control_raw_count"] == 0
    assert report["high_steer_count"] == 0
    assert report["steering_target"]["count"] == 0
    assert "artifacts/apollo_control_raw.jsonl" in report["missing_inputs"]
    assert "artifacts/lateral_geometry_debug.csv" in report["missing_inputs"]


def test_writer_and_cli(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_run(no_lateral, success=False, stabilizer_enabled=False)
    _write_run(stabilizer, success=True, stabilizer_enabled=True)

    report = analyze_baguang_apollo_source_steer(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )
    outputs = write_baguang_apollo_source_steer_report(report, out)

    assert Path(outputs["report"]).exists()
    assert "Baguang Apollo Source Steer" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_apollo_source_steer.py",
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
    assert (cli_out / "baguang_apollo_source_steer_report.json").exists()
