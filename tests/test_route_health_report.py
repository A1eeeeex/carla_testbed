from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.route_health import analyze_route_health
from carla_testbed.analysis.route_health_report import (
    CURVE_SEGMENTS_CSV_FIELDS,
    analyze_route_health_run_dir,
    write_route_health_report,
)
from carla_testbed.routes.io import load_route_json


FIXTURES = Path(__file__).resolve().parent / "fixtures"
REPO_ROOT = Path(__file__).resolve().parents[1]


def test_route_health_report_files_created(tmp_path: Path) -> None:
    route = load_route_json(FIXTURES / "routes" / "spawn_alignment_curve.json")
    report = analyze_route_health(route)

    outputs = write_route_health_report(tmp_path, route, report)

    for path in outputs.values():
        assert Path(path).exists()
    payload = json.loads((tmp_path / "route_health.json").read_text(encoding="utf-8"))
    summary = (tmp_path / "route_health_summary.md").read_text(encoding="utf-8")
    assert payload["route_id"] == "spawn_alignment_curve"
    assert payload["route_source"] == "configured_route_file"
    assert payload["evidence_level"] == "claim_grade"
    assert payload["hard_gate_eligible"] is True
    assert payload["route_evidence_reason"] == "configured_route_file_with_valid_route_identity_and_geometry"
    assert payload["route_geometry"]["curve_segments_count"] >= 1
    assert payload["route_geometry"]["spawn_alignment"]["direction_consistent"] is True
    assert "curve segment count" in summary
    assert "spawn alignment" in summary
    assert "ego_speed mean/p95/max" in summary
    assert "brake_throttle_conflict_frames" in summary
    assert "Route source" in summary
    assert "Evidence level" in summary
    assert "Hard gate eligible" in summary
    assert "Route evidence reason" in summary


def test_curve_segments_csv_header_is_fixed(tmp_path: Path) -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_curve.json")
    report = analyze_route_health(route)

    write_route_health_report(tmp_path, route, report)

    with (tmp_path / "curve_segments.csv").open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        assert next(reader) == CURVE_SEGMENTS_CSV_FIELDS


def test_missing_spawn_pose_does_not_crash(tmp_path: Path) -> None:
    route = load_route_json(FIXTURES / "routes" / "simple_curve.json")
    route.spawn_pose = None

    report = analyze_route_health(route)
    write_route_health_report(tmp_path, route, report)

    payload = json.loads((tmp_path / "route_health.json").read_text(encoding="utf-8"))
    spawn = payload["route_geometry"]["spawn_alignment"]
    assert "spawn_pose" in payload["missing_fields"]
    assert spawn["distance_m"] is None
    assert spawn["heading_error_rad"] is None
    assert spawn["direction_consistent"] is None


def test_analyze_route_health_dry_run_does_not_require_runtime(tmp_path: Path) -> None:
    out_dir = tmp_path / "dry"
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "tools" / "analyze_route_health.py"),
            "--route",
            str(FIXTURES / "routes" / "simple_curve.json"),
            "--out",
            str(out_dir),
            "--dry-run",
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["outputs"]["route_health_json"].endswith("route_health.json")
    assert not (out_dir / "route_health.json").exists()


def test_run_dir_route_health_uses_bridge_control_decode_as_control_evidence(tmp_path: Path) -> None:
    route_path = FIXTURES / "routes" / "simple_straight.json"
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "route.json").write_text(route_path.read_text(encoding="utf-8"), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps({"success": False}) + "\n", encoding="utf-8")
    with (run_dir / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame_id",
                "sim_time",
                "ego_x",
                "ego_y",
                "ego_heading",
                "ego_speed",
                "cross_track_error",
                "heading_error",
                "throttle_applied",
                "brake_applied",
                "carla_steer_applied",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "frame_id": 1,
                "sim_time": 0.05,
                "ego_x": 0.0,
                "ego_y": 0.0,
                "ego_heading": 0.0,
                "ego_speed": 0.1,
                "cross_track_error": 0.0,
                "heading_error": 0.0,
                "throttle_applied": 0.2,
                "brake_applied": 0.0,
                "carla_steer_applied": 0.01,
            }
        )
    bridge_decode = {
        "ts_sec": 0.05,
        "parsed_control": {
            "throttle": 0.12,
            "brake": 0.03,
            "steer": 0.2,
        },
        "output_to_carla": {
            "mapped_throttle_cmd": 0.18,
            "mapped_brake_cmd": 0.04,
            "mapped_carla_steer_cmd": 0.05,
            "trajectory_contract_lateral_guard_applied": False,
        },
    }
    (artifacts / "bridge_control_decode.jsonl").write_text(json.dumps(bridge_decode) + "\n", encoding="utf-8")

    result = analyze_route_health_run_dir(run_dir)
    report = result["report"]

    assert "throttle_raw" not in report["missing_fields"]
    assert "throttle_mapped" not in report["missing_fields"]
    assert "brake_raw" not in report["missing_fields"]
    assert "brake_mapped" not in report["missing_fields"]
    assert report["run_metrics"]["throttle_raw_p95"] == 0.12
    assert report["run_metrics"]["throttle_mapped_p95"] == 0.18
    assert report["run_metrics"]["brake_raw_p95"] == 0.03
    assert report["run_metrics"]["brake_mapped_p95"] == 0.04
    assert report["source"]["bridge_control_decode_path"].endswith("bridge_control_decode.jsonl")


def test_run_dir_route_health_reads_flat_bridge_control_decode_format(tmp_path: Path) -> None:
    route_path = FIXTURES / "routes" / "simple_straight.json"
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "route.json").write_text(route_path.read_text(encoding="utf-8"), encoding="utf-8")
    with (run_dir / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame_id",
                "sim_time",
                "ego_x",
                "ego_y",
                "ego_heading",
                "ego_speed",
                "cross_track_error",
                "heading_error",
                "throttle_applied",
                "brake_applied",
                "carla_steer_applied",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "frame_id": 1,
                "sim_time": 0.05,
                "ego_x": 0.0,
                "ego_y": 0.0,
                "ego_heading": 0.0,
                "ego_speed": 0.1,
                "cross_track_error": 0.0,
                "heading_error": 0.0,
                "throttle_applied": 0.2,
                "brake_applied": 0.0,
                "carla_steer_applied": 0.01,
            }
        )
    bridge_decode = {
        "ts_sec": 0.05,
        "raw_throttle": 0.16,
        "raw_brake": 0.02,
        "raw_steer": 0.1,
        "mapped_throttle_cmd": 0.24,
        "mapped_brake_cmd": 0.03,
        "mapped_carla_steer_cmd": 0.04,
    }
    (artifacts / "bridge_control_decode.jsonl").write_text(json.dumps(bridge_decode) + "\n", encoding="utf-8")

    result = analyze_route_health_run_dir(run_dir)
    report = result["report"]

    assert "throttle_mapped" not in report["missing_fields"]
    assert "brake_mapped" not in report["missing_fields"]
    assert report["run_metrics"]["throttle_mapped_p95"] == 0.24
    assert report["run_metrics"]["brake_mapped_p95"] == 0.03
