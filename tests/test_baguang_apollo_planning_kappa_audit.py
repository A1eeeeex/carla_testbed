from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_apollo_planning_kappa_audit import (
    analyze_baguang_apollo_planning_kappa_audit,
    analyze_baguang_apollo_planning_kappa_audit_run,
    write_baguang_apollo_planning_kappa_audit_report,
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


def _write_apollo_map(root: Path) -> Path:
    map_dir = root / "apollo_map"
    map_dir.mkdir(parents=True, exist_ok=True)
    (map_dir / "base_map.xml").write_text(
        """<?xml version='1.0' encoding='UTF-8'?>
<OpenDRIVE>
  <road>
    <lanes>
      <laneSection>
        <lane uid="0_0_2" type="driving" direction="backward">
          <centralCurve>
            <segment>
              <lineSegment>
                <pointSet>
                  <point x="10.0" y="0.0" z="0.0" />
                  <point x="0.0" y="0.0" z="0.0" />
                </pointSet>
              </lineSegment>
            </segment>
          </centralCurve>
        </lane>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
        encoding="utf-8",
    )
    return map_dir


def _write_run(root: Path, *, success: bool) -> None:
    map_dir = _write_apollo_map(root)
    root.mkdir(parents=True, exist_ok=True)
    (root / "effective.yaml").write_text("bridge:\n  localization_back_offset_m: 1.0\n", encoding="utf-8")
    _write_json(root / "summary.json", {"success": success, "exit_reason": "success" if success else "COLLISION"})
    _write_json(
        root / "artifacts" / "map_contract_guard.json",
        {
            "map_contract_invalid": False,
            "map_contract_mismatch_active": False,
            "map_contract_mismatch_classification": "aligned",
            "high_risk_mismatch": False,
            "dreamview_selected_map": "straight_road_for_baguang",
            "runtime_map_dir": str(map_dir),
            "map_identity_hash_or_signature": "abc123",
        },
    )
    _write_jsonl(
        root / "artifacts" / "planning_topic_debug.jsonl",
        [
            {
                "planning_header_sequence_num": 1,
                "world_frame": 10,
                "trajectory_point_count": 111,
                "trajectory_header_status": "",
                "reference_line_count": 0,
                "first_trajectory_point_kappa": 0.18,
                "first_trajectory_point_x": 5.0,
                "first_trajectory_point_y": 0.2,
                "first_trajectory_point_theta": 3.1415926535,
                "trajectory_kappa": {"count": 3, "min": 0.0, "max": 0.18, "max_abs": 0.18, "p95_abs": 0.12},
                "trajectory_theta_delta_abs": {
                    "count": 2,
                    "min": 0.01,
                    "max": 0.02,
                    "max_abs": 0.02,
                    "p95_abs": 0.01,
                },
                "trajectory_xy_step_m": {
                    "count": 2,
                    "min": 0.05,
                    "max": 0.1,
                    "max_abs": 0.1,
                    "p95_abs": 0.05,
                },
                "trajectory_first_segment_heading": 3.08,
                "trajectory_first_theta_minus_first_segment_heading_rad": 0.05,
                "trajectory_kappa_spike_count_abs_ge_0_05": 2,
                "trajectory_kappa_spike_count_abs_ge_0_10": 2,
                "trajectory_sample_points": [{"index": 0, "x": 5.0, "y": 0.2, "theta": 3.1415926535, "kappa": 0.18}],
                "routing_unique_lane_signature": "0_0_2",
                "routing_lane_window_signature": "0_0_2@3.5->312.0",
            }
        ],
    )
    _write_jsonl(
        root / "artifacts" / "apollo_reference_line_debug.jsonl",
        [
            {
                "planning_header_sequence_num": 1,
                "reference_line_count": 0,
                "route_segment_count": 1,
                "route_segment_total_length": 308.0,
                "reference_line_provider_status": "failed",
                "lane_follow_map_status": "route_segments_present_reference_line_missing",
                "planning_empty_reason_guess": "reference_line_missing",
                "reference_line_debug_missing_but_trajectory_nonzero": True,
                "path_end_like_condition": True,
            }
        ],
    )
    _write_lateral_geometry(root / "artifacts" / "lateral_geometry_debug.csv")
    with (root / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["frame_id", "ego_x", "ego_y", "ego_heading"])
        writer.writeheader()
        writer.writerow({"frame_id": 10, "ego_x": 4.0, "ego_y": "-0.2", "ego_heading": "-3.1415926535"})


def test_planning_kappa_audit_detects_refline_missing_with_kappa_spike(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    _write_run(no_lateral, success=False)
    _write_run(stabilizer, success=True)

    report = analyze_baguang_apollo_planning_kappa_audit(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )

    assert report["status"] == "warn"
    assert "planning_kappa_spike_not_explained_by_route_geometry" in report["findings"]
    assert "reference_line_provider_or_debug_gap_is_on_kappa_path" in report["findings"]
    assert "reference_line_debug_missing_but_planning_nonzero_is_on_kappa_path" in report["findings"]
    assert "map_contract_mismatch_not_primary_evidence" in report["findings"]
    run = report["runs"]["no_lateral"]
    assert run["planning"]["nonempty_trajectory_rows"] == 1
    assert run["planning"]["kappa_spike_rows"] == 1
    assert run["planning"]["trajectory_kappa_debug_available_rows"] == 1
    assert run["planning"]["trajectory_kappa_debug_max_abs"]["max_abs"] == 0.18
    assert run["planning"]["trajectory_xy_step_m"]["max_abs"] == 0.1
    assert run["planning"]["trajectory_theta_delta_abs"]["max_abs"] == 0.02
    assert run["planning"]["trajectory_first_theta_minus_first_segment_heading_rad"]["max_abs"] == 0.05
    assert run["planning"]["trajectory_kappa_spike_count_abs_ge_0_05_sum"] == 2
    assert run["reference_line_debug"]["route_segment_present_reference_line_zero_rows"] == 1
    assert run["high_kappa_context"]["provider_failed_count"] == 1
    assert run["high_kappa_context"]["planning_trajectory_kappa_debug_max_abs"]["max_abs"] == 0.18
    assert run["high_kappa_context"]["planning_trajectory_xy_step_m_max_abs"]["max_abs"] == 0.1
    assert run["high_kappa_context"]["planning_trajectory_theta_delta_abs_max_abs"]["max_abs"] == 0.02
    assert run["apollo_map_alignment"]["status"] == "available"
    assert run["apollo_map_alignment"]["sample_to_lane_distance_m"]["max_abs"] == 0.2
    assert run["apollo_map_alignment"]["sample_heading_error_rad"]["max_abs"] < 1e-9
    assert run["apollo_localization_alignment"]["status"] == "available"
    assert run["apollo_localization_alignment"]["high_kappa_first_point_to_localization_distance_m"]["max_abs"] < 1e-9
    assert run["apollo_localization_alignment"]["high_kappa_first_point_heading_error_rad"]["max_abs"] < 1e-9
    assert run["lateral_geometry"]["reference_lane_curvature"]["max_abs"] == 0.0
    assert report["claim_boundary"]["proves_apollo_algorithm_limitation"] is False


def test_missing_artifacts_gracefully_degrade(tmp_path: Path) -> None:
    run = tmp_path / "missing"
    _write_json(run / "summary.json", {"success": False})

    report = analyze_baguang_apollo_planning_kappa_audit_run(run)

    assert report["planning"]["rows"] == 0
    assert report["high_kappa_context"]["count"] == 0
    assert "artifacts/planning_topic_debug.jsonl" in report["missing_inputs"]
    assert "artifacts/apollo_reference_line_debug.jsonl" in report["missing_inputs"]


def test_writer_and_cli(tmp_path: Path) -> None:
    no_lateral = tmp_path / "no_lateral"
    stabilizer = tmp_path / "stabilizer"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_run(no_lateral, success=False)
    _write_run(stabilizer, success=True)

    report = analyze_baguang_apollo_planning_kappa_audit(
        no_lateral_run=no_lateral,
        stabilizer_run=stabilizer,
    )
    outputs = write_baguang_apollo_planning_kappa_audit_report(report, out)

    assert Path(outputs["report"]).exists()
    assert "Baguang Apollo Planning Kappa Audit" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_apollo_planning_kappa_audit.py",
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
    assert (cli_out / "baguang_apollo_planning_kappa_audit_report.json").exists()
