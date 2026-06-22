from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.baguang_lane_event_contract import (
    analyze_baguang_lane_event_contract,
    parse_xodr_lane_contracts,
    write_baguang_lane_event_contract_report,
)


def _write_xodr(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" standalone="yes"?>
<OpenDRIVE>
  <road name="straight" length="400" id="0" junction="-1">
    <planView><geometry s="0" x="300" y="-5.25" hdg="3.14159" length="400"><line /></geometry></planView>
    <lanes>
      <laneSection s="0">
        <left>
          <lane id="1" type="driving" level="false">
            <width sOffset="0" a="3.75" b="0" c="0" d="0" />
            <roadMark sOffset="0" type="broken" weight="standard" color="white" width="0.1" laneChange="both" />
            <userData><vectorLane travelDir="backward" /></userData>
          </lane>
        </left>
        <center>
          <lane id="0" type="none" level="false">
            <roadMark sOffset="0" type="solid solid" weight="standard" color="yellow" width="0.1" laneChange="none" />
          </lane>
        </center>
        <right>
          <lane id="-1" type="driving" level="false">
            <width sOffset="0" a="3.75" b="0" c="0" d="0" />
            <roadMark sOffset="0" type="broken" weight="standard" color="white" width="0.1" laneChange="both" />
            <userData><vectorLane travelDir="forward" /></userData>
          </lane>
          <lane id="-2" type="driving" level="false">
            <width sOffset="0" a="3.75" b="0" c="0" d="0" />
            <roadMark sOffset="0" type="solid" weight="standard" color="yellow" width="0.1" laneChange="none" />
            <userData><vectorLane travelDir="forward" /></userData>
          </lane>
        </right>
      </laneSection>
    </lanes>
  </road>
</OpenDRIVE>
""",
        encoding="utf-8",
    )


def _write_run(
    root: Path,
    *,
    cross_track_error: float,
    heading_error: float = 0.001,
    distance_x: float = 0.9,
    event_type: str = "lane_invasion",
) -> None:
    root.mkdir(parents=True)
    (root / "summary.json").write_text(
        json.dumps(
            {
                "success": False,
                "fail_reason": "LANE_INVASION",
                "exit_reason": "LANE_INVASION",
                "frames": 2,
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "events.jsonl").write_text(
        json.dumps({"event_type": event_type, "frame": 100, "step": 1, "t": 0.1}) + "\n",
        encoding="utf-8",
    )
    with (root / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame",
                "sim_time",
                "ego_x",
                "ego_y",
                "ego_speed",
                "cross_track_error",
                "heading_error",
                "lane_invasion_count",
                "collision_count",
                "apollo_steer_raw",
                "bridge_steer_mapped",
                "carla_steer_applied",
                "applied_steer",
                "applied_throttle",
                "applied_brake",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "frame": 99,
                "sim_time": 0.0,
                "ego_x": 299.7,
                "ego_y": -5.25,
                "ego_speed": 0.0,
                "cross_track_error": 0.0,
                "heading_error": 0.0,
                "lane_invasion_count": 0,
                "collision_count": 0,
                "apollo_steer_raw": 0.0,
                "bridge_steer_mapped": 0.0,
                "carla_steer_applied": 0.0,
                "applied_steer": 0.0,
                "applied_throttle": 0.2,
                "applied_brake": 0.0,
            }
        )
        writer.writerow(
            {
                "frame": 100,
                "sim_time": 0.1,
                "ego_x": 299.7 - distance_x,
                "ego_y": -5.25,
                "ego_speed": 1.0,
                "cross_track_error": cross_track_error,
                "heading_error": heading_error,
                "lane_invasion_count": 1,
                "collision_count": 0,
                "apollo_steer_raw": cross_track_error / 10.0,
                "bridge_steer_mapped": cross_track_error / 10.0,
                "carla_steer_applied": cross_track_error / 10.0,
                "applied_steer": cross_track_error / 10.0,
                "applied_throttle": 0.0,
                "applied_brake": 0.3,
            }
        )


def _write_lane_event_attribution_inputs(run: Path) -> None:
    control_health = run / "analysis" / "control_health" / "control_health_report.json"
    control_health.parent.mkdir(parents=True, exist_ok=True)
    control_health.write_text(
        json.dumps(
            {
                "status": "warn",
                "metrics": {
                    "lane_event_response_context": {
                        "classification": "applied_steer_yaw_response_tracks_progressive_lateral_departure",
                        "cross_track_error_abs_growth_m": 0.47,
                        "heading_error_abs_growth_rad": 0.11,
                        "applied_steer_mean": -0.07,
                        "applied_steer_end": -0.13,
                        "yaw_rate_mean_rad_s": -0.12,
                        "yaw_rate_end_rad_s": -0.20,
                        "vehicle_response_rows_available": True,
                        "vehicle_response_sample_count": 16,
                        "applied_steer_yaw_rate_same_sign": True,
                        "cte_growth_yaw_rate_same_sign": True,
                        "heading_growth_yaw_rate_same_sign": True,
                        "cte_growth_applied_steer_same_sign": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    reference_line = (
        run
        / "analysis"
        / "apollo_reference_line_contract"
        / "apollo_reference_line_contract_report.json"
    )
    reference_line.parent.mkdir(parents=True, exist_ok=True)
    reference_line.write_text(
        json.dumps(
            {
                "status": "warn",
                "control_target_point_vs_planning_path_candidate_sample": {
                    "classification": "control_target_between_planning_path_candidate_lateral_bounds",
                    "target_inside_path_lateral_envelope": True,
                    "target_point_lane_l_abs_p95_m": 0.0,
                    "target_point_to_path_candidate_line_abs_p95_m": 0.82,
                    "path_candidate_lane_l_min_m": -1.25,
                    "path_candidate_lane_l_max_m": 0.82,
                    "path_candidate_lane_l_abs_p95_m": 1.25,
                    "reference_line_claim_grade_allowed": False,
                },
                "planning_debug_path_candidate_hdmap_projection_alignment": {
                    "classification": "planning_debug_path_candidate_lateral_offset_from_hdmap_lane_center",
                    "routing_lane_window_compatible": True,
                    "path_candidate_lane_l_abs_p95_m": 1.25,
                    "reference_line_claim_grade_allowed": False,
                },
                "planning_debug_path_candidate_vs_trajectory_sample": {
                    "classification": "planning_debug_path_candidate_offset_from_planning_trajectory_sample_support",
                    "path_candidate_to_planning_sample_line_abs_p95_m": 3.08,
                    "reference_line_claim_grade_allowed": False,
                },
            }
        ),
        encoding="utf-8",
    )


def test_parse_xodr_lane_contracts(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    _write_xodr(xodr)

    lanes = parse_xodr_lane_contracts(xodr)
    target = next(lane for lane in lanes if lane.lane_id == -2)

    assert target.lane_type == "driving"
    assert target.width_m == 3.75
    assert target.road_marks[0].mark_type == "solid"
    assert target.travel_dir == "forward"


def test_low_cte_lane_invasion_recommends_quarantine(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert report["status"] == "warn"
    assert report["quarantine_recommended"] is True
    assert report["claim_boundary"]["lane_invasion_event_can_be_used_as_hard_gate"] is False
    assert run_report["reason"] == "lane_invasion_trigger_inconsistent_with_centerline_evidence"
    assert run_report["static_crossing_check"]["trigger_geometrically_implausible"] is True
    assert run_report["static_crossing_check"]["estimated_center_offset_to_cross_mark_m"] == 0.825


def test_high_cte_lane_invasion_is_not_quarantined(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=1.2, distance_x=40.0)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])

    assert report["status"] == "fail"
    assert report["quarantine_recommended"] is False
    assert report["claim_boundary"]["lane_invasion_event_can_be_used_as_hard_gate"] is True
    diagnostics = report["run_reports"][0]["departure_diagnostics"]
    assert diagnostics["classification"] == "downstream_progressive_lane_departure"
    assert "lane_event_after_road_start_window" in diagnostics["interpretation"]
    assert "cross_track_error_grew_before_event" in diagnostics["interpretation"]
    assert diagnostics["control"]["raw_mapped_applied_steer_available"] is True
    assert diagnostics["control"]["carla_steer_applied_end"] == 0.12
    assert diagnostics["control"]["mapped_to_applied_steer_abs_error"]["max_abs_error"] == 0.0


def test_lane_event_contract_carries_vehicle_response_and_path_control_attribution(
    tmp_path: Path,
) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=1.2, distance_x=40.0)
    _write_lane_event_attribution_inputs(run)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert report["status"] == "fail"
    assert run_report["departure_diagnostics"]["classification"] == "downstream_progressive_lane_departure"
    assert run_report["vehicle_response_context"]["classification"] == (
        "applied_steer_yaw_response_tracks_progressive_lateral_departure"
    )
    assert run_report["path_control_context"]["control_target_vs_path_candidate_classification"] == (
        "control_target_between_planning_path_candidate_lateral_bounds"
    )
    assert run_report["path_control_context"]["reference_line_claim_grade_allowed"] is False
    attribution = run_report["lane_event_attribution"]
    assert attribution["classification"] == (
        "progressive_lane_departure_with_vehicle_response_and_control_target_between_path_candidate_bounds"
    )
    assert attribution["reference_line_claim_grade_allowed"] is False
    assert "control_target_between_path_candidate_lateral_bounds" in attribution["reasons"]
    assert "does not alter the lane-event hard gate" in attribution["claim_boundary"]


def test_downstream_lane_invasion_before_footprint_crossing_is_quarantined(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.6, heading_error=0.15, distance_x=40.0)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert report["status"] == "warn"
    assert report["quarantine_recommended"] is True
    assert report["claim_boundary"]["lane_invasion_event_can_be_used_as_hard_gate"] is False
    assert run_report["lane_invasion_event_can_be_used_as_hard_gate"] is False
    assert run_report["reason"] == "lane_invasion_trigger_before_static_footprint_crossing"
    assert run_report["static_crossing_check"]["trigger_geometrically_implausible"] is True
    assert run_report["static_crossing_check"]["trigger_footprint_intersects_marking"] is False


def test_yaw_aware_vehicle_footprint_prevents_false_quarantine(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.62, heading_error=0.166, distance_x=40.0)
    artifacts = run / "artifacts"
    artifacts.mkdir()
    (artifacts / "carla_vehicle_characteristics.json").write_text(
        json.dumps(
            {
                "front_edge_to_center": 2.44,
                "back_edge_to_center": 2.45,
                "left_edge_to_center": 0.918,
                "right_edge_to_center": 0.918,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]
    crossing = run_report["static_crossing_check"]

    assert report["quarantine_recommended"] is False
    assert report["claim_boundary"]["lane_invasion_event_can_be_used_as_hard_gate"] is True
    assert run_report["reason"] == "possible_real_lane_departure_or_unclassified_lane_event"
    assert crossing["trigger_footprint_intersects_marking"] is True
    assert crossing["trigger_footprint_lateral_extent_m"] > crossing["ego_half_width_m"]
    assert crossing["trigger_center_offset_to_footprint_crossing_threshold_m"] < 0.62


def test_departure_diagnostics_reports_mapped_applied_steer_mismatch(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=1.2, distance_x=40.0)
    rows = list(csv.DictReader((run / "timeseries.csv").open()))
    fieldnames = list(rows[0].keys())
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row["apollo_steer_raw"] = "0.0"
            row["bridge_steer_mapped"] = "0.0"
            row["carla_steer_applied"] = "-0.04" if row["lane_invasion_count"] == "1" else "-0.02"
            row["applied_steer"] = row["carla_steer_applied"]
            writer.writerow(row)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    diagnostics = report["run_reports"][0]["departure_diagnostics"]
    control = diagnostics["control"]

    assert diagnostics["classification"] == "downstream_progressive_lane_departure"
    assert "mapped_to_applied_steer_mismatch_before_event" in diagnostics["interpretation"]
    assert "applied_steer_nonzero_while_mapped_zero_before_event" in diagnostics["interpretation"]
    assert control["raw_to_mapped_steer_abs_error"]["max_abs_error"] == 0.0
    assert control["mapped_to_applied_steer_abs_error"]["max_abs_error"] == 0.04
    assert control["applied_nonzero_while_mapped_zero"]["count"] == 2
    assert control["carla_steer_applied_summary"]["max_abs"] == 0.04


def test_departure_diagnostics_prefers_control_apply_trace_over_timeseries(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=1.2, distance_x=40.0)
    rows = list(csv.DictReader((run / "timeseries.csv").open()))
    fieldnames = list(rows[0].keys())
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row["apollo_steer_raw"] = "0.0"
            row["bridge_steer_mapped"] = "0.0"
            row["carla_steer_applied"] = "-0.04" if row["lane_invasion_count"] == "1" else "-0.02"
            row["applied_steer"] = row["carla_steer_applied"]
            writer.writerow(row)
    artifacts = run / "artifacts"
    artifacts.mkdir()
    (artifacts / "control_apply_trace.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "sim_time": 0.0,
                        "apollo_raw": {"steer": -0.08, "throttle": 0.2, "brake": 0.0},
                        "bridge_mapped": {"mapped_carla_steer_cmd": -0.02},
                        "carla_applied": {"steer": -0.02, "throttle": 0.2, "brake": 0.0},
                    }
                ),
                json.dumps(
                    {
                        "sim_time": 0.1,
                        "apollo_raw": {"steer": -0.16, "throttle": 0.0, "brake": 0.3},
                        "bridge_mapped": {"mapped_carla_steer_cmd": -0.04},
                        "carla_applied": {"steer": -0.04, "throttle": 0.0, "brake": 0.3},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    diagnostics = report["run_reports"][0]["departure_diagnostics"]
    control = diagnostics["control"]

    assert control["source"] == "artifacts/control_apply_trace.jsonl"
    assert control["control_apply_trace_rows_used"] == 2
    assert "control_apply_trace_used_for_control_diagnostics" in diagnostics["interpretation"]
    assert "mapped_to_applied_steer_mismatch_before_event" not in diagnostics["interpretation"]
    assert "mapped_to_applied_steer_consistent_before_event" in diagnostics["interpretation"]
    assert control["mapped_to_applied_steer_abs_error"]["max_abs_error"] == 0.0
    assert control["applied_nonzero_while_mapped_zero"]["count"] == 0
    assert control["apollo_steer_raw_end"] == -0.16
    assert abs(control["raw_to_mapped_steer_gain"]["mean"] - 0.25) < 1e-12
    assert control["raw_to_mapped_steer_gain"]["sample_count"] == 2
    assert control["apollo_steer_raw_same_sign_as_cross_track_error"] is False
    assert control["bridge_steer_mapped_same_sign_as_cross_track_error"] is False
    assert "raw_to_mapped_steer_gain_legacy_scale_like" in diagnostics["interpretation"]


def test_contract_reads_safety_event_trace_artifact(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)
    (run / "events.jsonl").unlink()
    safety_dir = run / "artifacts"
    safety_dir.mkdir()
    (safety_dir / "safety_event_trace.jsonl").write_text(
        json.dumps(
            {
                "event_type": "lane_invasion",
                "frame": 100,
                "timestamp": 0.1,
                "crossed_lane_marking_types": ["Solid"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert report["quarantine_recommended"] is True
    assert run_report["event_sources"] == ["artifacts/safety_event_trace.jsonl"]
    assert run_report["first_lane_invasion"]["event_source"] == "artifacts/safety_event_trace.jsonl"
    assert run_report["first_lane_invasion"]["crossed_lane_marking_types"] == ["solid"]
    assert run_report["crossed_marking_check"]["types_match_target_lane"] is True
    assert "events.jsonl_or_artifacts/safety_event_trace.jsonl" not in run_report["missing_inputs"]


def test_contract_reports_crossed_marking_type_mismatch(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)
    (run / "events.jsonl").unlink()
    (run / "artifacts").mkdir()
    (run / "artifacts" / "safety_event_trace.jsonl").write_text(
        json.dumps(
            {
                "event_type": "lane_invasion",
                "frame": 100,
                "timestamp": 0.1,
                "crossed_lane_marking_types": ["Broken", "Broken"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert run_report["first_lane_invasion"]["crossed_lane_marking_types"] == ["broken"]
    assert run_report["crossed_marking_check"]["available"] is True
    assert run_report["crossed_marking_check"]["target_lane_roadmark_types"] == ["solid"]
    assert run_report["crossed_marking_check"]["types_match_target_lane"] is False
    assert run_report["static_crossing_check"]["trigger_marking_type_reason"] == (
        "crossed_marking_types_do_not_match_target_lane"
    )


def test_contract_reports_road_start_only_offset_sweep(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)
    with (run / "offset_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "offset_m",
                "spawned",
                "lane_invasion_count",
                "static_lane_invasion_count",
                "first_event_displacement_m",
                "first_event_cte_m",
                "first_event_heading_error_rad",
                "first_event_marking_types",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "offset_m": 0.0,
                "spawned": True,
                "lane_invasion_count": 1,
                "static_lane_invasion_count": 0,
                "first_event_displacement_m": 0.8,
                "first_event_cte_m": 0.001,
                "first_event_heading_error_rad": 0.001,
                "first_event_marking_types": "['Broken', 'Broken']",
            }
        )
        writer.writerow(
            {
                "offset_m": 2.0,
                "spawned": True,
                "lane_invasion_count": 0,
                "static_lane_invasion_count": 0,
                "first_event_displacement_m": "",
                "first_event_cte_m": "",
                "first_event_heading_error_rad": "",
                "first_event_marking_types": "[]",
            }
        )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    offset_sweep = report["run_reports"][0]["offset_sweep"]

    assert offset_sweep["available"] is True
    assert offset_sweep["reason"] == "road_start_only_lane_event"
    assert offset_sweep["road_start_only_trigger"] is True
    assert offset_sweep["event_offsets_m"] == [0.0]
    assert offset_sweep["min_clear_offset_without_event_m"] == 2.0


def test_contract_accepts_builtin_cte_heading_aliases(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)
    (run / "events.jsonl").unlink()
    (run / "artifacts").mkdir()
    (run / "artifacts" / "safety_event_trace.jsonl").write_text(
        json.dumps({"event_type": "lane_invasion", "frame": 100, "timestamp": 0.1}) + "\n",
        encoding="utf-8",
    )
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "frame_id",
                "sim_time",
                "ego_x",
                "ego_y",
                "ego_speed_mps",
                "cross_track_error_m",
                "heading_error_rad",
                "bbox_extent_y_m",
                "lane_invasion_count",
                "collision_count",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "frame_id": 99,
                "sim_time": 0.0,
                "ego_x": 299.7,
                "ego_y": -5.25,
                "ego_speed_mps": 0.0,
                "cross_track_error_m": 0.0,
                "heading_error_rad": 0.0,
                "bbox_extent_y_m": 0.9,
                "lane_invasion_count": 0,
                "collision_count": 0,
            }
        )
        writer.writerow(
            {
                "frame_id": 100,
                "sim_time": 0.1,
                "ego_x": 298.8,
                "ego_y": -5.25,
                "ego_speed_mps": 1.0,
                "cross_track_error_m": 0.001,
                "heading_error_rad": 0.001,
                "bbox_extent_y_m": 0.9,
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert run_report["reason"] == "lane_invasion_trigger_inconsistent_with_centerline_evidence"
    assert run_report["first_lane_invasion"]["cross_track_error"] == 0.001
    assert run_report["first_lane_invasion"]["heading_error"] == 0.001
    assert run_report["vehicle_footprint"]["source"] == "timeseries.bbox_extent_y_m"
    assert run_report["static_crossing_check"]["ego_half_width_m"] == 0.9
    assert run_report["static_crossing_check"]["trigger_footprint_intersects_marking"] is False


def test_contract_uses_vehicle_characteristics_for_apollo_width(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)
    artifacts = run / "artifacts"
    artifacts.mkdir()
    (artifacts / "carla_vehicle_characteristics.json").write_text(
        json.dumps(
            {
                "width": 1.84,
                "left_edge_to_center": 0.91,
                "right_edge_to_center": 0.93,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    run_report = report["run_reports"][0]

    assert run_report["vehicle_footprint"]["source"] == "artifacts/carla_vehicle_characteristics.json"
    assert run_report["vehicle_footprint"]["ego_half_width_m"] == 0.93
    assert run_report["static_crossing_check"]["ego_half_width_m"] == 0.93
    assert run_report["static_crossing_check"]["trigger_clearance_to_marking_m"] > 0.8


def test_writer_and_cli_create_report_files(tmp_path: Path) -> None:
    xodr = tmp_path / "straight_road_for_baguang.xodr"
    run = tmp_path / "run"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write_xodr(xodr)
    _write_run(run, cross_track_error=0.001)

    report = analyze_baguang_lane_event_contract(xodr_path=xodr, run_dirs=[run])
    outputs = write_baguang_lane_event_contract_report(report, out)

    assert Path(outputs["report"]).exists()
    assert Path(outputs["summary"]).exists()
    summary = Path(outputs["summary"]).read_text(encoding="utf-8")
    assert "Claim Boundary" in summary
    assert "raw_to_mapped_steer_gain_mean" in summary

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_lane_event_contract.py",
            "--xodr",
            str(xodr),
            "--run-dir",
            str(run),
            "--out",
            str(cli_out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert '"status": "warn"' in result.stdout
    assert (cli_out / "baguang_lane_event_contract_report.json").exists()
    assert (cli_out / "baguang_lane_event_contract_summary.md").exists()
