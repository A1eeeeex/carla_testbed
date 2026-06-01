from __future__ import annotations

import csv
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

from carla_testbed.analysis.baguang_stack_comparison import (
    analyze_baguang_stack_comparison,
    load_baguang_stack_comparison_suite,
    validate_baguang_stack_comparison_suite,
    write_baguang_stack_comparison_report,
)

SUITE_PATH = Path("configs/scenarios/baguang_stack_comparison_suite.yaml")


def _write(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_run(root: Path, *, success: bool = True, max_speed: float = 22.0, final_speed: float = 0.1) -> None:
    _write(
        root / "summary.json",
        json.dumps(
            {
                "success": success,
                "fail_reason": None if success else "failed",
                "exit_reason": "success" if success else "failed",
                "frames": 20,
                "sim_duration_s": 10.0,
                "wall_duration_s": 11.0,
                "max_speed_mps": max_speed,
                "collision_count": 0,
                "lane_invasion_count": 0,
                "metrics": {"min_lead_distance_m": 11.0},
            }
        ),
    )
    root.mkdir(parents=True, exist_ok=True)
    with (root / "timeseries.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["ego_x", "ego_speed", "v_mps"])
        writer.writeheader()
        writer.writerow({"ego_x": "0.0", "ego_speed": "0.0", "v_mps": "0.0"})
        writer.writerow({"ego_x": "290.0", "ego_speed": str(final_speed), "v_mps": str(final_speed)})
    for rel in (
        "manifest.json",
        "config.resolved.yaml",
        "events.jsonl",
        "video/dual_cam/demo_third_person.mp4",
    ):
        _write(root / rel)


def _write_lane_event_contract_report(path: Path, *, quarantine: bool = True) -> None:
    _write(
        path,
        json.dumps(
            {
                "schema_version": "baguang_lane_event_contract.v1",
                "status": "warn" if quarantine else "pass",
                "quarantine_recommended": quarantine,
                "claim_boundary": {
                    "lane_invasion_event_can_be_used_as_hard_gate": not quarantine,
                    "reason": "lane_invasion_trigger_inconsistent_with_centerline_evidence"
                    if quarantine
                    else "no_inconsistent_lane_invasion_trigger_observed",
                },
            }
        ),
    )


def _synthetic_suite(tmp_path: Path) -> dict:
    apollo_run = tmp_path / "apollo_run"
    autoware_run = tmp_path / "autoware_run"
    _write_run(apollo_run)
    _write_run(autoware_run)
    return {
        "schema_version": "baguang_stack_comparison_suite.v1",
        "name": "synthetic",
        "common_scene": {
            "scenario_id": "baguang_followstop_300m_80kph",
            "scenario_class": "follow_stop_80kph",
            "map": "straight_road_for_baguang",
            "route_id": "straight_road_for_baguang_mainline_followstop_300m",
            "duration_s": 90,
            "target_speed_kph": 80,
            "lead_distance_m": 300,
            "ego_spawn_ref": "carla_spawn_index_0",
            "front_vehicle": {"placement": "300m_ahead_same_lane"},
            "route_ref": "carla_waypoint_trace_from_ego_to_380m",
            "vehicle_blueprint": "vehicle.lincoln.mkz_2020",
            "required_artifacts": [
                "manifest.json",
                "summary.json",
                "config.resolved.yaml",
                "timeseries.csv",
                "events.jsonl",
                "video/dual_cam/demo_third_person.mp4",
            ],
        },
        "comparison_requirements": {
            "behavior_gate": {
                "max_speed_mps_min": 20.0,
                "final_speed_mps_max": 0.5,
                "min_lead_distance_m_max": 15.0,
                "collision_count_max": 0,
                "lane_invasion_count_max": 0,
            },
            "claim_boundary": {
                "assisted_profiles_can_be_compared": True,
                "assisted_profiles_prove_unassisted_natural_driving": False,
                "full_sensor_perception_reproduced": False,
            },
        },
        "stacks": [
            {
                "stack_id": "apollo",
                "stack": "apollo",
                "backend": "carla_direct",
                "algorithm_variant_id": "apollo_assisted",
                "evidence_run": str(apollo_run),
                "status": "experimental_assisted_verified_once",
                "required_artifacts": ["video/dual_cam/demo_third_person.mp4"],
                "declared_assists": ["straight_acc_override"],
                "not_proven": ["unassisted_apollo"],
            },
            {
                "stack_id": "autoware",
                "stack": "autoware",
                "backend": "ros2_autoware",
                "algorithm_variant_id": "autoware_assisted",
                "evidence_run": str(autoware_run),
                "status": "experimental_assisted_verified_once",
                "required_artifacts": ["video/dual_cam/demo_third_person.mp4"],
                "declared_assists": ["bridge_smoothing"],
                "not_proven": ["unmodified_autoware"],
            },
        ],
    }


def test_baguang_stack_comparison_suite_loads_and_declares_boundaries() -> None:
    suite = load_baguang_stack_comparison_suite(SUITE_PATH)

    assert suite["schema_version"] == "baguang_stack_comparison_suite.v1"
    assert suite["common_scene"]["map"] == "straight_road_for_baguang"
    assert suite["common_scene"]["lead_distance_m"] == 300
    stacks = {item["stack"]: item for item in suite["stacks"]}
    assert {"apollo", "autoware"} == set(stacks)
    assert "straight_acc_override" in stacks["apollo"]["declared_assists"]
    assert "speed_feedback_bridge_profile" in stacks["autoware"]["declared_assists"]
    assert suite["comparison_requirements"]["claim_boundary"]["assisted_profiles_prove_unassisted_natural_driving"] is False


def test_comparison_analyzer_passes_synthetic_assisted_pair(tmp_path: Path) -> None:
    suite = _synthetic_suite(tmp_path)

    report = analyze_baguang_stack_comparison(suite)

    assert report["schema_version"] == "baguang_stack_comparison_report.v1"
    assert report["verdict"]["status"] == "pass"
    assert report["claim_boundary"]["can_compare_assisted_profiles"] is True
    assert report["claim_boundary"]["can_claim_unassisted_natural_driving"] is False
    assert report["claim_boundary"]["lane_event_contract"]["status"] == "not_required"
    assert {result["verdict"] for result in report["stack_results"]} == {"pass"}


def test_lane_invasion_disabled_requires_contract_boundary(tmp_path: Path) -> None:
    suite = _synthetic_suite(tmp_path)
    suite["stacks"][0]["declared_assists"].append("lane_invasion_event_disabled")
    contract_path = tmp_path / "lane_event_contract" / "baguang_lane_event_contract_report.json"
    _write_lane_event_contract_report(contract_path, quarantine=True)
    suite["comparison_requirements"]["lane_event_contract"] = {"report_path": str(contract_path)}

    report = analyze_baguang_stack_comparison(suite)
    boundary = report["claim_boundary"]["lane_event_contract"]

    assert report["verdict"]["status"] == "pass"
    assert boundary["status"] == "quarantined"
    assert boundary["disabled_stacks"] == ["apollo"]
    assert boundary["quarantine_recommended"] is True
    assert boundary["lane_invasion_event_can_be_used_as_hard_gate"] is False


def test_lane_invasion_disabled_without_contract_is_visible_in_boundary(tmp_path: Path) -> None:
    suite = _synthetic_suite(tmp_path)
    suite["stacks"][0]["declared_assists"].append("lane_invasion_event_disabled")

    report = analyze_baguang_stack_comparison(suite)
    boundary = report["claim_boundary"]["lane_event_contract"]

    assert report["verdict"]["status"] == "pass"
    assert boundary["status"] == "missing"
    assert boundary["reason"] == "lane_invasion_disabled_without_contract_report_path"
    assert boundary["disabled_stacks"] == ["apollo"]


def test_missing_required_stack_fails_validation(tmp_path: Path) -> None:
    suite = _synthetic_suite(tmp_path)
    suite["stacks"] = [suite["stacks"][0]]

    validation = validate_baguang_stack_comparison_suite(suite)

    assert validation["errors"]
    assert any("autoware" in error for error in validation["errors"])


def test_low_speed_stack_fails_comparison(tmp_path: Path) -> None:
    suite = _synthetic_suite(tmp_path)
    autoware_run = Path(suite["stacks"][1]["evidence_run"])
    _write_run(autoware_run, max_speed=3.0)

    report = analyze_baguang_stack_comparison(suite)

    assert report["verdict"]["status"] == "fail"
    by_stack = {result["stack"]: result for result in report["stack_results"]}
    assert by_stack["autoware"]["verdict"] == "fail"


def test_report_writer_and_cli(tmp_path: Path) -> None:
    suite = _synthetic_suite(tmp_path)
    report = analyze_baguang_stack_comparison(suite)
    outputs = write_baguang_stack_comparison_report(report, tmp_path / "out")

    assert Path(outputs["comparison_report"]).exists()
    assert Path(outputs["comparison_csv"]).read_text(encoding="utf-8").startswith("stack_id,")
    assert "unassisted natural-driving" in Path(outputs["comparison_summary"]).read_text(encoding="utf-8")

    suite_path = tmp_path / "suite.yaml"
    suite_for_yaml = deepcopy(suite)
    suite_path.write_text(json.dumps(suite_for_yaml), encoding="utf-8")
    cli_out = tmp_path / "cli_out"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_stack_comparison.py",
            "--suite",
            str(suite_path),
            "--out",
            str(cli_out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    assert '"status": "pass"' in result.stdout
    assert (cli_out / "baguang_stack_comparison_report.json").exists()
