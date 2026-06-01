from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml

from carla_testbed.analysis.baguang_assist_debt import (
    analyze_baguang_assist_debt,
    write_baguang_assist_debt_report,
)


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix in {".yaml", ".yml"}:
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _config() -> dict:
    return {
        "schema_version": "baguang_assist_reduction.v1",
        "comparison_suite": "suite.yaml",
        "stacks": {
            "apollo": {
                "profiles": [
                    {
                        "profile_id": "apollo_assisted_baseline",
                        "expected_assists": [
                            "carla_direct_transport",
                            "straight_lane_lateral_stabilizer",
                            "straight_acc_override",
                            "terminal_stop_hold",
                            "lane_invasion_event_disabled",
                        ],
                    },
                    {
                        "profile_id": "apollo_no_lateral_stabilizer",
                        "expected_assists": [
                            "carla_direct_transport",
                            "straight_acc_override",
                            "terminal_stop_hold",
                            "lane_invasion_event_disabled",
                        ],
                    },
                    {
                        "profile_id": "apollo_no_terminal_stop_hold",
                        "expected_assists": [
                            "carla_direct_transport",
                            "straight_lane_lateral_stabilizer",
                            "straight_acc_override",
                            "lane_invasion_event_disabled",
                        ],
                    },
                    {
                        "profile_id": "apollo_lane_invasion_enabled_probe",
                        "expected_assists": [
                            "carla_direct_transport",
                            "straight_lane_lateral_stabilizer",
                            "straight_acc_override",
                            "terminal_stop_hold",
                        ],
                    },
                ]
            },
            "autoware": {
                "profiles": [
                    {
                        "profile_id": "autoware_assisted_baseline",
                        "expected_assists": [
                            "goal_planner_module_disabled",
                            "bridge_smoothing",
                            "recording_timeout_sec_5",
                            "lane_invasion_event_disabled",
                        ],
                    },
                    {
                        "profile_id": "autoware_no_bridge_smoothing_probe",
                        "expected_assists": [
                            "goal_planner_module_disabled",
                            "recording_timeout_sec_5",
                            "lane_invasion_event_disabled",
                        ],
                    },
                ]
            },
        },
    }


def _comparison() -> dict:
    return {
        "schema_version": "baguang_stack_comparison_report.v1",
        "claim_boundary": {
            "lane_event_contract": {
                "status": "quarantined",
                "quarantine_recommended": True,
                "lane_invasion_event_can_be_used_as_hard_gate": False,
            }
        },
        "stack_results": [
            {
                "stack_id": "apollo_carla_direct_assisted",
                "stack": "apollo",
                "declared_assists": [
                    "carla_direct_transport",
                    "straight_lane_lateral_stabilizer",
                    "straight_acc_override",
                    "lane_invasion_event_disabled",
                ],
            },
            {
                "stack_id": "autoware_ros2_assisted",
                "stack": "autoware",
                "declared_assists": [
                    "goal_planner_module_disabled",
                    "recording_timeout_sec_5",
                    "lane_invasion_event_disabled",
                ],
            },
        ],
    }


def _manifest() -> dict:
    return {
        "schema_version": "baguang_assist_reduction_manifest.v1",
        "dry_run": False,
        "runs": [
            {
                "stack": "apollo",
                "profile_id": "apollo_no_lateral_stabilizer",
                "status": "failed",
                "failure_reason": "collision",
                "expected_assists": [
                    "carla_direct_transport",
                    "straight_acc_override",
                    "terminal_stop_hold",
                    "lane_invasion_event_disabled",
                ],
                "actual_run_dir": "runs/apollo/no_lateral",
            },
            {
                "stack": "apollo",
                "profile_id": "apollo_no_terminal_stop_hold",
                "status": "success",
                "expected_assists": [
                    "carla_direct_transport",
                    "straight_lane_lateral_stabilizer",
                    "straight_acc_override",
                    "lane_invasion_event_disabled",
                ],
                "actual_run_dir": "runs/apollo/no_terminal",
            },
            {
                "stack": "apollo",
                "profile_id": "apollo_lane_invasion_enabled_probe",
                "status": "failed",
                "failure_reason": "lane_invasion",
                "expected_assists": [
                    "carla_direct_transport",
                    "straight_lane_lateral_stabilizer",
                    "straight_acc_override",
                    "terminal_stop_hold",
                ],
                "actual_run_dir": "runs/apollo/lane_enabled",
            },
            {
                "stack": "autoware",
                "profile_id": "autoware_no_bridge_smoothing_probe",
                "status": "success",
                "expected_assists": [
                    "goal_planner_module_disabled",
                    "recording_timeout_sec_5",
                    "lane_invasion_event_disabled",
                ],
                "actual_run_dir": "runs/autoware/no_smoothing",
            },
        ],
    }


def test_assist_debt_classifies_reduction_evidence(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    comparison = tmp_path / "comparison.json"
    manifest = tmp_path / "batch" / "assist_reduction_manifest.json"
    _write(config, _config())
    _write(comparison, _comparison())
    _write(manifest, _manifest())

    report = analyze_baguang_assist_debt(
        config_path=config,
        comparison_report_path=comparison,
        reduction_roots=[manifest],
    )
    by_key = {(item["stack"], item["assist"]): item for item in report["assist_results"]}

    assert by_key[("apollo", "terminal_stop_hold")]["status"] == "removable_once"
    assert by_key[("apollo", "straight_lane_lateral_stabilizer")]["status"] == "blocking_when_removed_once"
    assert by_key[("apollo", "lane_invasion_event_disabled")]["status"] == "quarantined_by_lane_event_contract"
    assert by_key[("autoware", "bridge_smoothing")]["status"] == "removable_once"
    assert report["verdict"]["status"] == "warn"
    assert "apollo:straight_lane_lateral_stabilizer" in report["verdict"]["blocking_assists"]
    assert report["verdict"]["next_priority"] == "debug blocker apollo:straight_lane_lateral_stabilizer"


def test_writer_and_cli(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    comparison = tmp_path / "comparison.json"
    manifest = tmp_path / "batch" / "assist_reduction_manifest.json"
    out = tmp_path / "out"
    cli_out = tmp_path / "cli_out"
    _write(config, _config())
    _write(comparison, _comparison())
    _write(manifest, _manifest())

    report = analyze_baguang_assist_debt(
        config_path=config,
        comparison_report_path=comparison,
        reduction_roots=[manifest],
    )
    outputs = write_baguang_assist_debt_report(report, out)

    assert Path(outputs["report"]).exists()
    assert Path(outputs["csv"]).read_text(encoding="utf-8").startswith("stack,assist,")
    assert "Baguang Assist Debt Report" in Path(outputs["summary"]).read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_baguang_assist_debt.py",
            "--config",
            str(config),
            "--comparison-report",
            str(comparison),
            "--reduction-root",
            str(manifest),
            "--out",
            str(cli_out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert '"status": "warn"' in result.stdout
    assert (cli_out / "baguang_assist_debt_report.json").exists()
