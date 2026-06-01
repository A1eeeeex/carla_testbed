from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

from carla_testbed.analysis.town01_postprocess import postprocess_town01_goal
from tools.postprocess_town01_goal import main as postprocess_main

FIXTURE_BATCH = Path(__file__).resolve().parent / "fixtures" / "ab" / "simple_batch"
PROFILE = Path("configs/calibration/control_actuation.yaml")


def _natural_driving_report() -> dict:
    traffic_light_behaviors = {
        "traffic_light_red_stop": "red_stop",
        "traffic_light_green_go": "green_go",
        "traffic_light_red_to_green_release": "red_to_green_release",
    }
    scenario_classes = (
        "lane_keep",
        "curve_diagnostic",
        "junction_turn",
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    )
    route_ids = {
        "lane_keep": "lane097",
        "curve_diagnostic": "curve217",
        "junction_turn": "junction031",
        "traffic_light_red_stop": "traffic_light_red_stop_tbd",
        "traffic_light_green_go": "traffic_light_green_go_tbd",
        "traffic_light_red_to_green_release": "traffic_light_red_to_green_release_tbd",
    }
    scenario_ids = {
        "lane_keep": "lane_keep_097",
        "curve_diagnostic": "curve_diagnostic_217",
        "junction_turn": "junction_turn_031",
        "traffic_light_red_stop": "traffic_light_red_stop_placeholder",
        "traffic_light_green_go": "traffic_light_green_go_placeholder",
        "traffic_light_red_to_green_release": "traffic_light_red_to_green_release_placeholder",
    }
    return {
        "schema_version": "town01_natural_driving_report.v1",
        "run_count": len(scenario_classes),
        "summary": {
            "pass_count": len(scenario_classes),
            "warn_count": 0,
            "fail_count": 0,
            "insufficient_data_count": 0,
        },
        "verdict": {
            "status": "pass",
            "can_claim_full_natural_driving": True,
            "suite_plan_missing": [],
            "missing_required_scenario_classes": [],
            "unproven_required_scenario_classes": [],
            "missing_required_scenario_ids": [],
            "unproven_required_scenario_ids": [],
            "scenario_identity_mismatches": [],
            "failed_runs": [],
            "insufficient_data_runs": [],
            "warning_runs": [],
        },
        "capability_coverage": {
            "required_scenario_classes": list(scenario_classes),
            "observed_scenario_classes": list(scenario_classes),
            "missing_required_scenario_classes": [],
            "unproven_required_scenario_classes": [],
            "missing_required_scenario_ids": [],
            "unproven_required_scenario_ids": [],
            "scenario_identity_mismatches": [],
            "can_claim_full_natural_driving": True,
        },
        "suite_plan_scope": {
            "has_suite_manifest": True,
            "has_run_matrix": True,
            "filtered": False,
            "scenario_class_filter": [],
            "scenario_id_filter": [],
        },
        "run_results": [
            {
                "run_id": f"{scenario_class}_run",
                "scenario_id": scenario_ids[scenario_class],
                "scenario_class": scenario_class,
                "route_id": route_ids[scenario_class],
                "run_dir": f"runs/natural/{scenario_class}_run",
                "verdict": "pass",
                "artifacts": {
                    "artifact_completeness": (
                        f"runs/natural/{scenario_class}_run/analysis/artifact_completeness/"
                        "artifact_completeness_report.json"
                    ),
                },
                "artifact_completeness": {
                    "status": "pass",
                    "artifact_complete": True,
                    "missing_artifacts": [],
                    "missing_manifest_fields": [],
                    "missing_control_trace_fields": [],
                },
                "traffic_light_expected_behavior": traffic_light_behaviors.get(scenario_class),
                "traffic_light_expectation_source": "manifest"
                if scenario_class in traffic_light_behaviors
                else None,
                "traffic_light_stimulus_mode": "deterministic_gt_control"
                if scenario_class in traffic_light_behaviors
                else None,
                "traffic_light_claim_grade": True if scenario_class in traffic_light_behaviors else None,
            }
            for scenario_class in scenario_classes
        ],
    }


def _write_artifact_completeness_files(report: dict, root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    report["suite_run_root"] = str(root)
    with (root / "suite_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "schema_version": "natural_driving_suite_manifest.v1",
                "dry_run": False,
                "run_count": len(report["run_results"]),
                "scenario_class_filter": [],
                "scenario_id_filter": [],
                "runs": [
                    {
                        "run_id": run["run_id"],
                        "scenario_id": run["scenario_id"],
                        "route_id": run["route_id"],
                        "scenario_class": run["scenario_class"],
                        "run_dir": str(root / str(run["run_id"])),
                        "status": "success",
                        "return_code": 0,
                        "artifact_index_status": "found",
                    }
                    for run in report["run_results"]
                ],
            },
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")
    with (root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "scenario_id",
                "route_id",
                "scenario_class",
                "run_dir",
                "status",
                "return_code",
                "artifact_index_status",
            ],
        )
        writer.writeheader()
        for run in report["run_results"]:
            writer.writerow(
                {
                    "run_id": run["run_id"],
                    "scenario_id": run["scenario_id"],
                    "route_id": run["route_id"],
                    "scenario_class": run["scenario_class"],
                    "run_dir": str(root / str(run["run_id"])),
                    "status": "success",
                    "return_code": 0,
                    "artifact_index_status": "found",
                }
            )
    for run in report["run_results"]:
        actual_run_dir = root / str(run["run_id"])
        run["run_dir"] = str(actual_run_dir)
        output = actual_run_dir / "analysis" / "artifact_completeness"
        output.mkdir(parents=True, exist_ok=True)
        path = output / "artifact_completeness_report.json"
        payload = {
            "schema_version": "run_artifact_completeness.v1",
            "status": "pass",
            "artifact_complete": True,
            "run_dir": str(actual_run_dir),
            "run_id": run["run_id"],
            "scenario_id": run["scenario_id"],
            "route_id": run["route_id"],
            "scenario_class": run["scenario_class"],
            "missing_artifacts": [],
            "missing_manifest_fields": [],
            "invalid_manifest_source_fields": [],
            "missing_control_trace_fields": [],
            "invalid_report_source_fields": [],
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        run["artifacts"]["artifact_completeness"] = str(path)


def _insufficient_natural_driving_report() -> dict:
    report = _natural_driving_report()
    report["summary"] = {
        "pass_count": 5,
        "warn_count": 0,
        "fail_count": 0,
        "insufficient_data_count": 1,
    }
    report["verdict"] = {
        "status": "insufficient_data",
        "failed_runs": [],
        "insufficient_data_runs": ["traffic_light_red_stop_run"],
        "warning_runs": [],
    }
    for run in report["run_results"]:
        if run["scenario_class"] == "traffic_light_red_stop":
            run.update(
                {
                    "verdict": "insufficient_data",
                    "failure_reason": "missing_required_artifacts",
                    "route_id": "traffic_light_red_stop",
                    "missing_artifacts": ["traffic_light_contract_report.json"],
                    "missing_fields": ["stopped_at_red"],
                }
            )
    return report


def test_postprocess_generates_evidence_bundle_without_running_online_stack(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    out = tmp_path / "postprocess"

    result = postprocess_town01_goal(
        hard_gate_batch=batch,
        out_dir=out,
        calibration_profile=PROFILE,
    )

    assert result["schema_version"] == "town01_postprocess.v1"
    assert (out / "town01_postprocess.json").is_file()
    assert (out / "town01_postprocess.md").is_file()
    assert (out / "ab" / "hard_gates" / "ab_report.json").is_file()
    assert (out / "calibration" / "calibration_gate_results.json").is_file()
    assert (out / "calibration" / "calibration_report.json").is_file()
    assert (out / "audit" / "town01_goal_audit.json").is_file()

    assert result["route_health"]["existing"]
    assert all(path.endswith("route_health.json") for path in result["route_health"]["generated"])
    assert all(str(out / "route_health") in path for path in result["route_health"]["generated"])
    assert result["route_curve_artifact_gap"]["generated"]
    assert all(
        path.endswith("route_curve_artifact_gap_report.json")
        for path in result["route_curve_artifact_gap"]["generated"]
    )
    assert all(
        str(out / "route_curve_artifact_gap") in path
        for path in result["route_curve_artifact_gap"]["generated"]
    )
    assert not (batch / "runs" / "candidate_junction031" / "analysis").exists()
    assert result["calibration"]["trials"]["trial_count"] > 0
    assert result["natural_driving"]["status"] == "missing"
    with (out / "calibration" / "calibration_trials.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert {"throttle", "steer"}.issubset({row["command_type"] for row in rows})

    report = json.loads((out / "town01_postprocess.json").read_text(encoding="utf-8"))
    assert report["status"] == result["status"]
    markdown = (out / "town01_postprocess.md").read_text(encoding="utf-8").lower()
    assert "curve health from short-window transport evidence" in markdown


def test_postprocess_accepts_natural_driving_report_for_goal_audit(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    natural_report = tmp_path / "natural_driving_report.json"
    report = _natural_driving_report()
    _write_artifact_completeness_files(report, tmp_path / "natural_artifacts")
    natural_report.write_text(json.dumps(report), encoding="utf-8")
    out = tmp_path / "postprocess"

    result = postprocess_town01_goal(
        hard_gate_batch=batch,
        out_dir=out,
        calibration_profile=PROFILE,
        natural_driving_report=natural_report,
    )

    assert result["inputs"]["natural_driving_report"] == str(natural_report)
    assert result["natural_driving"]["status"] == "pass"
    audit = json.loads((out / "audit" / "town01_goal_audit.json").read_text(encoding="utf-8"))
    assert audit["sources"]["natural_driving_report"] == str(natural_report)
    assert audit["sections"]["natural_driving"]["status"] == "pass"


def test_postprocess_cli_requires_a_batch(tmp_path: Path, capsys) -> None:
    rc = postprocess_main(["--out", str(tmp_path / "out")])
    assert rc == 2
    assert "at least one" in capsys.readouterr().err


def test_postprocess_cli_writes_summary(tmp_path: Path, capsys) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    natural_report = tmp_path / "natural_driving_report.json"
    report = _natural_driving_report()
    _write_artifact_completeness_files(report, tmp_path / "natural_artifacts")
    natural_report.write_text(json.dumps(report), encoding="utf-8")
    out = tmp_path / "postprocess"

    rc = postprocess_main(
        [
            "--hard-gate-batch",
            str(batch),
            "--natural-driving-report",
            str(natural_report),
            "--out",
            str(out),
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["outputs"]["town01_postprocess_json"] == str(out / "town01_postprocess.json")
    assert payload["audit_status"] == payload["status"]
    assert isinstance(payload["missing_evidence"], list)
    assert payload["missing_evidence_count"] == len(payload["missing_evidence"])
    assert payload["next_actions"]
    assert "strict_goal_audit" in payload["next_action_commands"]
    assert "natural_driving_online" in payload["next_action_commands"]
    assert "--fail-on-status incomplete" in payload["next_action_commands"]["strict_goal_audit"]
    assert payload["ab_report_count"] == 1
    assert payload["route_health_existing_count"] >= 0
    assert payload["route_health_generated_count"] >= 0
    assert "calibration_status" in payload
    assert payload["natural_driving_status"] == "pass"
    assert payload["natural_driving_problem_count"] == 0
    assert payload["natural_driving_problem_details"] == []
    assert "curve_pair_status" in payload
    assert (out / "town01_postprocess.json").is_file()


def test_postprocess_cli_surfaces_natural_driving_problem_details(tmp_path: Path, capsys) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    natural_report = tmp_path / "natural_driving_report.json"
    natural_report.write_text(json.dumps(_insufficient_natural_driving_report()), encoding="utf-8")
    out = tmp_path / "postprocess"

    rc = postprocess_main(
        [
            "--hard-gate-batch",
            str(batch),
            "--natural-driving-report",
            str(natural_report),
            "--out",
            str(out),
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["natural_driving_status"] == "insufficient_data"
    assert payload["natural_driving_problem_count"] == 1
    detail = payload["natural_driving_problem_details"][0]
    assert detail["failure_reason"] == "missing_required_artifacts"
    assert "traffic_light_contract_report.json" in detail["missing_artifacts"]
    assert "stopped_at_red" in detail["missing_fields"]
