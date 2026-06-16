from __future__ import annotations

import csv
import json

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs, write_scenario_comparison


def test_two_evaluable_runs_are_comparable(tmp_path) -> None:
    run_a = _write_run(tmp_path, "apollo", "apollo_cyberrt", "apollo_reference_backend", "success")
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "degraded")

    report = compare_scenario_runs([run_a, run_b])

    assert report["schema_version"] == "phase1_comparison.v1"
    assert report["comparison_status"] == "comparable"
    assert all(item["counts_as_backend_loss"] is False for item in report["backend_results"])


def test_invalid_run_does_not_count_as_backend_loss(tmp_path) -> None:
    run_a = _write_run(tmp_path, "apollo", "apollo_cyberrt", "apollo_reference_backend", "invalid", "backend_not_ready")
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "partially_evaluable"
    apollo = [item for item in report["backend_results"] if item["backend"] == "apollo_cyberrt"][0]
    assert apollo["counts_as_backend_loss"] is False


def test_scenario_mismatch_is_invalid(tmp_path) -> None:
    run_a = _write_run(tmp_path, "a", "apollo_cyberrt", "apollo_reference_backend", "success", scenario_id="s1")
    run_b = _write_run(tmp_path, "b", "carla_builtin", "planning_control_backend", "success", scenario_id="s2")

    report = compare_scenario_runs([run_a, run_b])

    assert report["comparison_status"] == "invalid"
    assert report["reason"] == "scenario_id_mismatch"


def test_scenario_comparison_writer(tmp_path) -> None:
    run_a = _write_run(tmp_path, "apollo", "apollo_cyberrt", "apollo_reference_backend", "success")
    run_b = _write_run(tmp_path, "builtin", "carla_builtin", "planning_control_backend", "success")
    report = compare_scenario_runs([run_a, run_b])

    outputs = write_scenario_comparison(report, tmp_path / "comparison")

    assert outputs["manifest"].endswith("comparison_manifest.json")
    assert outputs["summary"].endswith("comparison_summary.json")
    assert outputs["v_t_gap_csv"].endswith("comparison_curves/v_t_gap.csv")


def _write_run(tmp_path, name, backend, backend_type, status, reason=None, scenario_id="follow_stop_static"):
    run = tmp_path / name
    analysis = run / "analysis" / "phase1_status"
    analysis.mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps({"run_id": name, "scenario_id": scenario_id, "backend": backend, "backend_type": backend_type}),
        encoding="utf-8",
    )
    (analysis / "phase1_status.json").write_text(
        json.dumps(
            {
                "run_id": name,
                "scenario_id": scenario_id,
                "backend": backend,
                "backend_type": backend_type,
                "status": status,
                "failure_reason": reason,
                "evaluable": status != "invalid",
            }
        ),
        encoding="utf-8",
    )
    v_t_gap_dir = run / "analysis" / "v_t_gap"
    v_t_gap_dir.mkdir(parents=True)
    (v_t_gap_dir / "v_t_gap_report.json").write_text(json.dumps({"status": "pass", "rows": []}), encoding="utf-8")
    with (v_t_gap_dir / "v_t_gap.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time_s",
                "ego_speed_mps",
                "target_speed_mps",
                "gap_m",
                "relative_speed_mps",
                "target_actor_id",
                "target_actor_role",
                "gap_method",
                "gap_degraded",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time_s": 0.0,
                "ego_speed_mps": 1.0,
                "target_speed_mps": 0.0,
                "gap_m": 10.0,
                "relative_speed_mps": -1.0,
                "target_actor_id": "lead-1",
                "target_actor_role": "lead_vehicle",
                "gap_method": "bumper_to_bumper_longitudinal_projection",
                "gap_degraded": "False",
            }
        )
    return run
