from __future__ import annotations

import json

from carla_testbed.analysis.phase1_acceptance import (
    analyze_phase1_acceptance,
    write_phase1_acceptance,
)


def test_phase1_acceptance_clean_pair_is_done(tmp_path) -> None:
    comparison = _write_comparison(tmp_path, "clean")

    report = analyze_phase1_acceptance(comparison)

    assert report["status"] == "DONE"
    assert report["blocking_reasons"] == []
    assert report["gates"]["backend_coverage"] is True
    assert report["gates"]["scenario_interaction_evaluable"] is True
    assert report["gates"]["target_metric_evaluable"] is True
    assert report["gates"]["artifact_complete"] is True
    assert report["apollo_run_id"] == "apollo_run"
    assert report["planning_control_run_id"] == "builtin_run"


def test_phase1_acceptance_blocks_when_target_interaction_not_exercised(tmp_path) -> None:
    comparison = _write_comparison(
        tmp_path,
        "interaction_missing",
        apollo_overrides={
            "scenario_interaction_evaluable": False,
            "scenario_interaction_reason": "required_phase_not_reached",
            "target_metric_evaluable": False,
            "target_metric_status": "invalid",
        },
        comparison_status="partially_evaluable",
        comparison_target_status="target_interaction_not_exercised",
    )

    report = analyze_phase1_acceptance(comparison)

    assert report["status"] == "PARTIAL"
    assert "comparison_valid" in report["blocking_reasons"]
    assert "scenario_interaction_evaluable" in report["blocking_reasons"]
    assert "target_metric_evaluable" in report["blocking_reasons"]


def test_phase1_acceptance_blocks_reference_bundle_with_blocking_assist(tmp_path) -> None:
    comparison = _write_comparison(
        tmp_path,
        "assist",
        apollo_overrides={"blocking_assists": ["route_follower"]},
    )

    report = analyze_phase1_acceptance(comparison)

    assert report["status"] == "PARTIAL"
    assert "blocking_assists_absent" in report["blocking_reasons"]


def test_phase1_acceptance_requires_explicit_artifact_completeness(tmp_path) -> None:
    comparison = _write_comparison(
        tmp_path,
        "artifact_incomplete",
        builtin_overrides={"artifact_complete": False, "missing_artifacts": ["summary.json"]},
    )

    report = analyze_phase1_acceptance(comparison)

    assert report["status"] == "PARTIAL"
    assert "artifact_complete" in report["blocking_reasons"]


def test_phase1_acceptance_does_not_require_curves_for_route_only_not_applicable_metric(
    tmp_path,
) -> None:
    comparison = _write_comparison(
        tmp_path,
        "route_only",
        apollo_overrides={"target_metric_status": "not_applicable"},
        builtin_overrides={"target_metric_status": "not_applicable"},
        write_curves=False,
    )

    report = analyze_phase1_acceptance(comparison)

    assert report["status"] == "DONE"
    assert report["gates"]["comparison_curves_present"] is True
    assert "comparison_curves_present" not in report["blocking_reasons"]


def test_phase1_acceptance_writer_outputs_json_and_summary(tmp_path) -> None:
    comparison = _write_comparison(tmp_path, "write")
    report = analyze_phase1_acceptance(comparison)
    outputs = write_phase1_acceptance(report, tmp_path / "acceptance")

    assert outputs["report"].endswith("phase1_acceptance_report.json")
    assert outputs["summary"].endswith("phase1_acceptance_summary.md")
    written = json.loads((tmp_path / "acceptance" / "phase1_acceptance_report.json").read_text())
    assert written["status"] == "DONE"
    assert written["gates"]["bundle_self_contained"] is True
    assert written["bundle_materialization"]["self_contained"] is True
    assert (tmp_path / "acceptance" / "evidence" / "comparison" / "comparison_summary.json").exists()
    assert (
        tmp_path
        / "acceptance"
        / "evidence"
        / "runs"
        / "apollo_run"
        / "timeseries.csv"
    ).exists()


def test_phase1_acceptance_writer_blocks_non_self_contained_bundle(tmp_path) -> None:
    comparison = _write_comparison(tmp_path, "missing_raw")
    apollo_run = tmp_path / "missing_raw_apollo"
    (apollo_run / "timeseries.csv").unlink()
    report = analyze_phase1_acceptance(comparison)

    outputs = write_phase1_acceptance(report, tmp_path / "acceptance")

    written = json.loads((tmp_path / "acceptance" / "phase1_acceptance_report.json").read_text())
    assert outputs["report"].endswith("phase1_acceptance_report.json")
    assert written["status"] == "PARTIAL"
    assert written["gates"]["bundle_self_contained"] is False
    assert "bundle_self_contained" in written["blocking_reasons"]
    missing_roles = {
        item.get("role") for item in written["bundle_materialization"]["missing_required_files"]
    }
    assert "timeseries_surface" in missing_roles


def test_phase1_acceptance_writer_requires_control_trace_surface(tmp_path) -> None:
    comparison = _write_comparison(tmp_path, "missing_control_trace")
    apollo_run = tmp_path / "missing_control_trace_apollo"
    (apollo_run / "artifacts" / "control_apply_trace.jsonl").unlink()
    report = analyze_phase1_acceptance(comparison)

    write_phase1_acceptance(report, tmp_path / "acceptance")

    written = json.loads((tmp_path / "acceptance" / "phase1_acceptance_report.json").read_text())
    assert written["status"] == "PARTIAL"
    assert written["gates"]["bundle_self_contained"] is False
    missing_roles = {
        item.get("role") for item in written["bundle_materialization"]["missing_required_files"]
    }
    assert "control_trace_surface" in missing_roles


def _write_comparison(
    tmp_path,
    name,
    *,
    apollo_overrides=None,
    builtin_overrides=None,
    comparison_status="comparable",
    comparison_target_status="apollo_vs_planning_control_evaluable",
    write_curves=True,
):
    apollo = tmp_path / f"{name}_apollo"
    builtin = tmp_path / f"{name}_builtin"
    _write_run(apollo, "apollo_run", "apollo_cyberrt", "apollo_reference_backend")
    _write_run(builtin, "builtin_run", "carla_builtin", "planning_control_backend")
    comparison = tmp_path / "comparisons" / name
    if write_curves:
        (comparison / "comparison_curves").mkdir(parents=True)
        (comparison / "comparison_curves" / "v_t_gap_combined.csv").write_text(
            "run_id,backend,t,gap_m\napollo_run,apollo_cyberrt,0,25\n",
            encoding="utf-8",
        )
    apollo_row = _run_row(apollo, "apollo_run", "apollo_cyberrt", "apollo_reference_backend")
    builtin_row = _run_row(builtin, "builtin_run", "carla_builtin", "planning_control_backend")
    apollo_row.update(apollo_overrides or {})
    builtin_row.update(builtin_overrides or {})
    comparison.mkdir(parents=True, exist_ok=True)
    summary = {
        "schema_version": "phase1_comparison.v1",
        "comparison_id": name,
        "scenario_case": "baguang_follow_stop_static_300m",
        "comparison_status": comparison_status,
        "comparison_target_status": comparison_target_status,
        "participating_runs": [apollo_row, builtin_row],
    }
    manifest = {
        "schema_version": "phase1_scenario_comparison_manifest.v1",
        "comparison_id": name,
        "scenario_case": "baguang_follow_stop_static_300m",
    }
    (comparison / "comparison_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (comparison / "comparison_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return comparison


def _write_run(path, run_id, backend, backend_type):
    (path / "analysis" / "phase1_status").mkdir(parents=True)
    (path / "analysis" / "v_t_gap").mkdir(parents=True)
    (path / "artifacts").mkdir(parents=True)
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "scenario_id": "baguang_follow_stop_static_300m",
                "scenario_case": "baguang_follow_stop_static_300m",
                "backend": backend,
                "backend_type": backend_type,
            }
        ),
        encoding="utf-8",
    )
    (path / "summary.json").write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    (path / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps\n0.0,0.0\n0.1,1.0\n",
        encoding="utf-8",
    )
    (path / "events.jsonl").write_text(
        json.dumps({"event": "start", "sim_time": 0.0}) + "\n",
        encoding="utf-8",
    )
    if backend == "apollo_cyberrt":
        (path / "artifacts" / "control_apply_trace.jsonl").write_text(
            json.dumps({"sim_time": 0.0, "throttle": 0.0, "brake": 0.0, "steer": 0.0})
            + "\n",
            encoding="utf-8",
        )
    else:
        (path / "artifacts" / "ego_control_trace.jsonl").write_text(
            json.dumps({"sim_time": 0.0, "throttle": 0.0, "brake": 0.0, "steer": 0.0})
            + "\n",
            encoding="utf-8",
        )
    (path / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps({"status": "success", "run_evaluable": True}),
        encoding="utf-8",
    )
    (path / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass"}),
        encoding="utf-8",
    )
    (path / "analysis" / "v_t_gap" / "v_t_gap.csv").write_text(
        "sim_time,gap_m\n0.0,25.0\n",
        encoding="utf-8",
    )
    (path / "artifacts" / "scenario_actor_trace.jsonl").write_text(
        json.dumps(
            {
                "sim_time": 0.0,
                "role": "lead_vehicle",
                "actor_id": 101,
                "x": 25.0,
                "y": 0.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (path / "artifacts" / "scenario_phase_events.jsonl").write_text(
        json.dumps(
            {
                "sim_time": 0.0,
                "phase": "follow_stop_static",
                "target_actor_role": "lead_vehicle",
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _run_row(path, run_id, backend, backend_type):
    return {
        "run_dir": str(path),
        "run_id": run_id,
        "scenario_case": "baguang_follow_stop_static_300m",
        "backend": backend,
        "backend_type": backend_type,
        "run_evaluable": True,
        "scenario_interaction_evaluable": True,
        "target_metric_evaluable": True,
        "target_metric_status": "pass",
        "artifact_complete": True,
        "blocking_assists": [],
    }
