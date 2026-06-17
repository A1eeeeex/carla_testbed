from __future__ import annotations

import json

from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status


def test_missing_timeseries_is_invalid_not_backend_failure(tmp_path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(json.dumps({"run_id": "r1", "backend": "apollo_cyberrt"}), encoding="utf-8")
    (run / "summary.json").write_text(json.dumps({"success": False}), encoding="utf-8")

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "no_timeseries"
    assert report["invalid_reasons"] == ["no_timeseries"]
    assert report["failed_reasons"] == []
    assert report["degraded_reasons"] == []
    assert report["evaluable"] is False
    assert any(path.endswith("manifest.json") for path in report["evidence_files"])
    assert "invalid_run_is_setup_or_evidence_failure_not_backend_loss" in report["notes"]


def test_backend_not_ready_takes_priority_over_missing_timeseries(tmp_path) -> None:
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps({"run_id": "r1", "backend": "apollo_cyberrt", "backend_ready": False}),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "failure_reason": "backend_not_ready"}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "backend_not_ready"
    assert report["invalid_reasons"] == ["backend_not_ready"]
    assert report["evaluable"] is False


def test_missing_target_actor_is_invalid(tmp_path) -> None:
    run = _base_run(tmp_path)
    (run / "artifacts").mkdir()
    (run / "artifacts" / "fixed_scene_resolved.json").write_text(
        json.dumps({"target_actor_contract": {"status": "missing", "invalid_reason": "missing_target_actor"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "missing_target_actor"


def test_negative_gap_is_failed_unsafe_gap(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": -0.5}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "unsafe_gap"
    assert report["failed_reasons"] == ["unsafe_gap"]
    assert report["invalid_reasons"] == []
    assert report["degraded_reasons"] == []
    assert report["evaluable"] is True


def test_large_final_gap_is_degraded(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 120.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "degraded"
    assert report["failure_reason"] == "large_final_gap"
    assert report["degraded_reasons"] == ["large_final_gap"]
    assert report["failed_reasons"] == []
    assert report["invalid_reasons"] == []
    assert report["evaluable"] is True
    assert report["scenario_case"] == "baguang_follow_stop_static_300m"


def test_degraded_gap_method_is_not_rewritten_as_large_final_gap(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps(
            {
                "status": "warn",
                "degraded_reason_counts": {"existing_lead_gap_m_degraded": 2},
                "rows": [{"gap_m": 120.0, "gap_degraded": True}],
                "target_actor_contract": {"status": "resolved"},
            }
        ),
        encoding="utf-8",
    )

    report = classify_phase1_run(run)

    assert report["status"] == "degraded"
    assert report["failure_reason"] == "degraded_gap_method"
    assert report["degraded_reasons"] == ["degraded_gap_method"]


def test_missing_v_t_gap_report_is_invalid_missing_required_artifact(tmp_path) -> None:
    run = _base_run(tmp_path)

    report = classify_phase1_run(run)

    assert report["status"] == "invalid"
    assert report["failure_reason"] == "missing_required_artifact"
    assert report["invalid_reasons"] == ["missing_required_artifact"]
    assert report["evaluable"] is False


def test_phase1_status_writer(tmp_path) -> None:
    run = _base_run(tmp_path)
    out = run / "analysis" / "v_t_gap"
    out.mkdir(parents=True)
    (out / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": [{"gap_m": 10.0}], "target_actor_contract": {"status": "resolved"}}),
        encoding="utf-8",
    )
    report = classify_phase1_run(run)

    outputs = write_phase1_status(report, tmp_path / "out")

    assert outputs["report"].endswith("phase1_status.json")
    written = json.loads((tmp_path / "out" / "phase1_status.json").read_text(encoding="utf-8"))
    assert written["scenario_case"] == "baguang_follow_stop_static_300m"
    assert any(path.endswith("analysis/v_t_gap/v_t_gap_report.json") for path in written["evidence_files"])


def _base_run(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "scenario_case": "baguang_follow_stop_static_300m",
                "scenario_id": "baguang_follow_stop_static_300m",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(json.dumps({"success": True, "status": "pass"}), encoding="utf-8")
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    return run
