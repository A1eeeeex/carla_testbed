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
    assert report["failure_reason"] == "missing_required_artifact"
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
    assert report["evaluable"] is True


def test_phase1_status_writer(tmp_path) -> None:
    run = _base_run(tmp_path)
    report = classify_phase1_run(run)

    outputs = write_phase1_status(report, tmp_path / "out")

    assert outputs["report"].endswith("phase1_status.json")


def _base_run(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps({"run_id": "r1", "backend": "carla_builtin", "backend_type": "planning_control_backend"}),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(json.dumps({"success": True, "status": "pass"}), encoding="utf-8")
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    return run
