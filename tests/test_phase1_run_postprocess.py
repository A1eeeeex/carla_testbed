from __future__ import annotations

import csv
import json

from carla_testbed.analysis.phase1_postprocess import run_phase1_postprocess
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_phase1_postprocess_writes_v_t_gap_status_and_completeness(tmp_path) -> None:
    run = _write_complete_run(tmp_path)

    report = run_phase1_postprocess(run)

    assert report["v_t_gap_status"] == "pass"
    assert report["phase1_status"] == "success"
    assert (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").exists()
    assert (run / "analysis" / "phase1_status" / "phase1_status.json").exists()
    completeness = json.loads(
        (run / "analysis" / "phase1_status" / "artifact_completeness.json").read_text(encoding="utf-8")
    )
    assert completeness["status"] == "pass"


def test_phase1_postprocess_missing_timeseries_is_invalid_not_backend_loss(tmp_path) -> None:
    run = _write_complete_run(tmp_path)
    (run / "timeseries.csv").unlink()

    report = run_phase1_postprocess(run)
    status = json.loads((run / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8"))

    assert report["phase1_status"] == "invalid"
    assert status["failure_reason"] == "no_timeseries"
    assert status["evaluable"] is False


def test_phase1_postprocess_missing_target_actor_is_invalid(tmp_path) -> None:
    run = _write_complete_run(tmp_path)
    resolved = json.loads((run / "artifacts" / "fixed_scene_resolved.json").read_text(encoding="utf-8"))
    resolved["target_actor_contract"] = {"status": "missing"}
    (run / "artifacts" / "fixed_scene_resolved.json").write_text(json.dumps(resolved), encoding="utf-8")

    run_phase1_postprocess(run)
    status = json.loads((run / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8"))

    assert status["status"] == "invalid"
    assert status["failure_reason"] == "missing_target_actor"


def _write_complete_run(tmp_path):
    run = tmp_path / "run"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    storyboard = compile_fixed_scene_template(
        load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    )
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run",
                "scenario_id": storyboard["scene_id"],
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(json.dumps({"run_id": "run", "status": "pass", "success": True}), encoding="utf-8")
    (run / "events.jsonl").write_text(json.dumps({"event": "run_finished"}) + "\n", encoding="utf-8")
    (artifacts / "fixed_scene_resolved.json").write_text(json.dumps(storyboard), encoding="utf-8")
    (artifacts / "fixed_scene_runtime_state.json").write_text(json.dumps({"status": "pass"}), encoding="utf-8")
    (artifacts / "scenario_phase_events.jsonl").write_text(json.dumps({"event": "phase_started"}) + "\n", encoding="utf-8")
    (artifacts / "ego_control_trace.jsonl").write_text(json.dumps({"command": {}}) + "\n", encoding="utf-8")
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sim_time", "ego_speed_mps", "ego_x", "ego_y", "ego_yaw_rad", "ego_length_m", "ego_width_m"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": 0.0,
                "ego_speed_mps": 5.0,
                "ego_x": 0.0,
                "ego_y": 0.0,
                "ego_yaw_rad": 0.0,
                "ego_length_m": 4.0,
                "ego_width_m": 2.0,
            }
        )
    (artifacts / "scenario_actor_trace.jsonl").write_text(
        json.dumps(
            {
                "sim_time_sec": 0.0,
                "actor_role": "lead_vehicle",
                "actor_id": "lead-1",
                "actual_speed_mps": 0.0,
                "x": 20.0,
                "y": 0.0,
                "yaw_rad": 0.0,
                "length_m": 4.0,
                "width_m": 2.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return run
