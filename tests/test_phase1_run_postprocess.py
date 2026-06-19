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


def test_phase1_postprocess_writes_apollo_fixed_scene_readiness(tmp_path) -> None:
    run = _write_complete_run(tmp_path)
    _make_run_apollo_fixed_scene(run)

    report = run_phase1_postprocess(run)
    readiness = json.loads(
        (
            run
            / "analysis"
            / "phase1_apollo_fixed_scene_readiness"
            / "phase1_apollo_fixed_scene_readiness_report.json"
        ).read_text(encoding="utf-8")
    )
    status = json.loads((run / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8"))

    assert report["apollo_fixed_scene_readiness_status"] == "fail"
    assert report["apollo_fixed_scene_compat_status"] == "insufficient_data"
    assert readiness["target_actor_role"] == "lead_vehicle"
    assert "front_obstacle_actor_probe_disabled" in readiness["blocking_reasons"]
    assert "target_role_not_in_front_obstacle_role_names" in readiness["blocking_reasons"]
    assert status["status"] == "invalid"
    assert status["failure_reason"] == "missing_apollo_obstacle_gt_contract"


def test_phase1_postprocess_uses_manifest_bridge_config_for_readiness(tmp_path) -> None:
    run = _write_complete_run(tmp_path)
    _make_run_apollo_fixed_scene(run)
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["bridge_config_path"] = "configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    report = run_phase1_postprocess(run)
    readiness = json.loads(
        (
            run
            / "analysis"
            / "phase1_apollo_fixed_scene_readiness"
            / "phase1_apollo_fixed_scene_readiness_report.json"
        ).read_text(encoding="utf-8")
    )

    assert report["apollo_fixed_scene_readiness_status"] == "pass"
    assert readiness["bridge_config_source"] == "explicit_path"
    assert readiness["status"] == "pass"
    assert readiness["target_role_covered_by_bridge_roles"] is True


def test_phase1_postprocess_derives_apollo_fixed_scene_trace_from_obstacle_rows(tmp_path) -> None:
    run = _write_complete_run(tmp_path)
    _make_run_apollo_fixed_scene(run)
    (run / "artifacts" / "fixed_scene_runtime_state.json").unlink()
    (run / "artifacts" / "scenario_actor_trace.jsonl").unlink()
    (run / "artifacts" / "scenario_phase_events.jsonl").unlink()
    (run / "artifacts" / "obstacle_gt_contract.jsonl").write_text(
        json.dumps(
            {
                "timestamp": 0.0,
                "ego_actor_id": "42",
                "carla_actor_id": "101",
                "apollo_perception_id": "lead_vehicle_101",
                "front_obstacle_actor_role": "lead_vehicle",
                "front_obstacle_actor_id": "101",
                "is_ego": False,
                "front_obstacle_actor_x": 20.0,
                "front_obstacle_actor_y": 0.0,
                "front_obstacle_actor_yaw_deg": 0.0,
                "front_obstacle_actor_speed_mps": 0.0,
                "front_obstacle_gap_lon_m": 20.0,
                "front_obstacle_gap_lat_m": 0.0,
                "front_obstacle_gap_distance_m": 20.0,
                "length": 4.0,
                "width": 2.0,
                "height": 1.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = run_phase1_postprocess(run)
    compat = json.loads(
        (
            run
            / "analysis"
            / "phase1_apollo_fixed_scene_compat"
            / "phase1_apollo_fixed_scene_compat_report.json"
        ).read_text(encoding="utf-8")
    )
    runtime_state = json.loads((run / "artifacts" / "fixed_scene_runtime_state.json").read_text(encoding="utf-8"))
    trace_rows = [
        json.loads(line)
        for line in (run / "artifacts" / "scenario_actor_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    phase_events = [
        json.loads(line)
        for line in (run / "artifacts" / "scenario_phase_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    vt_gap = json.loads((run / "analysis" / "v_t_gap" / "v_t_gap_report.json").read_text(encoding="utf-8"))
    status = json.loads((run / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8"))

    assert report["apollo_fixed_scene_compat_status"] == "pass"
    assert report["v_t_gap_status"] == "pass"
    assert compat["actor_roles"] == {"lead_vehicle": "101"}
    assert runtime_state["actor_roles"] == {"lead_vehicle": "101"}
    assert runtime_state["actor_role_source"] == "obstacle_gt_contract_record_roles"
    assert trace_rows[0]["source"] == "derived_from_apollo_obstacle_gt_contract"
    assert phase_events[0]["source"] == "derived_from_apollo_obstacle_gt_contract"
    assert vt_gap["rows"][0]["target_actor_id"] == "101"
    assert status["status"] == "success"
    assert status["evaluable"] is True


def test_phase1_postprocess_auto_binds_legacy_apollo_scenario_path_from_config(tmp_path) -> None:
    run = tmp_path / "legacy_apollo_auto_bind"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "legacy_apollo_auto_bind",
                "backend": "apollo_cyberrt",
                "backend_name": "harness",
                "metadata": {"scenario_metadata": {"front_role": "front"}},
            }
        ),
        encoding="utf-8",
    )
    (run / "config.resolved.yaml").write_text(
        "runtime:\n"
        "  postprocess:\n"
        "    phase1_scenario_path: configs/scenarios/baguang/follow_stop_static_300m.yaml\n",
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": True, "status": "pass", "metadata": {"scenario_metadata": {"front_role": "front"}}}),
        encoding="utf-8",
    )
    (run / "events.jsonl").write_text(json.dumps({"event": "run_finished"}) + "\n", encoding="utf-8")
    (run / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps,ego_x,ego_y,ego_yaw_rad,ego_length_m,ego_width_m\n"
        "0.0,1.0,0.0,0.0,0.0,4.0,2.0\n"
        "10.0,20.0,250.0,0.0,0.0,4.0,2.0\n",
        encoding="utf-8",
    )
    (artifacts / "control_apply_trace.jsonl").write_text(json.dumps({"control": {}}) + "\n", encoding="utf-8")
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "front_obstacle_behavior": {
                    "mode": "cruise_then_stop",
                    "actor_probe_enabled": True,
                    "role_names": ["front"],
                }
            }
        ),
        encoding="utf-8",
    )
    base_obstacle = {
        "carla_actor_id": "212",
        "apollo_perception_id": "212",
        "front_obstacle_actor_role": "front",
        "front_obstacle_actor_id": "212",
        "is_ego": False,
        "front_obstacle_actor_y": 0.0,
        "front_obstacle_actor_yaw_deg": 0.0,
        "front_obstacle_actor_speed_mps": 0.0,
        "front_obstacle_gap_lat_m": 0.0,
        "length": 4.0,
        "width": 2.0,
        "height": 1.5,
    }
    obstacle_rows = [
        {
            **base_obstacle,
            "timestamp": 0.0,
            "front_obstacle_actor_x": 300.0,
            "front_obstacle_gap_lon_m": 300.0,
            "front_obstacle_gap_distance_m": 300.0,
        },
        {
            **base_obstacle,
            "timestamp": 10.0,
            "front_obstacle_actor_x": 300.0,
            "front_obstacle_gap_lon_m": 50.0,
            "front_obstacle_gap_distance_m": 50.0,
        },
    ]
    (artifacts / "obstacle_gt_contract.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in obstacle_rows),
        encoding="utf-8",
    )

    report = run_phase1_postprocess(run)
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    binding = json.loads(
        (
            run
            / "analysis"
            / "phase1_scenario_binding"
            / "phase1_scenario_binding_report.json"
        ).read_text(encoding="utf-8")
    )
    compat = json.loads(
        (
            run
            / "analysis"
            / "phase1_apollo_fixed_scene_compat"
            / "phase1_apollo_fixed_scene_compat_report.json"
        ).read_text(encoding="utf-8")
    )
    status = json.loads((run / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8"))

    assert report["phase1_scenario_binding_status"] == "pass"
    assert report["apollo_fixed_scene_readiness_status"] == "warn"
    assert report["apollo_fixed_scene_compat_status"] == "pass"
    assert binding["scenario_case"] == "baguang_follow_stop_static_300m"
    assert binding["role_aliases"] == {"lead_vehicle": "front"}
    assert manifest["backend_type"] == "apollo_reference_backend"
    assert manifest["scenario_case"] == "baguang_follow_stop_static_300m"
    assert manifest["target_actor_contract"]["role_aliases"] == {"lead_vehicle": "front"}
    assert compat["actor_roles"] == {"lead_vehicle": "212"}
    assert status["status"] == "success"
    assert status["scenario_case"] == "baguang_follow_stop_static_300m"


def _make_run_apollo_fixed_scene(run) -> None:
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        {
            "backend": "apollo_cyberrt",
            "backend_type": "apollo_reference_backend",
            "scenario_class": "follow_stop_static",
        }
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (run / "artifacts" / "control_apply_trace.jsonl").write_text(json.dumps({"control": {}}) + "\n", encoding="utf-8")
