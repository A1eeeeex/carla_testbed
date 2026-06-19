from __future__ import annotations

import csv
import json

import carla_testbed.analysis.phase1_postprocess as phase1_postprocess
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
                "type": "VEHICLE",
                "frame_transform_checked": True,
                "position_frame_apollo_map": True,
                "theta_frame_checked": True,
                "tracking_time": 0.0,
                "velocity_source": "carla_actor_state",
                "vx": 0.0,
                "vy": 0.0,
                "vz": 0.0,
                "dynamic": False,
                "actually_stationary": True,
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
        "type": "VEHICLE",
        "frame_transform_checked": True,
        "position_frame_apollo_map": True,
        "theta_frame_checked": True,
        "velocity_source": "carla_actor_state",
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "dynamic": False,
        "actually_stationary": True,
    }
    obstacle_rows = [
        {
            **base_obstacle,
            "timestamp": 0.0,
            "front_obstacle_actor_x": 300.0,
            "front_obstacle_gap_lon_m": 300.0,
            "front_obstacle_gap_distance_m": 300.0,
            "tracking_time": 0.0,
        },
        {
            **base_obstacle,
            "timestamp": 10.0,
            "front_obstacle_actor_x": 300.0,
            "front_obstacle_gap_lon_m": 50.0,
            "front_obstacle_gap_distance_m": 50.0,
            "tracking_time": 10.0,
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


def test_phase1_postprocess_refreshes_apollo_control_reports_before_status(tmp_path, monkeypatch) -> None:
    run = _write_complete_run(tmp_path)
    _make_run_apollo_fixed_scene(run)
    handoff_dir = run / "analysis" / "apollo_control_handoff"
    health_dir = run / "analysis" / "control_health"
    handoff_dir.mkdir(parents=True)
    health_dir.mkdir(parents=True)
    (handoff_dir / "apollo_control_handoff_report.json").write_text(
        json.dumps({"schema_version": "apollo_control_handoff.v1", "status": "insufficient_data"}),
        encoding="utf-8",
    )
    (health_dir / "control_health_report.json").write_text(
        json.dumps({"schema_version": "control_health_report.v1", "status": "insufficient_data"}),
        encoding="utf-8",
    )
    calls: list[str] = []

    def fake_handoff(*, run_dir, out_dir):
        calls.append("handoff")
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "apollo_control_handoff_report.json"
        report_path.write_text(
            json.dumps({"schema_version": "apollo_control_handoff.v1", "status": "warn", "verdict": "warn"}),
            encoding="utf-8",
        )
        return {
            "apollo_control_handoff_report": str(report_path),
            "apollo_control_handoff_summary": str(out_dir / "apollo_control_handoff_summary.md"),
        }

    def fake_analyze_control_health(run_dir):
        calls.append("control_health")
        handoff = json.loads(
            (run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json").read_text(
                encoding="utf-8"
            )
        )
        assert handoff["status"] == "warn"
        return {"schema_version": "control_health_report.v1", "status": "warn", "failure_reason": None}

    def fake_write_control_health(report, out_dir):
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "control_health_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return {"control_health_report": str(report_path), "control_health_summary": str(out_dir / "summary.md")}

    monkeypatch.setattr(phase1_postprocess, "analyze_and_write_apollo_control_handoff", fake_handoff)
    monkeypatch.setattr(phase1_postprocess, "analyze_control_health_run_dir", fake_analyze_control_health)
    monkeypatch.setattr(phase1_postprocess, "write_control_health_report", fake_write_control_health)

    report = run_phase1_postprocess(run)
    handoff = json.loads((handoff_dir / "apollo_control_handoff_report.json").read_text(encoding="utf-8"))
    control_health = json.loads((health_dir / "control_health_report.json").read_text(encoding="utf-8"))

    assert calls == ["handoff", "control_health"]
    assert report["apollo_control_handoff_status"] == "warn"
    assert report["control_health_status"] == "warn"
    assert handoff["status"] == "warn"
    assert control_health["status"] == "warn"


def test_phase1_postprocess_refreshes_projection_and_reference_before_link_health(tmp_path, monkeypatch) -> None:
    run = _write_complete_run(tmp_path)
    _make_run_apollo_fixed_scene(run)
    projection_dir = run / "analysis" / "apollo_hdmap_projection"
    route_dir = run / "analysis" / "apollo_route_contract"
    reference_dir = run / "analysis" / "apollo_reference_line_contract"
    module_dir = run / "analysis" / "apollo_module_consumption"
    projection_dir.mkdir(parents=True)
    route_dir.mkdir(parents=True)
    reference_dir.mkdir(parents=True)
    module_dir.mkdir(parents=True)
    (projection_dir / "apollo_hdmap_projection_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_hdmap_projection_report.v1",
                "status": "insufficient_data",
                "claim_grade": False,
                "artifact_status": "artifact_empty",
            }
        ),
        encoding="utf-8",
    )
    (route_dir / "apollo_route_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_route_contract.v1",
                "status": "insufficient_data",
                "warnings": ["stale_route_contract"],
            }
        ),
        encoding="utf-8",
    )
    (reference_dir / "apollo_reference_line_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_reference_line_contract.v1",
                "status": "insufficient_data",
                "warnings": ["apollo_hdmap_projection_missing"],
            }
        ),
        encoding="utf-8",
    )
    (module_dir / "apollo_module_consumption_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_module_consumption.v1",
                "status": "fail",
                "blocking_reasons": [
                    "claim_route_consumption_unverified",
                    "route_contract_unverified_before_module_consumption_claim",
                ],
            }
        ),
        encoding="utf-8",
    )
    projection_rows = []
    for idx in range(60):
        projection_rows.append(
            {
                "timestamp": float(idx),
                "sample_type": "ego",
                "source": "apollo_hdmap_api",
                "status": "ok",
                "map_name": "straight_road_for_baguang",
                "map_dir": "/apollo/modules/map/data/straight_road_for_baguang",
                "nearest_lane_id": "lane_1",
                "projection_s": float(idx),
                "projection_l": 0.01,
                "heading_error_rad": 0.001,
                "lateral_error_m": 0.01,
                "expected_duration_s": 60.0,
                "expected_route_distance_m": 60.0,
            }
        )
    (run / "artifacts" / "apollo_hdmap_projection.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in projection_rows),
        encoding="utf-8",
    )
    (run / "artifacts" / "apollo_reference_line_contract.jsonl").write_text(
        json.dumps({"timestamp": 0.0, "planning_reference_available": True}) + "\n",
        encoding="utf-8",
    )
    (run / "artifacts" / "planning_topic_debug_summary.json").write_text(
        json.dumps({"total_messages_received": 10, "messages_with_nonzero_trajectory_points": 9}),
        encoding="utf-8",
    )

    def fake_route_contract(run_dir, **kwargs):
        assert str(kwargs.get("frame_transform", "")).endswith("configs/town01/apollo_frame_transform.example.yaml")
        refreshed_projection = json.loads(
            (run_dir / "analysis" / "apollo_hdmap_projection" / "apollo_hdmap_projection_report.json").read_text(
                encoding="utf-8"
            )
        )
        assert refreshed_projection["status"] == "pass"
        return {
            "schema_version": "apollo_route_contract.v1",
            "status": "warn",
            "blocking_reasons": [],
            "warnings": ["apollo_routing_goal_snap_distance_high"],
        }

    def fake_write_route_contract(report, out_dir):
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "apollo_route_contract_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return {
            "apollo_route_contract_report": str(report_path),
            "apollo_route_contract_summary": str(out_dir / "apollo_route_contract_summary.md"),
            "route_definition_claim": str(out_dir / "route_definition_claim.json"),
        }

    def fake_reference(run_dir):
        refreshed_projection = json.loads(
            (run_dir / "analysis" / "apollo_hdmap_projection" / "apollo_hdmap_projection_report.json").read_text(
                encoding="utf-8"
            )
        )
        assert refreshed_projection["status"] == "pass"
        refreshed_route = json.loads(
            (
                run_dir
                / "analysis"
                / "apollo_route_contract"
                / "apollo_route_contract_report.json"
            ).read_text(encoding="utf-8")
        )
        assert refreshed_route["status"] == "warn"
        assert "stale_route_contract" not in refreshed_route.get("warnings", [])
        return {
            "schema_version": "apollo_reference_line_contract.v1",
            "status": "warn",
            "warnings": ["apollo_route_contract_warn_reference_line_claim_warning_propagated"],
            "evidence": {
                "apollo_hdmap_projection_available": True,
                "apollo_hdmap_projection_claim_grade": True,
            },
        }

    def fake_write_reference(report, out_dir):
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "apollo_reference_line_contract_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return {
            "apollo_reference_line_contract_report": str(report_path),
            "apollo_reference_line_contract_summary": str(out_dir / "apollo_reference_line_contract_summary.md"),
        }

    def fake_module_consumption(run_dir):
        refreshed_reference = json.loads(
            (
                run_dir
                / "analysis"
                / "apollo_reference_line_contract"
                / "apollo_reference_line_contract_report.json"
            ).read_text(encoding="utf-8")
        )
        assert refreshed_reference["status"] == "warn"
        return {
            "schema_version": "apollo_module_consumption.v1",
            "status": "warn",
            "blocking_reasons": [],
            "warnings": ["route_or_reference_line_failure_logs_transient"],
            "apollo_route_contract_status": "warn",
            "routing_response_consumed_by_planning": True,
        }

    def fake_write_module_consumption(report, out_dir):
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "apollo_module_consumption_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return {
            "apollo_module_consumption_report": str(report_path),
            "apollo_module_consumption_summary": str(out_dir / "apollo_module_consumption_summary.md"),
        }

    def fake_link_health(run_dir, out_dir):
        refreshed = json.loads(
            (run_dir / "analysis" / "apollo_hdmap_projection" / "apollo_hdmap_projection_report.json").read_text(
                encoding="utf-8"
            )
        )
        assert refreshed["status"] == "pass"
        assert refreshed["claim_grade"] is True
        reference = json.loads(
            (
                run_dir
                / "analysis"
                / "apollo_reference_line_contract"
                / "apollo_reference_line_contract_report.json"
            ).read_text(encoding="utf-8")
        )
        assert reference["status"] == "warn"
        assert reference["evidence"]["apollo_hdmap_projection_claim_grade"] is True
        module = json.loads(
            (
                run_dir / "analysis" / "apollo_module_consumption" / "apollo_module_consumption_report.json"
            ).read_text(encoding="utf-8")
        )
        assert module["status"] == "warn"
        assert module["blocking_reasons"] == []
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "apollo_link_health_report.json"
        report_path.write_text(
            json.dumps(
                {
                    "schema_version": "apollo_link_health.v1",
                    "primary_blocker": "planning_reference_line:insufficient_data",
                }
            ),
            encoding="utf-8",
        )
        return {"apollo_link_health_report": str(report_path), "apollo_link_health_summary": str(out_dir / "summary.md")}

    monkeypatch.setattr(phase1_postprocess, "analyze_apollo_route_contract_run_dir", fake_route_contract)
    monkeypatch.setattr(phase1_postprocess, "write_apollo_route_contract_report", fake_write_route_contract)
    monkeypatch.setattr(phase1_postprocess, "analyze_apollo_reference_line_contract_run_dir", fake_reference)
    monkeypatch.setattr(phase1_postprocess, "write_apollo_reference_line_contract_report", fake_write_reference)
    monkeypatch.setattr(phase1_postprocess, "analyze_apollo_module_consumption_run_dir", fake_module_consumption)
    monkeypatch.setattr(phase1_postprocess, "write_apollo_module_consumption_report", fake_write_module_consumption)
    monkeypatch.setattr(phase1_postprocess, "analyze_and_write_apollo_link_health", fake_link_health)

    report = run_phase1_postprocess(run)
    refreshed = json.loads((projection_dir / "apollo_hdmap_projection_report.json").read_text(encoding="utf-8"))
    refreshed_route = json.loads((route_dir / "apollo_route_contract_report.json").read_text(encoding="utf-8"))
    refreshed_reference = json.loads(
        (reference_dir / "apollo_reference_line_contract_report.json").read_text(encoding="utf-8")
    )
    refreshed_module = json.loads((module_dir / "apollo_module_consumption_report.json").read_text(encoding="utf-8"))

    assert report["apollo_hdmap_projection_status"] == "pass"
    assert report["apollo_route_contract_status"] == "warn"
    assert report["apollo_reference_line_contract_status"] == "warn"
    assert report["apollo_module_consumption_status"] == "warn"
    assert report["apollo_link_health_primary_blocker"] == "planning_reference_line:insufficient_data"
    assert refreshed["artifact_status"] == "projection_rows_present"
    assert refreshed["projection"]["official_row_count"] == 60
    assert refreshed_route["status"] == "warn"
    assert refreshed_reference["status"] == "warn"
    assert refreshed_module["blocking_reasons"] == []


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
