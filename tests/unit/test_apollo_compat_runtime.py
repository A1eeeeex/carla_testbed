from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import yaml

from carla_testbed.config import load_config
from carla_testbed.runtime import apollo_compat
from tbio.backends.cyberrt import CyberRTBackend


def test_transition_backend_snapshots_prediction_log() -> None:
    log_files = CyberRTBackend._apollo_core_log_files()

    assert "prediction.INFO" in log_files


def test_transition_backend_snapshots_planning_bvar_dump(tmp_path: Path) -> None:
    app_root = tmp_path / "Apollo10.0" / "application-core"
    (app_root / ".aem").mkdir(parents=True)
    dumps = app_root / "dumps"
    dumps.mkdir()
    (dumps / "planning.data").write_text(
        "mainboard_planning_apollo_prediction_recv_msgs_nums : 7\n",
        encoding="utf-8",
    )
    artifacts = tmp_path / "run" / "artifacts"
    backend = CyberRTBackend(
        {
            "artifacts": {"dir": str(artifacts)},
            "algo": {"apollo": {"application_core_root": str(app_root)}},
        }
    )

    backend._snapshot_apollo_bvar_dumps(artifacts)

    assert (artifacts / "apollo_planning.data").read_text(encoding="utf-8").strip().endswith(": 7")
    meta = json.loads((artifacts / "apollo_bvar_dump_snapshot_meta.json").read_text(encoding="utf-8"))
    assert meta["files"]["planning.data"]["status"] == "ok"
    assert meta["files"]["planning.data"]["snapshot"].endswith("apollo_planning.data")


def _write_transition_config(tmp_path: Path, *, phase1_scenario_path: str | None = None) -> Path:
    config_path = tmp_path / "town01_apollo_route_only_claim_probe.yaml"
    config_path.write_text(
        "\n".join(
            [
                "run:",
                "  id: claim_runtime",
                "  max_ticks: 3",
                "  fixed_dt_s: 0.05",
                f"  output_root: {tmp_path.as_posix()}",
                "  claim_profile: true",
                "  profile_name: unit_claim_transition",
                "sim:",
                "  town: Town01",
                "scenario:",
                "  name: carla_town01_route_health",
                "  driver: carla_town01_route_health",
                "  route_health:",
                "    route_id: town01_rh_spawn097_goal046",
                "backend:",
                "  name: apollo_cyberrt",
                "algo:",
                "  stack: apollo",
                "  apollo:",
                "    transport_mode: ros2_gt",
                "io:",
                "  mode: ros2_native",
                *(
                    [
                        "recording:",
                        "  artifacts:",
                        f"    phase1_scenario_path: {phase1_scenario_path}",
                    ]
                    if phase1_scenario_path
                    else []
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def test_metadata_from_config_preserves_explicit_legacy_run_scenario_class(tmp_path: Path) -> None:
    config_path = tmp_path / "baguang_dynamic_sidecar.yaml"
    config_path.write_text(
        "\n".join(
            [
                "run:",
                "  id: dynamic_sidecar",
                "  max_ticks: 3",
                "  fixed_dt_s: 0.05",
                f"  output_root: {tmp_path.as_posix()}",
                "sim:",
                "  town: straight_road_for_baguang",
                "scenario:",
                "  name: carla_followstop",
                "backend:",
                "  name: apollo_cyberrt",
                "  params:",
                "    legacy_run:",
                "      profile_name: phase1_dynamic_sidecar",
                "      scenario_id: baguang_lead_decel_70_to_40_20m",
                "      scenario_class: lead_vehicle_decel",
                "      route_id: straight_road_for_baguang_mainline_lead_decel_20m",
                "      capability_profile: phase1_fixed_scene_sidecar",
                "",
            ]
        ),
        encoding="utf-8",
    )
    cfg = load_config(config_path)

    metadata = apollo_compat._metadata_from_config(
        cfg,
        config_path=config_path,
        run_dir=tmp_path / "dynamic_sidecar_run",
    )

    assert metadata["scenario_id"] == "baguang_lead_decel_70_to_40_20m"
    assert metadata["scenario_class"] == "lead_vehicle_decel"
    assert metadata["route_id"] == "straight_road_for_baguang_mainline_lead_decel_20m"


def test_typed_transition_backend_uses_resolved_config_and_preserves_reports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_transition_config(tmp_path)
    cfg = load_config(config_path)
    run_dir = tmp_path / "claim_transition"
    invoked: dict[str, str] = {}

    def fake_invoke(effective_path: Path, root: Path) -> int:
        invoked["effective_path"] = str(effective_path)
        payload = yaml.safe_load(effective_path.read_text(encoding="utf-8"))
        assert payload["typed_runtime"]["runtime_dispatch_kind"] == "typed_apollo_claim_runtime"
        assert payload["typed_runtime"]["legacy_fallback_used"] is False
        assert payload["scenario"]["driver"] == "carla_town01_route_health"
        assert payload["run"]["ticks"] == 3
        assert payload["run"]["claim_profile"] is True
        assert payload["run"]["materialization_probe"] is False
        report_path = root / "analysis" / "routing_response_decoded" / "routing_response_decoded_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "schema_version": "routing_response_decoded.v1",
                    "status": "pass",
                    "lane_segment_count": 1,
                    "lane_segments": [{"lane_id": "lane_1", "length_m": 42.0}],
                    "total_length_m": 42.0,
                }
            ),
            encoding="utf-8",
        )
        (root / "summary.json").write_text(
            json.dumps({"success": True, "exit_reason": "fake_transition_ok", "frames": 1}),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(apollo_compat, "_invoke_tbio_transition_backend", fake_invoke)

    result = apollo_compat.run_compat_apollo_cyber_gt_runtime(
        cfg,
        config_path=config_path,
        run_dir=run_dir,
        resolved_config=dataclasses.asdict(cfg),
    )

    assert result.exit_code == 0
    assert "typed_apollo_claim_runtime" in result.message
    assert Path(invoked["effective_path"]).name == "typed_runtime.effective_legacy.yaml"
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    boundary = json.loads(
        (run_dir / "analysis" / "runtime_claim_boundary" / "runtime_claim_boundary_report.json").read_text(
            encoding="utf-8"
        )
    )
    routing = json.loads(
        (
            run_dir
            / "analysis"
            / "routing_response_decoded"
            / "routing_response_decoded_report.json"
        ).read_text(encoding="utf-8")
    )
    assert manifest["runtime_dispatch_kind"] == "typed_apollo_claim_runtime"
    assert manifest["legacy_fallback_used"] is False
    assert manifest["compat_layers"] == ["ros2_gt_transition", "legacy_route_health_transition"]
    assert manifest["transport_mode"] == "apollo_cyberrt_gt_over_ros2_transition"
    assert manifest["typed_runtime_effective_config_path"] == "typed_runtime.effective_legacy.yaml"
    assert summary["runtime_dispatch_kind"] == "typed_apollo_claim_runtime"
    assert summary["can_claim_unassisted_natural_driving"] is False
    assert boundary["status"] == "pass"
    assert routing["status"] == "pass"
    assert routing["lane_segment_count"] == 1


def test_transition_events_append_without_erasing_backend_events(tmp_path: Path) -> None:
    run_dir = tmp_path / "claim_transition"
    run_dir.mkdir()
    events_path = run_dir / "events.jsonl"
    backend_event = {
        "event_type": "lane_invasion",
        "crossed_lane_marking_count": 1,
        "crossed_lane_marking_types": ["Solid"],
    }
    events_path.write_text(json.dumps(backend_event, sort_keys=True) + "\n", encoding="utf-8")

    apollo_compat._append_transition_events(
        run_dir,
        start_wall=1.0,
        end_wall=2.0,
        exit_code=0,
        error_text="",
        staged_effective_config_path=tmp_path / "typed_runtime.effective_legacy.yaml",
    )

    rows = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0] == backend_event
    assert [row["event_type"] for row in rows[1:]] == [
        "typed_apollo_claim_runtime_start",
        "typed_apollo_claim_runtime_end",
    ]


def test_transition_backend_syncs_valid_routing_response_into_report_and_jsonl(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_transition_config(tmp_path)
    cfg = load_config(config_path)
    run_dir = tmp_path / "claim_transition"

    def fake_invoke(_effective_path: Path, root: Path) -> int:
        artifacts = root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "routing_response_decoded.json").write_text(
            json.dumps(
                {
                    "schema_version": "routing_response_decoded.v1",
                    "status": "pass",
                    "source": "/apollo/raw_routing_response",
                    "lane_segment_count": 1,
                    "lane_segments": [{"lane_id": "lane_1", "length_m": 42.0}],
                    "total_length_m": 42.0,
                }
            ),
            encoding="utf-8",
        )
        (artifacts / "routing_response_decoded.jsonl").write_text(
            json.dumps(
                {
                    "status": "insufficient_data",
                    "message_count": 0,
                    "source": "/apollo/routing_response",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        report_path = root / "analysis" / "routing_response_decoded" / "routing_response_decoded_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps({"schema_version": "routing_response_decoded.v1", "status": "insufficient_data"}),
            encoding="utf-8",
        )
        (root / "summary.json").write_text(
            json.dumps(
                {
                    "success": True,
                    "exit_reason": "fake_transition_ok",
                    "frames": 1,
                    "route_id": "town01_rh_spawn068_goal079",
                }
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(apollo_compat, "_invoke_tbio_transition_backend", fake_invoke)

    result = apollo_compat.run_compat_apollo_cyber_gt_runtime(
        cfg,
        config_path=config_path,
        run_dir=run_dir,
        resolved_config=dataclasses.asdict(cfg),
    )

    assert result.exit_code == 0
    routing_report = json.loads(
        (
            run_dir
            / "analysis"
            / "routing_response_decoded"
            / "routing_response_decoded_report.json"
        ).read_text(encoding="utf-8")
    )
    routing_jsonl_rows = [
        json.loads(line)
        for line in (run_dir / "artifacts" / "routing_response_decoded.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]
    assert routing_report["status"] == "pass"
    assert routing_report["lane_segment_count"] == 1
    assert routing_jsonl_rows[-1]["status"] == "pass"
    assert routing_jsonl_rows[-1]["lane_segment_count"] == 1
    assert routing_jsonl_rows[-1]["lane_segments"][0]["lane_id"] == "lane_1"
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    route_health = json.loads((run_dir / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    assert manifest["route_id"] == "town01_rh_spawn068_goal079"
    assert summary["route_id"] == "town01_rh_spawn068_goal079"
    assert route_health["route_id"] == "town01_rh_spawn068_goal079"


def test_transition_backend_materializes_route_json_from_scenario_route_trace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = _write_transition_config(tmp_path)
    cfg = load_config(config_path)
    run_dir = tmp_path / "claim_transition"

    def fake_invoke(_effective_path: Path, root: Path) -> int:
        artifacts = root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "scenario_metadata.json").write_text(
            json.dumps(
                {
                    "route_id": "town01_rh_spawn097_goal046",
                    "map": "Town01",
                    "route_trace_source": "town01_forward_waypoint_trace",
                    "route_trace_length_m": 10.0,
                    "route_trace_length_source": "route_trace_s_span",
                    "claim_route_length_m": 10.0,
                    "claim_route_length_source": "route_trace_s_span",
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0, "yaw_deg": 0.0},
                    "goal": {"x": 10.0, "y": 0.0, "z": 0.0, "yaw_deg": 0.0},
                    "route_trace": [
                        {"index": 0, "x": 0.0, "y": 0.0, "z": 0.0, "s": 0.0, "heading": 0.0, "lane_id": "15:0:1"},
                        {"index": 1, "x": 5.0, "y": 0.0, "z": 0.0, "s": 5.0, "heading": 0.0, "lane_id": "15:0:1"},
                        {"index": 2, "x": 10.0, "y": 0.0, "z": 0.0, "s": 10.0, "heading": 0.0, "lane_id": "15:0:1"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        (root / "route.json").write_text(
            json.dumps(
                {
                    "schema_version": "route_stub.v1",
                    "route_id": "town01_rh_spawn097_goal046",
                    "status": "insufficient_data",
                    "points": [],
                }
            ),
            encoding="utf-8",
        )
        (root / "summary.json").write_text(
            json.dumps(
                {
                    "success": True,
                    "exit_reason": "fake_transition_ok",
                    "frames": 1,
                    "route_id": "town01_rh_spawn097_goal046",
                }
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(apollo_compat, "_invoke_tbio_transition_backend", fake_invoke)

    result = apollo_compat.run_compat_apollo_cyber_gt_runtime(
        cfg,
        config_path=config_path,
        run_dir=run_dir,
        resolved_config=dataclasses.asdict(cfg),
    )

    assert result.exit_code == 0
    route = json.loads((run_dir / "route.json").read_text(encoding="utf-8"))
    assert route["schema_version"] == "runtime_route_trace.v1"
    assert route["route_id"] == "town01_rh_spawn097_goal046"
    assert route["map"] == "Town01"
    assert route["source"] == "artifacts/scenario_metadata.json:route_trace"
    assert route["coordinate_frame"] == "carla_world"
    assert route["target_projection_frame"] == "apollo_map"
    assert len(route["points"]) == 3
    assert route["points"][0]["lane_id"] == "15:0:1"
    assert route["metadata"]["claim_route_length_m"] == 10.0
    assert "Apollo natural-driving claims still require" in route["claim_boundary"]


def test_transition_backend_runs_phase1_postprocess_when_scenario_path_declared(
    tmp_path: Path,
    monkeypatch,
) -> None:
    scenario_path = "configs/scenarios/baguang/follow_stop_static_300m.yaml"
    config_path = _write_transition_config(tmp_path, phase1_scenario_path=scenario_path)
    cfg = load_config(config_path)
    run_dir = tmp_path / "phase1_claim_transition"

    def fake_invoke(_effective_path: Path, root: Path) -> int:
        artifacts = root / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "run_id": "phase1_claim_transition",
                    "backend": "apollo_cyberrt",
                    "metadata": {"scenario_metadata": {"front_role": "front"}},
                }
            ),
            encoding="utf-8",
        )
        (root / "summary.json").write_text(
            json.dumps(
                {
                    "success": True,
                    "exit_reason": "fake_transition_ok",
                    "frames": 2,
                    "metadata": {"scenario_metadata": {"front_role": "front"}},
                }
            ),
            encoding="utf-8",
        )
        (root / "events.jsonl").write_text(json.dumps({"event": "run_finished"}) + "\n", encoding="utf-8")
        (root / "timeseries.csv").write_text(
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
                        "mode": "static_hold",
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
        rows = [
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
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(apollo_compat, "_invoke_tbio_transition_backend", fake_invoke)

    result = apollo_compat.run_compat_apollo_cyber_gt_runtime(
        cfg,
        config_path=config_path,
        run_dir=run_dir,
        resolved_config=dataclasses.asdict(cfg),
    )

    assert result.exit_code == 0
    assert "phase1_postprocess" in result.outputs
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    binding = json.loads(
        (
            run_dir
            / "analysis"
            / "phase1_scenario_binding"
            / "phase1_scenario_binding_report.json"
        ).read_text(encoding="utf-8")
    )
    compat = json.loads(
        (
            run_dir
            / "analysis"
            / "phase1_apollo_fixed_scene_compat"
            / "phase1_apollo_fixed_scene_compat_report.json"
        ).read_text(encoding="utf-8")
    )
    postprocess = json.loads(
        (
            run_dir
            / "analysis"
            / "phase1_postprocess"
            / "phase1_postprocess_report.json"
        ).read_text(encoding="utf-8")
    )
    assert manifest["scenario_case"] == "baguang_follow_stop_static_300m"
    assert manifest["target_actor_contract"]["role_aliases"] == {"lead_vehicle": "front"}
    assert binding["status"] == "pass"
    assert compat["status"] == "pass"
    assert compat["actor_roles"] == {"lead_vehicle": "212"}
    assert postprocess["phase1_scenario_binding_status"] == "pass"
    assert postprocess["apollo_fixed_scene_compat_status"] == "pass"


def test_phase1_auto_hdmap_projection_export_writes_status_and_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "config.resolved.yaml").write_text(
        "\n".join(
            [
                "runtime:",
                "  postprocess:",
                "    phase1_scenario_path: configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml",
                "    auto_export_apollo_hdmap_projection: true",
            ]
        ),
        encoding="utf-8",
    )

    def fake_export(**kwargs):
        out_path = Path(kwargs["out_path"])
        rows = []
        for index in range(60):
            rows.append(
                {
                    "timestamp": float(index),
                    "localization_x": float(index),
                    "localization_y": 0.0,
                    "localization_heading": 0.0,
                    "nearest_lane_id": "lane_1",
                    "projection_s": float(index) * 10.0,
                    "projection_l": 0.01,
                    "lane_heading_at_s": 0.0,
                    "heading_error_rad": 0.001,
                    "lateral_error_m": 0.01,
                    "road_id": None,
                    "junction_id": None,
                    "source": "apollo_hdmap_api",
                    "map_name": "straight_road_for_baguang",
                    "map_dir": "/apollo/modules/map/data/straight_road_for_baguang",
                    "status": "ok",
                    "sample_type": "route",
                    "route_s": float(index) * 10.0,
                }
            )
        out_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return {
            "schema_version": "apollo_hdmap_projection_export.v1",
            "status": "pass",
            "row_count": len(rows),
            "ok_row_count": len(rows),
            "out_path": str(out_path),
        }

    import carla_testbed.analysis.apollo_hdmap_projection_export as projection_export

    monkeypatch.setattr(projection_export, "export_apollo_hdmap_projection_jsonl", fake_export)

    outputs = apollo_compat._maybe_export_apollo_hdmap_projection(run_dir)

    status_path = Path(outputs["apollo_hdmap_projection_auto_export"])
    status = json.loads(status_path.read_text(encoding="utf-8"))
    projection = json.loads(
        (
            run_dir
            / "analysis"
            / "apollo_hdmap_projection"
            / "apollo_hdmap_projection_report.json"
        ).read_text(encoding="utf-8")
    )
    assert status["status"] == "pass"
    assert status["analysis"]["status"] == "pass"
    assert projection["status"] == "pass"
    assert projection["claim_grade"] is True


def test_phase1_auto_hdmap_projection_export_skips_existing_non_empty_artifact(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "apollo_hdmap_projection.jsonl").write_text('{"status":"ok"}\n', encoding="utf-8")
    (run_dir / "config.resolved.yaml").write_text(
        "\n".join(
            [
                "runtime:",
                "  postprocess:",
                "    auto_export_apollo_hdmap_projection: true",
            ]
        ),
        encoding="utf-8",
    )

    outputs = apollo_compat._maybe_export_apollo_hdmap_projection(run_dir)

    status = json.loads(Path(outputs["apollo_hdmap_projection_auto_export"]).read_text(encoding="utf-8"))
    assert status["status"] == "skipped"
    assert status["reason"] == "existing_non_empty_projection_artifact"


def test_transition_python_resolver_does_not_use_unexpanded_placeholder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_python = tmp_path / "carla16" / "bin" / "python"
    fake_python.parent.mkdir(parents=True)
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    effective = tmp_path / "typed_runtime.effective_legacy.yaml"
    effective.write_text(
        "\n".join(
            [
                "algo:",
                "  apollo:",
                "    docker:",
                "      host_python_exec: ${CARLA16_PYTHON}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("CARLA16_PYTHON", raising=False)
    monkeypatch.setattr(
        apollo_compat,
        "_is_executable_file",
        lambda candidate: str(candidate) == fake_python.as_posix(),
    )
    monkeypatch.setattr(
        apollo_compat,
        "_expand_runtime_python_candidate",
        lambda raw: fake_python.as_posix()
        if str(raw or "") == "/home/ubuntu/miniconda3/envs/carla16/bin/python"
        else "",
    )

    resolved = apollo_compat._resolve_transition_python(effective)

    assert resolved == fake_python.as_posix()
    assert "${CARLA16_PYTHON}" not in resolved


def test_transition_backend_invocation_does_not_prewrite_target_run_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    effective = tmp_path / "staging" / "typed_runtime.effective_legacy.yaml"
    effective.parent.mkdir(parents=True)
    effective.write_text(
        "\n".join(
            [
                "run:",
                "  map: Town01",
                "runtime:",
                "  carla:",
                "    start: false",
                "",
            ]
        ),
        encoding="utf-8",
    )
    run_dir = tmp_path / "online_run"
    captured: dict[str, list[str]] = {}

    class _Completed:
        returncode = 0

    def fake_run(command, **_kwargs):
        captured["command"] = list(command)
        assert not run_dir.exists()
        return _Completed()

    monkeypatch.setattr(apollo_compat, "_resolve_transition_python", lambda _path: fake_python.as_posix())
    monkeypatch.setattr(apollo_compat.subprocess, "run", fake_run)

    rc = apollo_compat._invoke_tbio_transition_backend(effective, run_dir)

    assert rc == 0
    assert not run_dir.exists()
    assert "examples.run_followstop" in captured["command"]
    assert "tbio.scripts.run" not in captured["command"]
    runtime = yaml.safe_load((effective.parent / "typed_transition_runtime.json").read_text(encoding="utf-8"))
    assert runtime["transition_entrypoint"] == "examples.run_followstop"
