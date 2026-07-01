from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.phase1_artifact_normalization import normalize_phase1_artifacts


def test_promotes_unique_nested_timeseries_to_phase1_root(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested.mkdir(parents=True)
    (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,1\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert report["status"] == "promoted"
    assert (run / "timeseries.csv").read_text(encoding="utf-8") == "sim_time,ego_speed_mps\n0,1\n"
    assert report["promoted_artifacts"] == [
        {"name": "timeseries", "source": str(nested / "timeseries.csv"), "destination": str(run / "timeseries.csv")}
    ]
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]
    written = json.loads(
        (run / "analysis" / "phase1_artifact_normalization" / "phase1_artifact_normalization_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert written["status"] == "promoted"


def test_existing_root_timeseries_is_preserved(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,root\n", encoding="utf-8")
    (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,nested\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert report["status"] == "already_present"
    assert (run / "timeseries.csv").read_text(encoding="utf-8") == "sim_time,ego_speed_mps\n0,root\n"
    assert report["promoted_artifacts"] == []


def test_sparse_root_timeseries_control_fields_are_overlaid_from_control_trace(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested_artifacts = nested / "artifacts"
    nested_artifacts.mkdir(parents=True)
    (nested / "timeseries.csv").write_text(
        "sim_time,apollo_steer_raw,bridge_steer_mapped,carla_steer_applied,ego_speed\n"
        "0.00,0.0,,0.0,0.0\n"
        "0.05,0.0,,0.0,1.0\n",
        encoding="utf-8",
    )
    (nested_artifacts / "control_apply_trace.jsonl").write_text(
        json.dumps(
            {
                "schema_version": "control_apply_trace.v1",
                "timestamp": 0.05,
                "apollo_raw": {"steer": 0.2, "throttle": 0.3, "brake": 0.0},
                "bridge_mapped": {"steer": 0.05, "mapped_carla_steer_cmd": 0.05},
                "carla_applied": {"steer": 0.05},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = normalize_phase1_artifacts(run)

    rows = (run / "timeseries.csv").read_text(encoding="utf-8").splitlines()
    header = rows[0].split(",")
    values = dict(zip(header, rows[2].split(",")))
    assert values["apollo_steer_raw"] == "0.2"
    assert values["bridge_steer_mapped"] == "0.05"
    assert values["carla_steer_applied"] == "0.05"
    assert "throttle_raw" in header
    assert any(item["name"] == "timeseries_control_trace_overlay" for item in report["promoted_artifacts"])
    assert "root_timeseries_control_fields_overlaid_from_control_apply_trace" in report["warnings"]
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]


def test_config_resolved_ambiguous_candidates_select_non_hidden_runtime_config(tmp_path: Path) -> None:
    run = tmp_path / "run"
    hidden = run / "legacy" / ".typed_runtime"
    visible = run / "legacy" / "actual"
    hidden.mkdir(parents=True)
    visible.mkdir(parents=True)
    (visible / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (hidden / "config.resolved.yaml").write_text("source: hidden\n", encoding="utf-8")
    (visible / "config.resolved.yaml").write_text("source: visible\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert (run / "config.resolved.yaml").read_text(encoding="utf-8") == "source: visible\n"
    assert any(item["name"] == "config_resolved" for item in report["promoted_artifacts"])
    assert "multiple_nested_config_resolved_candidates_selected_non_hidden" in report["warnings"]


def test_promotes_unique_nested_route_json_for_static_projection_exports(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    route_payload = {
        "schema_version": "runtime_route_trace.v1",
        "coordinate_frame": "carla_world",
        "points": [
            {"x": 1.0, "y": 2.0, "z": 0.0, "yaw_deg": -90.0},
            {"x": 1.0, "y": 1.0, "z": 0.0, "yaw_deg": -90.0},
        ],
    }
    (nested / "route.json").write_text(json.dumps(route_payload) + "\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    promoted_names = {item["name"] for item in report["promoted_artifacts"]}
    assert "route_json" in promoted_names
    promoted = json.loads((run / "route.json").read_text(encoding="utf-8"))
    assert promoted["schema_version"] == "runtime_route_trace.v1"
    assert promoted["coordinate_frame"] == "carla_world"
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]


def test_promotes_unique_nested_route_health_reports_without_rewriting_status(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested_route_health = run / "legacy" / "actual" / "analysis" / "route_health"
    nested_route_health.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (nested_route_health / "route_health.json").write_text(
        json.dumps(
            {
                "schema_version": "route_health_report.v1",
                "status": "fail",
                "route_source": "configured_route_file",
                "hard_gate_eligible": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (nested_route_health / "route_health.csv").write_text("route_s,lateral_error\n0,0.0\n", encoding="utf-8")
    (nested_route_health / "curve_segments.csv").write_text("start_s,end_s\n", encoding="utf-8")
    (nested_route_health / "route_health_summary.md").write_text("# Route Health\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    promoted_names = {item["name"] for item in report["promoted_artifacts"]}
    assert {
        "route_health_json",
        "route_health_csv",
        "route_health_curve_segments",
        "route_health_summary",
    }.issubset(promoted_names)
    promoted = json.loads((run / "analysis" / "route_health" / "route_health.json").read_text(encoding="utf-8"))
    assert promoted["status"] == "fail"
    assert promoted["hard_gate_eligible"] is True
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]


def test_promotes_unique_nested_reference_line_contract_without_rewriting_status(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested_reference = run / "legacy" / "actual" / "analysis" / "apollo_reference_line_contract"
    nested_reference.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (nested_reference / "apollo_reference_line_contract_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_reference_line_contract.v1",
                "status": "insufficient_data",
                "blocking_reasons": ["apollo_reference_line_runtime_evidence_missing"],
                "warnings": ["compat_runtime_did_not_observe_planning_reference_line"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (nested_reference / "apollo_reference_line_contract_summary.md").write_text(
        "# Apollo Reference Line Contract\n",
        encoding="utf-8",
    )

    report = normalize_phase1_artifacts(run)

    promoted_names = {item["name"] for item in report["promoted_artifacts"]}
    assert {
        "apollo_reference_line_contract_report",
        "apollo_reference_line_contract_summary",
    }.issubset(promoted_names)
    promoted = json.loads(
        (
            run
            / "analysis"
            / "apollo_reference_line_contract"
            / "apollo_reference_line_contract_report.json"
        ).read_text(encoding="utf-8")
    )
    assert promoted["status"] == "insufficient_data"
    assert promoted["blocking_reasons"] == ["apollo_reference_line_runtime_evidence_missing"]
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]


def test_promotes_unique_nested_apollo_hdmap_projection_without_rewriting_status(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested_artifacts = nested / "artifacts"
    nested_projection = nested / "analysis" / "apollo_hdmap_projection"
    nested_artifacts.mkdir(parents=True)
    nested_projection.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (nested_artifacts / "apollo_hdmap_projection.jsonl").write_text("", encoding="utf-8")
    (nested_projection / "apollo_hdmap_projection_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_hdmap_projection_report.v1",
                "status": "insufficient_data",
                "claim_grade": False,
                "artifact_status": "artifact_empty",
                "warnings": ["apollo_hdmap_projection_empty"],
                "missing_fields": ["apollo_hdmap_projection_rows"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (nested_projection / "apollo_hdmap_projection_summary.md").write_text(
        "# Apollo HDMap Projection\n",
        encoding="utf-8",
    )

    report = normalize_phase1_artifacts(run)

    promoted_names = {item["name"] for item in report["promoted_artifacts"]}
    assert {
        "apollo_hdmap_projection_raw",
        "apollo_hdmap_projection_report",
        "apollo_hdmap_projection_summary",
    }.issubset(promoted_names)
    promoted = json.loads(
        (run / "analysis" / "apollo_hdmap_projection" / "apollo_hdmap_projection_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert promoted["status"] == "insufficient_data"
    assert promoted["artifact_status"] == "artifact_empty"
    assert (run / "artifacts" / "apollo_hdmap_projection.jsonl").is_file()
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]


def test_promotes_unique_nested_apollo_reference_line_raw_debug_inputs(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested_artifacts = nested / "artifacts"
    nested_planning = nested / "analysis" / "planning_materialization"
    nested_artifacts.mkdir(parents=True)
    nested_planning.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    raw_files = {
        "apollo_reference_line_contract.jsonl": {"planning_reference_available": True},
        "planning_topic_debug.jsonl": {"trajectory_point_count": 10},
        "planning_topic_debug_summary.json": {"total_messages_received": 1},
        "planning_route_segment_debug.jsonl": {"reference_line_provider_status": "ready"},
        "apollo_route_segment_debug.jsonl": {"lane_id": "15_1_1"},
        "apollo_planning.INFO": "I reference_line_provider ready\n",
        "control_trajectory_consume_debug.jsonl": {"planning_sequence_num": 1},
    }
    for name, payload in raw_files.items():
        path = nested_artifacts / name
        if isinstance(payload, str):
            path.write_text(payload, encoding="utf-8")
        elif name.endswith(".json"):
            path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        else:
            path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    (nested_planning / "planning_materialization_report.json").write_text(
        json.dumps({"schema_version": "planning_materialization.v1", "verdict": "warn"}) + "\n",
        encoding="utf-8",
    )

    report = normalize_phase1_artifacts(run)

    promoted_names = {item["name"] for item in report["promoted_artifacts"]}
    assert {
        "apollo_reference_line_contract_raw",
        "planning_topic_debug",
        "planning_topic_debug_summary",
        "planning_route_segment_debug",
        "apollo_route_segment_debug",
        "apollo_planning_info_log",
        "control_trajectory_consume_debug",
        "planning_materialization_report",
    }.issubset(promoted_names)
    assert (run / "artifacts" / "apollo_reference_line_contract.jsonl").is_file()
    assert (run / "artifacts" / "planning_topic_debug.jsonl").is_file()
    assert (run / "artifacts" / "planning_topic_debug_summary.json").is_file()
    assert (run / "artifacts" / "planning_route_segment_debug.jsonl").is_file()
    assert (run / "artifacts" / "apollo_route_segment_debug.jsonl").is_file()
    assert (run / "artifacts" / "apollo_planning.INFO").is_file()
    assert (run / "artifacts" / "control_trajectory_consume_debug.jsonl").is_file()
    assert (run / "analysis" / "planning_materialization" / "planning_materialization_report.json").is_file()
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]


def test_promotes_unique_nested_route_start_alignment_and_startup_geometry(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested_alignment = nested / "analysis" / "route_start_alignment"
    nested_artifacts = nested / "artifacts"
    nested_alignment.mkdir(parents=True)
    nested_artifacts.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (nested_alignment / "route_start_alignment_report.json").write_text(
        json.dumps(
            {
                "schema_version": "route_start_alignment_report.v1",
                "status": "warn",
                "reason": "spawn_lateral_offset_high",
                "startup_geometry_alignment": {
                    "localization_to_final_start_distance_m": 0.51,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (nested_alignment / "route_start_alignment_summary.md").write_text("# Route Start\n", encoding="utf-8")
    (nested_artifacts / "startup_geometry_summary.json").write_text(
        json.dumps({"summary_status": "provisional", "record_count": 1}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "publish_gap_trace.jsonl").write_text(
        json.dumps({"schema_version": "publish_gap_trace.v1", "sim_time_sec": 1.0}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "topic_publish_stats.jsonl").write_text(
        json.dumps({"schema_version": "topic_publish_stats.v1", "topic": "/apollo/localization/pose"}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "carla_tick_health.jsonl").write_text(
        json.dumps({"schema_version": "carla_tick_health.v1", "frame": 1}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "carla_tick_health_summary.json").write_text(
        json.dumps({"schema_version": "carla_tick_health_summary.v1", "status": "warn"}) + "\n",
        encoding="utf-8",
    )

    report = normalize_phase1_artifacts(run)

    promoted_names = {item["name"] for item in report["promoted_artifacts"]}
    assert {
        "route_start_alignment_json",
        "route_start_alignment_summary",
        "startup_geometry_summary",
        "publish_gap_trace",
        "topic_publish_stats",
        "carla_tick_health",
        "carla_tick_health_summary",
    }.issubset(promoted_names)
    promoted_alignment = json.loads(
        (run / "analysis" / "route_start_alignment" / "route_start_alignment_report.json").read_text(
            encoding="utf-8"
        )
    )
    promoted_startup = json.loads((run / "artifacts" / "startup_geometry_summary.json").read_text(encoding="utf-8"))
    assert promoted_alignment["reason"] == "spawn_lateral_offset_high"
    assert promoted_startup["summary_status"] == "provisional"
    assert (run / "artifacts" / "publish_gap_trace.jsonl").is_file()
    assert (run / "artifacts" / "topic_publish_stats.jsonl").is_file()
    assert (run / "artifacts" / "carla_tick_health.jsonl").is_file()
    assert (run / "artifacts" / "carla_tick_health_summary.json").is_file()


def test_ambiguous_nested_timeseries_is_not_promoted(tmp_path: Path) -> None:
    run = tmp_path / "run"
    first = run / "legacy_a" / "actual"
    second = run / "legacy_b" / "actual"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (second / "timeseries.csv").write_text("sim_time\n1\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert report["status"] == "ambiguous"
    assert not (run / "timeseries.csv").exists()
    assert "multiple_nested_timeseries_candidates" in report["warnings"]


def test_file_path_is_reported_as_error_without_crashing(tmp_path: Path) -> None:
    path = tmp_path / "LATEST.txt"
    path.write_text("run\n", encoding="utf-8")

    report = normalize_phase1_artifacts(path)

    assert report["status"] == "error"
    assert "run_dir_is_not_directory" in report["warnings"]


def test_route_only_phase1_comparison_artifacts_can_pass_after_promotion(tmp_path: Path) -> None:
    from carla_testbed.analysis.phase1_artifact_normalization import ensure_phase1_comparison_artifacts

    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested_artifacts = nested / "artifacts"
    nested_artifacts.mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "route_only",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "platform_timeout"}),
        encoding="utf-8",
    )
    status_dir = run / "analysis" / "phase1_status"
    status_dir.mkdir(parents=True)
    (status_dir / "phase1_status.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_status.v1",
                "status": "failed",
                "failure_reason": "timeout",
                "run_evaluable": True,
                "target_actor_contract": {"status": "not_required", "required": False},
            }
        ),
        encoding="utf-8",
    )
    (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,1\n", encoding="utf-8")
    (nested / "events.jsonl").write_text(json.dumps({"event": "run_end"}) + "\n", encoding="utf-8")
    (nested_artifacts / "control_apply_trace.jsonl").write_text(
        json.dumps({"sim_time": 0.0, "steer": 0.0}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "bridge_health_summary.json").write_text(
        json.dumps({"control_apply_path": "ros2_control_bridge"}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps({"control_tx_count": 3}) + "\n",
        encoding="utf-8",
    )
    (nested_artifacts / "control_handoff_summary.json").write_text(
        json.dumps({"status": "pass"}) + "\n",
        encoding="utf-8",
    )

    normalize_phase1_artifacts(run)
    surface = ensure_phase1_comparison_artifacts(run)

    assert surface["status"] == "pass"
    assert surface["artifact_complete"] is True
    assert surface["v_t_gap_status"] == "not_applicable"
    assert (run / "events.jsonl").exists()
    assert (run / "artifacts" / "control_apply_trace.jsonl").exists()
    assert (run / "artifacts" / "bridge_health_summary.json").exists()
    assert (run / "artifacts" / "cyber_bridge_stats.json").exists()
    assert (run / "artifacts" / "control_handoff_summary.json").exists()
    assert (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").exists()
    legacy = json.loads((run / "analysis" / "phase1_status" / "artifact_completeness.json").read_text())
    assert legacy["schema_version"] == "phase1_artifact_completeness.v1"
    assert legacy["status"] == "pass"
