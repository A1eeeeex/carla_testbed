from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_link_health import (
    APOLLO_LINK_HEALTH_SCHEMA_VERSION,
    analyze_apollo_link_health_run_dir,
    apollo_link_health_summary_md,
    write_apollo_link_health_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "lane097",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "routing_materialized": True,
            "routing_success_count": 1,
            "planning_materialized": True,
            "control_handoff_status": "control_consuming_with_nonzero_planning",
            "metrics": {"planning_nonempty_ratio": 1.0},
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "lane097",
            "backend": "apollo_cyberrt",
            "control_source": "/apollo/control",
            "transport_mode": "ros2_gt",
            "runtime_contract": {"status": "aligned"},
            "carla_world": {
                "configured_town": "Town01",
                "loaded_map_name": "Town01",
                "matches_configured_town": True,
                "spawn_point_count": 255,
            },
            "assist_ledger": {
                "schema_version": "assist_ledger.v1",
                "active_assists": [],
                "blocking_assists": [],
                "non_blocking_assists": [],
                "assist_sources": {},
                "assist_confidence": "explicit",
                "source_artifact": "manifest",
                "can_claim_unassisted_natural_driving": True,
            },
        },
    )
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_success_count": 1,
            "control_rx_count": 100,
            "control_tx_count": 100,
            "apply_control_count": 100,
        },
    )
    _write_json(
        run_dir / "artifacts/bridge_health_summary.json",
        {"status": "pass", "bridge_runtime_import_ok": True},
    )
    _write_json(
        run_dir / "artifacts/bridge_transport_summary.json",
        {"status": "pass", "transport_mode": "ros2_gt"},
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "status": "pass",
            "total_messages_received": 100,
            "messages_with_nonzero_trajectory_points": 100,
        },
    )
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "pass",
            "missing_required_channels": [],
            "low_rate_channels": [],
            "timestamp_failures": [],
        },
    )
    _write_json(
        run_dir / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "verdict": {"status": "pass", "blocking_reasons": []},
            "channel": {"status": "pass", "header_frame_id": "map"},
            "pose_consistency": {
                "heading_error_to_route_p95_rad": 0.01,
                "heading_error_to_lane_p95_rad": 0.02,
            },
            "reference_point": {
                "position_uses_vrp": True,
                "vehicle_reference_hard_gate_eligible": True,
            },
            "time": {"measurement_header_delta_ms_p95": 0.0},
            "apollo_hdmap_projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "pass",
                "claim_grade": True,
                "heading_error_p95_rad": 0.01,
                "lateral_error_p95_m": 0.05,
                "blocking_reasons": [],
                "warnings": [],
            },
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract.v1",
            "status": "pass",
            "blocking_reasons": [],
            "warnings": [],
            "evidence": {
                "planning_reference_available": True,
                "control_reference_available": True,
                "nonempty_trajectory_ratio": 1.0,
            },
            "metrics": {
                "planning_ref_heading_error_p95_rad": 0.01,
                "control_ref_heading_error_p95_rad": 0.02,
                "control_lateral_error_p95_m": 0.1,
            },
            "apollo_hdmap_projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "pass",
                "claim_grade": True,
                "heading_error_p95_rad": 0.01,
                "lateral_error_p95_m": 0.05,
                "blocking_reasons": [],
                "warnings": [],
            },
        },
    )
    _write_json(
        run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
        {
            "schema_version": "apollo_control_handoff.v1",
            "verdict": "pass",
            "failure_stage": "none",
            "blocking_reasons": [],
            "warnings": [],
            "control_channel": {"message_count": 100},
            "bridge_receive": {"control_rx_count": 100},
            "mapping_and_apply": {"apply_control_count": 100},
            "vehicle_response": {"status": "pass"},
        },
    )
    _write_json(
        run_dir / "analysis/control_health/control_health_report.json",
        {
            "schema_version": "control_health_report.v1",
            "status": "pass",
            "failure_reason": None,
            "control_handoff_status": "control_consuming_with_nonzero_planning",
            "metrics": {
                "lateral_guard_apply_count": 0,
                "trajectory_contract_guard_apply_count": 0,
                "mapped_applied_steer_abs_error_p95": 0.0,
                "mapped_applied_throttle_abs_error_p95": 0.0,
                "mapped_applied_brake_abs_error_p95": 0.0,
                "route_s_after_first_applied_control_delta_m": 30.0,
            },
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        {
            "schema_version": "obstacle_gt_contract.v1",
            "status": "pass",
            "object_count": 1,
            "errors": [],
            "warnings": [],
            "missing_fields": [],
        },
    )
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "scenario_class": "lane_keep",
            "prediction_mode": "native_observed",
            "prediction_channel_available": True,
            "prediction_message_count": 100,
            "planning_requires_prediction": True,
            "hard_gate_eligible": True,
            "blocking_capabilities": [],
            "warnings": [],
            "verdict": "pass",
        },
    )
    _write_json(
        run_dir / "analysis/natural_driving/natural_driving_report.json",
        {
            "schema_version": "natural_driving_report.v1",
            "verdict": {"status": "pass"},
            "can_claim_unassisted_natural_driving": True,
            "warnings": [],
            "blocking_reasons": [],
        },
    )
    return run_dir


def test_all_green_link_health_is_claimable(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["schema_version"] == APOLLO_LINK_HEALTH_SCHEMA_VERSION
    assert report["primary_blocker"] is None
    assert report["can_claim_unassisted_natural_driving"] is True
    assert report["layers"]["traffic_light_gt"]["status"] == "not_applicable"
    assert report["layers"]["hdmap_projection"]["status"] == "pass"
    assert report["layers"]["route_establishment"]["status"] == "pass"
    assert report["layers"]["prediction_evidence"]["status"] == "pass"


def test_missing_prediction_evidence_blocks_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/prediction_evidence/prediction_evidence_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["can_claim_unassisted_natural_driving"] is False
    assert report["layers"]["prediction_evidence"]["status"] == "insufficient_data"
    assert "prediction_evidence:insufficient_data" in report["why_not_claimable"]


def test_explicit_non_apollo_control_source_conflicts_with_apollo_control_rx(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["control_source"] = "route_follower"
    _write_json(manifest_path, manifest)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "fail"
    assert "control_source_conflict" in layer["blocking_reasons"]
    assert "control_source_not_apollo_control" in layer["blocking_reasons"]
    assert layer["key_metrics"]["explicit_control_source"] == "route_follower"
    assert layer["key_metrics"]["apollo_control_topic_observed"] is True
    assert report["can_claim_unassisted_natural_driving"] is False


def test_low_planning_materialization_is_primary_blocker(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["fail_reason"] = "ROUTE_ESTABLISHMENT_LATENCY_SEC"
    _write_json(summary_path, summary)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "status": "fail",
            "total_messages_received": 550,
            "messages_with_nonzero_trajectory_points": 14,
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "route_establishment:planning_trajectory_materialization_low"
    layer = report["layers"]["route_establishment"]
    assert layer["status"] == "fail"
    assert layer["key_metrics"]["nonempty_trajectory_ratio"] == 14 / 550
    assert "route_establishment_latency" in layer["blocking_reasons"]


def test_independent_hdmap_projection_report_is_consumed(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    loc_path = run_dir / "analysis/localization_contract/localization_contract_report.json"
    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    loc = json.loads(loc_path.read_text(encoding="utf-8"))
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    loc.pop("apollo_hdmap_projection", None)
    ref.pop("apollo_hdmap_projection", None)
    _write_json(loc_path, loc)
    _write_json(ref_path, ref)
    _write_json(
        run_dir / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        {
            "schema_version": "apollo_hdmap_projection_report.v1",
            "status": "pass",
            "claim_grade": True,
            "projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "pass",
                "claim_grade": True,
                "heading_error_p95_rad": 0.01,
                "lateral_error_p95_m": 0.05,
                "nearest_lane_id_topk": ["15_1_1"],
                "blocking_reasons": [],
                "warnings": [],
            },
            "blocking_reasons": [],
            "warnings": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["hdmap_projection"]
    assert layer["status"] == "pass"
    assert layer["key_metrics"]["official_source_available"] is True
    assert layer["key_metrics"]["claim_grade"] is True
    assert layer["artifact_paths"]["apollo_hdmap_projection"].endswith(
        "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json"
    )


def test_world_tick_timeout_before_routing_is_environment_blocker(tmp_path: Path) -> None:
    actual = tmp_path / "suite" / "lane_keep__02"
    planned = actual.with_name("lane_keep")
    _write_json(
        actual / "summary.json",
        {
            "run_id": "lane_keep__02",
            "scenario_class": "lane_keep",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "fail_reason": "ROUTING_REQUEST_COUNT",
            "frames": None,
            "routing_request_count": 0,
            "routing_materialized": False,
            "planning_materialized": False,
        },
    )
    _write_json(
        actual / "manifest.json",
        {
            "run_id": "lane_keep__02",
            "scenario_class": "lane_keep",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "carla_world": {
                "configured_town": "Town01",
                "loaded_map_name": "Town01",
                "matches_configured_town": True,
                "spawn_point_count": 255,
            },
        },
    )
    planned.mkdir(parents=True)
    (planned / "RUN_DIR_REDIRECT.txt").write_text(str(actual), encoding="utf-8")
    stderr_path = planned / "artifacts" / "followstop_child.stderr.log"
    stderr_path.parent.mkdir(parents=True)
    stderr_path.write_text(
        "Traceback\n"
        "  File \"carla_testbed/runner/harness.py\", line 568, in run\n"
        "    frame_id, timestamp, _ = tick_world(world)\n"
        "RuntimeError: time-out of 30000ms while waiting for the simulator\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(actual)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "fail"
    assert environment["blocking_reasons"] == ["carla_world_tick_timeout_before_routing"]
    assert environment["key_metrics"]["carla_world_tick_timeout_detected"] is True
    assert environment["artifact_paths"]["followstop_child_stderr"] == str(stderr_path)
    assert report["primary_blocker"] == "environment_world:carla_world_tick_timeout_before_routing"
    assert "routing_request_count_zero_is_downstream_of_world_tick_timeout" in environment["warnings"]


def test_world_tick_timeout_structured_artifact_is_environment_blocker(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_class": "lane_keep",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "fail_reason": "CARLA_WORLD_TICK_TIMEOUT",
            "frames": 2,
            "routing_request_count": 0,
            "carla_tick_health": {
                "last_failure_reason": "CARLA_WORLD_TICK_TIMEOUT",
                "tick_count": 2,
                "tick_fail_count": 1,
                "max_tick_wall_duration_s": 30.1,
            },
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "scenario_class": "lane_keep",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "carla_world": {
                "configured_town": "Town01",
                "loaded_map_name": "Town01",
                "matches_configured_town": True,
                "spawn_point_count": 255,
            },
        },
    )
    tick_summary_path = run_dir / "artifacts" / "carla_tick_health_summary.json"
    _write_json(
        tick_summary_path,
        {
            "schema_version": "carla_tick_health.v1",
            "last_failure_reason": "CARLA_WORLD_TICK_TIMEOUT",
            "tick_count": 2,
            "tick_fail_count": 1,
            "max_tick_wall_duration_s": 30.1,
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "fail"
    assert environment["blocking_reasons"] == ["carla_world_tick_timeout_before_routing"]
    assert environment["key_metrics"]["carla_world_tick_timeout_detected"] is True
    assert environment["key_metrics"]["carla_tick_count"] == 2
    assert environment["key_metrics"]["carla_tick_fail_count"] == 1
    assert environment["key_metrics"]["carla_tick_max_wall_duration_s"] == 30.1
    assert environment["artifact_paths"]["carla_tick_health_summary"] == str(tick_summary_path)
    assert report["primary_blocker"] == "environment_world:carla_world_tick_timeout_before_routing"


def test_environment_reports_tick_cadence_from_summary_without_failing_world(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    tick_summary_path = run_dir / "artifacts" / "carla_tick_health_summary.json"
    _write_json(
        tick_summary_path,
        {
            "schema_version": "carla_tick_health.v1",
            "tick_count": 180,
            "tick_fail_count": 0,
            "last_failure_reason": None,
            "max_tick_wall_duration_s": 0.2,
            "max_inter_tick_wall_interval_s": 3.5,
            "inter_tick_wall_interval_p95_s": 0.4,
            "inter_tick_wall_interval_count": 179,
            "max_frame_post_tick_wall_duration_s": 3.4,
            "max_frame_loop_wall_duration_s": 3.6,
            "max_stage_name": "gt_publish_and_hooks",
            "max_stage_duration_s": 2.7,
            "stage_duration_max_s": {
                "gt_publish_and_hooks": 2.7,
                "hook.after_world_tick.tick_callback_0": 2.5,
                "control_apply": 0.1,
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "pass"
    assert "carla_inter_tick_wall_interval_high" in environment["warnings"]
    assert environment["key_metrics"]["carla_tick_max_wall_duration_s"] == 0.2
    assert environment["key_metrics"]["carla_tick_max_inter_tick_wall_interval_s"] == 3.5
    assert environment["key_metrics"]["carla_tick_inter_tick_wall_interval_p95_s"] == 0.4
    assert environment["key_metrics"]["carla_tick_cadence_source"] == "carla_tick_health_summary"
    assert environment["key_metrics"]["carla_tick_max_stage_name"] == "gt_publish_and_hooks"
    assert environment["key_metrics"]["carla_tick_max_stage_duration_s"] == 2.7
    assert environment["key_metrics"]["carla_tick_max_hook_stage_name"] == "hook.after_world_tick.tick_callback_0"
    assert environment["key_metrics"]["carla_tick_max_hook_stage_duration_s"] == 2.5


def test_environment_backfills_tick_cadence_from_sparse_tick_log(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    tick_log_path = run_dir / "artifacts" / "carla_tick_health.jsonl"
    tick_log_path.parent.mkdir(parents=True, exist_ok=True)
    tick_log_path.write_text(
        "\n".join(
            json.dumps(row, sort_keys=True)
            for row in [
                {"event_type": "world_tick", "step": 0, "wall_time_s": 100.0, "tick_wall_duration_s": 0.01},
                {"event_type": "world_tick", "step": 1, "wall_time_s": 100.1, "tick_wall_duration_s": 0.01},
                {"event_type": "world_tick", "step": 20, "wall_time_s": 104.6, "tick_wall_duration_s": 0.01},
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "pass"
    assert "carla_inter_tick_wall_interval_high" in environment["warnings"]
    assert environment["key_metrics"]["carla_tick_max_inter_tick_wall_interval_s"] == 4.5
    assert environment["key_metrics"]["carla_tick_inter_tick_wall_interval_count"] == 2
    assert environment["key_metrics"]["carla_tick_cadence_source"] == "carla_tick_health_jsonl_sparse"
    assert environment["artifact_paths"]["carla_tick_health_log"] == str(tick_log_path)


def test_environment_prefers_frame_loop_timing_tick_cadence(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    tick_log_path = run_dir / "artifacts" / "carla_tick_health.jsonl"
    tick_log_path.parent.mkdir(parents=True, exist_ok=True)
    tick_log_path.write_text(
        "\n".join(
            json.dumps(row, sort_keys=True)
            for row in [
                {
                    "event_type": "frame_loop_timing",
                    "step": 1,
                    "inter_tick_wall_interval_s": 0.1,
                    "frame_post_tick_wall_duration_s": 0.2,
                    "frame_loop_wall_duration_s": 0.3,
                    "stage_durations_s": {"gt_publish_and_hooks": 0.1, "record_and_eval": 0.2},
                },
                {
                    "event_type": "frame_loop_timing",
                    "step": 2,
                    "inter_tick_wall_interval_s": 4.0,
                    "frame_post_tick_wall_duration_s": 3.9,
                    "frame_loop_wall_duration_s": 4.1,
                    "stage_durations_s": {
                        "after_world_tick_hooks": 3.3,
                        "hook.after_world_tick.tick_callback_0": 3.25,
                        "gt_publish_and_hooks": 0.2,
                        "record_and_eval": 0.4,
                    },
                },
                {"event_type": "world_tick", "step": 2, "wall_time_s": 100.0, "tick_wall_duration_s": 0.01},
                {"event_type": "world_tick", "step": 3, "wall_time_s": 120.0, "tick_wall_duration_s": 0.01},
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "pass"
    assert environment["key_metrics"]["carla_tick_cadence_source"] == "carla_tick_health_frame_loop_timing"
    assert environment["key_metrics"]["carla_tick_max_inter_tick_wall_interval_s"] == 4.0
    assert environment["key_metrics"]["carla_tick_max_frame_post_tick_wall_duration_s"] == 3.9
    assert environment["key_metrics"]["carla_tick_max_frame_loop_wall_duration_s"] == 4.1
    assert environment["key_metrics"]["carla_tick_max_stage_name"] == "after_world_tick_hooks"
    assert environment["key_metrics"]["carla_tick_max_hook_stage_name"] == "hook.after_world_tick.tick_callback_0"
    assert environment["key_metrics"]["carla_tick_max_hook_stage_duration_s"] == 3.25
    assert environment["key_metrics"]["carla_tick_stage_duration_max_s"]["hook.after_world_tick.tick_callback_0"] == 3.25


def test_external_carla_missing_is_environment_blocker(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update({"fail_reason": "ROUTING_REQUEST_COUNT", "routing_success_count": 0})
    _write_json(summary_path, summary)
    _write_json(
        run_dir / "artifacts/carla_world_ready_summary.json",
        {
            "status": "external_carla_missing",
            "target_town": "Town01",
            "start_carla": False,
            "final_town": None,
        },
    )
    _write_json(
        run_dir / "artifacts/carla_bootstrap_summary.json",
        {
            "carla_bootstrap_status": "bootstrap_unknown",
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "fail"
    assert environment["blocking_reasons"] == ["external_carla_missing"]
    assert environment["artifact_paths"]["carla_world_ready_summary"] == str(
        run_dir / "artifacts/carla_world_ready_summary.json"
    )
    assert report["primary_blocker"] == "environment_world:external_carla_missing"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_runtime_contract_not_required_points_to_true_lateral_rerun(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["runtime_contract"] = {
        "capability_profile": "lane_keep",
        "requires_true_lateral": False,
        "requested_true_lateral": False,
        "status": "not_required",
        "blockers": [],
    }
    _write_json(summary_path, summary)

    report = analyze_apollo_link_health_run_dir(run_dir)

    environment = report["layers"]["environment_world"]
    assert environment["status"] == "fail"
    assert environment["blocking_reasons"] == ["runtime_contract_not_required"]
    assert "--enable-lateral" in environment["next_action"]
    assert "diagnostic-only" in environment["next_action"]
    assert report["primary_blocker"] == "environment_world:runtime_contract_not_required"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_raw_handoff_artifact_prioritizes_routing_before_no_assist_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json").unlink()
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "fail_reason": "ROUTING_REQUEST_COUNT",
            "frames": 80,
            "routing_request_count": 0,
            "routing_success_count": 0,
            "planning_materialized": True,
        }
    )
    _write_json(summary_path, summary)
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_request_count": 0,
            "routing_success_count": 0,
            "control_rx_count": 0,
            "control_tx_count": 0,
            "apply_control_count": 0,
            "gt_stale_sample_skip_count": 356,
            "gt_stale_sample_republish_count": 0,
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 43,
            "messages_with_nonzero_trajectory_points": 0,
            "messages_with_zero_trajectory_points": 43,
        },
    )
    handoff_path = run_dir / "artifacts/control_handoff_summary.json"
    _write_json(
        handoff_path,
        {
            "control_handoff_status": "control_process_missing",
            "planning_first_nonempty_at": None,
            "control_consume_row_count": 0,
            "apollo_control_process_alive": False,
        },
    )
    _write_json(
        run_dir / "artifacts/carla_tick_health_summary.json",
        {
            "schema_version": "carla_tick_health.v1",
            "tick_count": 80,
            "tick_fail_count": 0,
            "last_failure_reason": None,
            "max_tick_wall_duration_s": 0.2,
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    handoff = report["layers"]["routing_planning_control_handoff"]
    assert handoff["artifact_paths"]["apollo_control_handoff"] == str(handoff_path)
    assert handoff["status"] == "fail"
    assert "routing_success_missing" in handoff["blocking_reasons"]
    assert "planning_nonempty_missing" in handoff["blocking_reasons"]
    assert "control_process_missing" in handoff["blocking_reasons"]
    assert report["primary_blocker"] == "routing_planning_control_handoff:routing_success_missing"
    assert "no_assist_claim_boundary:control_apply_missing" in report["secondary_blockers"]


def test_command_materialization_warmup_incomplete_is_primary_insufficient_data(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json").unlink()
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "fail_reason": "ROUTING_REQUEST_COUNT",
            "frames": 80,
            "routing_request_count": 0,
            "routing_success_count": 0,
            "planning_materialized": True,
        }
    )
    _write_json(summary_path, summary)
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_request_count": 0,
            "routing_success_count": 0,
            "control_rx_count": 0,
            "control_tx_count": 0,
            "apply_control_count": 0,
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 43,
            "messages_with_nonzero_trajectory_points": 0,
        },
    )
    handoff_path = run_dir / "artifacts/control_handoff_summary.json"
    _write_json(
        handoff_path,
        {
            "control_handoff_status": "control_process_missing",
            "control_consume_row_count": 0,
        },
    )
    command_path = run_dir / "artifacts/command_materialization_summary.json"
    _write_json(
        command_path,
        {
            "command_path_stage": "command_interface_unavailable",
            "first_divergence_reason": "apollo_startup_warmup",
            "gate_state": {
                "last_blocking_reason": "apollo_startup_warmup",
                "last_blocking_detail": "waiting for configured Apollo startup warmup window",
                "apollo_warmup_remaining_sec": 3.34,
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    handoff = report["layers"]["routing_planning_control_handoff"]
    assert handoff["status"] == "insufficient_data"
    assert handoff["blocking_reasons"] == ["apollo_startup_warmup_incomplete"]
    assert handoff["artifact_paths"]["apollo_control_handoff"] == str(handoff_path)
    assert handoff["artifact_paths"]["command_materialization_summary"] == str(command_path)
    assert handoff["key_metrics"]["apollo_warmup_remaining_sec"] == 3.34
    assert report["primary_blocker"] == "routing_planning_control_handoff:apollo_startup_warmup_incomplete"
    assert "no_assist_claim_boundary:control_apply_missing" in report["secondary_blockers"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_lane_keep_empty_obstacle_contract_does_not_block_link_health(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        {
            "schema_version": "obstacle_gt_contract.v1",
            "status": "pass_empty",
            "message_count": 20,
            "empty_message_count": 20,
            "empty_obstacle_messages_healthy": True,
            "object_count": 0,
            "errors": [],
            "warnings": [],
            "missing_fields": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    obstacle = report["layers"]["perception_gt_obstacles"]
    assert obstacle["status"] == "pass"
    assert obstacle["blocking_reasons"] == []
    assert report["can_claim_unassisted_natural_driving"] is True


def test_link_health_uses_suite_level_natural_driving_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path / "suite")
    local_report = run_dir / "analysis/natural_driving/natural_driving_report.json"
    suite_report = tmp_path / "suite" / "analysis/natural_driving/natural_driving_report.json"
    suite_report.parent.mkdir(parents=True, exist_ok=True)
    local_report.unlink()
    _write_json(
        suite_report,
        {
            "schema_version": "natural_driving_report.v1",
            "verdict": {"status": "warn"},
            "can_claim_unassisted_natural_driving": False,
            "warnings": ["suite_level_warning"],
            "blocking_reasons": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["layers"]["natural_driving_outcome"]["status"] == "warn"
    assert report["layers"]["natural_driving_outcome"]["artifact_paths"]["natural_driving_report"] == str(
        suite_report
    )


def test_reference_line_localization_lane_heading_mismatch_is_primary_blocker(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    loc_path = run_dir / "analysis/localization_contract/localization_contract_report.json"
    loc = json.loads(loc_path.read_text(encoding="utf-8"))
    loc["verdict"] = {"status": "fail", "blocking_reasons": ["heading_error_to_lane_high"]}
    loc["pose_consistency"]["heading_error_to_lane_p95_rad"] = 0.3586
    _write_json(loc_path, loc)

    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["status"] = "fail"
    ref["blocking_reasons"] = ["reference_line_heading_error_high"]
    ref["metrics"]["control_ref_heading_error_p95_rad"] = 0.30
    _write_json(ref_path, ref)

    control_path = run_dir / "analysis/control_health/control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["status"] = "fail"
    control["failure_reason"] = "applied_actuation_oscillation"
    _write_json(control_path, control)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "reference_line/localization lane-heading mismatch"
    assert "control_mapping_apply:applied_actuation_oscillation" in report["secondary_blockers"]
    assert report["can_claim_unassisted_natural_driving"] is False
    assert report["layers"]["control_mapping_apply"]["status"] == "fail"


def test_control_oscillation_becomes_primary_only_after_localization_and_reference_pass(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    control_path = run_dir / "analysis/control_health/control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["status"] = "fail"
    control["failure_reason"] = "applied_actuation_oscillation"
    control.setdefault("metrics", {}).setdefault("control_decode_debug", {})[
        "longitudinal_oscillation_attribution"
    ] = {
        "transition_count": 12,
        "dominant_suspected_factor": "apollo_simple_lon_acceleration_cmd_sign_switching",
    }
    control["metrics"]["control_decode_debug"]["trajectory_consume_correlation"] = {
        "transition_count": 12,
        "dominant_suspected_factor": "planning_sequence_update_correlates_with_switches",
    }
    control["metrics"]["control_decode_debug"]["planning_trajectory_correlation"] = {
        "transition_count": 12,
        "transition_buckets": {
            "trajectory_path_length_delta_gt_5m": 8,
            "speed_fallback_involved": 6,
        },
        "dominant_suspected_factor": "planning_trajectory_length_switching",
    }
    control["metrics"]["planning_log_fallback_diagnostics"] = {
        "matched_point_lon_diff_replan_count": 4,
        "piecewise_jerk_speed_optimizer_fail_count": 2,
        "dominant_suspected_factor": "trajectory_stitcher_matched_point_lon_diff_replans",
    }
    control["metrics"]["gt_state_sampling_cadence"] = {
        "control_to_chassis_count_ratio": 10.0,
        "control_rx_wall_hz": 20.0,
        "chassis_wall_hz": 2.0,
    }
    _write_json(control_path, control)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "control_mapping_apply:applied_actuation_oscillation"
    assert report["can_claim_unassisted_natural_driving"] is False
    metrics = report["layers"]["control_mapping_apply"]["key_metrics"]
    assert (
        metrics["longitudinal_oscillation_attribution"]["dominant_suspected_factor"]
        == "apollo_simple_lon_acceleration_cmd_sign_switching"
    )
    assert (
        metrics["trajectory_consume_correlation"]["dominant_suspected_factor"]
        == "planning_sequence_update_correlates_with_switches"
    )
    assert (
        metrics["planning_trajectory_correlation"]["dominant_suspected_factor"]
        == "planning_trajectory_length_switching"
    )
    assert (
        metrics["planning_log_fallback_diagnostics"]["dominant_suspected_factor"]
        == "trajectory_stitcher_matched_point_lon_diff_replans"
    )
    assert metrics["gt_state_sampling_cadence"]["control_to_chassis_count_ratio"] == 10.0


def test_control_cadence_is_secondary_until_reference_line_is_non_blocking(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["planning_nonempty_ratio"] = 0.41
    _write_json(summary_path, summary)

    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["status"] = "insufficient_data"
    ref["warnings"] = ["apollo_hdmap_projection_missing", "nonempty_trajectory_ratio_low"]
    _write_json(ref_path, ref)

    control_path = run_dir / "analysis/control_health/control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["status"] = "fail"
    control["failure_reason"] = "control_bridge_world_frame_cadence_low"
    _write_json(control_path, control)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "planning_reference_line:insufficient_data"
    assert "control_mapping_apply:control_bridge_world_frame_cadence_low" in report["secondary_blockers"]
    assert "no_assist_claim_boundary:planning_nonempty_ratio_not_claim_grade" in report["secondary_blockers"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_planning_nonempty_claim_boundary_next_action_points_to_planning_not_assist(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["planning_nonempty_ratio"] = 0.41
    _write_json(summary_path, summary)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "no_assist_claim_boundary:planning_nonempty_ratio_not_claim_grade"
    assert "Planning availability" in report["next_highest_value_validation"]
    assert "not an assist cleanup" in report["next_highest_value_validation"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_planning_nonempty_claim_window_overrides_startup_diluted_overall_ratio(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["planning_nonempty_ratio"] = 0.41
    _write_json(summary_path, summary)

    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["evidence"]["nonempty_trajectory_ratio"] = 0.41
    ref["evidence"]["nonempty_trajectory_ratio_after_routing_segment_available"] = 0.95
    ref["evidence"]["nonempty_trajectory_ratio_after_first_nonempty"] = 1.0
    ref["evidence"]["planning_claim_window_nonempty_trajectory_ratio"] = 0.95
    ref["evidence"]["planning_claim_window_source"] = "after_routing_segment_available"
    _write_json(ref_path, ref)

    report = analyze_apollo_link_health_run_dir(run_dir)
    no_assist = report["layers"]["no_assist_claim_boundary"]

    assert no_assist["status"] == "pass"
    assert "planning_nonempty_ratio_not_claim_grade" not in no_assist["blocking_reasons"]
    assert no_assist["key_metrics"]["planning_nonempty_ratio"] == 0.95
    assert no_assist["key_metrics"]["planning_nonempty_ratio_overall"] == 0.41
    assert no_assist["key_metrics"]["planning_nonempty_ratio_source"] == (
        "apollo_reference_line_contract.after_routing_segment_available"
    )
    assert report["primary_blocker"] is None
    assert report["can_claim_unassisted_natural_driving"] is True


def test_summary_dummy_lateral_blocks_no_assist_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("assist_ledger", None)
    _write_json(manifest_path, manifest)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["lateral_mode"] = "dummy"
    summary["controller"] = "composite"
    _write_json(summary_path, summary)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "fail"
    assert layer["key_metrics"]["active_assists"] == ["dummy_lateral"]
    assert layer["key_metrics"]["blocking_assists"] == ["dummy_lateral"]
    assert "blocking_assists_active" in layer["blocking_reasons"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_external_stack_legacy_placeholder_blocks_no_interference_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "controller": "external_stack",
            "lateral_mode": "dummy",
            "control_source": "external_stack",
            "harness_control_disabled": True,
            "legacy_controller_role": "compatibility_placeholder",
            "legacy_controller_applied": False,
        }
    )
    _write_json(summary_path, summary)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "fail"
    assert layer["key_metrics"]["active_assists"] == []
    assert layer["key_metrics"]["blocking_assists"] == []
    assert layer["key_metrics"]["assist_confidence"] == "explicit"
    assert layer["key_metrics"]["control_source"] == "external_stack"
    assert "control_source_not_apollo_control" in layer["blocking_reasons"]
    assert "control_source_conflict" in layer["blocking_reasons"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_no_assist_layer_infers_apollo_control_from_handoff_artifacts(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.pop("backend", None)
    summary["metrics"] = {}
    _write_json(summary_path, summary)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("backend", None)
    manifest.pop("control_source", None)
    _write_json(manifest_path, manifest)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 100,
            "messages_with_nonzero_trajectory_points": 95,
        },
    )
    handoff_path = run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["control_channel"]["name"] = "/apollo/control"
    _write_json(handoff_path, handoff)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "pass"
    assert layer["key_metrics"]["backend"] == "apollo_cyberrt"
    assert layer["key_metrics"]["control_source"] == "/apollo/control"
    assert layer["key_metrics"]["planning_nonempty_ratio"] == 0.95
    assert report["can_claim_unassisted_natural_driving"] is True


def test_no_assist_layer_does_not_let_bridge_rx_override_external_stack_placeholder(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["control_source"] = "external_stack"
    summary["harness_control_disabled"] = True
    summary["legacy_controller_role"] = "compatibility_placeholder"
    summary["legacy_controller_applied"] = False
    summary["metrics"] = {}
    _write_json(summary_path, summary)

    handoff_path = run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
    handoff_path.unlink()
    _write_json(
        run_dir / "artifacts/control_handoff_summary.json",
        {
            "control_handoff_status": "control_consuming_with_nonzero_planning",
            "control_consume_row_count": 224,
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 100,
            "messages_with_nonzero_trajectory_points": 95,
        },
    )
    stats_path = run_dir / "artifacts/cyber_bridge_stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    stats.pop("apply_control_count", None)
    stats["control_rx_count"] = 174
    stats["control_tx_count"] = 174
    _write_json(stats_path, stats)
    control_health_path = run_dir / "analysis/control_health/control_health_report.json"
    control_health = json.loads(control_health_path.read_text(encoding="utf-8"))
    control_health["metrics"]["nonzero_applied_control_frames"] = 75
    control_health["metrics"]["control_bridge_log"] = {"final_applied_count": 76}
    _write_json(control_health_path, control_health)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "fail"
    assert layer["key_metrics"]["control_source"] == "external_stack"
    assert layer["key_metrics"]["explicit_control_source"] == "external_stack"
    assert layer["key_metrics"]["apollo_control_topic_observed"] is True
    assert layer["key_metrics"]["control_apply_count"] == 75
    assert "control_source_not_apollo_control" in layer["blocking_reasons"]
    assert "control_source_conflict" in layer["blocking_reasons"]
    assert "control_apply_missing" not in layer["blocking_reasons"]


def test_missing_backend_evidence_is_insufficient_not_wrong_backend(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.pop("backend", None)
    _write_json(summary_path, summary)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("backend", None)
    manifest.pop("control_source", None)
    _write_json(manifest_path, manifest)
    (run_dir / "artifacts/cyber_bridge_stats.json").unlink()
    (run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "insufficient_data"
    assert "backend_missing" in layer["blocking_reasons"]
    assert "backend_not_apollo_cyberrt" not in layer["blocking_reasons"]


def test_missing_required_artifact_is_insufficient_not_pass(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["layers"]["channel_health"]["status"] == "insufficient_data"
    assert "required_artifact_missing" in report["layers"]["channel_health"]["warnings"]
    assert report["primary_blocker"] == "channel_health:insufficient_data"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_planning_gap_channel_fail_prefers_reference_line_context(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "fail",
            "missing_required_channels": [],
            "low_rate_channels": [],
            "timestamp_failures": [],
            "warnings": ["traffic_light_has_no_messages_optional_for_lane_keep"],
            "channel_results": {
                "planning": {
                    "name": "planning",
                    "channel": "/apollo/planning",
                    "status": "fail",
                    "issues": ["message_gap_too_large"],
                    "message_count": 407,
                    "hz": 2.8,
                    "max_gap_ms": 11249.1,
                    "gap_p95_ms": 364.5,
                    "gap_count_over_1000ms": 1,
                    "primary_time_axis": "planning_header_timestamp_sec",
                    "time_axis_diagnosis": "header_or_wall_time_gap_large_sim_time_gap_ok",
                    "sim_time_hz": 19.8,
                    "sim_time_max_gap_ms": 150.0,
                    "sim_time_gap_p95_ms": 50.0,
                    "sim_time_gap_count_over_1000ms": 0,
                    "sim_time_timestamp_monotonic": True,
                    "source": "planning_topic_debug.jsonl",
                }
            },
        },
    )
    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["status"] = "insufficient_data"
    ref["warnings"] = [
        "nonempty_trajectory_ratio_low",
        "control_reference.control_reference_debug",
        "apollo_hdmap_projection_missing",
    ]
    ref.setdefault("contracts", {})
    ref["contracts"]["planning_trajectory"] = {
        "status": "warn",
        "warnings": ["nonempty_trajectory_ratio_low"],
    }
    ref["contracts"]["control_reference"] = {
        "status": "insufficient_data",
        "warnings": [],
    }
    ref["contracts"]["apollo_hdmap_projection"] = {
        "status": "insufficient_data",
        "warnings": ["apollo_hdmap_projection_missing"],
    }
    _write_json(ref_path, ref)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "planning_reference_line:insufficient_data"
    assert "channel_health:fail" in report["secondary_blockers"]
    channel_metrics = report["layers"]["channel_health"]["key_metrics"]
    assert channel_metrics["failed_channels"] == ["planning"]
    assert channel_metrics["planning_issues"] == ["message_gap_too_large"]
    assert channel_metrics["planning_primary_time_axis"] == "planning_header_timestamp_sec"
    assert channel_metrics["planning_time_axis_diagnosis"] == "header_or_wall_time_gap_large_sim_time_gap_ok"
    assert channel_metrics["planning_sim_time_max_gap_ms"] == 150.0
    assert channel_metrics["planning_sim_time_gap_count_over_1000ms"] == 0
    assert channel_metrics["failed_channel_details"]["planning"]["time_axis_diagnosis"] == (
        "header_or_wall_time_gap_large_sim_time_gap_ok"
    )
    assert channel_metrics["failed_channel_details"]["planning"]["sim_time_gap_p95_ms"] == 50.0
    summary_md = apollo_link_health_summary_md(report)
    assert "planning_time_axis_diagnosis=header_or_wall_time_gap_large_sim_time_gap_ok" in summary_md
    assert "planning_sim_time_max_gap_ms=150" in summary_md
    assert report["can_claim_unassisted_natural_driving"] is False


def test_planning_gap_with_inter_tick_pause_points_to_loop_cadence(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/carla_tick_health_summary.json",
        {
            "schema_version": "carla_tick_health.v1",
            "tick_count": 180,
            "tick_fail_count": 0,
            "last_failure_reason": None,
            "max_tick_wall_duration_s": 0.2,
            "max_inter_tick_wall_interval_s": 11.5,
            "inter_tick_wall_interval_p95_s": 0.4,
            "inter_tick_wall_interval_count": 179,
        },
    )
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "fail",
            "missing_required_channels": [],
            "low_rate_channels": [],
            "timestamp_failures": [],
            "warnings": ["planning_header_or_wall_gap_large_but_sim_time_gap_within_limit"],
            "channel_results": {
                "planning": {
                    "name": "planning",
                    "channel": "/apollo/planning",
                    "status": "fail",
                    "issues": ["message_gap_too_large"],
                    "message_count": 77,
                    "max_gap_ms": 11334.7,
                    "primary_time_axis": "planning_header_timestamp_sec",
                    "time_axis_diagnosis": "header_or_wall_time_gap_large_sim_time_gap_ok",
                    "sim_time_max_gap_ms": 150.0,
                    "sim_time_gap_p95_ms": 50.0,
                    "sim_time_gap_count_over_1000ms": 0,
                    "source": "planning_topic_debug.jsonl",
                }
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "channel_health:fail"
    assert "harness loop cadence" in report["next_highest_value_validation"]
    assert report["layers"]["environment_world"]["status"] == "pass"
    assert "carla_inter_tick_wall_interval_high" in report["layers"]["environment_world"]["warnings"]


def test_required_channel_missing_remains_channel_primary(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "fail",
            "missing_required_channels": ["/apollo/control"],
            "low_rate_channels": [],
            "timestamp_failures": [],
            "channel_results": {},
        },
    )
    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["status"] = "insufficient_data"
    _write_json(ref_path, ref)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "channel_health:fail"
    assert "planning_reference_line:insufficient_data" in report["secondary_blockers"]


def test_missing_obstacle_contract_blocks_unassisted_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["layers"]["perception_gt_obstacles"]["status"] == "insufficient_data"
    assert "perception_gt_obstacles:insufficient_data" in report["why_not_claimable"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_lane_keep_raw_empty_obstacle_contract_is_non_blocking(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json").unlink()
    raw_path = run_dir / "artifacts/obstacle_gt_contract.jsonl"
    raw_path.write_text(
        "\n".join(
            json.dumps(
                {
                    "timestamp": 1.0 + index * 0.05,
                    "published_obstacle_count": 0,
                    "is_ego": False,
                },
                sort_keys=True,
            )
            for index in range(2)
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["perception_gt_obstacles"]

    assert layer["status"] == "pass"
    assert layer["key_metrics"]["empty_obstacle_messages_healthy"] is True
    assert layer["artifact_paths"]["source_kind"] == "raw_artifact"
    assert "perception_gt_obstacles:insufficient_data" not in report["why_not_claimable"]


def test_dynamic_scenario_raw_empty_obstacle_contract_fails(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json").unlink()
    for rel in ["summary.json", "manifest.json"]:
        path = run_dir / rel
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["scenario_class"] = "follow_stop"
        payload["scenario_id"] = "follow_stop"
        _write_json(path, payload)
    raw_path = run_dir / "artifacts/obstacle_gt_contract.jsonl"
    raw_path.write_text(
        json.dumps({"timestamp": 1.0, "published_obstacle_count": 0}, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["perception_gt_obstacles"]

    assert layer["status"] == "fail"
    assert "required_dynamic_obstacle_missing" in layer["blocking_reasons"]
    assert "perception_gt_obstacles:required_dynamic_obstacle_missing" in report["why_not_claimable"]


def test_traffic_light_scenario_missing_contract_blocks_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    manifest_path = run_dir / "manifest.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary["scenario_id"] = "traffic_light_red_stop"
    summary["scenario_class"] = "traffic_light_red_stop"
    manifest["scenario_id"] = "traffic_light_red_stop"
    manifest["scenario_class"] = "traffic_light_red_stop"
    _write_json(summary_path, summary)
    _write_json(manifest_path, manifest)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["layers"]["traffic_light_gt"]["status"] == "insufficient_data"
    assert "traffic_light_gt:insufficient_data" in report["why_not_claimable"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_cli_writes_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_link_health.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)
    assert payload["primary_blocker"] is None
    assert (out / "apollo_link_health_report.json").is_file()
    assert (out / "apollo_link_health_summary.md").is_file()


def test_write_report_outputs_json_and_markdown(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    report = analyze_apollo_link_health_run_dir(run_dir)

    outputs = write_apollo_link_health_report(report, tmp_path / "written")

    assert Path(outputs["apollo_link_health_report"]).is_file()
    assert Path(outputs["apollo_link_health_summary"]).is_file()
