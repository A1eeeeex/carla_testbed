from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_link_health import (
    APOLLO_LINK_HEALTH_SCHEMA_VERSION,
    _should_regenerate_apollo_route_contract,
    _should_regenerate_apollo_reference_line_contract,
    analyze_apollo_link_health_run_dir,
    apollo_link_health_summary_md,
    write_apollo_link_health_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


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
        run_dir / "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
        {
            "schema_version": "chassis_gt_contract.v1",
            "status": "pass",
            "claim_grade": True,
            "channel": {
                "status": "pass",
                "message_count": 100,
                "hz": 20.0,
                "max_gap_ms": 80.0,
                "timestamp_monotonic": True,
                "sequence_monotonic": True,
            },
            "speed_consistency": {
                "status": "pass",
                "sample_count": 100,
                "speed_delta_p95_mps": 0.02,
            },
            "state": {
                "status": "pass",
                "driving_modes": ["COMPLETE_AUTO_DRIVE"],
                "gear_locations": ["GEAR_DRIVE"],
                "error_codes": ["NO_ERROR"],
            },
            "warnings": [],
            "blocking_reasons": [],
            "verdict": {"status": "pass", "blocking_reasons": []},
        },
    )
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "pass",
            "scenario_route": {
                "route_id": "lane097",
                "route_length_m": 230.0,
                "lane_sequence": ["15_1_1"],
            },
            "apollo_route": {
                "routing_success_count": 1,
                "routing_total_length_m": 230.0,
                "lane_window_count": 1,
                "lane_sequence": ["15_1_1"],
            },
            "metrics": {"routing_length_ratio": 1.0},
            "blocking_reasons": [],
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
        run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
        {
            "schema_version": "apollo_module_consumption.v1",
            "status": "pass",
            "routing_response_consumed_by_planning": True,
            "prediction_mode": "native_observed",
            "pattern_counts": {
                "localization_timeout": 0,
                "chassis_timeout": 0,
                "reference_line_provider_failure": 0,
            },
            "empty_reason_histogram": {},
            "planning_input_age": {
                "localization_age_ms_p95": 10.0,
                "chassis_age_ms_p95": 10.0,
            },
            "blocking_reasons": [],
            "warnings": [],
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


def test_link_health_prefers_phase1_manifest_identity_over_legacy_summary(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_id": "legacy_lane_keep",
            "scenario_class": "lane_keep",
            "route_id": "legacy_route",
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "scenario_id": "baguang_follow_stop_static_300m_spawn2m",
            "scenario_class": "follow_stop_static",
            "route_id": "straight_road_for_baguang_mainline_followstop_300m_spawn2m",
            "backend": "apollo_cyberrt",
        },
    )
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "insufficient_data",
            "scenario_id": "baguang_follow_stop_static_300m_spawn2m",
            "scenario_class": "follow_stop_static",
            "route_id": "straight_road_for_baguang_mainline_followstop_300m_spawn2m",
            "missing_fields": ["apollo_hdmap_projection_for_lane_equivalence"],
            "warnings": ["apollo_hdmap_projection_required_for_cross_namespace_lane_equivalence"],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["scenario_id"] == "baguang_follow_stop_static_300m_spawn2m"
    assert report["scenario_class"] == "follow_stop_static"
    assert report["route_id"] == "straight_road_for_baguang_mainline_followstop_300m_spawn2m"


def test_reference_line_fail_report_without_path_fallback_metrics_is_regenerated() -> None:
    stale_report = {
        "schema_version": "apollo_reference_line_contract.v1",
        "status": "fail",
        "blocking_reasons": ["planning_reference_heading_error_high"],
        "metrics": {
            "planning_ref_heading_error_p95_rad": 0.31,
            "control_ref_heading_error_p95_rad": 0.0,
        },
    }

    fresh_report = {
        "schema_version": "apollo_reference_line_contract.v1",
        "status": "fail",
        "blocking_reasons": ["planning_path_fallback_heading_error_high"],
        "metrics": {
            "planning_ref_heading_error_p95_rad": 0.31,
            "path_fallback_trajectory_ratio": 0.10,
        },
    }

    assert _should_regenerate_apollo_reference_line_contract(stale_report) is True
    assert _should_regenerate_apollo_reference_line_contract(fresh_report) is False


def test_all_green_link_health_is_claimable(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["schema_version"] == APOLLO_LINK_HEALTH_SCHEMA_VERSION
    assert report["primary_blocker"] is None
    assert report["can_claim_unassisted_natural_driving"] is True
    assert report["layers"]["traffic_light_gt"]["status"] == "not_applicable"
    assert report["layers"]["hdmap_projection"]["status"] == "pass"
    assert report["layers"]["route_establishment"]["status"] == "pass"
    assert report["layers"]["apollo_module_consumption"]["status"] == "pass"
    assert report["layers"]["prediction_evidence"]["status"] == "pass"


def test_missing_prediction_evidence_blocks_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/prediction_evidence/prediction_evidence_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["can_claim_unassisted_natural_driving"] is False
    assert report["layers"]["prediction_evidence"]["status"] == "insufficient_data"
    assert "prediction_evidence:insufficient_data" in report["why_not_claimable"]


def test_prediction_evidence_unknown_placeholder_is_regenerated_as_explicit_bypass(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "scenario_class": "lane_keep",
            "prediction_mode": "unknown",
            "status": "insufficient_data",
            "verdict": "insufficient_data",
            "hard_gate_eligible": False,
            "blocking_capabilities": ["prediction_status_unknown"],
        },
    )
    _write_json(
        run_dir / "channel_stats.json",
        {
            "schema_version": "channel_stats.v1",
            "channels": {
                "/apollo/perception/obstacles": {
                    "message_count": 20,
                    "hz": 20.0,
                    "max_gap_ms": 60.0,
                    "timestamp_monotonic": True,
                    "sequence_monotonic": True,
                    "stale_count": 0,
                }
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["prediction_evidence"]

    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert layer["key_metrics"]["prediction_mode"] == "bypassed_with_gt_obstacles"
    assert layer["key_metrics"]["prediction_bypass_scope"] == "static_lane_keep_no_dynamic_obstacles"
    assert layer["key_metrics"]["hard_gate_eligible"] is True
    assert "prediction_status_unknown" not in layer["blocking_reasons"]
    assert "prediction_not_hard_gate_eligible" not in layer["blocking_reasons"]
    assert "prediction_evidence:hard_gate_eligible_false" not in report["why_not_claimable"]
    assert not any(
        reason.startswith("prediction_evidence:")
        for reason in report["why_not_claimable"]
    )


def test_dynamic_prediction_placeholder_is_regenerated_and_written_before_consumption(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    for rel in ("summary.json", "manifest.json"):
        path = run_dir / rel
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["scenario_id"] = "baguang_cut_in_35kph_left_to_right_10m"
        payload["scenario_class"] = "cut_in"
        _write_json(path, payload)
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "scenario_class": "cut_in",
            "prediction_mode": "unknown",
            "status": "insufficient_data",
            "verdict": "insufficient_data",
            "hard_gate_eligible": False,
            "blocking_capabilities": ["prediction_status_unknown"],
        },
    )
    _write_json(
        run_dir / "channel_stats.json",
        {
            "schema_version": "channel_stats.v1",
            "channels": {
                "/apollo/perception/obstacles": {
                    "message_count": 20,
                    "hz": 20.0,
                    "max_gap_ms": 60.0,
                    "timestamp_monotonic": True,
                    "sequence_monotonic": True,
                    "stale_count": 0,
                }
            },
        },
    )
    (run_dir / "artifacts/apollo_modules_status.log").write_text(
        "96128 mainboard -d modules/prediction/dag/prediction.dag -p prediction -s CYBER_DEFAULT\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts/apollo_prediction.INFO").write_text(
        "I0620 prediction_component.cc:111] Normal Obstacle: 52 used CRUISE_MLP_EVALUATOR\n",
        encoding="utf-8",
    )
    module_report = json.loads(
        (run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json").read_text(
            encoding="utf-8"
        )
    )
    module_report["prediction_mode"] = "unknown"
    _write_json(
        run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
        module_report,
    )
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "warn",
            "planning_message_count": 100,
            "nonempty_trajectory_ratio": 1.0,
            "claim_window_nonempty_ratio": 1.0,
            "claim_window_source": "after_routing_success",
            "blocking_reasons": [],
            "warnings": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    prediction_layer = report["layers"]["prediction_evidence"]
    assert prediction_layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert prediction_layer["status"] == "fail"
    assert prediction_layer["key_metrics"]["prediction_mode"] == "missing"
    assert prediction_layer["key_metrics"]["prediction_runtime_observed"] is True
    assert prediction_layer["key_metrics"]["prediction_internal_log_activity_observed"] is True
    assert prediction_layer["key_metrics"]["prediction_internal_log_activity_count"] == 1
    assert "prediction_evidence:closed_loop" in report["why_not_claimable"]

    persisted = json.loads(
        (run_dir / "analysis/prediction_evidence/prediction_evidence_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert persisted["prediction_mode"] == "missing"
    assert persisted["prediction_runtime_observed"] is True
    assert persisted["prediction_internal_log_activity_observed"] is True
    assert "prediction_module_observed_without_prediction_channel_output" in persisted["warnings"]
    assert "prediction_internal_log_activity_without_channel_output" in persisted["warnings"]

    module_layer = report["layers"]["apollo_module_consumption"]
    assert module_layer["key_metrics"]["prediction_mode"] == "missing"


def test_missing_module_consumption_blocks_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["can_claim_unassisted_natural_driving"] is False
    assert report["layers"]["apollo_module_consumption"]["status"] == "insufficient_data"
    assert "apollo_module_consumption:insufficient_data" in report["why_not_claimable"]


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


def test_external_stack_label_is_overridden_by_apollo_control_handoff(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    for rel in ("summary.json", "manifest.json"):
        path = run_dir / rel
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["control_source"] = "external_stack"
        _write_json(path, payload)
    handoff_path = run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["verdict"] = "warn"
    handoff["failure_stage"] = "none"
    handoff["control_channel"]["name"] = "/apollo/control"
    handoff["control_channel"]["message_count"] = 100
    handoff["bridge_receive"]["control_rx_count"] = 100
    handoff["mapping_and_apply"]["apply_control_count"] = 100
    _write_json(handoff_path, handoff)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "pass"
    assert "control_source_conflict" not in layer["blocking_reasons"]
    assert layer["key_metrics"]["explicit_control_source"] == "external_stack"
    assert layer["key_metrics"]["control_source"] == "/apollo/control"
    assert layer["key_metrics"]["applied_control_source"] == "apollo_control"
    assert layer["key_metrics"]["handoff_proves_apollo_control"] is True
    assert "summary_control_source_overridden_by_apollo_control_handoff" in layer["warnings"]


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


def test_apollo_planning_materialization_report_takes_precedence_over_stale_legacy_report(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "fail",
            "planning_message_count": 81,
            "nonempty_trajectory_ratio": 0.61,
            "claim_window_nonempty_ratio": 0.61,
            "claim_window_source": "after_routing_success",
            "blocking_reasons": [
                "planning_trajectory_materialization_low",
                "route_establishment_not_confirmed",
            ],
        },
    )
    _write_json(
        run_dir / "analysis/apollo_planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "warn",
            "planning_message_count": 81,
            "nonempty_trajectory_ratio": 0.61,
            "after_first_nonempty_trajectory_ratio": 1.0,
            "claim_window_nonempty_ratio": 1.0,
            "claim_window_source": "after_first_nonempty_trajectory",
            "route_establishment": {
                "route_established": True,
                "blocking_reasons": [],
            },
            "blocking_reasons": [],
            "warnings": [
                "planning_nonempty_after_routing_low_but_sustained_after_first_nonempty"
            ],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["route_establishment"]

    assert report["primary_blocker"] != "route_establishment:planning_trajectory_materialization_low"
    assert layer["status"] == "warn"
    assert layer["key_metrics"]["claim_window_nonempty_ratio"] == 1.0
    assert layer["key_metrics"]["claim_window_source"] == "after_first_nonempty_trajectory"
    assert "planning_trajectory_materialization_low" not in layer["blocking_reasons"]
    no_assist = report["layers"]["no_assist_claim_boundary"]
    assert no_assist["key_metrics"]["planning_materialization_claim_window_ratio"] == 1.0
    assert (
        no_assist["key_metrics"]["planning_materialization_claim_window_source"]
        == "after_first_nonempty_trajectory"
    )


def test_apollo_route_contract_mismatch_is_route_establishment_blocker(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "scenario_route_length_m": 230.0,
            "apollo_routing_total_length_m": 648.0,
            "routing_length_ratio": 2.817,
            "blocking_reasons": ["apollo_routing_length_mismatch"],
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["route_establishment"]
    assert layer["status"] == "fail"
    assert "apollo_routing_length_mismatch" in layer["blocking_reasons"]
    assert layer["key_metrics"]["apollo_routing_total_length_m"] == 648.0
    assert report["primary_blocker"] == "route_establishment:apollo_routing_length_mismatch"


def test_route_contract_mismatch_outranks_route_establishment_latency(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "pass",
            "planning_message_count": 583,
            "materialization_status": "observed_nonempty",
            "nonempty_trajectory_count": 485,
            "nonempty_trajectory_ratio": 485 / 583,
            "route_establishment": {
                "route_established": False,
                "blocking_reasons": [
                    "planning_nonempty_after_routing_missing",
                    "route_establishment_latency",
                ],
            },
            "blocking_reasons": [
                "planning_nonempty_after_routing_missing",
                "route_establishment_latency",
            ],
            "warnings": ["planning_time_domain_mixed_or_unverified"],
        },
    )
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "scenario_route_length_m": 229.2,
            "apollo_routing_total_length_m": 829.8,
            "routing_length_ratio": 3.62,
            "routing_phase": "long_goal",
            "blocking_reasons": [
                "apollo_routing_goal_mismatch",
                "apollo_routing_length_mismatch",
                "long_goal_not_compatible_with_scenario_route",
                "route_identity_inconsistent",
            ],
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["route_establishment"]
    assert layer["status"] == "fail"
    assert "route_establishment_latency" in layer["blocking_reasons"]
    assert "apollo_routing_goal_mismatch" in layer["blocking_reasons"]
    assert report["primary_blocker"] == "route_establishment:apollo_routing_goal_mismatch"


def test_reference_line_missing_after_routing_request_outranks_runtime_missing(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "blocking_reasons": [
                "routing_response_runtime_evidence_missing",
                "reference_line_missing_after_routing_request",
                "route_segments_failed_after_routing_request",
            ],
            "last_routing_request": {
                "available": True,
                "routing_phase": "long",
                "routing_request_kind": "long_phase_route",
                "goal_distance_m": 300.0,
                "goal_validity_snapshot": {
                    "available": True,
                    "reference_line_provider_status": "failed",
                    "create_route_segments_status": "failed",
                    "lane_follow_map_status": "reference_line_missing",
                },
            },
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["route_establishment"]
    assert "reference_line_missing_after_routing_request" in layer["blocking_reasons"]
    assert "routing_response_runtime_evidence_missing" in layer["blocking_reasons"]
    assert report["primary_blocker"] == "route_establishment:reference_line_missing_after_routing_request"


def test_goal_projection_failure_outranks_long_goal_route_boundary(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "blocking_reasons": [
                "apollo_routing_goal_projection_not_accepted",
                "apollo_routing_goal_snap_distance_high",
                "long_goal_not_compatible_with_scenario_route",
            ],
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert (
        report["primary_blocker"]
        == "route_establishment:apollo_routing_goal_projection_not_accepted"
    )


def test_claim_route_failure_does_not_mask_runtime_route_established(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "pass",
            "planning_message_count": 380,
            "materialization_status": "observed_nonempty",
            "nonempty_trajectory_count": 222,
            "nonempty_trajectory_ratio": 222 / 380,
            "route_establishment": {"route_established": True, "blocking_reasons": []},
            "blocking_reasons": [],
            "warnings": [],
        },
    )
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "blocking_reasons": [
                "apollo_routing_goal_snap_distance_high",
                "long_goal_not_compatible_with_scenario_route",
            ],
            "routing_response_decoded": {"available": True, "status": "pass"},
            "last_routing_response": {"available": True, "lane_window_count": 1},
            "latest_planning_active_route_segment": {
                "available": True,
                "create_route_segments_status": "ready",
                "route_segment_count": 1,
                "route_segment_total_length_m": 305.1,
            },
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["route_establishment"]
    assert layer["status"] == "warn"
    assert "apollo_routing_goal_snap_distance_high" not in layer["blocking_reasons"]
    assert "claim_route_contract_failed_but_runtime_route_established" in layer["warnings"]
    assert layer["key_metrics"]["runtime_route_established_from_route_contract"] is True
    assert layer["key_metrics"]["route_contract_blocking_reasons"] == [
        "apollo_routing_goal_snap_distance_high",
        "long_goal_not_compatible_with_scenario_route",
    ]


def test_goal_projection_disabled_by_config_outranks_generic_projection_failure(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "blocking_reasons": [
                "apollo_routing_goal_projection_disabled_by_config",
                "apollo_routing_goal_projection_not_accepted",
                "apollo_routing_goal_snap_distance_high",
                "long_goal_not_compatible_with_scenario_route",
            ],
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert (
        report["primary_blocker"]
        == "route_establishment:apollo_routing_goal_projection_disabled_by_config"
    )


def test_scenario_route_length_inconsistency_outranks_apollo_goal_mismatch(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "fail",
            "scenario_route_length_m": 229.2,
            "scenario_route_length_source": "declared_metadata",
            "scenario_route_declared_length_m": 229.2,
            "scenario_route_claim_length_m": None,
            "scenario_route_legacy_length_m": 229.2,
            "scenario_route_legacy_length_role": "legacy_selection_straight_line_distance",
            "scenario_route_trace_length_m": 388.4,
            "scenario_route_length_consistency_status": "inconsistent",
            "scenario_route_length_consistency_reason": "declared_route_length_disagrees_with_route_trace",
            "apollo_routing_total_length_m": 829.8,
            "routing_length_ratio": 3.62,
            "blocking_reasons": [
                "scenario_route_length_inconsistent",
                "apollo_routing_goal_mismatch",
                "apollo_routing_length_mismatch",
                "route_identity_inconsistent",
            ],
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["route_establishment"]
    assert "scenario_route_length_inconsistent" in layer["blocking_reasons"]
    assert layer["key_metrics"]["scenario_route_length_consistency_status"] == "inconsistent"
    assert layer["key_metrics"]["scenario_route_trace_length_m"] == 388.4
    assert layer["key_metrics"]["scenario_route_legacy_length_m"] == 229.2
    assert layer["key_metrics"]["scenario_route_legacy_length_role"] == "legacy_selection_straight_line_distance"
    assert report["primary_blocker"] == "route_establishment:scenario_route_length_inconsistent"


def test_route_contract_insufficient_outranks_downstream_module_consumption_fail(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "insufficient_data",
            "missing_fields": ["apollo_hdmap_projection_for_lane_equivalence"],
            "blocking_reasons": [],
            "warnings": ["apollo_hdmap_projection_required_for_cross_namespace_lane_equivalence"],
        }
    )
    _write_json(route_contract_path, route_contract)
    _write_json(
        run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
        {
            "schema_version": "apollo_module_consumption.v1",
            "status": "fail",
            "blocking_reasons": [
                "claim_route_consumption_unverified",
                "route_contract_unverified_before_module_consumption_claim",
            ],
            "warnings": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    route_layer = report["layers"]["route_establishment"]
    assert route_layer["status"] == "insufficient_data"
    assert route_layer["key_metrics"]["route_contract_missing_fields"] == [
        "apollo_hdmap_projection_for_lane_equivalence"
    ]
    assert report["primary_blocker"] == "route_establishment:apollo_hdmap_projection_for_lane_equivalence"
    assert "apollo_module_consumption:claim_route_consumption_unverified" in report["secondary_blockers"]


def test_hdmap_projection_empty_reason_reaches_link_health(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        {
            "schema_version": "apollo_hdmap_projection_report.v1",
            "status": "insufficient_data",
            "claim_grade": False,
            "projection": {
                "file_present": True,
                "artifact_status": "artifact_empty",
                "empty_reason": "apollo_hdmap_projection_artifact_empty_no_exported_rows",
                "next_action": "Run tools/export_apollo_hdmap_projection.py.",
                "official_source_available": False,
                "claim_grade": False,
                "warnings": ["apollo_hdmap_projection_empty"],
                "insufficient_reasons": [],
                "missing_fields": ["apollo_hdmap_projection_rows"],
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["hdmap_projection"]
    assert layer["status"] == "insufficient_data"
    assert (
        layer["key_metrics"]["empty_reason"]
        == "apollo_hdmap_projection_artifact_empty_no_exported_rows"
    )
    assert layer["next_action"] == "Run tools/export_apollo_hdmap_projection.py."


def test_hdmap_runtime_unavailable_outranks_prediction_claim_boundary(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        {
            "schema_version": "apollo_hdmap_projection_report.v1",
            "status": "insufficient_data",
            "claim_grade": False,
            "projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "insufficient_data",
                "claim_grade": False,
                "status_counts": {"environment_unavailable": 60},
                "environment_unavailable_count": 60,
                "blocking_reasons": [],
                "insufficient_reasons": [
                    "apollo_hdmap_projection_route_s_coverage_missing",
                    "apollo_hdmap_projection_runtime_unavailable",
                    "apollo_hdmap_projection_sample_count_low",
                ],
                "missing_fields": ["apollo_map_xysl_runtime", "projection_s"],
                "warnings": ["apollo_hdmap_projection_environment_unavailable"],
            },
            "blocking_reasons": [],
            "insufficient_reasons": [
                "apollo_hdmap_projection_route_s_coverage_missing",
                "apollo_hdmap_projection_runtime_unavailable",
                "apollo_hdmap_projection_sample_count_low",
            ],
            "missing_fields": ["apollo_map_xysl_runtime", "projection_s"],
            "warnings": ["apollo_hdmap_projection_environment_unavailable"],
        },
    )
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract.v1",
            "status": "insufficient_data",
            "blocking_reasons": [],
            "warnings": ["apollo_hdmap_projection_runtime_unavailable"],
            "evidence": {"planning_reference_available": True},
            "apollo_hdmap_projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "insufficient_data",
                "claim_grade": False,
                "insufficient_reasons": ["apollo_hdmap_projection_runtime_unavailable"],
                "blocking_reasons": [],
                "warnings": ["apollo_hdmap_projection_environment_unavailable"],
            },
        },
    )
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "scenario_class": "lane_keep",
            "prediction_mode": "missing",
            "prediction_channel_available": False,
            "prediction_message_count": 0,
            "planning_requires_prediction": True,
            "hard_gate_eligible": False,
            "blocking_capabilities": ["closed_loop"],
            "warnings": [],
            "verdict": "fail",
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "hdmap_projection:apollo_hdmap_projection_runtime_unavailable"
    assert "prediction_evidence:closed_loop" in report["secondary_blockers"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_localization_gap_outranks_prediction_claim_boundary_after_map_evidence_passes(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    loc_path = run_dir / "analysis/localization_contract/localization_contract_report.json"
    loc = json.loads(loc_path.read_text(encoding="utf-8"))
    loc["verdict"] = {
        "status": "insufficient_data",
        "blocking_reasons": [],
    }
    loc["missing_fields"] = ["vehicle_reference_hard_gate_eligible"]
    _write_json(loc_path, loc)
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "scenario_class": None,
            "prediction_mode": "bypassed_with_gt_obstacles",
            "prediction_channel_available": False,
            "prediction_message_count": None,
            "planning_requires_prediction": True,
            "hard_gate_eligible": False,
            "blocking_capabilities": ["closed_loop"],
            "warnings": ["prediction_bypass_not_allowed_for_scenario"],
            "verdict": "fail",
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "localization_gt_contract:insufficient_data"
    assert "prediction_evidence:closed_loop" in report["secondary_blockers"]
    assert "prediction_evidence:closed_loop" in report["why_not_claimable"]
    assert report["can_claim_unassisted_natural_driving"] is False


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


def test_bridge_runtime_uses_raw_materialization_when_summary_flags_are_stale(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["routing_materialized"] = False
    summary["planning_materialized"] = False
    summary["routing_success_count"] = 1
    summary["planning_message_count"] = 12
    _write_json(summary_path, summary)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "lane_window_signature": "15_1_1@0.0->10.0",
            "total_length_m": 10.0,
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 12,
            "messages_with_nonzero_trajectory_points": 10,
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    bridge = report["layers"]["bridge_runtime"]
    assert bridge["status"] == "warn"
    assert "bridge_materialization_missing" not in bridge["blocking_reasons"]
    assert bridge["key_metrics"]["routing_raw_materialized"] is True
    assert bridge["key_metrics"]["planning_raw_materialized"] is True
    assert "summary_routing_materialized_false_overridden_by_raw_artifacts" in bridge["warnings"]
    assert "summary_planning_materialized_false_overridden_by_raw_artifacts" in bridge["warnings"]
    assert report["primary_blocker"] != "bridge_runtime:bridge_materialization_missing"


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


def test_link_health_prefers_latest_postprocess_natural_driving_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    direct_report = run_dir / "analysis/natural_driving/natural_driving_report.json"
    postprocess_report = run_dir / "analysis/natural_driving_postprocess/natural_driving_report.json"
    _write_json(
        postprocess_report,
        {
            "schema_version": "town01_natural_driving_report.v1",
            "verdict": {"status": "insufficient_data"},
            "summary": {"insufficient_data_count": 1},
            "run_results": [
                {
                    "run_id": "run",
                    "verdict": "insufficient_data",
                    "failure_reason": "localization_contract_blocking",
                    "can_claim_unassisted_natural_driving": False,
                }
            ],
        },
    )
    os.utime(direct_report, (10, 10))
    os.utime(postprocess_report, (20, 20))

    report = analyze_apollo_link_health_run_dir(run_dir)

    natural = report["layers"]["natural_driving_outcome"]
    assert natural["status"] == "insufficient_data"
    assert natural["artifact_paths"]["natural_driving_report"] == str(postprocess_report)


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


def test_link_health_surfaces_hdmap_route_lateral_drift_attribution(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    loc_path = run_dir / "analysis/localization_contract/localization_contract_report.json"
    loc = json.loads(loc_path.read_text(encoding="utf-8"))
    loc["verdict"] = {"status": "fail", "blocking_reasons": ["apollo_hdmap_projection_lateral_error_high"]}
    loc["apollo_hdmap_projection"]["status"] = "fail"
    loc["apollo_hdmap_projection"]["claim_grade"] = False
    loc["apollo_hdmap_projection"]["lateral_error_p95_m"] = 0.82
    loc["hdmap_route_lateral_consistency"] = {
        "status": "pass",
        "alignment_mode": "opposite_sign",
        "best_abs_delta_p95_m": 0.015,
        "interpretation": "hdmap_lateral_matches_route_cross_track_actual_lateral_drift",
    }
    _write_json(loc_path, loc)
    _write_json(
        run_dir / "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
        {
            "schema_version": "apollo_lateral_semantics.v1",
            "verdict": {
                "status": "warn",
                "failure_reason": "lateral_semantics_anomaly",
                "suspected_layer": "target_point_semantics",
                "confidence": "high",
            },
            "suspected_layer": "target_point_semantics",
            "confidence": "high",
            "anomalies": [
                {
                    "type": "actual_lateral_drift_matches_hdmap_projection",
                    "suspected_layer": "target_point_semantics",
                    "reason": "Apollo HDMap projection_l matches CARLA route cross-track while lateral error is high",
                },
                {
                    "type": "high_lateral_drift_with_low_source_steer",
                    "suspected_layer": "target_point_semantics",
                    "reason": "source steer remains low while cross-track error grows",
                },
                {
                    "type": "route_lateral_high_but_simple_lat_and_source_steer_near_zero",
                    "suspected_layer": "target_point_semantics",
                    "reason": "Apollo simple_lat lateral error remains near zero while route lateral error grows",
                },
                {
                    "type": "simple_lat_station_frame_not_route_s_aligned",
                    "suspected_layer": "target_point_semantics",
                    "reason": "Apollo simple_lon station differs materially from route_s",
                },
                {
                    "type": "matched_point_tracks_ego_not_route_centerline",
                    "suspected_layer": "target_point_semantics",
                    "reason": "Apollo matched point is near ego while route centerline is far from matched point",
                },
            ],
            "correlation_summary": {
                "cross_track_error_abs": {"p95": 0.82},
                "apollo_simple_lat_lateral_error_abs": {"p95": 0.002},
                "route_s_vs_apollo_current_station_abs_delta": {"p95": 148.0},
                "ego_to_apollo_matched_point_xy_distance": {"p95": 0.02},
                "route_to_apollo_matched_point_xy_distance": {"p95": 0.82},
                "apollo_steer_raw_abs": {"p95": 0.0},
                "carla_steer_applied_abs": {"p95": 0.001},
            },
            "hdmap_route_lateral_consistency": {
                "status": "pass",
                "alignment_mode": "opposite_sign",
                "best_abs_delta_p95_m": 0.015,
                "interpretation": "hdmap_lateral_matches_route_cross_track_actual_lateral_drift",
            },
            "reference_debug_summary": {
                "available": True,
                "status": "fail",
                "nonempty_trajectory_ratio": 0.91,
                "reference_line_provider_ready_ratio": 0.0,
                "reference_line_count_zero_ratio": 1.0,
                "routing_segment_count_zero_ratio": 0.08,
                "nonempty_planning_with_reference_debug_missing": True,
                "warnings": [
                    "reference_line_count_zero_debug_counter_with_nonempty_trajectory",
                    "reference_line_provider_ready_ratio_low",
                ],
            },
            "drift_window_summary": {
                "status": "available",
                "first_high_lateral": {
                    "sim_time": 86.2,
                    "route_s": 109.8,
                    "cross_track_error": -0.50,
                    "heading_error": 0.008,
                },
                "max_abs_lateral": {
                    "sim_time": 103.4,
                    "route_s": 149.0,
                    "cross_track_error": -0.84,
                    "heading_error": 0.010,
                },
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["localization_gt_contract"]
    lateral = report["layers"]["apollo_lateral_semantics"]

    assert report["primary_blocker"] == "natural_driving_outcome:actual_lateral_drift_matches_hdmap_projection"
    assert "localization_gt_contract:apollo_hdmap_projection_lateral_error_high" in report["secondary_blockers"]
    assert layer["key_metrics"]["hdmap_route_lateral_consistency_status"] == "pass"
    assert layer["key_metrics"]["hdmap_route_lateral_interpretation"] == (
        "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"
    )
    assert "raw/mapped/applied steering" in layer["next_action"]
    assert lateral["status"] == "warn"
    assert lateral["key_metrics"]["suspected_layer"] == "target_point_semantics"
    assert lateral["key_metrics"]["confidence"] == "high"
    assert lateral["key_metrics"]["anomaly_types"] == [
        "actual_lateral_drift_matches_hdmap_projection",
        "high_lateral_drift_with_low_source_steer",
        "route_lateral_high_but_simple_lat_and_source_steer_near_zero",
        "simple_lat_station_frame_not_route_s_aligned",
        "matched_point_tracks_ego_not_route_centerline",
    ]
    assert lateral["key_metrics"]["first_high_lateral_route_s"] == 109.8
    assert lateral["key_metrics"]["max_abs_lateral_route_s"] == 149.0
    assert lateral["key_metrics"]["apollo_simple_lat_lateral_error_abs_p95"] == 0.002
    assert lateral["key_metrics"]["route_s_vs_apollo_current_station_abs_delta_p95"] == 148.0
    assert lateral["key_metrics"]["ego_to_apollo_matched_point_xy_distance_p95"] == 0.02
    assert lateral["key_metrics"]["route_to_apollo_matched_point_xy_distance_p95"] == 0.82
    assert lateral["key_metrics"]["reference_debug_status"] == "fail"
    assert lateral["key_metrics"]["reference_line_provider_ready_ratio"] == 0.0
    assert lateral["key_metrics"]["reference_line_count_zero_ratio"] == 1.0
    assert "target/matched point semantics" in lateral["next_action"]
    assert "real closed-loop lateral drift" in report["next_highest_value_validation"]


def test_lateral_semantics_warn_outranks_missing_natural_driving_report_for_phase1_triage(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/natural_driving/natural_driving_report.json").unlink()
    _write_json(
        run_dir / "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
        {
            "schema_version": "apollo_lateral_semantics.v1",
            "verdict": {
                "status": "warn",
                "failure_reason": "lateral_semantics_anomaly",
                "suspected_layer": "reference_line_semantics",
                "confidence": "medium",
            },
            "suspected_layer": "reference_line_semantics",
            "confidence": "medium",
            "anomalies": [
                {
                    "type": "route_simple_lat_sign_convention_mismatch_candidate",
                    "suspected_layer": "reference_line_semantics",
                    "reason": "route lateral and simple_lat lateral have opposite signs with matching magnitudes",
                },
                {
                    "type": "route_lateral_error_opposes_simple_lat_lateral_error",
                    "suspected_layer": "reference_line_semantics",
                    "reason": "route lateral and simple_lat lateral have opposite signs",
                },
                {
                    "type": "planning_nonempty_but_reference_line_debug_missing",
                    "suspected_layer": "reference_line_semantics",
                    "reason": "Planning trajectories are non-empty while reference-line debug is missing",
                }
            ],
            "correlation_summary": {
                "cross_track_error_abs": {"p95": 0.20},
                "apollo_steer_raw_abs": {"p95": 0.28},
                "carla_steer_applied_abs": {"p95": 0.04},
            },
            "reference_debug_summary": {
                "status": "warn",
                "reference_line_provider_ready_ratio": 0.0,
                "reference_line_count_zero_ratio": 1.0,
                "debug_gap_classification": "planning_reference_line_debug_export_gap",
                "control_simple_lat_reference_available": True,
                "control_reference_join_coverage_ratio": 0.95,
            },
            "lateral_sign_alignment": {
                "status": "available",
                "first_high_lateral_sample": {
                    "source_steer_vs_route_lateral_error": "same_sign",
                    "applied_steer_vs_route_lateral_error": "same_sign",
                    "route_lateral_error_vs_simple_lat_lateral_error": "opposite_sign",
                },
                "source_steer_vs_route_lateral_error": {"same_sign_ratio": 0.55},
                "source_steer_vs_simple_lat_lateral_error": {"same_sign_ratio": 0.0},
                "route_lateral_error_vs_simple_lat_lateral_error": {"opposite_sign_ratio": 1.0},
                "route_lateral_provenance": {
                    "evidence_level": "hdmap_projection_consistency",
                    "interpretation": "route_lateral_sign_supported_by_hdmap_projection_consistency",
                    "route_lateral_source_field": "cross_track_error",
                    "route_geometry_sample_count": 0,
                    "route_geometry_recomputed_cte_abs_delta_p95_m": None,
                    "route_definition_geometry_status": "stub_or_insufficient",
                },
                "route_simple_lat_magnitude_alignment": {
                    "magnitude_agreement_candidate": True,
                    "opposite_sign_abs_sum_p95_m": 0.02,
                    "abs_magnitude_delta_p95_m": 0.02,
                    "interpretation": "opposite_sign_matching_magnitude_suggests_route_simple_lat_sign_convention_mismatch",
                },
                "official_hdmap_projection_alignment": {
                    "matched_sample_count": 13,
                    "route_lateral_vs_projection_lateral": {
                        "opposite_sign_ratio": 1.0,
                        "abs_magnitude_delta_p95_m": 0.195,
                    },
                    "simple_lat_vs_projection_lateral": {
                        "same_sign_ratio": 1.0,
                    },
                    "simple_lat_points_vs_projection_line": {
                        "matched_point_lateral_abs_p95_m": 0.000001,
                        "matched_point_sample_count": 13,
                        "current_reference_point_lateral_abs_p95_m": None,
                        "current_reference_point_sample_count": 0,
                        "target_point_lateral_abs_p95_m": 0.000002,
                        "target_point_sample_count": 13,
                        "point_coverage_status": "matched_and_target_available_current_reference_missing",
                        "missing_point_fields": ["apollo_current_reference_point_x_y"],
                    },
                    "simple_lat_station_vs_projection_s": {
                        "station_coverage_status": "projection_current_matched_target_s_available",
                        "missing_station_fields": [],
                        "current_station_minus_projection_s_abs_p95_m": 25.08,
                        "matched_s_minus_projection_s_abs_p95_m": 23.64,
                        "target_s_minus_projection_s_abs_p95_m": 25.12,
                        "target_s_minus_current_station_abs_p95_m": 0.043,
                    },
                },
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == (
        "apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate"
    )
    assert "natural_driving_outcome:insufficient_data" in report["secondary_blockers"]
    lateral = report["layers"]["apollo_lateral_semantics"]
    assert lateral["key_metrics"]["reference_debug_classification"] == "planning_reference_line_debug_export_gap"
    assert lateral["key_metrics"]["control_simple_lat_reference_available"] is True
    assert lateral["key_metrics"]["control_reference_join_coverage_ratio"] == 0.95
    assert lateral["key_metrics"]["first_high_source_steer_vs_route_lateral_error"] == "same_sign"
    assert lateral["key_metrics"]["first_high_applied_steer_vs_route_lateral_error"] == "same_sign"
    assert lateral["key_metrics"]["source_steer_route_lateral_same_sign_ratio"] == 0.55
    assert lateral["key_metrics"]["route_lateral_simple_lat_opposite_sign_ratio"] == 1.0
    assert lateral["key_metrics"]["route_lateral_provenance_evidence_level"] == "hdmap_projection_consistency"
    assert lateral["key_metrics"]["route_lateral_provenance_interpretation"] == (
        "route_lateral_sign_supported_by_hdmap_projection_consistency"
    )
    assert lateral["key_metrics"]["route_lateral_source_field"] == "cross_track_error"
    assert lateral["key_metrics"]["route_geometry_sample_count"] == 0
    assert lateral["key_metrics"]["route_definition_geometry_status"] == "stub_or_insufficient"
    assert lateral["key_metrics"]["official_hdmap_projection_matched_sample_count"] == 13
    assert lateral["key_metrics"]["route_lateral_projection_lateral_opposite_sign_ratio"] == 1.0
    assert lateral["key_metrics"]["simple_lat_projection_lateral_same_sign_ratio"] == 1.0
    assert lateral["key_metrics"]["route_projection_abs_magnitude_delta_p95_m"] == 0.195
    assert lateral["key_metrics"]["simple_lat_matched_point_projection_line_lateral_abs_p95_m"] == 0.000001
    assert lateral["key_metrics"]["simple_lat_matched_point_projection_line_sample_count"] == 13
    assert lateral["key_metrics"]["simple_lat_current_reference_point_projection_line_lateral_abs_p95_m"] is None
    assert lateral["key_metrics"]["simple_lat_current_reference_point_projection_line_sample_count"] == 0
    assert lateral["key_metrics"]["simple_lat_target_point_projection_line_lateral_abs_p95_m"] == 0.000002
    assert lateral["key_metrics"]["simple_lat_target_point_projection_line_sample_count"] == 13
    assert lateral["key_metrics"]["simple_lat_point_coverage_status"] == (
        "matched_and_target_available_current_reference_missing"
    )
    assert lateral["key_metrics"]["simple_lat_missing_point_fields"] == [
        "apollo_current_reference_point_x_y"
    ]
    assert lateral["key_metrics"]["simple_lat_station_coverage_status"] == (
        "projection_current_matched_target_s_available"
    )
    assert lateral["key_metrics"]["simple_lat_missing_station_fields"] == []
    assert lateral["key_metrics"]["simple_lat_current_station_projection_s_delta_p95_m"] == 25.08
    assert lateral["key_metrics"]["simple_lat_matched_s_projection_s_delta_p95_m"] == 23.64
    assert lateral["key_metrics"]["simple_lat_target_s_projection_s_delta_p95_m"] == 25.12
    assert lateral["key_metrics"]["simple_lat_target_s_current_station_delta_p95_m"] == 0.043
    assert lateral["key_metrics"]["route_simple_lat_sign_convention_candidate"] is True
    assert lateral["key_metrics"]["route_simple_lat_opposite_sign_abs_sum_p95_m"] == 0.02
    assert lateral["key_metrics"]["route_simple_lat_abs_magnitude_delta_p95_m"] == 0.02
    assert lateral["key_metrics"]["route_simple_lat_alignment_interpretation"] == (
        "opposite_sign_matching_magnitude_suggests_route_simple_lat_sign_convention_mismatch"
    )
    assert lateral["key_metrics"]["first_high_route_lateral_vs_simple_lat_lateral_error"] == "opposite_sign"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_control_process_crash_is_explicit_handoff_blocker(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    handoff_path = run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["verdict"] = "fail"
    handoff["failure_stage"] = "process_health"
    handoff["blocking_reasons"] = ["process_health_failed"]
    handoff["process_health"] = {
        "status": "fail",
        "crash_detected": True,
        "crash_reason": "tcmalloc_invalid_free",
    }
    _write_json(handoff_path, handoff)

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["routing_planning_control_handoff"]

    assert "control_process_crash_before_control_output" in layer["blocking_reasons"]
    assert layer["key_metrics"]["process_health"]["crash_reason"] == "tcmalloc_invalid_free"
    assert (
        report["primary_blocker"]
        == "routing_planning_control_handoff:control_process_crash_before_control_output"
    )


def test_control_process_failure_outranks_control_reference_gap(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["status"] = "insufficient_data"
    ref["warnings"] = [
        "apollo_route_contract_warn_reference_line_claim_warning_propagated",
        "control_reference.control_reference_debug",
    ]
    ref.setdefault("contracts", {})
    ref["contracts"]["apollo_hdmap_projection"] = {"status": "pass", "blocking_reasons": []}
    ref["contracts"]["planning_trajectory"] = {"status": "pass", "blocking_reasons": []}
    ref["contracts"]["control_reference"] = {
        "status": "insufficient_data",
        "missing_fields": ["control_reference_debug"],
    }
    _write_json(ref_path, ref)

    handoff_path = run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["verdict"] = "fail"
    handoff["failure_stage"] = "process_health"
    handoff["blocking_reasons"] = ["process_health_failed"]
    handoff["process_health"] = {
        "status": "fail",
        "crash_detected": True,
        "crash_reason": "process exited before control output",
    }
    handoff["bridge_receive"] = {"control_rx_count": 0}
    handoff["control_channel"] = {"message_count": 0}
    _write_json(handoff_path, handoff)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert (
        report["primary_blocker"]
        == "routing_planning_control_handoff:control_process_crash_before_control_output"
    )
    assert "planning_reference_line:insufficient_data" in report["secondary_blockers"]


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
        "transition_window_summary": {
            "available": True,
            "dominant_transition_mode": "planning_sequence_update",
            "same_planning_sequence_ratio": 0.25,
            "planning_sequence_changed_ratio": 0.75,
        },
        "dominant_suspected_factor": "planning_trajectory_length_switching",
    }
    control["metrics"]["oscillation_decomposition"] = {
        "dominant_oscillation_layer": "apollo_raw_command",
        "layers": {
            "apollo_raw_command": {
                "status": "fail",
                "reason": "throttle_brake_switching",
                "throttle_brake_switch_count": 12,
            },
            "bridge_mapped_command": {"status": "fail"},
            "carla_applied_command": {"status": "fail"},
            "bridge_apply_cadence": {
                "status": "pass",
                "observed_apply_world_frame_hz": 19.8,
                "configured_apply_hz": 20.0,
                "same_frame_drop_ratio": 0.05,
                "same_frame_drop_expected_from_sync_tick": True,
            },
        },
    }
    control["metrics"]["planning_log_fallback_diagnostics"] = {
        "matched_point_lon_diff_replan_count": 4,
        "piecewise_jerk_speed_optimizer_fail_count": 2,
        "dominant_suspected_factor": "trajectory_stitcher_matched_point_lon_diff_replans",
    }
    control["metrics"]["gt_state_sampling_cadence"] = {
        "available": True,
        "carla_tick_wall_hz": 2.0,
        "carla_tick_count": 100,
        "carla_inter_tick_wall_interval_p95_s": 0.5,
        "control_to_chassis_count_ratio": 10.0,
        "control_to_localization_count_ratio": 10.0,
        "control_rx_wall_hz": 20.0,
        "localization_wall_hz": 2.0,
        "chassis_wall_hz": 2.0,
        "interpretation": "control oversamples GT state in wall time",
    }
    control["metrics"]["control_mapping_claim_boundary"] = {
        "claim_grade_control_mapping": False,
        "actuator_mapping_mode": "legacy",
    }
    _write_json(control_path, control)
    tick_summary_path = run_dir / "artifacts/carla_tick_health_summary.json"
    _write_json(
        tick_summary_path,
        {
            "schema_version": "carla_tick_health.v1",
            "tick_count": 100,
            "tick_fail_count": 0,
            "inter_tick_wall_interval_p95_s": 0.5,
            "max_inter_tick_wall_interval_s": 0.75,
            "inter_tick_wall_interval_count": 99,
            "max_stage_name": "sensor_capture",
            "max_stage_duration_s": 0.42,
            "stage_duration_max_s": {
                "sensor_capture": 0.42,
                "hook.after_world_tick.tick_callback_0": 0.05,
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "control_mapping_apply:applied_actuation_oscillation"
    detail = report["primary_blocker_detail"]
    assert detail["layer"] == "control_mapping_apply"
    assert detail["reason"] == "applied_actuation_oscillation"
    assert detail["dominant_oscillation_layer"] == "apollo_raw_command"
    assert detail["primary_suspected_layer"] == "planning_control_semantics"
    assert detail["raw_throttle_brake_switch_count"] == 12
    assert detail["claim_grade_control_mapping"] is False
    cadence = detail["gt_state_sampling_cadence"]
    assert cadence["available"] is True
    assert cadence["tick_health_artifact_available"] is True
    assert cadence["tick_health_summary_path"] == str(tick_summary_path)
    assert cadence["carla_tick_cadence_source"] == "carla_tick_health_summary"
    assert cadence["carla_tick_wall_hz"] == 2.0
    assert cadence["control_rx_wall_hz"] == 20.0
    assert cadence["control_to_chassis_count_ratio"] == 10.0
    assert cadence["control_to_localization_count_ratio"] == 10.0
    assert cadence["gt_state_oversampling_present"] is True
    assert cadence["slowest_frame_stage_name"] == "sensor_capture"
    assert cadence["slowest_hook_stage_name"] == "hook.after_world_tick.tick_callback_0"
    assert "recording/sensor capture" in cadence["next_online_validation"]
    assert "planning_trajectory_length_switching" in detail["control_semantics_suspected_factors"]
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
    assert metrics["control_semantics_primary_factor"] == "planning_trajectory_length_switching"
    assert "planning_trajectory_length_switching" in metrics["control_semantics_suspected_factors"]
    diagnosis = metrics["control_oscillation_diagnosis"]
    assert diagnosis["primary_suspected_layer"] == "planning_control_semantics"
    assert diagnosis["raw_command_oscillation_present"] is True
    assert diagnosis["gt_state_oversampling_present"] is True
    assert diagnosis["control_to_chassis_count_ratio"] == 10.0
    assert diagnosis["transition_window_dominant_mode"] == "planning_sequence_update"
    assert diagnosis["planning_sequence_changed_transition_ratio"] == 0.75
    assert "control_mapping_claim_boundary" in diagnosis["suspected_layers"]
    assert (
        metrics["control_semantics_evidence"]["dominant_by_source"][
            "longitudinal_oscillation_attribution"
        ]
        == "apollo_simple_lon_acceleration_cmd_sign_switching"
    )
    compact = metrics["control_oscillation_compact"]
    assert compact["dominant_oscillation_layer"] == "apollo_raw_command"
    assert compact["primary_suspected_layer"] == "planning_control_semantics"
    assert compact["apollo_raw_throttle_brake_switch_count"] == 12
    assert compact["bridge_apply_cadence_status"] == "pass"
    assert compact["bridge_apply_same_frame_drop_expected_from_sync_tick"] is True
    assert compact["transition_window_dominant_mode"] == "planning_sequence_update"
    assert compact["planning_sequence_changed_ratio"] == 0.75
    next_action = report["layers"]["control_mapping_apply"]["next_action"]
    assert (
        "planning_trajectory_length_switching" in next_action
        or "control_trajectory_consume_debug" in next_action
    )
    summary = apollo_link_health_summary_md(report)
    assert "Primary blocker detail" in summary
    assert "control_oscillation_compact" in summary
    assert "planning_control_semantics" in summary
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
    layer = report["layers"]["no_assist_claim_boundary"]
    assert "Planning availability" in layer["next_action"]
    assert "blocking assists" not in layer["next_action"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_planning_nonempty_filtered_window_is_diagnostic_only(
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

    assert no_assist["status"] == "fail"
    assert "planning_nonempty_ratio_not_claim_grade" in no_assist["blocking_reasons"]
    assert no_assist["key_metrics"]["planning_nonempty_ratio"] == 0.41
    assert no_assist["key_metrics"]["planning_nonempty_ratio_for_claim"] == 0.41
    assert no_assist["key_metrics"]["planning_nonempty_ratio_overall"] == 0.41
    assert no_assist["key_metrics"]["planning_nonempty_ratio_filtered_after_routing_segment_available"] == 0.95
    assert no_assist["key_metrics"]["planning_nonempty_ratio_source"] == "summary_or_control_handoff"
    assert report["primary_blocker"] == "no_assist_claim_boundary:planning_nonempty_ratio_not_claim_grade"
    assert report["can_claim_unassisted_natural_driving"] is False


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


def test_explicit_assist_ledger_report_overrides_stale_runtime_ledger(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    stale_ledger = {
        "schema_version": "assist_ledger.v1",
        "active_assists": ["legacy_followstop"],
        "blocking_assists": ["legacy_followstop"],
        "non_blocking_assists": [],
        "assist_confidence": "explicit",
        "source_artifact": "config",
        "can_claim_unassisted_natural_driving": False,
    }
    for name in ("summary.json", "manifest.json"):
        path = run_dir / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["assist_ledger"] = stale_ledger
        _write_json(path, payload)
    explicit_clean_ledger = {
        "schema_version": "assist_ledger.v1",
        "active_assists": [],
        "blocking_assists": [],
        "non_blocking_assists": [],
        "assist_sources": {},
        "assist_confidence": "explicit",
        "source_artifact": "analysis",
        "can_claim_unassisted_natural_driving": True,
        "warnings": [],
    }
    _write_json(run_dir / "analysis/assist_ledger/assist_ledger.json", explicit_clean_ledger)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "pass"
    assert layer["key_metrics"]["active_assists"] == []
    assert layer["key_metrics"]["blocking_assists"] == []
    assert layer["key_metrics"]["assist_confidence"] == "explicit"
    assert "blocking_assists_active" not in layer["blocking_reasons"]
    assert layer["artifact_paths"]["assist_ledger"].endswith(
        "analysis/assist_ledger/assist_ledger.json"
    )
    assert report["can_claim_unassisted_natural_driving"] is True


def test_stale_runtime_ledger_still_blocks_when_explicit_report_missing(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    stale_ledger = {
        "schema_version": "assist_ledger.v1",
        "active_assists": ["legacy_followstop"],
        "blocking_assists": ["legacy_followstop"],
        "non_blocking_assists": [],
        "assist_confidence": "explicit",
        "source_artifact": "config",
        "can_claim_unassisted_natural_driving": False,
    }
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["assist_ledger"] = stale_ledger
    _write_json(summary_path, summary)

    report = analyze_apollo_link_health_run_dir(run_dir)

    layer = report["layers"]["no_assist_claim_boundary"]
    assert layer["status"] == "fail"
    assert layer["key_metrics"]["active_assists"] == ["legacy_followstop"]
    assert layer["key_metrics"]["blocking_assists"] == ["legacy_followstop"]
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


def test_channel_health_uses_channel_stats_when_report_is_placeholder(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "insufficient_data",
            "warnings": ["compat_runtime_did_not_sample_cyber_channels"],
        },
    )
    common = {
        "message_count": 100,
        "hz": 20.0,
        "max_gap_ms": 50.0,
        "timestamp_monotonic": True,
        "sequence_monotonic": True,
        "stale_count": 0,
    }
    _write_json(
        run_dir / "channel_stats.json",
        {
            "schema_version": "channel_stats.v1",
            "channels": {
                "/apollo/localization/pose": dict(common),
                "/apollo/canbus/chassis": dict(common),
                "/apollo/perception/obstacles": dict(common),
                "/apollo/planning": dict(common),
                "/apollo/control": dict(common),
                "/apollo/perception/traffic_light": {
                    "message_count": 0,
                    "hz": 0.0,
                    "max_gap_ms": None,
                    "timestamp_monotonic": False,
                    "sequence_monotonic": False,
                    "stale_count": None,
                },
            },
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["channel_health"]

    assert layer["status"] == "warn"
    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_channel_stats"
    assert layer["artifact_paths"]["channel_stats"].endswith("channel_stats.json")
    assert layer["key_metrics"]["planning_message_count"] == 100
    assert layer["warnings"] == ["traffic_light_has_no_messages_optional_for_lane_keep"]
    assert report["primary_blocker"] != "channel_health:insufficient_data"


def test_chassis_gt_contract_uses_run_artifacts_when_report_is_placeholder(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
        {
            "schema_version": "chassis_gt_contract_report.v1",
            "status": "insufficient_data",
            "blocking_reasons": ["chassis_runtime_samples_missing"],
        },
    )
    _write_json(
        run_dir / "channel_stats.json",
        {
            "schema_version": "channel_stats.v1",
            "channels": {
                "/apollo/canbus/chassis": {
                    "message_count": 100,
                    "hz": 20.0,
                    "max_gap_ms": 50.0,
                    "timestamp_monotonic": True,
                    "sequence_monotonic": True,
                    "stale_count": 0,
                }
            },
        },
    )
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (run_dir / "artifacts/debug_timeseries.csv").write_text(
        "sim_time,chassis_speed_mps,localization_speed_mps\n"
        "0.00,4.0,4.0\n"
        "0.05,4.5,4.5\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["chassis_gt_contract"]

    assert layer["status"] == "warn"
    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert layer["key_metrics"]["channel"]["message_count"] == 100
    assert layer["key_metrics"]["speed_consistency"]["sample_count"] == 2
    assert layer["key_metrics"]["claim_grade"] is False
    assert "driving_mode" in layer["key_metrics"]["missing_fields"]
    assert "chassis_gt_contract:claim_grade_false" in report["why_not_claimable"]
    assert report["can_claim_unassisted_natural_driving"] is False


def test_localization_contract_uses_run_artifacts_when_report_is_placeholder(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "verdict": {
                "status": "insufficient_data",
                "blocking_reasons": ["localization_runtime_samples_missing"],
            },
            "channel": {"status": "insufficient_data", "message_count": 0},
        },
    )
    _write_json(
        run_dir / "channel_stats.json",
        {
            "schema_version": "channel_stats.v1",
            "channels": {
                "/apollo/localization/pose": {
                    "message_count": 3,
                    "hz": 20.0,
                    "max_gap_ms": 50.0,
                    "timestamp_monotonic": True,
                    "sequence_monotonic": True,
                    "stale_count": 0,
                }
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
            "localization": {
                "reference_mode": "rear_axle",
                "back_offset_m": 1.4235,
                "expected_rear_axle_back_offset_m": 1.4235,
            },
            "last_pose_debug": {
                "localization_frame_id": "map",
                "localization_reference_mode": "rear_axle",
                "localization_back_offset_m": 1.4235,
                "vehicle_reference_confidence": "verified",
                "vehicle_reference_hard_gate_eligible": True,
            },
        },
    )
    _write_json(
        run_dir / "artifacts/ros2_gt_live_stats.json",
        {
            "reference": {
                "localization_reference_mode": "vehicle_origin",
                "apollo_control_state_reference": "rear_axle_input_com_internal",
            }
        },
    )
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (run_dir / "artifacts/debug_timeseries.csv").write_text(
        "\n".join(
            [
                (
                    "ts_sec,localization_header_timestamp_sec,localization_measurement_time,"
                    "localization_sequence_num,localization_frame_id,localization_time_base,"
                    "localization_heading,localization_orientation_qx,localization_orientation_qy,"
                    "localization_orientation_qz,localization_orientation_qw,decoded_orientation_heading,"
                    "decoded_orientation_heading_diff_rad,orientation_heading_diff_rad,heading_source,"
                    "orientation_convention,heading_error,e_psi_deg,lane_inside,lane_dist_m,e_y_m,"
                    "localization_speed_mps,chassis_speed_mps,velocity_norm_vs_chassis_speed_mps,"
                    "localization_chassis_timestamp_delta_ms,ego_yaw_rate_rad_s,heading_fd_yaw_rate_rad_s,"
                    "localization_angular_velocity_unit,linear_acceleration_x,linear_acceleration_vrf_x,"
                    "linear_acceleration_available,linear_acceleration_vrf_available,"
                    "angular_velocity_vrf_available,acceleration_source,planning_message_age_ms,"
                    "localization_uncertainty_policy,localization_msf_status_policy,"
                    "localization_sensor_status_policy"
                ),
                (
                    "0.00,0.00,0.00,1,map,sim_time,0.0,0.0,0.0,-0.7071067811865475,0.7071067811865476,0.0,"
                    "0.0,0.0,odom_quaternion_yaw_after_frame_transform,RFU_to_ENU,"
                    "0.01,0.5,true,0.02,0.02,5.0,5.0,0.0,0.0,0.0,0.0,rad_per_s,"
                    "0.0,0.0,true,true,true,carla_actor,10.0,not_modelled_gt_truth,"
                    "not_applicable_gt_truth,not_applicable_gt_truth"
                ),
                (
                    "0.05,0.05,0.05,2,map,sim_time,0.0,0.0,0.0,-0.7071067811865475,0.7071067811865476,0.0,"
                    "0.0,0.0,odom_quaternion_yaw_after_frame_transform,RFU_to_ENU,"
                    "0.01,0.5,true,0.02,0.02,5.2,5.2,0.0,0.0,0.0,0.0,rad_per_s,"
                    "0.0,0.0,true,true,true,carla_actor,11.0,not_modelled_gt_truth,"
                    "not_applicable_gt_truth,not_applicable_gt_truth"
                ),
                (
                    "0.10,0.10,0.10,3,map,sim_time,0.0,0.0,0.0,-0.7071067811865475,0.7071067811865476,0.0,"
                    "0.0,0.0,odom_quaternion_yaw_after_frame_transform,RFU_to_ENU,"
                    "0.01,0.5,true,0.02,0.02,5.4,5.4,0.0,0.0,0.0,0.0,rad_per_s,"
                    "0.0,0.0,true,true,true,carla_actor,12.0,not_modelled_gt_truth,"
                    "not_applicable_gt_truth,not_applicable_gt_truth"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["localization_gt_contract"]

    assert layer["status"] == "warn"
    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert layer["key_metrics"]["channel_status"] == "pass"
    assert layer["key_metrics"]["position_uses_vrp"] is True
    assert layer["key_metrics"]["vehicle_reference_hard_gate_eligible"] is True
    assert layer["key_metrics"]["measurement_header_delta_ms_p95"] == 0.0
    assert "localization_gt_contract:insufficient_data" not in report["why_not_claimable"]


def test_planning_and_module_consumption_use_run_artifacts_when_reports_are_placeholder(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization_report.v1",
            "status": "insufficient_data",
            "route_establishment": {"route_established": False},
            "blocking_reasons": ["planning_runtime_messages_missing"],
        },
    )
    _write_json(
        run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
        {
            "schema_version": "apollo_module_consumption_report.v1",
            "status": "insufficient_data",
            "blocking_reasons": ["apollo_module_runtime_logs_missing"],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    route_layer = report["layers"]["route_establishment"]
    consumption_layer = report["layers"]["apollo_module_consumption"]

    assert route_layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert route_layer["status"] == "pass"
    assert "planning_runtime_messages_missing" not in route_layer["blocking_reasons"]
    assert route_layer["key_metrics"]["planning_message_count"] == 100
    refreshed_planning = json.loads(
        (
            run_dir / "analysis/planning_materialization/planning_materialization_report.json"
        ).read_text(encoding="utf-8")
    )
    assert refreshed_planning["schema_version"] == "planning_materialization.v1"
    assert refreshed_planning["verdict"] == "pass"
    assert consumption_layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert "apollo_module_runtime_logs_missing" not in consumption_layer["blocking_reasons"]
    assert consumption_layer["key_metrics"]["routing_response_consumed_by_planning"] is True
    refreshed_consumption = json.loads(
        (
            run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json"
        ).read_text(encoding="utf-8")
    )
    assert refreshed_consumption["schema_version"] == "apollo_module_consumption.v1"
    assert refreshed_consumption["status"] == "warn"
    assert refreshed_consumption["blocking_reasons"] == []


def test_module_consumption_route_contract_stale_fail_is_regenerated(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "warn",
            "routing_phase": "claim",
            "claim_route_contract": {
                "status": "warn",
                "materialized": True,
                "blocking_reasons": [],
            },
            "blocking_reasons": [],
            "warnings": ["apollo_routing_goal_snap_distance_high"],
        },
    )
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "route_establishment": {"route_established": True},
            "first_nonempty_after_routing_latency_s": 0.2,
            "empty_reason_histogram": {},
        },
    )
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {
                "timestamp": 1.0,
                "trajectory_point_count": 10,
                "routing_header_present": True,
                "localization_age_ms": 10.0,
                "chassis_age_ms": 12.0,
                "routing_response_age_ms": 20.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/topic_publish_stats.jsonl",
        [
            {"topic": "/apollo/localization/pose"},
            {"topic": "/apollo/canbus/chassis"},
            {"topic": "/apollo/planning"},
            {"topic": "/apollo/control"},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/control_decode_debug.jsonl",
        [
            {
                "parsed_control": {
                    "control_timestamp": 1.0,
                    "control_header_sequence_num": 1,
                    "input_trajectory_header_sequence_num": 7,
                }
            }
        ],
    )
    _write_json(
        run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
        {
            "schema_version": "apollo_module_consumption.v1",
            "status": "fail",
            "blocking_reasons": [
                "claim_route_consumption_unverified",
                "route_contract_unverified_before_module_consumption_claim",
            ],
            "warnings": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["apollo_module_consumption"]

    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert "route_contract_unverified_before_module_consumption_claim" not in layer[
        "blocking_reasons"
    ]
    assert "claim_route_consumption_unverified" not in layer["blocking_reasons"]
    refreshed = json.loads(
        (
            run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json"
        ).read_text(encoding="utf-8")
    )
    assert refreshed["status"] in {"pass", "warn"}
    assert refreshed["apollo_route_contract_status"] == "warn"
    assert refreshed["consumed_route_phase"] == "claim"
    assert refreshed["blocking_reasons"] == []


def test_control_health_uses_run_artifacts_when_report_is_placeholder(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/control_health/control_health_report.json",
        {
            "schema_version": "control_health_report.v1",
            "status": "insufficient_data",
            "failure_reason": "missing_control_trace_fields",
            "raw_mapped_applied_control_available": False,
            "blocking_reasons": ["control_runtime_trace_missing"],
            "missing_fields": ["apollo_steer_raw", "bridge_steer_mapped"],
        },
    )
    _write_jsonl(
        run_dir / "artifacts/control_decode_debug.jsonl",
        [
            {
                "parsed_control": {
                    "control_latency_ms": 1.0,
                    "throttle": 0.2,
                    "brake": 0.0,
                    "steer": 0.0,
                },
                "output_to_carla": {
                    "mapped_throttle_cmd": 0.2,
                    "mapped_brake_cmd": 0.0,
                    "mapped_carla_steer_cmd": 0.0,
                    "throttle": 0.2,
                    "brake": 0.0,
                    "steer": 0.0,
                },
            }
        ],
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["control_mapping_apply"]

    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert "control_runtime_trace_missing" not in layer["blocking_reasons"]
    refreshed = json.loads(
        (run_dir / "analysis/control_health/control_health_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert refreshed["schema_version"] == "control_health_report.v1"
    assert refreshed["failure_reason"] != "missing_control_trace_fields"
    assert refreshed["source"]["control_decode_debug_path"] is not None
    assert refreshed["metrics"]["control_decode_debug"]["line_count"] == 1


def test_control_health_existing_fail_report_is_not_overwritten(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/control_health/control_health_report.json",
        {
            "schema_version": "control_health_report.v1",
            "status": "fail",
            "failure_reason": "applied_actuation_oscillation",
            "raw_mapped_applied_control_available": True,
            "metrics": {"sentinel": "keep_existing_failure"},
            "warnings": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["control_mapping_apply"]

    assert layer["artifact_paths"]["source_kind"] == "report"
    refreshed = json.loads(
        (run_dir / "analysis/control_health/control_health_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert refreshed["metrics"]["sentinel"] == "keep_existing_failure"


def test_route_establishment_after_routing_materialization_supports_no_assist_claim_window(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization_report.v1",
            "status": "insufficient_data",
            "route_establishment": {"route_established": False},
            "blocking_reasons": ["planning_runtime_messages_missing"],
        },
    )
    stats_path = run_dir / "artifacts/cyber_bridge_stats.json"
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    stats["routing_first_success_response_after_last_routing_send_boundary_ts_sec"] = 8.0
    _write_json(stats_path, stats)
    rows = []
    for index in range(12):
        rows.append(
            {
                "sim_time_sec": float(index),
                "planning_header_sequence_num": index,
                "trajectory_point_count": 12 if index >= 8 else 0,
            }
        )
    _write_jsonl(run_dir / "artifacts/planning_topic_debug.jsonl", rows)

    report = analyze_apollo_link_health_run_dir(run_dir)
    route_layer = report["layers"]["route_establishment"]
    no_assist = report["layers"]["no_assist_claim_boundary"]

    assert route_layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert route_layer["status"] == "warn"
    assert abs(route_layer["key_metrics"]["nonempty_trajectory_ratio"] - (4 / 12)) < 1e-12
    assert route_layer["key_metrics"]["after_routing_success_nonempty_ratio"] == 1.0
    assert route_layer["key_metrics"]["claim_window_nonempty_ratio"] == 1.0
    assert route_layer["key_metrics"]["claim_window_source"] == "after_routing_success"
    assert "planning_trajectory_materialization_low" not in route_layer["blocking_reasons"]
    refreshed_planning = json.loads(
        (
            run_dir / "analysis/planning_materialization/planning_materialization_report.json"
        ).read_text(encoding="utf-8")
    )
    assert refreshed_planning["verdict"] == "warn"
    assert refreshed_planning["claim_window_source"] == "after_routing_success"
    refreshed_consumption = json.loads(
        (
            run_dir / "analysis/apollo_module_consumption/apollo_module_consumption_report.json"
        ).read_text(encoding="utf-8")
    )
    assert refreshed_consumption["schema_version"] == "apollo_module_consumption.v1"
    assert "reference_line_provider_not_ready_empty_planning" not in refreshed_consumption[
        "blocking_reasons"
    ]
    assert no_assist["status"] == "pass"
    assert "planning_nonempty_ratio_not_claim_grade" not in no_assist["blocking_reasons"]
    assert no_assist["key_metrics"]["planning_nonempty_ratio"] == 1.0
    assert (
        no_assist["key_metrics"]["planning_nonempty_ratio_source"]
        == "planning_materialization.after_routing_success"
    )
    assert abs(no_assist["key_metrics"]["planning_nonempty_ratio_overall"] - (4 / 12)) < 1e-12
    assert report["primary_blocker"] != "no_assist_claim_boundary:planning_nonempty_ratio_not_claim_grade"


def test_route_contract_placeholder_regenerates_from_decoded_routing_response(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["metadata"] = {
        "scenario_metadata": {
            "route_id": "lane097",
            "route_length_m": 230.0,
            "spawn": {"x": 2.0, "y": 249.0},
            "goal": {"x": 2.0, "y": 19.0},
            "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
            "goal_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
            "route_trace": [
                {"x": 2.0, "y": 249.0, "s": 0.0, "lane_id": "15:0:1"},
                {"x": 2.0, "y": 19.0, "s": 230.0, "lane_id": "15:0:1"},
            ],
        }
    }
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract_report.v1",
            "status": "insufficient_data",
            "blocking_reasons": ["routing_response_runtime_evidence_missing"],
        },
    )
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "schema_version": "routing_response_decoded.v1",
            "source": "/apollo/raw_routing_response",
            "planning_facing_channel": "/apollo/routing_response",
            "raw_routing_response_channel": "/apollo/raw_routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 296.9}],
            "lane_sequence_signature": ["15_1_1"],
            "lane_segment_count": 1,
            "total_length_m": 230.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 230.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->296.9",
                "routing_unique_lane_signature": "15_1_1",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    route_layer = report["layers"]["route_establishment"]
    route_contract = json.loads(
        (run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json").read_text(
            encoding="utf-8"
        )
    )

    assert route_layer["artifact_paths"]["route_contract_source_kind"] == "regenerated_from_run_artifacts"
    assert "routing_response_runtime_evidence_missing" not in route_layer["blocking_reasons"]
    assert route_contract["routing_response_decoded"]["available"] is True
    assert route_contract["last_routing_response"]["source"] == "routing_response_decoded"
    assert route_layer["key_metrics"]["apollo_routing_total_length_m"] == 230.0


def test_stale_route_contract_warn_regenerates_after_projection_or_transform_available() -> None:
    report = {
        "schema_version": "apollo_route_contract.v1",
        "status": "warn",
        "warnings": ["apollo_route_xy_comparison_frame_transform_missing"],
        "routing_response_decoded": {"available": True},
    }

    assert _should_regenerate_apollo_route_contract(report) is True


def test_stale_route_contract_regenerates_for_disabled_goal_projection_reason() -> None:
    report = {
        "schema_version": "apollo_route_contract.v1",
        "status": "fail",
        "blocking_reasons": [
            "apollo_routing_goal_projection_not_accepted",
            "apollo_routing_goal_snap_distance_high",
        ],
        "last_routing_request": {
            "goal_projection": {
                "available": True,
                "accepted": False,
                "applied": False,
                "trusted_lane_centerline": True,
                "distance_m": 5.1,
                "reason": "disabled_by_config",
            }
        },
    }

    assert _should_regenerate_apollo_route_contract(report) is True


def test_reference_line_placeholder_regenerates_from_runtime_artifacts(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract_report.v1",
            "status": "insufficient_data",
            "blocking_reasons": ["apollo_reference_line_runtime_evidence_missing"],
            "warnings": [],
        },
    )
    planning_rows = []
    control_rows = []
    projection_rows = []
    for index in range(60):
        ts_sec = index * 0.05
        planning_rows.append(
            {
                "ts_sec": ts_sec,
                "localization_heading": 1.57,
                "trajectory_point_count": 10,
                "first_trajectory_point_theta": 1.571,
                "reference_line_count": 1,
                "reference_line_provider_status": "ready",
                "route_segment_count": 1,
                "lane_id_first": "15_1_1",
            }
        )
        control_rows.append(
            {
                "ts_sec": ts_sec,
                "debug_simple_lat_ref_heading": 1.57,
                "debug_simple_lat_heading": 1.57,
                "debug_simple_lat_heading_error": 0.0,
                "debug_simple_lat_lateral_error": 0.02,
            }
        )
        projection_rows.append(
            {
                "timestamp": ts_sec,
                "source": "apollo_hdmap_api",
                "status": "ok",
                "nearest_lane_id": "15_1_1",
                "heading_error_rad": 0.01,
                "lateral_error_m": 0.04,
                "projection_s": float(index),
            }
        )
    _write_jsonl(run_dir / "artifacts/planning_topic_debug.jsonl", planning_rows)
    _write_jsonl(run_dir / "artifacts/bridge_control_decode.jsonl", control_rows)
    _write_jsonl(run_dir / "artifacts/apollo_hdmap_projection.jsonl", projection_rows)

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["planning_reference_line"]
    refreshed = json.loads(
        (
            run_dir
            / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
        ).read_text(encoding="utf-8")
    )

    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert layer["status"] == "pass"
    assert "apollo_reference_line_runtime_evidence_missing" not in layer["blocking_reasons"]
    assert layer["key_metrics"]["evidence"]["planning_reference_available"] is True
    assert layer["key_metrics"]["apollo_hdmap_projection"]["claim_grade"] is True
    assert refreshed["source"]["planning_topic_debug_path"].endswith("planning_topic_debug.jsonl")
    assert refreshed["source"]["control_decode_debug_path"].endswith("bridge_control_decode.jsonl")


def test_control_handoff_placeholder_regenerates_from_runtime_artifacts(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
        {
            "schema_version": "apollo_control_handoff.v1",
            "status": "insufficient_data",
            "verdict": "insufficient_data",
            "failure_stage": "insufficient_data",
            "blocking_reasons": ["control_runtime_messages_missing"],
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "channel_stats.json",
        {
            "channels": {
                channel: {
                    "message_count": 100,
                    "hz": 20.0,
                    "max_gap_ms": 60.0,
                    "timestamp_monotonic": True,
                    "sequence_monotonic": True,
                }
                for channel in (
                    "/apollo/planning",
                    "/apollo/localization/pose",
                    "/apollo/canbus/chassis",
                    "/apollo/control",
                )
            }
        },
    )
    _write_json(
        run_dir / "artifacts/control_handoff_summary.json",
        {
            "control_process_started": True,
            "control_handoff_status": "control_consuming_with_nonzero_planning",
            "control_consume_row_count": 2,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/bridge_control_decode.jsonl",
        [
            {
                "ts_sec": 0.0,
                "apollo_steer_raw": 0.01,
                "throttle_raw": 20.0,
                "brake_raw": 0.0,
                "bridge_steer_mapped": 0.01,
                "throttle_mapped": 0.2,
                "brake_mapped": 0.0,
            },
            {
                "ts_sec": 0.05,
                "apollo_steer_raw": 0.01,
                "throttle_raw": 20.0,
                "brake_raw": 0.0,
                "bridge_steer_mapped": 0.01,
                "throttle_mapped": 0.2,
                "brake_mapped": 0.0,
            },
        ],
    )
    (run_dir / "timeseries.csv").write_text(
        "sim_time,ego_speed,route_s,carla_steer_applied,throttle_applied,brake_applied,ego_yaw_rate\n"
        "0.0,1.0,0.0,0.01,0.2,0.0,0.001\n"
        "0.05,1.3,0.3,0.01,0.2,0.0,0.001\n",
        encoding="utf-8",
    )

    report = analyze_apollo_link_health_run_dir(run_dir)
    layer = report["layers"]["routing_planning_control_handoff"]
    refreshed = json.loads(
        (
            run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json"
        ).read_text(encoding="utf-8")
    )

    assert layer["artifact_paths"]["source_kind"] == "regenerated_from_run_artifacts"
    assert layer["status"] == "warn"
    assert "control_runtime_messages_missing" not in layer["blocking_reasons"]
    assert layer["key_metrics"]["control_message_count"] == 100
    assert layer["key_metrics"]["control_rx_count"] == 100
    assert layer["key_metrics"]["apply_control_count"] == 100
    assert refreshed["failure_stage"] == "none"
    assert refreshed["mapping_and_apply"]["raw_mapped_applied_available"] is True


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


def test_channel_cadence_diagnosis_refines_channel_primary(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    stats_src = Path("tests/fixtures/channel_cadence/header_sim_gap_channel_stats.json")
    (run_dir / "channel_stats.json").write_text(stats_src.read_text(encoding="utf-8"), encoding="utf-8")
    tick_src = Path("tests/fixtures/channel_cadence/carla_tick_health_summary.json")
    (run_dir / "artifacts/carla_tick_health_summary.json").write_text(
        tick_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    gap_src = Path("tests/fixtures/channel_cadence/publish_gap_trace.jsonl")
    (run_dir / "artifacts/publish_gap_trace.jsonl").write_text(
        gap_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    topic_src = Path("tests/fixtures/channel_cadence/topic_publish_stats.jsonl")
    (run_dir / "artifacts/topic_publish_stats.jsonl").write_text(
        topic_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    tick_log_src = Path("tests/fixtures/channel_cadence/carla_tick_health.jsonl")
    (run_dir / "artifacts/carla_tick_health.jsonl").write_text(
        tick_log_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)
    channel_layer = report["layers"]["channel_health"]

    assert report["primary_blocker"] == "channel_health:localization:isolated_header_sim_gap_over_contract"
    assert channel_layer["key_metrics"]["channel_cadence_status"] == "fail"
    assert (
        channel_layer["key_metrics"]["channel_cadence_primary_issue"]
        == "localization:isolated_header_sim_gap_over_contract"
    )
    assert channel_layer["key_metrics"]["channel_cadence_top_gap_windows"][0]["channel_name"] == "localization"
    assert channel_layer["artifact_paths"]["channel_cadence_source_kind"] == "regenerated_from_run_artifacts"
    cadence_path = run_dir / "analysis/channel_cadence_diagnosis/channel_cadence_diagnosis_report.json"
    assert channel_layer["artifact_paths"]["channel_cadence_diagnosis"] == str(cadence_path)
    assert cadence_path.is_file()
    cadence_report = json.loads(cadence_path.read_text(encoding="utf-8"))
    assert cadence_report["schema_version"] == "channel_cadence_diagnosis.v1"
    assert cadence_report["primary_cadence_issue"] == "localization:isolated_header_sim_gap_over_contract"


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
