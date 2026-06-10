from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import yaml

from carla_testbed.algorithms.gt_replacement_matrix import load_gt_replacement_matrix
from carla_testbed.analysis.apollo_chain_completion import (
    APOLLO_CHAIN_COMPLETION_SCHEMA_VERSION,
    analyze_apollo_chain_completion_run_dir,
    write_apollo_chain_completion_report,
)
from carla_testbed.analysis.apollo_link_health import analyze_apollo_link_health_run_dir

REFERENCE = "configs/reference/apollo_reference_chain.yaml"
DEFAULT_REPLACEMENT = "configs/reference/apollo_gt_replacement_matrix.yaml"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_run(tmp_path: Path, *, scenario_class: str = "lane_keep") -> Path:
    run_dir = tmp_path / "run"
    scenario_id = "traffic_light_red_stop" if scenario_class == "traffic_light_red_stop" else "lane_keep_097"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_id": scenario_id,
            "scenario_class": scenario_class,
            "route_id": "097",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "routing_materialized": True,
            "routing_success_count": 1,
            "planning_materialized": True,
            "planning_nonempty_ratio": 1.0,
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "scenario_id": scenario_id,
            "scenario_class": scenario_class,
            "route_id": "097",
            "backend": "apollo_cyberrt",
            "control_source": "/apollo/control",
            "transport_mode": "ros2_gt",
            "algorithm_variant_id": "apollo_ported_carla_gt",
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
    _write_json(run_dir / "artifacts/bridge_health_summary.json", {"status": "pass"})
    _write_json(run_dir / "artifacts/bridge_transport_summary.json", {"status": "pass"})
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {"status": "pass", "messages_with_nonzero_trajectory_points": 100},
    )
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {"schema_version": "apollo_channel_health_report.v1", "status": "pass"},
    )
    _write_json(
        run_dir / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "verdict": {"status": "pass", "blocking_reasons": []},
            "channel": {"status": "pass"},
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
                "claim_grade": True,
                "status": "pass",
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
                "speed_delta_p95_mps": 0.01,
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
                "route_id": "097",
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
            "metrics": {"control_ref_heading_error_p95_rad": 0.01},
            "evidence": {"nonempty_trajectory_ratio": 1.0},
            "apollo_hdmap_projection": {
                "file_present": True,
                "official_source_available": True,
                "claim_grade": True,
                "status": "pass",
                "blocking_reasons": [],
                "warnings": [],
            },
        },
    )
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "run_id": "run",
            "route_id": "097",
            "scenario_class": scenario_class,
            "planning_message_count": 100,
            "nonempty_trajectory_count": 100,
            "nonempty_trajectory_ratio": 1.0,
            "after_routing_success_nonempty_ratio": 1.0,
            "after_localization_chassis_ready_nonempty_ratio": 1.0,
            "first_nonempty_after_routing_latency_s": 0.2,
            "longest_empty_streak": 0,
            "empty_reason_histogram": {},
            "route_establishment": {
                "route_established": True,
                "blocking_reasons": [],
                "route_completion_ratio": 1.0,
            },
            "blocking_reasons": [],
            "warnings": [],
            "verdict": "pass",
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
            "metrics": {
                "lateral_guard_apply_count": 0,
                "trajectory_contract_guard_apply_count": 0,
            },
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/control_attribution/control_attribution_report.json",
        {
            "schema_version": "control_attribution.v1",
            "verdict": "pass",
            "applied_control_source": "apollo_control",
        },
    )
    _write_json(
        run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        {"schema_version": "obstacle_gt_contract.v1", "status": "pass", "errors": [], "warnings": []},
    )
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "prediction_mode": "native_observed",
            "prediction_message_count": 50,
            "prediction_channel_available": True,
            "hard_gate_eligible": True,
            "blocking_capabilities": [],
            "verdict": "pass",
            "warnings": [],
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
            "blocking_reasons": [],
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/traffic_light_contract/traffic_light_contract_report.json",
        {"schema_version": "traffic_light_contract_report.v1", "status": "pass"},
    )
    _write_json(
        run_dir / "analysis/traffic_light_evidence/traffic_light_evidence_report.json",
        {
            "schema_version": "traffic_light_evidence.v1",
            "status": "pass",
            "planning_consumed": True,
            "behavior_observed": True,
        },
    )
    _write_json(
        run_dir / "analysis/traffic_light_behavior/traffic_light_behavior_report.json",
        {"schema_version": "traffic_light_behavior.v1", "status": "pass"},
    )
    return run_dir


def _claim_grade_replacement_matrix(tmp_path: Path, *, prediction_bypass_reason: bool = True) -> Path:
    matrix = deepcopy(load_gt_replacement_matrix(DEFAULT_REPLACEMENT))
    for module in matrix["modules"]:
        module["current_evidence_status"] = "pass"
        module["hard_gate_eligible"] = module["name"] != "dreamview"
        module["blocked_capabilities"] = []
        module["allowed_for_capabilities"] = [
            "lane_keep",
            "curve",
            "junction",
            "traffic_light",
            "closed_loop",
        ]
        if module["name"] == "prediction":
            if prediction_bypass_reason:
                module["replacement_status"] = "native"
                module["bypass_reason"] = None
            else:
                module["replacement_status"] = "bypassed"
                module["bypass_reason"] = None
        if module["name"] == "traffic_light_perception":
            module["required_evidence"] = [
                "traffic_light_contract_report.json",
                "traffic_light_evidence_report.json",
                "planning_consumed traffic-light evidence",
                "traffic_light_behavior_report.json behavior observed evidence",
            ]
        if module["name"] == "vehicle_interface":
            module["required_evidence"] = [
                "control_attribution_report.json with raw/mapped/applied/vehicle response evidence",
                "control_health_report.json",
            ]
    path = tmp_path / "replacement.yaml"
    path.write_text(yaml.safe_dump(matrix, sort_keys=False), encoding="utf-8")
    return path


def test_full_fixture_closed_loop_pass(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["schema_version"] == APOLLO_CHAIN_COMPLETION_SCHEMA_VERSION
    assert report["capability_status"]["closed_loop"] == "pass"
    assert report["failure_stage"] == "none"
    assert report["can_claim_truth_input_closed_loop"] is True
    assert report["can_claim_unassisted_natural_driving"] is True
    assert "dreamview" not in report["blocking_modules"]
    localization = report["module_statuses"]["localization"]
    assert localization["project_matrix_status"] == "pass"
    assert localization["run_evidence_status"] == "pass"
    assert localization["run_claim_grade"] is True
    assert localization["effective_status"] == "pass"


def test_missing_control_handoff_blocks_closed_loop(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    (run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json").unlink()

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["module_statuses"]["control"]["evidence_status"] == "missing"
    assert report["capability_status"]["closed_loop"] == "insufficient_data"
    assert report["failure_stage"] == "control_handoff"


def test_route_establishment_failure_is_planning_materialization_stage(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "run_id": "run",
            "route_id": "097",
            "scenario_class": "lane_keep",
            "planning_message_count": 550,
            "nonempty_trajectory_count": 14,
            "nonempty_trajectory_ratio": 14 / 550,
            "after_routing_success_nonempty_ratio": 0.03,
            "route_establishment": {
                "route_established": False,
                "blocking_reasons": ["route_establishment_latency"],
                "route_completion_ratio": 0.0,
            },
            "blocking_reasons": ["planning_trajectory_materialization_low"],
            "warnings": [],
            "verdict": "fail",
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["link_health_layers"]["route_establishment"]["status"] == "fail"
    assert report["failure_stage"] == "planning_materialization"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_route_contract_mismatch_is_route_establishment_stage(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
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

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["link_health_layers"]["route_establishment"]["status"] == "fail"
    assert report["failure_stage"] == "route_establishment"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_route_establishment_failure_has_priority_over_missing_hdmap_projection(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    localization = json.loads(
        (run_dir / "analysis/localization_contract/localization_contract_report.json").read_text(
            encoding="utf-8"
        )
    )
    localization["apollo_hdmap_projection"] = {
        "file_present": False,
        "official_source_available": False,
        "claim_grade": False,
        "status": "insufficient_data",
        "blocking_reasons": [],
        "warnings": ["apollo_hdmap_projection_missing"],
    }
    _write_json(run_dir / "analysis/localization_contract/localization_contract_report.json", localization)
    reference_line = json.loads(
        (
            run_dir
            / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
        ).read_text(encoding="utf-8")
    )
    reference_line["apollo_hdmap_projection"] = localization["apollo_hdmap_projection"]
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        reference_line,
    )
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "run_id": "run",
            "route_id": "097",
            "scenario_class": "lane_keep",
            "planning_message_count": 219,
            "nonempty_trajectory_count": 0,
            "nonempty_trajectory_ratio": 0.0,
            "after_routing_success_nonempty_ratio": None,
            "longest_empty_streak": 219,
            "empty_reason_histogram": {"reference_line_provider_not_ready": 219},
            "route_establishment": {
                "route_established": False,
                "blocking_reasons": ["routing_success_missing"],
                "route_completion_ratio": 0.0,
            },
            "blocking_reasons": [
                "planning_trajectory_materialization_low",
                "route_establishment_not_confirmed",
            ],
            "warnings": ["apollo_hdmap_projection_missing"],
            "verdict": "fail",
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["link_health_layers"]["hdmap_projection"]["status"] == "insufficient_data"
    assert report["link_health_layers"]["route_establishment"]["status"] == "fail"
    assert report["failure_stage"] == "planning_materialization"


def test_traffic_light_only_schema_blocks_traffic_light_capability(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path, scenario_class="traffic_light_red_stop")
    replacement = _claim_grade_replacement_matrix(tmp_path)
    (run_dir / "analysis/traffic_light_evidence/traffic_light_evidence_report.json").unlink()
    (run_dir / "analysis/traffic_light_behavior/traffic_light_behavior_report.json").unlink()

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["module_statuses"]["traffic_light_perception"]["evidence_status"] == "missing"
    assert report["capability_status"]["traffic_light"] == "insufficient_data"


def test_prediction_bypass_without_reason_fails_matrix_validation(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path, prediction_bypass_reason=False)

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["failure_stage"] == "invalid_replacement_matrix"
    assert report["verdict"] == "fail"


def test_chain_completion_reads_prediction_evidence_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "prediction_mode": "bypassed_with_gt_obstacles",
            "bypass_reason": "static lane_keep route-only diagnostic",
            "hard_gate_eligible": True,
            "blocking_capabilities": [],
            "verdict": "warn",
            "warnings": ["prediction_bypassed_with_reason"],
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["module_statuses"]["prediction"]["evidence_status"] == "warn"
    assert report["module_statuses"]["prediction"]["hard_gate_eligible"] is True


def test_prediction_evidence_report_overrides_default_matrix_blocking_scope(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "prediction_mode": "bypassed_with_gt_obstacles",
            "bypass_reason": "static lane_keep route-only diagnostic",
            "hard_gate_eligible": True,
            "blocking_capabilities": [],
            "verdict": "warn",
            "warnings": ["prediction_bypassed_with_reason"],
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=DEFAULT_REPLACEMENT,
    )

    prediction = report["module_statuses"]["prediction"]
    assert prediction["evidence_status"] == "warn"
    assert prediction["hard_gate_eligible"] is True
    assert prediction["blocking_capabilities"] == []
    assert prediction["observed_evidence"]["prediction_mode native_observed or bypassed_with_gt_obstacles"] == (
        "bypassed_with_gt_obstacles"
    )
    assert prediction["observed_evidence"]["bypass_reason when prediction is bypassed"]


def test_lane_keep_failure_stage_does_not_prioritize_non_applicable_traffic_light(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path, scenario_class="lane_keep")
    (run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json").unlink()
    (run_dir / "analysis/traffic_light_contract/traffic_light_contract_report.json").unlink()
    (run_dir / "analysis/traffic_light_evidence/traffic_light_evidence_report.json").unlink()
    (run_dir / "analysis/traffic_light_behavior/traffic_light_behavior_report.json").unlink()
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "prediction_mode": "bypassed_with_gt_obstacles",
            "bypass_reason": "static lane_keep route-only diagnostic",
            "hard_gate_eligible": True,
            "blocking_capabilities": [],
            "verdict": "warn",
            "warnings": ["prediction_bypassed_with_reason"],
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=DEFAULT_REPLACEMENT,
    )

    assert report["target_capability"] == "lane_keep"
    assert report["module_statuses"]["traffic_light_perception"]["blocking_capabilities"] == [
        "traffic_light"
    ]
    assert report["failure_stage"] == "planning_materialization"


def test_route_health_without_reference_line_blocks_curve_and_junction(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    (run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json").unlink()
    _write_json(
        run_dir / "analysis/route_health/route_health.json",
        {
            "schema_version": "route_health.v1",
            "hard_gate_eligible": True,
            "evidence_level": "claim_grade",
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["capability_status"]["lane_keep"] == "warn"
    assert report["capability_status"]["curve"] == "insufficient_data"
    assert report["capability_status"]["junction"] == "insufficient_data"
    assert report["module_statuses"]["planning"]["evidence_status"] == "missing"


def test_planning_channel_gap_does_not_mark_chassis_module_failed(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "fail",
            "channel_results": {
                "chassis": {
                    "name": "chassis",
                    "channel": "/apollo/canbus/chassis",
                    "status": "pass",
                },
                "planning": {
                    "name": "planning",
                    "channel": "/apollo/planning",
                    "status": "fail",
                    "issues": ["message_gap_too_large"],
                },
            },
            "missing_required_channels": [],
            "warnings": [],
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["module_statuses"]["chassis"]["evidence_status"] == "pass"
    assert report["module_statuses"]["cyberrt"]["evidence_status"] == "fail"


def test_channel_health_failure_is_reported_before_vehicle_interface(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "fail",
            "channel_results": {
                "planning": {
                    "name": "planning",
                    "channel": "/apollo/planning",
                    "status": "fail",
                    "issues": ["message_gap_too_large"],
                }
            },
            "missing_required_channels": [],
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/control_health/control_health_report.json",
        {
            "schema_version": "control_health_report.v1",
            "status": "fail",
            "failure_reason": "control_bridge_world_frame_cadence_low",
            "metrics": {},
            "warnings": [],
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert "channel_health" in report["blocking_layers"]
    assert "vehicle_interface" in report["blocking_modules"]
    assert report["failure_stage"] == "channel_health"


def test_localization_fail_blocks_driving_capabilities(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    _write_json(
        run_dir / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "verdict": {
                "status": "fail",
                "blocking_reasons": ["heading_error_to_lane_high"],
            },
            "warnings": [],
        },
    )

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["module_statuses"]["localization"]["evidence_status"] == "fail"
    assert all(report["capability_status"][capability] == "fail" for capability in report["capability_status"])
    assert report["failure_stage"] == "localization_contract"


def test_blocking_assist_prevents_unassisted_claim(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["assist_ledger"]["active_assists"] = ["straight_lane_lateral_stabilizer"]
    manifest["assist_ledger"]["blocking_assists"] = ["straight_lane_lateral_stabilizer"]
    manifest["assist_ledger"]["can_claim_unassisted_natural_driving"] = False
    _write_json(run_dir / "manifest.json", manifest)

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["can_claim_unassisted_natural_driving"] is False
    assert "no_assist_claim_boundary" in report["blocking_layers"]


def test_missing_artifacts_are_insufficient_data_not_pass(tmp_path: Path) -> None:
    run_dir = tmp_path / "empty_run"
    run_dir.mkdir()
    replacement = _claim_grade_replacement_matrix(tmp_path)

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["verdict"] == "insufficient_data"
    assert report["can_claim_truth_input_closed_loop"] is False


def test_link_health_layer_summary_is_reused_and_compatible(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    link_report = analyze_apollo_link_health_run_dir(run_dir)

    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )

    assert report["link_health_layers"]["localization_gt_contract"] == link_report["layers"][
        "localization_gt_contract"
    ]
    assert report["source_link_health"]["schema_version"] == link_report["schema_version"]


def test_write_report_outputs_chain_and_compatible_link_health(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    replacement = _claim_grade_replacement_matrix(tmp_path)
    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=REFERENCE,
        replacement_path=replacement,
    )
    out_dir = tmp_path / "out"

    outputs = write_apollo_chain_completion_report(
        report,
        out_dir,
        link_health_report=analyze_apollo_link_health_run_dir(run_dir),
    )

    assert Path(outputs["apollo_chain_completion_report"]).exists()
    assert Path(outputs["apollo_chain_completion_summary"]).exists()
    assert Path(outputs["apollo_link_health_report"]).exists()
