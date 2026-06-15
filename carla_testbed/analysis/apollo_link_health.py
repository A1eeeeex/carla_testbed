from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.apollo_channel_health import analyze_apollo_channel_health_files
from carla_testbed.analysis.apollo_route_contract import (
    analyze_apollo_route_contract_run_dir,
    write_apollo_route_contract_report,
)
from carla_testbed.analysis.apollo_module_consumption import analyze_apollo_module_consumption_run_dir
from carla_testbed.analysis.chassis_gt_contract import analyze_chassis_gt_contract_files
from carla_testbed.analysis.channel_cadence_diagnosis import analyze_channel_cadence_diagnosis_run_dir
from carla_testbed.analysis.obstacle_gt_contract import analyze_obstacle_gt_contract_file
from carla_testbed.analysis.planning_materialization import analyze_planning_materialization_run_dir
from carla_testbed.analysis.prediction_evidence import analyze_prediction_evidence_run_dir
from carla_testbed.analysis.assist_ledger import build_runtime_assist_ledger

APOLLO_LINK_HEALTH_SCHEMA_VERSION = "apollo_link_health.v1"

LAYER_ORDER = (
    "environment_world",
    "bridge_runtime",
    "channel_health",
    "localization_gt_contract",
    "chassis_gt_contract",
    "hdmap_projection",
    "planning_reference_line",
    "apollo_lateral_semantics",
    "route_establishment",
    "apollo_module_consumption",
    "routing_planning_control_handoff",
    "control_mapping_apply",
    "perception_gt_obstacles",
    "prediction_evidence",
    "traffic_light_gt",
    "no_assist_claim_boundary",
    "natural_driving_outcome",
)

PASS_WARN = {"pass", "warn", "not_applicable"}
BLOCKING_STATUSES = {"fail", "insufficient_data"}
TRAFFIC_LIGHT_SCENARIOS = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
ROUTE_CONTRACT_BLOCKER_PRIORITY = (
    "scenario_route_length_inconsistent",
    "apollo_routing_goal_mismatch",
    "apollo_routing_lane_sequence_mismatch",
    "apollo_routing_length_mismatch",
    "apollo_routing_missing_scenario_lane",
    "route_identity_inconsistent",
    "claim_route_not_materialized",
    "long_goal_not_compatible_with_scenario_route",
    "routing_response_runtime_evidence_missing",
)


def analyze_apollo_link_health_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    inputs = _resolve_inputs(root)
    return analyze_apollo_link_health(inputs, run_dir=root)


def analyze_apollo_link_health(
    inputs: Mapping[str, Path | None],
    *,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    inputs = dict(inputs)
    root = Path(run_dir).expanduser() if run_dir is not None else None
    payloads = {name: _read_json(path) for name, path in inputs.items() if name != "suite_manifest"}
    suite_manifest = _read_json(inputs.get("suite_manifest"))
    summary = payloads.get("summary", {})
    manifest = payloads.get("manifest", {})
    cyber_bridge_stats = payloads.get("cyber_bridge_stats", {})
    scenario_class = _first_text(summary, "scenario_class", manifest, "scenario_class")
    source_kinds: dict[str, str] = {}
    if root is not None:
        if _should_regenerate_apollo_route_contract(
            payloads.get("apollo_route_contract")
        ) and _apollo_route_contract_raw_inputs_available(root):
            regenerated_route_contract = analyze_apollo_route_contract_run_dir(root)
            if _apollo_route_contract_has_runtime_evidence(regenerated_route_contract) or not payloads.get(
                "apollo_route_contract"
            ):
                payloads["apollo_route_contract"] = regenerated_route_contract
                source_kinds["apollo_route_contract"] = "regenerated_from_run_artifacts"
                outputs = write_apollo_route_contract_report(
                    regenerated_route_contract,
                    root / "analysis" / "apollo_route_contract",
                )
                inputs["apollo_route_contract"] = Path(outputs["apollo_route_contract_report"])
        if _should_regenerate_planning_materialization(payloads.get("planning_materialization")):
            regenerated_planning = analyze_planning_materialization_run_dir(root)
            if _planning_materialization_has_runtime_evidence(regenerated_planning) or not payloads.get("planning_materialization"):
                payloads["planning_materialization"] = regenerated_planning
                source_kinds["planning_materialization"] = "regenerated_from_run_artifacts"
        if _should_regenerate_apollo_module_consumption(payloads.get("apollo_module_consumption")):
            regenerated_consumption = analyze_apollo_module_consumption_run_dir(root)
            if _apollo_module_consumption_has_runtime_evidence(regenerated_consumption) or not payloads.get("apollo_module_consumption"):
                payloads["apollo_module_consumption"] = regenerated_consumption
                source_kinds["apollo_module_consumption"] = "regenerated_from_run_artifacts"
        if _should_regenerate_prediction_evidence(payloads.get("prediction_evidence")):
            regenerated_prediction = analyze_prediction_evidence_run_dir(root)
            if _prediction_evidence_has_boundary(regenerated_prediction):
                payloads["prediction_evidence"] = regenerated_prediction
                source_kinds["prediction_evidence"] = "regenerated_from_run_artifacts"
        if inputs.get("channel_stats") is not None and _should_regenerate_channel_cadence(
            payloads.get("channel_cadence_diagnosis")
        ):
            regenerated_cadence = analyze_channel_cadence_diagnosis_run_dir(root)
            if regenerated_cadence.get("status") != "insufficient_data" or not payloads.get("channel_cadence_diagnosis"):
                payloads["channel_cadence_diagnosis"] = regenerated_cadence
                source_kinds["channel_cadence_diagnosis"] = "regenerated_from_run_artifacts"

    layers = {
        "environment_world": _environment_world_layer(
            summary,
            manifest,
            payloads.get("carla_world_ready_summary", {}),
            payloads.get("carla_bootstrap_summary", {}),
            inputs,
        ),
        "bridge_runtime": _bridge_runtime_layer(summary, payloads, inputs),
        "channel_health": _channel_health_layer(
            payloads.get("apollo_channel_health"),
            inputs.get("apollo_channel_health"),
            stats_path=inputs.get("channel_stats"),
            scenario_class=scenario_class,
            cadence_report=payloads.get("channel_cadence_diagnosis"),
            cadence_path=inputs.get("channel_cadence_diagnosis"),
            cadence_source_kind=source_kinds.get("channel_cadence_diagnosis", "report"),
        ),
        "localization_gt_contract": _localization_layer(
            payloads.get("localization_contract"),
            inputs.get("localization_contract"),
        ),
        "chassis_gt_contract": _chassis_gt_layer(
            payloads.get("chassis_gt_contract"),
            inputs.get("chassis_gt_contract"),
            run_dir=root,
        ),
        "hdmap_projection": _hdmap_projection_layer(
            payloads.get("apollo_hdmap_projection"),
            payloads.get("localization_contract"),
            payloads.get("apollo_reference_line_contract"),
            inputs,
        ),
        "planning_reference_line": _report_layer(
            name="planning_reference_line",
            report=payloads.get("apollo_reference_line_contract"),
            path=inputs.get("apollo_reference_line_contract"),
            status_keys=("status",),
            blocking_keys=("blocking_reasons",),
            warning_keys=("warnings",),
            next_action="Inspect Apollo reference-line, routing lane ids, and HDMap projection evidence.",
            key_metric_fields=("metrics", "evidence", "apollo_hdmap_projection"),
        ),
        "apollo_lateral_semantics": _apollo_lateral_semantics_layer(
            payloads.get("apollo_lateral_semantics"),
            inputs.get("apollo_lateral_semantics"),
        ),
        "route_establishment": _route_establishment_layer(
            payloads.get("planning_materialization"),
            inputs.get("planning_materialization"),
            source_kind=source_kinds.get("planning_materialization", "report"),
            route_contract=payloads.get("apollo_route_contract"),
            route_contract_path=inputs.get("apollo_route_contract"),
            route_contract_source_kind=source_kinds.get("apollo_route_contract", "report"),
            summary=summary,
            bridge_stats=cyber_bridge_stats,
            planning_topic_debug_summary=payloads.get("planning_topic_debug_summary", {}),
        ),
        "apollo_module_consumption": _apollo_module_consumption_layer(
            payloads.get("apollo_module_consumption"),
            inputs.get("apollo_module_consumption"),
            source_kind=source_kinds.get("apollo_module_consumption", "report"),
        ),
        "routing_planning_control_handoff": _control_handoff_layer(
            payloads.get("apollo_control_handoff"),
            inputs.get("apollo_control_handoff"),
            summary=summary,
            bridge_stats=cyber_bridge_stats,
            planning_topic_debug_summary=payloads.get("planning_topic_debug_summary", {}),
            command_materialization_summary=payloads.get("command_materialization_summary", {}),
            command_materialization_path=inputs.get("command_materialization_summary"),
        ),
        "control_mapping_apply": _control_health_layer(
            payloads.get("control_health"),
            inputs.get("control_health"),
        ),
        "perception_gt_obstacles": _obstacle_gt_layer(
            payloads.get("obstacle_gt_contract"),
            inputs.get("obstacle_gt_contract"),
            raw_path=inputs.get("obstacle_gt_contract_raw"),
            scenario_class=scenario_class,
        ),
        "prediction_evidence": _prediction_layer(
            payloads.get("prediction_evidence"),
            inputs.get("prediction_evidence"),
            scenario_class=scenario_class,
            source_kind=source_kinds.get("prediction_evidence", "report"),
        ),
        "traffic_light_gt": _traffic_light_layer(
            payloads.get("traffic_light_contract"),
            inputs.get("traffic_light_contract"),
            scenario_class=scenario_class,
        ),
        "no_assist_claim_boundary": _no_assist_layer(
            summary=summary,
            manifest=manifest,
            bridge_stats=cyber_bridge_stats,
            control_handoff=payloads.get("apollo_control_handoff", {}),
            control_health=payloads.get("control_health", {}),
            apollo_reference_line_contract=payloads.get("apollo_reference_line_contract", {}),
            planning_materialization=payloads.get("planning_materialization", {}),
            planning_topic_debug_summary=payloads.get("planning_topic_debug_summary", {}),
            inputs=inputs,
        ),
        "natural_driving_outcome": _natural_driving_layer(
            payloads.get("natural_driving_report"),
            inputs.get("natural_driving_report"),
            summary=summary,
        ),
    }

    primary, secondary = _blocker_summary(layers)
    can_claim = _can_claim_unassisted(layers)
    return {
        "schema_version": APOLLO_LINK_HEALTH_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=root.name if root else None),
        "route_id": _first_text(summary, "route_id", manifest, "route_id"),
        "scenario_id": _first_text(summary, "scenario_id", manifest, "scenario_id"),
        "scenario_class": scenario_class,
        "backend": _first_text(summary, "backend", manifest, "backend"),
        "transport_mode": _first_text(summary, "transport_mode", manifest, "transport_mode"),
        "suite_id": _first_text(suite_manifest, "suite_id", "batch_id"),
        "layers": {name: layers[name] for name in LAYER_ORDER},
        "primary_blocker": primary,
        "secondary_blockers": secondary,
        "can_claim_unassisted_natural_driving": can_claim,
        "why_not_claimable": _why_not_claimable(layers),
        "next_highest_value_validation": _next_highest_value_validation(primary, layers),
        "source": {
            "run_dir": str(root) if root else None,
            **{name: str(path) if path else None for name, path in inputs.items()},
        },
        "interpretation_boundary": (
            "Apollo link health is an evidence index. It does not recompute behavior success "
            "and must not be used to bypass localization, reference-line, assist, perception, "
            "or natural-driving gates."
        ),
    }


def write_apollo_link_health_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "apollo_link_health_report.json"
    summary_path = output_dir / "apollo_link_health_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_link_health_summary_md(report), encoding="utf-8")
    return {
        "apollo_link_health_report": str(json_path),
        "apollo_link_health_summary": str(summary_path),
    }


def analyze_and_write_apollo_link_health(
    run_dir: str | Path,
    *,
    out_dir: str | Path | None = None,
) -> dict[str, str]:
    root = Path(run_dir).expanduser()
    report = analyze_apollo_link_health_run_dir(root)
    output = Path(out_dir).expanduser() if out_dir is not None else root / "analysis" / "apollo_link_health"
    return write_apollo_link_health_report(report, output)


def apollo_link_health_summary_md(report: Mapping[str, Any]) -> str:
    layers = report.get("layers") if isinstance(report.get("layers"), Mapping) else {}
    lines = [
        "# Apollo Link Health Summary",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Route ID: `{report.get('route_id')}`",
        f"- Scenario: `{report.get('scenario_id')}` / `{report.get('scenario_class')}`",
        f"- Backend: `{report.get('backend')}` transport=`{report.get('transport_mode')}`",
        f"- Primary blocker: `{report.get('primary_blocker')}`",
        f"- Secondary blockers: `{', '.join(report.get('secondary_blockers') or []) or 'none'}`",
        f"- Can claim unassisted natural driving: `{report.get('can_claim_unassisted_natural_driving')}`",
        f"- Next highest-value validation: `{report.get('next_highest_value_validation')}`",
        "",
        "## Layers",
        "",
    ]
    for name in LAYER_ORDER:
        layer = layers.get(name) if isinstance(layers, Mapping) else {}
        if not isinstance(layer, Mapping):
            layer = {}
        key_metrics = _summary_key_metrics(name, layer.get("key_metrics"))
        lines.extend(
            [
                f"### {name}",
                f"- status: `{layer.get('status')}`",
                f"- blocking_reasons: `{', '.join(layer.get('blocking_reasons') or []) or 'none'}`",
                f"- warnings: `{', '.join(layer.get('warnings') or []) or 'none'}`",
                *([f"- key_metrics: `{key_metrics}`"] if key_metrics else []),
                f"- next_action: `{layer.get('next_action')}`",
                "",
            ]
        )
    lines.extend([str(report.get("interpretation_boundary") or ""), ""])
    return "\n".join(lines)


def _summary_key_metrics(layer_name: str, metrics: Any) -> str:
    if not isinstance(metrics, Mapping):
        return ""
    preferred_by_layer = {
        "environment_world": (
            "runtime_contract_status",
            "carla_world_ready_status",
            "loaded_map_name",
            "carla_tick_max_wall_duration_s",
            "carla_tick_max_inter_tick_wall_interval_s",
            "carla_tick_inter_tick_wall_interval_p95_s",
            "carla_tick_cadence_source",
            "carla_tick_inter_tick_wall_gap_high",
            "carla_tick_max_stage_name",
            "carla_tick_max_stage_duration_s",
            "carla_tick_max_hook_stage_name",
            "carla_tick_max_hook_stage_duration_s",
        ),
        "channel_health": (
            "failed_channels",
            "channel_cadence_primary_issue",
            "channel_cadence_primary_gap_source",
            "channel_cadence_carla_tick_stage",
            "channel_cadence_publish_skip_reason",
            "channel_cadence_artifact_writer_lag_ms",
            "channel_cadence_status",
            "channel_cadence_top_gap_windows",
            "planning_status",
            "planning_primary_time_axis",
            "planning_time_axis_diagnosis",
            "planning_max_gap_ms",
            "planning_sim_time_max_gap_ms",
            "planning_sim_time_gap_p95_ms",
            "planning_gap_count_over_1000ms",
            "planning_sim_time_gap_count_over_1000ms",
        ),
        "localization_gt_contract": (
            "channel_status",
            "heading_error_to_route_p95_rad",
            "heading_error_to_lane_p95_rad",
            "position_uses_vrp",
            "vehicle_reference_hard_gate_eligible",
            "measurement_header_delta_ms_p95",
        ),
        "chassis_gt_contract": (
            "claim_grade",
            "channel",
            "speed_consistency",
            "state",
        ),
        "hdmap_projection": (
            "official_source_available",
            "claim_grade",
            "sim_time_coverage_ratio",
            "projection_s_coverage_m",
            "route_s_coverage_ratio",
            "heading_error_p95_rad",
            "lateral_error_p95_m",
        ),
        "planning_reference_line": (
            "metrics",
            "evidence",
        ),
        "apollo_lateral_semantics": (
            "suspected_layer",
            "confidence",
            "anomaly_types",
            "hdmap_route_lateral_consistency_status",
            "hdmap_route_lateral_interpretation",
            "first_high_lateral_route_s",
            "first_high_lateral_sim_time",
            "max_abs_lateral_route_s",
            "max_abs_lateral_sim_time",
            "cross_track_error_abs_p95",
            "apollo_simple_lat_lateral_error_abs_p95",
            "route_s_vs_apollo_current_station_abs_delta_p95",
            "ego_to_apollo_matched_point_xy_distance_p95",
            "route_to_apollo_matched_point_xy_distance_p95",
            "reference_debug_status",
            "reference_line_provider_ready_ratio",
            "reference_line_count_zero_ratio",
            "apollo_steer_raw_abs_p95",
            "carla_steer_applied_abs_p95",
        ),
        "route_establishment": (
            "planning_message_count",
            "nonempty_trajectory_count",
            "nonempty_trajectory_ratio",
            "after_routing_success_nonempty_ratio",
            "route_established",
            "route_contract_status",
            "scenario_route_length_m",
            "scenario_route_length_source",
            "scenario_route_claim_length_m",
            "scenario_route_claim_length_source",
            "scenario_route_legacy_length_m",
            "scenario_route_legacy_length_role",
            "scenario_route_trace_length_m",
            "scenario_route_length_consistency_status",
            "apollo_routing_total_length_m",
            "routing_length_ratio",
            "summary_fail_reason",
        ),
        "apollo_module_consumption": (
            "routing_response_consumed_by_planning",
            "pattern_counts",
            "empty_reason_histogram",
            "planning_input_age",
            "prediction_mode",
        ),
        "control_mapping_apply": (
            "failure_reason",
            "oscillation_decomposition",
            "control_mapping_claim_boundary",
            "route_s_after_first_applied_control_delta_m",
        ),
        "prediction_evidence": (
            "prediction_mode",
            "prediction_channel_available",
            "prediction_message_count",
            "planning_requires_prediction",
            "hard_gate_eligible",
            "bypass_reason",
        ),
        "no_assist_claim_boundary": (
            "backend",
            "control_source",
            "explicit_control_source",
            "apollo_control_topic_observed",
            "active_assists",
            "blocking_assists",
            "planning_nonempty_ratio",
            "lateral_guard_apply_count",
            "force_green",
        ),
    }
    keys = preferred_by_layer.get(layer_name) or tuple(metrics.keys())
    parts: list[str] = []
    for key in keys:
        if key not in metrics:
            continue
        value = metrics.get(key)
        if value is None or value == [] or value == {}:
            continue
        parts.append(f"{key}={_summary_metric_value(value)}")
        if len(parts) >= 8:
            break
    text = "; ".join(parts)
    return text[:900] + ("..." if len(text) > 900 else "")


def _summary_metric_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, (str, int, bool)):
        return str(value)
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except TypeError:
        return str(value)


def _resolve_inputs(root: Path) -> dict[str, Path | None]:
    return {
        "summary": _find_first(root, ["summary.json"]),
        "manifest": _find_first(root, ["manifest.json"]),
        "suite_manifest": _find_first(root, ["suite_manifest.json", "../suite_manifest.json", "../../suite_manifest.json"]),
        "cyber_bridge_stats": _find_first(root, ["artifacts/cyber_bridge_stats.json", "cyber_bridge_stats.json"]),
        "bridge_health_summary": _find_first(
            root,
            [
                "artifacts/bridge_health_summary.json",
                "artifacts/bridge_health_summary.finalized.json",
                "bridge_health_summary.json",
            ],
        ),
        "bridge_transport_summary": _find_first(
            root,
            [
                "artifacts/bridge_transport_summary.json",
                "artifacts/transport_summary.json",
                "bridge_transport_summary.json",
            ],
        ),
        "planning_topic_debug_summary": _find_first(
            root,
            [
                "artifacts/planning_topic_debug_summary.json",
                "planning_topic_debug_summary.json",
            ],
        ),
        "command_materialization_summary": _find_first(
            root,
            [
                "artifacts/command_materialization_summary.json",
                "command_materialization_summary.json",
            ],
        ),
        "planning_materialization": _find_first(
            root,
            [
                "analysis/planning_materialization/planning_materialization_report.json",
                "planning_materialization_report.json",
            ],
        ),
        "carla_tick_health_summary": _find_first(
            root,
            [
                "artifacts/carla_tick_health_summary.json",
                "carla_tick_health_summary.json",
            ],
        ),
        "carla_tick_health_log": _find_first(
            root,
            [
                "artifacts/carla_tick_health.jsonl",
                "carla_tick_health.jsonl",
            ],
        ),
        "carla_world_ready_summary": _find_first(
            root,
            [
                "artifacts/carla_world_ready_summary.json",
                "carla_world_ready_summary.json",
            ],
        ),
        "carla_bootstrap_summary": _find_first(
            root,
            [
                "artifacts/carla_bootstrap_summary.json",
                "carla_bootstrap_summary.json",
            ],
        ),
        "apollo_channel_health": _find_first(
            root,
            [
                "analysis/apollo_channel_health/apollo_channel_health_report.json",
                "apollo_channel_health_report.json",
                "artifacts/apollo_channel_health_report.json",
            ],
        ),
        "channel_cadence_diagnosis": _find_first(
            root,
            [
                "analysis/channel_cadence_diagnosis/channel_cadence_diagnosis_report.json",
                "channel_cadence_diagnosis_report.json",
            ],
        ),
        "channel_stats": _find_first(root, ["channel_stats.json", "artifacts/channel_stats.json"]),
        "localization_contract": _find_first(
            root,
            [
                "analysis/localization_contract/localization_contract_report.json",
                "localization_contract_report.json",
            ],
        ),
        "chassis_gt_contract": _find_first(
            root,
            [
                "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
                "chassis_gt_contract_report.json",
            ],
        ),
        "apollo_reference_line_contract": _find_first(
            root,
            [
                "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
                "apollo_reference_line_contract_report.json",
            ],
        ),
        "apollo_lateral_semantics": _find_first(
            root,
            [
                "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
                "apollo_lateral_semantics_report.json",
            ],
        ),
        "apollo_hdmap_projection": _find_first(
            root,
            [
                "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
                "apollo_hdmap_projection_report.json",
            ],
        ),
        "apollo_route_contract": _find_first(
            root,
            [
                "analysis/apollo_route_contract/apollo_route_contract_report.json",
                "apollo_route_contract_report.json",
            ],
        ),
        "apollo_module_consumption": _find_first(
            root,
            [
                "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
                "apollo_module_consumption_report.json",
            ],
        ),
        "apollo_control_handoff": _find_first(
            root,
            [
                "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
                "apollo_control_handoff_report.json",
                "artifacts/control_handoff_summary.json",
            ],
        ),
        "control_health": _find_first(
            root,
            [
                "analysis/control_health/control_health_report.json",
                "control_health_report.json",
            ],
        ),
        "traffic_light_contract": _find_first(
            root,
            [
                "analysis/traffic_light_contract/traffic_light_contract_report.json",
                "traffic_light_contract_report.json",
            ],
        ),
        "prediction_evidence": _find_first(
            root,
            [
                "analysis/prediction_evidence/prediction_evidence_report.json",
                "prediction_evidence_report.json",
            ],
        ),
        "obstacle_gt_contract": _find_first(
            root,
            [
                "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
                "obstacle_gt_contract_report.json",
            ],
        ),
        "obstacle_gt_contract_raw": _find_first(
            root,
            [
                "artifacts/obstacle_gt_contract.jsonl",
                "obstacle_gt_contract.jsonl",
                "artifacts/obstacle_gt_contract.json",
                "obstacle_gt_contract.json",
            ],
        ),
        "natural_driving_report": _find_natural_driving_report(root),
        "followstop_child_stderr": _find_followstop_child_stderr(root),
    }


def _environment_world_layer(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    carla_world_ready_summary: Mapping[str, Any],
    carla_bootstrap_summary: Mapping[str, Any],
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    runtime_status = _runtime_contract_status(summary, manifest)
    carla_world = manifest.get("carla_world") if isinstance(manifest.get("carla_world"), Mapping) else {}
    world_ready_status = _first_text(carla_world_ready_summary, "status")
    bootstrap_status = _first_text(carla_bootstrap_summary, "carla_bootstrap_status")
    final_town = _first_text(carla_world_ready_summary, "final_town")
    matches_town = _bool_or_none(carla_world.get("matches_configured_town"))
    tick_timeout = _detect_carla_world_tick_timeout(summary, inputs)
    tick_cadence = _carla_tick_cadence(inputs)
    blocking: list[str] = []
    warnings: list[str] = []
    if tick_cadence.get("inter_tick_wall_gap_high") is True:
        warnings.append("carla_inter_tick_wall_interval_high")
    if world_ready_status and world_ready_status != "world_ready":
        status = "fail"
        blocking.append(_world_ready_blocker(world_ready_status))
    elif tick_timeout["detected"]:
        status = "fail"
        blocking.append("carla_world_tick_timeout_before_routing")
        warnings.append("routing_request_count_zero_is_downstream_of_world_tick_timeout")
    elif not summary and not manifest:
        status = "insufficient_data"
        blocking.append("summary_or_manifest_missing")
    elif runtime_status and runtime_status != "aligned":
        status = "fail"
        blocking.append(f"runtime_contract_{runtime_status}")
    elif matches_town is False:
        status = "fail"
        blocking.append("carla_world_not_matching_configured_town")
    elif runtime_status == "aligned" or matches_town is True:
        status = "pass"
    else:
        status = "insufficient_data"
        warnings.append("runtime_contract_or_world_identity_missing")
    return _layer(
        status=status,
        blocking_reasons=blocking,
        warnings=warnings,
        key_metrics={
            "runtime_contract_status": runtime_status,
            "carla_world_ready_status": world_ready_status,
            "carla_bootstrap_status": bootstrap_status,
            "loaded_map_name": carla_world.get("loaded_map_name") or final_town,
            "configured_town": carla_world.get("configured_town"),
            "carla_final_town": final_town,
            "spawn_point_count": carla_world.get("spawn_point_count"),
            "carla_world_tick_timeout_detected": tick_timeout["detected"],
            "carla_world_tick_timeout_artifact": tick_timeout["artifact_path"],
            "carla_tick_count": tick_timeout.get("tick_count"),
            "carla_tick_fail_count": tick_timeout.get("tick_fail_count"),
            "carla_tick_max_wall_duration_s": tick_timeout.get("max_tick_wall_duration_s"),
            "carla_tick_max_inter_tick_wall_interval_s": tick_cadence.get("max_inter_tick_wall_interval_s"),
            "carla_tick_inter_tick_wall_interval_p95_s": tick_cadence.get("inter_tick_wall_interval_p95_s"),
            "carla_tick_inter_tick_wall_interval_count": tick_cadence.get("inter_tick_wall_interval_count"),
            "carla_tick_cadence_source": tick_cadence.get("source"),
            "carla_tick_inter_tick_wall_gap_high": tick_cadence.get("inter_tick_wall_gap_high"),
            "carla_tick_max_frame_post_tick_wall_duration_s": tick_cadence.get("max_frame_post_tick_wall_duration_s"),
            "carla_tick_max_frame_loop_wall_duration_s": tick_cadence.get("max_frame_loop_wall_duration_s"),
            "carla_tick_max_stage_name": tick_cadence.get("max_stage_name"),
            "carla_tick_max_stage_duration_s": tick_cadence.get("max_stage_duration_s"),
            "carla_tick_max_hook_stage_name": tick_cadence.get("max_hook_stage_name"),
            "carla_tick_max_hook_stage_duration_s": tick_cadence.get("max_hook_stage_duration_s"),
            "carla_tick_stage_duration_max_s": tick_cadence.get("stage_duration_max_s"),
            "summary_fail_reason": summary.get("fail_reason"),
            "summary_frames": summary.get("frames"),
            "summary_routing_request_count": summary.get("routing_request_count"),
        },
        artifact_paths=_paths(
            inputs,
            "summary",
            "manifest",
            "carla_world_ready_summary",
            "carla_bootstrap_summary",
            "carla_tick_health_summary",
            "carla_tick_health_log",
            "followstop_child_stderr",
        ),
        next_action=_environment_world_next_action(runtime_status, blocking),
    )


def _bridge_runtime_layer(
    summary: Mapping[str, Any],
    payloads: Mapping[str, Mapping[str, Any]],
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    bridge_health = payloads.get("bridge_health_summary", {})
    transport = payloads.get("bridge_transport_summary", {})
    cyber_stats = payloads.get("cyber_bridge_stats", {})
    preflight = _first_text(summary, "bridge_runtime_preflight_status", bridge_health, "bridge_runtime_preflight_status")
    runtime_import_ok = _bool_or_none(_first_raw(summary, "bridge_runtime_import_ok", bridge_health, "bridge_runtime_import_ok"))
    routing_materialized = _bool_or_none(summary.get("routing_materialized"))
    planning_materialized = _bool_or_none(summary.get("planning_materialized"))
    missing = [
        name
        for name in ("cyber_bridge_stats", "bridge_health_summary", "bridge_transport_summary")
        if inputs.get(name) is None
    ]
    blocking: list[str] = []
    warnings: list[str] = []
    if preflight == "bridge_runtime_import_failed" or runtime_import_ok is False:
        status = "fail"
        blocking.append("bridge_runtime_import_failed")
    elif missing:
        status = "insufficient_data"
        warnings.extend(f"{name}_missing" for name in missing)
    elif routing_materialized is False and planning_materialized is False:
        status = "fail"
        blocking.append("bridge_materialization_missing")
    else:
        status = "warn" if warnings else "pass"
    return _layer(
        status=status,
        blocking_reasons=blocking,
        warnings=warnings,
        key_metrics={
            "bridge_runtime_preflight_status": preflight,
            "bridge_runtime_import_ok": runtime_import_ok,
            "routing_materialized": routing_materialized,
            "planning_materialized": planning_materialized,
            "routing_success_count": _first_raw(summary, "routing_success_count", cyber_stats, "routing_success_count"),
            "control_rx_count": _first_raw(cyber_stats, "control_rx_count", cyber_stats, "control", "rx_count"),
            "transport_status": transport.get("status") or transport.get("verdict"),
        },
        artifact_paths=_paths(inputs, "summary", "cyber_bridge_stats", "bridge_health_summary", "bridge_transport_summary"),
        next_action="Resolve bridge runtime/materialization before routing, planning, or actuation interpretation.",
    )


def _localization_layer(report: Mapping[str, Any] | None, path: Path | None) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(
            path=path,
            next_action="Generate localization_contract_report.json; verify sim_time, frame_id, VRP, heading, velocity, and lane projection.",
        )
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    blocking = list(verdict.get("blocking_reasons") or report.get("blocking_reasons") or [])
    warnings = list(report.get("warnings") or [])
    status = _normalize_status(verdict.get("status") or report.get("status"))
    lateral_consistency = report.get("hdmap_route_lateral_consistency")
    lateral_consistency = lateral_consistency if isinstance(lateral_consistency, Mapping) else {}
    next_action = "Fix GT localization contract before blaming reference-line, control, curve, junction, or traffic-light behavior."
    if lateral_consistency.get("interpretation") == "hdmap_lateral_matches_route_cross_track_actual_lateral_drift":
        next_action = (
            "HDMap lateral error matches CARLA route cross-track; inspect Apollo lateral "
            "target/reference semantics and raw/mapped/applied steering before changing map or transform."
        )
    return _layer(
        status=status,
        blocking_reasons=blocking,
        warnings=warnings,
        key_metrics={
            "channel_status": _nested(report, "channel.status"),
            "heading_error_to_route_p95_rad": _nested(report, "pose_consistency.heading_error_to_route_p95_rad"),
            "heading_error_to_lane_p95_rad": _nested(report, "pose_consistency.heading_error_to_lane_p95_rad"),
            "position_uses_vrp": _nested(report, "reference_point.position_uses_vrp"),
            "vehicle_reference_hard_gate_eligible": _nested(report, "reference_point.vehicle_reference_hard_gate_eligible"),
            "measurement_header_delta_ms_p95": _nested(report, "time.measurement_header_delta_ms_p95"),
            "hdmap_route_lateral_consistency_status": lateral_consistency.get("status"),
            "hdmap_route_lateral_alignment_mode": lateral_consistency.get("alignment_mode"),
            "hdmap_route_lateral_delta_p95_m": lateral_consistency.get("best_abs_delta_p95_m"),
            "hdmap_route_lateral_interpretation": lateral_consistency.get("interpretation"),
        },
        artifact_paths={"localization_contract": str(path) if path else None},
        next_action=next_action,
    )


def _chassis_gt_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    run_dir: Path | None,
) -> dict[str, Any]:
    source_report: Mapping[str, Any] | None = report
    source_kind = "report"
    if _should_regenerate_chassis_gt_contract(report) and run_dir is not None:
        regenerated = analyze_chassis_gt_contract_files(run_dir=run_dir)
        if _chassis_gt_contract_has_runtime_evidence(regenerated) or not report:
            source_report = regenerated
            source_kind = "regenerated_from_run_artifacts"
    layer = _report_layer(
        name="chassis_gt_contract",
        report=source_report,
        path=path,
        status_keys=("status",),
        blocking_keys=("blocking_reasons",),
        warning_keys=("warnings",),
        next_action=(
            "Generate chassis_gt_contract_report.json and verify chassis speed, "
            "driving mode, gear, and error-code semantics."
        ),
        key_metric_fields=("channel", "speed_consistency", "state", "claim_grade", "missing_fields"),
    )
    layer["artifact_paths"]["source_kind"] = source_kind
    if run_dir is not None:
        layer["artifact_paths"]["run_dir"] = str(run_dir)
    return layer


def _should_regenerate_chassis_gt_contract(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return True
    status = _normalize_status(report.get("status") or _nested(report, "verdict.status"))
    if status == "fail":
        return False
    blocking = {str(item) for item in report.get("blocking_reasons") or [] if item}
    if blocking and blocking != {"chassis_runtime_samples_missing"}:
        return False
    return (
        status == "insufficient_data"
        and (
            not isinstance(report.get("channel"), Mapping)
            or "chassis_runtime_samples_missing" in blocking
            or not isinstance(report.get("speed_consistency"), Mapping)
        )
    )


def _chassis_gt_contract_has_runtime_evidence(report: Mapping[str, Any]) -> bool:
    channel = report.get("channel") if isinstance(report.get("channel"), Mapping) else {}
    speed = report.get("speed_consistency") if isinstance(report.get("speed_consistency"), Mapping) else {}
    return bool(
        _first_num(channel.get("message_count")) is not None
        or _first_num(speed.get("sample_count")) is not None
    )


def _apollo_lateral_semantics_layer(report: Mapping[str, Any] | None, path: Path | None) -> dict[str, Any]:
    if not report:
        return _layer(
            status="not_applicable",
            blocking_reasons=[],
            warnings=["apollo_lateral_semantics_report_missing"],
            key_metrics={},
            artifact_paths={"apollo_lateral_semantics": str(path) if path else None},
            next_action=(
                "Generate apollo_lateral_semantics_report.json when lateral drift, matched/target point, "
                "or source-steer behavior needs attribution."
            ),
        )
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    anomaly_types = _lateral_semantics_anomaly_types(report)
    suspected_layer = str(report.get("suspected_layer") or verdict.get("suspected_layer") or "")
    confidence = str(report.get("confidence") or verdict.get("confidence") or "")
    status = _normalize_status(verdict.get("status") or report.get("status"))
    warnings = list(report.get("warnings") or [])
    if anomaly_types and "lateral_semantics_anomaly" not in warnings:
        warnings.append("lateral_semantics_anomaly")
    next_action = "Use this report as attribution evidence only; do not treat suspected_layer as definitive root cause."
    if suspected_layer == "target_point_semantics":
        next_action = (
            "Inspect Apollo target/matched point semantics, source-steer generation, and the route_s window "
            "where HDMap projection and CARLA cross-track both show drift."
        )
    elif suspected_layer == "reference_line_semantics":
        next_action = "Inspect reference-line curvature/heading and Apollo routing lane ids before changing control mapping."
    elif suspected_layer == "control_mapping":
        next_action = "Inspect raw/mapped/applied steer consistency before changing steer_scale or controller parameters."
    elif suspected_layer == "vehicle_response":
        next_action = "Inspect CARLA vehicle response and calibration profile after upstream reference-line evidence is non-blocking."
    return _layer(
        status=status,
        blocking_reasons=[],
        warnings=warnings,
        key_metrics={
            "suspected_layer": suspected_layer or None,
            "confidence": confidence or None,
            "failure_reason": verdict.get("failure_reason") or report.get("failure_reason"),
            "anomaly_types": anomaly_types,
            "hdmap_route_lateral_consistency_status": _nested(
                report,
                "hdmap_route_lateral_consistency.status",
            ),
            "hdmap_route_lateral_interpretation": _nested(
                report,
                "hdmap_route_lateral_consistency.interpretation",
            ),
            "hdmap_route_lateral_delta_p95_m": _nested(
                report,
                "hdmap_route_lateral_consistency.best_abs_delta_p95_m",
            ),
            "first_high_lateral_route_s": _nested(
                report,
                "drift_window_summary.first_high_lateral.route_s",
            ),
            "first_high_lateral_sim_time": _nested(
                report,
                "drift_window_summary.first_high_lateral.sim_time",
            ),
            "max_abs_lateral_route_s": _nested(
                report,
                "drift_window_summary.max_abs_lateral.route_s",
            ),
            "max_abs_lateral_sim_time": _nested(
                report,
                "drift_window_summary.max_abs_lateral.sim_time",
            ),
            "cross_track_error_abs_p95": _nested(report, "correlation_summary.cross_track_error_abs.p95"),
            "apollo_simple_lat_lateral_error_abs_p95": _nested(
                report,
                "correlation_summary.apollo_simple_lat_lateral_error_abs.p95",
            ),
            "route_s_vs_apollo_current_station_abs_delta_p95": _nested(
                report,
                "correlation_summary.route_s_vs_apollo_current_station_abs_delta.p95",
            ),
            "ego_to_apollo_matched_point_xy_distance_p95": _nested(
                report,
                "correlation_summary.ego_to_apollo_matched_point_xy_distance.p95",
            ),
            "route_to_apollo_matched_point_xy_distance_p95": _nested(
                report,
                "correlation_summary.route_to_apollo_matched_point_xy_distance.p95",
            ),
            "reference_debug_status": _nested(report, "reference_debug_summary.status"),
            "reference_line_provider_ready_ratio": _nested(
                report,
                "reference_debug_summary.reference_line_provider_ready_ratio",
            ),
            "reference_line_count_zero_ratio": _nested(
                report,
                "reference_debug_summary.reference_line_count_zero_ratio",
            ),
            "apollo_steer_raw_abs_p95": _nested(report, "correlation_summary.apollo_steer_raw_abs.p95"),
            "carla_steer_applied_abs_p95": _nested(report, "correlation_summary.carla_steer_applied_abs.p95"),
        },
        artifact_paths={"apollo_lateral_semantics": str(path) if path else None},
        next_action=next_action,
    )


def _lateral_semantics_anomaly_types(report: Mapping[str, Any]) -> list[str]:
    anomalies = report.get("anomalies") if isinstance(report.get("anomalies"), list) else []
    result: list[str] = []
    for item in anomalies:
        if isinstance(item, Mapping) and item.get("type") not in {None, ""}:
            result.append(str(item.get("type")))
    return result


def _hdmap_projection_layer(
    projection_report: Mapping[str, Any] | None,
    localization_report: Mapping[str, Any] | None,
    reference_line_report: Mapping[str, Any] | None,
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    projection = {}
    if isinstance(projection_report, Mapping) and projection_report:
        nested_projection = projection_report.get("projection")
        projection = dict(nested_projection if isinstance(nested_projection, Mapping) else projection_report)
    for report in (reference_line_report, localization_report):
        if projection:
            break
        candidate = report.get("apollo_hdmap_projection") if isinstance(report, Mapping) else None
        if isinstance(candidate, Mapping) and candidate:
            projection = dict(candidate)
            break
    if not projection:
        return _layer(
            status="insufficient_data",
            blocking_reasons=[],
            warnings=["apollo_hdmap_projection_missing"],
            key_metrics={"apollo_hdmap_projection_available": False},
            artifact_paths=_paths(inputs, "apollo_hdmap_projection", "localization_contract", "apollo_reference_line_contract"),
            next_action="Generate artifacts/apollo_hdmap_projection.jsonl using Apollo HDMap API projection evidence.",
        )
    insufficient_reasons = list(projection.get("insufficient_reasons") or [])
    warnings = list(projection.get("warnings") or [])
    warnings.extend(str(item) for item in insufficient_reasons if item)
    next_action = "If projection is high-error, check map alignment, lane direction, lane id, and routing snap."
    if any("coverage" in str(item) or "sample_count" in str(item) for item in insufficient_reasons):
        next_action = (
            "Run a longer claim-window sample or export denser Apollo HDMap projection rows; "
            "current projection quality may be good but coverage is not claim-grade."
        )
    if projection.get("empty_reason"):
        next_action = str(projection.get("next_action") or next_action)
    return _layer(
        status=_normalize_status(projection.get("status")),
        blocking_reasons=list(projection.get("blocking_reasons") or []),
        warnings=warnings,
        key_metrics={
            "file_present": projection.get("file_present"),
            "official_source_available": projection.get("official_source_available"),
            "claim_grade": projection.get("claim_grade"),
            "empty_reason": projection.get("empty_reason"),
            "insufficient_reasons": insufficient_reasons,
            "heading_error_p95_rad": projection.get("heading_error_p95_rad"),
            "lateral_error_p95_m": projection.get("lateral_error_p95_m"),
            "sim_time_coverage_ratio": projection.get("sim_time_coverage_ratio"),
            "projection_s_coverage_m": projection.get("projection_s_coverage_m"),
            "route_s_coverage_ratio": projection.get("route_s_coverage_ratio"),
            "nearest_lane_id_topk": projection.get("nearest_lane_id_topk"),
        },
        artifact_paths=_paths(inputs, "apollo_hdmap_projection", "localization_contract", "apollo_reference_line_contract"),
        next_action=next_action,
    )


def _should_regenerate_planning_materialization(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return False
    status = _normalize_status(report.get("verdict") or report.get("status"))
    if status == "fail":
        return False
    blocking = {str(item) for item in report.get("blocking_reasons") or [] if item}
    if blocking and blocking != {"planning_runtime_messages_missing"}:
        return False
    return (
        status == "insufficient_data"
        and (
            "planning_runtime_messages_missing" in blocking
            or _first_num(report.get("planning_message_count")) is None
        )
    )


def _planning_materialization_has_runtime_evidence(report: Mapping[str, Any]) -> bool:
    return _first_num(report.get("planning_message_count")) is not None


def _should_regenerate_apollo_module_consumption(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return False
    status = _normalize_status(report.get("status"))
    if status == "fail":
        return False
    blocking = {str(item) for item in report.get("blocking_reasons") or [] if item}
    if blocking and blocking != {"apollo_module_runtime_logs_missing"}:
        return False
    return (
        status == "insufficient_data"
        and (
            "apollo_module_runtime_logs_missing" in blocking
            or not isinstance(report.get("pattern_counts"), Mapping)
        )
    )


def _apollo_module_consumption_has_runtime_evidence(report: Mapping[str, Any]) -> bool:
    source = report.get("source") if isinstance(report.get("source"), Mapping) else {}
    return bool(
        report.get("routing_response_consumed_by_planning") is not None
        or isinstance(report.get("pattern_counts"), Mapping)
        or source.get("planning_topic_debug")
    )


def _should_regenerate_apollo_route_contract(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return True
    if report.get("schema_version") != "apollo_route_contract.v1":
        return True
    decoded = report.get("routing_response_decoded")
    decoded_available = isinstance(decoded, Mapping) and decoded.get("available") is True
    blockers = {str(item) for item in (report.get("blocking_reasons") or []) if item}
    status = _normalize_status(report.get("status"))
    if status == "insufficient_data" and "routing_response_runtime_evidence_missing" in blockers:
        return True
    if not decoded_available and "routing_response_runtime_evidence_missing" in blockers:
        return True
    return False


def _apollo_route_contract_raw_inputs_available(root: Path) -> bool:
    return any(
        (root / relative).is_file()
        for relative in (
            "artifacts/routing_response_decoded.json",
            "artifacts/routing_response_decoded.jsonl",
            "routing_response_decoded.json",
            "routing_response_decoded.jsonl",
        )
    )


def _apollo_route_contract_has_runtime_evidence(report: Mapping[str, Any]) -> bool:
    decoded = report.get("routing_response_decoded")
    last_response = report.get("last_routing_response")
    return bool(
        (isinstance(decoded, Mapping) and decoded.get("available") is True)
        or (isinstance(last_response, Mapping) and last_response.get("available") is True)
    )


def _should_regenerate_prediction_evidence(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return False
    status = _normalize_status(report.get("verdict") or report.get("status"))
    if status == "fail":
        return False
    mode = str(report.get("prediction_mode") or "").strip()
    blockers = {str(item) for item in report.get("blocking_capabilities") or [] if item}
    return status == "insufficient_data" and (
        mode in {"", "unknown"} or "prediction_status_unknown" in blockers
    )


def _prediction_evidence_has_boundary(report: Mapping[str, Any]) -> bool:
    return str(report.get("prediction_mode") or "").strip() not in {"", "unknown"}


def _should_regenerate_channel_cadence(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return True
    if report.get("schema_version") != "channel_cadence_diagnosis.v1":
        return True
    return _normalize_status(report.get("status")) == "insufficient_data"


def _route_establishment_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    source_kind: str,
    route_contract: Mapping[str, Any] | None,
    route_contract_path: Path | None,
    route_contract_source_kind: str,
    summary: Mapping[str, Any],
    bridge_stats: Mapping[str, Any],
    planning_topic_debug_summary: Mapping[str, Any],
) -> dict[str, Any]:
    route_contract_status = _normalize_status(route_contract.get("status")) if route_contract else "insufficient_data"
    route_contract_blocking = list(route_contract.get("blocking_reasons") or []) if route_contract else []
    route_contract_warnings = list(route_contract.get("warnings") or []) if route_contract else ["apollo_route_contract_missing"]
    if report:
        route_establishment = report.get("route_establishment")
        if not isinstance(route_establishment, Mapping):
            route_establishment = {}
        blocking = list(report.get("blocking_reasons") or [])
        blocking.extend(route_contract_blocking)
        if route_establishment.get("route_established") is False:
            blocking.extend(route_establishment.get("blocking_reasons") or [])
        status = _normalize_status(report.get("verdict") or report.get("status"))
        if route_contract_status == "fail":
            status = "fail"
        elif route_contract_status == "insufficient_data" and status == "pass":
            status = "insufficient_data"
        return _layer(
            status=status,
            blocking_reasons=blocking,
            warnings=list(report.get("warnings") or []) + route_contract_warnings,
            key_metrics={
                "planning_message_count": report.get("planning_message_count"),
                "materialization_status": report.get("materialization_status")
                or report.get("materialization_observation_status"),
                "nonempty_trajectory_count": report.get("nonempty_trajectory_count"),
                "nonempty_trajectory_ratio": report.get("nonempty_trajectory_ratio"),
                "after_routing_success_nonempty_ratio": report.get(
                    "after_routing_success_nonempty_ratio"
                ),
                "first_nonempty_after_routing_latency_s": report.get(
                    "first_nonempty_after_routing_latency_s"
                ),
                "longest_empty_streak": report.get("longest_empty_streak"),
                "route_established": route_establishment.get("route_established"),
                "route_completion_ratio": route_establishment.get("route_completion_ratio"),
                "summary_fail_reason": summary.get("fail_reason"),
                "route_contract_status": route_contract_status,
                "route_contract_missing_fields": (
                    list(route_contract.get("missing_fields") or []) if route_contract else []
                ),
                "insufficient_reasons": (
                    list(route_contract.get("missing_fields") or [])
                    if route_contract_status == "insufficient_data" and route_contract
                    else []
                ),
                "scenario_route_length_m": route_contract.get("scenario_route_length_m") if route_contract else None,
                "scenario_route_length_source": route_contract.get("scenario_route_length_source") if route_contract else None,
                "scenario_route_declared_length_m": route_contract.get("scenario_route_declared_length_m") if route_contract else None,
                "scenario_route_claim_length_m": route_contract.get("scenario_route_claim_length_m") if route_contract else None,
                "scenario_route_claim_length_source": route_contract.get("scenario_route_claim_length_source") if route_contract else None,
                "scenario_route_legacy_length_m": route_contract.get("scenario_route_legacy_length_m") if route_contract else None,
                "scenario_route_legacy_length_role": route_contract.get("scenario_route_legacy_length_role") if route_contract else None,
                "scenario_route_trace_length_m": route_contract.get("scenario_route_trace_length_m") if route_contract else None,
                "scenario_route_length_consistency_status": (
                    route_contract.get("scenario_route_length_consistency_status") if route_contract else None
                ),
                "scenario_route_length_consistency_reason": (
                    route_contract.get("scenario_route_length_consistency_reason") if route_contract else None
                ),
                "apollo_routing_total_length_m": route_contract.get("apollo_routing_total_length_m") if route_contract else None,
                "routing_length_ratio": route_contract.get("routing_length_ratio") if route_contract else None,
                "apollo_routing_lane_window_count": route_contract.get("apollo_routing_lane_window_count") if route_contract else None,
                "apollo_routing_lane_signature": route_contract.get("apollo_routing_lane_signature") if route_contract else None,
            },
            artifact_paths={
                "planning_materialization": str(path) if path else None,
                "apollo_route_contract": str(route_contract_path) if route_contract_path else None,
                "source_kind": source_kind,
                "route_contract_source_kind": route_contract_source_kind,
            },
            next_action=(
                "If route establishment fails, inspect Apollo route contract and planning "
                "materialization before control tuning; control rx/tx does not prove route progression."
            ),
        )

    planning_total = _first_num(planning_topic_debug_summary.get("total_messages_received"))
    planning_nonempty = _first_num(
        planning_topic_debug_summary.get("messages_with_nonzero_trajectory_points")
    )
    routing_success_count = _first_num(
        summary.get("routing_success_count"),
        bridge_stats.get("routing_success_count"),
    )
    fail_reason = _first_text(summary, "fail_reason", summary, "exit_reason")
    ratio = (
        float(planning_nonempty) / float(planning_total)
        if planning_total is not None and planning_total > 0 and planning_nonempty is not None
        else None
    )
    blocking: list[str] = []
    warnings: list[str] = list(route_contract_warnings)
    blocking.extend(route_contract_blocking)
    if planning_total is None:
        status = "insufficient_data"
        blocking.append("planning_materialization_report_or_summary_missing")
    elif ratio is not None and ratio >= 0.80 and fail_reason != "ROUTE_ESTABLISHMENT_LATENCY_SEC":
        status = "pass"
    else:
        status = "fail"
        if ratio is None or ratio < 0.80:
            blocking.append("planning_trajectory_materialization_low")
        if fail_reason == "ROUTE_ESTABLISHMENT_LATENCY_SEC":
            blocking.append("route_establishment_latency")
        if routing_success_count is None or routing_success_count < 1:
            blocking.append("routing_success_missing")
        warnings.append("planning_materialization_report_missing_using_summary_fallback")
    if route_contract_status == "fail":
        status = "fail"
    elif route_contract_status == "insufficient_data" and status == "pass":
        status = "insufficient_data"
    route_established = status == "pass"
    return _layer(
        status=status,
        blocking_reasons=blocking,
        warnings=warnings,
        key_metrics={
            "planning_message_count": planning_total,
            "materialization_status": (
                "missing"
                if planning_total is None
                else ("observed_nonempty" if (planning_nonempty or 0) > 0 else "observed_empty")
            ),
            "nonempty_trajectory_count": planning_nonempty,
            "nonempty_trajectory_ratio": ratio,
            "after_routing_success_nonempty_ratio": None,
            "route_established": route_established,
            "routing_success_count": routing_success_count,
            "summary_fail_reason": fail_reason,
            "route_contract_status": route_contract_status,
            "route_contract_missing_fields": (
                list(route_contract.get("missing_fields") or []) if route_contract else []
            ),
            "insufficient_reasons": (
                list(route_contract.get("missing_fields") or [])
                if route_contract_status == "insufficient_data" and route_contract
                else []
            ),
            "scenario_route_length_m": route_contract.get("scenario_route_length_m") if route_contract else None,
            "scenario_route_length_source": route_contract.get("scenario_route_length_source") if route_contract else None,
            "scenario_route_declared_length_m": route_contract.get("scenario_route_declared_length_m") if route_contract else None,
            "scenario_route_claim_length_m": route_contract.get("scenario_route_claim_length_m") if route_contract else None,
            "scenario_route_claim_length_source": route_contract.get("scenario_route_claim_length_source") if route_contract else None,
            "scenario_route_legacy_length_m": route_contract.get("scenario_route_legacy_length_m") if route_contract else None,
            "scenario_route_legacy_length_role": route_contract.get("scenario_route_legacy_length_role") if route_contract else None,
            "scenario_route_trace_length_m": route_contract.get("scenario_route_trace_length_m") if route_contract else None,
            "scenario_route_length_consistency_status": (
                route_contract.get("scenario_route_length_consistency_status") if route_contract else None
            ),
            "scenario_route_length_consistency_reason": (
                route_contract.get("scenario_route_length_consistency_reason") if route_contract else None
            ),
            "apollo_routing_total_length_m": route_contract.get("apollo_routing_total_length_m") if route_contract else None,
            "routing_length_ratio": route_contract.get("routing_length_ratio") if route_contract else None,
        },
        artifact_paths={
            "planning_materialization": str(path) if path else None,
            "apollo_route_contract": str(route_contract_path) if route_contract_path else None,
            "source_kind": source_kind,
            "route_contract_source_kind": route_contract_source_kind,
        },
        next_action=(
            "Generate planning_materialization_report.json and apollo_route_contract_report.json; "
            "align empty trajectory rows with routing, localization/chassis freshness, "
            "HDMap projection, and planning logs."
        ),
    )


def _control_handoff_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    summary: Mapping[str, Any],
    bridge_stats: Mapping[str, Any],
    planning_topic_debug_summary: Mapping[str, Any],
    command_materialization_summary: Mapping[str, Any],
    command_materialization_path: Path | None,
) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(
            path=path,
            next_action="Generate apollo_control_handoff_report.json; inspect process, control channel, bridge receive, raw decode, apply, and response.",
        )
    routing_success_count = _first_num(
        summary.get("routing_success_count"),
        bridge_stats.get("routing_success_count"),
        report.get("routing_success_count"),
    )
    planning_total = _first_num(
        planning_topic_debug_summary.get("total_messages_received"),
        report.get("planning_total_messages"),
    )
    planning_nonzero = _first_num(
        planning_topic_debug_summary.get("messages_with_nonzero_trajectory_points"),
        report.get("planning_nonempty_messages"),
        _nested(report, "input_readiness.planning_nonempty_messages"),
    )
    control_rx_count = _first_num(
        bridge_stats.get("control_rx_count"),
        _nested(report, "bridge_receive.control_rx_count"),
        _nested(report, "control_channel.message_count"),
    )
    control_apply_count = _first_num(
        bridge_stats.get("apply_control_count"),
        _nested(report, "mapping_and_apply.apply_control_count"),
    )
    control_handoff_status = _first_text(
        report,
        "control_handoff_status",
        report,
        "failure_stage",
    )
    command_gate = command_materialization_summary.get("gate_state")
    if not isinstance(command_gate, Mapping):
        command_gate = {}
    command_path_stage = _first_text(command_materialization_summary, "command_path_stage")
    command_first_divergence_reason = _first_text(command_materialization_summary, "first_divergence_reason")
    command_last_blocking_reason = _first_text(command_gate, "last_blocking_reason")
    apollo_warmup_remaining_sec = _num(command_gate.get("apollo_warmup_remaining_sec"))
    startup_warmup_incomplete = bool(
        routing_success_count is not None
        and routing_success_count < 1
        and (
            command_first_divergence_reason == "apollo_startup_warmup"
            or command_last_blocking_reason == "apollo_startup_warmup"
        )
        and (apollo_warmup_remaining_sec is None or apollo_warmup_remaining_sec > 0.0)
    )
    blocking = list(report.get("blocking_reasons") or [])
    if startup_warmup_incomplete:
        blocking.append("apollo_startup_warmup_incomplete")
    elif routing_success_count is not None and routing_success_count < 1:
        blocking.append("routing_success_missing")
    if not startup_warmup_incomplete and planning_total is not None and planning_total > 0 and planning_nonzero is not None and planning_nonzero < 1:
        blocking.append("planning_nonempty_missing")
    if not startup_warmup_incomplete and control_handoff_status in {
        "control_process_missing",
        "planning_ready_control_not_consuming",
        "control_output_stopped_before_nonzero_planning",
        "control_process_crashed",
    }:
        blocking.append(control_handoff_status)
    process_health = report.get("process_health")
    if isinstance(process_health, Mapping):
        crash_reason = str(
            process_health.get("crash_reason")
            or process_health.get("fatal_signal")
            or process_health.get("failure_reason")
            or ""
        ).strip()
        if _bool_or_none(process_health.get("crash_detected")) is True or crash_reason:
            blocking.append("control_process_crash_before_control_output")
    if planning_nonzero is not None and planning_nonzero > 0 and control_rx_count is not None and control_rx_count < 1:
        blocking.append("control_rx_missing")
    if control_rx_count is not None and control_rx_count > 0 and control_apply_count is not None and control_apply_count < 1:
        blocking.append("control_apply_missing")
    explicit_status = report.get("verdict") or report.get("status")
    if startup_warmup_incomplete:
        status = "insufficient_data"
    elif blocking:
        status = "fail"
    elif explicit_status not in {None, ""}:
        status = _normalize_status(explicit_status)
    else:
        status = "pass" if control_handoff_status in {"control_consuming_with_nonzero_planning", "pass"} else "insufficient_data"
    return _layer(
        status=status,
        blocking_reasons=blocking,
        warnings=list(report.get("warnings") or []),
        key_metrics={
            "control_handoff_status": control_handoff_status,
            "failure_stage": report.get("failure_stage"),
            "evidence_level": report.get("evidence_level"),
            "routing_success_count": routing_success_count,
            "planning_total_messages": planning_total,
            "planning_nonempty_messages": planning_nonzero,
            "command_path_stage": command_path_stage,
            "command_first_divergence_reason": command_first_divergence_reason,
            "command_last_blocking_reason": command_last_blocking_reason,
            "apollo_warmup_remaining_sec": apollo_warmup_remaining_sec,
            "control_message_count": _nested(report, "control_channel.message_count"),
            "control_rx_count": control_rx_count,
            "apply_control_count": control_apply_count,
            "process_health": process_health if isinstance(process_health, Mapping) else None,
            "vehicle_response_status": _nested(report, "vehicle_response.status"),
        },
        artifact_paths={
            "apollo_control_handoff": str(path) if path else None,
            "command_materialization_summary": str(command_materialization_path) if command_materialization_path else None,
        },
        next_action="If failed, follow the handoff stage: process -> input readiness -> control channel -> bridge receive -> raw decode -> apply -> response.",
    )


def _control_health_layer(report: Mapping[str, Any] | None, path: Path | None) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(
            path=path,
            next_action="Generate control_health_report.json; inspect raw/mapped/applied controls, cadence, and vehicle response.",
        )
    semantics = _control_semantics_from_control_health(report)
    semantics_primary = report.get("control_semantics_primary_factor") or semantics.get(
        "primary_factor"
    )
    semantics_suspected = report.get("control_semantics_suspected_factors")
    if not isinstance(semantics_suspected, list):
        semantics_suspected = list(semantics.get("suspected_factors") or [])
    return _layer(
        status=_normalize_status(report.get("status")),
        blocking_reasons=[reason for reason in [report.get("failure_reason")] if reason and report.get("status") == "fail"],
        warnings=list(report.get("warnings") or []),
        key_metrics={
            "failure_reason": report.get("failure_reason"),
            "control_semantics_primary_factor": semantics_primary,
            "control_semantics_suspected_factors": semantics_suspected,
            "control_semantics_evidence": dict(semantics),
            "control_handoff_status": report.get("control_handoff_status"),
            "control_process_health": _nested(report, "metrics.control_process_health"),
            "oscillation_decomposition": _nested(report, "metrics.oscillation_decomposition"),
            "longitudinal_oscillation_attribution": _nested(
                report,
                "metrics.control_decode_debug.longitudinal_oscillation_attribution",
            ),
            "trajectory_consume_correlation": _nested(
                report,
                "metrics.control_decode_debug.trajectory_consume_correlation",
            ),
            "planning_trajectory_correlation": _nested(
                report,
                "metrics.control_decode_debug.planning_trajectory_correlation",
            ),
            "planning_log_fallback_diagnostics": _nested(
                report,
                "metrics.planning_log_fallback_diagnostics",
            ),
            "gt_state_sampling_cadence": _nested(
                report,
                "metrics.gt_state_sampling_cadence",
            ),
            "control_mapping_claim_boundary": _nested(report, "metrics.control_mapping_claim_boundary"),
            "mapped_applied_steer_abs_error_p95": _nested(report, "metrics.mapped_applied_steer_abs_error_p95"),
            "mapped_applied_throttle_abs_error_p95": _nested(report, "metrics.mapped_applied_throttle_abs_error_p95"),
            "mapped_applied_brake_abs_error_p95": _nested(report, "metrics.mapped_applied_brake_abs_error_p95"),
            "route_s_after_first_applied_control_delta_m": _nested(report, "metrics.route_s_after_first_applied_control_delta_m"),
        },
        artifact_paths={"control_health": str(path) if path else None},
        next_action=_control_health_next_action(semantics_primary),
    )


def _control_health_next_action(semantics_primary: Any) -> str:
    if semantics_primary:
        return (
            "If localization/reference-line is non-blocking, inspect Apollo Control simple_lon "
            f"and consumed trajectory evidence for `{semantics_primary}` before tuning bridge "
            "mapping or actuation."
        )
    return (
        "If localization/reference-line is non-blocking, inspect longitudinal_oscillation_attribution "
        "plus trajectory_consume_correlation/planning_trajectory_correlation before tuning bridge "
        "mapping or actuation."
    )


def _control_semantics_from_control_health(report: Mapping[str, Any]) -> dict[str, Any]:
    semantics = report.get("control_semantics_evidence")
    if isinstance(semantics, Mapping) and (
        semantics.get("primary_factor") or semantics.get("suspected_factors")
    ):
        return dict(semantics)

    control_debug = _nested(report, "metrics.control_decode_debug")
    if not isinstance(control_debug, Mapping):
        control_debug = {}
    sources: dict[str, Mapping[str, Any]] = {}
    for name in (
        "longitudinal_oscillation_attribution",
        "trajectory_consume_correlation",
        "planning_trajectory_correlation",
    ):
        value = control_debug.get(name)
        if isinstance(value, Mapping):
            sources[name] = value
    planning_log = _nested(report, "metrics.planning_log_fallback_diagnostics")
    if isinstance(planning_log, Mapping):
        sources["planning_log_fallback_diagnostics"] = planning_log

    suspected: list[str] = []
    dominant_by_source: dict[str, Any] = {}
    source_availability: dict[str, Any] = {}
    transition_counts: dict[str, Any] = {}
    for name, payload in sources.items():
        source_availability[name] = payload.get("available")
        if payload.get("transition_count") is not None:
            transition_counts[name] = payload.get("transition_count")
        dominant = payload.get("dominant_suspected_factor")
        if dominant:
            dominant_by_source[name] = dominant
            suspected.append(str(dominant))
        suspected.extend(str(item) for item in payload.get("suspected_factors") or [])

    suspected = _dedupe_preserve_order(suspected)
    primary_source = None
    primary_factor = None
    for name in (
        "planning_trajectory_correlation",
        "trajectory_consume_correlation",
        "longitudinal_oscillation_attribution",
        "planning_log_fallback_diagnostics",
    ):
        factor = dominant_by_source.get(name)
        if factor:
            primary_source = name
            primary_factor = factor
            break
    if primary_factor is None and suspected:
        primary_factor = suspected[0]
    return {
        "available": bool(primary_factor or suspected),
        "primary_source": primary_source,
        "primary_factor": primary_factor,
        "suspected_factors": suspected,
        "dominant_by_source": dominant_by_source,
        "source_availability": source_availability,
        "transition_counts": transition_counts,
    }


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _channel_health_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    stats_path: Path | None = None,
    scenario_class: str | None = None,
    cadence_report: Mapping[str, Any] | None = None,
    cadence_path: Path | None = None,
    cadence_source_kind: str = "report",
) -> dict[str, Any]:
    source_kind = "report"
    if _channel_report_needs_stats_fallback(report) and stats_path is not None and stats_path.exists():
        try:
            report = analyze_apollo_channel_health_files(
                "configs/algorithms/apollo_natural_driving_channels.yaml",
                stats_path,
                scenario_class=scenario_class,
            )
            source_kind = "regenerated_from_channel_stats"
        except Exception:
            source_kind = "report_stats_fallback_failed"
    if not report:
        return _missing_report_layer(
            path=path,
            next_action="Generate apollo_channel_health_report.json from channel_stats before behavior claims.",
        )
    cadence_report = cadence_report if isinstance(cadence_report, Mapping) else {}
    channel_results = report.get("channel_results") if isinstance(report.get("channel_results"), Mapping) else {}
    failed_channels: list[str] = []
    failed_details: dict[str, Any] = {}
    planning_result: Mapping[str, Any] = {}
    for key, value in channel_results.items():
        if not isinstance(value, Mapping):
            continue
        name = str(value.get("name") or key)
        channel = str(value.get("channel") or "")
        if name == "planning" or channel == "/apollo/planning" or "planning" in str(key):
            planning_result = value
        if _normalize_status(value.get("status")) == "fail":
            failed_channels.append(name)
            failed_details[name] = _channel_failure_detail(value)
    return _layer(
        status=_normalize_status(report.get("status")),
        blocking_reasons=_collect_lists(report, ("blocking_reasons", "errors")),
        warnings=_collect_lists(report, ("warnings",)),
        key_metrics={
            "missing_required_channels": report.get("missing_required_channels") or [],
            "low_rate_channels": report.get("low_rate_channels") or [],
            "timestamp_failures": report.get("timestamp_failures") or [],
            "failed_channels": failed_channels,
            "failed_channel_details": failed_details,
            "channel_cadence_status": cadence_report.get("status"),
            "channel_cadence_primary_issue": cadence_report.get("primary_cadence_issue"),
            "channel_cadence_primary_gap_source": cadence_report.get("primary_gap_source"),
            "channel_cadence_carla_tick_stage": cadence_report.get("carla_tick_stage"),
            "channel_cadence_publish_skip_reason": cadence_report.get("publish_skip_reason"),
            "channel_cadence_artifact_writer_lag_ms": cadence_report.get("artifact_writer_lag_ms"),
            "channel_cadence_blocking_reasons": cadence_report.get("blocking_reasons") or [],
            "channel_cadence_top_gap_windows": cadence_report.get("top_gap_windows") or [],
            "channel_cadence_next_action": cadence_report.get("next_action"),
            "planning_status": planning_result.get("status"),
            "planning_issues": list(planning_result.get("issues") or []),
            "planning_message_count": planning_result.get("message_count"),
            "planning_hz": planning_result.get("hz"),
            "planning_max_gap_ms": planning_result.get("max_gap_ms"),
            "planning_gap_p95_ms": planning_result.get("gap_p95_ms"),
            "planning_gap_count_over_1000ms": planning_result.get("gap_count_over_1000ms"),
            "planning_primary_time_axis": planning_result.get("primary_time_axis"),
            "planning_time_axis_diagnosis": planning_result.get("time_axis_diagnosis"),
            "planning_sim_time_hz": planning_result.get("sim_time_hz"),
            "planning_sim_time_max_gap_ms": planning_result.get("sim_time_max_gap_ms"),
            "planning_sim_time_gap_p95_ms": planning_result.get("sim_time_gap_p95_ms"),
            "planning_sim_time_gap_count_over_1000ms": planning_result.get("sim_time_gap_count_over_1000ms"),
            "planning_sim_time_timestamp_monotonic": planning_result.get("sim_time_timestamp_monotonic"),
            "planning_source": planning_result.get("source"),
        },
        artifact_paths={
            "channel_health": str(path) if path else None,
            "channel_stats": str(stats_path) if stats_path else None,
            "channel_cadence_diagnosis": str(cadence_path) if cadence_path else None,
            "source_kind": source_kind,
            "channel_cadence_source_kind": cadence_source_kind if cadence_report else None,
        },
        next_action=str(
            cadence_report.get("next_action")
            or "Generate/inspect apollo_channel_health_report.json; required channel absence is a transport issue, while planning-only gaps must be interpreted with planning/reference-line artifacts."
        ),
    )


def _channel_report_needs_stats_fallback(report: Mapping[str, Any] | None) -> bool:
    if not report:
        return True
    if not isinstance(report.get("channel_results"), Mapping) or not report.get("channel_results"):
        return True
    return False


def _channel_failure_detail(channel_result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "channel": channel_result.get("channel"),
        "issues": list(channel_result.get("issues") or []),
        "message_count": channel_result.get("message_count"),
        "hz": channel_result.get("hz"),
        "max_gap_ms": channel_result.get("max_gap_ms"),
        "gap_p95_ms": channel_result.get("gap_p95_ms"),
        "gap_count_over_1000ms": channel_result.get("gap_count_over_1000ms"),
        "primary_time_axis": channel_result.get("primary_time_axis"),
        "time_axis_diagnosis": channel_result.get("time_axis_diagnosis"),
        "sim_time_hz": channel_result.get("sim_time_hz"),
        "sim_time_max_gap_ms": channel_result.get("sim_time_max_gap_ms"),
        "sim_time_gap_p95_ms": channel_result.get("sim_time_gap_p95_ms"),
        "sim_time_gap_count_over_1000ms": channel_result.get("sim_time_gap_count_over_1000ms"),
        "sim_time_timestamp_monotonic": channel_result.get("sim_time_timestamp_monotonic"),
        "promotion_grade_evidence": channel_result.get("promotion_grade_evidence"),
        "source": channel_result.get("source"),
    }


def _traffic_light_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    scenario_class: str | None,
) -> dict[str, Any]:
    if scenario_class not in TRAFFIC_LIGHT_SCENARIOS and not report:
        return _layer(
            status="not_applicable",
            blocking_reasons=[],
            warnings=[],
            key_metrics={"scenario_class": scenario_class},
            artifact_paths={"traffic_light_contract": None},
            next_action="Traffic-light GT contract is required only for traffic-light scenarios.",
        )
    return _report_layer(
        name="traffic_light_gt",
        report=report,
        path=path,
        status_keys=("status",),
        blocking_keys=("errors", "blocking_reasons"),
        warning_keys=("warnings",),
        next_action="For traffic-light scenarios, verify carla_actual policy, HDMap signal id, stop line/lane overlap, color source, and confidence.",
        key_metric_fields=("claim_grade_ready", "claim_grade_requirements", "errors", "missing_inputs"),
    )


def _prediction_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    scenario_class: str | None,
    source_kind: str,
) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(
            path=path,
            next_action=(
                "Generate prediction_evidence_report.json. /apollo/perception/obstacles "
                "does not prove /apollo/prediction availability or a valid bypass boundary."
            ),
        )
    status = _normalize_status(report.get("verdict") or report.get("status"))
    blocking = list(report.get("blocking_capabilities") or [])
    if status in {"pass", "warn"} and report.get("hard_gate_eligible") is not True:
        blocking.append("prediction_not_hard_gate_eligible")
        status = "insufficient_data"
    return _layer(
        status=status,
        blocking_reasons=blocking,
        warnings=list(report.get("warnings") or []),
        key_metrics={
            "scenario_class": scenario_class,
            "prediction_mode": report.get("prediction_mode"),
            "prediction_channel_available": report.get("prediction_channel_available"),
            "prediction_message_count": report.get("prediction_message_count"),
            "prediction_hz": report.get("prediction_hz"),
            "planning_requires_prediction": report.get("planning_requires_prediction"),
            "hard_gate_eligible": report.get("hard_gate_eligible"),
            "bypass_reason": report.get("bypass_reason"),
            "blocking_capabilities": report.get("blocking_capabilities") or [],
        },
        artifact_paths={
            "prediction_evidence": str(path) if path else None,
            "source_kind": source_kind,
        },
        next_action=(
            "For dynamic, junction, and traffic-light claims, require native prediction "
            "or an explicit GT prediction/bypass boundary in prediction_evidence_report.json."
        ),
    )


def _no_assist_layer(
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    bridge_stats: Mapping[str, Any],
    control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
    apollo_reference_line_contract: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
    planning_topic_debug_summary: Mapping[str, Any],
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    ledger = build_runtime_assist_ledger(
        summary=summary,
        manifest=manifest,
        bridge_stats=bridge_stats,
    )
    active = list(ledger.get("active_assists") or [])
    blocking_assists = list(ledger.get("blocking_assists") or [])
    warnings = list(ledger.get("warnings") or [])
    reasons: list[str] = []
    explicit_blockers: list[str] = []
    backend = _infer_backend(summary, manifest, bridge_stats, control_handoff)
    control_source = _infer_control_source(summary, manifest, control_handoff, bridge_stats)
    applied_control_source = _normalize_applied_control_source(control_source)
    explicit_control_source = _explicit_control_source(summary, manifest)
    apollo_control_topic_observed = _apollo_control_topic_observed(control_handoff, bridge_stats)
    handoff_proves_apollo_control = _handoff_proves_apollo_control(control_handoff)
    routing_success_count = _first_num(summary.get("routing_success_count"), bridge_stats.get("routing_success_count"))
    planning_nonempty = _planning_nonempty_claim_metric(
        summary=summary,
        control_handoff=control_handoff,
        apollo_reference_line_contract=apollo_reference_line_contract,
        planning_materialization=planning_materialization,
        planning_topic_debug_summary=planning_topic_debug_summary,
    )
    planning_nonempty_ratio = planning_nonempty["ratio"]
    control_rx_count = _first_num(
        bridge_stats.get("control_rx_count"),
        _nested(control_handoff, "bridge_receive.control_rx_count"),
        _nested(control_handoff, "control_channel.message_count"),
    )
    control_tx_count = _first_num(bridge_stats.get("control_tx_count"), bridge_stats.get("apply_control_count"))
    control_apply_count = _first_num(
        bridge_stats.get("apply_control_count"),
        _nested(control_handoff, "mapping_and_apply.apply_control_count"),
        _control_apply_count_from_control_health(control_health),
    )
    if backend is None:
        reasons.append("backend_missing")
    elif backend != "apollo_cyberrt":
        reasons.append("backend_not_apollo_cyberrt")
        explicit_blockers.append("backend_not_apollo_cyberrt")
    if control_source is None:
        reasons.append("control_source_missing")
    elif applied_control_source != "apollo_control":
        reasons.append("control_source_not_apollo_control")
        explicit_blockers.append("control_source_not_apollo_control")
    if (
        explicit_control_source
        and explicit_control_source != "/apollo/control"
        and apollo_control_topic_observed
        and not handoff_proves_apollo_control
    ):
        reasons.append("control_source_conflict")
        explicit_blockers.append("control_source_conflict")
    if handoff_proves_apollo_control and explicit_control_source not in {None, "/apollo/control"}:
        warnings.append("summary_control_source_overridden_by_apollo_control_handoff")
    if routing_success_count is None or routing_success_count < 1:
        reasons.append("routing_success_missing")
    if planning_nonempty_ratio is None:
        reasons.append("planning_nonempty_ratio_missing")
    elif planning_nonempty_ratio < 0.80:
        reasons.append("planning_nonempty_ratio_not_claim_grade")
        explicit_blockers.append("planning_nonempty_ratio_not_claim_grade")
    if control_rx_count is None or control_rx_count < 1:
        reasons.append("control_rx_missing")
    if control_tx_count is None or control_tx_count < 1:
        reasons.append("control_tx_missing")
    if control_apply_count is None or control_apply_count < 1:
        reasons.append("control_apply_missing")
    if ledger.get("assist_confidence") == "unknown":
        reasons.append("assist_evidence_missing")
    if blocking_assists:
        reasons.append("blocking_assists_active")
        explicit_blockers.append("blocking_assists_active")
    metrics = control_health.get("metrics") if isinstance(control_health.get("metrics"), Mapping) else {}
    lateral_guard = _num(metrics.get("lateral_guard_apply_count"))
    trajectory_guard = _num(metrics.get("trajectory_contract_guard_apply_count"))
    if lateral_guard and lateral_guard > 0:
        reasons.append("lateral_guard_applied")
        explicit_blockers.append("lateral_guard_applied")
    if trajectory_guard and trajectory_guard > 0:
        reasons.append("trajectory_contract_guard_applied")
        explicit_blockers.append("trajectory_contract_guard_applied")
    if _force_green_enabled(summary, manifest, bridge_stats):
        reasons.append("force_green_enabled")
        explicit_blockers.append("force_green_enabled")
    if explicit_blockers:
        status = "fail"
    elif reasons:
        status = "insufficient_data"
    else:
        status = "warn" if active else "pass"
    return _layer(
        status=status,
        blocking_reasons=reasons,
        warnings=warnings,
        key_metrics={
            "active_assists": active,
            "blocking_assists": blocking_assists,
            "non_blocking_assists": list(ledger.get("non_blocking_assists") or []),
            "assist_confidence": ledger.get("assist_confidence"),
            "can_claim_unassisted_natural_driving": bool(
                ledger.get("can_claim_unassisted_natural_driving") and not reasons
            ),
            "backend": backend,
            "control_source": control_source,
            "applied_control_source": applied_control_source,
            "explicit_control_source": explicit_control_source,
            "apollo_control_topic_observed": apollo_control_topic_observed,
            "handoff_proves_apollo_control": handoff_proves_apollo_control,
            "routing_success_count": routing_success_count,
            "planning_nonempty_ratio": planning_nonempty_ratio,
            "planning_nonempty_ratio_for_claim": planning_nonempty_ratio,
            "planning_nonempty_ratio_source": planning_nonempty["source"],
            "planning_nonempty_ratio_overall": planning_nonempty["overall_ratio"],
            "planning_nonempty_ratio_filtered_after_routing_segment_available": planning_nonempty[
                "after_routing_segment_available"
            ],
            "planning_nonempty_ratio_after_routing_segment_available": planning_nonempty[
                "after_routing_segment_available"
            ],
            "planning_nonempty_ratio_after_first_nonempty": planning_nonempty["after_first_nonempty"],
            "control_rx_count": control_rx_count,
            "control_tx_count": control_tx_count,
            "control_apply_count": control_apply_count,
            "lateral_guard_apply_count": lateral_guard,
            "trajectory_contract_guard_apply_count": trajectory_guard,
            "why_not_claimable": reasons,
            "explicit_claim_blockers": explicit_blockers,
        },
        artifact_paths=_paths(inputs, "summary", "manifest", "cyber_bridge_stats", "control_health"),
        next_action="Declare and eliminate blocking assists before unassisted natural-driving claims.",
    )


def _natural_driving_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    if not report:
        return _layer(
            status="insufficient_data",
            blocking_reasons=[],
            warnings=["natural_driving_report_missing"],
            key_metrics={"summary_metrics_present": bool(summary.get("metrics"))},
            artifact_paths={"natural_driving_report": str(path) if path else None},
            next_action="Generate natural_driving_report.json; summary success alone is not an acceptance artifact.",
        )
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping):
        status_value = verdict.get("status")
    else:
        status_value = verdict
    return _layer(
        status=_normalize_status(status_value or report.get("status")),
        blocking_reasons=list(report.get("blocking_reasons") or []),
        warnings=list(report.get("warnings") or []),
        key_metrics={
            "verdict": verdict,
            "can_claim_unassisted_natural_driving": report.get("can_claim_unassisted_natural_driving"),
            "summary": report.get("summary"),
        },
        artifact_paths={"natural_driving_report": str(path) if path else None},
        next_action="Use natural_driving_report.json as the final behavior gate after link evidence is non-blocking.",
    )


def _obstacle_gt_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    raw_path: Path | None,
    scenario_class: str | None,
) -> dict[str, Any]:
    source_report: Mapping[str, Any] | None = report
    source_path = path
    source_kind = "report"
    if not source_report and raw_path is not None and raw_path.exists():
        source_report = analyze_obstacle_gt_contract_file(raw_path, scenario_class=scenario_class)
        source_path = raw_path
        source_kind = "raw_artifact"
    layer = _report_layer(
        name="perception_gt_obstacles",
        report=source_report,
        path=source_path,
        status_keys=("status",),
        blocking_keys=("errors", "blocking_reasons"),
        warning_keys=("warnings",),
        next_action=(
            "Generate obstacle_gt_contract_report.json; verify ego exclusion, ids, "
            "frame, dimensions, velocity, and tracking_time."
        ),
        key_metric_fields=(
            "object_count",
            "message_count",
            "empty_message_count",
            "empty_obstacle_messages_healthy",
            "dynamic_obstacle_required",
            "errors",
            "missing_fields",
        ),
    )
    layer["artifact_paths"]["source_kind"] = source_kind
    if raw_path is not None:
        layer["artifact_paths"]["obstacle_gt_contract_raw"] = str(raw_path)
    return layer


def _apollo_module_consumption_layer(
    report: Mapping[str, Any] | None,
    path: Path | None,
    *,
    source_kind: str,
) -> dict[str, Any]:
    layer = _report_layer(
        name="apollo_module_consumption",
        report=report,
        path=path,
        status_keys=("status",),
        blocking_keys=("blocking_reasons",),
        warning_keys=("warnings",),
        next_action=(
            "Generate apollo_module_consumption_report.json to prove Apollo Planning/Control "
            "consumed GT inputs; bridge-side publish stats alone are not enough."
        ),
        key_metric_fields=(
            "routing_response_consumed_by_planning",
            "pattern_counts",
            "empty_reason_histogram",
            "planning_input_age",
            "prediction_mode",
        ),
    )
    layer["artifact_paths"]["source_kind"] = source_kind
    return layer


def _report_layer(
    *,
    name: str,
    report: Mapping[str, Any] | None,
    path: Path | None,
    status_keys: Sequence[str],
    next_action: str,
    blocking_keys: Sequence[str] = ("blocking_reasons", "errors"),
    warning_keys: Sequence[str] = ("warnings",),
    key_metric_fields: Sequence[str] = (),
) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(path=path, next_action=next_action)
    status = "insufficient_data"
    for key in status_keys:
        value = _nested(report, key)
        if value not in {None, ""}:
            status = _normalize_status(value)
            break
    key_metrics = {field: _nested(report, field) for field in key_metric_fields}
    return _layer(
        status=status,
        blocking_reasons=_collect_lists(report, blocking_keys),
        warnings=_collect_lists(report, warning_keys),
        key_metrics=key_metrics,
        artifact_paths={name: str(path) if path else None},
        next_action=next_action,
    )


def _missing_report_layer(*, path: Path | None, next_action: str) -> dict[str, Any]:
    return _layer(
        status="insufficient_data",
        blocking_reasons=[],
        warnings=["required_artifact_missing"],
        key_metrics={},
        artifact_paths={"report": str(path) if path else None},
        next_action=next_action,
    )


def _layer(
    *,
    status: str,
    blocking_reasons: Sequence[str],
    warnings: Sequence[str],
    key_metrics: Mapping[str, Any],
    artifact_paths: Mapping[str, str | None],
    next_action: str,
) -> dict[str, Any]:
    return {
        "status": _normalize_status(status),
        "blocking_reasons": sorted({str(item) for item in blocking_reasons if item}),
        "warnings": sorted({str(item) for item in warnings if item}),
        "key_metrics": dict(key_metrics),
        "artifact_paths": dict(artifact_paths),
        "next_action": next_action,
    }


def _blocker_summary(layers: Mapping[str, Mapping[str, Any]]) -> tuple[str | None, list[str]]:
    warmup_primary = _startup_warmup_incomplete_primary(layers)
    blockers = _all_blockers(layers)
    if warmup_primary:
        secondary = [item for item in blockers if item != warmup_primary]
        return warmup_primary, secondary

    routing_primary = _routing_success_missing_primary(layers)
    if routing_primary:
        secondary = [item for item in blockers if item != routing_primary]
        return routing_primary, secondary

    route_contract_primary = _route_contract_unverified_primary(layers)
    if route_contract_primary:
        secondary = [item for item in blockers if item != route_contract_primary]
        return route_contract_primary, secondary

    special = _reference_line_localization_mismatch(layers)
    if special:
        secondary = [item for item in blockers if item != special]
        return special, secondary

    actual_lateral_drift = _actual_lateral_drift_primary(layers)
    if actual_lateral_drift:
        secondary = [item for item in blockers if item != actual_lateral_drift]
        return actual_lateral_drift, secondary

    planning_primary = _planning_materialization_primary(layers)
    if planning_primary:
        secondary = [item for item in blockers if item != planning_primary]
        return planning_primary, secondary

    semantic_primary = _semantic_primary_for_planning_channel_gap(layers)
    if semantic_primary:
        secondary = [item for item in blockers if item != semantic_primary]
        return semantic_primary, secondary

    hdmap_reference_gap = _hdmap_reference_evidence_gap_primary(layers)
    if hdmap_reference_gap:
        secondary = [item for item in blockers if item != hdmap_reference_gap]
        return hdmap_reference_gap, secondary

    localization_gap = _localization_evidence_gap_primary(layers)
    if localization_gap:
        secondary = [item for item in blockers if item != localization_gap]
        return localization_gap, secondary

    claim_boundary_primary = _semantic_primary_for_claim_boundary(layers)
    if claim_boundary_primary:
        secondary = [item for item in blockers if item != claim_boundary_primary]
        return claim_boundary_primary, secondary

    defer_control_primary = _defer_control_primary_until_loc_ref_pass(layers)

    primary: str | None = None
    for status_group in ({"fail"}, {"insufficient_data"}, {"warn"}):
        for name in LAYER_ORDER:
            if name == "control_mapping_apply" and defer_control_primary:
                continue
            layer = layers.get(name, {})
            if layer.get("status") in status_group and layer.get("status") not in PASS_WARN:
                primary = _layer_blocker_name(name, layer)
                break
        if primary:
            break
    if primary is None and any((layers.get(name, {}) or {}).get("status") == "warn" for name in LAYER_ORDER):
        for name in LAYER_ORDER:
            layer = layers.get(name, {})
            if layer.get("status") == "warn":
                primary = _layer_blocker_name(name, layer)
                break
    secondary = [item for item in blockers if item != primary]
    return primary, secondary


def _defer_control_primary_until_loc_ref_pass(layers: Mapping[str, Mapping[str, Any]]) -> bool:
    """Control symptoms should not outrank localization/reference-line evidence gaps."""
    loc_ref_pass = _non_blocking(layers.get("localization_gt_contract", {})) and _non_blocking(
        layers.get("planning_reference_line", {})
    )
    if loc_ref_pass:
        return False
    control = layers.get("control_mapping_apply", {})
    return bool(control.get("status") in BLOCKING_STATUSES or control.get("blocking_reasons"))


def _startup_warmup_incomplete_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    handoff = layers.get("routing_planning_control_handoff", {})
    if "apollo_startup_warmup_incomplete" in set(handoff.get("blocking_reasons") or []):
        return "routing_planning_control_handoff:apollo_startup_warmup_incomplete"
    return None


def _routing_success_missing_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    if not _non_blocking(layers.get("environment_world", {})):
        return None
    handoff = layers.get("routing_planning_control_handoff", {})
    if "routing_success_missing" in set(handoff.get("blocking_reasons") or []):
        return "routing_planning_control_handoff:routing_success_missing"
    return None


def _semantic_primary_for_planning_channel_gap(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    channel = layers.get("channel_health", {})
    if not _is_planning_gap_only_channel_failure(channel):
        return None
    localization = layers.get("localization_gt_contract", {})
    if localization.get("status") == "fail" or localization.get("blocking_reasons"):
        return _layer_blocker_name("localization_gt_contract", localization)
    reference = layers.get("planning_reference_line", {})
    if reference.get("status") in BLOCKING_STATUSES or reference.get("blocking_reasons"):
        return _layer_blocker_name("planning_reference_line", reference)
    hdmap = layers.get("hdmap_projection", {})
    if hdmap.get("status") in BLOCKING_STATUSES or hdmap.get("blocking_reasons"):
        return _layer_blocker_name("hdmap_projection", hdmap)
    return None


def _planning_materialization_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    route = layers.get("route_establishment", {})
    reasons = set(route.get("blocking_reasons") or [])
    if "routing_success_missing" in reasons:
        return None
    for reason in ROUTE_CONTRACT_BLOCKER_PRIORITY:
        if reason in reasons:
            return f"route_establishment:{reason}"
    preferred = (
        "planning_trajectory_materialization_low",
        "route_establishment_latency",
        "planning_nonempty_after_routing_below_threshold",
        "route_establishment_not_confirmed",
    )
    for reason in preferred:
        if reason in reasons:
            return f"route_establishment:{reason}"
    return None


def _route_contract_unverified_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    if not _non_blocking(layers.get("environment_world", {})):
        return None
    if not _non_blocking(layers.get("bridge_runtime", {})):
        return None
    route = layers.get("route_establishment", {})
    if route.get("status") != "insufficient_data":
        return None
    metrics = route.get("key_metrics") if isinstance(route.get("key_metrics"), Mapping) else {}
    if metrics.get("route_contract_status") != "insufficient_data":
        return None
    for reason in _layer_insufficient_reasons(route):
        if reason:
            return f"route_establishment:{reason}"
    return "route_establishment:apollo_route_contract_insufficient"


def _is_planning_gap_only_channel_failure(channel_layer: Mapping[str, Any]) -> bool:
    if channel_layer.get("status") != "fail":
        return False
    metrics = channel_layer.get("key_metrics") if isinstance(channel_layer.get("key_metrics"), Mapping) else {}
    if metrics.get("missing_required_channels") or metrics.get("low_rate_channels") or metrics.get("timestamp_failures"):
        return False
    failed_channels = [str(item) for item in metrics.get("failed_channels") or []]
    if failed_channels != ["planning"]:
        return False
    issues = {str(item) for item in metrics.get("planning_issues") or []}
    return bool(issues) and issues.issubset({"message_gap_too_large"})


def _semantic_primary_for_claim_boundary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    no_assist = layers.get("no_assist_claim_boundary", {})
    reasons = set(no_assist.get("blocking_reasons") or [])
    if "planning_nonempty_ratio_not_claim_grade" not in reasons:
        return None
    reference = layers.get("planning_reference_line", {})
    if reference.get("status") in BLOCKING_STATUSES or reference.get("blocking_reasons"):
        return _layer_blocker_name("planning_reference_line", reference)
    hdmap = layers.get("hdmap_projection", {})
    if hdmap.get("status") in BLOCKING_STATUSES or hdmap.get("blocking_reasons"):
        return _layer_blocker_name("hdmap_projection", hdmap)
    handoff = layers.get("routing_planning_control_handoff", {})
    if handoff.get("status") in BLOCKING_STATUSES or handoff.get("blocking_reasons"):
        return _layer_blocker_name("routing_planning_control_handoff", handoff)
    channel = layers.get("channel_health", {})
    if channel.get("status") in BLOCKING_STATUSES or channel.get("blocking_reasons"):
        return _layer_blocker_name("channel_health", channel)
    return None


def _hdmap_reference_evidence_gap_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    """HDMap/reference evidence gaps should outrank generic claim-boundary failures."""
    for prerequisite in ("environment_world", "bridge_runtime", "channel_health", "route_establishment"):
        if not _non_blocking(layers.get(prerequisite, {})):
            return None

    hdmap = layers.get("hdmap_projection", {})
    if hdmap.get("status") == "insufficient_data":
        return _layer_blocker_name("hdmap_projection", hdmap)

    reference = layers.get("planning_reference_line", {})
    if reference.get("status") == "insufficient_data":
        return _layer_blocker_name("planning_reference_line", reference)
    return None


def _localization_evidence_gap_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    """Localization contract gaps should be fixed before closed-loop claim-boundary issues."""
    for prerequisite in (
        "environment_world",
        "bridge_runtime",
        "channel_health",
        "route_establishment",
        "hdmap_projection",
        "planning_reference_line",
    ):
        if not _non_blocking(layers.get(prerequisite, {})):
            return None
    localization = layers.get("localization_gt_contract", {})
    if localization.get("status") in BLOCKING_STATUSES or localization.get("blocking_reasons"):
        return _layer_blocker_name("localization_gt_contract", localization)
    return None


def _all_blockers(layers: Mapping[str, Mapping[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for name in LAYER_ORDER:
        layer = layers.get(name, {})
        if layer.get("status") in BLOCKING_STATUSES or layer.get("blocking_reasons"):
            blockers.append(_layer_blocker_name(name, layer))
    return blockers


def _reference_line_localization_mismatch(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    localization = layers.get("localization_gt_contract", {})
    reference = layers.get("planning_reference_line", {})
    loc_reasons = set(localization.get("blocking_reasons") or [])
    ref_reasons = set(reference.get("blocking_reasons") or [])
    loc_metrics = localization.get("key_metrics") if isinstance(localization.get("key_metrics"), Mapping) else {}
    ref_metrics = reference.get("key_metrics") if isinstance(reference.get("key_metrics"), Mapping) else {}
    ref_inner_metrics = ref_metrics.get("metrics") if isinstance(ref_metrics.get("metrics"), Mapping) else {}
    lane_heading = _num(loc_metrics.get("heading_error_to_lane_p95_rad"))
    if (
        "heading_error_to_lane_high" in loc_reasons
        or "reference_line_heading_error_high" in ref_reasons
        or "control_reference_heading_error_high" in ref_reasons
        or (lane_heading is not None and lane_heading >= 0.35)
        or _num(ref_inner_metrics.get("control_ref_heading_error_p95_rad")) is not None
        and (_num(ref_inner_metrics.get("control_ref_heading_error_p95_rad")) or 0.0) >= 0.20
    ):
        return "reference_line/localization lane-heading mismatch"
    return None


def _actual_lateral_drift_primary(layers: Mapping[str, Mapping[str, Any]]) -> str | None:
    localization = layers.get("localization_gt_contract", {})
    loc_reasons = set(localization.get("blocking_reasons") or [])
    loc_metrics = localization.get("key_metrics") if isinstance(localization.get("key_metrics"), Mapping) else {}
    if "apollo_hdmap_projection_lateral_error_high" not in loc_reasons:
        return None
    if (
        loc_metrics.get("hdmap_route_lateral_consistency_status") == "pass"
        and loc_metrics.get("hdmap_route_lateral_interpretation")
        == "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"
    ):
        return "natural_driving_outcome:actual_lateral_drift_matches_hdmap_projection"
    return None


def _layer_blocker_name(name: str, layer: Mapping[str, Any]) -> str:
    reasons = list(layer.get("blocking_reasons") or [])
    insufficient_reasons = _layer_insufficient_reasons(layer)
    metrics = layer.get("key_metrics") if isinstance(layer.get("key_metrics"), Mapping) else {}
    if name == "channel_health":
        cadence_primary = metrics.get("channel_cadence_primary_issue")
        if cadence_primary:
            return f"{name}:{cadence_primary}"
    if name == "route_establishment":
        preferred = (
            *ROUTE_CONTRACT_BLOCKER_PRIORITY,
            "apollo_hdmap_projection_for_lane_equivalence",
            "apollo_route_contract_insufficient",
            "planning_trajectory_materialization_low",
            "route_establishment_latency",
            "planning_nonempty_after_routing_below_threshold",
            "route_establishment_not_confirmed",
            "planning_materialization_report_or_summary_missing",
            "routing_success_missing",
        )
        reason_set = set(reasons)
        for reason in preferred:
            if reason in reason_set:
                return f"{name}:{reason}"
    if name == "hdmap_projection":
        preferred_insufficient = (
            "apollo_hdmap_projection_runtime_unavailable",
            "apollo_hdmap_projection_route_s_coverage_missing",
            "apollo_hdmap_projection_sample_count_low",
            "apollo_hdmap_projection_metrics_missing",
            "apollo_hdmap_projection_rows_missing",
            "apollo_hdmap_projection_empty",
        )
        insufficient_set = set(insufficient_reasons)
        for reason in preferred_insufficient:
            if reason in insufficient_set:
                return f"{name}:{reason}"
    if name == "routing_planning_control_handoff":
        preferred = (
            "apollo_startup_warmup_incomplete",
            "routing_success_missing",
            "planning_nonempty_missing",
            "planning_ready_control_not_consuming",
            "control_process_missing",
            "control_process_crash_before_control_output",
            "control_rx_missing",
            "control_apply_missing",
            "control_process_crashed",
        )
        reason_set = set(reasons)
        for reason in preferred:
            if reason in reason_set:
                return f"{name}:{reason}"
    if reasons:
        return f"{name}:{reasons[0]}"
    if insufficient_reasons:
        return f"{name}:{insufficient_reasons[0]}"
    return f"{name}:{layer.get('status')}"


def _layer_insufficient_reasons(layer: Mapping[str, Any]) -> list[str]:
    direct = [str(item) for item in layer.get("insufficient_reasons") or [] if item]
    metrics = layer.get("key_metrics") if isinstance(layer.get("key_metrics"), Mapping) else {}
    metric_reasons = [str(item) for item in metrics.get("insufficient_reasons") or [] if item]
    projection = metrics.get("apollo_hdmap_projection") if isinstance(metrics.get("apollo_hdmap_projection"), Mapping) else {}
    projection_reasons = [str(item) for item in projection.get("insufficient_reasons") or [] if item]
    return [*direct, *metric_reasons, *projection_reasons]


def _can_claim_unassisted(layers: Mapping[str, Mapping[str, Any]]) -> bool:
    required = (
        "environment_world",
        "bridge_runtime",
        "channel_health",
        "localization_gt_contract",
        "chassis_gt_contract",
        "hdmap_projection",
        "planning_reference_line",
        "route_establishment",
        "apollo_module_consumption",
        "routing_planning_control_handoff",
        "control_mapping_apply",
        "perception_gt_obstacles",
        "prediction_evidence",
        "traffic_light_gt",
        "no_assist_claim_boundary",
        "natural_driving_outcome",
    )
    if any(not _non_blocking(layers.get(name, {})) for name in required):
        return False
    if _claim_grade_reasons(layers):
        return False
    no_assist = layers.get("no_assist_claim_boundary", {})
    no_assist_metrics = no_assist.get("key_metrics") if isinstance(no_assist.get("key_metrics"), Mapping) else {}
    natural = layers.get("natural_driving_outcome", {})
    natural_metrics = natural.get("key_metrics") if isinstance(natural.get("key_metrics"), Mapping) else {}
    return bool(
        no_assist_metrics.get("can_claim_unassisted_natural_driving") is True
        and natural_metrics.get("can_claim_unassisted_natural_driving") is not False
    )


def _why_not_claimable(layers: Mapping[str, Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for name in LAYER_ORDER:
        layer = layers.get(name, {})
        if not _non_blocking(layer):
            status = layer.get("status")
            layer_reasons = list(layer.get("blocking_reasons") or [])
            if layer_reasons:
                reasons.extend(f"{name}:{reason}" for reason in layer_reasons)
            else:
                reasons.append(f"{name}:{status}")
    reasons.extend(_claim_grade_reasons(layers))
    no_assist = layers.get("no_assist_claim_boundary", {})
    metrics = no_assist.get("key_metrics") if isinstance(no_assist.get("key_metrics"), Mapping) else {}
    reasons.extend(str(item) for item in metrics.get("why_not_claimable") or [])
    return sorted(set(reason for reason in reasons if reason and reason != "traffic_light_gt:not_applicable"))


def _claim_grade_reasons(layers: Mapping[str, Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for name in LAYER_ORDER:
        layer = layers.get(name, {})
        metrics = layer.get("key_metrics") if isinstance(layer.get("key_metrics"), Mapping) else {}
        if metrics.get("claim_grade") is False:
            reasons.append(f"{name}:claim_grade_false")
        if metrics.get("hard_gate_eligible") is False:
            reasons.append(f"{name}:hard_gate_eligible_false")
        if metrics.get("vehicle_reference_hard_gate_eligible") is False:
            reasons.append(f"{name}:vehicle_reference_hard_gate_eligible_false")
    return reasons


def _non_blocking(layer: Mapping[str, Any]) -> bool:
    return layer.get("status") in {"pass", "warn", "not_applicable"} and not layer.get("blocking_reasons")


def _next_highest_value_validation(primary: str | None, layers: Mapping[str, Mapping[str, Any]]) -> str:
    if primary is None:
        return "Run longer route-level validation and natural-driving evaluator with the same artifact set."
    if primary == "reference_line/localization lane-heading mismatch":
        return "Compare localization lane heading, Apollo HDMap projection, and planning/control reference-line heading on the same route_s window."
    if primary == "natural_driving_outcome:actual_lateral_drift_matches_hdmap_projection":
        return (
            "HDMap lateral error matches CARLA route cross-track, so treat it as real closed-loop lateral drift. "
            "Inspect route_s windows around drift growth, Apollo target/matched point semantics, and raw/mapped/applied steer before changing localization transform."
        )
    if primary and "apollo_hdmap_projection_route_s_coverage_low" in primary:
        return (
            "Run a longer claim-window route sample and regenerate artifacts/apollo_hdmap_projection.jsonl; "
            "current official projection quality is usable but projected route-s coverage is not claim-grade."
        )
    if primary and "apollo_hdmap_projection_sample_count_low" in primary:
        return "Regenerate Apollo HDMap projection with at least the configured minimum claim-grade sample count."
    if primary == "channel_health:fail" and _planning_gap_coincides_with_inter_tick_pause(layers):
        return (
            "Inspect harness loop cadence and blocking hooks between world ticks. "
            "The Planning gap is on Apollo header/wall time while sim-time remains near-continuous."
        )
    if primary == "no_assist_claim_boundary:planning_nonempty_ratio_not_claim_grade":
        return (
            "Inspect Planning availability over the claim window: startup warmup, reference-line provider readiness, "
            "routing segment continuity, and planning_topic_debug non-empty trajectory ratio. This is not an assist cleanup."
        )
    layer_name = primary.split(":", 1)[0]
    layer = layers.get(layer_name, {})
    return str(layer.get("next_action") or "Inspect the primary blocker artifact and regenerate missing evidence.")


def _planning_gap_coincides_with_inter_tick_pause(layers: Mapping[str, Mapping[str, Any]]) -> bool:
    channel = layers.get("channel_health", {})
    if not _is_planning_gap_only_channel_failure(channel):
        return False
    metrics = channel.get("key_metrics") if isinstance(channel.get("key_metrics"), Mapping) else {}
    if metrics.get("planning_time_axis_diagnosis") != "header_or_wall_time_gap_large_sim_time_gap_ok":
        return False
    environment = layers.get("environment_world", {})
    env_metrics = environment.get("key_metrics") if isinstance(environment.get("key_metrics"), Mapping) else {}
    return env_metrics.get("carla_tick_inter_tick_wall_gap_high") is True


def _paths(inputs: Mapping[str, Path | None], *names: str) -> dict[str, str | None]:
    return {name: str(inputs.get(name)) if inputs.get(name) else None for name in names}


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = (root / relative).resolve() if relative.startswith("..") else root / relative
        if path.exists():
            return path
    return None


def _find_natural_driving_report(root: Path) -> Path | None:
    candidates: list[Path] = []
    for base in (root, *root.parents):
        for relative in (
            "analysis/natural_driving/natural_driving_report.json",
            "analysis/natural_driving_postprocess/natural_driving_report.json",
            "natural_driving_report.json",
        ):
            path = base / relative
            if path.exists():
                candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _find_followstop_child_stderr(root: Path) -> Path | None:
    direct = root / "artifacts" / "followstop_child.stderr.log"
    if direct.exists():
        return direct

    # run_followstop redirects non-empty planned run dirs to suffixed actual
    # run dirs such as `...__02`, while the child stderr stays in the planned
    # wrapper dir. Link-health is usually invoked on the actual run dir.
    planned_name = re.sub(r"__\d+$", "", root.name)
    if planned_name != root.name:
        sibling = root.with_name(planned_name) / "artifacts" / "followstop_child.stderr.log"
        if sibling.exists():
            return sibling

    try:
        root_resolved = root.resolve()
        for redirect in root.parent.glob("*/RUN_DIR_REDIRECT.txt"):
            try:
                target = Path(redirect.read_text(encoding="utf-8").strip()).expanduser().resolve()
            except Exception:
                continue
            if target != root_resolved:
                continue
            stderr = redirect.parent / "artifacts" / "followstop_child.stderr.log"
            if stderr.exists():
                return stderr
    except Exception:
        return None
    return None


def _detect_carla_world_tick_timeout(
    summary: Mapping[str, Any],
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    tick_summary_path = inputs.get("carla_tick_health_summary")
    tick_summary = _read_json(tick_summary_path)
    fail_reason = str(summary.get("fail_reason") or "").strip()
    tick_failure_reason = str(tick_summary.get("last_failure_reason") or "").strip()
    detected_in_structured_artifact = tick_failure_reason == "CARLA_WORLD_TICK_TIMEOUT" or fail_reason == "CARLA_WORLD_TICK_TIMEOUT"

    stderr_path = inputs.get("followstop_child_stderr")
    detected_in_stderr = False
    if stderr_path is not None:
        try:
            lowered = stderr_path.read_text(encoding="utf-8", errors="replace").lower()
        except Exception:
            lowered = ""
        detected_in_stderr = (
            ("world.tick" in lowered or "tick_world" in lowered)
            and "time-out" in lowered
            and "waiting for the simulator" in lowered
        )

    routing_count = _num(summary.get("routing_request_count"))
    frames = _num(summary.get("frames"))
    detected = bool(
        (detected_in_structured_artifact or detected_in_stderr)
        and (routing_count is None or routing_count <= 0)
        and (
            fail_reason in {"", "ROUTING_REQUEST_COUNT", "CARLA_WORLD_TICK_TIMEOUT"}
            or frames is None
            or frames <= 2
        )
    )
    artifact_path = str(tick_summary_path) if tick_summary_path else str(stderr_path) if stderr_path else None
    return {
        "detected": detected,
        "artifact_path": artifact_path,
        "tick_count": tick_summary.get("tick_count"),
        "tick_fail_count": tick_summary.get("tick_fail_count"),
        "max_tick_wall_duration_s": tick_summary.get("max_tick_wall_duration_s"),
    }


def _carla_tick_cadence(inputs: Mapping[str, Path | None]) -> dict[str, Any]:
    tick_summary_path = inputs.get("carla_tick_health_summary")
    tick_summary = _read_json(tick_summary_path)
    max_interval = _num(tick_summary.get("max_inter_tick_wall_interval_s"))
    p95_interval = _num(tick_summary.get("inter_tick_wall_interval_p95_s"))
    interval_count = _num(tick_summary.get("inter_tick_wall_interval_count"))
    if max_interval is not None or p95_interval is not None:
        stage_duration_max_s = (
            dict(tick_summary.get("stage_duration_max_s"))
            if isinstance(tick_summary.get("stage_duration_max_s"), Mapping)
            else {}
        )
        max_hook_stage_name, max_hook_stage_duration_s = _max_hook_stage(stage_duration_max_s)
        return {
            "source": "carla_tick_health_summary",
            "max_inter_tick_wall_interval_s": max_interval,
            "inter_tick_wall_interval_p95_s": p95_interval,
            "inter_tick_wall_interval_count": interval_count,
            "inter_tick_wall_gap_high": _is_inter_tick_gap_high(max_interval),
            "max_frame_post_tick_wall_duration_s": _num(tick_summary.get("max_frame_post_tick_wall_duration_s")),
            "max_frame_loop_wall_duration_s": _num(tick_summary.get("max_frame_loop_wall_duration_s")),
            "max_stage_name": tick_summary.get("max_stage_name"),
            "max_stage_duration_s": _num(tick_summary.get("max_stage_duration_s")),
            "max_hook_stage_name": max_hook_stage_name,
            "max_hook_stage_duration_s": max_hook_stage_duration_s,
            "stage_duration_max_s": stage_duration_max_s,
        }

    tick_log_path = inputs.get("carla_tick_health_log")
    rows = _read_jsonl(tick_log_path)
    timing_rows = [row for row in rows if isinstance(row, Mapping) and row.get("event_type") == "frame_loop_timing"]
    if timing_rows:
        intervals = [
            value
            for value in (_num(row.get("inter_tick_wall_interval_s")) for row in timing_rows)
            if value is not None
        ]
        frame_post = [
            value
            for value in (_num(row.get("frame_post_tick_wall_duration_s")) for row in timing_rows)
            if value is not None
        ]
        frame_loop = [
            value for value in (_num(row.get("frame_loop_wall_duration_s")) for row in timing_rows) if value is not None
        ]
        stage_max: dict[str, float] = {}
        for row in timing_rows:
            stages = row.get("stage_durations_s")
            if not isinstance(stages, Mapping):
                continue
            for stage, raw_duration in stages.items():
                duration = _num(raw_duration)
                if duration is None:
                    continue
                stage_name = str(stage)
                stage_max[stage_name] = max(stage_max.get(stage_name, 0.0), duration)
        max_stage_name = None
        max_stage_duration = None
        if stage_max:
            max_stage_name, max_stage_duration = max(stage_max.items(), key=lambda item: item[1])
        max_hook_stage_name, max_hook_stage_duration_s = _max_hook_stage(stage_max)
        max_interval = max(intervals) if intervals else None
        return {
            "source": "carla_tick_health_frame_loop_timing",
            "max_inter_tick_wall_interval_s": max_interval,
            "inter_tick_wall_interval_p95_s": _percentile(intervals, 95.0),
            "inter_tick_wall_interval_count": len(intervals),
            "inter_tick_wall_gap_high": _is_inter_tick_gap_high(max_interval),
            "max_frame_post_tick_wall_duration_s": max(frame_post) if frame_post else None,
            "max_frame_loop_wall_duration_s": max(frame_loop) if frame_loop else None,
            "max_stage_name": max_stage_name,
            "max_stage_duration_s": max_stage_duration,
            "max_hook_stage_name": max_hook_stage_name,
            "max_hook_stage_duration_s": max_hook_stage_duration_s,
            "stage_duration_max_s": stage_max,
        }

    wall_times: list[float] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("event_type") != "world_tick":
            continue
        wall_time = _num(row.get("wall_time_s"))
        if wall_time is not None:
            wall_times.append(wall_time)
    wall_times.sort()
    intervals = [b - a for a, b in zip(wall_times, wall_times[1:]) if b >= a]
    if not intervals:
        return {
            "source": None,
            "max_inter_tick_wall_interval_s": None,
            "inter_tick_wall_interval_p95_s": None,
            "inter_tick_wall_interval_count": 0,
            "inter_tick_wall_gap_high": None,
            "max_frame_post_tick_wall_duration_s": None,
            "max_frame_loop_wall_duration_s": None,
            "max_stage_name": None,
            "max_stage_duration_s": None,
            "max_hook_stage_name": None,
            "max_hook_stage_duration_s": None,
            "stage_duration_max_s": {},
        }
    max_interval = max(intervals)
    p95_interval = _percentile(intervals, 95.0)
    return {
        "source": "carla_tick_health_jsonl_sparse",
        "max_inter_tick_wall_interval_s": max_interval,
        "inter_tick_wall_interval_p95_s": p95_interval,
        "inter_tick_wall_interval_count": len(intervals),
        "inter_tick_wall_gap_high": _is_inter_tick_gap_high(max_interval),
        "max_frame_post_tick_wall_duration_s": None,
        "max_frame_loop_wall_duration_s": None,
        "max_stage_name": None,
        "max_stage_duration_s": None,
        "max_hook_stage_name": None,
        "max_hook_stage_duration_s": None,
        "stage_duration_max_s": {},
    }


def _max_hook_stage(stage_duration_max_s: Mapping[str, Any]) -> tuple[str | None, float | None]:
    candidates: list[tuple[str, float]] = []
    for stage, raw_duration in stage_duration_max_s.items():
        stage_name = str(stage)
        if not stage_name.startswith("hook."):
            continue
        duration = _num(raw_duration)
        if duration is None:
            continue
        candidates.append((stage_name, duration))
    if not candidates:
        return None, None
    return max(candidates, key=lambda item: item[1])


def _is_inter_tick_gap_high(max_interval_s: float | None) -> bool | None:
    if max_interval_s is None:
        return None
    return max_interval_s >= 2.0


def _world_ready_blocker(status: str) -> str:
    if status == "external_carla_missing":
        return "external_carla_missing"
    if status in {"rpc_not_ready", "world_not_ready", "carla_failed_to_become_ready"}:
        return "carla_world_not_ready"
    if "crash" in status or "sig11" in status:
        return "carla_process_crashed"
    return f"carla_world_ready_{status}"


def _environment_world_next_action(runtime_status: str | None, blocking: Sequence[str]) -> str:
    blockers = {str(item) for item in blocking}
    if runtime_status == "not_required":
        return (
            "Rerun the capability sample with true-lateral runtime enabled "
            "(for example --enable-lateral); runtime_contract=not_required is diagnostic-only."
        )
    if runtime_status == "misconfigured":
        return "Fix runtime contract blockers before interpreting Apollo behavior."
    if "carla_world_not_matching_configured_town" in blockers:
        return "Fix CARLA loaded map/configured town identity before interpreting Apollo behavior."
    if blockers:
        return "Fix CARLA world/runtime contract before interpreting Apollo behavior."
    return "Environment/world contract is non-blocking; continue with bridge, channel, localization, and reference-line evidence."


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    payload = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    finite = sorted(value for value in (_num(v) for v in values) if value is not None)
    if not finite:
        return None
    if len(finite) == 1:
        return float(finite[0])
    rank = (len(finite) - 1) * max(0.0, min(100.0, percentile)) / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(finite) - 1)
    fraction = rank - lower
    return float(finite[lower] * (1.0 - fraction) + finite[upper] * fraction)


def _normalize_status(value: Any) -> str:
    text = str(value or "insufficient_data").strip()
    if text in {"pass", "warn", "fail", "insufficient_data", "not_applicable"}:
        return text
    if text == "pass_empty":
        return "pass"
    if text in {"candidate_positive", "success", "ok"}:
        return "pass"
    if text in {"candidate_negative", "failed"}:
        return "fail"
    if text in {"diagnostic_only", "assisted_pass"}:
        return "warn"
    return "insufficient_data"


def _runtime_contract_status(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> str | None:
    for payload in (summary, manifest):
        runtime_contract = payload.get("runtime_contract")
        if isinstance(runtime_contract, Mapping) and runtime_contract.get("status") not in {None, ""}:
            return str(runtime_contract.get("status"))
        value = payload.get("runtime_contract_status")
        if value not in {None, ""}:
            return str(value)
    return None


def _collect_lists(report: Mapping[str, Any], keys: Sequence[str]) -> list[str]:
    values: list[str] = []
    for key in keys:
        item = _nested(report, key)
        if isinstance(item, list):
            values.extend(str(value) for value in item if value)
        elif isinstance(item, str) and item:
            values.append(item)
    return sorted(set(values))


def _nested(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _first_text(*args: Any, default: str | None = None) -> str | None:
    for value in _first_values(*args):
        if value not in {None, ""}:
            return str(value)
    return default


def _first_raw(*args: Any) -> Any:
    for value in _first_values(*args):
        if value not in {None, ""}:
            return value
    return None


def _first_values(*args: Any) -> list[Any]:
    if not args:
        return []
    if len(args) == 2 and isinstance(args[0], Mapping) and isinstance(args[1], str):
        return [args[0].get(args[1])]
    values: list[Any] = []
    index = 0
    while index < len(args):
        if isinstance(args[index], Mapping):
            mapping = args[index]
            index += 1
            while index < len(args) and isinstance(args[index], str):
                values.append(mapping.get(args[index]))
                index += 1
        else:
            values.append(args[index])
            index += 1
    return values


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in {None, ""}:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "pass", "aligned"}:
        return True
    if text in {"0", "false", "no", "n", "off", "fail", "failed"}:
        return False
    return None


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_num(*values: Any) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _infer_backend(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    bridge_stats: Mapping[str, Any],
    control_handoff: Mapping[str, Any],
) -> str | None:
    explicit = _first_text(summary, "backend", manifest, "backend")
    if explicit:
        return explicit
    if bridge_stats or control_handoff:
        control_channel = _nested(control_handoff, "control_channel.name")
        if control_channel == "/apollo/control" or bridge_stats.get("control_rx_count") not in {None, ""}:
            return "apollo_cyberrt"
    return None


def _infer_control_source(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    control_handoff: Mapping[str, Any],
    bridge_stats: Mapping[str, Any] | None = None,
) -> str | None:
    if _handoff_proves_apollo_control(control_handoff):
        return "/apollo/control"
    explicit = _explicit_control_source(summary, manifest)
    if explicit:
        return explicit
    control_channel = _nested(control_handoff, "control_channel.name")
    if control_channel == "/apollo/control":
        return "/apollo/control"
    if bridge_stats and _num(bridge_stats.get("control_rx_count")) and _num(bridge_stats.get("control_rx_count")) > 0:
        return "/apollo/control"
    return None


def _normalize_applied_control_source(control_source: str | None) -> str | None:
    if control_source is None:
        return None
    normalized = str(control_source).strip().lower()
    if normalized in {"/apollo/control", "apollo", "apollo_control", "apollo_cyberrt"}:
        return "apollo_control"
    if normalized in {"external_stack", "external", "autoware", "autoware_ros2"}:
        return "external_stack"
    if normalized in {"carla_builtin", "builtin", "carla_testbed_builtin_controller"}:
        return "carla_testbed_builtin_controller"
    if normalized in {"route_follower", "legacy_followstop", "manual", "direct_autopilot", "dummy_lateral"}:
        return normalized
    return normalized.replace("/", "_").strip("_") or None


def _handoff_proves_apollo_control(control_handoff: Mapping[str, Any]) -> bool:
    if not isinstance(control_handoff, Mapping):
        return False
    if _nested(control_handoff, "control_channel.name") != "/apollo/control":
        return False
    count = _first_num(
        _nested(control_handoff, "control_channel.message_count"),
        _nested(control_handoff, "bridge_receive.control_rx_count"),
        _nested(control_handoff, "mapping_and_apply.apply_control_count"),
    )
    if count is None or count <= 0:
        return False
    status = _normalize_status(control_handoff.get("verdict") or control_handoff.get("status"))
    failure_stage = str(control_handoff.get("failure_stage") or "").strip().lower()
    return status in {"pass", "warn"} and failure_stage in {"", "none", "pass"}


def _explicit_control_source(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> str | None:
    return _first_text(
        summary,
        "control_source",
        manifest,
        "control_source",
        summary,
        "applied_control_source",
        manifest,
        "applied_control_source",
        summary,
        "controller",
        manifest,
        "controller",
        summary,
        "lateral_mode",
        manifest,
        "lateral_mode",
    )


def _apollo_control_topic_observed(
    control_handoff: Mapping[str, Any],
    bridge_stats: Mapping[str, Any] | None = None,
) -> bool:
    if _nested(control_handoff, "control_channel.name") == "/apollo/control":
        return True
    if _first_num(
        _nested(control_handoff, "control_channel.message_count"),
        _nested(control_handoff, "bridge_receive.control_rx_count"),
        (bridge_stats or {}).get("control_rx_count") if isinstance(bridge_stats, Mapping) else None,
    ):
        return True
    return False


def _control_apply_count_from_control_health(control_health: Mapping[str, Any]) -> float | None:
    metrics = control_health.get("metrics") if isinstance(control_health.get("metrics"), Mapping) else {}
    candidates = (
        _nested(control_health, "mapping_and_apply.apply_control_count"),
        metrics.get("nonzero_applied_control_frames"),
        metrics.get("applied_control_frames"),
        _nested(metrics, "control_bridge_log.final_applied_count"),
        _nested(metrics, "control_bridge_log.max_applied_count"),
    )
    return _first_num(*candidates)


def _planning_nonempty_ratio(planning_topic_debug_summary: Mapping[str, Any]) -> float | None:
    total = _num(planning_topic_debug_summary.get("total_messages_received"))
    nonzero = _num(planning_topic_debug_summary.get("messages_with_nonzero_trajectory_points"))
    if total is None or nonzero is None or total <= 0:
        return None
    return nonzero / total


def _planning_nonempty_claim_metric(
    *,
    summary: Mapping[str, Any],
    control_handoff: Mapping[str, Any],
    apollo_reference_line_contract: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
    planning_topic_debug_summary: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = (
        apollo_reference_line_contract.get("evidence")
        if isinstance(apollo_reference_line_contract.get("evidence"), Mapping)
        else {}
    )
    claim_ratio = _num(evidence.get("planning_claim_window_nonempty_trajectory_ratio"))
    claim_source = str(evidence.get("planning_claim_window_source") or "").strip()
    after_routing = _num(evidence.get("nonempty_trajectory_ratio_after_routing_segment_available"))
    after_first_nonempty = _num(evidence.get("nonempty_trajectory_ratio_after_first_nonempty"))
    reference_overall = _num(evidence.get("nonempty_trajectory_ratio"))
    planning_metrics = (
        planning_materialization.get("metrics")
        if isinstance(planning_materialization.get("metrics"), Mapping)
        else {}
    )
    planning_materialization_ratio = _first_num(
        planning_materialization.get("nonempty_trajectory_ratio"),
        planning_metrics.get("nonempty_trajectory_ratio"),
    )
    topic_overall = _planning_nonempty_ratio(planning_topic_debug_summary)
    summary_ratio = _first_num(
        _nested(summary, "metrics.planning_nonempty_ratio"),
        summary.get("planning_nonempty_ratio"),
        _nested(control_handoff, "input_readiness.planning_nonempty_ratio"),
    )
    ratio = _first_num(planning_materialization_ratio, summary_ratio, topic_overall, reference_overall)
    if planning_materialization_ratio is not None:
        source = "planning_materialization.overall"
    elif summary_ratio is not None:
        source = "summary_or_control_handoff"
    elif topic_overall is not None:
        source = "planning_topic_debug_summary.overall"
    elif reference_overall is not None:
        source = "apollo_reference_line_contract.overall"
    else:
        source = None
    return {
        "ratio": ratio,
        "source": source,
        "overall_ratio": ratio,
        "filtered_ratio": claim_ratio,
        "filtered_ratio_source": (
            f"apollo_reference_line_contract.{claim_source}"
            if claim_ratio is not None and claim_source
            else None
        ),
        "after_routing_segment_available": after_routing,
        "after_first_nonempty": after_first_nonempty,
    }


def _force_green_enabled(*payloads: Mapping[str, Any]) -> bool:
    for payload in payloads:
        if _contains_force_green(payload):
            return True
    return False


def _contains_force_green(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            if key_text in {"force_green", "traffic_light_force_green"} and _bool_or_none(item) is True:
                return True
            if key_text in {"traffic_light_policy", "policy"} and str(item) == "force_green":
                return True
            if _contains_force_green(item):
                return True
    elif isinstance(value, list):
        return any(_contains_force_green(item) for item in value)
    return False
