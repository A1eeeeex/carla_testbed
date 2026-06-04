from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.assist_ledger import build_runtime_assist_ledger

APOLLO_LINK_HEALTH_SCHEMA_VERSION = "apollo_link_health.v1"

LAYER_ORDER = (
    "environment_world",
    "bridge_runtime",
    "channel_health",
    "localization_gt_contract",
    "hdmap_projection",
    "planning_reference_line",
    "routing_planning_control_handoff",
    "control_mapping_apply",
    "perception_gt_obstacles",
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


def analyze_apollo_link_health_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    inputs = _resolve_inputs(root)
    return analyze_apollo_link_health(inputs, run_dir=root)


def analyze_apollo_link_health(
    inputs: Mapping[str, Path | None],
    *,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser() if run_dir is not None else None
    payloads = {name: _read_json(path) for name, path in inputs.items() if name != "suite_manifest"}
    suite_manifest = _read_json(inputs.get("suite_manifest"))
    summary = payloads.get("summary", {})
    manifest = payloads.get("manifest", {})
    cyber_bridge_stats = payloads.get("cyber_bridge_stats", {})
    scenario_class = _first_text(summary, "scenario_class", manifest, "scenario_class")

    layers = {
        "environment_world": _environment_world_layer(summary, manifest, inputs),
        "bridge_runtime": _bridge_runtime_layer(summary, payloads, inputs),
        "channel_health": _report_layer(
            name="channel_health",
            report=payloads.get("apollo_channel_health"),
            path=inputs.get("apollo_channel_health"),
            status_keys=("status",),
            next_action="Generate apollo_channel_health_report.json from channel_stats before behavior claims.",
            key_metric_fields=("missing_required_channels", "low_rate_channels", "timestamp_failures"),
        ),
        "localization_gt_contract": _localization_layer(
            payloads.get("localization_contract"),
            inputs.get("localization_contract"),
        ),
        "hdmap_projection": _hdmap_projection_layer(
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
        "routing_planning_control_handoff": _control_handoff_layer(
            payloads.get("apollo_control_handoff"),
            inputs.get("apollo_control_handoff"),
        ),
        "control_mapping_apply": _control_health_layer(
            payloads.get("control_health"),
            inputs.get("control_health"),
        ),
        "perception_gt_obstacles": _report_layer(
            name="perception_gt_obstacles",
            report=payloads.get("obstacle_gt_contract"),
            path=inputs.get("obstacle_gt_contract"),
            status_keys=("status",),
            blocking_keys=("errors", "blocking_reasons"),
            warning_keys=("warnings",),
            next_action="Generate obstacle_gt_contract_report.json; verify ego exclusion, ids, frame, dimensions, velocity, and tracking_time.",
            key_metric_fields=("object_count", "dynamic_obstacle_required", "errors", "missing_fields"),
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
        lines.extend(
            [
                f"### {name}",
                f"- status: `{layer.get('status')}`",
                f"- blocking_reasons: `{', '.join(layer.get('blocking_reasons') or []) or 'none'}`",
                f"- warnings: `{', '.join(layer.get('warnings') or []) or 'none'}`",
                f"- next_action: `{layer.get('next_action')}`",
                "",
            ]
        )
    lines.extend([str(report.get("interpretation_boundary") or ""), ""])
    return "\n".join(lines)


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
        "apollo_channel_health": _find_first(
            root,
            [
                "analysis/apollo_channel_health/apollo_channel_health_report.json",
                "apollo_channel_health_report.json",
                "artifacts/apollo_channel_health_report.json",
            ],
        ),
        "localization_contract": _find_first(
            root,
            [
                "analysis/localization_contract/localization_contract_report.json",
                "localization_contract_report.json",
            ],
        ),
        "apollo_reference_line_contract": _find_first(
            root,
            [
                "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
                "apollo_reference_line_contract_report.json",
            ],
        ),
        "apollo_control_handoff": _find_first(
            root,
            [
                "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
                "apollo_control_handoff_report.json",
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
        "obstacle_gt_contract": _find_first(
            root,
            [
                "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
                "obstacle_gt_contract_report.json",
            ],
        ),
        "natural_driving_report": _find_first(
            root,
            [
                "analysis/natural_driving/natural_driving_report.json",
                "natural_driving_report.json",
            ],
        )
        or _find_natural_driving_report_in_ancestors(root),
    }


def _environment_world_layer(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    runtime_status = _runtime_contract_status(summary, manifest)
    carla_world = manifest.get("carla_world") if isinstance(manifest.get("carla_world"), Mapping) else {}
    matches_town = _bool_or_none(carla_world.get("matches_configured_town"))
    blocking: list[str] = []
    warnings: list[str] = []
    if not summary and not manifest:
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
            "loaded_map_name": carla_world.get("loaded_map_name"),
            "configured_town": carla_world.get("configured_town"),
            "spawn_point_count": carla_world.get("spawn_point_count"),
        },
        artifact_paths=_paths(inputs, "summary", "manifest"),
        next_action="Fix CARLA world/runtime contract before interpreting Apollo behavior.",
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
        },
        artifact_paths={"localization_contract": str(path) if path else None},
        next_action="Fix GT localization contract before blaming reference-line, control, curve, junction, or traffic-light behavior.",
    )


def _hdmap_projection_layer(
    localization_report: Mapping[str, Any] | None,
    reference_line_report: Mapping[str, Any] | None,
    inputs: Mapping[str, Path | None],
) -> dict[str, Any]:
    projection = {}
    for report in (reference_line_report, localization_report):
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
            artifact_paths=_paths(inputs, "localization_contract", "apollo_reference_line_contract"),
            next_action="Generate artifacts/apollo_hdmap_projection.jsonl using Apollo HDMap API projection evidence.",
        )
    return _layer(
        status=_normalize_status(projection.get("status")),
        blocking_reasons=list(projection.get("blocking_reasons") or []),
        warnings=list(projection.get("warnings") or []),
        key_metrics={
            "file_present": projection.get("file_present"),
            "official_source_available": projection.get("official_source_available"),
            "claim_grade": projection.get("claim_grade"),
            "heading_error_p95_rad": projection.get("heading_error_p95_rad"),
            "lateral_error_p95_m": projection.get("lateral_error_p95_m"),
            "nearest_lane_id_topk": projection.get("nearest_lane_id_topk"),
        },
        artifact_paths=_paths(inputs, "localization_contract", "apollo_reference_line_contract"),
        next_action="If projection is high-error, check map alignment, lane direction, lane id, and routing snap.",
    )


def _control_handoff_layer(report: Mapping[str, Any] | None, path: Path | None) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(
            path=path,
            next_action="Generate apollo_control_handoff_report.json; inspect process, control channel, bridge receive, raw decode, apply, and response.",
        )
    status = _normalize_status(report.get("verdict") or report.get("status"))
    return _layer(
        status=status,
        blocking_reasons=list(report.get("blocking_reasons") or []),
        warnings=list(report.get("warnings") or []),
        key_metrics={
            "failure_stage": report.get("failure_stage"),
            "evidence_level": report.get("evidence_level"),
            "control_message_count": _nested(report, "control_channel.message_count"),
            "control_rx_count": _nested(report, "bridge_receive.control_rx_count"),
            "apply_control_count": _nested(report, "mapping_and_apply.apply_control_count"),
            "vehicle_response_status": _nested(report, "vehicle_response.status"),
        },
        artifact_paths={"apollo_control_handoff": str(path) if path else None},
        next_action="If failed, follow the handoff stage: process -> input readiness -> control channel -> bridge receive -> raw decode -> apply -> response.",
    )


def _control_health_layer(report: Mapping[str, Any] | None, path: Path | None) -> dict[str, Any]:
    if not report:
        return _missing_report_layer(
            path=path,
            next_action="Generate control_health_report.json; inspect raw/mapped/applied controls, cadence, and vehicle response.",
        )
    return _layer(
        status=_normalize_status(report.get("status")),
        blocking_reasons=[reason for reason in [report.get("failure_reason")] if reason and report.get("status") == "fail"],
        warnings=list(report.get("warnings") or []),
        key_metrics={
            "failure_reason": report.get("failure_reason"),
            "control_handoff_status": report.get("control_handoff_status"),
            "oscillation_decomposition": _nested(report, "metrics.oscillation_decomposition"),
            "control_mapping_claim_boundary": _nested(report, "metrics.control_mapping_claim_boundary"),
            "mapped_applied_steer_abs_error_p95": _nested(report, "metrics.mapped_applied_steer_abs_error_p95"),
            "mapped_applied_throttle_abs_error_p95": _nested(report, "metrics.mapped_applied_throttle_abs_error_p95"),
            "mapped_applied_brake_abs_error_p95": _nested(report, "metrics.mapped_applied_brake_abs_error_p95"),
            "route_s_after_first_applied_control_delta_m": _nested(report, "metrics.route_s_after_first_applied_control_delta_m"),
        },
        artifact_paths={"control_health": str(path) if path else None},
        next_action="If localization/reference-line is non-blocking, fix cadence/apply/mapping/vehicle-response according to oscillation_decomposition.",
    )


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


def _no_assist_layer(
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    bridge_stats: Mapping[str, Any],
    control_handoff: Mapping[str, Any],
    control_health: Mapping[str, Any],
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
    backend = _first_text(summary, "backend", manifest, "backend")
    control_source = _first_text(summary, "control_source", manifest, "control_source")
    routing_success_count = _first_num(summary.get("routing_success_count"), bridge_stats.get("routing_success_count"))
    planning_nonempty_ratio = _first_num(
        _nested(summary, "metrics.planning_nonempty_ratio"),
        summary.get("planning_nonempty_ratio"),
        _nested(control_handoff, "input_readiness.planning_nonempty_ratio"),
    )
    control_rx_count = _first_num(
        bridge_stats.get("control_rx_count"),
        _nested(control_handoff, "bridge_receive.control_rx_count"),
        _nested(control_handoff, "control_channel.message_count"),
    )
    control_tx_count = _first_num(bridge_stats.get("control_tx_count"), bridge_stats.get("apply_control_count"))
    control_apply_count = _first_num(
        bridge_stats.get("apply_control_count"),
        _nested(control_handoff, "mapping_and_apply.apply_control_count"),
    )
    if backend != "apollo_cyberrt":
        reasons.append("backend_not_apollo_cyberrt")
        explicit_blockers.append("backend_not_apollo_cyberrt")
    if control_source is None:
        reasons.append("control_source_missing")
    elif control_source != "/apollo/control":
        reasons.append("control_source_not_apollo_control")
        explicit_blockers.append("control_source_not_apollo_control")
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
            "routing_success_count": routing_success_count,
            "planning_nonempty_ratio": planning_nonempty_ratio,
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
    special = _reference_line_localization_mismatch(layers)
    blockers = _all_blockers(layers)
    if special:
        secondary = [item for item in blockers if item != special]
        return special, secondary

    loc_ref_pass = _non_blocking(layers.get("localization_gt_contract", {})) and _non_blocking(
        layers.get("planning_reference_line", {})
    )
    control = layers.get("control_mapping_apply", {})
    control_is_oscillation = "applied_actuation_oscillation" in set(control.get("blocking_reasons") or [])

    primary: str | None = None
    for status_group in ({"fail"}, {"insufficient_data"}, {"warn"}):
        for name in LAYER_ORDER:
            if name == "control_mapping_apply" and control_is_oscillation and not loc_ref_pass:
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


def _layer_blocker_name(name: str, layer: Mapping[str, Any]) -> str:
    reasons = list(layer.get("blocking_reasons") or [])
    return f"{name}:{reasons[0]}" if reasons else f"{name}:{layer.get('status')}"


def _can_claim_unassisted(layers: Mapping[str, Mapping[str, Any]]) -> bool:
    required = (
        "environment_world",
        "bridge_runtime",
        "channel_health",
        "localization_gt_contract",
        "hdmap_projection",
        "planning_reference_line",
        "routing_planning_control_handoff",
        "control_mapping_apply",
        "perception_gt_obstacles",
        "traffic_light_gt",
        "no_assist_claim_boundary",
        "natural_driving_outcome",
    )
    if any(not _non_blocking(layers.get(name, {})) for name in required):
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
    no_assist = layers.get("no_assist_claim_boundary", {})
    metrics = no_assist.get("key_metrics") if isinstance(no_assist.get("key_metrics"), Mapping) else {}
    reasons.extend(str(item) for item in metrics.get("why_not_claimable") or [])
    return sorted(set(reason for reason in reasons if reason and reason != "traffic_light_gt:not_applicable"))


def _non_blocking(layer: Mapping[str, Any]) -> bool:
    return layer.get("status") in {"pass", "warn", "not_applicable"} and not layer.get("blocking_reasons")


def _next_highest_value_validation(primary: str | None, layers: Mapping[str, Mapping[str, Any]]) -> str:
    if primary is None:
        return "Run longer route-level validation and natural-driving evaluator with the same artifact set."
    if primary == "reference_line/localization lane-heading mismatch":
        return "Compare localization lane heading, Apollo HDMap projection, and planning/control reference-line heading on the same route_s window."
    layer_name = primary.split(":", 1)[0]
    layer = layers.get(layer_name, {})
    return str(layer.get("next_action") or "Inspect the primary blocker artifact and regenerate missing evidence.")


def _paths(inputs: Mapping[str, Path | None], *names: str) -> dict[str, str | None]:
    return {name: str(inputs.get(name)) if inputs.get(name) else None for name in names}


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = (root / relative).resolve() if relative.startswith("..") else root / relative
        if path.exists():
            return path
    return None


def _find_natural_driving_report_in_ancestors(root: Path) -> Path | None:
    for parent in root.parents:
        for relative in (
            "analysis/natural_driving/natural_driving_report.json",
            "natural_driving_report.json",
        ):
            path = parent / relative
            if path.exists():
                return path
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _normalize_status(value: Any) -> str:
    text = str(value or "insufficient_data").strip()
    if text in {"pass", "warn", "fail", "insufficient_data", "not_applicable"}:
        return text
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
