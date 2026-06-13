from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.adapters.apollo.frame_transform import (
    ApolloFrameTransform,
    Vector3,
    carla_point_to_apollo,
    load_frame_transform,
)
from carla_testbed.analysis.routing_response_decoded import read_routing_response_decoded

APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION = "apollo_route_contract.v1"

MAX_ABS_ROUTE_LENGTH_DIFF_M = 20.0
MAX_REL_ROUTE_LENGTH_DIFF = 0.15
MAX_SCENARIO_ROUTE_SOURCE_DIFF_M = 5.0
MAX_SCENARIO_ROUTE_SOURCE_REL_DIFF = 0.05
MAX_GOAL_ERROR_M = 20.0
GOAL_NEAR_THRESHOLD_RATIO = 0.8
MAX_GOAL_SNAP_DISTANCE_M = 3.0
MAX_GOAL_LATERAL_ERROR_M = 1.0
MAX_EXTRA_LANE_WINDOWS_FOR_CLAIM = 2


def analyze_apollo_route_contract_run_dir(
    run_dir: str | Path,
    *,
    frame_transform: str | Path | Mapping[str, Any] | ApolloFrameTransform | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    frame_transform_obj, frame_transform_source = _resolve_frame_transform(frame_transform)
    routing_response_decoded_path = _find_first(
        root,
        (
            "artifacts/routing_response_decoded.json",
            "artifacts/routing_response_decoded.jsonl",
            "routing_response_decoded.json",
            "routing_response_decoded.jsonl",
        ),
    )
    return analyze_apollo_route_contract(
        manifest=_read_json(_find_first(root, ("manifest.json",))),
        summary=_read_json(_find_first(root, ("summary.json",))),
        cyber_bridge_stats=_read_json(
            _find_first(root, ("artifacts/cyber_bridge_stats.json", "cyber_bridge_stats.json"))
        ),
        planning_topic_debug_summary=_read_json(
            _find_first(
                root,
                (
                    "artifacts/planning_topic_debug_summary.json",
                    "artifacts/planning_topic_debug_summary.finalized.json",
                    "planning_topic_debug_summary.json",
                ),
            )
        ),
        routing_event_debug=_read_jsonl(
            _find_first(root, ("artifacts/routing_event_debug.jsonl", "routing_event_debug.jsonl"))
        ),
        planning_route_segment_debug=_read_jsonl(
            _find_first(
                root,
                (
                    "artifacts/planning_route_segment_debug.jsonl",
                    "artifacts/apollo_route_segment_debug.jsonl",
                    "planning_route_segment_debug.jsonl",
                ),
            )
        ),
        hdmap_projection=_read_jsonl(
            _find_first(root, ("artifacts/apollo_hdmap_projection.jsonl", "apollo_hdmap_projection.jsonl"))
        ),
        routing_response_decoded=read_routing_response_decoded(routing_response_decoded_path),
        frame_transform=frame_transform_obj,
        frame_transform_source=frame_transform_source,
        source={
            "run_dir": str(root),
            "manifest": _path_str(_find_first(root, ("manifest.json",))),
            "summary": _path_str(_find_first(root, ("summary.json",))),
            "cyber_bridge_stats": _path_str(
                _find_first(root, ("artifacts/cyber_bridge_stats.json", "cyber_bridge_stats.json"))
            ),
            "frame_transform": frame_transform_source,
            "planning_topic_debug_summary": _path_str(
                _find_first(
                    root,
                    (
                        "artifacts/planning_topic_debug_summary.json",
                        "artifacts/planning_topic_debug_summary.finalized.json",
                        "planning_topic_debug_summary.json",
                    ),
                )
            ),
            "routing_event_debug": _path_str(
                _find_first(root, ("artifacts/routing_event_debug.jsonl", "routing_event_debug.jsonl"))
            ),
            "routing_response_decoded": _path_str(routing_response_decoded_path),
            "planning_route_segment_debug": _path_str(
                _find_first(
                    root,
                    (
                        "artifacts/planning_route_segment_debug.jsonl",
                        "artifacts/apollo_route_segment_debug.jsonl",
                        "planning_route_segment_debug.jsonl",
                    ),
                )
            ),
            "apollo_hdmap_projection": _path_str(
                _find_first(root, ("artifacts/apollo_hdmap_projection.jsonl", "apollo_hdmap_projection.jsonl"))
            ),
        },
    )


def analyze_apollo_route_contract(
    *,
    manifest: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
    cyber_bridge_stats: Mapping[str, Any] | None = None,
    planning_topic_debug_summary: Mapping[str, Any] | None = None,
    routing_event_debug: Sequence[Mapping[str, Any]] | None = None,
    planning_route_segment_debug: Sequence[Mapping[str, Any]] | None = None,
    hdmap_projection: Sequence[Mapping[str, Any]] | None = None,
    routing_response_decoded: Mapping[str, Any] | None = None,
    frame_transform: ApolloFrameTransform | None = None,
    frame_transform_source: str | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = manifest or {}
    summary = summary or {}
    cyber_bridge_stats = cyber_bridge_stats or {}
    planning_topic_debug_summary = planning_topic_debug_summary or {}
    routing_rows = list(routing_event_debug or [])
    route_rows = list(planning_route_segment_debug or [])
    hdmap_rows = list(hdmap_projection or [])
    routing_response_decoded = routing_response_decoded or {}

    scenario = _scenario_route(manifest, summary)
    apollo = _apollo_route(planning_topic_debug_summary, routing_rows, route_rows, routing_response_decoded)
    projection = _projection_route(hdmap_rows)
    raw_route_phase = _route_phase(apollo, cyber_bridge_stats, scenario.get("route_length_m"))
    last_routing_request = _routing_request_summary(routing_rows, scenario)
    last_routing_response = _routing_response_summary(
        planning_topic_debug_summary,
        routing_rows,
        route_rows,
        apollo,
        routing_response_decoded,
    )
    latest_planning_active_route_segment = _planning_active_route_segment(route_rows)
    scenario_start_xy_carla = scenario.get("start_xy")
    scenario_goal_xy_carla = scenario.get("goal_xy")
    scenario_start_xy_apollo = _transform_xy(scenario_start_xy_carla, frame_transform)
    scenario_goal_xy_apollo = _transform_xy(scenario_goal_xy_carla, frame_transform)
    comparison_frame = "apollo_map" if frame_transform is not None else "unavailable"

    blocking: list[str] = []
    warnings: list[str] = []
    missing: list[str] = []

    scenario_length = scenario.get("route_length_m")
    apollo_length = apollo.get("routing_total_length_m")
    length_delta = _delta(apollo_length, scenario_length)
    length_ratio = (
        apollo_length / scenario_length
        if apollo_length is not None and scenario_length and scenario_length > 0
        else None
    )
    length_tolerance = (
        max(MAX_ABS_ROUTE_LENGTH_DIFF_M, MAX_REL_ROUTE_LENGTH_DIFF * scenario_length)
        if scenario_length and scenario_length > 0
        else None
    )
    if scenario_length is None:
        missing.append("scenario_route_length_m")
    if apollo_length is None:
        missing.append("apollo_routing_total_length_m")
    if scenario.get("route_length_consistency_status") == "inconsistent":
        blocking.append("scenario_route_length_inconsistent")
    if length_delta is not None and length_tolerance is not None and abs(length_delta) > length_tolerance:
        blocking.append("apollo_routing_length_mismatch")

    scenario_lane_keys = set(scenario.get("route_lane_keys") or [])
    apollo_lane_keys = set(apollo.get("routing_lane_keys") or [])
    apollo_extra_lane_keys = sorted(apollo_lane_keys - scenario_lane_keys)
    missing_scenario_lane_keys = sorted(scenario_lane_keys - apollo_lane_keys)
    scenario_lane_namespace = _scenario_lane_namespace(scenario)
    apollo_lane_namespace = "apollo_hdmap"
    lane_namespaces_directly_comparable = scenario_lane_namespace == apollo_lane_namespace
    if not scenario_lane_keys:
        missing.append("scenario_route_lane_ids")
    if not apollo_lane_keys:
        missing.append("apollo_routing_lane_ids")
    expected_lane_window_count = max(1, len(scenario_lane_keys) + MAX_EXTRA_LANE_WINDOWS_FOR_CLAIM)
    apollo_lane_window_count = apollo.get("routing_lane_window_count")
    lane_window_sequence_mismatch = bool(
        apollo_lane_window_count is not None
        and scenario_lane_keys
        and apollo_lane_window_count > expected_lane_window_count
        and apollo_extra_lane_keys
    )

    scenario_goal = scenario_goal_xy_apollo
    apollo_goal = apollo.get("goal_xy")
    goal_error = _distance(scenario_goal, apollo_goal)
    start_error = _distance(scenario_start_xy_apollo, apollo.get("start_xy"))
    if apollo_goal is not None and scenario_goal_xy_carla is not None and frame_transform is None:
        warnings.append("apollo_route_xy_comparison_frame_transform_missing")
    elif goal_error is not None and goal_error > MAX_GOAL_ERROR_M:
        blocking.append("apollo_routing_goal_mismatch")
    elif apollo_goal is None:
        warnings.append("apollo_routing_goal_xy_missing")
    if goal_error is not None and goal_error > MAX_GOAL_ERROR_M * GOAL_NEAR_THRESHOLD_RATIO:
        warnings.append("apollo_routing_goal_near_threshold")
    goal_projection = last_routing_request.get("goal_projection")
    if isinstance(goal_projection, Mapping):
        goal_projection_applied = _parse_bool(goal_projection.get("applied"))
        goal_projection_accepted = _parse_bool(goal_projection.get("accepted"))
        goal_projection_trusted = _parse_bool(
            goal_projection.get("trusted_lane_centerline")
            if "trusted_lane_centerline" in goal_projection
            else goal_projection.get("source_trusted_lane_centerline")
        )
        snap_distance = _num(goal_projection.get("distance_m"))
        lateral_error = _num(goal_projection.get("signed_lateral_error_m"))
        s_error = _num(
            goal_projection.get("s_error_m")
            or goal_projection.get("goal_s_error_m")
            or goal_projection.get("projection_s_error_m")
        )
        if goal_projection_applied is True and goal_projection_accepted is not True:
            blocking.append("unaccepted_goal_projection_applied")
        if goal_projection_applied is True and goal_projection_trusted is not True:
            blocking.append("untrusted_goal_projection_applied")
        if goal_projection_accepted is False:
            blocking.append("apollo_routing_goal_projection_not_accepted")
        if goal_projection_trusted is False:
            blocking.append("apollo_routing_goal_projection_untrusted")
        if snap_distance is not None and snap_distance > MAX_GOAL_SNAP_DISTANCE_M:
            blocking.append("apollo_routing_goal_snap_distance_high")
        if lateral_error is not None and abs(lateral_error) > MAX_GOAL_LATERAL_ERROR_M:
            blocking.append("apollo_routing_goal_lateral_error_high")
        if s_error is not None and abs(s_error) > 5.0:
            blocking.append("apollo_routing_goal_s_error_high")
        if goal_error is not None and goal_error > MAX_GOAL_ERROR_M * GOAL_NEAR_THRESHOLD_RATIO:
            if last_routing_request.get("goal_projection_trusted_lane_centerline") is not True:
                warnings.append("apollo_routing_goal_lane_compatibility_unverified")

    length_compatible = (
        length_delta is not None
        and length_tolerance is not None
        and abs(length_delta) <= length_tolerance
    )
    goal_compatible = goal_error is not None and goal_error <= MAX_GOAL_ERROR_M
    start_compatible = start_error is None or start_error <= MAX_GOAL_ERROR_M
    has_claim_projection = bool(projection.get("available"))
    lane_equivalence_status = "not_evaluated"
    if scenario_lane_keys and apollo_lane_keys:
        if not missing_scenario_lane_keys and not lane_window_sequence_mismatch:
            lane_equivalence_status = "direct_match"
        elif lane_namespaces_directly_comparable:
            lane_equivalence_status = "mismatch"
            if missing_scenario_lane_keys:
                blocking.append("apollo_routing_missing_scenario_lane")
            if lane_window_sequence_mismatch:
                blocking.append("apollo_routing_lane_sequence_mismatch")
        elif length_compatible and goal_compatible and start_compatible:
            lane_equivalence_status = "cross_namespace_unverified"
            warnings.append("scenario_apollo_lane_namespace_equivalence_unverified")
            if not has_claim_projection:
                missing.append("apollo_hdmap_projection_for_lane_equivalence")
                warnings.append("apollo_hdmap_projection_required_for_cross_namespace_lane_equivalence")
        else:
            lane_equivalence_status = "mismatch"
            if missing_scenario_lane_keys:
                blocking.append("apollo_routing_missing_scenario_lane")
            if lane_window_sequence_mismatch:
                blocking.append("apollo_routing_lane_sequence_mismatch")

    route_identity = _route_identity(
        scenario=scenario,
        apollo=apollo,
        raw_route_phase=raw_route_phase,
        last_routing_request=last_routing_request,
        latest_planning_active_route_segment=latest_planning_active_route_segment,
        length_tolerance=length_tolerance,
    )
    if route_identity["status"] == "inconsistent":
        blocking.append("route_identity_inconsistent")

    if not routing_rows:
        warnings.append("routing_event_debug_missing")
    if routing_response_decoded.get("status") != "pass":
        warnings.append("routing_response_decoded_missing")
    if not route_rows:
        warnings.append("planning_route_segment_debug_missing")
    if projection.get("available") is False:
        warnings.append("apollo_hdmap_projection_missing")
    if raw_route_phase["routing_phase"] == "startup":
        blocking.append("claim_route_not_materialized")
    if raw_route_phase["routing_phase"] == "long_goal" and blocking:
        blocking.append("long_goal_not_compatible_with_scenario_route")

    if blocking:
        status = "fail"
    elif missing:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"
    route_phase = _resolved_route_phase(raw_route_phase, status=status)

    startup_route_contract = _startup_route_contract(
        route_phase,
        apollo_length=apollo_length,
        scenario_length=scenario_length,
    )
    claim_route_contract = _claim_route_contract(
        route_phase,
        status=status,
        blocking_reasons=blocking,
        scenario_length=scenario_length,
        apollo_length=apollo_length,
    )

    return {
        "schema_version": APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id"),
        "route_id": _first_text(summary, "route_id", manifest, "route_id", default=scenario.get("route_id")),
        "scenario_id": _first_text(summary, "scenario_id", manifest, "scenario_id"),
        "scenario_class": _first_text(summary, "scenario_class", manifest, "scenario_class"),
        "scenario_route_length_m": scenario_length,
        "scenario_route_length_source": scenario.get("route_length_source"),
        "scenario_route_declared_length_m": scenario.get("route_length_declared_m"),
        "scenario_route_claim_length_m": scenario.get("route_length_claim_m"),
        "scenario_route_claim_length_source": scenario.get("route_length_claim_source"),
        "scenario_route_legacy_length_m": scenario.get("route_length_legacy_m"),
        "scenario_route_legacy_length_role": scenario.get("route_length_legacy_role"),
        "scenario_route_trace_length_m": scenario.get("route_trace_length_m"),
        "scenario_route_trace_length_source": scenario.get("route_trace_length_source"),
        "scenario_route_trace_point_count": scenario.get("route_trace_point_count"),
        "scenario_route_length_consistency_status": scenario.get("route_length_consistency_status"),
        "scenario_route_length_consistency_reason": scenario.get("route_length_consistency_reason"),
        "scenario_route_length_source_delta_m": scenario.get("route_length_source_delta_m"),
        "scenario_route_length_source_ratio": scenario.get("route_length_source_ratio"),
        "scenario_start_lane": scenario.get("start_lane"),
        "scenario_goal_lane": scenario.get("goal_lane"),
        "scenario_route_lane_keys": sorted(scenario_lane_keys),
        "scenario_lane_namespace": scenario_lane_namespace,
        "apollo_lane_namespace": apollo_lane_namespace,
        "lane_namespaces_directly_comparable": lane_namespaces_directly_comparable,
        "lane_equivalence_status": lane_equivalence_status,
        "configured_scenario_route": _configured_scenario_route_report(
            scenario,
            start_xy_apollo=scenario_start_xy_apollo,
            goal_xy_apollo=scenario_goal_xy_apollo,
        ),
        "comparison_frame": comparison_frame,
        "transform_source": frame_transform_source,
        "scenario_start_xy": scenario_start_xy_apollo,
        "scenario_goal_xy": scenario_goal_xy_apollo,
        "scenario_start_xy_carla": scenario_start_xy_carla,
        "scenario_goal_xy_carla": scenario_goal_xy_carla,
        "scenario_start_xy_apollo": scenario_start_xy_apollo,
        "scenario_goal_xy_apollo": scenario_goal_xy_apollo,
        "apollo_routing_total_length_m": apollo_length,
        "apollo_routing_lane_window_count": apollo_lane_window_count,
        "apollo_routing_lane_signature": apollo.get("routing_lane_signature"),
        "apollo_routing_unique_lane_signature": apollo.get("routing_unique_lane_signature"),
        "apollo_routing_lane_keys": sorted(apollo_lane_keys),
        "apollo_routing_extra_lane_keys": apollo_extra_lane_keys,
        "apollo_routing_missing_scenario_lane_keys": missing_scenario_lane_keys,
        "apollo_start_xy": apollo.get("start_xy"),
        "apollo_goal_xy": apollo_goal,
        "start_xy_error_m": start_error,
        "goal_xy_error_m": goal_error,
        "routing_length_delta_m": length_delta,
        "routing_length_ratio": length_ratio,
        "routing_length_tolerance_m": length_tolerance,
        "goal_xy_error_threshold_m": MAX_GOAL_ERROR_M,
        "goal_xy_near_threshold_ratio": GOAL_NEAR_THRESHOLD_RATIO,
        "goal_snap_distance_threshold_m": MAX_GOAL_SNAP_DISTANCE_M,
        "goal_lateral_error_threshold_m": MAX_GOAL_LATERAL_ERROR_M,
        "routing_phase": route_phase["routing_phase"],
        "raw_routing_phase": raw_route_phase["routing_phase"],
        "routing_phase_reason": route_phase["routing_phase_reason"],
        "last_routing_request": last_routing_request,
        "last_routing_response": last_routing_response,
        "routing_response_decoded": _routing_response_decoded_summary(routing_response_decoded),
        "latest_planning_active_route_segment": latest_planning_active_route_segment,
        "route_identity_status": route_identity["status"],
        "route_identity_issues": route_identity["issues"],
        "route_identity": route_identity,
        "startup_route_contract": startup_route_contract,
        "claim_route_contract": claim_route_contract,
        "hdmap_projection_available": projection.get("available"),
        "hdmap_projection_lane_keys": projection.get("lane_keys"),
        "status": status,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "source": dict(source or {}),
        "interpretation_boundary": (
            "Routing success only proves Apollo produced a response. This contract checks "
            "whether that response is compatible with the scenario route before Planning "
            "or Control output can support a natural-driving claim."
        ),
    }


def write_apollo_route_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "apollo_route_contract_report.json"
    summary_path = output / "apollo_route_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_route_contract_summary_md(report), encoding="utf-8")
    return {
        "apollo_route_contract_report": str(report_path),
        "apollo_route_contract_summary": str(summary_path),
    }


def apollo_route_contract_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Apollo Route Contract Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Routing phase: `{report.get('routing_phase')}`",
            f"- Raw routing phase: `{report.get('raw_routing_phase')}`",
            f"- Comparison frame: `{report.get('comparison_frame')}`",
            f"- Transform source: `{report.get('transform_source')}`",
            f"- Route ID: `{report.get('route_id')}`",
            f"- Scenario route length m: `{report.get('scenario_route_length_m')}`",
            f"- Scenario route length source: `{report.get('scenario_route_length_source')}`",
            f"- Scenario route claim length m: `{report.get('scenario_route_claim_length_m')}`",
            f"- Scenario route legacy length m: `{report.get('scenario_route_legacy_length_m')}`",
            f"- Scenario route legacy length role: `{report.get('scenario_route_legacy_length_role')}`",
            f"- Scenario route trace length m: `{report.get('scenario_route_trace_length_m')}`",
            f"- Scenario route length consistency: `{report.get('scenario_route_length_consistency_status')}`",
            f"- Scenario route length consistency reason: `{report.get('scenario_route_length_consistency_reason')}`",
            f"- Apollo routing total length m: `{report.get('apollo_routing_total_length_m')}`",
            f"- Routing length ratio: `{report.get('routing_length_ratio')}`",
            f"- Scenario start/goal lane: `{report.get('scenario_start_lane')}` / `{report.get('scenario_goal_lane')}`",
            f"- Scenario lane namespace: `{report.get('scenario_lane_namespace')}`",
            f"- Apollo lane namespace: `{report.get('apollo_lane_namespace')}`",
            f"- Lane equivalence status: `{report.get('lane_equivalence_status')}`",
            f"- Apollo lane windows: `{report.get('apollo_routing_lane_window_count')}`",
            f"- Apollo lane signature: `{report.get('apollo_routing_lane_signature')}`",
            f"- Extra Apollo lane keys: `{', '.join(report.get('apollo_routing_extra_lane_keys') or []) or 'none'}`",
            f"- Route identity: `{_json_compact(report.get('route_identity'))}`",
            f"- Last routing request: `{_json_compact(report.get('last_routing_request'))}`",
            f"- Latest Planning active route segment: `{_json_compact(report.get('latest_planning_active_route_segment'))}`",
            f"- Startup route contract: `{_json_compact(report.get('startup_route_contract'))}`",
            f"- Claim route contract: `{_json_compact(report.get('claim_route_contract'))}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )


def _resolve_frame_transform(
    value: str | Path | Mapping[str, Any] | ApolloFrameTransform | None,
) -> tuple[ApolloFrameTransform | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, ApolloFrameTransform):
        return value, "object"
    if isinstance(value, Mapping):
        return ApolloFrameTransform.from_mapping(value), "mapping"
    path = Path(value).expanduser()
    return load_frame_transform(path), str(path)


def _transform_xy(value: Any, transform: ApolloFrameTransform | None) -> dict[str, float] | None:
    if transform is None or not isinstance(value, Mapping):
        return None
    x = _num(value.get("x"))
    y = _num(value.get("y"))
    if x is None or y is None:
        return None
    point = carla_point_to_apollo(Vector3(float(x), float(y), 0.0), transform)
    return {"x": point.x, "y": point.y}


def _route_phase(
    apollo: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
    scenario_length: Any,
) -> dict[str, Any]:
    health = cyber_bridge_stats.get("health")
    if not isinstance(health, Mapping):
        health = {}
    reason = _first_nonempty_text(
        cyber_bridge_stats.get("last_routing_reason"),
        health.get("last_routing_reason"),
        cyber_bridge_stats.get("routing_goal_mode"),
        health.get("routing_goal_mode"),
    )
    startup_used = _truthy(cyber_bridge_stats.get("routing_startup_phase_used")) or _truthy(
        health.get("routing_startup_phase_used")
    )
    goal_mode = _first_nonempty_text(
        cyber_bridge_stats.get("routing_goal_mode"),
        health.get("routing_goal_mode"),
        apollo.get("routing_goal_mode"),
    )
    event_phase = str(apollo.get("routing_event_phase") or "").strip()
    request_kind = str(apollo.get("routing_request_kind") or "").strip()
    if event_phase == "startup" or request_kind == "startup_route":
        return {"routing_phase": "startup", "routing_phase_reason": reason or "startup_route_detected"}
    if event_phase in {"long", "long_goal"} or request_kind == "long_phase_route":
        return {"routing_phase": "long_goal", "routing_phase_reason": reason or "long_goal_route_detected"}
    goal_dist = _first_num(
        cyber_bridge_stats.get("routing_goal_dist_m"),
        health.get("routing_goal_dist_m"),
        apollo.get("routing_total_length_m"),
    )
    scenario_length_num = _num(scenario_length)
    if startup_used or goal_mode == "ego_seed_ahead" or str(reason or "").startswith("startup"):
        return {"routing_phase": "startup", "routing_phase_reason": reason or "startup_route_detected"}
    if (
        goal_dist is not None
        and scenario_length_num is not None
        and scenario_length_num > 80.0
        and goal_dist <= max(60.0, scenario_length_num * 0.35)
    ):
        return {"routing_phase": "startup", "routing_phase_reason": "routing_length_matches_short_startup_goal"}
    if apollo.get("routing_total_length_m") is not None:
        return {"routing_phase": "claim", "routing_phase_reason": reason or "routing_response_present"}
    return {"routing_phase": "unknown", "routing_phase_reason": reason or "routing_phase_unverified"}


def _resolved_route_phase(route_phase: Mapping[str, Any], *, status: str) -> dict[str, Any]:
    raw = str(route_phase.get("routing_phase") or "unknown")
    reason = str(route_phase.get("routing_phase_reason") or "")
    if raw in {"startup", "unknown"}:
        return {"routing_phase": raw, "routing_phase_reason": reason}
    if status in {"pass", "warn"}:
        return {"routing_phase": "claim", "routing_phase_reason": reason or "scenario_route_compatible"}
    if raw == "long_goal":
        return {"routing_phase": "long_goal", "routing_phase_reason": reason or "long_goal_not_claim_compatible"}
    return {"routing_phase": raw, "routing_phase_reason": reason}


def _startup_route_contract(
    route_phase: Mapping[str, Any],
    *,
    apollo_length: float | None,
    scenario_length: float | None,
) -> dict[str, Any]:
    if route_phase.get("routing_phase") != "startup":
        return {
            "status": "not_applicable",
            "diagnostic_only": False,
            "reason": route_phase.get("routing_phase_reason"),
        }
    return {
        "status": "diagnostic_only",
        "diagnostic_only": True,
        "reason": route_phase.get("routing_phase_reason"),
        "apollo_routing_total_length_m": apollo_length,
        "scenario_route_length_m": scenario_length,
        "claim_boundary": "Startup ego-seed routing can unblock module startup but cannot materialize the full scenario route.",
    }


def _claim_route_contract(
    route_phase: Mapping[str, Any],
    *,
    status: str,
    blocking_reasons: Sequence[str],
    scenario_length: float | None,
    apollo_length: float | None,
) -> dict[str, Any]:
    if route_phase.get("routing_phase") == "startup":
        return {
            "status": "fail",
            "materialized": False,
            "blocking_reasons": ["claim_route_not_materialized"],
            "scenario_route_length_m": scenario_length,
            "apollo_routing_total_length_m": apollo_length,
        }
    if route_phase.get("routing_phase") == "unknown":
        return {
            "status": "insufficient_data",
            "materialized": False,
            "blocking_reasons": ["claim_route_phase_unverified"],
            "scenario_route_length_m": scenario_length,
            "apollo_routing_total_length_m": apollo_length,
        }
    return {
        "status": status,
        "materialized": status in {"pass", "warn"},
        "blocking_reasons": sorted(set(blocking_reasons)),
        "scenario_route_length_m": scenario_length,
        "apollo_routing_total_length_m": apollo_length,
    }


def _configured_scenario_route_report(
    scenario: Mapping[str, Any],
    *,
    start_xy_apollo: Mapping[str, Any] | None,
    goal_xy_apollo: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "route_id": scenario.get("route_id"),
        "route_length_m": scenario.get("route_length_m"),
        "route_length_source": scenario.get("route_length_source"),
        "route_length_declared_m": scenario.get("route_length_declared_m"),
        "route_length_claim_m": scenario.get("route_length_claim_m"),
        "route_length_claim_source": scenario.get("route_length_claim_source"),
        "route_length_legacy_m": scenario.get("route_length_legacy_m"),
        "route_length_legacy_role": scenario.get("route_length_legacy_role"),
        "route_trace_length_m": scenario.get("route_trace_length_m"),
        "route_trace_length_source": scenario.get("route_trace_length_source"),
        "route_trace_point_count": scenario.get("route_trace_point_count"),
        "route_length_consistency_status": scenario.get("route_length_consistency_status"),
        "route_length_consistency_reason": scenario.get("route_length_consistency_reason"),
        "start_lane": scenario.get("start_lane"),
        "goal_lane": scenario.get("goal_lane"),
        "route_lane_keys": list(scenario.get("route_lane_keys") or []),
        "start_xy_carla": scenario.get("start_xy"),
        "goal_xy_carla": scenario.get("goal_xy"),
        "start_xy_apollo": dict(start_xy_apollo) if isinstance(start_xy_apollo, Mapping) else None,
        "goal_xy_apollo": dict(goal_xy_apollo) if isinstance(goal_xy_apollo, Mapping) else None,
    }


def _routing_request_summary(
    routing_rows: Sequence[Mapping[str, Any]],
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    row = _last_mapping(routing_rows)
    if not row:
        return {
            "available": False,
            "expected_route_id": scenario.get("route_id"),
            "expected_route_length_m": scenario.get("route_length_m"),
        }
    goal_projection = _projection_summary(row.get("goal_projection"))
    start_projection = _projection_summary(row.get("start_projection"))
    return {
        "available": True,
        "request_sequence": _first_int(row.get("routing_request_index")),
        "routing_phase": _text_or_none(row.get("routing_phase")),
        "routing_request_kind": _text_or_none(row.get("routing_request_kind")),
        "reroute_reason": _text_or_none(row.get("reroute_reason")),
        "trigger_source": _text_or_none(row.get("trigger_source")),
        "goal_mode": _text_or_none(row.get("goal_mode")),
        "requested_goal_mode": _text_or_none(row.get("requested_goal_mode")),
        "goal_source": _text_or_none(row.get("goal_source")),
        "anchor_mode": _text_or_none(row.get("anchor_mode")),
        "goal_distance_m": _num(row.get("goal_distance_m")),
        "start_xy": _xy_from_keys(row, ("start_raw_x", "start_raw_y")) or _xy_from_keys(row, ("start_x", "start_y")),
        "goal_xy": _xy_from_keys(row, ("goal_raw_x", "goal_raw_y")) or _xy_from_keys(row, ("goal_x", "goal_y")),
        "start_projection": start_projection,
        "goal_projection": goal_projection,
        "goal_projection_distance_m": _num(goal_projection.get("distance_m")),
        "goal_projection_trusted_lane_centerline": goal_projection.get("trusted_lane_centerline"),
        "expected_route_id": scenario.get("route_id"),
        "expected_route_length_m": scenario.get("route_length_m"),
    }


def _routing_response_summary(
    planning_summary: Mapping[str, Any],
    routing_rows: Sequence[Mapping[str, Any]],
    route_rows: Sequence[Mapping[str, Any]],
    apollo: Mapping[str, Any],
    routing_response_decoded: Mapping[str, Any],
) -> dict[str, Any]:
    last_request = _last_mapping(routing_rows)
    last_route = _last_route_row_with_signature(route_rows)
    decoded = _routing_response_decoded_summary(routing_response_decoded)
    decoded_available = decoded.get("available") is True
    return {
        "available": apollo.get("routing_total_length_m") is not None,
        "response_sequence": _first_int(
            routing_response_decoded.get("response_sequence"),
            planning_summary.get("last_planning_header_sequence_num"),
            last_route.get("planning_header_sequence_num") if last_route else None,
        ),
        "matched_request_sequence": _first_int(last_request.get("routing_request_index") if last_request else None),
        "response_total_length_m": apollo.get("routing_total_length_m"),
        "lane_window_count": apollo.get("routing_lane_window_count"),
        "lane_window_signature": apollo.get("routing_lane_signature"),
        "unique_lane_signature": apollo.get("routing_unique_lane_signature"),
        "decoded": decoded,
        "source": (
            "routing_response_decoded"
            if decoded_available
            else "planning_topic_debug_summary_or_route_segment_debug"
        ),
    }


def _planning_active_route_segment(route_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    row = _last_route_row_with_signature(route_rows)
    if not row:
        return {"available": False}
    return {
        "available": True,
        "planning_header_sequence_num": _first_int(row.get("planning_header_sequence_num")),
        "planning_header_timestamp_sec": _num(row.get("planning_header_timestamp_sec")),
        "sim_time_sec": _num(row.get("sim_time_sec")),
        "route_segment_total_length_m": _num(row.get("route_segment_total_length")),
        "route_segment_count": _first_int(row.get("route_segment_count")),
        "routing_lane_window_count": _first_int(row.get("routing_lane_window_count")),
        "routing_lane_window_signature": _text_or_none(row.get("routing_lane_window_signature")),
        "routing_unique_lane_signature": _text_or_none(row.get("routing_unique_lane_signature")),
        "current_lane_id": _text_or_none(row.get("current_lane_id")),
        "lane_id_first": _text_or_none(row.get("lane_id_first")),
        "target_lane_id_first": _text_or_none(row.get("target_lane_id_first")),
        "reference_line_provider_status": _text_or_none(row.get("reference_line_provider_status")),
        "create_route_segments_status": _text_or_none(row.get("create_route_segments_status")),
        "lane_follow_map_status": _text_or_none(row.get("lane_follow_map_status")),
    }


def _route_identity(
    *,
    scenario: Mapping[str, Any],
    apollo: Mapping[str, Any],
    raw_route_phase: Mapping[str, Any],
    last_routing_request: Mapping[str, Any],
    latest_planning_active_route_segment: Mapping[str, Any],
    length_tolerance: float | None,
) -> dict[str, Any]:
    issues: list[str] = []
    scenario_length = _num(scenario.get("route_length_m"))
    apollo_length = _num(apollo.get("routing_total_length_m"))
    active_length = _num(latest_planning_active_route_segment.get("route_segment_total_length_m"))
    raw_phase = str(raw_route_phase.get("routing_phase") or "unknown")
    if scenario.get("route_length_consistency_status") == "inconsistent":
        issues.append("scenario_route_length_inconsistent")
    if scenario_length is None:
        issues.append("scenario_route_length_missing")
    if apollo_length is None:
        issues.append("apollo_routing_response_missing")
    if (
        scenario_length is not None
        and apollo_length is not None
        and length_tolerance is not None
        and abs(apollo_length - scenario_length) > length_tolerance
    ):
        issues.append("routing_response_length_not_scenario_route")
    if (
        scenario_length is not None
        and active_length is not None
        and length_tolerance is not None
        and abs(active_length - scenario_length) > length_tolerance
    ):
        issues.append("planning_active_route_segment_length_not_scenario_route")
    if raw_phase == "long_goal" and issues:
        issues.append("long_goal_not_proven_as_claim_route")
    request_phase = str(last_routing_request.get("routing_phase") or "")
    if request_phase == "startup" and active_length is not None:
        issues.append("planning_active_route_after_startup_request_only")
    status = "insufficient_data" if any(item.endswith("_missing") for item in issues) else (
        "inconsistent" if issues else "consistent"
    )
    return {
        "status": status,
        "issues": sorted(set(issues)),
        "scenario_route_length_m": scenario_length,
        "scenario_route_length_source": scenario.get("route_length_source"),
        "scenario_route_declared_length_m": scenario.get("route_length_declared_m"),
        "scenario_route_trace_length_m": scenario.get("route_trace_length_m"),
        "scenario_route_length_consistency_status": scenario.get("route_length_consistency_status"),
        "scenario_route_length_consistency_reason": scenario.get("route_length_consistency_reason"),
        "apollo_routing_total_length_m": apollo_length,
        "latest_planning_active_route_length_m": active_length,
        "raw_routing_phase": raw_phase,
        "last_routing_request_phase": last_routing_request.get("routing_phase"),
        "last_routing_request_kind": last_routing_request.get("routing_request_kind"),
        "claim_boundary": (
            "A Planning response can consume a routing response without consuming the configured "
            "scenario route. Natural-driving claims require the active route identity to be compatible "
            "with the configured scenario route."
        ),
    }


def _projection_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"available": False}
    return {
        "available": bool(value.get("available", True)),
        "accepted": value.get("accepted"),
        "rejected": value.get("rejected"),
        "applied": value.get("applied"),
        "source_type": _text_or_none(value.get("source_type")),
        "trusted_lane_centerline": _parse_bool(
            value.get("trusted_lane_centerline")
            if "trusted_lane_centerline" in value
            else value.get("source_trusted_lane_centerline")
        ),
        "distance_m": _num(value.get("distance_m")),
        "signed_lateral_error_m": _num(value.get("signed_e_y_m") or value.get("lateral_error_m")),
        "s_error_m": _num(
            value.get("s_error_m")
            or value.get("goal_s_error_m")
            or value.get("projection_s_error_m")
        ),
        "lane_yaw_deg": _num(value.get("lane_yaw_deg")),
        "map_file": _text_or_none(value.get("map_file")),
        "reason": _text_or_none(value.get("reason")),
        "reject_reason": _text_or_none(value.get("reject_reason")),
    }


def _scenario_route(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> dict[str, Any]:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), Mapping) else {}
    scenario = metadata.get("scenario_metadata") if isinstance(metadata.get("scenario_metadata"), Mapping) else {}
    route_trace = scenario.get("route_trace") if isinstance(scenario.get("route_trace"), list) else []
    route_lanes = [_lane_id_from_trace(row) for row in route_trace if isinstance(row, Mapping)]
    route_lanes = [lane for lane in route_lanes if lane]
    start_lane = _lane_id_from_mapping(scenario.get("spawn_lane")) or (route_lanes[0] if route_lanes else None)
    goal_lane = _lane_id_from_mapping(scenario.get("goal_lane")) or (route_lanes[-1] if route_lanes else None)
    start_xy = _xy(scenario.get("spawn")) or _xy(route_trace[0]) if route_trace else _xy(scenario.get("spawn"))
    goal_xy = _xy(scenario.get("goal")) or _xy(route_trace[-1]) if route_trace else _xy(scenario.get("goal"))
    legacy_declared_length = _first_num(
        scenario.get("route_length_m"),
        summary.get("route_length_m"),
        summary.get("expected_route_distance_m"),
    )
    explicit_claim_length = _first_num(
        scenario.get("claim_route_length_m"),
        scenario.get("route_trace_length_m"),
        scenario.get("route_claim_length_m"),
    )
    trace_length, trace_length_source = _route_trace_length(route_trace)
    if explicit_claim_length is not None:
        route_length = explicit_claim_length
        route_length_source = _first_nonempty_text(
            scenario.get("claim_route_length_source"),
            "claim_route_length_m"
            if scenario.get("claim_route_length_m") is not None
            else scenario.get("route_trace_length_source"),
            "route_trace_length_m",
        )
        consistency = _route_length_consistency(route_length, trace_length)
        consistency["route_length_consistency_reason"] = _claim_route_length_reason(
            consistency.get("route_length_consistency_reason")
        )
    else:
        route_length = legacy_declared_length if legacy_declared_length is not None else trace_length
        route_length_source = "declared_metadata" if legacy_declared_length is not None else (
            trace_length_source if trace_length is not None else "missing"
        )
        consistency = _route_length_consistency(legacy_declared_length, trace_length)
    lane_keys = {_lane_key(lane) for lane in [*route_lanes, start_lane, goal_lane] if lane}
    return {
        "route_id": scenario.get("route_id") or summary.get("route_id") or manifest.get("route_id"),
        "route_length_m": route_length,
        "route_length_source": route_length_source,
        "route_length_declared_m": legacy_declared_length,
        "route_length_claim_m": explicit_claim_length,
        "route_length_claim_source": _text_or_none(scenario.get("claim_route_length_source")),
        "route_length_legacy_m": legacy_declared_length,
        "route_length_legacy_role": _text_or_none(scenario.get("route_length_m_role")),
        "route_trace_length_m": trace_length,
        "route_trace_length_source": trace_length_source,
        "route_trace_point_count": len(route_trace),
        "route_trace_source": _text_or_none(scenario.get("route_trace_source")),
        **consistency,
        "start_lane": start_lane,
        "goal_lane": goal_lane,
        "route_lane_keys": sorted(key for key in lane_keys if key),
        "start_xy": start_xy,
        "goal_xy": goal_xy,
    }


def _claim_route_length_reason(reason: Any) -> str | None:
    text = _text_or_none(reason)
    if text is None:
        return None
    replacements = {
        "declared_route_length_missing_trace_used": "claim_route_length_missing_trace_used",
        "route_trace_length_missing_declared_used": "route_trace_length_missing_claim_used",
        "declared_route_length_disagrees_with_route_trace": "claim_route_length_disagrees_with_route_trace",
        "declared_route_length_matches_route_trace": "claim_route_length_matches_route_trace",
    }
    return replacements.get(text, text)


def _scenario_lane_namespace(scenario: Mapping[str, Any]) -> str:
    source = str(scenario.get("route_trace_source") or "").strip().lower()
    if "apollo" in source or "hdmap" in source:
        return "apollo_hdmap"
    if "carla" in source or "waypoint" in source or source.startswith("town01_"):
        return "carla_waypoint"
    # Town01 route-health metadata records lanes as CARLA road/section/lane ids.
    if scenario.get("route_trace_point_count") or scenario.get("route_trace_length_m"):
        return "carla_waypoint"
    return "unknown"


def _route_trace_length(route_trace: Sequence[Any]) -> tuple[float | None, str | None]:
    rows = [row for row in route_trace if isinstance(row, Mapping)]
    if len(rows) < 2:
        return None, None
    first_s = _num(rows[0].get("s"))
    last_s = _num(rows[-1].get("s"))
    if first_s is not None and last_s is not None:
        return abs(last_s - first_s), "route_trace_s_span"
    total = 0.0
    used_segments = 0
    previous = _xy(rows[0])
    for row in rows[1:]:
        current = _xy(row)
        if previous is not None and current is not None:
            total += math.hypot(current["x"] - previous["x"], current["y"] - previous["y"])
            used_segments += 1
        previous = current
    if used_segments:
        return total, "route_trace_xy_polyline"
    return None, None


def _route_length_consistency(
    declared_length: float | None,
    trace_length: float | None,
) -> dict[str, Any]:
    if declared_length is None and trace_length is None:
        return {
            "route_length_consistency_status": "missing",
            "route_length_consistency_reason": "scenario_route_length_missing",
            "route_length_source_delta_m": None,
            "route_length_source_ratio": None,
        }
    if declared_length is None:
        return {
            "route_length_consistency_status": "trace_only",
            "route_length_consistency_reason": "declared_route_length_missing_trace_used",
            "route_length_source_delta_m": None,
            "route_length_source_ratio": None,
        }
    if trace_length is None:
        return {
            "route_length_consistency_status": "declared_only",
            "route_length_consistency_reason": "route_trace_length_missing_declared_used",
            "route_length_source_delta_m": None,
            "route_length_source_ratio": None,
        }
    delta = trace_length - declared_length
    ratio = trace_length / declared_length if declared_length > 0 else None
    tolerance = max(
        MAX_SCENARIO_ROUTE_SOURCE_DIFF_M,
        MAX_SCENARIO_ROUTE_SOURCE_REL_DIFF * max(abs(declared_length), abs(trace_length)),
    )
    if abs(delta) > tolerance:
        return {
            "route_length_consistency_status": "inconsistent",
            "route_length_consistency_reason": "declared_route_length_disagrees_with_route_trace",
            "route_length_source_delta_m": delta,
            "route_length_source_ratio": ratio,
        }
    return {
        "route_length_consistency_status": "consistent",
        "route_length_consistency_reason": "declared_route_length_matches_route_trace",
        "route_length_source_delta_m": delta,
        "route_length_source_ratio": ratio,
    }


def _apollo_route(
    planning_summary: Mapping[str, Any],
    routing_rows: Sequence[Mapping[str, Any]],
    route_rows: Sequence[Mapping[str, Any]],
    routing_response_decoded: Mapping[str, Any],
) -> dict[str, Any]:
    last_routing_row = _last_mapping(routing_rows)
    last_route_row = _last_route_row_with_signature(route_rows)
    decoded_status = str(routing_response_decoded.get("status") or "")
    decoded_lane_ids = [
        str(item).strip()
        for item in (routing_response_decoded.get("lane_sequence_signature") or [])
        if str(item).strip()
    ]
    decoded_lane_segments = (
        routing_response_decoded.get("lane_segments")
        if isinstance(routing_response_decoded.get("lane_segments"), list)
        else []
    )
    if not decoded_lane_ids:
        decoded_lane_ids = [
            str(row.get("lane_id") or "").strip()
            for row in decoded_lane_segments
            if isinstance(row, Mapping) and str(row.get("lane_id") or "").strip()
        ]
    lane_signature = _first_text(
        {"decoded": " | ".join(decoded_lane_ids) if decoded_status == "pass" and decoded_lane_ids else None},
        "decoded",
        planning_summary,
        "last_routing_lane_window_signature",
        last_routing_row,
        "routing_lane_window_signature",
        last_route_row,
        "routing_lane_window_signature",
    )
    unique_signature = _first_text(
        {"decoded": " | ".join(_unique_keep_order(decoded_lane_ids)) if decoded_status == "pass" and decoded_lane_ids else None},
        "decoded",
        planning_summary,
        "last_routing_unique_lane_signature",
        last_routing_row,
        "routing_unique_lane_signature",
        last_route_row,
        "routing_unique_lane_signature",
    )
    total_length = _first_num(
        routing_response_decoded.get("total_length_m") if decoded_status == "pass" else None,
        planning_summary.get("last_routing_total_length"),
        last_routing_row.get("routing_total_length") if last_routing_row else None,
        last_routing_row.get("route_total_length_m") if last_routing_row else None,
        last_routing_row.get("goal_distance_m") if last_routing_row else None,
    )
    window_count = _first_int(
        routing_response_decoded.get("lane_segment_count") if decoded_status == "pass" else None,
        planning_summary.get("last_routing_lane_window_count"),
        last_routing_row.get("routing_lane_window_count") if last_routing_row else None,
        last_route_row.get("routing_lane_window_count") if last_route_row else None,
    )
    lane_ids = _lane_ids_from_signature(lane_signature) or _lane_ids_from_signature(unique_signature)
    if decoded_status == "pass" and decoded_lane_ids:
        lane_ids = decoded_lane_ids
    start_xy = _xy_from_keys(last_routing_row, ("start_raw_x", "start_raw_y")) or _xy_from_keys(
        last_routing_row,
        ("start_x", "start_y"),
    )
    goal_xy = _xy_from_keys(last_routing_row, ("goal_raw_x", "goal_raw_y")) or _xy_from_keys(
        last_routing_row,
        ("goal_x", "goal_y"),
    )
    return {
        "routing_total_length_m": total_length,
        "routing_lane_window_count": window_count,
        "routing_lane_signature": lane_signature,
        "routing_unique_lane_signature": unique_signature,
        "routing_lane_keys": sorted({_lane_key(lane) for lane in lane_ids if _lane_key(lane)}),
        "start_xy": start_xy,
        "goal_xy": goal_xy,
        "routing_event_phase": _text_or_none(last_routing_row.get("routing_phase") if last_routing_row else None),
        "routing_request_kind": _text_or_none(last_routing_row.get("routing_request_kind") if last_routing_row else None),
        "routing_goal_mode": _text_or_none(last_routing_row.get("goal_mode") if last_routing_row else None),
        "routing_source": "routing_response_decoded" if decoded_status == "pass" else "planning_topic_debug_summary_or_route_segment_debug",
    }


def _routing_response_decoded_summary(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("status") != "pass":
        return {
            "available": False,
            "status": value.get("status") if isinstance(value, Mapping) else "missing",
            "missing_fields": list(value.get("missing_fields") or []) if isinstance(value, Mapping) else [],
        }
    return {
        "available": True,
        "status": value.get("status"),
        "source": value.get("source"),
        "response_sequence": _first_int(value.get("response_sequence")),
        "total_length_m": _num(value.get("total_length_m")),
        "lane_segment_count": _first_int(value.get("lane_segment_count")),
        "lane_sequence_signature": list(value.get("lane_sequence_signature") or []),
    }


def _projection_route(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    official = [row for row in rows if str(row.get("source") or "") == "apollo_hdmap_api"]
    lanes = sorted({_lane_key(str(row.get("nearest_lane_id") or "")) for row in official if row.get("nearest_lane_id")})
    lanes = [lane for lane in lanes if lane]
    return {"available": bool(official), "lane_keys": lanes}


def _lane_id_from_mapping(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    road = value.get("road_id")
    section = value.get("section_id", 0)
    lane = value.get("lane_id")
    if road is None or lane is None:
        return None
    return f"{road}:{section}:{lane}"


def _lane_id_from_trace(row: Mapping[str, Any]) -> str | None:
    lane_id = row.get("lane_id")
    if lane_id:
        return str(lane_id)
    return _lane_id_from_mapping(row)


def _lane_ids_from_signature(signature: str | None) -> list[str]:
    if not signature:
        return []
    lane_ids: list[str] = []
    for token in signature.split("|"):
        clean = token.strip()
        if not clean or clean.startswith("..."):
            continue
        clean = clean.split("@", 1)[0].strip()
        if re.match(r"^-?\d+_-?\d+_-?\d+$", clean):
            lane_ids.append(clean)
    return lane_ids


def _unique_keep_order(items: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _lane_key(lane_id: str | None) -> str | None:
    if not lane_id:
        return None
    text = str(lane_id).strip()
    if not text or text == "none":
        return None
    sep = ":" if ":" in text else "_"
    parts = text.split(sep)
    if len(parts) < 3:
        return text
    road = parts[0]
    lane = parts[-1]
    return f"{road}:{lane}"


def _last_route_row_with_signature(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    for row in reversed(list(rows)):
        if row.get("routing_lane_window_signature") not in {None, "", "none"}:
            return row
    return {}


def _last_mapping(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    for row in reversed(list(rows)):
        if isinstance(row, Mapping):
            return row
    return {}


def _xy(value: Any) -> dict[str, float] | None:
    if not isinstance(value, Mapping):
        return None
    x = _num(value.get("x"))
    y = _num(value.get("y"))
    if x is None or y is None:
        return None
    return {"x": x, "y": y}


def _xy_from_keys(row: Mapping[str, Any], keys: tuple[str, str]) -> dict[str, float] | None:
    if not isinstance(row, Mapping):
        return None
    x = _num(row.get(keys[0]))
    y = _num(row.get(keys[1]))
    if x is None or y is None:
        return None
    return {"x": x, "y": y}


def _distance(left: Any, right: Any) -> float | None:
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return None
    lx = _num(left.get("x"))
    ly = _num(left.get("y"))
    rx = _num(right.get("x"))
    ry = _num(right.get("y"))
    if None in {lx, ly, rx, ry}:
        return None
    return math.hypot(float(lx) - float(rx), float(ly) - float(ry))


def _delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _find_first(root: Path, rels: Sequence[str]) -> Path | None:
    for rel in rels:
        path = root / rel
        if path.is_file():
            return path
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path | None) -> list[Mapping[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[Mapping[str, Any]] = []
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, Mapping):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _path_str(path: Path | None) -> str | None:
    return str(path) if path else None


def _first_text(*items: Any, default: Any = None) -> str | None:
    if len(items) % 2 != 0:
        return str(default) if default is not None else None
    for index in range(0, len(items), 2):
        mapping, key = items[index], items[index + 1]
        if isinstance(mapping, Mapping):
            value = mapping.get(key)
            if value not in {None, ""}:
                return str(value)
    return str(default) if default is not None else None


def _first_num(*items: Any) -> float | None:
    for item in items:
        number = _num(item)
        if number is not None:
            return number
    return None


def _first_int(*items: Any) -> int | None:
    number = _first_num(*items)
    return int(number) if number is not None else None


def _first_nonempty_text(*items: Any) -> str | None:
    for item in items:
        if item not in {None, ""}:
            text = str(item).strip()
            if text:
                return text
    return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _text_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    return text or None


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError:
        return str(value)


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None
