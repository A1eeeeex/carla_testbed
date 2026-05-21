#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from functools import lru_cache
import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Sequence

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import (
    _capability_direction_class,
    _capability_geometry_class,
    _capability_pair_coverage,
    _capability_supports_pairs,
    canonical_comparison_route_ids,
    canonical_comparison_set,
    capability_layer_summary,
    default_corpus_path,
    load_route_corpus,
    safe_bool,
    safe_float,
)


PROFILE_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep",
    "curve_lane_follow_candidate": "curve_lane_follow",
    "junction_traverse_candidate": "junction_traverse",
    "traffic_light_candidate": "traffic_light_actual",
}

FIRST_WAVE_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_first_wave_smoke",
    "curve_lane_follow_candidate": "curve_lane_follow_first_wave_smoke",
    "junction_traverse_candidate": "junction_traverse_first_wave_smoke",
    "traffic_light_candidate": "traffic_light_first_wave_smoke",
}

NEXT_REVIEW_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_next_review_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_next_review_queue",
    "junction_traverse_candidate": "junction_traverse_next_review_queue",
    "traffic_light_candidate": "traffic_light_next_review_queue",
}

CONTRAST_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_contrast_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_contrast_queue",
    "junction_traverse_candidate": "junction_traverse_contrast_queue",
    "traffic_light_candidate": "traffic_light_contrast_queue",
}

REVIEW_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_review_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_review_pack",
    "junction_traverse_candidate": "junction_traverse_review_pack",
    "traffic_light_candidate": "traffic_light_review_pack",
}

REVIEW_PRIORITY_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_review_priority_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_review_priority_queue",
    "junction_traverse_candidate": "junction_traverse_review_priority_queue",
    "traffic_light_candidate": "traffic_light_review_priority_queue",
}

FOCUS_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_focus_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_focus_pack",
    "junction_traverse_candidate": "junction_traverse_focus_pack",
    "traffic_light_candidate": "traffic_light_focus_pack",
}

PROXY_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_proxy_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_proxy_pack",
    "junction_traverse_candidate": "junction_traverse_proxy_pack",
    "traffic_light_candidate": "traffic_light_proxy_pack",
}

SEED_PACK_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_seed_pack",
    "curve_lane_follow_candidate": "curve_lane_follow_seed_pack",
    "junction_traverse_candidate": "junction_traverse_seed_pack",
    "traffic_light_candidate": "traffic_light_seed_pack",
}

HISTORY_GAP_FOR_SUBSET = {
    "lane_keep_candidate": "lane_keep_history_gap_queue",
    "curve_lane_follow_candidate": "curve_lane_follow_history_gap_queue",
    "junction_traverse_candidate": "junction_traverse_history_gap_queue",
    "traffic_light_candidate": "traffic_light_history_gap_queue",
}

PAIR_GAP_FOR_SUBSET = {
    "curve_lane_follow_candidate": "curve_lane_follow_pair_gap_queue",
    "junction_traverse_candidate": "junction_traverse_pair_gap_queue",
    "traffic_light_candidate": "traffic_light_pair_gap_queue",
}

RISK_TAGS = {
    "persistent_path_fallback_risk",
    "recoverable_fallback_pressure",
    "relapse_heavy_after_recovery",
    "repeat_instability_risk",
    "current_time_smaller_precursor_risk",
    "reference_line_provider_bridge_risk",
    "reroute_propagation_frame_bridge_risk",
}


def _startup_probe_attempt_rows(probe_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    attempts = probe_payload.get("attempts")
    if isinstance(attempts, list) and attempts:
        return [dict(row) for row in attempts if isinstance(row, dict)]
    if isinstance(probe_payload, dict):
        return [dict(probe_payload)]
    return []


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _nested_get(payload: Dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _csv_trueish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def _lane_road_id(lane_id: Any) -> str:
    text = str(lane_id or "").strip()
    if not text:
        return ""
    return text.split("_", 1)[0].strip()


def _transition_count(values: Sequence[str]) -> int:
    transitions = 0
    previous = ""
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        if previous and value != previous:
            transitions += 1
        previous = value
    return transitions


def _apollo_routing_preview_text(runtime_summary: Dict[str, Any]) -> str:
    first_road_count = int(runtime_summary.get("runtime_apollo_planning_first_routing_road_count") or 0)
    last_road_count = int(runtime_summary.get("runtime_apollo_planning_last_routing_road_count") or 0)
    max_road_count = int(runtime_summary.get("runtime_apollo_planning_max_routing_road_count") or 0)
    multi_road_frame_count = int(runtime_summary.get("runtime_apollo_planning_multi_road_frame_count") or 0)
    max_segment_count = int(runtime_summary.get("runtime_apollo_planning_max_routing_segment_count") or 0)
    if max_road_count <= 0 and max_segment_count <= 0:
        return ""
    parts: List[str] = []
    if max_road_count > 1:
        if first_road_count > 0 and last_road_count > 0 and first_road_count != last_road_count:
            parts.append(
                f"Apollo routing preview expanded from {first_road_count} to {last_road_count} roads"
            )
        elif max_road_count > 0:
            parts.append(f"Apollo routing preview spanned up to {max_road_count} roads")
        if multi_road_frame_count > 0:
            parts[-1] += f" across {multi_road_frame_count} planning frames"
    elif max_road_count == 1 and max_segment_count > 1:
        parts.append("Apollo routing preview stayed on a single road")
    if max_segment_count > 1:
        parts.append(f"the routing preview kept up to {max_segment_count} route segments ready")
    return ", and ".join(parts)


@lru_cache(maxsize=None)
def _startup_probe_summary(summary_path_value: str) -> Dict[str, Any]:
    summary = {
        "startup_probe_present": False,
        "startup_probe_attempt_count": 0,
        "startup_probe_first_world_ready_attempt": 0,
        "startup_probe_recovered_after_retry": False,
        "startup_probe_lineage_class": "probe_missing",
        "startup_probe_pre_world_ready_failure_families": "none",
        "startup_probe_final_status": "none",
        "startup_probe_final_failure_family": "none",
        "startup_probe_final_rpc_ready": False,
        "startup_probe_final_world_ready": False,
        "startup_probe_retry_policy_present": False,
        "startup_probe_retry_max_attempts": 0,
        "startup_probe_retry_delay_sec": 0.0,
        "startup_probe_retry_no_retry_families": "none",
        "startup_probe_final_retry_eligible": "",
        "startup_probe_final_retry_decision_reason": "none",
    }
    if not str(summary_path_value or "").strip():
        return summary
    summary_path = Path(summary_path_value).resolve()
    if not summary_path.exists():
        return summary
    probe_path = None
    for candidate_root in (
        summary_path.parent,
        summary_path.parent.parent,
        summary_path.parent.parent.parent,
    ):
        candidate_path = candidate_root / "carla_boot" / "carla_startup_probe.json"
        if candidate_path.exists():
            probe_path = candidate_path
            break
    if probe_path is None:
        return summary
    payload = _load_json(probe_path)
    rows = _startup_probe_attempt_rows(payload)
    if not rows:
        return summary
    retry_policy = payload.get("retry_policy") or {}
    if not isinstance(retry_policy, dict):
        retry_policy = {}
    no_retry_families = retry_policy.get("no_retry_failure_families") or []
    if not isinstance(no_retry_families, list):
        no_retry_families = [no_retry_families]
    final_row = rows[-1]
    first_world_ready_attempt = next(
        (int(safe_float(row.get("attempt")) or 0) for row in rows if bool(row.get("world_ready"))),
        0,
    )
    first_world_ready_index = next(
        (index for index, row in enumerate(rows) if bool(row.get("world_ready"))),
        -1,
    )
    pre_world_ready_rows = rows[:first_world_ready_index] if first_world_ready_index >= 0 else rows
    pre_world_ready_failure_families: List[str] = []
    for row in pre_world_ready_rows:
        failure_family = str(row.get("failure_family") or "").strip()
        if not failure_family or failure_family == "world_ready":
            continue
        if failure_family not in pre_world_ready_failure_families:
            pre_world_ready_failure_families.append(failure_family)
    lineage_class = "startup_failed"
    if first_world_ready_attempt == 1:
        lineage_class = "world_ready_direct"
    elif first_world_ready_attempt > 1:
        lineage_class = "world_ready_after_retry"
    summary.update(
        {
            "startup_probe_present": True,
            "startup_probe_attempt_count": len(rows),
            "startup_probe_first_world_ready_attempt": first_world_ready_attempt,
            "startup_probe_recovered_after_retry": bool(first_world_ready_attempt > 1),
            "startup_probe_lineage_class": lineage_class,
            "startup_probe_pre_world_ready_failure_families": ", ".join(pre_world_ready_failure_families)
            or "none",
            "startup_probe_final_status": str(final_row.get("status") or "").strip() or "none",
            "startup_probe_final_failure_family": str(final_row.get("failure_family") or "").strip() or "none",
            "startup_probe_final_rpc_ready": bool(final_row.get("rpc_ready")),
            "startup_probe_final_world_ready": bool(final_row.get("world_ready")),
            "startup_probe_retry_policy_present": bool(retry_policy),
            "startup_probe_retry_max_attempts": int(safe_float(retry_policy.get("max_attempts")) or 0),
            "startup_probe_retry_delay_sec": float(safe_float(retry_policy.get("retry_delay_sec")) or 0.0),
            "startup_probe_retry_no_retry_families": ", ".join(
                sorted(str(item).strip() for item in no_retry_families if str(item).strip())
            ) or "none",
            "startup_probe_final_retry_eligible": ""
            if final_row.get("retry_eligible") is None
            else str(bool(final_row.get("retry_eligible"))),
            "startup_probe_final_retry_decision_reason": str(
                final_row.get("retry_decision_reason") or ""
            ).strip()
            or "none",
        }
    )
    return summary


@lru_cache(maxsize=None)
def _runtime_signal_summary(summary_path_value: str) -> Dict[str, Any]:
    summary = {
        "runtime_total_frame_count": 0,
        "runtime_junction_frame_count": 0,
        "runtime_ref_junction_frame_count": 0,
        "runtime_target_junction_frame_count": 0,
        "runtime_external_control_frame_count": 0,
        "runtime_harness_target_wp_frame_count": 0,
        "runtime_harness_junction_flag_reliability": "unknown",
        "runtime_commanded_steer_intent_frame_count": 0,
        "runtime_measured_steer_nonzero_frame_count": 0,
        "runtime_force_zero_steer_frame_count": 0,
        "runtime_brake_frame_count": 0,
        "runtime_terminal_stop_hold_frame_count": 0,
        "runtime_terminal_stop_hold_engaged_count": 0,
        "runtime_apollo_route_segment_ready_frame_count": 0,
        "runtime_apollo_route_segment_multi_frame_count": 0,
        "runtime_apollo_current_lane_ids": "none",
        "runtime_apollo_current_lane_transition_count": 0,
        "runtime_apollo_current_lane_road_transition_count": 0,
        "runtime_apollo_planning_lane_ids": "none",
        "runtime_apollo_planning_target_lane_ids": "none",
        "runtime_apollo_lane_transition_count": 0,
        "runtime_apollo_target_lane_transition_count": 0,
        "runtime_apollo_lane_road_transition_count": 0,
        "runtime_apollo_target_lane_road_transition_count": 0,
        "runtime_apollo_planning_first_routing_road_count": 0,
        "runtime_apollo_planning_last_routing_road_count": 0,
        "runtime_apollo_planning_max_routing_road_count": 0,
        "runtime_apollo_planning_multi_road_frame_count": 0,
        "runtime_apollo_planning_first_routing_segment_count": 0,
        "runtime_apollo_planning_last_routing_segment_count": 0,
        "runtime_apollo_planning_max_routing_segment_count": 0,
        "runtime_traffic_light_policy": "",
        "runtime_traffic_light_force_green_publish_count": 0,
    }
    if not str(summary_path_value or "").strip():
        return summary
    summary_path = Path(summary_path_value).resolve()
    if not summary_path.exists():
        return summary

    timeseries_path = summary_path.with_name("timeseries.csv")
    if timeseries_path.exists():
        current_lane_ids: set[str] = set()
        with timeseries_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                summary["runtime_total_frame_count"] += 1
                ref_junction = _csv_trueish(row.get("dbg_ref_wp_is_junction"))
                target_junction = _csv_trueish(row.get("dbg_target_wp_is_junction"))
                if ref_junction:
                    summary["runtime_ref_junction_frame_count"] += 1
                if target_junction:
                    summary["runtime_target_junction_frame_count"] += 1
                if str(row.get("control_source") or "").strip() == "external_stack":
                    summary["runtime_external_control_frame_count"] += 1
                target_wp_lane = str(row.get("dbg_target_wp_lane_id") or "").strip()
                target_wp_road = str(row.get("dbg_target_wp_road_id") or "").strip()
                target_wp_section = str(row.get("dbg_target_wp_section_id") or "").strip()
                if target_wp_lane or target_wp_road or target_wp_section:
                    summary["runtime_harness_target_wp_frame_count"] += 1
        summary["runtime_junction_frame_count"] = (
            summary["runtime_ref_junction_frame_count"] + summary["runtime_target_junction_frame_count"]
        )
        if summary["runtime_total_frame_count"] > 0:
            if summary["runtime_external_control_frame_count"] > 0 and summary["runtime_harness_target_wp_frame_count"] <= 0:
                summary["runtime_harness_junction_flag_reliability"] = "harness_external_stack_targetless_unreliable"
            elif summary["runtime_junction_frame_count"] > 0 or summary["runtime_harness_target_wp_frame_count"] > 0:
                summary["runtime_harness_junction_flag_reliability"] = "usable"
            else:
                summary["runtime_harness_junction_flag_reliability"] = "absent"

    debug_timeseries_path = summary_path.parent / "artifacts" / "debug_timeseries.csv"
    observed_policies: List[str] = []
    if debug_timeseries_path.exists():
        with debug_timeseries_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                commanded_steer_pre_guard = abs(
                    safe_float(row.get("commanded_steer_pre_lateral_guards")) or 0.0
                )
                measured_steer = abs(safe_float(row.get("measured_steer")) or 0.0)
                measured_brake = safe_float(row.get("measured_brake")) or 0.0
                if commanded_steer_pre_guard > 1e-4:
                    summary["runtime_commanded_steer_intent_frame_count"] += 1
                if measured_steer > 1e-4:
                    summary["runtime_measured_steer_nonzero_frame_count"] += 1
                if _csv_trueish(row.get("force_zero_steer_applied")):
                    summary["runtime_force_zero_steer_frame_count"] += 1
                if measured_brake > 0.05:
                    summary["runtime_brake_frame_count"] += 1
                if _csv_trueish(row.get("terminal_stop_hold_active")):
                    summary["runtime_terminal_stop_hold_frame_count"] += 1
                traffic_light_policy = str(row.get("traffic_light_policy") or "").strip()
                if traffic_light_policy:
                    observed_policies.append(traffic_light_policy)
    if observed_policies:
        summary["runtime_traffic_light_policy"] = sorted(set(observed_policies))[0]

    route_segment_debug_path = summary_path.parent / "artifacts" / "stage5_apollo_reference_line_debug.jsonl"
    if route_segment_debug_path.exists():
        current_lane_ids: set[str] = set()
        current_lane_sequence: List[str] = []
        current_lane_road_sequence: List[str] = []
        with route_segment_debug_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                route_segment_count = int(safe_float(row.get("route_segment_count")) or 0)
                if route_segment_count > 0:
                    summary["runtime_apollo_route_segment_ready_frame_count"] += 1
                if route_segment_count > 1:
                    summary["runtime_apollo_route_segment_multi_frame_count"] += 1
                current_lane_id = str(row.get("current_lane_id") or "").strip()
                if current_lane_id:
                    current_lane_ids.add(current_lane_id)
                    current_lane_sequence.append(current_lane_id)
                    current_lane_road_id = _lane_road_id(current_lane_id)
                    if current_lane_road_id:
                        current_lane_road_sequence.append(current_lane_road_id)
        if current_lane_ids:
            summary["runtime_apollo_current_lane_ids"] = ", ".join(sorted(current_lane_ids))
        summary["runtime_apollo_current_lane_transition_count"] = _transition_count(current_lane_sequence)
        summary["runtime_apollo_current_lane_road_transition_count"] = _transition_count(
            current_lane_road_sequence
        )

    planning_topic_debug_path = summary_path.parent / "artifacts" / "planning_topic_debug.jsonl"
    if planning_topic_debug_path.exists():
        planning_lane_ids: set[str] = set()
        planning_target_lane_ids: set[str] = set()
        planning_lane_sequence: List[str] = []
        planning_target_lane_sequence: List[str] = []
        planning_lane_road_sequence: List[str] = []
        planning_target_lane_road_sequence: List[str] = []
        with planning_topic_debug_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                lane_id = str(row.get("lane_id_first") or "").strip()
                target_lane_id = str(row.get("target_lane_id_first") or "").strip()
                if lane_id:
                    planning_lane_ids.add(lane_id)
                    planning_lane_sequence.append(lane_id)
                    road_id = _lane_road_id(lane_id)
                    if road_id:
                        planning_lane_road_sequence.append(road_id)
                if target_lane_id:
                    planning_target_lane_ids.add(target_lane_id)
                    planning_target_lane_sequence.append(target_lane_id)
                    target_road_id = _lane_road_id(target_lane_id)
                    if target_road_id:
                        planning_target_lane_road_sequence.append(target_road_id)
                routing_road_count = int(safe_float(row.get("routing_road_count")) or 0)
                if routing_road_count > 0:
                    if summary["runtime_apollo_planning_first_routing_road_count"] <= 0:
                        summary["runtime_apollo_planning_first_routing_road_count"] = routing_road_count
                    summary["runtime_apollo_planning_last_routing_road_count"] = routing_road_count
                    summary["runtime_apollo_planning_max_routing_road_count"] = max(
                        summary["runtime_apollo_planning_max_routing_road_count"],
                        routing_road_count,
                    )
                    if routing_road_count > 1:
                        summary["runtime_apollo_planning_multi_road_frame_count"] += 1
                routing_segment_count = int(safe_float(row.get("routing_segment_count")) or 0)
                if routing_segment_count > 0:
                    if summary["runtime_apollo_planning_first_routing_segment_count"] <= 0:
                        summary["runtime_apollo_planning_first_routing_segment_count"] = routing_segment_count
                    summary["runtime_apollo_planning_last_routing_segment_count"] = routing_segment_count
                    summary["runtime_apollo_planning_max_routing_segment_count"] = max(
                        summary["runtime_apollo_planning_max_routing_segment_count"],
                        routing_segment_count,
                    )
        if planning_lane_ids:
            summary["runtime_apollo_planning_lane_ids"] = ", ".join(sorted(planning_lane_ids))
        if planning_target_lane_ids:
            summary["runtime_apollo_planning_target_lane_ids"] = ", ".join(sorted(planning_target_lane_ids))
        summary["runtime_apollo_lane_transition_count"] = _transition_count(planning_lane_sequence)
        summary["runtime_apollo_target_lane_transition_count"] = _transition_count(planning_target_lane_sequence)
        summary["runtime_apollo_lane_road_transition_count"] = _transition_count(
            planning_lane_road_sequence
        )
        summary["runtime_apollo_target_lane_road_transition_count"] = _transition_count(
            planning_target_lane_road_sequence
        )

    bridge_summary_path = summary_path.parent / "artifacts" / "bridge_health_summary.finalized.json"
    if bridge_summary_path.exists():
        bridge_summary = _load_json(bridge_summary_path)
        bridge_policy = str(bridge_summary.get("traffic_light_policy") or "").strip()
        if bridge_policy and not summary["runtime_traffic_light_policy"]:
            summary["runtime_traffic_light_policy"] = bridge_policy
        summary["runtime_terminal_stop_hold_engaged_count"] = int(
            safe_float(bridge_summary.get("terminal_stop_hold_engaged_count")) or 0
        )
        summary["runtime_traffic_light_force_green_publish_count"] = int(
            safe_float(bridge_summary.get("traffic_light_force_green_publish_count")) or 0
        )
    return summary


def _semantic_runtime_assessment(
    profile_name: str,
    best_history: Dict[str, Any],
    runtime_summary: Dict[str, Any],
) -> Dict[str, str]:
    if not best_history:
        return {
            "semantic_observability": "none",
            "semantic_evidence_level": "no_semantic_history",
            "semantic_pass_confidence": "unknown",
            "semantic_note": "No semantic history row has been captured for this capability.",
        }

    label = str(best_history.get("route_health_label") or "").strip()
    if profile_name == "lane_keep":
        if label == "route_health_pass":
            return {
                "semantic_observability": "direct_runtime",
                "semantic_evidence_level": "lane_follow_pass_supported",
                "semantic_pass_confidence": "supported",
                "semantic_note": "Lane-follow pass anchor exists; completion and route-health both support lane keeping.",
            }
        if label:
            return {
                "semantic_observability": "direct_runtime",
                "semantic_evidence_level": "lane_follow_behavior_observed",
                "semantic_pass_confidence": "partial",
                "semantic_note": "Lane-follow behavior was observed, but the best semantic row is still below pass.",
            }

    if profile_name == "curve_lane_follow":
        if runtime_summary["runtime_measured_steer_nonzero_frame_count"] > 0:
            observability = "direct_runtime"
            note = "Runtime captured non-zero measured steering during the curve capability run."
        elif runtime_summary["runtime_commanded_steer_intent_frame_count"] > 0:
            observability = "indirect_runtime"
            note = "Curve route executed and steering intent was present, but measured steering stayed near zero."
        else:
            observability = "route_only"
            note = "Curve route executed, but runtime logs did not expose clear turning behavior."
        if label == "route_health_pass":
            evidence = "curve_pass_supported"
            confidence = "supported"
        elif label:
            evidence = "curve_behavior_observed_but_unhealthy"
            confidence = "not_proven"
        else:
            evidence = "curve_route_seen_without_review"
            confidence = "not_proven"
        return {
            "semantic_observability": observability,
            "semantic_evidence_level": evidence,
            "semantic_pass_confidence": confidence,
            "semantic_note": note,
        }

    if profile_name == "junction_traverse":
        direct_runtime_mode = ""
        apollo_transition_sources: List[str] = []
        routing_preview_text = _apollo_routing_preview_text(runtime_summary)
        if max(
            runtime_summary["runtime_apollo_lane_road_transition_count"],
            runtime_summary["runtime_apollo_target_lane_road_transition_count"],
        ) > 0:
            apollo_transition_sources.append("planning lane ids")
        if runtime_summary["runtime_apollo_current_lane_road_transition_count"] > 0:
            apollo_transition_sources.append("reference-line current lane ids")
        if runtime_summary["runtime_junction_frame_count"] > 0:
            observability = "direct_runtime"
            note = "Junction flags were asserted in runtime timeseries, so junction traversal was directly observed."
            direct_runtime_mode = "explicit_junction_flags"
        elif (
            runtime_summary["runtime_external_control_frame_count"] > 0
            and runtime_summary["runtime_harness_junction_flag_reliability"] == "harness_external_stack_targetless_unreliable"
            and runtime_summary["runtime_apollo_route_segment_multi_frame_count"] > 0
            and runtime_summary["runtime_measured_steer_nonzero_frame_count"] > 0
            and apollo_transition_sources
        ):
            observability = "direct_runtime"
            source_text = " and ".join(apollo_transition_sources)
            note = (
                f"Apollo {source_text} transitioned across distinct roads during the finalized runtime, "
                "and the run also retained stable route segments with non-zero measured steering even though "
                "the harness-side junction flags were absent under external-stack control."
            )
            if routing_preview_text:
                note = note[:-1] + f"; {routing_preview_text}."
            direct_runtime_mode = "apollo_lane_transition"
        elif (
            runtime_summary["runtime_external_control_frame_count"] > 0
            and runtime_summary["runtime_harness_junction_flag_reliability"] == "harness_external_stack_targetless_unreliable"
            and runtime_summary["runtime_apollo_route_segment_multi_frame_count"] > 0
            and runtime_summary["runtime_measured_steer_nonzero_frame_count"] > 0
        ):
            observability = "indirect_runtime"
            if routing_preview_text:
                note = (
                    "External-stack runtime showed non-zero measured steering and stable Apollo route segments, "
                    f"and {routing_preview_text}, but current/planning lane ids never transitioned across roads; "
                    "the harness-side junction flags are not reliable because no target waypoint was exposed."
                )
            else:
                note = (
                    "External-stack runtime showed non-zero measured steering and stable Apollo route segments, "
                    "but the harness-side junction flags are not reliable because no target waypoint was exposed."
                )
        else:
            observability = "route_only"
            note = "Junction-capability config ran, but runtime junction flags never asserted in timeseries."
        if label == "route_health_pass" and direct_runtime_mode == "explicit_junction_flags":
            evidence = "junction_pass_supported"
            confidence = "supported"
        elif label and direct_runtime_mode == "apollo_lane_transition":
            evidence = "junction_behavior_supported_via_apollo_lane_transition"
            confidence = "supported"
        elif label and direct_runtime_mode == "explicit_junction_flags":
            evidence = "junction_behavior_directly_observed"
            confidence = "supported"
        elif label and observability == "indirect_runtime":
            evidence = "junction_behavior_supported_via_external_runtime"
            confidence = "partial"
        elif label:
            evidence = "route_progress_without_junction_proof"
            confidence = "not_proven"
        else:
            evidence = "junction_route_seen_without_review"
            confidence = "not_proven"
        return {
            "semantic_observability": observability,
            "semantic_evidence_level": evidence,
            "semantic_pass_confidence": confidence,
            "semantic_note": note,
        }

    if profile_name == "traffic_light_actual":
        policy = runtime_summary["runtime_traffic_light_policy"]
        if policy == "carla_actual":
            observability = "policy_runtime"
            note = "Runtime confirmed `carla_actual` traffic-light policy, but direct light-state interaction is not logged yet."
        else:
            observability = "route_only"
            note = "Traffic-light capability route ran, but runtime policy confirmation is missing."
        if label == "route_health_pass" and policy == "carla_actual":
            evidence = "traffic_policy_pass_candidate"
            confidence = "partial"
        elif label and policy == "carla_actual":
            evidence = "traffic_policy_observed_candidate"
            confidence = "not_proven"
        elif label:
            evidence = "traffic_route_seen_without_policy_proof"
            confidence = "not_proven"
        else:
            evidence = "traffic_route_seen_without_review"
            confidence = "not_proven"
        return {
            "semantic_observability": observability,
            "semantic_evidence_level": evidence,
            "semantic_pass_confidence": confidence,
            "semantic_note": note,
        }

    return {
        "semantic_observability": "none",
        "semantic_evidence_level": "unknown",
        "semantic_pass_confidence": "unknown",
        "semantic_note": "Unsupported capability profile.",
    }


def _historical_semantic_match(candidate_subset: str, effective_cfg: Dict[str, Any]) -> bool:
    lane_follow_only = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "planning", "lane_follow_only_scenario"))
    disable_tl_rule = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "planning", "disable_traffic_light_rule"))
    traffic_light_policy = str(_nested_get(effective_cfg, "algo", "apollo", "traffic_light", "policy") or "").strip()
    traffic_light_freeze = safe_bool(_nested_get(effective_cfg, "scenario", "traffic_lights", "freeze"))
    spawn_reject_junction = safe_bool(_nested_get(effective_cfg, "scenario", "route_health", "spawn_reject_junction"))
    goal_reject_junction = safe_bool(_nested_get(effective_cfg, "scenario", "route_health", "goal_reject_junction"))
    if candidate_subset in {"lane_keep_candidate", "curve_lane_follow_candidate"}:
        return lane_follow_only is True and disable_tl_rule is True
    if candidate_subset == "junction_traverse_candidate":
        return (
            lane_follow_only is False
            and disable_tl_rule is True
            and spawn_reject_junction is False
            and goal_reject_junction is False
        )
    if candidate_subset == "traffic_light_candidate":
        return (
            lane_follow_only is False
            and disable_tl_rule is False
            and traffic_light_policy == "carla_actual"
            and traffic_light_freeze is False
        )
    return False


def _load_historical_summary_rows(runs_root: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not runs_root.exists():
        return rows
    for summary_path in sorted(runs_root.rglob("summary.json")):
        summary = _load_json(summary_path)
        route_id = str(summary.get("route_id") or "").strip()
        if not route_id:
            continue
        effective_path = summary_path.with_name("effective.yaml")
        effective_cfg = _load_yaml(effective_path) if effective_path.exists() else {}
        row: Dict[str, Any] = {
            "route_id": route_id,
            "comparison_label": str(summary.get("comparison_label") or "").strip(),
            "route_health_label": str(summary.get("route_health_label") or "").strip(),
            "route_completion_ratio": safe_float(summary.get("route_completion_ratio")) or 0.0,
            "route_distance_achieved_m": safe_float(summary.get("route_distance_achieved_m")) or 0.0,
            "runtime_contract_status": str(_nested_get(summary, "runtime_contract", "status") or "").strip(),
            "control_handoff_status": str(summary.get("control_handoff_status") or "").strip(),
            "persistent_path_fallback_at_end": bool(summary.get("persistent_path_fallback_at_end")),
            "semantic_window_anchor_kind": str(summary.get("semantic_window_anchor_kind") or "").strip(),
            "summary_path": str(summary_path),
            "effective_path": str(effective_path) if effective_path.exists() else "",
        }
        for candidate_subset in PROFILE_FOR_SUBSET:
            row[f"semantic::{candidate_subset}"] = _historical_semantic_match(candidate_subset, effective_cfg)
        rows.append(row)
    return rows


def _rows_by_route(rows: Sequence[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        route_id = str(row.get("route_id") or "").strip()
        if not route_id:
            continue
        grouped.setdefault(route_id, []).append(row)
    return grouped


def _comparison_row_score(row: Dict[str, str]) -> tuple[int, float, float, str]:
    label = str(row.get("route_health_label") or "").strip()
    label_rank = 3
    if label == "route_health_pass":
        label_rank = 0
    elif label == "route_health_candidate":
        label_rank = 1
    elif label:
        label_rank = 2
    return (
        label_rank,
        -(safe_float(row.get("route_completion_ratio")) or 0.0),
        -(safe_float(row.get("route_distance_achieved_m")) or 0.0),
        str(row.get("comparison_label") or ""),
    )


def _best_row(rows: Sequence[Dict[str, str]]) -> Dict[str, str]:
    if not rows:
        return {}
    return sorted(rows, key=_comparison_row_score)[0]


def _best_history_row(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return sorted(
        rows,
        key=lambda row: (
            0 if str(row.get("route_health_label") or "").strip() == "route_health_pass" else
            1 if str(row.get("route_health_label") or "").strip() == "route_health_candidate" else
            2 if str(row.get("route_health_label") or "").strip() else 3,
            -(safe_float(row.get("route_completion_ratio")) or 0.0),
            -(safe_float(row.get("route_distance_achieved_m")) or 0.0),
            str(row.get("comparison_label") or ""),
        ),
    )[0]


def _current_runtime_sort_key(profile_name: str, row: Dict[str, Any]) -> tuple[int, int, int, int, int, float, float]:
    runtime_contract_status = str(row.get("runtime_contract_status") or "").strip()
    control_handoff_status = str(row.get("control_handoff_status") or "").strip()
    semantic_window_anchor_kind = str(row.get("semantic_window_anchor_kind") or "").strip()
    persistent_path_fallback_at_end = bool(row.get("persistent_path_fallback_at_end"))
    label = str(row.get("route_health_label") or "").strip()

    if runtime_contract_status == "aligned":
        runtime_rank = 0
    elif runtime_contract_status:
        runtime_rank = 1
    else:
        runtime_rank = 2

    if control_handoff_status == "control_consuming_with_nonzero_planning":
        handoff_rank = 0
    elif control_handoff_status:
        handoff_rank = 1
    else:
        handoff_rank = 2

    if profile_name == "curve_lane_follow":
        anchor_rank = 0 if semantic_window_anchor_kind and semantic_window_anchor_kind != "none" else 1
    else:
        anchor_rank = 0

    if label == "route_health_pass":
        label_rank = 0
    elif label == "route_health_candidate":
        label_rank = 1
    elif label:
        label_rank = 2
    else:
        label_rank = 3

    fallback_rank = 1 if persistent_path_fallback_at_end else 0
    return (
        runtime_rank,
        handoff_rank,
        anchor_rank,
        fallback_rank,
        label_rank,
        -(safe_float(row.get("route_distance_achieved_m")) or 0.0),
        -(safe_float(row.get("route_completion_ratio")) or 0.0),
    )


def _best_current_runtime_row(profile_name: str, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return sorted(rows, key=lambda row: _current_runtime_sort_key(profile_name, row))[0]


def _semantic_history_sort_key(profile_name: str, row: Dict[str, Any]) -> tuple[int, int, int, float, float, str]:
    runtime_summary = _runtime_signal_summary(str(row.get("summary_path") or ""))
    semantic = _semantic_runtime_assessment(profile_name, row, runtime_summary)
    evidence = str(semantic.get("semantic_evidence_level") or "").strip()
    confidence = str(semantic.get("semantic_pass_confidence") or "").strip()
    observability = str(semantic.get("semantic_observability") or "").strip()
    if "pass_supported" in evidence:
        evidence_rank = 0
    elif confidence == "supported":
        evidence_rank = 1
    elif confidence == "partial":
        evidence_rank = 2
    elif confidence == "not_proven":
        evidence_rank = 3
    else:
        evidence_rank = 4
    if observability == "direct_runtime":
        observability_rank = 0
    elif observability in {"indirect_runtime", "policy_runtime"}:
        observability_rank = 1
    elif observability == "route_only":
        observability_rank = 2
    else:
        observability_rank = 3
    label = str(row.get("route_health_label") or "").strip()
    if label == "route_health_pass":
        label_rank = 0
    elif label == "route_health_candidate":
        label_rank = 1
    elif label:
        label_rank = 2
    else:
        label_rank = 3
    return (
        evidence_rank,
        observability_rank,
        label_rank,
        -(safe_float(row.get("route_completion_ratio")) or 0.0),
        -(safe_float(row.get("route_distance_achieved_m")) or 0.0),
        str(row.get("comparison_label") or ""),
    )


def _best_semantic_history_row(profile_name: str, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return sorted(rows, key=lambda row: _semantic_history_sort_key(profile_name, row))[0]


def _historical_row_status(row: Dict[str, Any]) -> str:
    label = str(row.get("route_health_label") or "").strip()
    if label == "route_health_pass":
        return "pass"
    if label == "route_health_candidate":
        return "candidate"
    if label:
        return "reviewed"
    return "unreviewed"


def _history_readiness(
    *,
    semantic_run_count: int,
    semantic_pass_count: int,
    semantic_candidate_count: int,
    semantic_reviewed_count: int,
) -> str:
    if semantic_run_count <= 0:
        return "no_semantic_history"
    if semantic_pass_count > 0:
        return "semantic_history_with_pass"
    if (semantic_candidate_count + semantic_reviewed_count) > 0:
        return "semantic_history_partial_reviewed"
    return "semantic_history_unreviewed"


def _best_rows_by_route(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_route: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        route_id = str(row.get("route_id") or "").strip()
        if not route_id:
            continue
        by_route.setdefault(route_id, []).append(row)
    return [_best_history_row(route_rows) for route_rows in by_route.values() if route_rows]


def route_status(route: Dict[str, Any]) -> str:
    tags = {str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip()}
    if tags.intersection(RISK_TAGS):
        return "risky"
    if "route_health_pass" in tags:
        return "pass"
    if "route_health_candidate" in tags or "guarded_lateral_runtime_ok" in tags:
        return "candidate"
    if "empirically_reviewed" in tags:
        return "reviewed"
    return "unreviewed"


def _summary_for_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    summary_path_value = str(row.get("summary_path") or "").strip()
    if not summary_path_value:
        return {}
    return _load_json(Path(summary_path_value).resolve())


def _best_semantic_history_row_for_subset(
    profile_name: str,
    candidate_subset: str,
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    semantic_rows = [row for row in rows if bool(row.get(f"semantic::{candidate_subset}"))]
    return _best_semantic_history_row(profile_name, semantic_rows or rows)


def build_canonical_comparison_rows(
    corpus: Dict[str, Any],
    comparison_rows: Sequence[Dict[str, str]],
    historical_summary_rows: Sequence[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    routes_by_id = {
        str(route.get("route_id") or "").strip(): route
        for route in list(corpus.get("routes") or [])
        if isinstance(route, dict) and str(route.get("route_id") or "").strip()
    }
    grouped_rows = _rows_by_route(comparison_rows)
    historical_by_route: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(historical_summary_rows or []):
        route_id = str(row.get("route_id") or "").strip()
        if route_id:
            historical_by_route.setdefault(route_id, []).append(row)

    results: List[Dict[str, Any]] = []
    for entry in canonical_comparison_set():
        route_id = entry["route_id"]
        profile_name = entry["capability_profile"]
        candidate_subset = entry["candidate_subset"]
        route = routes_by_id.get(route_id) or {}
        comparison_best = _best_row(grouped_rows.get(route_id) or [])
        current_runtime = _best_current_runtime_row(profile_name, historical_by_route.get(route_id) or [])
        history_best = _best_semantic_history_row_for_subset(
            profile_name,
            candidate_subset,
            historical_by_route.get(route_id) or [],
        )
        current_anchor = current_runtime or history_best
        summary = _summary_for_history_row(current_anchor)
        runtime_summary = _runtime_signal_summary(str(current_anchor.get("summary_path") or ""))
        semantic = _semantic_runtime_assessment(profile_name, current_anchor, runtime_summary)
        layers = capability_layer_summary(
            route_health_label=summary.get("route_health_label") or current_anchor.get("route_health_label"),
            runtime_contract_status=_nested_get(summary, "runtime_contract", "status"),
            control_handoff_status=summary.get("control_handoff_status"),
        )
        current_best_label = str(comparison_best.get("route_health_label") or "").strip()
        current_best_completion = safe_float(comparison_best.get("route_completion_ratio"))
        current_best_distance_m = safe_float(comparison_best.get("route_distance_achieved_m"))
        current_best_comparison_label = str(comparison_best.get("comparison_label") or "").strip()
        if not current_best_label:
            current_best_label = str(current_anchor.get("route_health_label") or "").strip() or "none"
            current_best_completion = safe_float(current_anchor.get("route_completion_ratio")) or 0.0
            current_best_distance_m = safe_float(current_anchor.get("route_distance_achieved_m")) or 0.0
            current_best_comparison_label = str(current_anchor.get("comparison_label") or "").strip() or "none"
        results.append(
            {
                "comparison_key": entry["comparison_key"],
                "display_name": entry["display_name"],
                "role": entry["role"],
                "capability_profile": profile_name,
                "candidate_subset": candidate_subset,
                "route_id": route_id,
                "geometry_class": _capability_geometry_class(route, candidate_subset),
                "direction_class": _capability_direction_class(route, candidate_subset),
                "corpus_status": route_status(route),
                "current_best_label": current_best_label,
                "current_best_completion": current_best_completion or 0.0,
                "current_best_distance_m": current_best_distance_m or 0.0,
                "current_best_comparison_label": current_best_comparison_label or "none",
                "history_best_label": str(history_best.get("route_health_label") or "").strip() or "none",
                "history_best_completion": safe_float(history_best.get("route_completion_ratio")) or 0.0,
                "history_best_distance_m": safe_float(history_best.get("route_distance_achieved_m")) or 0.0,
                "history_best_comparison_label": str(history_best.get("comparison_label") or "").strip() or "none",
                "runtime_contract_status": str(_nested_get(summary, "runtime_contract", "status") or "").strip() or "unknown",
                "control_handoff_status": str(summary.get("control_handoff_status") or "").strip() or "unknown",
                "engineering_gate": layers["engineering_gate"],
                "capability_evidence": layers["capability_evidence"],
                "promotion_candidate": layers["promotion_candidate"],
                "semantic_observability": str(semantic.get("semantic_observability") or ""),
                "semantic_evidence_level": str(semantic.get("semantic_evidence_level") or ""),
                "semantic_pass_confidence": str(semantic.get("semantic_pass_confidence") or ""),
                "semantic_note": str(semantic.get("semantic_note") or ""),
                "summary_path": str(current_anchor.get("summary_path") or ""),
                "persistent_path_fallback_at_end": bool(summary.get("persistent_path_fallback_at_end")),
                "semantic_window_anchor_kind": str(summary.get("semantic_window_anchor_kind") or "").strip() or "none",
                "first_high_steer_seq": int(safe_float(summary.get("first_high_steer_seq")) or 0),
                "first_matched_point_too_large_seq": int(
                    safe_float(summary.get("first_matched_point_too_large_seq")) or 0
                ),
                "simple_lat_lateral_error_abs_p95_before_anchor": safe_float(
                    summary.get("simple_lat_lateral_error_abs_p95_before_anchor")
                ),
                "simple_lat_heading_error_abs_p95_before_anchor": safe_float(
                    summary.get("simple_lat_heading_error_abs_p95_before_anchor")
                ),
                "target_point_kappa_abs_p95_before_anchor": safe_float(
                    summary.get("target_point_kappa_abs_p95_before_anchor")
                ),
                "mapped_carla_steer_cmd_abs_p95_before_anchor": safe_float(
                    summary.get("mapped_carla_steer_cmd_abs_p95_before_anchor")
                ),
                "lateral_guard_apply_count": int(safe_float(summary.get("lateral_guard_apply_count")) or 0),
                "trajectory_contract_lateral_guard_apply_count": int(
                    safe_float(summary.get("trajectory_contract_lateral_guard_apply_count")) or 0
                ),
                "bridge_policy_source": str(
                    _nested_get(summary, "bridge_policy", "source") or "none"
                ),
            }
        )
    return results


def _curve_family_interpretation(rows: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    curve_rows = {
        str(row.get("comparison_key") or ""): row
        for row in rows
        if str(row.get("capability_profile") or "") == "curve_lane_follow"
    }
    row_217 = curve_rows.get("curve217") or {}
    row_213 = curve_rows.get("curve213") or {}
    if not row_217 or not row_213:
        return {
            "classification": "insufficient_curve_evidence",
            "bridge_vs_apollo": "unknown",
            "note": "One or both canonical curve samples are missing from the report.",
        }
    persistent_both = bool(row_217.get("persistent_path_fallback_at_end")) and bool(
        row_213.get("persistent_path_fallback_at_end")
    )
    low_guard_pressure = (
        int(row_217.get("lateral_guard_apply_count") or 0) <= 0
        and int(row_217.get("trajectory_contract_lateral_guard_apply_count") or 0) <= 0
        and int(row_213.get("lateral_guard_apply_count") or 0) <= 0
        and int(row_213.get("trajectory_contract_lateral_guard_apply_count") or 0) <= 0
    )
    if (
        str(row_217.get("semantic_window_anchor_kind") or "") == "first_high_steer"
        and int(row_217.get("first_matched_point_too_large_seq") or 0) <= 0
        and int(row_213.get("first_matched_point_too_large_seq") or 0) > 0
    ):
        classification = "shared_family_different_onset"
        note = (
            "curve217 still looks like a high-steer onset sample, while curve213 shows the explicit "
            "matched-point / target-point degradation chain before persistent fallback."
        )
    elif persistent_both:
        classification = "shared_family_same_onset"
        note = (
            "Both canonical curve samples land in persistent fallback under aligned runtime, "
            "and the dominant evidence points to the same Apollo lateral-semantics family."
        )
    else:
        classification = "curve_family_not_yet_settled"
        note = "The canonical curve pair still does not expose a stable shared failure family."
    if low_guard_pressure and classification in {"shared_family_different_onset", "shared_family_same_onset"}:
        bridge_vs_apollo = "apollo_lateral_semantics_primary"
    elif persistent_both:
        bridge_vs_apollo = "mixed_signal_needs_more_guard_review"
    else:
        bridge_vs_apollo = "insufficient_runtime_alignment"
    return {
        "classification": classification,
        "bridge_vs_apollo": bridge_vs_apollo,
        "note": note,
    }


def _shape_key(geometry_class: str, direction_class: str) -> str:
    return f"{geometry_class}:{direction_class}"


def _review_pack_readiness(
    *,
    supports_pairs: bool,
    review_pack_count: int,
    review_pack_pass_count: int,
    review_pack_candidate_count: int,
    review_pack_reviewed_count: int,
    missing_review_pack_pair_coverage: Sequence[str],
) -> str:
    if review_pack_count <= 0:
        return "missing"
    if supports_pairs and list(missing_review_pack_pair_coverage):
        return "pair_incomplete"
    if review_pack_pass_count > 0:
        return "pair_complete_with_pass_anchor" if supports_pairs else "pass_anchor_present"
    if (review_pack_candidate_count + review_pack_reviewed_count) > 0:
        return "pair_complete_partial_reviewed" if supports_pairs else "partial_reviewed"
    return "pair_complete_unreviewed" if supports_pairs else "unreviewed_only"


def _focus_pack_readiness(
    *,
    supports_pairs: bool,
    focus_pack_count: int,
    focus_pack_pass_count: int,
    focus_pack_candidate_count: int,
    focus_pack_reviewed_count: int,
    focus_pack_pair_coverage: Sequence[str],
) -> str:
    if focus_pack_count <= 0:
        return "missing"
    if supports_pairs:
        return "minimal_pair_complete" if list(focus_pack_pair_coverage) else "pair_fragment"
    if focus_pack_pass_count > 0:
        return "pass_anchor_present"
    if (focus_pack_candidate_count + focus_pack_reviewed_count) > 0:
        return "evidence_first"
    return "unreviewed_only"


def _proxy_pack_readiness(
    *,
    supports_pairs: bool,
    proxy_pack_count: int,
    proxy_pack_pass_count: int,
    proxy_pack_candidate_count: int,
    proxy_pack_reviewed_count: int,
    proxy_pack_pair_coverage: Sequence[str],
) -> str:
    if proxy_pack_count <= 0:
        return "missing"
    if supports_pairs:
        if list(proxy_pack_pair_coverage):
            if proxy_pack_pass_count > 0:
                return "pair_complete_with_pass_anchor"
            if (proxy_pack_candidate_count + proxy_pack_reviewed_count) > 0:
                return "pair_complete_with_proxy_evidence"
            return "pair_complete_unreviewed"
        return "pair_fragment"
    if proxy_pack_pass_count > 0:
        return "pass_anchor_present"
    if (proxy_pack_candidate_count + proxy_pack_reviewed_count) > 0:
        return "evidence_proxy_present"
    return "unreviewed_only"


def _seed_pack_readiness(
    *,
    seed_pack_count: int,
    seed_pack_pass_count: int,
    seed_pack_candidate_count: int,
    seed_pack_reviewed_count: int,
) -> str:
    if seed_pack_count <= 0:
        return "missing"
    if seed_pack_pass_count > 0:
        return "pass_anchor_present"
    if (seed_pack_candidate_count + seed_pack_reviewed_count) > 0:
        return "evidence_seed_present"
    return "unreviewed_only"


def build_capability_progress_rows(
    corpus: Dict[str, Any],
    comparison_rows: Sequence[Dict[str, str]],
    historical_summary_rows: Sequence[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    routes_by_id = {
        str(route.get("route_id") or "").strip(): route
        for route in list(corpus.get("routes") or [])
        if isinstance(route, dict) and str(route.get("route_id") or "").strip()
    }
    grouped_rows = _rows_by_route(comparison_rows)
    historical_rows = list(historical_summary_rows or [])
    subsets = corpus.get("recommended_subsets") or {}
    results: List[Dict[str, Any]] = []
    for candidate_subset, profile_name in PROFILE_FOR_SUBSET.items():
        supports_pairs = _capability_supports_pairs(candidate_subset)
        first_wave_subset = FIRST_WAVE_FOR_SUBSET[candidate_subset]
        candidate_ids = [str(route_id).strip() for route_id in list(subsets.get(candidate_subset) or []) if str(route_id).strip()]
        first_wave_ids = [str(route_id).strip() for route_id in list(subsets.get(first_wave_subset) or []) if str(route_id).strip()]
        next_review_subset = NEXT_REVIEW_FOR_SUBSET[candidate_subset]
        next_review_ids = [str(route_id).strip() for route_id in list(subsets.get(next_review_subset) or []) if str(route_id).strip()]
        contrast_subset = CONTRAST_FOR_SUBSET[candidate_subset]
        contrast_ids = [str(route_id).strip() for route_id in list(subsets.get(contrast_subset) or []) if str(route_id).strip()]
        review_pack_subset = REVIEW_PACK_FOR_SUBSET[candidate_subset]
        review_pack_ids = [str(route_id).strip() for route_id in list(subsets.get(review_pack_subset) or []) if str(route_id).strip()]
        review_priority_subset = REVIEW_PRIORITY_FOR_SUBSET[candidate_subset]
        review_priority_ids = [str(route_id).strip() for route_id in list(subsets.get(review_priority_subset) or []) if str(route_id).strip()]
        focus_pack_subset = FOCUS_PACK_FOR_SUBSET[candidate_subset]
        focus_pack_ids = [str(route_id).strip() for route_id in list(subsets.get(focus_pack_subset) or []) if str(route_id).strip()]
        proxy_pack_subset = PROXY_PACK_FOR_SUBSET[candidate_subset]
        proxy_pack_ids = [str(route_id).strip() for route_id in list(subsets.get(proxy_pack_subset) or []) if str(route_id).strip()]
        seed_pack_subset = SEED_PACK_FOR_SUBSET[candidate_subset]
        seed_pack_ids = [str(route_id).strip() for route_id in list(subsets.get(seed_pack_subset) or []) if str(route_id).strip()]
        history_gap_subset = HISTORY_GAP_FOR_SUBSET[candidate_subset]
        history_gap_ids = [str(route_id).strip() for route_id in list(subsets.get(history_gap_subset) or []) if str(route_id).strip()]
        pair_gap_subset = PAIR_GAP_FOR_SUBSET.get(candidate_subset, "")
        pair_gap_ids = [str(route_id).strip() for route_id in list(subsets.get(pair_gap_subset) or []) if str(route_id).strip()] if pair_gap_subset else []
        candidate_shape_coverage = sorted(
            {
                _shape_key(
                    _capability_geometry_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    _capability_direction_class(routes_by_id.get(route_id) or {}, candidate_subset),
                )
                for route_id in candidate_ids
                if routes_by_id.get(route_id)
            }
        )
        first_wave_rows: List[Dict[str, Any]] = []
        status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in first_wave_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            status_counts[status] = status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            first_wave_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        readiness = "discovery"
        if status_counts["pass"] > 0:
            readiness = "validated"
        elif (status_counts["candidate"] + status_counts["reviewed"]) > 0:
            readiness = "provisional"
        excluded_candidate_rows: List[Dict[str, Any]] = []
        for route_id in candidate_ids:
            if route_id in first_wave_ids:
                continue
            route = routes_by_id.get(route_id) or {}
            best_row = _best_row(grouped_rows.get(route_id) or [])
            excluded_candidate_rows.append(
                {
                    "route_id": route_id,
                    "status": route_status(route),
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                }
            )
        next_review_rows: List[Dict[str, Any]] = []
        next_review_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in next_review_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            next_review_status_counts[status] = next_review_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            next_review_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        contrast_rows: List[Dict[str, Any]] = []
        contrast_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in contrast_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            contrast_status_counts[status] = contrast_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            contrast_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        review_pack_rows: List[Dict[str, Any]] = []
        review_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in review_pack_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            review_pack_status_counts[status] = review_pack_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            review_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        review_priority_rows: List[Dict[str, Any]] = []
        review_priority_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in review_priority_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            review_priority_status_counts[status] = review_priority_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            review_priority_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        focus_pack_rows: List[Dict[str, Any]] = []
        focus_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in focus_pack_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            focus_pack_status_counts[status] = focus_pack_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            focus_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        proxy_pack_rows: List[Dict[str, Any]] = []
        proxy_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in proxy_pack_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            proxy_pack_status_counts[status] = proxy_pack_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            proxy_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        seed_pack_rows: List[Dict[str, Any]] = []
        seed_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in seed_pack_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            seed_pack_status_counts[status] = seed_pack_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            seed_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        history_gap_rows: List[Dict[str, Any]] = []
        history_gap_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in history_gap_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            history_gap_status_counts[status] = history_gap_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            history_gap_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        pair_gap_rows: List[Dict[str, Any]] = []
        pair_gap_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "risky": 0, "unreviewed": 0}
        for route_id in pair_gap_ids:
            route = routes_by_id.get(route_id) or {}
            status = route_status(route)
            pair_gap_status_counts[status] = pair_gap_status_counts.get(status, 0) + 1
            best_row = _best_row(grouped_rows.get(route_id) or [])
            pair_gap_rows.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "health_tags": ", ".join(sorted(str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip())) or "none",
                    "best_label": str(best_row.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_row.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_row.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_row.get("comparison_label") or "").strip() or "none",
                }
            )
        first_wave_geometry_coverage = ", ".join(
            sorted({str(item["geometry_class"]) for item in first_wave_rows if str(item.get("geometry_class") or "").strip()})
        ) or "none"
        first_wave_direction_coverage = ", ".join(
            sorted({str(item["direction_class"]) for item in first_wave_rows if str(item.get("direction_class") or "").strip()})
        ) or "none"
        first_wave_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in first_wave_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        uncovered_candidate_shape_coverage = sorted(
            set(candidate_shape_coverage).difference(first_wave_shape_coverage)
        )
        next_review_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in next_review_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        contrast_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in contrast_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        review_pack_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in review_pack_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        review_priority_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in review_priority_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        focus_pack_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in focus_pack_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        proxy_pack_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in proxy_pack_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        seed_pack_shape_coverage = sorted(
            {
                _shape_key(str(item.get("geometry_class") or ""), str(item.get("direction_class") or ""))
                for item in seed_pack_rows
                if str(item.get("geometry_class") or "").strip() and str(item.get("direction_class") or "").strip()
            }
        )
        candidate_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in candidate_ids],
            candidate_subset,
        )
        first_wave_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in first_wave_ids],
            candidate_subset,
        )
        next_review_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in next_review_ids],
            candidate_subset,
        )
        contrast_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in contrast_ids],
            candidate_subset,
        )
        review_pack_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in review_pack_ids],
            candidate_subset,
        )
        review_priority_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in review_priority_ids],
            candidate_subset,
        )
        focus_pack_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in focus_pack_ids],
            candidate_subset,
        )
        proxy_pack_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in proxy_pack_ids],
            candidate_subset,
        )
        seed_pack_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in seed_pack_ids],
            candidate_subset,
        )
        pair_gap_pair_coverage = _capability_pair_coverage(
            [routes_by_id.get(route_id) or {} for route_id in pair_gap_ids],
            candidate_subset,
        )
        missing_first_wave_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(first_wave_pair_coverage)
        )
        missing_contrast_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(contrast_pair_coverage)
        )
        missing_review_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_pack_pair_coverage)
        )
        missing_review_priority_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_priority_pair_coverage)
        )
        missing_focus_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(focus_pack_pair_coverage)
        )
        missing_proxy_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(proxy_pack_pair_coverage)
        )
        missing_seed_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(seed_pack_pair_coverage)
        )
        review_pack_readiness = _review_pack_readiness(
            supports_pairs=supports_pairs,
            review_pack_count=len(review_pack_ids),
            review_pack_pass_count=review_pack_status_counts["pass"],
            review_pack_candidate_count=review_pack_status_counts["candidate"],
            review_pack_reviewed_count=review_pack_status_counts["reviewed"],
            missing_review_pack_pair_coverage=missing_review_pack_pair_coverage,
        )
        focus_pack_readiness = _focus_pack_readiness(
            supports_pairs=supports_pairs,
            focus_pack_count=len(focus_pack_ids),
            focus_pack_pass_count=focus_pack_status_counts["pass"],
            focus_pack_candidate_count=focus_pack_status_counts["candidate"],
            focus_pack_reviewed_count=focus_pack_status_counts["reviewed"],
            focus_pack_pair_coverage=focus_pack_pair_coverage,
        )
        proxy_pack_readiness = _proxy_pack_readiness(
            supports_pairs=supports_pairs,
            proxy_pack_count=len(proxy_pack_ids),
            proxy_pack_pass_count=proxy_pack_status_counts["pass"],
            proxy_pack_candidate_count=proxy_pack_status_counts["candidate"],
            proxy_pack_reviewed_count=proxy_pack_status_counts["reviewed"],
            proxy_pack_pair_coverage=proxy_pack_pair_coverage,
        )
        seed_pack_readiness = _seed_pack_readiness(
            seed_pack_count=len(seed_pack_ids),
            seed_pack_pass_count=seed_pack_status_counts["pass"],
            seed_pack_candidate_count=seed_pack_status_counts["candidate"],
            seed_pack_reviewed_count=seed_pack_status_counts["reviewed"],
        )
        historical_semantic_rows = [
            row
            for row in historical_rows
            if str(row.get("route_id") or "").strip() in candidate_ids
            and bool(row.get(f"semantic::{candidate_subset}"))
        ]
        historical_semantic_best_rows = _best_rows_by_route(historical_semantic_rows)
        historical_semantic_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "unreviewed": 0}
        for best_history in historical_semantic_best_rows:
            history_status = _historical_row_status(best_history)
            historical_semantic_status_counts[history_status] = (
                historical_semantic_status_counts.get(history_status, 0) + 1
            )
        historical_semantic_best_rows_render: List[Dict[str, Any]] = []
        for best_history in historical_semantic_best_rows:
            route_id = str(best_history.get("route_id") or "").strip()
            if not route_id:
                continue
            route_history_rows = [
                row
                for row in historical_semantic_rows
                if str(row.get("route_id") or "").strip() == route_id
            ]
            history_status = _historical_row_status(best_history)
            historical_semantic_best_rows_render.append(
                {
                    "route_id": route_id,
                    "status": history_status,
                    "geometry_class": _capability_geometry_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "direction_class": _capability_direction_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "health_tags": f"historical_runs={len(route_history_rows)}",
                    "best_label": str(best_history.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_history.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_history.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_history.get("comparison_label") or "").strip() or "none",
                }
            )
        def _history_rows_for_route_ids(route_ids: Sequence[str]) -> List[Dict[str, Any]]:
            route_id_set = {str(route_id).strip() for route_id in route_ids if str(route_id).strip()}
            if not route_id_set:
                return []
            return [
                row
                for row in historical_rows
                if str(row.get("route_id") or "").strip() in route_id_set
                and bool(row.get(f"semantic::{candidate_subset}"))
            ]

        historical_focus_pack_run_rows = [
            row for row in _history_rows_for_route_ids(focus_pack_ids)
        ]
        historical_focus_pack_rows: List[Dict[str, Any]] = []
        historical_focus_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "unreviewed": 0}
        historical_focus_pack_route_ids_with_history = set()
        for route_id in focus_pack_ids:
            route_history_rows = [
                row
                for row in historical_focus_pack_run_rows
                if str(row.get("route_id") or "").strip() == route_id
            ]
            best_history = _best_history_row(route_history_rows)
            if not best_history:
                continue
            historical_focus_pack_route_ids_with_history.add(route_id)
            history_status = _historical_row_status(best_history)
            historical_focus_pack_status_counts[history_status] = (
                historical_focus_pack_status_counts.get(history_status, 0) + 1
            )
            historical_focus_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": history_status,
                    "geometry_class": _capability_geometry_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "direction_class": _capability_direction_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "health_tags": f"historical_runs={len(route_history_rows)}",
                    "best_label": str(best_history.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_history.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_history.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_history.get("comparison_label") or "").strip() or "none",
                }
            )
        historical_focus_pack_missing_routes = [
            route_id
            for route_id in focus_pack_ids
            if route_id not in historical_focus_pack_route_ids_with_history
        ]
        historical_proxy_pack_run_rows = [
            row for row in _history_rows_for_route_ids(proxy_pack_ids)
        ]
        historical_proxy_pack_rows: List[Dict[str, Any]] = []
        historical_proxy_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "unreviewed": 0}
        historical_proxy_pack_route_ids_with_history = set()
        for route_id in proxy_pack_ids:
            route_history_rows = [
                row
                for row in historical_proxy_pack_run_rows
                if str(row.get("route_id") or "").strip() == route_id
            ]
            best_history = _best_history_row(route_history_rows)
            if not best_history:
                continue
            historical_proxy_pack_route_ids_with_history.add(route_id)
            history_status = _historical_row_status(best_history)
            historical_proxy_pack_status_counts[history_status] = (
                historical_proxy_pack_status_counts.get(history_status, 0) + 1
            )
            historical_proxy_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": history_status,
                    "geometry_class": _capability_geometry_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "direction_class": _capability_direction_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "health_tags": f"historical_runs={len(route_history_rows)}",
                    "best_label": str(best_history.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_history.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_history.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_history.get("comparison_label") or "").strip() or "none",
                }
            )
        historical_proxy_pack_missing_routes = [
            route_id
            for route_id in proxy_pack_ids
            if route_id not in historical_proxy_pack_route_ids_with_history
        ]
        historical_seed_pack_run_rows = [
            row for row in _history_rows_for_route_ids(seed_pack_ids)
        ]
        historical_seed_pack_rows: List[Dict[str, Any]] = []
        historical_seed_pack_status_counts = {"pass": 0, "candidate": 0, "reviewed": 0, "unreviewed": 0}
        historical_seed_pack_route_ids_with_history = set()
        for route_id in seed_pack_ids:
            route_history_rows = [
                row
                for row in historical_seed_pack_run_rows
                if str(row.get("route_id") or "").strip() == route_id
            ]
            best_history = _best_history_row(route_history_rows)
            if not best_history:
                continue
            historical_seed_pack_route_ids_with_history.add(route_id)
            history_status = _historical_row_status(best_history)
            historical_seed_pack_status_counts[history_status] = (
                historical_seed_pack_status_counts.get(history_status, 0) + 1
            )
            historical_seed_pack_rows.append(
                {
                    "route_id": route_id,
                    "status": history_status,
                    "geometry_class": _capability_geometry_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "direction_class": _capability_direction_class(routes_by_id.get(route_id) or {}, candidate_subset),
                    "health_tags": f"historical_runs={len(route_history_rows)}",
                    "best_label": str(best_history.get("route_health_label") or "").strip() or "none",
                    "best_completion": safe_float(best_history.get("route_completion_ratio")) or 0.0,
                    "best_distance_m": safe_float(best_history.get("route_distance_achieved_m")) or 0.0,
                    "best_comparison_label": str(best_history.get("comparison_label") or "").strip() or "none",
                }
            )
        historical_seed_pack_missing_routes = [
            route_id
            for route_id in seed_pack_ids
            if route_id not in historical_seed_pack_route_ids_with_history
        ]
        history_readiness = _history_readiness(
            semantic_run_count=len(historical_semantic_rows),
            semantic_pass_count=historical_semantic_status_counts["pass"],
            semantic_candidate_count=historical_semantic_status_counts["candidate"],
            semantic_reviewed_count=historical_semantic_status_counts["reviewed"],
        )
        best_semantic_history = _best_semantic_history_row(profile_name, historical_semantic_rows)
        semantic_runtime_summary = _runtime_signal_summary(str(best_semantic_history.get("summary_path") or ""))
        semantic_startup_summary = _startup_probe_summary(str(best_semantic_history.get("summary_path") or ""))
        semantic_assessment = _semantic_runtime_assessment(
            profile_name,
            best_semantic_history,
            semantic_runtime_summary,
        )
        results.append(
            {
                "capability": profile_name,
                "candidate_subset": candidate_subset,
                "first_wave_subset": first_wave_subset,
                "candidate_count": len(candidate_ids),
                "first_wave_count": len(first_wave_ids),
                "next_review_subset": next_review_subset,
                "next_review_count": len(next_review_ids),
                "contrast_subset": contrast_subset,
                "contrast_count": len(contrast_ids),
                "review_pack_subset": review_pack_subset,
                "review_pack_count": len(review_pack_ids),
                "review_priority_subset": review_priority_subset,
                "review_priority_count": len(review_priority_ids),
                "focus_pack_subset": focus_pack_subset,
                "focus_pack_count": len(focus_pack_ids),
                "proxy_pack_subset": proxy_pack_subset,
                "proxy_pack_count": len(proxy_pack_ids),
                "seed_pack_subset": seed_pack_subset,
                "seed_pack_count": len(seed_pack_ids),
                "history_gap_subset": history_gap_subset,
                "history_gap_count": len(history_gap_ids),
                "pair_gap_subset": pair_gap_subset or "n/a",
                "pair_gap_count": len(pair_gap_ids),
                "next_review_pass_count": next_review_status_counts["pass"],
                "next_review_candidate_count": next_review_status_counts["candidate"],
                "next_review_reviewed_count": next_review_status_counts["reviewed"],
                "next_review_risky_count": next_review_status_counts["risky"],
                "next_review_unreviewed_count": next_review_status_counts["unreviewed"],
                "contrast_pass_count": contrast_status_counts["pass"],
                "contrast_candidate_count": contrast_status_counts["candidate"],
                "contrast_reviewed_count": contrast_status_counts["reviewed"],
                "contrast_risky_count": contrast_status_counts["risky"],
                "contrast_unreviewed_count": contrast_status_counts["unreviewed"],
                "review_pack_pass_count": review_pack_status_counts["pass"],
                "review_pack_candidate_count": review_pack_status_counts["candidate"],
                "review_pack_reviewed_count": review_pack_status_counts["reviewed"],
                "review_pack_risky_count": review_pack_status_counts["risky"],
                "review_pack_unreviewed_count": review_pack_status_counts["unreviewed"],
                "review_priority_pass_count": review_priority_status_counts["pass"],
                "review_priority_candidate_count": review_priority_status_counts["candidate"],
                "review_priority_reviewed_count": review_priority_status_counts["reviewed"],
                "review_priority_risky_count": review_priority_status_counts["risky"],
                "review_priority_unreviewed_count": review_priority_status_counts["unreviewed"],
                "focus_pack_pass_count": focus_pack_status_counts["pass"],
                "focus_pack_candidate_count": focus_pack_status_counts["candidate"],
                "focus_pack_reviewed_count": focus_pack_status_counts["reviewed"],
                "focus_pack_risky_count": focus_pack_status_counts["risky"],
                "focus_pack_unreviewed_count": focus_pack_status_counts["unreviewed"],
                "proxy_pack_pass_count": proxy_pack_status_counts["pass"],
                "proxy_pack_candidate_count": proxy_pack_status_counts["candidate"],
                "proxy_pack_reviewed_count": proxy_pack_status_counts["reviewed"],
                "proxy_pack_risky_count": proxy_pack_status_counts["risky"],
                "proxy_pack_unreviewed_count": proxy_pack_status_counts["unreviewed"],
                "seed_pack_pass_count": seed_pack_status_counts["pass"],
                "seed_pack_candidate_count": seed_pack_status_counts["candidate"],
                "seed_pack_reviewed_count": seed_pack_status_counts["reviewed"],
                "seed_pack_risky_count": seed_pack_status_counts["risky"],
                "seed_pack_unreviewed_count": seed_pack_status_counts["unreviewed"],
                "history_gap_pass_count": history_gap_status_counts["pass"],
                "history_gap_candidate_count": history_gap_status_counts["candidate"],
                "history_gap_reviewed_count": history_gap_status_counts["reviewed"],
                "history_gap_risky_count": history_gap_status_counts["risky"],
                "history_gap_unreviewed_count": history_gap_status_counts["unreviewed"],
                "pair_gap_pass_count": pair_gap_status_counts["pass"],
                "pair_gap_candidate_count": pair_gap_status_counts["candidate"],
                "pair_gap_reviewed_count": pair_gap_status_counts["reviewed"],
                "pair_gap_risky_count": pair_gap_status_counts["risky"],
                "pair_gap_unreviewed_count": pair_gap_status_counts["unreviewed"],
                "first_wave_pass_count": status_counts["pass"],
                "first_wave_candidate_count": status_counts["candidate"],
                "first_wave_reviewed_count": status_counts["reviewed"],
                "first_wave_risky_count": status_counts["risky"],
                "first_wave_unreviewed_count": status_counts["unreviewed"],
                "readiness": readiness,
                "focus_pack_readiness": focus_pack_readiness,
                "proxy_pack_readiness": proxy_pack_readiness,
                "seed_pack_readiness": seed_pack_readiness,
                "review_pack_readiness": review_pack_readiness,
                "history_readiness": history_readiness,
                "historical_semantic_run_count": len(historical_semantic_rows),
                "historical_semantic_route_count": len(historical_semantic_best_rows),
                "historical_semantic_pass_count": historical_semantic_status_counts["pass"],
                "historical_semantic_candidate_count": historical_semantic_status_counts["candidate"],
                "historical_semantic_reviewed_count": historical_semantic_status_counts["reviewed"],
                "historical_semantic_unreviewed_count": historical_semantic_status_counts["unreviewed"],
                "semantic_best_route_id": str(best_semantic_history.get("route_id") or "").strip() or "none",
                "semantic_best_comparison_label": str(best_semantic_history.get("comparison_label") or "").strip() or "none",
                "semantic_best_label": str(best_semantic_history.get("route_health_label") or "").strip() or "none",
                "semantic_best_completion": safe_float(best_semantic_history.get("route_completion_ratio")) or 0.0,
                "semantic_best_distance_m": safe_float(best_semantic_history.get("route_distance_achieved_m")) or 0.0,
                "semantic_observability": semantic_assessment["semantic_observability"],
                "semantic_evidence_level": semantic_assessment["semantic_evidence_level"],
                "semantic_pass_confidence": semantic_assessment["semantic_pass_confidence"],
                "semantic_note": semantic_assessment["semantic_note"],
                "startup_probe_present": semantic_startup_summary["startup_probe_present"],
                "startup_probe_attempt_count": semantic_startup_summary["startup_probe_attempt_count"],
                "startup_probe_first_world_ready_attempt": semantic_startup_summary["startup_probe_first_world_ready_attempt"],
                "startup_probe_recovered_after_retry": semantic_startup_summary["startup_probe_recovered_after_retry"],
                "startup_probe_lineage_class": semantic_startup_summary["startup_probe_lineage_class"],
                "startup_probe_pre_world_ready_failure_families": semantic_startup_summary["startup_probe_pre_world_ready_failure_families"],
                "startup_probe_final_status": semantic_startup_summary["startup_probe_final_status"],
                "startup_probe_final_failure_family": semantic_startup_summary["startup_probe_final_failure_family"],
                "startup_probe_final_rpc_ready": semantic_startup_summary["startup_probe_final_rpc_ready"],
                "startup_probe_final_world_ready": semantic_startup_summary["startup_probe_final_world_ready"],
                "startup_probe_retry_policy_present": semantic_startup_summary["startup_probe_retry_policy_present"],
                "startup_probe_retry_max_attempts": semantic_startup_summary["startup_probe_retry_max_attempts"],
                "startup_probe_retry_delay_sec": semantic_startup_summary["startup_probe_retry_delay_sec"],
                "startup_probe_retry_no_retry_families": semantic_startup_summary["startup_probe_retry_no_retry_families"],
                "startup_probe_final_retry_eligible": semantic_startup_summary["startup_probe_final_retry_eligible"] or "none",
                "startup_probe_final_retry_decision_reason": semantic_startup_summary["startup_probe_final_retry_decision_reason"],
                "runtime_total_frame_count": semantic_runtime_summary["runtime_total_frame_count"],
                "runtime_junction_frame_count": semantic_runtime_summary["runtime_junction_frame_count"],
                "runtime_ref_junction_frame_count": semantic_runtime_summary["runtime_ref_junction_frame_count"],
                "runtime_target_junction_frame_count": semantic_runtime_summary["runtime_target_junction_frame_count"],
                "runtime_external_control_frame_count": semantic_runtime_summary["runtime_external_control_frame_count"],
                "runtime_harness_target_wp_frame_count": semantic_runtime_summary["runtime_harness_target_wp_frame_count"],
                "runtime_harness_junction_flag_reliability": semantic_runtime_summary["runtime_harness_junction_flag_reliability"],
                "runtime_commanded_steer_intent_frame_count": semantic_runtime_summary["runtime_commanded_steer_intent_frame_count"],
                "runtime_measured_steer_nonzero_frame_count": semantic_runtime_summary["runtime_measured_steer_nonzero_frame_count"],
                "runtime_force_zero_steer_frame_count": semantic_runtime_summary["runtime_force_zero_steer_frame_count"],
                "runtime_brake_frame_count": semantic_runtime_summary["runtime_brake_frame_count"],
                "runtime_terminal_stop_hold_frame_count": semantic_runtime_summary["runtime_terminal_stop_hold_frame_count"],
                "runtime_terminal_stop_hold_engaged_count": semantic_runtime_summary["runtime_terminal_stop_hold_engaged_count"],
                "runtime_apollo_route_segment_ready_frame_count": semantic_runtime_summary["runtime_apollo_route_segment_ready_frame_count"],
                "runtime_apollo_route_segment_multi_frame_count": semantic_runtime_summary["runtime_apollo_route_segment_multi_frame_count"],
                "runtime_apollo_current_lane_ids": semantic_runtime_summary["runtime_apollo_current_lane_ids"],
                "runtime_apollo_current_lane_transition_count": semantic_runtime_summary["runtime_apollo_current_lane_transition_count"],
                "runtime_apollo_current_lane_road_transition_count": semantic_runtime_summary["runtime_apollo_current_lane_road_transition_count"],
                "runtime_apollo_planning_lane_ids": semantic_runtime_summary["runtime_apollo_planning_lane_ids"],
                "runtime_apollo_planning_target_lane_ids": semantic_runtime_summary["runtime_apollo_planning_target_lane_ids"],
                "runtime_apollo_lane_transition_count": semantic_runtime_summary["runtime_apollo_lane_transition_count"],
                "runtime_apollo_target_lane_transition_count": semantic_runtime_summary["runtime_apollo_target_lane_transition_count"],
                "runtime_apollo_lane_road_transition_count": semantic_runtime_summary["runtime_apollo_lane_road_transition_count"],
                "runtime_apollo_target_lane_road_transition_count": semantic_runtime_summary["runtime_apollo_target_lane_road_transition_count"],
                "runtime_apollo_planning_first_routing_road_count": semantic_runtime_summary["runtime_apollo_planning_first_routing_road_count"],
                "runtime_apollo_planning_last_routing_road_count": semantic_runtime_summary["runtime_apollo_planning_last_routing_road_count"],
                "runtime_apollo_planning_max_routing_road_count": semantic_runtime_summary["runtime_apollo_planning_max_routing_road_count"],
                "runtime_apollo_planning_multi_road_frame_count": semantic_runtime_summary["runtime_apollo_planning_multi_road_frame_count"],
                "runtime_apollo_planning_first_routing_segment_count": semantic_runtime_summary["runtime_apollo_planning_first_routing_segment_count"],
                "runtime_apollo_planning_last_routing_segment_count": semantic_runtime_summary["runtime_apollo_planning_last_routing_segment_count"],
                "runtime_apollo_planning_max_routing_segment_count": semantic_runtime_summary["runtime_apollo_planning_max_routing_segment_count"],
                "runtime_traffic_light_policy": semantic_runtime_summary["runtime_traffic_light_policy"] or "none",
                "runtime_traffic_light_force_green_publish_count": semantic_runtime_summary["runtime_traffic_light_force_green_publish_count"],
                "historical_focus_pack_run_count": len(historical_focus_pack_run_rows),
                "historical_focus_pack_route_count": len(historical_focus_pack_rows),
                "historical_focus_pack_pass_count": historical_focus_pack_status_counts["pass"],
                "historical_focus_pack_candidate_count": historical_focus_pack_status_counts["candidate"],
                "historical_focus_pack_reviewed_count": historical_focus_pack_status_counts["reviewed"],
                "historical_focus_pack_unreviewed_count": historical_focus_pack_status_counts["unreviewed"],
                "historical_focus_pack_missing_count": len(historical_focus_pack_missing_routes),
                "historical_focus_pack_missing_routes": ", ".join(historical_focus_pack_missing_routes) or "none",
                "historical_proxy_pack_run_count": len(historical_proxy_pack_run_rows),
                "historical_proxy_pack_route_count": len(historical_proxy_pack_rows),
                "historical_proxy_pack_pass_count": historical_proxy_pack_status_counts["pass"],
                "historical_proxy_pack_candidate_count": historical_proxy_pack_status_counts["candidate"],
                "historical_proxy_pack_reviewed_count": historical_proxy_pack_status_counts["reviewed"],
                "historical_proxy_pack_unreviewed_count": historical_proxy_pack_status_counts["unreviewed"],
                "historical_proxy_pack_missing_count": len(historical_proxy_pack_missing_routes),
                "historical_proxy_pack_missing_routes": ", ".join(historical_proxy_pack_missing_routes) or "none",
                "historical_seed_pack_run_count": len(historical_seed_pack_run_rows),
                "historical_seed_pack_route_count": len(historical_seed_pack_rows),
                "historical_seed_pack_pass_count": historical_seed_pack_status_counts["pass"],
                "historical_seed_pack_candidate_count": historical_seed_pack_status_counts["candidate"],
                "historical_seed_pack_reviewed_count": historical_seed_pack_status_counts["reviewed"],
                "historical_seed_pack_unreviewed_count": historical_seed_pack_status_counts["unreviewed"],
                "historical_seed_pack_missing_count": len(historical_seed_pack_missing_routes),
                "historical_seed_pack_missing_routes": ", ".join(historical_seed_pack_missing_routes) or "none",
                "first_wave_geometry_coverage": first_wave_geometry_coverage,
                "first_wave_direction_coverage": first_wave_direction_coverage,
                "first_wave_shape_coverage": ", ".join(first_wave_shape_coverage) or "none",
                "next_review_shape_coverage": ", ".join(next_review_shape_coverage) or "none",
                "contrast_shape_coverage": ", ".join(contrast_shape_coverage) or "none",
                "review_pack_shape_coverage": ", ".join(review_pack_shape_coverage) or "none",
                "review_priority_shape_coverage": ", ".join(review_priority_shape_coverage) or "none",
                "focus_pack_shape_coverage": ", ".join(focus_pack_shape_coverage) or "none",
                "proxy_pack_shape_coverage": ", ".join(proxy_pack_shape_coverage) or "none",
                "seed_pack_shape_coverage": ", ".join(seed_pack_shape_coverage) or "none",
                "candidate_pair_coverage": ", ".join(candidate_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "first_wave_pair_coverage": ", ".join(first_wave_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "next_review_pair_coverage": ", ".join(next_review_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "contrast_pair_coverage": ", ".join(contrast_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "review_pack_pair_coverage": ", ".join(review_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "review_priority_pair_coverage": ", ".join(review_priority_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "focus_pack_pair_coverage": ", ".join(focus_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "proxy_pack_pair_coverage": ", ".join(proxy_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "seed_pack_pair_coverage": ", ".join(seed_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "pair_gap_pair_coverage": ", ".join(pair_gap_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_first_wave_pair_coverage": ", ".join(missing_first_wave_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_contrast_pair_coverage": ", ".join(missing_contrast_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_review_pack_pair_coverage": ", ".join(missing_review_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_review_priority_pair_coverage": ", ".join(missing_review_priority_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_focus_pack_pair_coverage": ", ".join(missing_focus_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_proxy_pack_pair_coverage": ", ".join(missing_proxy_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "missing_seed_pack_pair_coverage": ", ".join(missing_seed_pack_pair_coverage) or ("n/a" if not supports_pairs else "none"),
                "uncovered_candidate_shape_coverage": ", ".join(uncovered_candidate_shape_coverage) or "none",
                "first_wave_rows": first_wave_rows,
                "next_review_rows": next_review_rows,
                "contrast_rows": contrast_rows,
                "review_pack_rows": review_pack_rows,
                "review_priority_rows": review_priority_rows,
                "focus_pack_rows": focus_pack_rows,
                "proxy_pack_rows": proxy_pack_rows,
                "seed_pack_rows": seed_pack_rows,
                "history_gap_rows": history_gap_rows,
                "historical_semantic_rows": historical_semantic_best_rows_render,
                "historical_focus_pack_rows": historical_focus_pack_rows,
                "historical_proxy_pack_rows": historical_proxy_pack_rows,
                "historical_seed_pack_rows": historical_seed_pack_rows,
                "pair_gap_rows": pair_gap_rows,
                "excluded_candidate_rows": excluded_candidate_rows,
            }
        )
    return results


def render_canonical_report(
    corpus: Dict[str, Any],
    comparison_rows: Sequence[Dict[str, str]],
    *,
    corpus_path: Path,
    comparison_path: Path,
    historical_summary_rows: Sequence[Dict[str, Any]] | None = None,
) -> str:
    canonical_rows = build_canonical_comparison_rows(corpus, comparison_rows, historical_summary_rows)
    curve_family = _curve_family_interpretation(canonical_rows)
    lines = [
        "# Town01 Canonical Capability Report",
        "",
        f"- generated_at_local: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- corpus_path: `{corpus_path}`",
        f"- comparison_path: `{comparison_path}`",
        f"- canonical_route_ids: `{', '.join(canonical_comparison_route_ids())}`",
        "",
        "## Summary",
        "",
        "| key | profile | route_id | engineering_gate | capability_evidence | promotion_candidate | runtime_contract | control_handoff | current_best_label | persistent_path_fallback_at_end | semantic_window_anchor_kind |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in canonical_rows:
        lines.append(
            "| `{comparison_key}` | `{capability_profile}` | `{route_id}` | `{engineering_gate}` | `{capability_evidence}` | `{promotion_candidate}` | `{runtime_contract_status}` | `{control_handoff_status}` | `{current_best_label}` | `{persistent_path_fallback_at_end}` | `{semantic_window_anchor_kind}` |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Curve Family Interpretation",
            "",
            f"- classification: `{curve_family['classification']}`",
            f"- bridge_vs_apollo: `{curve_family['bridge_vs_apollo']}`",
            f"- note: `{curve_family['note']}`",
            "",
            "## Route Detail",
            "",
        ]
    )
    for row in canonical_rows:
        lines.extend(
            [
                f"### {row['display_name']}",
                "",
                f"- role: `{row['role']}`",
                f"- route_id: `{row['route_id']}`",
                f"- geometry_class: `{row['geometry_class']}`",
                f"- direction_class: `{row['direction_class']}`",
                f"- corpus_status: `{row['corpus_status']}`",
                f"- engineering_gate: `{row['engineering_gate']}`",
                f"- capability_evidence: `{row['capability_evidence']}`",
                f"- promotion_candidate: `{row['promotion_candidate']}`",
                f"- runtime_contract_status: `{row['runtime_contract_status']}`",
                f"- control_handoff_status: `{row['control_handoff_status']}`",
                f"- current_best: `{row['current_best_label']}` / completion=`{row['current_best_completion']:.3f}` / distance_m=`{row['current_best_distance_m']:.2f}` / label=`{row['current_best_comparison_label']}`",
                f"- history_best: `{row['history_best_label']}` / completion=`{row['history_best_completion']:.3f}` / distance_m=`{row['history_best_distance_m']:.2f}` / label=`{row['history_best_comparison_label']}`",
                f"- semantic_observability: `{row['semantic_observability']}`",
                f"- semantic_evidence_level: `{row['semantic_evidence_level']}`",
                f"- semantic_pass_confidence: `{row['semantic_pass_confidence']}`",
                f"- semantic_note: `{row['semantic_note']}`",
                f"- bridge_policy_source: `{row['bridge_policy_source']}`",
                f"- lateral_guard_apply_count: `{row['lateral_guard_apply_count']}`",
                f"- trajectory_contract_lateral_guard_apply_count: `{row['trajectory_contract_lateral_guard_apply_count']}`",
                f"- persistent_path_fallback_at_end: `{row['persistent_path_fallback_at_end']}`",
                f"- semantic_window_anchor_kind: `{row['semantic_window_anchor_kind']}`",
                f"- first_high_steer_seq: `{row['first_high_steer_seq']}`",
                f"- first_matched_point_too_large_seq: `{row['first_matched_point_too_large_seq']}`",
                f"- simple_lat_lateral_error_abs_p95_before_anchor: `{row['simple_lat_lateral_error_abs_p95_before_anchor']}`",
                f"- simple_lat_heading_error_abs_p95_before_anchor: `{row['simple_lat_heading_error_abs_p95_before_anchor']}`",
                f"- target_point_kappa_abs_p95_before_anchor: `{row['target_point_kappa_abs_p95_before_anchor']}`",
                f"- mapped_carla_steer_cmd_abs_p95_before_anchor: `{row['mapped_carla_steer_cmd_abs_p95_before_anchor']}`",
                f"- summary_path: `{row['summary_path'] or 'none'}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_canonical_summary_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "comparison_key",
        "display_name",
        "role",
        "capability_profile",
        "route_id",
        "engineering_gate",
        "capability_evidence",
        "promotion_candidate",
        "runtime_contract_status",
        "control_handoff_status",
        "current_best_label",
        "current_best_completion",
        "current_best_distance_m",
        "current_best_comparison_label",
        "history_best_label",
        "history_best_completion",
        "history_best_distance_m",
        "history_best_comparison_label",
        "semantic_observability",
        "semantic_evidence_level",
        "semantic_pass_confidence",
        "persistent_path_fallback_at_end",
        "semantic_window_anchor_kind",
        "first_high_steer_seq",
        "first_matched_point_too_large_seq",
        "lateral_guard_apply_count",
        "trajectory_contract_lateral_guard_apply_count",
        "summary_path",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def _render_row_markdown(rows: Iterable[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for row in rows:
        lines.append(
            "- `{route_id}` | class=`{geometry_class}` | dir=`{direction_class}` | status=`{status}` | best_label=`{best_label}` | "
            "completion=`{best_completion:.3f}` | distance_m=`{best_distance_m:.1f}` | "
            "comparison=`{best_comparison_label}` | tags={health_tags}".format(**row)
        )
    return lines


def render_report(
    corpus: Dict[str, Any],
    comparison_rows: Sequence[Dict[str, str]],
    *,
    corpus_path: Path,
    comparison_path: Path,
    historical_summary_rows: Sequence[Dict[str, Any]] | None = None,
) -> str:
    progress_rows = build_capability_progress_rows(corpus, comparison_rows, historical_summary_rows)
    lines = [
        "# Town01 Capability Progress Report",
        "",
        f"- generated_at_local: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- corpus_path: `{corpus_path}`",
        f"- comparison_path: `{comparison_path}`",
        "",
        "## Summary",
        "",
        "| capability | first_wave_subset | readiness | focus_pack_readiness | proxy_pack_readiness | seed_pack_readiness | review_pack_readiness | history_readiness | semantic_observability | semantic_evidence | semantic_pass_confidence | pass | candidate | reviewed | risky | unreviewed |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in progress_rows:
        lines.append(
            "| `{capability}` | `{first_wave_subset}` | `{readiness}` | `{focus_pack_readiness}` | `{proxy_pack_readiness}` | `{seed_pack_readiness}` | `{review_pack_readiness}` | `{history_readiness}` | `{semantic_observability}` | `{semantic_evidence_level}` | `{semantic_pass_confidence}` | `{first_wave_pass_count}` | `{first_wave_candidate_count}` | "
            "`{first_wave_reviewed_count}` | `{first_wave_risky_count}` | `{first_wave_unreviewed_count}` |".format(
                **row
            )
        )
    lines.extend(["", "## Suggested Commands", ""])
    for row in progress_rows:
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['proxy_pack_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['focus_pack_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['seed_pack_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['history_gap_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['review_priority_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['review_pack_subset']} --sample-size 6"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['first_wave_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['next_review_subset']} --sample-size 4"
        )
        lines.append("```")
        lines.append("```bash")
        lines.append(
            "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
            f"--capability-profile {row['capability']} --recommended-subset {row['contrast_subset']} --sample-size 4"
        )
        lines.append("```")
        if row["pair_gap_subset"] != "n/a":
            lines.append("```bash")
            lines.append(
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run "
                f"--capability-profile {row['capability']} --recommended-subset {row['pair_gap_subset']} --sample-size 4"
            )
            lines.append("```")
    for row in progress_rows:
        lines.extend(
            [
                "",
                f"## {row['capability']}",
                "",
                f"- candidate_subset: `{row['candidate_subset']}` (`{row['candidate_count']}` routes)",
                f"- first_wave_subset: `{row['first_wave_subset']}` (`{row['first_wave_count']}` routes)",
                f"- next_review_subset: `{row['next_review_subset']}` (`{row['next_review_count']}` routes)",
                f"- contrast_subset: `{row['contrast_subset']}` (`{row['contrast_count']}` routes)",
                f"- review_pack_subset: `{row['review_pack_subset']}` (`{row['review_pack_count']}` routes)",
                f"- review_priority_subset: `{row['review_priority_subset']}` (`{row['review_priority_count']}` routes)",
                f"- focus_pack_subset: `{row['focus_pack_subset']}` (`{row['focus_pack_count']}` routes)",
                f"- proxy_pack_subset: `{row['proxy_pack_subset']}` (`{row['proxy_pack_count']}` routes)",
                f"- seed_pack_subset: `{row['seed_pack_subset']}` (`{row['seed_pack_count']}` routes)",
                f"- history_gap_subset: `{row['history_gap_subset']}` (`{row['history_gap_count']}` routes)",
                f"- pair_gap_subset: `{row['pair_gap_subset']}` (`{row['pair_gap_count']}` routes)",
                f"- readiness: `{row['readiness']}`",
                f"- focus_pack_readiness: `{row['focus_pack_readiness']}`",
                f"- proxy_pack_readiness: `{row['proxy_pack_readiness']}`",
                f"- seed_pack_readiness: `{row['seed_pack_readiness']}`",
                f"- review_pack_readiness: `{row['review_pack_readiness']}`",
                f"- history_readiness: `{row['history_readiness']}`",
                f"- semantic_observability: `{row['semantic_observability']}`",
                f"- semantic_evidence_level: `{row['semantic_evidence_level']}`",
                f"- semantic_pass_confidence: `{row['semantic_pass_confidence']}`",
                f"- semantic_note: `{row['semantic_note']}`",
                f"- semantic_best_route_id: `{row['semantic_best_route_id']}`",
                f"- semantic_best_label: `{row['semantic_best_label']}`",
                f"- semantic_best_completion: `{row['semantic_best_completion']:.4f}`",
                f"- semantic_best_distance_m: `{row['semantic_best_distance_m']:.2f}`",
                f"- semantic_best_comparison_label: `{row['semantic_best_comparison_label']}`",
                f"- startup_probe_present: `{row['startup_probe_present']}`",
                f"- startup_probe_attempt_count: `{row['startup_probe_attempt_count']}`",
                f"- startup_probe_first_world_ready_attempt: `{row['startup_probe_first_world_ready_attempt']}`",
                f"- startup_probe_recovered_after_retry: `{row['startup_probe_recovered_after_retry']}`",
                f"- startup_probe_lineage_class: `{row['startup_probe_lineage_class']}`",
                f"- startup_probe_pre_world_ready_failure_families: `{row['startup_probe_pre_world_ready_failure_families']}`",
                f"- startup_probe_final_status: `{row['startup_probe_final_status']}`",
                f"- startup_probe_final_failure_family: `{row['startup_probe_final_failure_family']}`",
                f"- startup_probe_final_rpc_ready: `{row['startup_probe_final_rpc_ready']}`",
                f"- startup_probe_final_world_ready: `{row['startup_probe_final_world_ready']}`",
                f"- startup_probe_retry_policy_present: `{row['startup_probe_retry_policy_present']}`",
                f"- startup_probe_retry_max_attempts: `{row['startup_probe_retry_max_attempts']}`",
                f"- startup_probe_retry_delay_sec: `{row['startup_probe_retry_delay_sec']}`",
                f"- startup_probe_retry_no_retry_families: `{row['startup_probe_retry_no_retry_families']}`",
                f"- startup_probe_final_retry_eligible: `{row['startup_probe_final_retry_eligible']}`",
                f"- startup_probe_final_retry_decision_reason: `{row['startup_probe_final_retry_decision_reason']}`",
                f"- runtime_total_frame_count: `{row['runtime_total_frame_count']}`",
                f"- runtime_junction_frame_count: `{row['runtime_junction_frame_count']}`",
                f"- runtime_ref_junction_frame_count: `{row['runtime_ref_junction_frame_count']}`",
                f"- runtime_target_junction_frame_count: `{row['runtime_target_junction_frame_count']}`",
                f"- runtime_external_control_frame_count: `{row['runtime_external_control_frame_count']}`",
                f"- runtime_harness_target_wp_frame_count: `{row['runtime_harness_target_wp_frame_count']}`",
                f"- runtime_harness_junction_flag_reliability: `{row['runtime_harness_junction_flag_reliability']}`",
                f"- runtime_commanded_steer_intent_frame_count: `{row['runtime_commanded_steer_intent_frame_count']}`",
                f"- runtime_measured_steer_nonzero_frame_count: `{row['runtime_measured_steer_nonzero_frame_count']}`",
                f"- runtime_force_zero_steer_frame_count: `{row['runtime_force_zero_steer_frame_count']}`",
                f"- runtime_brake_frame_count: `{row['runtime_brake_frame_count']}`",
                f"- runtime_terminal_stop_hold_frame_count: `{row['runtime_terminal_stop_hold_frame_count']}`",
                f"- runtime_terminal_stop_hold_engaged_count: `{row['runtime_terminal_stop_hold_engaged_count']}`",
                f"- runtime_apollo_route_segment_ready_frame_count: `{row['runtime_apollo_route_segment_ready_frame_count']}`",
                f"- runtime_apollo_route_segment_multi_frame_count: `{row['runtime_apollo_route_segment_multi_frame_count']}`",
                f"- runtime_apollo_current_lane_ids: `{row['runtime_apollo_current_lane_ids']}`",
                f"- runtime_apollo_current_lane_transition_count: `{row['runtime_apollo_current_lane_transition_count']}`",
                f"- runtime_apollo_current_lane_road_transition_count: `{row['runtime_apollo_current_lane_road_transition_count']}`",
                f"- runtime_apollo_planning_lane_ids: `{row['runtime_apollo_planning_lane_ids']}`",
                f"- runtime_apollo_planning_target_lane_ids: `{row['runtime_apollo_planning_target_lane_ids']}`",
                f"- runtime_apollo_lane_transition_count: `{row['runtime_apollo_lane_transition_count']}`",
                f"- runtime_apollo_target_lane_transition_count: `{row['runtime_apollo_target_lane_transition_count']}`",
                f"- runtime_apollo_lane_road_transition_count: `{row['runtime_apollo_lane_road_transition_count']}`",
                f"- runtime_apollo_target_lane_road_transition_count: `{row['runtime_apollo_target_lane_road_transition_count']}`",
                f"- runtime_apollo_planning_first_routing_road_count: `{row['runtime_apollo_planning_first_routing_road_count']}`",
                f"- runtime_apollo_planning_last_routing_road_count: `{row['runtime_apollo_planning_last_routing_road_count']}`",
                f"- runtime_apollo_planning_max_routing_road_count: `{row['runtime_apollo_planning_max_routing_road_count']}`",
                f"- runtime_apollo_planning_multi_road_frame_count: `{row['runtime_apollo_planning_multi_road_frame_count']}`",
                f"- runtime_apollo_planning_first_routing_segment_count: `{row['runtime_apollo_planning_first_routing_segment_count']}`",
                f"- runtime_apollo_planning_last_routing_segment_count: `{row['runtime_apollo_planning_last_routing_segment_count']}`",
                f"- runtime_apollo_planning_max_routing_segment_count: `{row['runtime_apollo_planning_max_routing_segment_count']}`",
                f"- runtime_traffic_light_policy: `{row['runtime_traffic_light_policy']}`",
                f"- runtime_traffic_light_force_green_publish_count: `{row['runtime_traffic_light_force_green_publish_count']}`",
                f"- historical_semantic_run_count: `{row['historical_semantic_run_count']}` across `{row['historical_semantic_route_count']}` routes",
                f"- historical_semantic_status_counts: `pass={row['historical_semantic_pass_count']}, candidate={row['historical_semantic_candidate_count']}, reviewed={row['historical_semantic_reviewed_count']}, unreviewed={row['historical_semantic_unreviewed_count']}`",
                f"- historical_focus_pack_run_count: `{row['historical_focus_pack_run_count']}` across `{row['historical_focus_pack_route_count']}` routes",
                f"- historical_focus_pack_missing_count: `{row['historical_focus_pack_missing_count']}`",
                f"- historical_focus_pack_missing_routes: `{row['historical_focus_pack_missing_routes']}`",
                f"- historical_proxy_pack_run_count: `{row['historical_proxy_pack_run_count']}` across `{row['historical_proxy_pack_route_count']}` routes",
                f"- historical_proxy_pack_missing_count: `{row['historical_proxy_pack_missing_count']}`",
                f"- historical_proxy_pack_missing_routes: `{row['historical_proxy_pack_missing_routes']}`",
                f"- historical_seed_pack_run_count: `{row['historical_seed_pack_run_count']}` across `{row['historical_seed_pack_route_count']}` routes",
                f"- historical_seed_pack_missing_count: `{row['historical_seed_pack_missing_count']}`",
                f"- historical_seed_pack_missing_routes: `{row['historical_seed_pack_missing_routes']}`",
                f"- geometry_coverage: `{row['first_wave_geometry_coverage']}`",
                f"- direction_coverage: `{row['first_wave_direction_coverage']}`",
                f"- first_wave_shape_coverage: `{row['first_wave_shape_coverage']}`",
                f"- next_review_shape_coverage: `{row['next_review_shape_coverage']}`",
                f"- contrast_shape_coverage: `{row['contrast_shape_coverage']}`",
                f"- review_pack_shape_coverage: `{row['review_pack_shape_coverage']}`",
                f"- review_priority_shape_coverage: `{row['review_priority_shape_coverage']}`",
                f"- focus_pack_shape_coverage: `{row['focus_pack_shape_coverage']}`",
                f"- proxy_pack_shape_coverage: `{row['proxy_pack_shape_coverage']}`",
                f"- seed_pack_shape_coverage: `{row['seed_pack_shape_coverage']}`",
                f"- candidate_pair_coverage: `{row['candidate_pair_coverage']}`",
                f"- first_wave_pair_coverage: `{row['first_wave_pair_coverage']}`",
                f"- next_review_pair_coverage: `{row['next_review_pair_coverage']}`",
                f"- contrast_pair_coverage: `{row['contrast_pair_coverage']}`",
                f"- review_pack_pair_coverage: `{row['review_pack_pair_coverage']}`",
                f"- review_priority_pair_coverage: `{row['review_priority_pair_coverage']}`",
                f"- focus_pack_pair_coverage: `{row['focus_pack_pair_coverage']}`",
                f"- proxy_pack_pair_coverage: `{row['proxy_pack_pair_coverage']}`",
                f"- seed_pack_pair_coverage: `{row['seed_pack_pair_coverage']}`",
                f"- pair_gap_pair_coverage: `{row['pair_gap_pair_coverage']}`",
                f"- missing_first_wave_pair_coverage: `{row['missing_first_wave_pair_coverage']}`",
                f"- missing_contrast_pair_coverage: `{row['missing_contrast_pair_coverage']}`",
                f"- missing_review_pack_pair_coverage: `{row['missing_review_pack_pair_coverage']}`",
                f"- missing_review_priority_pair_coverage: `{row['missing_review_priority_pair_coverage']}`",
                f"- missing_focus_pack_pair_coverage: `{row['missing_focus_pack_pair_coverage']}`",
                f"- missing_proxy_pack_pair_coverage: `{row['missing_proxy_pack_pair_coverage']}`",
                f"- missing_seed_pack_pair_coverage: `{row['missing_seed_pack_pair_coverage']}`",
                f"- uncovered_candidate_shape_coverage: `{row['uncovered_candidate_shape_coverage']}`",
                "",
                "### First-wave Status",
                "",
            ]
        )
        if row["first_wave_rows"]:
            lines.extend(_render_row_markdown(row["first_wave_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Next Review Queue", ""])
        lines.append(
            "- pass=`{next_review_pass_count}` | candidate=`{next_review_candidate_count}` | reviewed=`{next_review_reviewed_count}` | risky=`{next_review_risky_count}` | unreviewed=`{next_review_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["next_review_rows"]:
            lines.extend(_render_row_markdown(row["next_review_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Contrast Queue", ""])
        lines.append(
            "- pass=`{contrast_pass_count}` | candidate=`{contrast_candidate_count}` | reviewed=`{contrast_reviewed_count}` | risky=`{contrast_risky_count}` | unreviewed=`{contrast_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["contrast_rows"]:
            lines.extend(_render_row_markdown(row["contrast_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Review Pack", ""])
        lines.append(
            "- pass=`{review_pack_pass_count}` | candidate=`{review_pack_candidate_count}` | reviewed=`{review_pack_reviewed_count}` | risky=`{review_pack_risky_count}` | unreviewed=`{review_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["review_pack_rows"]:
            lines.extend(_render_row_markdown(row["review_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Review Priority Queue", ""])
        lines.append(
            "- pass=`{review_priority_pass_count}` | candidate=`{review_priority_candidate_count}` | reviewed=`{review_priority_reviewed_count}` | risky=`{review_priority_risky_count}` | unreviewed=`{review_priority_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["review_priority_rows"]:
            lines.extend(_render_row_markdown(row["review_priority_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Focus Pack", ""])
        lines.append(
            "- pass=`{focus_pack_pass_count}` | candidate=`{focus_pack_candidate_count}` | reviewed=`{focus_pack_reviewed_count}` | risky=`{focus_pack_risky_count}` | unreviewed=`{focus_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["focus_pack_rows"]:
            lines.extend(_render_row_markdown(row["focus_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Proxy Pack", ""])
        lines.append(
            "- pass=`{proxy_pack_pass_count}` | candidate=`{proxy_pack_candidate_count}` | reviewed=`{proxy_pack_reviewed_count}` | risky=`{proxy_pack_risky_count}` | unreviewed=`{proxy_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["proxy_pack_rows"]:
            lines.extend(_render_row_markdown(row["proxy_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Seed Pack", ""])
        lines.append(
            "- pass=`{seed_pack_pass_count}` | candidate=`{seed_pack_candidate_count}` | reviewed=`{seed_pack_reviewed_count}` | risky=`{seed_pack_risky_count}` | unreviewed=`{seed_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["seed_pack_rows"]:
            lines.extend(_render_row_markdown(row["seed_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Historical Semantic Evidence", ""])
        if row["historical_semantic_rows"]:
            lines.extend(_render_row_markdown(row["historical_semantic_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Historical Focus-Pack Evidence", ""])
        lines.append(
            "- pass=`{historical_focus_pack_pass_count}` | candidate=`{historical_focus_pack_candidate_count}` | reviewed=`{historical_focus_pack_reviewed_count}` | unreviewed=`{historical_focus_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["historical_focus_pack_rows"]:
            lines.extend(_render_row_markdown(row["historical_focus_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Historical Proxy-Pack Evidence", ""])
        lines.append(
            "- pass=`{historical_proxy_pack_pass_count}` | candidate=`{historical_proxy_pack_candidate_count}` | reviewed=`{historical_proxy_pack_reviewed_count}` | unreviewed=`{historical_proxy_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["historical_proxy_pack_rows"]:
            lines.extend(_render_row_markdown(row["historical_proxy_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Historical Seed-Pack Evidence", ""])
        lines.append(
            "- pass=`{historical_seed_pack_pass_count}` | candidate=`{historical_seed_pack_candidate_count}` | reviewed=`{historical_seed_pack_reviewed_count}` | unreviewed=`{historical_seed_pack_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["historical_seed_pack_rows"]:
            lines.extend(_render_row_markdown(row["historical_seed_pack_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### History Gap Queue", ""])
        lines.append(
            "- pass=`{history_gap_pass_count}` | candidate=`{history_gap_candidate_count}` | reviewed=`{history_gap_reviewed_count}` | risky=`{history_gap_risky_count}` | unreviewed=`{history_gap_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["history_gap_rows"]:
            lines.extend(_render_row_markdown(row["history_gap_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Pair Gap Queue", ""])
        lines.append(
            "- pass=`{pair_gap_pass_count}` | candidate=`{pair_gap_candidate_count}` | reviewed=`{pair_gap_reviewed_count}` | risky=`{pair_gap_risky_count}` | unreviewed=`{pair_gap_unreviewed_count}`".format(
                **row
            )
        )
        lines.append("")
        if row["pair_gap_rows"]:
            lines.extend(_render_row_markdown(row["pair_gap_rows"]))
        else:
            lines.append("- none")
        lines.extend(["", "### Excluded Candidate Spillover", ""])
        if row["excluded_candidate_rows"]:
            for spill in row["excluded_candidate_rows"]:
                lines.append(
                    "- `{route_id}` | class=`{geometry_class}` | dir=`{direction_class}` | status=`{status}` | best_label=`{best_label}` | completion=`{best_completion:.3f}` | tags={health_tags}".format(
                        **spill
                    )
                )
        else:
            lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def write_summary_csv(path: Path, progress_rows: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "capability",
        "candidate_subset",
        "first_wave_subset",
        "candidate_count",
        "first_wave_count",
        "next_review_subset",
        "next_review_count",
        "contrast_subset",
        "contrast_count",
        "review_pack_subset",
        "review_pack_count",
        "review_priority_subset",
        "review_priority_count",
        "focus_pack_subset",
        "focus_pack_count",
        "proxy_pack_subset",
        "proxy_pack_count",
        "seed_pack_subset",
        "seed_pack_count",
        "history_gap_subset",
        "history_gap_count",
        "pair_gap_subset",
        "pair_gap_count",
        "next_review_pass_count",
        "next_review_candidate_count",
        "next_review_reviewed_count",
        "next_review_risky_count",
        "next_review_unreviewed_count",
        "contrast_pass_count",
        "contrast_candidate_count",
        "contrast_reviewed_count",
        "contrast_risky_count",
        "contrast_unreviewed_count",
        "review_pack_pass_count",
        "review_pack_candidate_count",
        "review_pack_reviewed_count",
        "review_pack_risky_count",
        "review_pack_unreviewed_count",
        "review_priority_pass_count",
        "review_priority_candidate_count",
        "review_priority_reviewed_count",
        "review_priority_risky_count",
        "review_priority_unreviewed_count",
        "focus_pack_pass_count",
        "focus_pack_candidate_count",
        "focus_pack_reviewed_count",
        "focus_pack_risky_count",
        "focus_pack_unreviewed_count",
        "proxy_pack_pass_count",
        "proxy_pack_candidate_count",
        "proxy_pack_reviewed_count",
        "proxy_pack_risky_count",
        "proxy_pack_unreviewed_count",
        "seed_pack_pass_count",
        "seed_pack_candidate_count",
        "seed_pack_reviewed_count",
        "seed_pack_risky_count",
        "seed_pack_unreviewed_count",
        "history_gap_pass_count",
        "history_gap_candidate_count",
        "history_gap_reviewed_count",
        "history_gap_risky_count",
        "history_gap_unreviewed_count",
        "pair_gap_pass_count",
        "pair_gap_candidate_count",
        "pair_gap_reviewed_count",
        "pair_gap_risky_count",
        "pair_gap_unreviewed_count",
        "first_wave_pass_count",
        "first_wave_candidate_count",
        "first_wave_reviewed_count",
        "first_wave_risky_count",
        "first_wave_unreviewed_count",
        "readiness",
        "focus_pack_readiness",
        "proxy_pack_readiness",
        "seed_pack_readiness",
        "review_pack_readiness",
        "history_readiness",
        "semantic_best_route_id",
        "semantic_best_comparison_label",
        "semantic_best_label",
        "semantic_best_completion",
        "semantic_best_distance_m",
        "semantic_observability",
        "semantic_evidence_level",
        "semantic_pass_confidence",
        "semantic_note",
        "startup_probe_present",
        "startup_probe_attempt_count",
        "startup_probe_first_world_ready_attempt",
        "startup_probe_recovered_after_retry",
        "startup_probe_lineage_class",
        "startup_probe_pre_world_ready_failure_families",
        "startup_probe_final_status",
        "startup_probe_final_failure_family",
        "startup_probe_final_rpc_ready",
        "startup_probe_final_world_ready",
        "startup_probe_retry_policy_present",
        "startup_probe_retry_max_attempts",
        "startup_probe_retry_delay_sec",
        "startup_probe_retry_no_retry_families",
        "startup_probe_final_retry_eligible",
        "startup_probe_final_retry_decision_reason",
        "runtime_total_frame_count",
        "runtime_junction_frame_count",
        "runtime_ref_junction_frame_count",
        "runtime_target_junction_frame_count",
        "runtime_external_control_frame_count",
        "runtime_harness_target_wp_frame_count",
        "runtime_harness_junction_flag_reliability",
        "runtime_commanded_steer_intent_frame_count",
        "runtime_measured_steer_nonzero_frame_count",
        "runtime_force_zero_steer_frame_count",
        "runtime_brake_frame_count",
        "runtime_terminal_stop_hold_frame_count",
        "runtime_terminal_stop_hold_engaged_count",
        "runtime_apollo_route_segment_ready_frame_count",
        "runtime_apollo_route_segment_multi_frame_count",
        "runtime_apollo_current_lane_ids",
        "runtime_apollo_current_lane_transition_count",
        "runtime_apollo_current_lane_road_transition_count",
        "runtime_apollo_planning_lane_ids",
        "runtime_apollo_planning_target_lane_ids",
        "runtime_apollo_lane_transition_count",
        "runtime_apollo_target_lane_transition_count",
        "runtime_apollo_lane_road_transition_count",
        "runtime_apollo_target_lane_road_transition_count",
        "runtime_apollo_planning_first_routing_road_count",
        "runtime_apollo_planning_last_routing_road_count",
        "runtime_apollo_planning_max_routing_road_count",
        "runtime_apollo_planning_multi_road_frame_count",
        "runtime_apollo_planning_first_routing_segment_count",
        "runtime_apollo_planning_last_routing_segment_count",
        "runtime_apollo_planning_max_routing_segment_count",
        "runtime_traffic_light_policy",
        "runtime_traffic_light_force_green_publish_count",
        "historical_semantic_run_count",
        "historical_semantic_route_count",
        "historical_semantic_pass_count",
        "historical_semantic_candidate_count",
        "historical_semantic_reviewed_count",
        "historical_semantic_unreviewed_count",
        "historical_focus_pack_run_count",
        "historical_focus_pack_route_count",
        "historical_focus_pack_pass_count",
        "historical_focus_pack_candidate_count",
        "historical_focus_pack_reviewed_count",
        "historical_focus_pack_unreviewed_count",
        "historical_focus_pack_missing_count",
        "historical_focus_pack_missing_routes",
        "historical_proxy_pack_run_count",
        "historical_proxy_pack_route_count",
        "historical_proxy_pack_pass_count",
        "historical_proxy_pack_candidate_count",
        "historical_proxy_pack_reviewed_count",
        "historical_proxy_pack_unreviewed_count",
        "historical_proxy_pack_missing_count",
        "historical_proxy_pack_missing_routes",
        "historical_seed_pack_run_count",
        "historical_seed_pack_route_count",
        "historical_seed_pack_pass_count",
        "historical_seed_pack_candidate_count",
        "historical_seed_pack_reviewed_count",
        "historical_seed_pack_unreviewed_count",
        "historical_seed_pack_missing_count",
        "historical_seed_pack_missing_routes",
        "first_wave_geometry_coverage",
        "first_wave_direction_coverage",
        "first_wave_shape_coverage",
        "next_review_shape_coverage",
        "contrast_shape_coverage",
        "review_pack_shape_coverage",
        "review_priority_shape_coverage",
        "focus_pack_shape_coverage",
        "proxy_pack_shape_coverage",
        "seed_pack_shape_coverage",
        "candidate_pair_coverage",
        "first_wave_pair_coverage",
        "next_review_pair_coverage",
        "contrast_pair_coverage",
        "review_pack_pair_coverage",
        "review_priority_pair_coverage",
        "focus_pack_pair_coverage",
        "proxy_pack_pair_coverage",
        "seed_pack_pair_coverage",
        "pair_gap_pair_coverage",
        "missing_first_wave_pair_coverage",
        "missing_contrast_pair_coverage",
        "missing_review_pack_pair_coverage",
        "missing_review_priority_pair_coverage",
        "missing_focus_pack_pair_coverage",
        "missing_proxy_pack_pair_coverage",
        "missing_seed_pack_pair_coverage",
        "uncovered_candidate_shape_coverage",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in progress_rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser(description="Render Town01 capability progress report from corpus + comparison evidence.")
    parser.add_argument("--corpus", default=str(default_corpus_path(REPO_ROOT)))
    parser.add_argument("--comparison-csv", default="artifacts/town01_route_health_platform_comparison.csv")
    parser.add_argument("--runs-root", default="runs")
    parser.add_argument("--output", default="")
    parser.add_argument("--summary-csv", default="")
    parser.add_argument(
        "--full-subsets",
        action="store_true",
        help="Render the legacy subset-heavy report instead of the canonical five-route comparison report.",
    )
    args = parser.parse_args()

    corpus_path = Path(args.corpus).expanduser().resolve()
    comparison_path = Path(args.comparison_csv).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else (
        REPO_ROOT / "artifacts" / f"town01_capability_progress_report_{datetime.now().strftime('%Y%m%d')}.md"
    )
    summary_csv_path = Path(args.summary_csv).expanduser().resolve() if args.summary_csv else (
        REPO_ROOT / "artifacts" / f"town01_capability_progress_summary_{datetime.now().strftime('%Y%m%d')}.csv"
    )

    corpus = load_route_corpus(corpus_path)
    comparison_rows = _load_csv_rows(comparison_path)
    historical_summary_rows = _load_historical_summary_rows(runs_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if args.full_subsets:
        progress_rows = build_capability_progress_rows(corpus, comparison_rows, historical_summary_rows)
        output_path.write_text(
            render_report(
                corpus,
                comparison_rows,
                corpus_path=corpus_path,
                comparison_path=comparison_path,
                historical_summary_rows=historical_summary_rows,
            ),
            encoding="utf-8",
        )
        write_summary_csv(summary_csv_path, progress_rows)
    else:
        canonical_rows = build_canonical_comparison_rows(corpus, comparison_rows, historical_summary_rows)
        output_path.write_text(
            render_canonical_report(
                corpus,
                comparison_rows,
                corpus_path=corpus_path,
                comparison_path=comparison_path,
                historical_summary_rows=historical_summary_rows,
            ),
            encoding="utf-8",
        )
        write_canonical_summary_csv(summary_csv_path, canonical_rows)
    print(f"[town01-capability-progress] written: {output_path}")
    print(f"[town01-capability-progress] summary_csv: {summary_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
