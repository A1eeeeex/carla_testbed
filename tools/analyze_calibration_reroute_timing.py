#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _fmt_float(value: Any, digits: int = 3) -> str:
    out = _safe_float(value)
    if out is None:
        return ""
    return f"{out:.{digits}f}"


def _fmt_bool(value: Optional[bool]) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return ""


def _compact_bool(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return ""


def _coerce_bool(value: Any) -> Optional[bool]:
    if value is True or value is False:
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def _first_finite(*values: Any) -> Optional[float]:
    for value in values:
        out = _safe_float(value)
        if out is not None:
            return out
    return None


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _fmt_sec_text(value: Any) -> str:
    text = str(value or "").strip()
    return f"{text}s" if text else ""


def _candidate_signature(metadata: Dict[str, Any]) -> str:
    candidate = metadata.get("candidate") or {}
    road_id = candidate.get("road_id")
    lane_id = candidate.get("lane_id")
    start_y = _fmt_float(candidate.get("start_y"))
    parts = []
    if road_id is not None:
        parts.append(f"road_id={road_id}")
    if lane_id is not None:
        parts.append(f"lane_id={lane_id}")
    if start_y:
        parts.append(f"start_y≈{start_y}")
    return " ".join(parts) if parts else "unknown"


def _branch_family(length_m: Any) -> str:
    length = _safe_float(length_m)
    if length is None:
        return "unknown"
    if length < 150.0:
        return "short"
    return "long"


def _load_stage_times(path: Path) -> Dict[str, float]:
    stage_times: Dict[str, float] = {}
    for row in _load_jsonl(path):
        stage = str(row.get("stage") or "").strip()
        ts_sec = _safe_float(row.get("ts_sec"))
        if not stage or ts_sec is None or stage in stage_times:
            continue
        stage_times[stage] = ts_sec
    return stage_times


def _parse_run_spec(raw: str) -> Tuple[str, Path]:
    text = str(raw or "").strip()
    if not text or "=" not in text:
        raise argparse.ArgumentTypeError("run spec must look like label=/abs/or/relative/run_dir")
    label, path_text = text.split("=", 1)
    label = label.strip()
    path_text = path_text.strip()
    if not label or not path_text:
        raise argparse.ArgumentTypeError("run spec must look like label=/abs/or/relative/run_dir")
    return label, Path(path_text).expanduser().resolve()


def _analyze_run(label: str, run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    metadata = _load_json(artifacts / "scenario_metadata.json")
    summary = _load_json(run_dir / "summary.json")
    planning_summary = _load_json(artifacts / "planning_topic_debug_summary.json")
    health_summary = _load_json(artifacts / "bridge_health_summary.json")
    planning_timing_summary = health_summary.get("planning_timing_summary") or {}
    if not isinstance(planning_timing_summary, dict):
        planning_timing_summary = {}
    stage_times = _load_stage_times(artifacts / "startup_stage_timeline.jsonl")
    reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    routing_event_rows = _load_jsonl(artifacts / "routing_event_debug.jsonl")
    goal_validity_rows = _load_jsonl(artifacts / "goal_validity_debug.jsonl")
    planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
    route_debug_rows = _load_jsonl(artifacts / "stage5_apollo_reference_line_debug.jsonl")
    upstream_route_segment_rows = _load_jsonl(artifacts / "apollo_route_segment_debug.jsonl")
    upstream_reference_line_rows = _load_jsonl(artifacts / "apollo_reference_line_debug.jsonl")
    upstream_lane_follow_rows = _load_jsonl(artifacts / "stage5_apollo_lane_follow_map_debug.jsonl")

    final_reroute = reroute_rows[-1] if reroute_rows else {}
    transition_reroute = next(
        (row for row in reroute_rows if str(row.get("reroute_reason") or "").strip() == "long_phase_transition"),
        {},
    )
    final_goal = final_reroute.get("current_goal") or {}
    final_goal_validity = final_reroute.get("goal_validity_snapshot") or {}
    final_goal_resolution = final_reroute.get("goal_resolution") or {}
    final_planning_state = final_reroute.get("planning_state") or {}
    final_trigger = final_reroute.get("trigger_condition_snapshot") or {}
    final_recent_route_debug = final_reroute.get("recent_route_debug_snapshot") or {}
    final_pre_fallback_goal_validity = final_reroute.get("pre_fallback_goal_validity_snapshot") or {}

    final_reroute_ts = _safe_float(final_reroute.get("timestamp"))
    transition_reroute_ts = _safe_float(transition_reroute.get("timestamp"))
    first_planning = planning_rows[0] if planning_rows else {}
    first_route_debug = route_debug_rows[0] if route_debug_rows else {}
    first_routing_event = routing_event_rows[0] if routing_event_rows else {}
    last_routing_event = routing_event_rows[-1] if routing_event_rows else {}
    final_goal_validity_row = goal_validity_rows[-1] if goal_validity_rows else {}
    first_planning_ts = _safe_float(first_planning.get("timestamp"))
    first_route_debug_ts = _safe_float(first_route_debug.get("timestamp"))
    first_routing_event_ts = _safe_float(first_routing_event.get("timestamp"))
    last_routing_event_ts = _safe_float(last_routing_event.get("timestamp"))
    last_routing_send_ts = _first_finite(
        planning_summary.get("first_msg_last_routing_send_ts_sec"),
        planning_summary.get("first_msg_last_reroute_ts_sec"),
        planning_timing_summary.get("first_msg_last_routing_send_ts_sec"),
        planning_timing_summary.get("first_msg_last_reroute_ts_sec"),
        health_summary.get("planning_first_msg_last_routing_send_ts_sec"),
        health_summary.get("planning_first_msg_last_reroute_ts_sec"),
        last_routing_event_ts,
    )
    reroute_reason_sequence = " -> ".join(
        str(row.get("reroute_reason") or "").strip()
        for row in reroute_rows
        if str(row.get("reroute_reason") or "").strip()
    )
    first_routing_response_after_last_send_ts = _first_finite(
        planning_summary.get("routing_first_response_after_last_routing_send_ts_sec"),
        planning_timing_summary.get("routing_first_response_after_last_routing_send_ts_sec"),
        health_summary.get("routing_first_response_after_last_routing_send_ts_sec"),
    )
    first_success_routing_response_after_last_send_ts = _first_finite(
        planning_summary.get("routing_first_success_response_after_last_routing_send_ts_sec"),
        planning_timing_summary.get("routing_first_success_response_after_last_routing_send_ts_sec"),
        health_summary.get("routing_first_success_response_after_last_routing_send_ts_sec"),
    )
    adapter_start_done_ts = _safe_float(stage_times.get("adapter_start_done"))
    scenario_build_done_ts = _safe_float(stage_times.get("scenario_build_done"))

    pre_refresh_routing_event_rows = []
    pre_refresh_goal_validity_rows = []
    pre_refresh_planning_rows = []
    pre_refresh_route_debug_rows = []
    if final_reroute_ts is not None:
        pre_refresh_routing_event_rows = [
            row for row in routing_event_rows if (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]
        pre_refresh_goal_validity_rows = [
            row for row in goal_validity_rows if (_safe_float(row.get("timestamp")) or 0.0) < final_reroute_ts
        ]
        pre_refresh_planning_rows = [
            row for row in planning_rows if (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]
        pre_refresh_route_debug_rows = [
            row for row in route_debug_rows if (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]
    transition_to_refresh_goal_validity_rows = []
    pre_refresh_visible_route_debug_rows = []
    post_refresh_visible_route_debug_rows = []
    if final_reroute_ts is not None:
        pre_refresh_visible_route_debug_rows = [
            row
            for row in route_debug_rows
            if (_safe_float(row.get("timestamp")) or 0.0) < final_reroute_ts
            and (_safe_float(row.get("route_segment_total_length")) is not None or (_safe_float(row.get("route_segment_count")) or 0.0) > 0)
        ]
        post_refresh_visible_route_debug_rows = [
            row
            for row in route_debug_rows
            if (_safe_float(row.get("timestamp")) or 0.0) >= final_reroute_ts
            and (_safe_float(row.get("route_segment_total_length")) is not None or (_safe_float(row.get("route_segment_count")) or 0.0) > 0)
        ]
    matching_final_routing_event_rows = [
        row
        for row in routing_event_rows
        if str(row.get("reroute_reason") or "").strip() == str(final_reroute.get("reroute_reason") or "").strip()
    ]
    transition_to_refresh_upstream_route_segment_rows = []
    transition_to_refresh_upstream_reference_line_rows = []
    transition_to_refresh_upstream_lane_follow_rows = []
    if transition_reroute_ts is not None and final_reroute_ts is not None:
        transition_to_refresh_upstream_route_segment_rows = [
            row
            for row in upstream_route_segment_rows
            if transition_reroute_ts <= (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]
        transition_to_refresh_upstream_reference_line_rows = [
            row
            for row in upstream_reference_line_rows
            if transition_reroute_ts <= (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]
        transition_to_refresh_upstream_lane_follow_rows = [
            row
            for row in upstream_lane_follow_rows
            if transition_reroute_ts <= (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]
        transition_to_refresh_goal_validity_rows = [
            row
            for row in goal_validity_rows
            if transition_reroute_ts <= (_safe_float(row.get("timestamp")) or 0.0) <= final_reroute_ts
        ]

    last_pre_refresh_goal_validity_row = pre_refresh_goal_validity_rows[-1] if pre_refresh_goal_validity_rows else {}

    planning_before = None
    if first_planning_ts is not None and final_reroute_ts is not None:
        planning_before = first_planning_ts <= final_reroute_ts

    route_debug_before = None
    if first_route_debug_ts is not None and final_reroute_ts is not None:
        route_debug_before = first_route_debug_ts <= final_reroute_ts

    routing_event_before = None
    if last_routing_event_ts is not None and final_reroute_ts is not None:
        routing_event_before = last_routing_event_ts <= final_reroute_ts

    final_route_debug_ready = final_trigger.get("route_debug_ready_for_long_phase")
    final_fallback_applied = final_goal_resolution.get("fallback_applied")
    if final_fallback_applied is None:
        final_fallback_applied = final_goal_validity.get("fallback_applied")

    final_invalid_goal_reason = str(
        final_goal_resolution.get("invalid_goal_reason")
        or final_goal_validity.get("invalid_goal_reason")
        or ""
    )
    final_reroute_routing_request_sent = _coerce_bool(final_reroute.get("routing_request_sent"))
    final_pre_fallback_goal_mode = (
        final_goal_resolution.get("pre_fallback_goal_mode")
        or (final_reroute.get("pre_fallback_goal") or {}).get("goal_mode")
        or final_goal.get("requested_goal_mode")
        or final_goal_validity.get("requested_goal_mode")
        or ""
    )
    final_pre_fallback_goal_source = (
        final_goal_resolution.get("pre_fallback_goal_source")
        or (final_reroute.get("pre_fallback_goal") or {}).get("goal_source")
        or final_goal.get("goal_source")
        or ""
    )
    final_pre_fallback_invalid_goal = _coerce_bool(
        final_pre_fallback_goal_validity.get("invalid_goal")
    )
    final_pre_fallback_invalid_goal_reason = str(
        final_pre_fallback_goal_validity.get("invalid_goal_reason") or ""
    )
    final_recent_route_debug_seen = bool(final_recent_route_debug)

    final_stage5 = route_debug_rows[-1] if route_debug_rows else {}
    final_length = _safe_float(final_stage5.get("route_segment_total_length"))
    reroute_rows_at_or_after_first_planning = 0
    if first_planning_ts is not None:
        reroute_rows_at_or_after_first_planning = sum(
            1
            for row in reroute_rows
            if (_safe_float(row.get("timestamp")) or 0.0) >= first_planning_ts
        )
    reroute_rows_at_or_after_first_route_debug = 0
    if first_route_debug_ts is not None:
        reroute_rows_at_or_after_first_route_debug = sum(
            1
            for row in reroute_rows
            if (_safe_float(row.get("timestamp")) or 0.0) >= first_route_debug_ts
        )

    return {
        "label": label,
        "run_dir": str(run_dir),
        "candidate_signature": _candidate_signature(metadata),
        "summary_status": str(summary.get("summary_status") or ""),
        "fail_reason": str(summary.get("fail_reason") or ""),
        "final_branch_family": _branch_family(final_length),
        "final_route_segment_total_length_m": _fmt_float(final_length),
        "reroute_decision_row_count": len(reroute_rows),
        "reroute_reason_sequence": reroute_reason_sequence,
        "final_reroute_reason": str(final_reroute.get("reroute_reason") or ""),
        "final_reroute_goal_mode": str(final_goal.get("goal_mode") or ""),
        "final_reroute_goal_source": str(final_goal.get("goal_source") or ""),
        "final_reroute_pre_fallback_goal_mode": str(final_pre_fallback_goal_mode),
        "final_reroute_pre_fallback_goal_source": str(final_pre_fallback_goal_source or ""),
        "final_reroute_pre_fallback_invalid_goal": _compact_bool(final_pre_fallback_invalid_goal),
        "final_reroute_pre_fallback_invalid_goal_reason": final_pre_fallback_invalid_goal_reason,
        "final_reroute_pre_fallback_reference_line_count": (
            int(final_pre_fallback_goal_validity.get("reference_line_count"))
            if final_pre_fallback_goal_validity.get("reference_line_count") is not None
            else ""
        ),
        "final_reroute_pre_fallback_route_segment_count": (
            int(final_pre_fallback_goal_validity.get("route_segment_count"))
            if final_pre_fallback_goal_validity.get("route_segment_count") is not None
            else ""
        ),
        "final_reroute_pre_fallback_goal_distance_m": _fmt_float(
            final_pre_fallback_goal_validity.get("goal_distance_m"),
            digits=3,
        ),
        "final_reroute_fallback_applied": _compact_bool(final_fallback_applied),
        "final_reroute_invalid_goal_reason": final_invalid_goal_reason,
        "final_reroute_route_debug_ready": _compact_bool(final_route_debug_ready),
        "final_reroute_route_debug_ready_reason": str(
            final_trigger.get("route_debug_ready_reason") or ""
        ),
        "final_reroute_routing_request_sent": _fmt_bool(final_reroute_routing_request_sent),
        "final_reroute_last_route_debug_event_seen": _compact_bool(
            final_planning_state.get("last_route_debug_event_seen")
        ),
        "final_reroute_recent_route_debug_seen": _compact_bool(final_recent_route_debug_seen),
        "final_reroute_recent_route_debug_timestamp": _fmt_float(
            final_recent_route_debug.get("timestamp"),
            digits=3,
        ),
        "final_reroute_recent_route_debug_source": str(
            final_recent_route_debug.get("route_debug_source") or ""
        ),
        "final_reroute_recent_route_debug_age_sec": _fmt_float(
            final_recent_route_debug.get("age_sec"),
            digits=3,
        ),
        "final_reroute_recent_route_debug_route_segment_count": (
            int(final_recent_route_debug.get("route_segment_count"))
            if final_recent_route_debug.get("route_segment_count") is not None
            else ""
        ),
        "final_reroute_recent_route_debug_total_length_m": _fmt_float(
            final_recent_route_debug.get("route_segment_total_length"),
            digits=3,
        ),
        "final_reroute_recent_route_debug_signature": str(
            final_recent_route_debug.get("routing_lane_window_signature") or ""
        ),
        "final_reroute_recent_route_debug_provider_status": str(
            final_recent_route_debug.get("reference_line_provider_status") or ""
        ),
        "final_reroute_recent_route_debug_lane_follow_map_status": str(
            final_recent_route_debug.get("lane_follow_map_status") or ""
        ),
        "final_reroute_planning_msg_count_snapshot": (
            int(final_planning_state.get("planning_msg_count"))
            if final_planning_state.get("planning_msg_count") is not None
            else ""
        ),
        "final_reroute_timestamp": _fmt_float(final_reroute_ts, digits=3),
        "transition_reroute_timestamp": _fmt_float(transition_reroute_ts, digits=3),
        "adapter_start_done_timestamp": _fmt_float(adapter_start_done_ts, digits=3),
        "scenario_build_done_timestamp": _fmt_float(scenario_build_done_ts, digits=3),
        "first_routing_event_timestamp": _fmt_float(first_routing_event_ts, digits=3),
        "last_routing_event_timestamp": _fmt_float(last_routing_event_ts, digits=3),
        "last_routing_send_timestamp": _fmt_float(last_routing_send_ts, digits=3),
        "first_routing_response_after_last_send_timestamp": _fmt_float(
            first_routing_response_after_last_send_ts,
            digits=3,
        ),
        "first_success_routing_response_after_last_send_timestamp": _fmt_float(
            first_success_routing_response_after_last_send_ts,
            digits=3,
        ),
        "first_planning_timestamp": _fmt_float(first_planning_ts, digits=3),
        "first_route_debug_timestamp": _fmt_float(first_route_debug_ts, digits=3),
        "delta_last_routing_event_minus_scenario_build_done_sec": _fmt_float(
            (last_routing_event_ts - scenario_build_done_ts)
            if last_routing_event_ts is not None and scenario_build_done_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_adapter_start_done_sec": _fmt_float(
            (first_planning_ts - adapter_start_done_ts)
            if first_planning_ts is not None and adapter_start_done_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_scenario_build_done_sec": _fmt_float(
            (first_planning_ts - scenario_build_done_ts)
            if first_planning_ts is not None and scenario_build_done_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_route_debug_minus_scenario_build_done_sec": _fmt_float(
            (first_route_debug_ts - scenario_build_done_ts)
            if first_route_debug_ts is not None and scenario_build_done_ts is not None
            else None,
            digits=3,
        ),
        "delta_last_routing_event_minus_final_reroute_sec": _fmt_float(
            (last_routing_event_ts - final_reroute_ts)
            if last_routing_event_ts is not None and final_reroute_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_last_routing_event_sec": _fmt_float(
            (first_planning_ts - last_routing_event_ts)
            if first_planning_ts is not None and last_routing_event_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_routing_response_minus_last_routing_send_sec": _fmt_float(
            (first_routing_response_after_last_send_ts - last_routing_send_ts)
            if first_routing_response_after_last_send_ts is not None and last_routing_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_success_routing_response_minus_last_routing_send_sec": _fmt_float(
            (first_success_routing_response_after_last_send_ts - last_routing_send_ts)
            if first_success_routing_response_after_last_send_ts is not None and last_routing_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_last_routing_send_sec": _fmt_float(
            (first_planning_ts - last_routing_send_ts)
            if first_planning_ts is not None and last_routing_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_final_reroute_sec": _fmt_float(
            (first_planning_ts - final_reroute_ts)
            if first_planning_ts is not None and final_reroute_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_route_debug_minus_last_routing_event_sec": _fmt_float(
            (first_route_debug_ts - last_routing_event_ts)
            if first_route_debug_ts is not None and last_routing_event_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_route_debug_minus_last_routing_send_sec": _fmt_float(
            (first_route_debug_ts - last_routing_send_ts)
            if first_route_debug_ts is not None and last_routing_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_route_debug_minus_final_reroute_sec": _fmt_float(
            (first_route_debug_ts - final_reroute_ts)
            if first_route_debug_ts is not None and final_reroute_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_first_routing_response_after_last_send_sec": _fmt_float(
            (first_planning_ts - first_routing_response_after_last_send_ts)
            if first_planning_ts is not None and first_routing_response_after_last_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_planning_minus_first_success_routing_response_after_last_send_sec": _fmt_float(
            (first_planning_ts - first_success_routing_response_after_last_send_ts)
            if first_planning_ts is not None and first_success_routing_response_after_last_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_route_debug_minus_first_routing_response_after_last_send_sec": _fmt_float(
            (first_route_debug_ts - first_routing_response_after_last_send_ts)
            if first_route_debug_ts is not None and first_routing_response_after_last_send_ts is not None
            else None,
            digits=3,
        ),
        "delta_first_route_debug_minus_first_success_routing_response_after_last_send_sec": _fmt_float(
            (first_route_debug_ts - first_success_routing_response_after_last_send_ts)
            if first_route_debug_ts is not None and first_success_routing_response_after_last_send_ts is not None
            else None,
            digits=3,
        ),
        "routing_event_before_final_reroute": _fmt_bool(routing_event_before),
        "planning_before_final_reroute": _fmt_bool(planning_before),
        "route_debug_before_final_reroute": _fmt_bool(route_debug_before),
        "pre_refresh_routing_event_count": len(pre_refresh_routing_event_rows),
        "pre_refresh_goal_validity_count": len(pre_refresh_goal_validity_rows),
        "pre_refresh_planning_msg_count": len(pre_refresh_planning_rows),
        "pre_refresh_route_debug_msg_count": len(pre_refresh_route_debug_rows),
        "pre_refresh_visible_route_debug_count": len(pre_refresh_visible_route_debug_rows),
        "post_refresh_visible_route_debug_count": len(post_refresh_visible_route_debug_rows),
        "reroute_rows_at_or_after_first_planning_count": reroute_rows_at_or_after_first_planning,
        "reroute_rows_at_or_after_first_route_debug_count": reroute_rows_at_or_after_first_route_debug,
        "final_routing_event_match_count": len(matching_final_routing_event_rows),
        "transition_to_refresh_upstream_route_segment_debug_count": len(
            transition_to_refresh_upstream_route_segment_rows
        ),
        "transition_to_refresh_upstream_reference_line_debug_count": len(
            transition_to_refresh_upstream_reference_line_rows
        ),
        "transition_to_refresh_upstream_lane_follow_debug_count": len(
            transition_to_refresh_upstream_lane_follow_rows
        ),
        "transition_to_refresh_goal_validity_count": len(transition_to_refresh_goal_validity_rows),
        "first_transition_to_refresh_upstream_route_segment_debug_timestamp": _fmt_float(
            transition_to_refresh_upstream_route_segment_rows[0].get("timestamp")
            if transition_to_refresh_upstream_route_segment_rows
            else None,
            digits=3,
        ),
        "first_transition_to_refresh_upstream_route_segment_debug_total_length_m": _fmt_float(
            transition_to_refresh_upstream_route_segment_rows[0].get("route_segment_total_length")
            if transition_to_refresh_upstream_route_segment_rows
            else None,
            digits=3,
        ),
        "first_transition_to_refresh_upstream_route_segment_debug_signature": str(
            (
                transition_to_refresh_upstream_route_segment_rows[0].get("routing_lane_window_signature")
                if transition_to_refresh_upstream_route_segment_rows
                else ""
            )
            or ""
        ),
        "delta_first_transition_to_refresh_upstream_route_segment_debug_minus_transition_sec": _fmt_float(
            (
                _safe_float(transition_to_refresh_upstream_route_segment_rows[0].get("timestamp")) - transition_reroute_ts
                if transition_to_refresh_upstream_route_segment_rows and transition_reroute_ts is not None
                else None
            ),
            digits=3,
        ),
        "delta_first_transition_to_refresh_upstream_route_segment_debug_minus_final_reroute_sec": _fmt_float(
            (
                _safe_float(transition_to_refresh_upstream_route_segment_rows[0].get("timestamp")) - final_reroute_ts
                if transition_to_refresh_upstream_route_segment_rows and final_reroute_ts is not None
                else None
            ),
            digits=3,
        ),
        "delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_routing_response_after_last_send_sec": _fmt_float(
            (
                _safe_float(transition_to_refresh_upstream_route_segment_rows[0].get("timestamp"))
                - first_routing_response_after_last_send_ts
                if transition_to_refresh_upstream_route_segment_rows
                and first_routing_response_after_last_send_ts is not None
                else None
            ),
            digits=3,
        ),
        "delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_success_routing_response_after_last_send_sec": _fmt_float(
            (
                _safe_float(transition_to_refresh_upstream_route_segment_rows[0].get("timestamp"))
                - first_success_routing_response_after_last_send_ts
                if transition_to_refresh_upstream_route_segment_rows
                and first_success_routing_response_after_last_send_ts is not None
                else None
            ),
            digits=3,
        ),
        "last_pre_refresh_routing_event_timestamp": _fmt_float(
            pre_refresh_routing_event_rows[-1].get("timestamp") if pre_refresh_routing_event_rows else None,
            digits=3,
        ),
        "last_pre_refresh_goal_validity_timestamp": _fmt_float(
            last_pre_refresh_goal_validity_row.get("timestamp") if last_pre_refresh_goal_validity_row else None,
            digits=3,
        ),
        "last_pre_refresh_goal_validity_goal_mode": str(
            last_pre_refresh_goal_validity_row.get("goal_mode") or ""
        ),
        "last_pre_refresh_goal_validity_goal_source": str(
            last_pre_refresh_goal_validity_row.get("goal_source") or ""
        ),
        "last_pre_refresh_goal_validity_invalid_goal": _compact_bool(
            last_pre_refresh_goal_validity_row.get("invalid_goal")
        ),
        "last_pre_refresh_goal_validity_invalid_goal_reason": str(
            last_pre_refresh_goal_validity_row.get("invalid_goal_reason") or ""
        ),
        "last_pre_refresh_goal_validity_fallback_applied": _compact_bool(
            last_pre_refresh_goal_validity_row.get("fallback_applied")
        ),
        "last_pre_refresh_goal_validity_total_length_m": _fmt_float(
            last_pre_refresh_goal_validity_row.get("route_segment_total_length"),
            digits=3,
        ),
        "last_pre_refresh_goal_validity_route_segment_count": (
            int(last_pre_refresh_goal_validity_row.get("route_segment_count"))
            if last_pre_refresh_goal_validity_row.get("route_segment_count") is not None
            else ""
        ),
        "last_pre_refresh_goal_validity_reference_line_count": (
            int(last_pre_refresh_goal_validity_row.get("reference_line_count"))
            if last_pre_refresh_goal_validity_row.get("reference_line_count") is not None
            else ""
        ),
        "last_pre_refresh_goal_validity_signature": str(
            last_pre_refresh_goal_validity_row.get("routing_lane_window_signature") or ""
        ),
        "last_pre_refresh_goal_validity_recent_route_debug_present": _compact_bool(
            last_pre_refresh_goal_validity_row.get("recent_route_debug_present")
        ),
        "last_pre_refresh_goal_validity_recent_route_debug_fresh": _compact_bool(
            last_pre_refresh_goal_validity_row.get("recent_route_debug_fresh")
        ),
        "delta_last_pre_refresh_goal_validity_minus_final_reroute_sec": _fmt_float(
            (
                _safe_float(last_pre_refresh_goal_validity_row.get("timestamp")) - final_reroute_ts
                if last_pre_refresh_goal_validity_row and final_reroute_ts is not None
                else None
            ),
            digits=3,
        ),
        "last_pre_refresh_planning_timestamp": _fmt_float(
            pre_refresh_planning_rows[-1].get("timestamp") if pre_refresh_planning_rows else None,
            digits=3,
        ),
        "last_pre_refresh_route_debug_timestamp": _fmt_float(
            pre_refresh_route_debug_rows[-1].get("timestamp") if pre_refresh_route_debug_rows else None,
            digits=3,
        ),
        "last_pre_refresh_visible_route_debug_timestamp": _fmt_float(
            pre_refresh_visible_route_debug_rows[-1].get("timestamp") if pre_refresh_visible_route_debug_rows else None,
            digits=3,
        ),
        "last_pre_refresh_visible_route_debug_total_length_m": _fmt_float(
            pre_refresh_visible_route_debug_rows[-1].get("route_segment_total_length") if pre_refresh_visible_route_debug_rows else None,
            digits=3,
        ),
        "last_pre_refresh_visible_route_debug_signature": str(
            (pre_refresh_visible_route_debug_rows[-1].get("routing_lane_window_signature") if pre_refresh_visible_route_debug_rows else "")
            or ""
        ),
        "delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec": _fmt_float(
            (
                _safe_float(pre_refresh_visible_route_debug_rows[-1].get("timestamp")) - final_reroute_ts
                if pre_refresh_visible_route_debug_rows and final_reroute_ts is not None
                else None
            ),
            digits=3,
        ),
        "first_post_refresh_visible_route_debug_timestamp": _fmt_float(
            post_refresh_visible_route_debug_rows[0].get("timestamp") if post_refresh_visible_route_debug_rows else None,
            digits=3,
        ),
        "first_post_refresh_visible_route_debug_total_length_m": _fmt_float(
            post_refresh_visible_route_debug_rows[0].get("route_segment_total_length") if post_refresh_visible_route_debug_rows else None,
            digits=3,
        ),
        "first_post_refresh_visible_route_debug_signature": str(
            (post_refresh_visible_route_debug_rows[0].get("routing_lane_window_signature") if post_refresh_visible_route_debug_rows else "")
            or ""
        ),
        "delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec": _fmt_float(
            (
                _safe_float(post_refresh_visible_route_debug_rows[0].get("timestamp")) - final_reroute_ts
                if post_refresh_visible_route_debug_rows and final_reroute_ts is not None
                else None
            ),
            digits=3,
        ),
        "final_goal_validity_timestamp": _fmt_float(
            final_goal_validity_row.get("timestamp") if final_goal_validity_row else None,
            digits=3,
        ),
        "final_goal_validity_goal_mode": str(final_goal_validity_row.get("goal_mode") or ""),
        "final_goal_validity_goal_source": str(final_goal_validity_row.get("goal_source") or ""),
        "final_goal_validity_invalid_goal": _compact_bool(final_goal_validity_row.get("invalid_goal")),
        "final_goal_validity_invalid_goal_reason": str(final_goal_validity_row.get("invalid_goal_reason") or ""),
        "final_goal_validity_fallback_applied": _compact_bool(final_goal_validity_row.get("fallback_applied")),
        "final_goal_validity_total_length_m": _fmt_float(
            final_goal_validity_row.get("route_segment_total_length"),
            digits=3,
        ),
        "final_goal_validity_route_segment_count": (
            int(final_goal_validity_row.get("route_segment_count"))
            if final_goal_validity_row.get("route_segment_count") is not None
            else ""
        ),
        "final_goal_validity_reference_line_count": (
            int(final_goal_validity_row.get("reference_line_count"))
            if final_goal_validity_row.get("reference_line_count") is not None
            else ""
        ),
        "final_goal_validity_signature": str(final_goal_validity_row.get("routing_lane_window_signature") or ""),
        "final_goal_validity_recent_route_debug_present": _compact_bool(
            final_goal_validity_row.get("recent_route_debug_present")
        ),
        "final_goal_validity_recent_route_debug_fresh": _compact_bool(
            final_goal_validity_row.get("recent_route_debug_fresh")
        ),
        "final_goal_validity_recent_route_debug_timestamp": _fmt_float(
            final_goal_validity_row.get("recent_route_debug_timestamp"),
            digits=3,
        ),
        "final_goal_validity_recent_route_debug_source": str(
            final_goal_validity_row.get("recent_route_debug_source") or ""
        ),
        "final_goal_validity_lane_follow_map_status": str(
            final_goal_validity_row.get("lane_follow_map_status") or ""
        ),
        "final_goal_validity_reference_line_provider_status": str(
            final_goal_validity_row.get("reference_line_provider_status") or ""
        ),
    }


def _report(rows: Sequence[Dict[str, Any]]) -> str:
    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = [
        "# Calibration Reroute Boundary Timing Weekend",
        "",
        f"- generated_at_local: `{generated_at}`",
        "",
        "## Timing Table",
        "",
        "| label | candidate | final branch | final reroute | first planning | first route debug | pre-refresh evidence | refresh input | refresh pivot |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for row in rows:
        final_branch = (
            f"{row['final_branch_family']} / "
            f"len≈{row['final_route_segment_total_length_m'] or ''}m / "
            f"goal={row['final_reroute_goal_mode'] or ''}"
        )
        final_reroute = (
            f"reason={row['final_reroute_reason'] or ''}, "
            f"ts={row['final_reroute_timestamp'] or ''}, "
            f"ready={row['final_reroute_route_debug_ready'] or ''}, "
            f"fb={row['final_reroute_fallback_applied'] or ''}, "
            f"sent={row['final_reroute_routing_request_sent'] or ''}"
        )
        first_planning = (
            f"ts={row['first_planning_timestamp'] or ''}, "
            f"delta={row['delta_first_planning_minus_final_reroute_sec'] or ''}s, "
            f"before={row['planning_before_final_reroute'] or ''}"
        )
        first_route_debug = (
            f"ts={row['first_route_debug_timestamp'] or ''}, "
            f"delta={row['delta_first_route_debug_minus_final_reroute_sec'] or ''}s, "
            f"before={row['route_debug_before_final_reroute'] or ''}"
        )
        pre_refresh = (
            f"routing={row['pre_refresh_routing_event_count']}, "
            f"goal_validity={row['pre_refresh_goal_validity_count']}, "
            f"planning={row['pre_refresh_planning_msg_count']}, "
            f"route_debug={row['pre_refresh_route_debug_msg_count']}, "
            f"upstream_route={row['transition_to_refresh_upstream_route_segment_debug_count']}, "
            f"last_routing_ts={row['last_pre_refresh_routing_event_timestamp'] or ''}"
        )
        refresh_input = (
            f"pre_gv={row['last_pre_refresh_goal_validity_goal_mode'] or 'none'}"
            f"/{row['last_pre_refresh_goal_validity_goal_source'] or 'none'}"
            f"@{_fmt_sec_text(row['delta_last_pre_refresh_goal_validity_minus_final_reroute_sec']) or ''}, "
            f"final_gv={row['final_goal_validity_goal_mode'] or 'none'}"
            f"/{row['final_goal_validity_goal_source'] or 'none'}, "
            f"recent={row['final_goal_validity_signature'] or 'none'}"
            f"/{row['final_goal_validity_recent_route_debug_present'] or ''}"
            f"/{row['final_goal_validity_recent_route_debug_fresh'] or ''}"
        )
        refresh_pivot = (
            f"event_rows={row['final_routing_event_match_count']}, "
            f"pre={row['last_pre_refresh_visible_route_debug_signature'] or 'none'}"
            f"@{_fmt_sec_text(row['delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec']) or ''}, "
            f"post={row['first_post_refresh_visible_route_debug_signature'] or 'none'}"
            f"@{_fmt_sec_text(row['delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec']) or ''}, "
            f"gv_rows={row['transition_to_refresh_goal_validity_count']}"
        )
        lines.append(
            f"| `{row['label']}` | `{row['candidate_signature']}` | `{final_branch}` | "
            f"`{final_reroute}` | `{first_planning}` | `{first_route_debug}` | `{pre_refresh}` | "
            f"`{refresh_input}` | `{refresh_pivot}` |"
        )

    lines.extend(
        [
            "",
            "## Per-Run Read",
            "",
        ]
    )

    for row in rows:
        lines.extend(
            [
                f"### `{row['label']}`",
                "",
                f"- run: [{Path(row['run_dir']).name}]({row['run_dir']})",
                f"- summary: `status={row['summary_status'] or ''}, fail_reason={row['fail_reason'] or ''}`",
                (
                    "- final reroute boundary: "
                    f"`reason={row['final_reroute_reason'] or ''}, "
                    f"pre_goal={row['final_reroute_pre_fallback_goal_mode'] or ''}, "
                    f"pre_source={row['final_reroute_pre_fallback_goal_source'] or ''}, "
                    f"goal={row['final_reroute_goal_mode'] or ''}, "
                    f"goal_source={row['final_reroute_goal_source'] or ''}, "
                    f"fallback={row['final_reroute_fallback_applied'] or ''}, "
                    f"routing_request_sent={row['final_reroute_routing_request_sent'] or ''}, "
                    f"invalid_reason={row['final_reroute_invalid_goal_reason'] or ''}, "
                    f"route_debug_ready={row['final_reroute_route_debug_ready'] or ''}, "
                    f"route_debug_ready_reason={row['final_reroute_route_debug_ready_reason'] or ''}, "
                    f"last_route_debug_seen={row['final_reroute_last_route_debug_event_seen'] or ''}, "
                    f"planning_msg_snapshot={row['final_reroute_planning_msg_count_snapshot']}`"
                ),
                (
                    "- reroute sequence: "
                    f"`count={row['reroute_decision_row_count']}, "
                    f"reasons={row['reroute_reason_sequence'] or ''}, "
                    f"reroutes_after_first_planning={row['reroute_rows_at_or_after_first_planning_count']}, "
                    f"reroutes_after_first_route_debug={row['reroute_rows_at_or_after_first_route_debug_count']}`"
                ),
                (
                    "- timing boundary: "
                    f"`adapter_start_done_ts={row['adapter_start_done_timestamp'] or ''}, "
                    f"scenario_build_done_ts={row['scenario_build_done_timestamp'] or ''}, "
                    f"final_reroute_ts={row['final_reroute_timestamp'] or ''}, "
                    f"last_routing_send_ts={row['last_routing_send_timestamp'] or ''}, "
                    f"last_routing_event_ts={row['last_routing_event_timestamp'] or ''}, "
                    f"routing_delta={_fmt_sec_text(row['delta_last_routing_event_minus_final_reroute_sec'])}, "
                    f"routing_response_after_send={_fmt_sec_text(row['delta_first_routing_response_minus_last_routing_send_sec'])}, "
                    f"routing_success_response_after_send={_fmt_sec_text(row['delta_first_success_routing_response_minus_last_routing_send_sec'])}, "
                    f"first_planning_ts={row['first_planning_timestamp'] or ''}, "
                    f"first_route_debug_ts={row['first_route_debug_timestamp'] or ''}, "
                    f"planning_after_send={_fmt_sec_text(row['delta_first_planning_minus_last_routing_send_sec'])}, "
                    f"planning_after_routing={_fmt_sec_text(row['delta_first_planning_minus_last_routing_event_sec'])}, "
                    f"planning_after_response={_fmt_sec_text(row['delta_first_planning_minus_first_routing_response_after_last_send_sec'])}, "
                    f"planning_after_success_response={_fmt_sec_text(row['delta_first_planning_minus_first_success_routing_response_after_last_send_sec'])}, "
                    f"route_debug_after_send={_fmt_sec_text(row['delta_first_route_debug_minus_last_routing_send_sec'])}, "
                    f"planning_delta={_fmt_sec_text(row['delta_first_planning_minus_final_reroute_sec'])}, "
                    f"route_debug_after_routing={_fmt_sec_text(row['delta_first_route_debug_minus_last_routing_event_sec'])}, "
                    f"route_debug_after_response={_fmt_sec_text(row['delta_first_route_debug_minus_first_routing_response_after_last_send_sec'])}, "
                    f"route_debug_after_success_response={_fmt_sec_text(row['delta_first_route_debug_minus_first_success_routing_response_after_last_send_sec'])}, "
                    f"route_debug_delta={_fmt_sec_text(row['delta_first_route_debug_minus_final_reroute_sec'])}`"
                ),
                (
                    "- pre-refresh evidence: "
                    f"`routing_before={row['routing_event_before_final_reroute'] or ''}, "
                    f"goal_validity_rows={row['pre_refresh_goal_validity_count']}, "
                    f"planning_before={row['planning_before_final_reroute'] or ''}, "
                    f"route_debug_before={row['route_debug_before_final_reroute'] or ''}, "
                    f"routing_rows={row['pre_refresh_routing_event_count']}, "
                    f"planning_rows={row['pre_refresh_planning_msg_count']}, "
                    f"route_debug_rows={row['pre_refresh_route_debug_msg_count']}`"
                ),
                (
                    "- transition-to-refresh upstream materialization: "
                    f"`transition_ts={row['transition_reroute_timestamp'] or ''}, "
                    f"upstream_route_segment_rows={row['transition_to_refresh_upstream_route_segment_debug_count']}, "
                    f"upstream_reference_line_rows={row['transition_to_refresh_upstream_reference_line_debug_count']}, "
                    f"upstream_lane_follow_rows={row['transition_to_refresh_upstream_lane_follow_debug_count']}, "
                    f"first_upstream_route_segment_ts={row['first_transition_to_refresh_upstream_route_segment_debug_timestamp'] or ''}, "
                    f"upstream_after_transition={_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_transition_sec'])}, "
                    f"upstream_vs_refresh={_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_final_reroute_sec'])}, "
                    f"upstream_vs_first_response={_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_routing_response_after_last_send_sec'])}, "
                    f"upstream_vs_first_success_response={_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_success_routing_response_after_last_send_sec'])}, "
                    f"first_upstream_route_segment_len={row['first_transition_to_refresh_upstream_route_segment_debug_total_length_m'] or ''}m, "
                    f"first_upstream_route_segment_sig={row['first_transition_to_refresh_upstream_route_segment_debug_signature'] or 'none'}`"
                ),
                (
                    "- refresh input contract: "
                    f"`pre_invalid={row['final_reroute_pre_fallback_invalid_goal'] or ''}, "
                    f"pre_invalid_reason={row['final_reroute_pre_fallback_invalid_goal_reason'] or ''}, "
                    f"pre_goal_distance={row['final_reroute_pre_fallback_goal_distance_m'] or ''}m, "
                    f"pre_ref_count={row['final_reroute_pre_fallback_reference_line_count']}, "
                    f"pre_route_segments={row['final_reroute_pre_fallback_route_segment_count']}, "
                    f"recent_route_debug_seen={row['final_reroute_recent_route_debug_seen'] or ''}, "
                    f"recent_route_debug_ts={row['final_reroute_recent_route_debug_timestamp'] or ''}, "
                    f"recent_route_debug_source={row['final_reroute_recent_route_debug_source'] or ''}, "
                    f"recent_route_debug_age={_fmt_sec_text(row['final_reroute_recent_route_debug_age_sec'])}, "
                    f"recent_route_debug_len={row['final_reroute_recent_route_debug_total_length_m'] or ''}m, "
                    f"recent_route_debug_sig={row['final_reroute_recent_route_debug_signature'] or 'none'}, "
                    f"recent_route_debug_provider={row['final_reroute_recent_route_debug_provider_status'] or ''}, "
                    f"recent_route_debug_lane_follow={row['final_reroute_recent_route_debug_lane_follow_map_status'] or ''}`"
                ),
                (
                    "- goal-validity boundary: "
                    f"`transition_to_refresh_goal_validity_rows={row['transition_to_refresh_goal_validity_count']}, "
                    f"last_pre_goal_validity_ts={row['last_pre_refresh_goal_validity_timestamp'] or ''}, "
                    f"last_pre_goal_validity={row['last_pre_refresh_goal_validity_goal_mode'] or 'none'}/"
                    f"{row['last_pre_refresh_goal_validity_goal_source'] or 'none'}, "
                    f"last_pre_invalid={row['last_pre_refresh_goal_validity_invalid_goal'] or ''}"
                    f"({row['last_pre_refresh_goal_validity_invalid_goal_reason'] or ''}), "
                    f"last_pre_len={row['last_pre_refresh_goal_validity_total_length_m'] or ''}m, "
                    f"last_pre_sig={row['last_pre_refresh_goal_validity_signature'] or 'none'}, "
                    f"last_pre_recent={row['last_pre_refresh_goal_validity_recent_route_debug_present'] or ''}/"
                    f"{row['last_pre_refresh_goal_validity_recent_route_debug_fresh'] or ''}, "
                    f"final_goal_validity_ts={row['final_goal_validity_timestamp'] or ''}, "
                    f"final_goal_validity={row['final_goal_validity_goal_mode'] or 'none'}/"
                    f"{row['final_goal_validity_goal_source'] or 'none'}, "
                    f"final_invalid={row['final_goal_validity_invalid_goal'] or ''}"
                    f"({row['final_goal_validity_invalid_goal_reason'] or ''}), "
                    f"final_len={row['final_goal_validity_total_length_m'] or ''}m, "
                    f"final_route_segments={row['final_goal_validity_route_segment_count']}, "
                    f"final_ref_count={row['final_goal_validity_reference_line_count']}, "
                    f"final_sig={row['final_goal_validity_signature'] or 'none'}, "
                    f"final_recent={row['final_goal_validity_recent_route_debug_present'] or ''}/"
                    f"{row['final_goal_validity_recent_route_debug_fresh'] or ''}, "
                    f"final_lane_follow={row['final_goal_validity_lane_follow_map_status'] or ''}, "
                    f"final_provider={row['final_goal_validity_reference_line_provider_status'] or ''}`"
                ),
                (
                    "- refresh pivot: "
                    f"`routing_event_rows={row['final_routing_event_match_count']}, "
                    f"pre_visible_route_debug_rows={row['pre_refresh_visible_route_debug_count']}, "
                    f"last_pre_visible_ts={row['last_pre_refresh_visible_route_debug_timestamp'] or ''}, "
                    f"last_pre_visible_len={row['last_pre_refresh_visible_route_debug_total_length_m'] or ''}m, "
                    f"last_pre_visible_sig={row['last_pre_refresh_visible_route_debug_signature'] or 'none'}, "
                    f"pre_delta={_fmt_sec_text(row['delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec']) or ''}, "
                    f"post_visible_route_debug_rows={row['post_refresh_visible_route_debug_count']}, "
                    f"first_post_visible_ts={row['first_post_refresh_visible_route_debug_timestamp'] or ''}, "
                    f"first_post_visible_len={row['first_post_refresh_visible_route_debug_total_length_m'] or ''}m, "
                    f"first_post_visible_sig={row['first_post_refresh_visible_route_debug_signature'] or 'none'}, "
                    f"post_delta={_fmt_sec_text(row['delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec']) or ''}`"
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## Conclusions",
            "",
        ]
    )

    fresh_rows = [row for row in rows if row["final_branch_family"] == "short"]
    long_rows = [row for row in rows if row["final_branch_family"] == "long"]

    if fresh_rows:
        rendered = "; ".join(
            (
                f"`{row['label']}`: planning_after_routing={row['delta_first_planning_minus_last_routing_event_sec']}s, "
                f"route_debug_after_routing={row['delta_first_route_debug_minus_last_routing_event_sec']}s, "
                f"pre_refresh_planning={row['pre_refresh_planning_msg_count']}, "
                f"pre_refresh_route_debug={row['pre_refresh_route_debug_msg_count']}"
            )
            for row in fresh_rows
        )
        lines.append(
            "- The short-branch fresh runs share the same boundary shape: "
            + rendered
            + "."
        )

    if long_rows:
        rendered = "; ".join(
            (
                f"`{row['label']}`: planning_after_routing={row['delta_first_planning_minus_last_routing_event_sec']}s, "
                f"route_debug_after_routing={row['delta_first_route_debug_minus_last_routing_event_sec']}s, "
                f"pre_refresh_planning={row['pre_refresh_planning_msg_count']}, "
                f"pre_refresh_route_debug={row['pre_refresh_route_debug_msg_count']}"
            )
            for row in long_rows
        )
        lines.append(
            "- The long-branch reference run differs before provider failure, not after it: "
            + rendered
            + "."
        )

    if fresh_rows and long_rows:
        routing_ready = {
            row["routing_event_before_final_reroute"]
            for row in rows
            if row["routing_event_before_final_reroute"] in {"true", "false"}
        }
        if routing_ready == {"true"}:
            rendered = "; ".join(
                (
                    f"`{row['label']}`: routing_delta={row['delta_last_routing_event_minus_final_reroute_sec']}s, "
                    f"routing_rows={row['pre_refresh_routing_event_count']}, "
                    f"planning_after_routing={row['delta_first_planning_minus_last_routing_event_sec']}s"
                )
                for row in rows
            )
            lines.append(
                "- `routing_event_debug` timing by itself does not explain the current split: "
                + rendered
                + "."
            )
            lines.append(
                "- That pushes the remaining question one layer deeper: routing publication is already present before final refresh in all compared runs, so the decisive difference is between routing emission and planning/stage5 evidence arrival."
            )
        final_reroute_request_flags = {
            row["final_reroute_routing_request_sent"]
            for row in rows
            if row["final_reroute_routing_request_sent"] in {"true", "false"}
        }
        if final_reroute_request_flags == {"false"}:
            lines.append(
                "- In the current truthful corpus, the final `long_phase_refresh` row is a decision/update boundary rather than a new routing send: `routing_request_sent=false` in every compared run."
            )
            lines.append(
                "- That means the measured `planning_after_routing` gap is anchored to the earlier long-phase routing request, not to a fresh routing publication at final refresh."
            )
        final_routing_event_counts = {row["final_routing_event_match_count"] for row in rows}
        if final_routing_event_counts == {0}:
            lines.append(
                "- No compared run emits a matching `long_phase_refresh` row in `routing_event_debug`; the refresh boundary is only visible in `reroute_decision_debug` plus route-debug rows."
            )
        response_split_rows = [
            row
            for row in rows
            if row["delta_first_routing_response_minus_last_routing_send_sec"]
            or row["delta_first_success_routing_response_minus_last_routing_send_sec"]
        ]
        if response_split_rows:
            rendered = "; ".join(
                (
                    f"`{row['label']}`: "
                    f"response_after_send={row['delta_first_routing_response_minus_last_routing_send_sec'] or ''}s, "
                    f"success_response_after_send={row['delta_first_success_routing_response_minus_last_routing_send_sec'] or ''}s, "
                    f"planning_after_response={row['delta_first_planning_minus_first_routing_response_after_last_send_sec'] or ''}s, "
                    f"route_debug_after_response={row['delta_first_route_debug_minus_first_routing_response_after_last_send_sec'] or ''}s"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The new response-aware split now makes the post-send window auditable in two stages: "
                + rendered
                + "."
            )
            sequence_rendered = "; ".join(
                (
                    f"`{row['label']}`: reroutes={row['reroute_decision_row_count']}, "
                    f"sequence={row['reroute_reason_sequence'] or ''}, "
                    f"reroutes_after_first_planning={row['reroute_rows_at_or_after_first_planning_count']}, "
                    f"reroutes_after_first_route_debug={row['reroute_rows_at_or_after_first_route_debug_count']}"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The response-aware low-y pair also shares the same reroute decision sequence, but not the same ordering against evidence arrival: "
                + sequence_rendered
                + "."
            )
            lines.append(
                "- Because the final `long_phase_refresh` row is already the last reroute/update boundary in both response-aware runs, the short branch currently records no later reroute boundary after its first planning/route-debug rows finally appear."
            )
            pivot_rendered = "; ".join(
                (
                    f"`{row['label']}`: "
                    f"pre_visible={row['last_pre_refresh_visible_route_debug_signature'] or 'none'}"
                    f"@{_fmt_sec_text(row['delta_last_pre_refresh_visible_route_debug_minus_final_reroute_sec']) or ''}, "
                    f"post_visible={row['first_post_refresh_visible_route_debug_signature'] or 'none'}"
                    f"@{_fmt_sec_text(row['delta_first_post_refresh_visible_route_debug_minus_final_reroute_sec']) or ''}, "
                    f"goal_source={row['final_reroute_goal_source'] or ''}"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The response-aware pair also now exposes a refresh-window pivot rather than only a timing gap: "
                + pivot_rendered
                + "."
            )
            upstream_rendered = "; ".join(
                (
                    f"`{row['label']}`: "
                    f"upstream_route_rows={row['transition_to_refresh_upstream_route_segment_debug_count']}, "
                    f"upstream_reference_line_rows={row['transition_to_refresh_upstream_reference_line_debug_count']}, "
                    f"upstream_lane_follow_rows={row['transition_to_refresh_upstream_lane_follow_debug_count']}, "
                    f"first_upstream={row['first_transition_to_refresh_upstream_route_segment_debug_signature'] or 'none'}"
                    f"@{_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_transition_sec']) or ''}"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The pre-refresh split is not only a stage5 artifact. The same response-aware pair also diverges in upstream route-segment materialization: "
                + upstream_rendered
                + "."
            )
            upstream_response_rendered = "; ".join(
                (
                    f"`{row['label']}`: "
                    f"upstream_vs_first_response="
                    f"{_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_routing_response_after_last_send_sec']) or ''}, "
                    f"upstream_vs_first_success_response="
                    f"{_fmt_sec_text(row['delta_first_transition_to_refresh_upstream_route_segment_debug_minus_first_success_routing_response_after_last_send_sec']) or ''}"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The same pair also now shows that recorded routing-response timing is not the full explanation: "
                + upstream_response_rendered
                + "."
            )
            refresh_input_rendered = "; ".join(
                (
                    f"`{row['label']}`: "
                    f"pre_invalid={row['final_reroute_pre_fallback_invalid_goal'] or ''}"
                    f"({row['final_reroute_pre_fallback_invalid_goal_reason'] or ''}), "
                    f"recent={row['final_reroute_recent_route_debug_signature'] or 'none'}"
                    f"@{_fmt_sec_text(row['final_reroute_recent_route_debug_age_sec']) or ''}, "
                    f"recent_lane_follow={row['final_reroute_recent_route_debug_lane_follow_map_status'] or ''}"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The refresh input contract is no longer implicit: "
                + refresh_input_rendered
                + "."
            )
            goal_validity_rendered = "; ".join(
                (
                    f"`{row['label']}`: "
                    f"pre_gv={row['last_pre_refresh_goal_validity_goal_mode'] or 'none'}"
                    f"/{row['last_pre_refresh_goal_validity_goal_source'] or 'none'}"
                    f"@{_fmt_sec_text(row['delta_last_pre_refresh_goal_validity_minus_final_reroute_sec']) or ''}, "
                    f"final_gv={row['final_goal_validity_goal_mode'] or 'none'}"
                    f"/{row['final_goal_validity_goal_source'] or 'none'}, "
                    f"final_invalid={row['final_goal_validity_invalid_goal'] or ''}"
                    f"({row['final_goal_validity_invalid_goal_reason'] or ''}), "
                    f"final_sig={row['final_goal_validity_signature'] or 'none'}, "
                    f"recent={row['final_goal_validity_recent_route_debug_present'] or ''}/"
                    f"{row['final_goal_validity_recent_route_debug_fresh'] or ''}"
                )
                for row in response_split_rows
            )
            lines.append(
                "- The goal-validity row itself now makes the refresh flip explicit: "
                + goal_validity_rendered
                + "."
            )
            lines.append(
                "- That means the long branch does not pivot to fallback from an otherwise identical refresh boundary. It reaches `long_phase_refresh` with a broader upstream route-segment materialization already in place, and the goal-validity row itself flips from pre-refresh `scenario_xy` to final `invalid_goal_fallback_ahead` with one visible `~6m` route segment plus `route_segments_present_reference_line_missing`. The short branch reaches the same reroute boundary with no upstream route-segment materialization, no recent route-debug snapshot, and a final goal-validity row that stays `scenario_xy`."
            )
        lines.append(
            "- The strongest current calibration split is now timing on the final `long_phase_refresh` boundary: the short family reaches final refresh with no planning/route-debug evidence yet, while the long family already has planning/route-debug rows before refresh."
        )
        lines.append(
            "- More specifically, the long response-aware branch holds a short visible window until just before refresh and then pivots almost immediately to a fallback-ahead long window, while the short branch has no pre-refresh window and only later surfaces the scenario-xy long window."
        )
        lines.append(
            "- This makes `high-y vs low-y` an insufficient explanation on its own. The more predictive question is whether planning/route-debug has already arrived before final refresh, because that is the point where the same truthful candidate family can still diverge into `scenario_xy` short vs fallback-bearing long branches."
        )
    lines.append(
        "- The next minimal push should target what materializes the upstream route-segment / lane-follow / reference-line debug rows before final refresh on the long branch, because that upstream materialization is now the direct source of the recent route-debug snapshot that separates `scenario_goal_file` retention from `invalid_goal_fallback_ahead` pivot."
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze reroute-boundary timing across calibration runs.")
    parser.add_argument(
        "--run",
        action="append",
        required=True,
        metavar="LABEL=RUN_DIR",
        help="Run label and run directory.",
    )
    parser.add_argument("--report", required=True, help="Markdown report output path.")
    parser.add_argument("--summary-csv", required=True, help="CSV summary output path.")
    args = parser.parse_args()

    rows: List[Dict[str, Any]] = []
    for raw in args.run:
        label, run_dir = _parse_run_spec(raw)
        rows.append(_analyze_run(label, run_dir))

    _write_csv(Path(args.summary_csv).resolve(), rows)
    _write_text(Path(args.report).resolve(), _report(rows))


if __name__ == "__main__":
    main()
