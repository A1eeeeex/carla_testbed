#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_float, safe_int


def _first_row(rows: Sequence[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    for row in rows:
        if predicate(row):
            return row
    return None


def _count_before(rows: Sequence[Dict[str, Any]], ts: Optional[float]) -> int:
    if ts is None:
        return 0
    return sum(1 for row in rows if safe_float(row.get("timestamp")) is not None and safe_float(row.get("timestamp")) < ts)


def _count_route_segment_before(rows: Sequence[Dict[str, Any]], ts: Optional[float]) -> int:
    if ts is None:
        return 0
    total = 0
    for row in rows:
        row_ts = safe_float(row.get("timestamp"))
        if row_ts is None or row_ts >= ts:
            continue
        if (safe_int(row.get("route_segment_count")) or 0) > 0:
            total += 1
    return total


def _materialized(row: Dict[str, Any]) -> bool:
    if (safe_int(row.get("route_segment_count")) or 0) > 0:
        return True
    if (safe_int(row.get("reference_line_count")) or 0) > 0:
        return True
    return bool(_text(row.get("routing_lane_window_signature")).strip())


def _dt(base_ts: Optional[float], event_ts: Optional[float]) -> Optional[float]:
    if base_ts is None or event_ts is None:
        return None
    return event_ts - base_ts


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _snapshot(label: str, run_dir: Path) -> Dict[str, Any]:
    planning_summary = load_json(run_dir / "artifacts" / "planning_topic_debug_summary.json")
    bridge_summary = load_json(run_dir / "artifacts" / "bridge_health_summary.json")
    snapshot_meta = load_json(run_dir / "artifacts" / "apollo_log_snapshot_meta.json")
    summary = load_json(run_dir / "summary.json")
    reroute_rows = load_jsonl(run_dir / "artifacts" / "reroute_decision_debug.jsonl")
    routing_event_rows = load_jsonl(run_dir / "artifacts" / "routing_event_debug.jsonl")
    planning_rows = load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")
    planning_route_segment_rows = load_jsonl(run_dir / "artifacts" / "planning_route_segment_debug.jsonl")
    apollo_route_segment_rows = load_jsonl(run_dir / "artifacts" / "apollo_route_segment_debug.jsonl")
    apollo_reference_line_rows = load_jsonl(run_dir / "artifacts" / "apollo_reference_line_debug.jsonl")
    stage5_rows = load_jsonl(run_dir / "artifacts" / "stage5_apollo_reference_line_debug.jsonl")

    if not reroute_rows:
        raise SystemExit(f"missing reroute_decision_debug rows: {run_dir}")
    if not routing_event_rows:
        raise SystemExit(f"missing routing_event_debug rows: {run_dir}")
    if not planning_rows:
        raise SystemExit(f"missing planning_topic_debug rows: {run_dir}")
    if not planning_route_segment_rows:
        raise SystemExit(f"missing planning_route_segment_debug rows: {run_dir}")
    if not apollo_route_segment_rows:
        raise SystemExit(f"missing apollo_route_segment_debug rows: {run_dir}")
    if not apollo_reference_line_rows:
        raise SystemExit(f"missing apollo_reference_line_debug rows: {run_dir}")
    if not stage5_rows:
        raise SystemExit(f"missing stage5 reference-line rows: {run_dir}")

    startup_event = _first_row(routing_event_rows, lambda row: str(row.get("reroute_reason") or "") == "startup_initial_route")
    transition_event = _first_row(routing_event_rows, lambda row: str(row.get("reroute_reason") or "") == "long_phase_transition")
    refresh_event = _first_row(routing_event_rows, lambda row: str(row.get("reroute_reason") or "") == "long_phase_refresh")
    startup_decision = _first_row(reroute_rows, lambda row: str(row.get("reroute_reason") or "") == "startup_initial_route")
    transition_decision = _first_row(reroute_rows, lambda row: str(row.get("reroute_reason") or "") == "long_phase_transition")
    final_refresh = reroute_rows[-1]
    last_send_ts = safe_float(planning_summary.get("first_msg_last_routing_send_ts_sec"))
    first_response_ts = safe_float(planning_summary.get("routing_first_response_after_last_routing_send_ts_sec"))
    final_refresh_ts = safe_float(final_refresh.get("timestamp"))
    transition_ts = safe_float((transition_decision or {}).get("timestamp"))
    first_planning_ts = safe_float(planning_summary.get("first_msg_ts_sec"))
    first_route_debug_ts = safe_float(planning_summary.get("first_route_debug_ts_sec"))
    first_stage5_ts = safe_float(stage5_rows[0].get("timestamp"))
    first_route_segment_row = _first_row(stage5_rows, lambda row: (safe_int(row.get("route_segment_count")) or 0) > 0)
    first_current_lane_row = _first_row(stage5_rows, lambda row: bool(_text(row.get("current_lane_id")).strip()))
    first_planning_route_segment_row = _first_row(planning_route_segment_rows, _materialized)
    first_apollo_route_segment_row = _first_row(apollo_route_segment_rows, _materialized)
    first_apollo_reference_line_row = _first_row(apollo_reference_line_rows, _materialized)
    last_pre_refresh_route_segment_row = _first_row(
        list(reversed(stage5_rows)),
        lambda row: final_refresh_ts is not None
        and safe_float(row.get("timestamp")) is not None
        and safe_float(row.get("timestamp")) < final_refresh_ts
        and (safe_int(row.get("route_segment_count")) or 0) > 0,
    )
    first_post_refresh_route_segment_row = _first_row(
        stage5_rows,
        lambda row: final_refresh_ts is not None
        and safe_float(row.get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= final_refresh_ts
        and (safe_int(row.get("route_segment_count")) or 0) > 0,
    )

    planning_before_response = _count_before(planning_rows, first_response_ts)
    stage5_before_response = _count_before(stage5_rows, first_response_ts)
    route_segments_before_response = _count_route_segment_before(stage5_rows, first_response_ts)
    planning_before_final_refresh = _count_before(planning_rows, final_refresh_ts)
    stage5_before_final_refresh = _count_before(stage5_rows, final_refresh_ts)
    route_segments_before_final_refresh = _count_route_segment_before(stage5_rows, final_refresh_ts)

    layer_first_rows = {
        "planning_route_segment": first_planning_route_segment_row,
        "apollo_route_segment": first_apollo_route_segment_row,
        "apollo_reference_line": first_apollo_reference_line_row,
        "stage5_reference_line": first_route_segment_row,
    }
    layer_first_ts_values = [
        safe_float((row or {}).get("timestamp"))
        for row in layer_first_rows.values()
        if safe_float((row or {}).get("timestamp")) is not None
    ]
    layer_first_sig_values = [
        _text((row or {}).get("routing_lane_window_signature")).strip()
        for row in layer_first_rows.values()
        if _text((row or {}).get("routing_lane_window_signature")).strip()
    ]
    layer_first_len_values = [
        safe_float((row or {}).get("route_segment_total_length"))
        for row in layer_first_rows.values()
        if safe_float((row or {}).get("route_segment_total_length")) is not None
    ]
    layer_surface_count = sum(1 for row in layer_first_rows.values() if row)
    layer_signature_match = bool(layer_first_sig_values) and len(set(layer_first_sig_values)) == 1
    layer_length_match = bool(layer_first_len_values) and len({round(value, 6) for value in layer_first_len_values}) == 1
    layer_ts_span_ms = None
    if layer_first_ts_values:
        layer_ts_span_ms = (max(layer_first_ts_values) - min(layer_first_ts_values)) * 1000.0

    snapshot_files = snapshot_meta.get("files") or {}
    routing_info_meta = snapshot_files.get("routing.INFO") or {}

    planning_state = final_refresh.get("planning_state") or {}
    trigger_snapshot = final_refresh.get("trigger_condition_snapshot") or {}
    current_goal = final_refresh.get("current_goal") or {}
    startup_goal_validity = (startup_event or {}).get("goal_validity_snapshot") or {}
    transition_goal_validity = (transition_event or {}).get("goal_validity_snapshot") or {}
    startup_route_debug = (startup_event or {}).get("recent_route_debug_snapshot") or {}
    transition_route_debug = (transition_event or {}).get("recent_route_debug_snapshot") or {}
    startup_goal = (startup_decision or {}).get("current_goal") or {}
    transition_goal = (transition_decision or {}).get("current_goal") or {}
    startup_trigger = (startup_decision or {}).get("trigger_condition_snapshot") or {}
    transition_trigger = (transition_decision or {}).get("trigger_condition_snapshot") or {}

    return {
        "label": label,
        "run_dir": str(run_dir),
        "summary_status": summary.get("summary_status"),
        "fail_reason": summary.get("fail_reason"),
        "routing_goal_mode": bridge_summary.get("routing_goal_mode"),
        "routing_phase": bridge_summary.get("routing_phase"),
        "routing_freeze_active": bridge_summary.get("routing_freeze_active"),
        "routing_freeze_after_success": bridge_summary.get("routing_freeze_after_success"),
        "last_routing_send_ts": last_send_ts,
        "first_response_ts": first_response_ts,
        "final_refresh_ts": final_refresh_ts,
        "first_planning_ts": first_planning_ts,
        "first_route_debug_ts": first_route_debug_ts,
        "first_stage5_row_ts": first_stage5_ts,
        "first_route_segment_visible_ts": safe_float((first_route_segment_row or {}).get("timestamp")),
        "first_current_lane_visible_ts": safe_float((first_current_lane_row or {}).get("timestamp")),
        "first_planning_route_segment_ts": safe_float((first_planning_route_segment_row or {}).get("timestamp")),
        "first_planning_route_segment_signature": _text((first_planning_route_segment_row or {}).get("routing_lane_window_signature")),
        "first_planning_route_segment_total_length": safe_float((first_planning_route_segment_row or {}).get("route_segment_total_length")),
        "first_apollo_route_segment_ts": safe_float((first_apollo_route_segment_row or {}).get("timestamp")),
        "first_apollo_route_segment_signature": _text((first_apollo_route_segment_row or {}).get("routing_lane_window_signature")),
        "first_apollo_route_segment_total_length": safe_float((first_apollo_route_segment_row or {}).get("route_segment_total_length")),
        "first_apollo_reference_line_ts": safe_float((first_apollo_reference_line_row or {}).get("timestamp")),
        "first_apollo_reference_line_signature": _text((first_apollo_reference_line_row or {}).get("routing_lane_window_signature")),
        "first_apollo_reference_line_total_length": safe_float((first_apollo_reference_line_row or {}).get("route_segment_total_length")),
        "debug_layer_first_materialization_surface_count": layer_surface_count,
        "debug_layer_first_materialization_signature_match": layer_signature_match,
        "debug_layer_first_materialization_length_match": layer_length_match,
        "debug_layer_first_materialization_ts_span_ms": layer_ts_span_ms,
        "startup_goal_mode": _text(startup_goal.get("goal_mode") or (startup_event or {}).get("goal_mode")),
        "startup_goal_source": _text(startup_goal.get("goal_source") or (startup_event or {}).get("goal_source")),
        "startup_goal_distance_m": safe_float(startup_goal.get("goal_distance_m") or (startup_event or {}).get("goal_distance_m")),
        "startup_invalid_goal": startup_goal_validity.get("invalid_goal"),
        "startup_invalid_goal_reason": _text(startup_goal_validity.get("invalid_goal_reason")),
        "startup_route_segment_count": safe_int(startup_goal_validity.get("route_segment_count")),
        "startup_reference_line_count": safe_int(startup_goal_validity.get("reference_line_count")),
        "startup_route_debug_signature": _text(startup_route_debug.get("routing_lane_window_signature")),
        "startup_route_debug_route_segment_count": safe_int(startup_route_debug.get("route_segment_count")),
        "startup_route_debug_reference_line_count": safe_int(startup_route_debug.get("reference_line_count")),
        "startup_trigger_route_debug_ready": startup_trigger.get("route_debug_ready_for_long_phase"),
        "transition_goal_mode": _text(transition_goal.get("goal_mode") or (transition_event or {}).get("goal_mode")),
        "transition_goal_source": _text(transition_goal.get("goal_source") or (transition_event or {}).get("goal_source")),
        "transition_goal_distance_m": safe_float(transition_goal.get("goal_distance_m") or (transition_event or {}).get("goal_distance_m")),
        "transition_invalid_goal": transition_goal_validity.get("invalid_goal"),
        "transition_invalid_goal_reason": _text(transition_goal_validity.get("invalid_goal_reason")),
        "transition_route_segment_count": safe_int(transition_goal_validity.get("route_segment_count")),
        "transition_reference_line_count": safe_int(transition_goal_validity.get("reference_line_count")),
        "transition_route_debug_signature": _text(transition_route_debug.get("routing_lane_window_signature")),
        "transition_route_debug_route_segment_count": safe_int(transition_route_debug.get("route_segment_count")),
        "transition_route_debug_reference_line_count": safe_int(transition_route_debug.get("reference_line_count")),
        "transition_trigger_route_debug_ready": transition_trigger.get("route_debug_ready_for_long_phase"),
        "refresh_event_present": refresh_event is not None,
        "first_current_lane_id": _text((first_current_lane_row or {}).get("current_lane_id")),
        "first_current_lane_road_id": _text((first_current_lane_row or {}).get("current_lane_road_id")),
        "first_route_segment_reference_line_count": safe_int((first_route_segment_row or {}).get("reference_line_count")),
        "first_route_segment_count": safe_int((first_route_segment_row or {}).get("route_segment_count")),
        "first_route_segment_status": _text((first_route_segment_row or {}).get("lane_follow_map_status")),
        "first_route_segment_total_length": safe_float((first_route_segment_row or {}).get("route_segment_total_length")),
        "first_route_segment_routing_road_count": safe_int((first_route_segment_row or {}).get("routing_road_count")),
        "first_route_segment_routing_lane_window_signature": _text(
            (first_route_segment_row or {}).get("routing_lane_window_signature")
        ),
        "last_pre_refresh_route_segment_total_length": safe_float((last_pre_refresh_route_segment_row or {}).get("route_segment_total_length")),
        "last_pre_refresh_route_segment_routing_lane_window_signature": _text(
            (last_pre_refresh_route_segment_row or {}).get("routing_lane_window_signature")
        ),
        "last_pre_refresh_lane_follow_map_status": _text((last_pre_refresh_route_segment_row or {}).get("lane_follow_map_status")),
        "last_pre_refresh_route_segment_before_refresh_sec": _dt(
            safe_float((last_pre_refresh_route_segment_row or {}).get("timestamp")), final_refresh_ts
        ),
        "first_post_refresh_route_segment_total_length": safe_float((first_post_refresh_route_segment_row or {}).get("route_segment_total_length")),
        "first_post_refresh_route_segment_routing_lane_window_signature": _text(
            (first_post_refresh_route_segment_row or {}).get("routing_lane_window_signature")
        ),
        "first_post_refresh_lane_follow_map_status": _text((first_post_refresh_route_segment_row or {}).get("lane_follow_map_status")),
        "first_post_refresh_route_segment_after_refresh_sec": _dt(
            final_refresh_ts, safe_float((first_post_refresh_route_segment_row or {}).get("timestamp"))
        ),
        "first_response_after_send_sec": _dt(last_send_ts, first_response_ts),
        "final_refresh_after_send_sec": _dt(last_send_ts, final_refresh_ts),
        "first_planning_after_send_sec": _dt(last_send_ts, first_planning_ts),
        "first_route_debug_after_send_sec": _dt(last_send_ts, first_route_debug_ts),
        "first_stage5_after_send_sec": _dt(last_send_ts, first_stage5_ts),
        "first_route_segment_visible_after_send_sec": _dt(last_send_ts, safe_float((first_route_segment_row or {}).get("timestamp"))),
        "first_planning_after_response_sec": _dt(first_response_ts, first_planning_ts),
        "first_route_debug_after_response_sec": _dt(first_response_ts, first_route_debug_ts),
        "first_stage5_after_response_sec": _dt(first_response_ts, first_stage5_ts),
        "first_route_segment_visible_after_response_sec": _dt(
            first_response_ts, safe_float((first_route_segment_row or {}).get("timestamp"))
        ),
        "first_route_segment_visible_after_transition_sec": _dt(
            transition_ts, safe_float((first_route_segment_row or {}).get("timestamp"))
        ),
        "first_route_segment_visible_before_final_refresh_sec": _dt(
            safe_float((first_route_segment_row or {}).get("timestamp")), final_refresh_ts
        ),
        "first_planning_after_final_refresh_sec": _dt(final_refresh_ts, first_planning_ts),
        "first_route_debug_after_final_refresh_sec": _dt(final_refresh_ts, first_route_debug_ts),
        "first_stage5_after_final_refresh_sec": _dt(final_refresh_ts, first_stage5_ts),
        "first_route_segment_visible_after_final_refresh_sec": _dt(
            final_refresh_ts, safe_float((first_route_segment_row or {}).get("timestamp"))
        ),
        "planning_rows_before_response": planning_before_response,
        "stage5_rows_before_response": stage5_before_response,
        "route_segment_rows_before_response": route_segments_before_response,
        "planning_rows_before_final_refresh": planning_before_final_refresh,
        "stage5_rows_before_final_refresh": stage5_before_final_refresh,
        "route_segment_rows_before_final_refresh": route_segments_before_final_refresh,
        "final_refresh_goal_source": _text(current_goal.get("goal_source")),
        "final_refresh_goal_mode": _text(current_goal.get("goal_mode")),
        "final_refresh_goal_distance_m": safe_float(current_goal.get("goal_distance_m")),
        "final_refresh_route_debug_ready_for_long_phase": trigger_snapshot.get("route_debug_ready_for_long_phase"),
        "final_refresh_recent_route_debug_fresh": trigger_snapshot.get("recent_route_debug_fresh"),
        "final_refresh_recent_route_debug_age_sec": trigger_snapshot.get("recent_route_debug_age_sec"),
        "final_refresh_reference_line_count_snapshot": trigger_snapshot.get("reference_line_count"),
        "final_refresh_route_segment_count_snapshot": trigger_snapshot.get("route_segment_count"),
        "final_refresh_lane_follow_map_status_snapshot": _text(trigger_snapshot.get("lane_follow_map_status")),
        "final_refresh_invalid_goal": trigger_snapshot.get("invalid_goal"),
        "final_refresh_invalid_goal_reason": _text(trigger_snapshot.get("invalid_goal_reason")),
        "final_refresh_planning_msg_count_snapshot": planning_state.get("planning_msg_count"),
        "final_refresh_last_route_debug_event_seen": planning_state.get("last_route_debug_event_seen"),
        "final_refresh_last_route_debug_reference_line_count": planning_state.get("last_route_debug_reference_line_count"),
        "final_refresh_last_route_debug_route_segment_count": planning_state.get("last_route_debug_route_segment_count"),
        "routing_info_snapshot_status": _text(routing_info_meta.get("status")),
        "routing_info_snapshot_bytes": safe_int(routing_info_meta.get("bytes")),
        "routing_info_snapshot_start_offset": safe_int(routing_info_meta.get("start_offset")),
        "routing_info_snapshot_end_offset": safe_int(routing_info_meta.get("end_offset")),
    }


def _fmt_num(value: Any, digits: int = 3) -> str:
    num = safe_float(value)
    if num is None:
        return ""
    return f"{num:.{digits}f}"


def _fmt_sec(value: Any, digits: int = 3, empty: str = "none") -> str:
    text = _fmt_num(value, digits=digits)
    if not text:
        return empty
    return f"{text}s"


def _render_report(snapshots: Sequence[Dict[str, Any]], *, generated_at_local: str) -> str:
    lines: List[str] = [
        "# Calibration Post-Response Readiness Timeline Weekend",
        "",
        f"- generated_at_local: `{generated_at_local}`",
        "",
        "## Scope",
        "",
    ]
    for snapshot in snapshots:
        lines.append(f"- `{snapshot['label']}`: `{snapshot['run_dir']}`")

    lines.extend(
        [
            "",
            "## Event Timeline",
            "",
            "| label | goal_mode | response_after_send | final_refresh_after_send | first_planning_after_send | first_stage5_after_send | first_planning_after_response | first_stage5_after_response | planning_before_response | stage5_before_response | stage5_before_final_refresh | final_refresh_route_debug_ready | final_refresh_planning_msg_snapshot |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            "| "
            + " | ".join(
                [
                    snapshot["label"],
                    _text(snapshot.get("routing_goal_mode")),
                    _fmt_num(snapshot.get("first_response_after_send_sec")),
                    _fmt_num(snapshot.get("final_refresh_after_send_sec")),
                    _fmt_num(snapshot.get("first_planning_after_send_sec")),
                    _fmt_num(snapshot.get("first_stage5_after_send_sec")),
                    _fmt_num(snapshot.get("first_planning_after_response_sec")),
                    _fmt_num(snapshot.get("first_stage5_after_response_sec")),
                    _text(snapshot.get("planning_rows_before_response")),
                    _text(snapshot.get("stage5_rows_before_response")),
                    _text(snapshot.get("stage5_rows_before_final_refresh")),
                    _text(snapshot.get("final_refresh_route_debug_ready_for_long_phase")),
                    _text(snapshot.get("final_refresh_planning_msg_count_snapshot")),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Key Findings",
            "",
            "- The post-response split is now measurable as a timeline rather than a single latency number:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: response-after-send=`{_fmt_num(snapshot.get('first_response_after_send_sec'))}s`, "
            f"first-planning-after-response=`{_fmt_num(snapshot.get('first_planning_after_response_sec'))}s`, "
            f"stage5-before-response=`{snapshot.get('stage5_rows_before_response')}`"
        )
    lines.extend(
        [
            "- The startup and long-phase-transition reroute payloads are still qualitatively shared across this truthful pair:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}` startup=`{snapshot.get('startup_goal_mode')}/{snapshot.get('startup_goal_source')}` "
            f"transition=`{snapshot.get('transition_goal_mode')}/{snapshot.get('transition_goal_source')}` "
            f"with startup `route_segment_count={snapshot.get('startup_route_segment_count')}`, "
            f"transition `route_segment_count={snapshot.get('transition_route_segment_count')}`, "
            f"and route-debug signatures `startup={snapshot.get('startup_route_debug_signature') or 'none'}`, "
            f"`transition={snapshot.get('transition_route_debug_signature') or 'none'}`"
        )
    lines.extend(
        [
            "- The earliest concrete split only becomes visible at the long-phase-refresh boundary:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: "
            f"`first_route_segment_after_transition={_fmt_sec(snapshot.get('first_route_segment_visible_after_transition_sec'))}`, "
            f"`first_route_segment_before_final_refresh={_fmt_sec(snapshot.get('first_route_segment_visible_before_final_refresh_sec'))}`, "
            f"`pre_refresh_window={snapshot.get('last_pre_refresh_route_segment_routing_lane_window_signature') or 'none'}`, "
            f"`post_refresh_window={snapshot.get('first_post_refresh_route_segment_routing_lane_window_signature') or 'none'}`, "
            f"`final_refresh_goal_mode={snapshot.get('final_refresh_goal_mode')}`, "
            f"`final_refresh_goal_source={snapshot.get('final_refresh_goal_source')}`, "
            f"`final_refresh_route_segment_count={snapshot.get('final_refresh_route_segment_count_snapshot')}`, "
            f"`final_refresh_invalid_goal={snapshot.get('final_refresh_invalid_goal')}`"
        )
    lines.extend(
        [
            "- The first observable stage5 row is not what separates the branches in this pair:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: first stage5 row has "
            f"`reference_line_count={snapshot.get('first_route_segment_reference_line_count')}`, "
            f"`route_segment_count={snapshot.get('first_route_segment_count')}`, "
            f"`route_segment_total_length={snapshot.get('first_route_segment_total_length')}`, "
            f"`routing_road_count={snapshot.get('first_route_segment_routing_road_count')}`, "
            f"`lane_follow_map_status={snapshot.get('first_route_segment_status')}`"
        )
    lines.extend(
        [
            "- The split is not between the four route-debug surfaces inside either run:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: "
            f"`surface_count={snapshot.get('debug_layer_first_materialization_surface_count')}`, "
            f"`signature_match={snapshot.get('debug_layer_first_materialization_signature_match')}`, "
            f"`length_match={snapshot.get('debug_layer_first_materialization_length_match')}`, "
            f"`ts_span_ms={_fmt_num(snapshot.get('debug_layer_first_materialization_ts_span_ms'))}`, "
            f"`planning_route_segment={snapshot.get('first_planning_route_segment_signature') or 'none'}`, "
            f"`apollo_route_segment={snapshot.get('first_apollo_route_segment_signature') or 'none'}`, "
            f"`apollo_reference_line={snapshot.get('first_apollo_reference_line_signature') or 'none'}`, "
            f"`stage5={snapshot.get('first_route_segment_routing_lane_window_signature') or 'none'}`"
        )
    lines.extend(
        [
            "- `apollo_routing.INFO` is also not equally comparable across the response-aware pair:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: "
            f"`routing_snapshot_status={snapshot.get('routing_info_snapshot_status') or 'none'}`, "
            f"`routing_snapshot_bytes={snapshot.get('routing_info_snapshot_bytes')}`, "
            f"`start_offset={snapshot.get('routing_info_snapshot_start_offset')}`, "
            f"`end_offset={snapshot.get('routing_info_snapshot_end_offset')}`"
        )
    lines.extend(
        [
            "- The sharper split is whether any planning / stage5 / route-segment rows already exist before the response and final-refresh boundaries:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: "
            f"`planning_before_response={snapshot.get('planning_rows_before_response')}`, "
            f"`stage5_before_response={snapshot.get('stage5_rows_before_response')}`, "
            f"`route_segment_before_final_refresh={snapshot.get('route_segment_rows_before_final_refresh')}`"
        )
    lines.extend(
        [
            "- The final-refresh state has also diverged by then:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: "
            f"`route_debug_ready_for_long_phase={snapshot.get('final_refresh_route_debug_ready_for_long_phase')}`, "
            f"`recent_route_debug_fresh={snapshot.get('final_refresh_recent_route_debug_fresh')}`, "
            f"`goal_mode={snapshot.get('final_refresh_goal_mode')}`, "
            f"`goal_distance_m={snapshot.get('final_refresh_goal_distance_m')}`, "
            f"`invalid_goal={snapshot.get('final_refresh_invalid_goal')}`, "
            f"`invalid_goal_reason={snapshot.get('final_refresh_invalid_goal_reason')}`, "
            f"`lane_follow_map_status={snapshot.get('final_refresh_lane_follow_map_status_snapshot')}`"
        )
    lines.extend(
        [
            "- The first visible row family is also not the same across the pair:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: "
            f"`first_route_segment_total_length={snapshot.get('first_route_segment_total_length')}`, "
            f"`routing_lane_window_signature={snapshot.get('first_route_segment_routing_lane_window_signature')}`"
        )
    lines.extend(
        [
            "- That means the current truthful calibration blocker is narrower than `reference line still zero`:",
            "  the long branch already accumulates planning / stage5 / route-segment evidence before the response and final-refresh boundaries,",
            "  while the short branch has accumulated none of those rows yet and has not even reached the same invalid-goal / route-debug-ready boundary state.",
            "- It also means the long branch's pre-boundary rows are not simply an earlier arrival of the same row family the short branch later sees:",
            "  the long branch first surfaces a short startup-like lane-local segment,",
            "  while the short branch's first visible row only appears later and already carries the longer scenario-xy lane-local segment.",
            "- In other words, the truthful split does not first appear in startup or transition reroute validity payloads.",
            "  It first appears between long-phase transition and long-phase refresh, where the long branch has already materialized a short visible route window and the short branch still has none.",
            "- The refresh boundary itself also now has a concrete window pivot:",
            "  long holds a short pre-refresh window and then flips almost immediately to a longer fallback-ahead window after refresh,",
            "  while short has no pre-refresh window at all and only surfaces its longer scenario-xy window well after refresh.",
            "",
            "## Per-Run Read",
            "",
        ]
    )
    for snapshot in snapshots:
        lines.extend(
            [
                f"### `{snapshot['label']}`",
                "",
                f"- summary: `status={snapshot.get('summary_status')}, fail_reason={snapshot.get('fail_reason')}`",
                f"- branch: `goal_mode={snapshot.get('routing_goal_mode')}`, `routing_phase={snapshot.get('routing_phase')}`, "
                f"`freeze_active={snapshot.get('routing_freeze_active')}`, `freeze_after_success={snapshot.get('routing_freeze_after_success')}`",
                f"- send / response / final_refresh: "
                f"`send=0.000s`, `response={_fmt_num(snapshot.get('first_response_after_send_sec'))}s`, "
                f"`final_refresh={_fmt_num(snapshot.get('final_refresh_after_send_sec'))}s`",
                f"- startup / transition payloads: "
                f"`startup={snapshot.get('startup_goal_mode')}/{snapshot.get('startup_goal_source')}`, "
                f"`startup_invalid_goal={snapshot.get('startup_invalid_goal')}`, "
                f"`startup_route_segment_count={snapshot.get('startup_route_segment_count')}`, "
                f"`startup_route_debug_signature={snapshot.get('startup_route_debug_signature') or 'none'}`, "
                f"`transition={snapshot.get('transition_goal_mode')}/{snapshot.get('transition_goal_source')}`, "
                f"`transition_invalid_goal={snapshot.get('transition_invalid_goal')}`, "
                f"`transition_route_segment_count={snapshot.get('transition_route_segment_count')}`, "
                f"`transition_route_debug_signature={snapshot.get('transition_route_debug_signature') or 'none'}`, "
                f"`refresh_event_present={snapshot.get('refresh_event_present')}`",
                f"- first planning / stage5 evidence: "
                f"`planning={_fmt_sec(snapshot.get('first_planning_after_send_sec'))}`, "
                f"`route_debug={_fmt_sec(snapshot.get('first_route_debug_after_send_sec'))}`, "
                f"`stage5={_fmt_sec(snapshot.get('first_stage5_after_send_sec'))}`, "
                f"`route_segment_visible={_fmt_sec(snapshot.get('first_route_segment_visible_after_send_sec'))}`",
                f"- first route-segment vs transition / refresh: "
                f"`after_transition={_fmt_sec(snapshot.get('first_route_segment_visible_after_transition_sec'))}`, "
                f"`before_final_refresh={_fmt_sec(snapshot.get('first_route_segment_visible_before_final_refresh_sec'))}`, "
                f"`last_pre_refresh_window={snapshot.get('last_pre_refresh_route_segment_routing_lane_window_signature') or 'none'}`, "
                f"`last_pre_refresh_before_refresh={_fmt_sec(snapshot.get('last_pre_refresh_route_segment_before_refresh_sec'))}`, "
                f"`first_post_refresh_window={snapshot.get('first_post_refresh_route_segment_routing_lane_window_signature') or 'none'}`, "
                f"`first_post_refresh_after_refresh={_fmt_sec(snapshot.get('first_post_refresh_route_segment_after_refresh_sec'))}`",
                f"- first stage5 row shape: "
                f"`refline={snapshot.get('first_route_segment_reference_line_count')}`, "
                f"`route_segment_count={snapshot.get('first_route_segment_count')}`, "
                f"`route_segment_total_length={snapshot.get('first_route_segment_total_length')}`, "
                f"`routing_road_count={snapshot.get('first_route_segment_routing_road_count')}`, "
                f"`routing_lane_window_signature={snapshot.get('first_route_segment_routing_lane_window_signature')}`, "
                f"`status={snapshot.get('first_route_segment_status')}`",
                f"- first materialized debug-layer alignment: "
                f"`planning_route_segment={snapshot.get('first_planning_route_segment_signature') or 'none'}@{_fmt_sec(_dt(snapshot.get('last_routing_send_ts'), snapshot.get('first_planning_route_segment_ts')) )}`, "
                f"`apollo_route_segment={snapshot.get('first_apollo_route_segment_signature') or 'none'}@{_fmt_sec(_dt(snapshot.get('last_routing_send_ts'), snapshot.get('first_apollo_route_segment_ts')) )}`, "
                f"`apollo_reference_line={snapshot.get('first_apollo_reference_line_signature') or 'none'}@{_fmt_sec(_dt(snapshot.get('last_routing_send_ts'), snapshot.get('first_apollo_reference_line_ts')) )}`, "
                f"`stage5={snapshot.get('first_route_segment_routing_lane_window_signature') or 'none'}@{_fmt_sec(_dt(snapshot.get('last_routing_send_ts'), snapshot.get('first_route_segment_visible_ts')) )}`, "
                f"`signature_match={snapshot.get('debug_layer_first_materialization_signature_match')}`, "
                f"`length_match={snapshot.get('debug_layer_first_materialization_length_match')}`, "
                f"`ts_span_ms={_fmt_num(snapshot.get('debug_layer_first_materialization_ts_span_ms'))}`",
                f"- rows before response / final refresh: "
                f"`planning_before_response={snapshot.get('planning_rows_before_response')}`, "
                f"`stage5_before_response={snapshot.get('stage5_rows_before_response')}`, "
                f"`route_segment_before_response={snapshot.get('route_segment_rows_before_response')}`, "
                f"`planning_before_final_refresh={snapshot.get('planning_rows_before_final_refresh')}`, "
                f"`stage5_before_final_refresh={snapshot.get('stage5_rows_before_final_refresh')}`",
                f"- routing log snapshot caveat: "
                f"`status={snapshot.get('routing_info_snapshot_status')}`, "
                f"`bytes={snapshot.get('routing_info_snapshot_bytes')}`, "
                f"`start_offset={snapshot.get('routing_info_snapshot_start_offset')}`, "
                f"`end_offset={snapshot.get('routing_info_snapshot_end_offset')}`",
                f"- final refresh snapshot: "
                f"`route_debug_ready_for_long_phase={snapshot.get('final_refresh_route_debug_ready_for_long_phase')}`, "
                f"`recent_route_debug_fresh={snapshot.get('final_refresh_recent_route_debug_fresh')}`, "
                f"`recent_route_debug_age_sec={snapshot.get('final_refresh_recent_route_debug_age_sec')}`, "
                f"`goal_mode={snapshot.get('final_refresh_goal_mode')}`, "
                f"`goal_source={snapshot.get('final_refresh_goal_source')}`, "
                f"`goal_distance_m={snapshot.get('final_refresh_goal_distance_m')}`, "
                f"`invalid_goal={snapshot.get('final_refresh_invalid_goal')}`, "
                f"`invalid_goal_reason={snapshot.get('final_refresh_invalid_goal_reason')}`, "
                f"`lane_follow_map_status={snapshot.get('final_refresh_lane_follow_map_status_snapshot')}`, "
                f"`reference_line_count={snapshot.get('final_refresh_reference_line_count_snapshot')}`, "
                f"`route_segment_count={snapshot.get('final_refresh_route_segment_count_snapshot')}`, "
                f"`planning_msg_snapshot={snapshot.get('final_refresh_planning_msg_count_snapshot')}`, "
                f"`last_route_debug_event_seen={snapshot.get('final_refresh_last_route_debug_event_seen')}`",
                "",
            ]
        )

    lines.extend(
        [
            "## Next Step",
            "",
            "- Compare the first post-send window as `rows already present` rather than only `first row latency`.",
            "- Prioritize any contract difference that allows the long branch to accumulate stage5/route-segment rows before final refresh while the short branch still has zero rows at that boundary.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze short-vs-long post-response readiness timelines across calibration runs.")
    parser.add_argument(
        "--case",
        action="append",
        required=True,
        help="Run label and directory, formatted as LABEL=RUN_DIR. Can be repeated.",
    )
    parser.add_argument("--report", required=True)
    parser.add_argument("--summary-csv", required=True)
    args = parser.parse_args()

    snapshots: List[Dict[str, Any]] = []
    for item in args.case:
        if "=" not in item:
            raise SystemExit(f"invalid --case, expected LABEL=RUN_DIR: {item}")
        label, run_dir_text = item.split("=", 1)
        snapshots.append(_snapshot(label.strip(), Path(run_dir_text).expanduser().resolve()))

    generated_at_local = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    report_path = Path(args.report).expanduser().resolve()
    csv_path = Path(args.summary_csv).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    report_path.write_text(_render_report(snapshots, generated_at_local=generated_at_local), encoding="utf-8")

    rows = [dict(snapshot) for snapshot in snapshots]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(
        "[calibration-post-response-readiness] "
        f"cases={len(rows)} "
        f"report={report_path} "
        f"summary_csv={csv_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
