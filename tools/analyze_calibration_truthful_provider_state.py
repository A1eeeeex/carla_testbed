#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


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


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
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


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    return out


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _fmt_float(value: Any, digits: int = 3) -> str:
    out = _safe_float(value)
    if out is None:
        return ""
    return f"{out:.{digits}f}"


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


def _compress_rows(
    rows: Sequence[Dict[str, Any]],
    keys: Sequence[str],
    *,
    round_float_keys: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    rounded = set(round_float_keys or [])
    events: List[Dict[str, Any]] = []
    prev_sig: Optional[Tuple[Any, ...]] = None
    for idx, row in enumerate(rows):
        sig_parts: List[Any] = []
        payload: Dict[str, Any] = {"frame_index": idx}
        for key in keys:
            value = row.get(key)
            if isinstance(value, str) and value.strip().lower() == "none":
                value = None
            if key in rounded:
                value = _safe_float(value)
                if value is not None:
                    value = round(float(value), 3)
            payload[key] = value
            sig_parts.append(value)
        sig = tuple(sig_parts)
        if sig == prev_sig:
            continue
        events.append(payload)
        prev_sig = sig
    return events


def _event_signature(events: Sequence[Dict[str, Any]], keys: Sequence[str]) -> str:
    parts: List[str] = []
    for row in events:
        bits: List[str] = []
        for key in keys:
            value = row.get(key)
            if value is None:
                continue
            bits.append(f"{key}={value}")
        frame_index = row.get("frame_index")
        parts.append(f"@{frame_index}: " + ", ".join(bits))
    return " | ".join(parts) if parts else "none"


def _provider_state_signature(
    stage5_rows: Sequence[Dict[str, Any]],
    map_guard: Dict[str, Any],
) -> str:
    if not stage5_rows:
        return "missing_stage5_rows"
    last = stage5_rows[-1]
    bits = [
        f"create={last.get('create_route_segments_status') or ''}",
        f"provider={last.get('reference_line_provider_status') or ''}",
        f"empty={last.get('planning_empty_reason_guess') or ''}",
        f"lane_follow={last.get('lane_follow_map_status') or ''}",
        f"contract={map_guard.get('mismatch_classification') or ''}",
    ]
    return "|".join(bits)


def _counter_string(rows: Sequence[Dict[str, Any]], key: str) -> str:
    counts = Counter(str(row.get(key) or "") for row in rows)
    if not counts:
        return ""
    return ", ".join(f"{name}:{count}" for name, count in counts.most_common())


def _parse_routing_lane_windows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    pattern = re.compile(
        r"navigator\.cc:89\]\s+([^\s]+)\s+\d+\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)"
    )
    for line in path.read_text(errors="ignore").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        rows.append(
            {
                "lane_id": match.group(1),
                "start_s": _safe_float(match.group(2)),
                "end_s": _safe_float(match.group(3)),
            }
        )
    return rows


def _existing_path(path_text: Any) -> Optional[Path]:
    text = str(path_text or "").strip()
    if not text:
        return None
    try:
        path = Path(text).expanduser()
    except Exception:
        return None
    if not path.exists():
        return None
    return path.resolve()


def _select_routing_log_snapshot(
    artifacts: Path,
    snapshot_meta: Dict[str, Any],
) -> Dict[str, Any]:
    files = snapshot_meta.get("files") or {}
    routing_meta_raw = files.get("routing.INFO") if isinstance(files, dict) else {}
    routing_meta = routing_meta_raw if isinstance(routing_meta_raw, dict) else {}
    snapshot_path = _existing_path(routing_meta.get("snapshot")) or (artifacts / "apollo_routing.INFO")
    tail_path = _existing_path(routing_meta.get("tail_snapshot")) or (artifacts / "apollo_routing.INFO.tail")
    snapshot_exists = snapshot_path.exists()
    tail_exists = tail_path.exists()
    snapshot_status = str(routing_meta.get("status") or "")
    snapshot_bytes = _safe_int(routing_meta.get("bytes"))
    tail_bytes = _safe_int(routing_meta.get("tail_bytes"))
    tail_reason = str(routing_meta.get("tail_reason") or "")

    selected_path: Optional[Path] = None
    selected_source = "apollo_routing_info_missing"
    if tail_exists and (
        tail_reason == "delta_below_threshold"
        or not snapshot_exists
        or (
            snapshot_bytes is not None
            and tail_bytes is not None
            and tail_bytes > snapshot_bytes
        )
    ):
        selected_path = tail_path
        selected_source = "apollo_routing_info_tail"
    elif snapshot_exists:
        selected_path = snapshot_path
        if snapshot_status == "rotated":
            selected_source = "apollo_routing_info_rotated_snapshot"
        elif snapshot_status == "ok" and snapshot_bytes is not None and snapshot_bytes <= 8:
            selected_source = "apollo_routing_info_delta_snapshot"
        else:
            selected_source = "apollo_routing_info_snapshot"
    elif tail_exists:
        selected_path = tail_path
        selected_source = "apollo_routing_info_tail"

    return {
        "routing_snapshot_selected_path": str(selected_path) if selected_path else "",
        "routing_snapshot_selected_source": selected_source,
        "routing_snapshot_status": snapshot_status,
        "routing_snapshot_bytes": snapshot_bytes,
        "routing_snapshot_tail_present": _compact_bool(tail_exists),
        "routing_snapshot_tail_bytes": tail_bytes,
        "routing_snapshot_tail_reason": tail_reason,
    }


def _routing_lane_window_signature(rows: Sequence[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for row in rows:
        lane_id = str(row.get("lane_id") or "").strip()
        start_s = _fmt_float(row.get("start_s"), 4)
        end_s = _fmt_float(row.get("end_s"), 4)
        parts.append(f"{lane_id}@{start_s}->{end_s}")
    return " | ".join(parts) if parts else "none"


def _routing_unique_lane_signature(rows: Sequence[Dict[str, Any]]) -> str:
    parts: List[str] = []
    seen = set()
    for row in rows:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id or lane_id in seen:
            continue
        seen.add(lane_id)
        parts.append(lane_id)
    return " | ".join(parts) if parts else "none"


def _latest_runtime_routing_summary(
    rows: Sequence[Dict[str, Any]],
    *,
    source: str,
) -> Dict[str, Any]:
    for row in reversed(rows):
        lane_window_count = _safe_int(row.get("routing_lane_window_count")) or 0
        lane_window_signature = str(row.get("routing_lane_window_signature") or "").strip()
        unique_lane_count = _safe_int(row.get("routing_unique_lane_count")) or 0
        unique_lane_signature = str(row.get("routing_unique_lane_signature") or "").strip()
        if lane_window_count <= 0 and lane_window_signature.lower() in {"", "none"} and unique_lane_count <= 0:
            continue
        return {
            "routing_lane_window_source": source,
            "routing_lane_window_count": lane_window_count,
            "routing_lane_window_signature": (
                lane_window_signature if lane_window_signature and lane_window_signature.lower() != "none" else "none"
            ),
            "routing_unique_lane_count": unique_lane_count,
            "routing_unique_lane_signature": (
                unique_lane_signature
                if unique_lane_signature and unique_lane_signature.lower() != "none"
                else "none"
            ),
        }
    return {}


def _compact_bool(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return ""


def _compress_reroute_events(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    prev_sig: Optional[Tuple[Any, ...]] = None
    for idx, row in enumerate(rows):
        current_goal = row.get("current_goal") or {}
        pre_fallback_goal = row.get("pre_fallback_goal") or {}
        trigger = row.get("trigger_condition_snapshot") or {}
        seed_pose = row.get("seed_pose") or {}
        start_projection = row.get("start_projection") or {}
        goal_projection = row.get("goal_projection") or {}
        route_debug_snapshot = row.get("recent_route_debug_snapshot") or {}
        goal_validity = row.get("goal_validity_snapshot") or {}
        goal_resolution = row.get("goal_resolution") or {}
        planning_state = row.get("planning_state") or {}
        goal_distance = _safe_float(current_goal.get("goal_distance_m"))
        pre_fallback_goal_distance = _safe_float(pre_fallback_goal.get("goal_distance_m"))
        seed_to_current_distance = _safe_float(seed_pose.get("seed_to_current_distance_m"))
        start_projection_distance = _safe_float(start_projection.get("distance_m"))
        goal_projection_distance = _safe_float(goal_projection.get("distance_m"))
        planning_last_msg_age = _safe_float(planning_state.get("planning_last_msg_age_sec"))
        pre_fallback_goal_mode = (
            goal_resolution.get("pre_fallback_goal_mode")
            or pre_fallback_goal.get("goal_mode")
            or current_goal.get("requested_goal_mode")
            or goal_validity.get("requested_goal_mode")
        )
        invalid_goal = goal_resolution.get("invalid_goal_detected")
        if invalid_goal is None:
            invalid_goal = goal_validity.get("invalid_goal")
        fallback_applied = goal_resolution.get("fallback_applied")
        if fallback_applied is None:
            fallback_applied = goal_validity.get("fallback_applied")
        invalid_goal_reason = str(
            goal_resolution.get("invalid_goal_reason")
            or goal_validity.get("invalid_goal_reason")
            or ""
        )
        fallback_from_invalid_reason = str(
            goal_resolution.get("fallback_from_invalid_reason")
            or goal_validity.get("fallback_from_invalid_reason")
            or ""
        )
        payload = {
            "frame_index": idx,
            "routing_phase": row.get("routing_phase"),
            "reroute_reason": row.get("reroute_reason"),
            "trigger_source": row.get("trigger_source"),
            "pre_fallback_goal_mode": pre_fallback_goal_mode,
            "goal_mode": current_goal.get("goal_mode"),
            "anchor_mode": current_goal.get("anchor_mode"),
            "pre_fallback_goal_distance_m": (
                round(pre_fallback_goal_distance, 3)
                if pre_fallback_goal_distance is not None
                else None
            ),
            "goal_distance_m": round(goal_distance, 3) if goal_distance is not None else None,
            "seed_to_current_distance_m": (
                round(seed_to_current_distance, 3) if seed_to_current_distance is not None else None
            ),
            "start_projection_distance_m": (
                round(start_projection_distance, 6) if start_projection_distance is not None else None
            ),
            "goal_projection_distance_m": (
                round(goal_projection_distance, 6) if goal_projection_distance is not None else None
            ),
            "goal_projection_source_trusted": _compact_bool(
                goal_projection.get("source_trusted_lane_centerline")
            ),
            "invalid_goal": _compact_bool(invalid_goal),
            "invalid_goal_reason": invalid_goal_reason,
            "fallback_applied": _compact_bool(fallback_applied),
            "fallback_from_invalid_reason": fallback_from_invalid_reason,
            "route_debug_ready_for_long_phase": _compact_bool(
                trigger.get("route_debug_ready_for_long_phase")
            ),
            "recent_route_debug_fresh": _compact_bool(trigger.get("recent_route_debug_fresh")),
            "recent_route_debug_snapshot_available": _compact_bool(bool(route_debug_snapshot)),
            "planning_msg_count": _safe_int(planning_state.get("planning_msg_count")),
            "planning_last_points": _safe_int(planning_state.get("planning_last_points")),
            "planning_last_msg_age_sec": (
                round(planning_last_msg_age, 3) if planning_last_msg_age is not None else None
            ),
            "last_route_debug_event_seen": _compact_bool(
                planning_state.get("last_route_debug_event_seen")
            ),
        }
        sig = tuple(payload[key] for key in payload if key != "event_index")
        if sig == prev_sig:
            continue
        events.append(payload)
        prev_sig = sig
    return events


def _latest_reroute_observability_summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    last = rows[-1]
    current_goal = last.get("current_goal") or {}
    pre_fallback_goal = last.get("pre_fallback_goal") or {}
    trigger = last.get("trigger_condition_snapshot") or {}
    seed_pose = last.get("seed_pose") or {}
    start_projection = last.get("start_projection") or {}
    goal_projection = last.get("goal_projection") or {}
    goal_validity = last.get("goal_validity_snapshot") or {}
    goal_resolution = last.get("goal_resolution") or {}
    route_debug_snapshot = last.get("recent_route_debug_snapshot") or {}
    planning_state = last.get("planning_state") or {}
    pre_fallback_goal_mode = (
        goal_resolution.get("pre_fallback_goal_mode")
        or pre_fallback_goal.get("goal_mode")
        or current_goal.get("requested_goal_mode")
        or goal_validity.get("requested_goal_mode")
    )
    invalid_goal = goal_resolution.get("invalid_goal_detected")
    if invalid_goal is None:
        invalid_goal = goal_validity.get("invalid_goal")
    fallback_applied = goal_resolution.get("fallback_applied")
    if fallback_applied is None:
        fallback_applied = goal_validity.get("fallback_applied")
    return {
        "last_reroute_phase": last.get("routing_phase"),
        "last_reroute_reason": last.get("reroute_reason"),
        "last_reroute_trigger_source": last.get("trigger_source"),
        "last_reroute_pre_fallback_goal_mode": pre_fallback_goal_mode,
        "last_reroute_goal_mode": current_goal.get("goal_mode"),
        "last_reroute_anchor_mode": current_goal.get("anchor_mode"),
        "last_reroute_pre_fallback_goal_distance_m": _fmt_float(
            goal_resolution.get("pre_fallback_goal_distance_m")
            or pre_fallback_goal.get("goal_distance_m")
        ),
        "last_reroute_goal_distance_m": _fmt_float(current_goal.get("goal_distance_m")),
        "last_reroute_seed_to_current_distance_m": _fmt_float(
            seed_pose.get("seed_to_current_distance_m")
        ),
        "last_reroute_seed_heading_delta_deg": _fmt_float(
            seed_pose.get("seed_heading_delta_deg")
        ),
        "last_reroute_start_projection_distance_m": _fmt_float(
            start_projection.get("distance_m"),
            digits=6,
        ),
        "last_reroute_goal_projection_distance_m": _fmt_float(
            goal_projection.get("distance_m"),
            digits=6,
        ),
        "last_reroute_goal_projection_source_trusted": _compact_bool(
            goal_projection.get("source_trusted_lane_centerline")
        ),
        "last_reroute_goal_projection_available": _compact_bool(
            goal_projection.get("available")
        ),
        "last_reroute_invalid_goal": _compact_bool(invalid_goal),
        "last_reroute_invalid_goal_reason": str(
            goal_resolution.get("invalid_goal_reason")
            or goal_validity.get("invalid_goal_reason")
            or ""
        ),
        "last_reroute_fallback_applied": _compact_bool(fallback_applied),
        "last_reroute_fallback_from_invalid_reason": str(
            goal_resolution.get("fallback_from_invalid_reason")
            or goal_validity.get("fallback_from_invalid_reason")
            or ""
        ),
        "last_reroute_goal_projection_available_in_validity": _compact_bool(
            goal_validity.get("goal_projection_available")
        ),
        "last_reroute_goal_projection_distance_in_validity_m": _fmt_float(
            goal_validity.get("goal_projection_distance_m"),
            digits=6,
        ),
        "last_reroute_planning_msg_count": _safe_int(planning_state.get("planning_msg_count")),
        "last_reroute_planning_last_points": _safe_int(planning_state.get("planning_last_points")),
        "last_reroute_planning_first_nonempty_seen": _compact_bool(
            planning_state.get("planning_first_nonempty_seen")
        ),
        "last_reroute_planning_last_msg_age_sec": _fmt_float(
            planning_state.get("planning_last_msg_age_sec")
        ),
        "last_reroute_last_route_debug_event_seen": _compact_bool(
            planning_state.get("last_route_debug_event_seen")
        ),
        "last_reroute_route_debug_ready_for_long_phase": _compact_bool(
            trigger.get("route_debug_ready_for_long_phase")
        ),
        "last_reroute_recent_route_debug_fresh": _compact_bool(
            trigger.get("recent_route_debug_fresh")
        ),
        "last_reroute_recent_route_debug_age_sec": _fmt_float(
            trigger.get("recent_route_debug_age_sec")
        ),
        "last_reroute_recent_route_debug_snapshot_available": _compact_bool(
            bool(route_debug_snapshot)
        ),
        "last_reroute_recent_route_debug_lane_window_signature": str(
            route_debug_snapshot.get("routing_lane_window_signature") or ""
        ),
        "last_reroute_recent_route_debug_unique_lane_signature": str(
            route_debug_snapshot.get("routing_unique_lane_signature") or ""
        ),
        "last_reroute_recent_route_debug_route_segment_count": _safe_int(
            route_debug_snapshot.get("route_segment_count")
        ),
        "last_reroute_recent_route_debug_route_segment_total_length": _fmt_float(
            route_debug_snapshot.get("route_segment_total_length")
        ),
    }


def _analyze_run(label: str, run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    scenario_metadata = _load_json(artifacts / "scenario_metadata.json")
    map_guard = _load_json(artifacts / "map_contract_guard.json")
    bridge_health = _load_json(artifacts / "bridge_health_summary.json")
    planning_summary = _load_json(artifacts / "planning_topic_debug_summary.json")
    snapshot_meta = _load_json(artifacts / "apollo_log_snapshot_meta.json")
    stage5_rows = _load_jsonl(artifacts / "stage5_apollo_reference_line_debug.jsonl")
    planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
    routing_event_rows = _load_jsonl(artifacts / "routing_event_debug.jsonl")
    reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    routing_log_snapshot = _select_routing_log_snapshot(artifacts, snapshot_meta)
    routing_snapshot_path_text = str(routing_log_snapshot.get("routing_snapshot_selected_path") or "")
    routing_lane_windows = (
        _parse_routing_lane_windows(Path(routing_snapshot_path_text))
        if routing_snapshot_path_text
        else []
    )

    stage5_events = _compress_rows(
        stage5_rows,
        (
            "route_segment_count",
            "route_segment_total_length",
            "routing_lane_window_count",
            "routing_lane_window_signature",
            "reference_line_provider_status",
            "lane_follow_map_status",
            "reference_line_count",
            "current_lane_id",
        ),
        round_float_keys=("route_segment_total_length",),
    )
    planning_events = _compress_rows(
        planning_rows,
        (
            "routing_road_count",
            "routing_segment_count",
            "routing_lane_window_count",
            "routing_lane_window_signature",
            "reference_line_count",
            "lane_id_first",
            "target_lane_id_first",
        ),
    )
    reroute_events = _compress_reroute_events(reroute_rows)

    stage5_last = stage5_rows[-1] if stage5_rows else {}
    planning_last = planning_rows[-1] if planning_rows else {}
    provider_sig = _provider_state_signature(stage5_rows, map_guard)
    runtime_routing_summary = _latest_runtime_routing_summary(stage5_rows, source="stage5_runtime")
    if not runtime_routing_summary:
        runtime_routing_summary = _latest_runtime_routing_summary(planning_rows, source="planning_runtime")
    parsed_unique_lane_signature = _routing_unique_lane_signature(routing_lane_windows)
    routing_lane_window_summary = runtime_routing_summary or {
        "routing_lane_window_source": routing_log_snapshot["routing_snapshot_selected_source"],
        "routing_lane_window_count": len(routing_lane_windows),
        "routing_lane_window_signature": _routing_lane_window_signature(routing_lane_windows),
        "routing_unique_lane_count": (
            len(parsed_unique_lane_signature.split(" | ")) if parsed_unique_lane_signature != "none" else 0
        ),
        "routing_unique_lane_signature": parsed_unique_lane_signature,
    }
    reroute_observability_summary = _latest_reroute_observability_summary(reroute_rows)

    return {
        "label": label,
        "run_dir": str(run_dir),
        "candidate_signature": _candidate_signature(scenario_metadata),
        "candidate_road_id": (scenario_metadata.get("candidate") or {}).get("road_id"),
        "candidate_lane_id": (scenario_metadata.get("candidate") or {}).get("lane_id"),
        "candidate_start_y": _fmt_float((scenario_metadata.get("candidate") or {}).get("start_y")),
        "planning_nonempty_trajectory_count": int(
            bridge_health.get("planning_nonempty_trajectory_count", 0) or 0
        ),
        "planning_empty_trajectory_count": int(
            bridge_health.get("planning_empty_trajectory_count", 0) or 0
        ),
        "planning_total_messages_received": int(
            planning_summary.get("total_messages_received", 0) or 0
        ),
        "map_contract_invalid": bool(map_guard.get("map_contract_invalid", False)),
        "mismatch_classification": str(map_guard.get("mismatch_classification") or ""),
        "provider_state_signature": provider_sig,
        "final_route_segment_count": stage5_last.get("route_segment_count"),
        "final_route_segment_total_length": _fmt_float(stage5_last.get("route_segment_total_length")),
        "final_reference_line_count": stage5_last.get("reference_line_count"),
        "final_current_lane_id": stage5_last.get("current_lane_id"),
        "final_create_route_segments_status": stage5_last.get("create_route_segments_status"),
        "final_reference_line_provider_status": stage5_last.get("reference_line_provider_status"),
        "final_lane_follow_map_status": stage5_last.get("lane_follow_map_status"),
        "final_planning_empty_reason_guess": stage5_last.get("planning_empty_reason_guess"),
        "final_routing_road_count": planning_last.get("routing_road_count"),
        "final_routing_segment_count": planning_last.get("routing_segment_count"),
        "stage5_transition_count": len(stage5_events),
        "planning_transition_count": len(planning_events),
        "routing_event_count": len(routing_event_rows),
        "reroute_decision_count": len(reroute_rows),
        **routing_log_snapshot,
        **routing_lane_window_summary,
        **reroute_observability_summary,
        "stage5_transition_signature": _event_signature(
            stage5_events,
            (
                "route_segment_count",
                "route_segment_total_length",
                "routing_lane_window_count",
                "routing_lane_window_signature",
                "reference_line_provider_status",
                "lane_follow_map_status",
                "reference_line_count",
                "current_lane_id",
            ),
        ),
        "planning_transition_signature": _event_signature(
            planning_events,
            (
                "routing_road_count",
                "routing_segment_count",
                "routing_lane_window_count",
                "routing_lane_window_signature",
                "reference_line_count",
                "lane_id_first",
                "target_lane_id_first",
            ),
        ),
        "reroute_transition_signature": _event_signature(
            reroute_events,
            (
                "routing_phase",
                "reroute_reason",
                "trigger_source",
                "pre_fallback_goal_mode",
                "goal_mode",
                "anchor_mode",
                "pre_fallback_goal_distance_m",
                "goal_distance_m",
                "seed_to_current_distance_m",
                "start_projection_distance_m",
                "goal_projection_distance_m",
                "goal_projection_source_trusted",
                "invalid_goal",
                "invalid_goal_reason",
                "fallback_applied",
                "fallback_from_invalid_reason",
                "route_debug_ready_for_long_phase",
                "recent_route_debug_fresh",
                "recent_route_debug_snapshot_available",
                "planning_msg_count",
                "planning_last_points",
                "last_route_debug_event_seen",
            ),
        ),
        "stage5_provider_status_counts": _counter_string(stage5_rows, "reference_line_provider_status"),
        "stage5_lane_follow_status_counts": _counter_string(stage5_rows, "lane_follow_map_status"),
        "stage5_empty_reason_counts": _counter_string(stage5_rows, "planning_empty_reason_guess"),
        "stage5_transition_rows": stage5_events,
        "planning_transition_rows": planning_events,
        "reroute_transition_rows": reroute_events,
    }


def _report(rows: Sequence[Dict[str, Any]]) -> str:
    provider_sigs = {str(row.get("provider_state_signature") or "") for row in rows}
    lines = [
        "# Calibration Truthful Provider State Compare",
        "",
        "## Run Table",
        "",
        "| label | candidate | planning outcome | final stage5 topology | final routing topology | reroute observability | provider-state signature |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        planning_outcome = (
            f"nonempty={row['planning_nonempty_trajectory_count']}, "
            f"empty={row['planning_empty_trajectory_count']}, "
            f"msgs={row['planning_total_messages_received']}"
        )
        stage5_topology = (
            f"segments={row['final_route_segment_count']}, "
            f"length≈{row['final_route_segment_total_length'] or ''}, "
            f"ref={row['final_reference_line_count']}, "
            f"lane={row['final_current_lane_id'] or 'null'}"
        )
        planning_topology = (
            f"roads={row['final_routing_road_count']}, "
            f"segments={row['final_routing_segment_count']}"
        )
        reroute_summary = (
            f"events={row['reroute_decision_count']}, "
            f"last={row.get('last_reroute_reason') or 'none'}, "
            f"pre={row.get('last_reroute_pre_fallback_goal_mode') or ''}, "
            f"final={row.get('last_reroute_goal_mode') or ''}, "
            f"fb={row.get('last_reroute_fallback_applied') or ''}, "
            f"goal≈{row.get('last_reroute_goal_distance_m') or ''}, "
            f"inv={row.get('last_reroute_invalid_goal_reason') or ''}, "
            f"route_debug_ready={row.get('last_reroute_route_debug_ready_for_long_phase') or ''}"
        )
        lines.append(
            f"| `{row['label']}` | `{row['candidate_signature']}` | `{planning_outcome}` | "
            f"`{stage5_topology}` | `{planning_topology}` | `{reroute_summary}` | "
            f"`{row['provider_state_signature']}` |"
        )

    lines.extend(
        [
            "",
            "## Transition Comparison",
            "",
        ]
    )
    for row in rows:
        snapshot_bits: List[str] = []
        if row.get("routing_snapshot_selected_source"):
            snapshot_bits.append(f"selected={row.get('routing_snapshot_selected_source')}")
        if row.get("routing_snapshot_status"):
            snapshot_bits.append(f"status={row.get('routing_snapshot_status')}")
        if row.get("routing_snapshot_bytes") not in {"", None}:
            snapshot_bits.append(f"bytes={row.get('routing_snapshot_bytes')}")
        if row.get("routing_snapshot_tail_present") in {"true", "false"}:
            snapshot_bits.append(f"tail_present={row.get('routing_snapshot_tail_present')}")
        if row.get("routing_snapshot_tail_bytes") not in {"", None}:
            snapshot_bits.append(f"tail_bytes={row.get('routing_snapshot_tail_bytes')}")
        if row.get("routing_snapshot_tail_reason"):
            snapshot_bits.append(f"tail_reason={row.get('routing_snapshot_tail_reason')}")
        lines.extend(
            [
                f"### `{row['label']}`",
                "",
                f"- stage5_transitions: `{row['stage5_transition_signature']}`",
                f"- planning_transitions: `{row['planning_transition_signature']}`",
                (
                    f"- routing_lane_windows[{row['routing_lane_window_source']}]: "
                    f"`count={row['routing_lane_window_count']}, {row['routing_lane_window_signature']}`"
                ),
                (
                    f"- routing_unique_lanes[{row['routing_lane_window_source']}]: "
                    f"`count={row['routing_unique_lane_count']}, {row['routing_unique_lane_signature']}`"
                ),
                (
                    f"- routing_snapshot_meta: `{', '.join(snapshot_bits)}`"
                    if snapshot_bits
                    else "- routing_snapshot_meta: `none`"
                ),
                f"- reroute_transitions: `{row['reroute_transition_signature']}`",
                (
                    "- reroute_last_snapshot: "
                    f"`phase={row.get('last_reroute_phase') or ''}, "
                    f"reason={row.get('last_reroute_reason') or ''}, "
                    f"trigger={row.get('last_reroute_trigger_source') or ''}, "
                    f"pre_goal_mode={row.get('last_reroute_pre_fallback_goal_mode') or ''}, "
                    f"goal_mode={row.get('last_reroute_goal_mode') or ''}, "
                    f"anchor={row.get('last_reroute_anchor_mode') or ''}, "
                    f"pre_goal≈{row.get('last_reroute_pre_fallback_goal_distance_m') or ''}, "
                    f"goal≈{row.get('last_reroute_goal_distance_m') or ''}, "
                    f"seedΔ≈{row.get('last_reroute_seed_to_current_distance_m') or ''}, "
                    f"start_proj≈{row.get('last_reroute_start_projection_distance_m') or ''}, "
                    f"goal_proj≈{row.get('last_reroute_goal_projection_distance_m') or ''}, "
                    f"goal_proj_src_trusted={row.get('last_reroute_goal_projection_source_trusted') or ''}, "
                    f"invalid_goal={row.get('last_reroute_invalid_goal') or ''}, "
                    f"invalid_reason={row.get('last_reroute_invalid_goal_reason') or ''}, "
                    f"fallback={row.get('last_reroute_fallback_applied') or ''}, "
                    f"fallback_reason={row.get('last_reroute_fallback_from_invalid_reason') or ''}, "
                    f"planning_msgs={row.get('last_reroute_planning_msg_count') or ''}, "
                    f"planning_last_points={row.get('last_reroute_planning_last_points') or ''}, "
                    f"planning_msg_age≈{row.get('last_reroute_planning_last_msg_age_sec') or ''}, "
                    f"last_route_debug_seen={row.get('last_reroute_last_route_debug_event_seen') or ''}, "
                    f"route_debug_ready={row.get('last_reroute_route_debug_ready_for_long_phase') or ''}, "
                    f"route_debug_fresh={row.get('last_reroute_recent_route_debug_fresh') or ''}, "
                    f"route_debug_snapshot={row.get('last_reroute_recent_route_debug_snapshot_available') or ''}`"
                ),
                f"- provider_status_counts: `{row['stage5_provider_status_counts']}`",
                f"- lane_follow_status_counts: `{row['stage5_lane_follow_status_counts']}`",
                f"- empty_reason_counts: `{row['stage5_empty_reason_counts']}`",
                "",
            ]
        )

    lines.extend(
        [
            "## Conclusions",
            "",
        ]
    )
    if len(provider_sigs) == 1:
        lines.append(
            "- All compared truthful runs terminate in the same provider-state family; the remaining split is upstream topology, not a different provider failure class."
        )
    else:
        lines.append(
            "- Compared truthful runs do not share a single provider-state family; provider-state divergence is still present and needs separate diagnosis."
        )

    if rows:
        by_stage5 = {
            (row.get("final_route_segment_count"), row.get("final_route_segment_total_length")) for row in rows
        }
        if len(by_stage5) > 1:
            rendered = ", ".join(
                f"{seg} segments / ~{length}m" for seg, length in sorted(by_stage5, key=lambda item: str(item))
            )
            lines.append(
                f"- Final stage5 topology still splits across runs: `{rendered}`."
            )
        by_planning = {
            (row.get("final_routing_road_count"), row.get("final_routing_segment_count")) for row in rows
        }
        if len(by_planning) > 1:
            rendered = ", ".join(
                f"{road} roads / {seg} segments" for road, seg in sorted(by_planning, key=lambda item: str(item))
            )
            lines.append(
                f"- Planning-side routing topology also diverges: `{rendered}`."
            )
        by_lane_windows = {str(row.get("routing_lane_window_signature") or "") for row in rows}
        if len(by_lane_windows) > 1:
            lane_window_sources = {str(row.get("routing_lane_window_source") or "") for row in rows}
            if any(source.endswith("_runtime") for source in lane_window_sources):
                lines.append(
                    "- Runtime planning/stage5 lane-window signatures also diverge before provider failure, so the split is now directly visible in debug rows rather than only in `apollo_routing.INFO`."
                )
            else:
                lines.append(
                    "- Routing lane-window signatures also diverge before provider failure, which means the split is already visible in emitted routing windows rather than only in late stage5 summaries."
                )
        unstable_candidates = []
        by_candidate: Dict[str, set] = {}
        for row in rows:
            candidate_sig = str(row.get("candidate_signature") or "")
            by_candidate.setdefault(candidate_sig, set()).add(str(row.get("routing_lane_window_signature") or ""))
        for candidate_sig, signatures in by_candidate.items():
            clean_signatures = {sig for sig in signatures if sig}
            if len(clean_signatures) > 1:
                unstable_candidates.append(
                    f"{candidate_sig}: " + " vs ".join(sorted(clean_signatures))
                )
        if unstable_candidates:
            lines.append(
                "- Identical candidate families do not yet produce a single stable routing topology: "
                + "; ".join(f"`{item}`" for item in unstable_candidates)
                + "."
            )
        reroute_ready_values = {
            str(row.get("last_reroute_route_debug_ready_for_long_phase") or "")
            for row in rows
            if row.get("reroute_decision_count")
        }
        if reroute_ready_values == {"false"}:
            lines.append(
                "- In every run that emitted reroute decision artifacts, the final reroute fired while `route_debug_ready_for_long_phase=false`; the branch split is happening before recent route-debug state is considered ready."
            )
        ready_to_lengths: Dict[str, set] = {}
        for row in rows:
            ready_flag = str(row.get("last_reroute_route_debug_ready_for_long_phase") or "")
            if ready_flag not in {"true", "false"}:
                continue
            length = row.get("final_route_segment_total_length")
            if not length:
                continue
            ready_to_lengths.setdefault(ready_flag, set()).add(str(length))
        if {"true", "false"}.issubset(ready_to_lengths):
            false_lengths = ", ".join(f"~{length}m" for length in sorted(ready_to_lengths["false"]))
            true_lengths = ", ".join(f"~{length}m" for length in sorted(ready_to_lengths["true"]))
            lines.append(
                "- Final reroute readiness now correlates with branch family in the current truthful corpus: "
                f"`route_debug_ready_for_long_phase=false` aligns with `{false_lengths}`, while "
                f"`route_debug_ready_for_long_phase=true` aligns with `{true_lengths}`."
            )
        goal_mode_to_lengths: Dict[str, set] = {}
        for row in rows:
            goal_mode = str(row.get("last_reroute_goal_mode") or "")
            if not goal_mode:
                continue
            length = row.get("final_route_segment_total_length")
            if not length:
                continue
            goal_mode_to_lengths.setdefault(goal_mode, set()).add(str(length))
        if len(goal_mode_to_lengths) > 1:
            rendered = []
            for goal_mode in sorted(goal_mode_to_lengths):
                lengths = ", ".join(f"~{length}m" for length in sorted(goal_mode_to_lengths[goal_mode]))
                rendered.append(f"`{goal_mode}` -> {lengths}")
            lines.append(
                "- Final reroute goal mode also tracks the branch families in the current corpus: "
                + "; ".join(rendered)
                + "."
            )
        fallback_mode_pairs: Dict[str, set] = {}
        for row in rows:
            pair = (
                f"{row.get('last_reroute_pre_fallback_goal_mode') or ''}"
                f" -> {row.get('last_reroute_goal_mode') or ''}"
            )
            fallback_flag = str(row.get("last_reroute_fallback_applied") or "")
            if not pair.strip() or fallback_flag not in {"true", "false"}:
                continue
            fallback_mode_pairs.setdefault(fallback_flag, set()).add(pair)
        if fallback_mode_pairs.get("true") and fallback_mode_pairs.get("false"):
            lines.append(
                "- Final reroute split is now directly explainable at the goal-resolution layer: "
                f"`fallback_applied=true` aligns with `{', '.join(sorted(fallback_mode_pairs['true']))}`, while "
                f"`fallback_applied=false` aligns with `{', '.join(sorted(fallback_mode_pairs['false']))}`."
            )
        fallback_reasons = {
            str(row.get("last_reroute_fallback_from_invalid_reason") or "")
            for row in rows
            if str(row.get("last_reroute_fallback_applied") or "") == "true"
            and str(row.get("last_reroute_fallback_from_invalid_reason") or "")
        }
        if fallback_reasons:
            rendered = ", ".join(f"`{reason}`" for reason in sorted(fallback_reasons))
            lines.append(
                "- The current fallback-bearing truthful branch is being forced by explicit invalid-goal reasons, not a silent goal-mode flip: "
                + rendered
                + "."
            )
        route_debug_seen_by_mode: Dict[str, set] = {}
        for row in rows:
            goal_mode = str(row.get("last_reroute_goal_mode") or "")
            seen = str(row.get("last_reroute_last_route_debug_event_seen") or "")
            if goal_mode and seen in {"true", "false"}:
                route_debug_seen_by_mode.setdefault(goal_mode, set()).add(seen)
        if route_debug_seen_by_mode.get("scenario_xy") == {"false"} and "ego_seed_ahead" in route_debug_seen_by_mode:
            lines.append(
                "- In the current truthful corpus, the `scenario_xy` terminal branch still coincides with `last_route_debug_event_seen=false`, while the `ego_seed_ahead` branch already has route-debug evidence on the reroute boundary."
            )
    lines.extend(
        [
            "- The next minimal push should target route-segment content / seed-lane observability at the boundary where `create_route_segments_status=ready` is followed by `reference_line_provider_status=failed`.",
            "",
        ]
    )
    return "\n".join(lines)


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare truthful calibration provider-state runs.")
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

    _write_csv(Path(args.summary_csv).resolve(), [{k: v for k, v in row.items() if not k.endswith("_rows")} for row in rows])
    _write_text(Path(args.report).resolve(), _report(rows))


if __name__ == "__main__":
    main()
