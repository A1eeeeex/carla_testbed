#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_float, safe_int


def _first_row(rows: Sequence[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    for row in rows:
        if predicate(row):
            return row
    return None


def _last_row(rows: Sequence[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    selected: Optional[Dict[str, Any]] = None
    for row in rows:
        if predicate(row):
            selected = row
    return selected


def _nearest_row(rows: Sequence[Dict[str, Any]], target_ts: Optional[float]) -> Optional[Dict[str, Any]]:
    if target_ts is None:
        return None
    candidates = [row for row in rows if safe_float(row.get("timestamp")) is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda row: abs((safe_float(row.get("timestamp")) or 0.0) - target_ts))


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _dt(base_ts: Optional[float], row: Optional[Dict[str, Any]]) -> Optional[float]:
    if base_ts is None or not row:
        return None
    row_ts = safe_float(row.get("timestamp"))
    if row_ts is None:
        return None
    return row_ts - base_ts


def _delta_rows(later: Optional[Dict[str, Any]], earlier: Optional[Dict[str, Any]]) -> Optional[float]:
    if not later or not earlier:
        return None
    later_ts = safe_float(later.get("timestamp"))
    earlier_ts = safe_float(earlier.get("timestamp"))
    if later_ts is None or earlier_ts is None:
        return None
    return later_ts - earlier_ts


def _pct(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return float(num) / float(den)


def _row_has_lane_semantics(row: Optional[Dict[str, Any]]) -> bool:
    if not row:
        return False
    return any(
        bool(_safe_text(row.get(key)).strip())
        for key in (
            "lane_id_first",
            "target_lane_id_first",
            "current_lane_id",
            "target_lane_id",
        )
    )


def _row_primary_lane_road(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return ""
    for key in (
        "lane_id_first_road_id",
        "target_lane_id_first_road_id",
        "current_lane_road_id",
        "target_lane_road_id",
    ):
        value = _safe_text(row.get(key)).strip()
        if value:
            return value
    return ""


def _row_boundary_status_tuple(row: Optional[Dict[str, Any]]) -> Tuple[Optional[int], str, str, str, str]:
    if not row:
        return (None, "", "", "", "")
    return (
        safe_int(row.get("reference_line_count")),
        _safe_text(row.get("create_route_segments_status")),
        _safe_text(row.get("reference_line_provider_status")),
        _safe_text(row.get("lane_follow_map_status")),
        _safe_text(row.get("planning_empty_reason_guess")),
    )


def _build_snapshot(label: str, run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    stage5_rows = load_jsonl(run_dir / "artifacts" / "stage5_apollo_reference_line_debug.jsonl")
    apollo_route_segment_rows = load_jsonl(run_dir / "artifacts" / "apollo_route_segment_debug.jsonl")
    apollo_reference_line_rows = load_jsonl(run_dir / "artifacts" / "apollo_reference_line_debug.jsonl")
    lane_follow_rows = load_jsonl(run_dir / "artifacts" / "stage5_apollo_lane_follow_map_debug.jsonl")
    planning_rows = load_jsonl(run_dir / "artifacts" / "planning_topic_debug.jsonl")
    routing_event_rows = load_jsonl(run_dir / "artifacts" / "routing_event_debug.jsonl")
    reroute_rows = load_jsonl(run_dir / "artifacts" / "reroute_decision_debug.jsonl")
    goal_validity_rows = load_jsonl(run_dir / "artifacts" / "goal_validity_debug.jsonl")

    if not stage5_rows:
        raise SystemExit(f"missing stage5 rows: {run_dir}")
    if not apollo_route_segment_rows:
        raise SystemExit(f"missing apollo route segment rows: {run_dir}")
    if not apollo_reference_line_rows:
        raise SystemExit(f"missing apollo reference line rows: {run_dir}")
    if not lane_follow_rows:
        raise SystemExit(f"missing lane follow rows: {run_dir}")
    if not planning_rows:
        raise SystemExit(f"missing planning rows: {run_dir}")
    if not routing_event_rows:
        raise SystemExit(f"missing routing_event rows: {run_dir}")
    if not reroute_rows:
        raise SystemExit(f"missing reroute rows: {run_dir}")
    if not goal_validity_rows:
        raise SystemExit(f"missing goal_validity rows: {run_dir}")

    stage5_start_ts = safe_float(stage5_rows[0].get("timestamp"))
    planning_start_ts = safe_float(planning_rows[0].get("timestamp"))

    materialized_window_predicate = (
        lambda row: (safe_int(row.get("route_segment_count")) is not None and safe_int(row.get("route_segment_count")) > 0)
        or (safe_int(row.get("routing_road_count")) is not None and safe_int(row.get("routing_road_count")) > 0)
        or bool(str(row.get("current_lane_id") or "").strip())
        or bool(str(row.get("lane_id_first") or "").strip())
    )

    first_apollo_route_window = _first_row(apollo_route_segment_rows, materialized_window_predicate)
    first_apollo_reference_line_window = _first_row(apollo_reference_line_rows, materialized_window_predicate)
    first_lane_follow_window = _first_row(lane_follow_rows, materialized_window_predicate)

    first_stage5_route_segment = _first_row(
        stage5_rows, lambda row: safe_int(row.get("route_segment_count")) is not None and safe_int(row.get("route_segment_count")) > 0
    )
    first_stage5_current_lane = _first_row(stage5_rows, lambda row: bool(str(row.get("current_lane_id") or "").strip()))
    first_stage5_road11 = _first_row(stage5_rows, lambda row: str(row.get("current_lane_road_id") or "") == "11")
    first_stage5_road8 = _first_row(stage5_rows, lambda row: str(row.get("current_lane_road_id") or "") == "8")
    last_stage5 = stage5_rows[-1]

    first_planning_multi = _first_row(
        planning_rows, lambda row: safe_int(row.get("routing_road_count")) is not None and safe_int(row.get("routing_road_count")) > 1
    )
    first_planning_window = _first_row(planning_rows, materialized_window_predicate)
    first_planning_lane = _first_row(planning_rows, lambda row: bool(str(row.get("lane_id_first") or "").strip()))
    first_planning_road11 = _first_row(
        planning_rows,
        lambda row: str(row.get("lane_id_first_road_id") or row.get("current_lane_road_id") or "") == "11",
    )
    first_planning_road8 = _first_row(
        planning_rows,
        lambda row: str(row.get("lane_id_first_road_id") or row.get("current_lane_road_id") or "") == "8",
    )
    first_planning_road22 = _first_row(
        planning_rows, lambda row: safe_int(row.get("routing_road_count")) is not None and safe_int(row.get("routing_road_count")) >= 22
    )
    last_planning = planning_rows[-1]
    startup_route_event = _first_row(routing_event_rows, lambda row: str(row.get("reroute_reason") or "") == "startup_initial_route")
    startup_route_decision = _first_row(reroute_rows, lambda row: str(row.get("reroute_reason") or "") == "startup_initial_route")
    long_phase_transition_event = _first_row(
        routing_event_rows, lambda row: str(row.get("reroute_reason") or "") == "long_phase_transition"
    )
    long_phase_transition_decision = _first_row(
        reroute_rows, lambda row: str(row.get("reroute_reason") or "") == "long_phase_transition"
    )
    startup_goal_validity = _first_row(goal_validity_rows, lambda row: str(row.get("routing_phase") or "") == "startup")
    post_transition_planning_rows = [
        row
        for row in planning_rows
        if safe_float(row.get("timestamp")) is not None
        and safe_float((long_phase_transition_event or {}).get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= safe_float((long_phase_transition_event or {}).get("timestamp"))
    ]
    post_transition_stage5_rows = [
        row
        for row in stage5_rows
        if safe_float(row.get("timestamp")) is not None
        and safe_float((long_phase_transition_event or {}).get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= safe_float((long_phase_transition_event or {}).get("timestamp"))
    ]
    first_post_transition_stage5_current_lane = _first_row(
        post_transition_stage5_rows, lambda row: bool(_safe_text(row.get("current_lane_id")).strip())
    )
    first_post_transition_stage5_target_lane = _first_row(
        post_transition_stage5_rows, lambda row: bool(_safe_text(row.get("target_lane_id")).strip())
    )
    first_post_transition_planning_lane = _first_row(
        post_transition_planning_rows, lambda row: bool(_safe_text(row.get("lane_id_first")).strip())
    )
    first_post_transition_planning_target_lane = _first_row(
        post_transition_planning_rows, lambda row: bool(_safe_text(row.get("target_lane_id_first")).strip())
    )
    first_post_transition_planning_road22 = _first_row(
        post_transition_planning_rows,
        lambda row: safe_int(row.get("routing_road_count")) is not None and safe_int(row.get("routing_road_count")) >= 22,
    )
    first_post_transition_planning_row_before_road22 = _last_row(
        post_transition_planning_rows,
        lambda row: first_post_transition_planning_road22 is not None
        and safe_float(row.get("timestamp")) is not None
        and safe_float((first_post_transition_planning_road22 or {}).get("timestamp")) is not None
        and safe_float(row.get("timestamp")) < safe_float((first_post_transition_planning_road22 or {}).get("timestamp")),
    )
    first_post_transition_planning_lane_after_road22 = _first_row(
        post_transition_planning_rows,
        lambda row: first_post_transition_planning_road22 is not None
        and safe_float(row.get("timestamp")) is not None
        and safe_float((first_post_transition_planning_road22 or {}).get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= safe_float((first_post_transition_planning_road22 or {}).get("timestamp"))
        and (
            bool(_safe_text(row.get("lane_id_first")).strip())
            or bool(_safe_text(row.get("target_lane_id_first")).strip())
        ),
    )
    first_post_transition_stage5_row_at_road22 = _nearest_row(
        post_transition_stage5_rows,
        safe_float((first_post_transition_planning_road22 or {}).get("timestamp")),
    )
    first_post_transition_apollo_route_row_at_road22 = _nearest_row(
        apollo_route_segment_rows,
        safe_float((first_post_transition_planning_road22 or {}).get("timestamp")),
    )
    first_post_transition_apollo_reference_row_at_road22 = _nearest_row(
        apollo_reference_line_rows,
        safe_float((first_post_transition_planning_road22 or {}).get("timestamp")),
    )
    first_post_transition_lane_follow_row_at_road22 = _nearest_row(
        lane_follow_rows,
        safe_float((first_post_transition_planning_road22 or {}).get("timestamp")),
    )
    first_post_transition_stage5_lane_after_road22 = _first_row(
        post_transition_stage5_rows,
        lambda row: first_post_transition_planning_road22 is not None
        and safe_float(row.get("timestamp")) is not None
        and safe_float((first_post_transition_planning_road22 or {}).get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= safe_float((first_post_transition_planning_road22 or {}).get("timestamp"))
        and (
            bool(_safe_text(row.get("current_lane_id")).strip())
            or bool(_safe_text(row.get("target_lane_id")).strip())
        ),
    )
    post_transition_reference_line_nonzero_rows = [
        row for row in post_transition_stage5_rows if (safe_int(row.get("reference_line_count")) or 0) > 0
    ]
    post_transition_any_non8_current_lane = any(
        _safe_text(row.get("current_lane_road_id")) not in {"", "8"} for row in post_transition_stage5_rows
    )
    post_transition_any_non8_target_lane = any(
        _safe_text(row.get("target_lane_road_id")) not in {"", "8"} for row in post_transition_stage5_rows
    )
    post_transition_any_non8_planning_lane = any(
        _safe_text(row.get("lane_id_first_road_id")) not in {"", "8"} for row in post_transition_planning_rows
    )
    post_transition_any_non8_planning_target_lane = any(
        _safe_text(row.get("target_lane_id_first_road_id")) not in {"", "8"} for row in post_transition_planning_rows
    )
    post_transition_any_current_lane_junction = any(bool(row.get("current_lane_is_junction")) for row in post_transition_stage5_rows)
    post_transition_any_target_lane_junction = any(bool(row.get("target_lane_is_junction")) for row in post_transition_stage5_rows)
    post_transition_any_planning_lane_junction = any(bool(row.get("lane_id_first_is_junction")) for row in post_transition_planning_rows)
    post_transition_any_planning_target_lane_junction = any(
        bool(row.get("target_lane_id_first_is_junction")) for row in post_transition_planning_rows
    )

    route_segment_visible_rows = [
        row for row in stage5_rows if safe_int(row.get("route_segment_count")) is not None and safe_int(row.get("route_segment_count")) > 0
    ]
    route_segment_visible_refline_zero_rows = [
        row for row in route_segment_visible_rows if safe_int(row.get("reference_line_count")) is None or safe_int(row.get("reference_line_count")) == 0
    ]
    any_reference_line_nonzero = any((safe_int(row.get("reference_line_count")) or 0) > 0 for row in stage5_rows)

    first_stage5_route_segment_routing_road_count = safe_int((first_stage5_route_segment or {}).get("routing_road_count"))
    first_stage5_current_lane_routing_road_count = safe_int((first_stage5_current_lane or {}).get("routing_road_count"))
    onset_family = ""
    if (first_stage5_route_segment_routing_road_count or 0) > 1 and _safe_text((first_stage5_current_lane or {}).get("current_lane_road_id")) == "11":
        onset_family = "multi_road_onset_with_road11_anchor"
    elif (first_stage5_route_segment_routing_road_count or 0) == 1 and _safe_text((first_stage5_current_lane or {}).get("current_lane_road_id")) == "8":
        onset_family = "single_road_8_onset_then_later_preview_expansion"
    elif (first_stage5_route_segment_routing_road_count or 0) > 1:
        onset_family = "multi_road_onset"
    elif (first_stage5_route_segment_routing_road_count or 0) == 1:
        onset_family = "single_road_onset"

    startup_route_debug_snapshot = (startup_route_event or {}).get("recent_route_debug_snapshot") or {}
    startup_route_goal_validity = (startup_route_event or {}).get("goal_validity_snapshot") or {}
    startup_trigger_snapshot = (startup_route_decision or {}).get("trigger_condition_snapshot") or {}
    startup_start_projection = (startup_route_event or {}).get("start_projection") or {}
    startup_goal_projection = (startup_route_event or {}).get("goal_projection") or {}
    startup_current_goal = (startup_route_decision or {}).get("current_goal") or {}
    startup_goal_distance_m = safe_float((startup_route_event or {}).get("goal_distance_m"))
    first_apollo_route_window_total_length = safe_float((first_apollo_route_window or {}).get("route_segment_total_length"))
    startup_goal_vs_first_apollo_window_length_delta_m: Optional[float] = None
    if startup_goal_distance_m is not None and first_apollo_route_window_total_length is not None:
        startup_goal_vs_first_apollo_window_length_delta_m = (
            startup_goal_distance_m - first_apollo_route_window_total_length
        )
    startup_payload_family = ""
    if (
        _safe_text((startup_route_event or {}).get("goal_mode")) == "ego_seed_ahead"
        and _safe_text((startup_route_event or {}).get("goal_source")) == "startup_short_ahead"
        and not startup_route_goal_validity.get("invalid_goal")
        and (safe_int(startup_route_goal_validity.get("route_segment_count")) or 0) == 0
        and (safe_int(startup_route_goal_validity.get("reference_line_count")) or 0) == 0
        and _safe_text(startup_route_debug_snapshot.get("routing_lane_window_signature")) in {"", "none"}
    ):
        startup_payload_family = "shared_startup_goal_without_visible_route_window"

    earliest_split_boundary = "startup_reroute_payload"
    if startup_payload_family == "shared_startup_goal_without_visible_route_window" and first_apollo_route_window:
        earliest_split_boundary = "first_apollo_route_window_materialization"
    elif startup_payload_family == "shared_startup_goal_without_visible_route_window":
        earliest_split_boundary = "first_stage5_route_segment_window"

    startup_first_apollo_window_contract_family = ""
    startup_first_apollo_window_matches_goal_distance = False
    if (
        _safe_text((startup_route_event or {}).get("goal_mode")) == "ego_seed_ahead"
        and _safe_text((startup_route_event or {}).get("goal_source")) == "startup_short_ahead"
        and startup_goal_vs_first_apollo_window_length_delta_m is not None
        and abs(startup_goal_vs_first_apollo_window_length_delta_m) <= 0.1
    ):
        startup_first_apollo_window_matches_goal_distance = True
        first_window_routing_road_count = safe_int((first_apollo_route_window or {}).get("routing_road_count")) or 0
        if first_window_routing_road_count > 1:
            startup_first_apollo_window_contract_family = "startup_short_ahead_30m_crosses_road_boundary"
        elif first_window_routing_road_count == 1:
            startup_first_apollo_window_contract_family = "startup_short_ahead_30m_single_road"
        elif first_apollo_route_window:
            startup_first_apollo_window_contract_family = "startup_short_ahead_30m_materialized_without_road_count"

    apollo_stage5_sync_delta = _delta_rows(first_stage5_route_segment, first_apollo_route_window)
    apollo_reference_sync_delta = _delta_rows(first_apollo_reference_line_window, first_apollo_route_window)
    apollo_lane_follow_sync_delta = _delta_rows(first_lane_follow_window, first_apollo_route_window)
    apollo_planning_sync_delta = _delta_rows(first_planning_window, first_apollo_route_window)
    first_apollo_materialization_sync = all(
        delta is not None and abs(delta) <= 1e-6
        for delta in [
            apollo_stage5_sync_delta,
            apollo_reference_sync_delta,
            apollo_lane_follow_sync_delta,
            apollo_planning_sync_delta,
        ]
    )
    post_long_phase_upgrade_family = "no_long_phase_preview"
    if first_post_transition_planning_road22:
        if (
            post_transition_any_non8_current_lane
            or post_transition_any_non8_target_lane
            or post_transition_any_non8_planning_lane
            or post_transition_any_non8_planning_target_lane
            or post_transition_any_current_lane_junction
            or post_transition_any_target_lane_junction
            or post_transition_any_planning_lane_junction
            or post_transition_any_planning_target_lane_junction
        ):
            post_long_phase_upgrade_family = "deep_preview_with_semantic_upgrade"
        else:
            post_long_phase_upgrade_family = "deep_preview_without_semantic_upgrade"
    post_long_phase_lane_semantic_order_family = "no_long_phase_preview"
    if first_post_transition_planning_road22:
        road22_has_lane_semantics = bool(
            _safe_text((first_post_transition_planning_road22 or {}).get("lane_id_first")).strip()
            or _safe_text((first_post_transition_planning_road22 or {}).get("target_lane_id_first")).strip()
        )
        first_lane_before_road22 = (
            first_post_transition_planning_lane is not None
            and safe_float((first_post_transition_planning_lane or {}).get("timestamp")) is not None
            and safe_float((first_post_transition_planning_road22 or {}).get("timestamp")) is not None
            and safe_float((first_post_transition_planning_lane or {}).get("timestamp"))
            < safe_float((first_post_transition_planning_road22 or {}).get("timestamp"))
        )
        if road22_has_lane_semantics:
            post_long_phase_lane_semantic_order_family = "road22_preview_arrives_with_lane_semantics"
        elif first_lane_before_road22 and first_post_transition_planning_lane_after_road22:
            post_long_phase_lane_semantic_order_family = "road22_preview_blanks_existing_lane_semantics_then_road8_resumes"
        elif first_lane_before_road22:
            post_long_phase_lane_semantic_order_family = "road22_preview_blanks_existing_lane_semantics_without_recovery"
        elif first_post_transition_planning_lane_after_road22:
            post_long_phase_lane_semantic_order_family = "road22_preview_precedes_first_lane_semantics_then_road8_resumes"
        else:
            post_long_phase_lane_semantic_order_family = "road22_preview_without_any_lane_semantics"
    road22_boundary_semantic_alignment_family = "no_long_phase_preview"
    if first_post_transition_planning_road22:
        road22_stage5_has_semantics = bool(
            _safe_text((first_post_transition_stage5_row_at_road22 or {}).get("current_lane_id")).strip()
            or _safe_text((first_post_transition_stage5_row_at_road22 or {}).get("target_lane_id")).strip()
        )
        road22_planning_before_has_semantics = bool(
            _safe_text((first_post_transition_planning_row_before_road22 or {}).get("lane_id_first")).strip()
            or _safe_text((first_post_transition_planning_row_before_road22 or {}).get("target_lane_id_first")).strip()
        )
        road22_stage5_after_road = _safe_text(
            (first_post_transition_stage5_lane_after_road22 or {}).get("current_lane_road_id")
            or (first_post_transition_stage5_lane_after_road22 or {}).get("target_lane_road_id")
        )
        road22_planning_after_road = _safe_text(
            (first_post_transition_planning_lane_after_road22 or {}).get("lane_id_first_road_id")
            or (first_post_transition_planning_lane_after_road22 or {}).get("target_lane_id_first_road_id")
        )
        if road22_has_lane_semantics and road22_stage5_has_semantics:
            road22_boundary_semantic_alignment_family = "road22_boundary_aligned_planning_and_stage5"
        elif (
            road22_planning_before_has_semantics
            and not road22_has_lane_semantics
            and not road22_stage5_has_semantics
            and first_post_transition_planning_lane_after_road22
            and first_post_transition_stage5_lane_after_road22
            and road22_planning_after_road == "8"
            and road22_stage5_after_road == "8"
        ):
            road22_boundary_semantic_alignment_family = (
                "road22_boundary_blanks_planning_and_stage5_then_road8_resumes"
            )
        elif (
            not road22_planning_before_has_semantics
            and not road22_has_lane_semantics
            and not road22_stage5_has_semantics
            and first_post_transition_planning_lane_after_road22
            and first_post_transition_stage5_lane_after_road22
            and road22_planning_after_road == "8"
            and road22_stage5_after_road == "8"
        ):
            road22_boundary_semantic_alignment_family = (
                "road22_boundary_precedes_planning_and_stage5_then_road8_resumes"
            )
        elif road22_has_lane_semantics and not road22_stage5_has_semantics:
            road22_boundary_semantic_alignment_family = "road22_boundary_planning_only"
        elif not road22_has_lane_semantics and road22_stage5_has_semantics:
            road22_boundary_semantic_alignment_family = "road22_boundary_stage5_only"
        else:
            road22_boundary_semantic_alignment_family = "road22_boundary_semantics_missing"

    road22_boundary_surface_sync_span_ms: Optional[float] = None
    road22_boundary_surface_alignment_family = "no_long_phase_preview"
    road22_boundary_surface_status_family = "no_long_phase_preview"
    if first_post_transition_planning_road22:
        road22_surface_rows = [
            first_post_transition_planning_road22,
            first_post_transition_stage5_row_at_road22,
            first_post_transition_apollo_route_row_at_road22,
            first_post_transition_apollo_reference_row_at_road22,
            first_post_transition_lane_follow_row_at_road22,
        ]
        road22_surface_ts = [
            safe_float((row or {}).get("timestamp"))
            for row in road22_surface_rows
            if safe_float((row or {}).get("timestamp")) is not None
        ]
        if road22_surface_ts:
            road22_boundary_surface_sync_span_ms = (
                max(road22_surface_ts) - min(road22_surface_ts)
            ) * 1000.0
        road22_surface_primary_roads = [_row_primary_lane_road(row) for row in road22_surface_rows]
        road22_surface_has_semantics = [_row_has_lane_semantics(row) for row in road22_surface_rows]
        nonplanning_surface_rows = [
            first_post_transition_stage5_row_at_road22,
            first_post_transition_apollo_route_row_at_road22,
            first_post_transition_apollo_reference_row_at_road22,
            first_post_transition_lane_follow_row_at_road22,
        ]
        nonplanning_status_tuples = [_row_boundary_status_tuple(row) for row in nonplanning_surface_rows]
        nonplanning_status_values = {status for status in nonplanning_status_tuples if any(item not in {None, ""} for item in status)}
        if all(road == "11" for road in road22_surface_primary_roads if road) and all(road22_surface_has_semantics):
            road22_boundary_surface_alignment_family = "road22_boundary_all_surfaces_resolved_on_road11"
        elif all(not has_semantics for has_semantics in road22_surface_has_semantics):
            road22_boundary_surface_alignment_family = "road22_boundary_all_surfaces_blank_semantics"
        elif all(road == "8" for road in road22_surface_primary_roads if road) and any(road22_surface_has_semantics):
            road22_boundary_surface_alignment_family = "road22_boundary_all_surfaces_resolved_on_road8"
        else:
            road22_boundary_surface_alignment_family = "road22_boundary_surfaces_mixed"

        if len(nonplanning_status_values) == 1:
            refline_count, create_status, provider_status, lane_follow_status, empty_reason = next(iter(nonplanning_status_values))
            if (
                refline_count == 0
                and create_status == "ready"
                and provider_status == "failed"
                and lane_follow_status == "route_segments_present_reference_line_missing"
            ):
                road22_boundary_surface_status_family = "road22_boundary_nonplanning_surfaces_ref0_ready_failed_reference_line_missing"
            elif (
                refline_count == 0
                and create_status == "ready"
                and provider_status == "trajectory_nonzero_debug_missing"
                and lane_follow_status == "trajectory_nonzero_reference_line_debug_missing"
            ):
                road22_boundary_surface_status_family = "road22_boundary_nonplanning_surfaces_ref0_ready_trajectory_nonzero_debug_missing"
            else:
                road22_boundary_surface_status_family = (
                    "road22_boundary_nonplanning_surfaces_aligned_status_"
                    f"ref{refline_count if refline_count is not None else 'none'}_"
                    f"{create_status or 'none'}_{provider_status or 'none'}_{lane_follow_status or 'none'}_"
                    f"{empty_reason or 'none'}"
                )
        elif nonplanning_status_values:
            road22_boundary_surface_status_family = "road22_boundary_nonplanning_surfaces_mixed_status"

    return {
        "label": label,
        "run_dir": str(run_dir),
        "route_id": summary.get("route_id"),
        "comparison_label": summary.get("comparison_label"),
        "route_health_label": summary.get("route_health_label"),
        "route_completion_ratio": summary.get("route_completion_ratio"),
        "route_distance_achieved_m": summary.get("route_distance_achieved_m"),
        "path_fallback_count": summary.get("path_fallback_count"),
        "planning_nonempty_trajectory_count": summary.get("planning_nonempty_trajectory_count"),
        "stage5_start_ts": stage5_start_ts,
        "planning_start_ts": planning_start_ts,
        "first_apollo_route_window_dt_sec": _dt(stage5_start_ts, first_apollo_route_window),
        "first_apollo_route_window_after_startup_route_sec": _delta_rows(first_apollo_route_window, startup_route_event),
        "first_apollo_route_window_before_long_phase_transition_sec": _delta_rows(long_phase_transition_event, first_apollo_route_window),
        "first_apollo_route_window_route_segment_count": safe_int((first_apollo_route_window or {}).get("route_segment_count")),
        "first_apollo_route_window_total_length": safe_float((first_apollo_route_window or {}).get("route_segment_total_length")),
        "first_apollo_route_window_routing_road_count": safe_int((first_apollo_route_window or {}).get("routing_road_count")),
        "first_apollo_route_window_routing_unique_lane_signature": _safe_text(
            (first_apollo_route_window or {}).get("routing_unique_lane_signature")
        ),
        "first_apollo_route_window_routing_lane_window_signature": _safe_text(
            (first_apollo_route_window or {}).get("routing_lane_window_signature")
        ),
        "first_apollo_route_window_reference_line_count": safe_int((first_apollo_route_window or {}).get("reference_line_count")),
        "first_apollo_route_window_lane_follow_map_status": _safe_text((first_apollo_route_window or {}).get("lane_follow_map_status")),
        "first_apollo_route_window_create_route_segments_status": _safe_text(
            (first_apollo_route_window or {}).get("create_route_segments_status")
        ),
        "first_apollo_route_window_reference_line_provider_status": _safe_text(
            (first_apollo_route_window or {}).get("reference_line_provider_status")
        ),
        "first_stage5_route_segment_dt_sec": _dt(stage5_start_ts, first_stage5_route_segment),
        "first_stage5_current_lane_dt_sec": _dt(stage5_start_ts, first_stage5_current_lane),
        "first_stage5_road11_dt_sec": _dt(stage5_start_ts, first_stage5_road11),
        "first_stage5_road8_dt_sec": _dt(stage5_start_ts, first_stage5_road8),
        "startup_route_dt_sec": _dt(stage5_start_ts, startup_route_event),
        "long_phase_transition_dt_sec": _dt(stage5_start_ts, long_phase_transition_event),
        "first_route_segment_after_startup_route_sec": _delta_rows(first_stage5_route_segment, startup_route_event),
        "first_current_lane_after_startup_route_sec": _delta_rows(first_stage5_current_lane, startup_route_event),
        "first_route_segment_before_long_phase_transition_sec": _delta_rows(long_phase_transition_event, first_stage5_route_segment),
        "first_current_lane_before_long_phase_transition_sec": _delta_rows(long_phase_transition_event, first_stage5_current_lane),
        "last_stage5_dt_sec": _dt(stage5_start_ts, last_stage5),
        "first_stage5_route_segment_count": safe_int((first_stage5_route_segment or {}).get("route_segment_count")),
        "first_stage5_route_segment_total_length": safe_float((first_stage5_route_segment or {}).get("route_segment_total_length")),
        "first_stage5_route_segment_routing_road_count": first_stage5_route_segment_routing_road_count,
        "first_stage5_route_segment_routing_unique_lane_signature": _safe_text(
            (first_stage5_route_segment or {}).get("routing_unique_lane_signature")
        ),
        "first_stage5_route_segment_routing_lane_window_signature": _safe_text(
            (first_stage5_route_segment or {}).get("routing_lane_window_signature")
        ),
        "first_stage5_current_lane_id": _safe_text((first_stage5_current_lane or {}).get("current_lane_id")),
        "first_stage5_current_lane_road_id": _safe_text((first_stage5_current_lane or {}).get("current_lane_road_id")),
        "first_stage5_current_lane_routing_road_count": first_stage5_current_lane_routing_road_count,
        "first_stage5_current_lane_routing_unique_lane_signature": _safe_text(
            (first_stage5_current_lane or {}).get("routing_unique_lane_signature")
        ),
        "first_stage5_current_lane_routing_lane_window_signature": _safe_text(
            (first_stage5_current_lane or {}).get("routing_lane_window_signature")
        ),
        "first_stage5_lane_follow_map_status": _safe_text((first_stage5_current_lane or first_stage5_route_segment or {}).get("lane_follow_map_status")),
        "startup_goal_mode": _safe_text((startup_route_event or {}).get("goal_mode")),
        "startup_goal_source": _safe_text((startup_route_event or {}).get("goal_source")),
        "startup_goal_distance_m": startup_goal_distance_m,
        "startup_start_projection_signed_e_y_m": safe_float(startup_start_projection.get("signed_e_y_m")),
        "startup_start_projection_heading_diff_to_vehicle_deg_abs": safe_float(
            startup_start_projection.get("heading_diff_to_vehicle_deg_abs")
        ),
        "startup_start_projection_lane_yaw_deg": safe_float(startup_start_projection.get("lane_yaw_deg")),
        "startup_goal_projection_source": _safe_text(startup_goal_projection.get("source_type")),
        "startup_goal_projection_signed_e_y_m": safe_float(startup_goal_projection.get("signed_e_y_m")),
        "startup_goal_projection_distance_m": safe_float(startup_goal_projection.get("distance_m")),
        "startup_goal_vs_first_apollo_window_length_delta_m": startup_goal_vs_first_apollo_window_length_delta_m,
        "startup_first_apollo_window_matches_goal_distance": startup_first_apollo_window_matches_goal_distance,
        "startup_first_apollo_window_contract_family": startup_first_apollo_window_contract_family,
        "startup_goal_validity_invalid_goal": startup_route_goal_validity.get("invalid_goal"),
        "startup_goal_validity_route_segment_count": safe_int(startup_route_goal_validity.get("route_segment_count")),
        "startup_goal_validity_reference_line_count": safe_int(startup_route_goal_validity.get("reference_line_count")),
        "startup_goal_validity_routing_phase": _safe_text(startup_route_goal_validity.get("routing_phase")),
        "startup_goal_validity_row_route_segment_count": safe_int((startup_goal_validity or {}).get("route_segment_count")),
        "startup_goal_validity_row_reference_line_count": safe_int((startup_goal_validity or {}).get("reference_line_count")),
        "startup_recent_route_debug_route_segment_count": safe_int(startup_route_debug_snapshot.get("route_segment_count")),
        "startup_recent_route_debug_reference_line_count": safe_int(startup_route_debug_snapshot.get("reference_line_count")),
        "startup_recent_route_debug_lane_follow_map_status": _safe_text(startup_route_debug_snapshot.get("lane_follow_map_status")),
        "startup_recent_route_debug_routing_lane_window_signature": _safe_text(
            startup_route_debug_snapshot.get("routing_lane_window_signature")
        ),
        "startup_trigger_route_segment_count": safe_int(startup_trigger_snapshot.get("route_segment_count")),
        "startup_trigger_reference_line_count": safe_int(startup_trigger_snapshot.get("reference_line_count")),
        "startup_trigger_route_debug_ready": startup_trigger_snapshot.get("route_debug_ready_for_long_phase"),
        "startup_trigger_recent_route_debug_fresh": startup_trigger_snapshot.get("recent_route_debug_fresh"),
        "startup_trigger_recent_route_debug_age_sec": safe_float(startup_trigger_snapshot.get("recent_route_debug_age_sec")),
        "startup_trigger_lane_follow_map_status": _safe_text(startup_trigger_snapshot.get("lane_follow_map_status")),
        "startup_current_goal_mode": _safe_text(startup_current_goal.get("goal_mode")),
        "startup_current_goal_distance_m": safe_float(startup_current_goal.get("goal_distance_m")),
        "startup_payload_family": startup_payload_family,
        "earliest_split_boundary": earliest_split_boundary,
        "last_stage5_current_lane_id": _safe_text(last_stage5.get("current_lane_id")),
        "last_stage5_current_lane_road_id": _safe_text(last_stage5.get("current_lane_road_id")),
        "last_stage5_current_lane_is_junction": last_stage5.get("current_lane_is_junction"),
        "last_stage5_reference_line_count": safe_int(last_stage5.get("reference_line_count")),
        "last_stage5_route_segment_count": safe_int(last_stage5.get("route_segment_count")),
        "last_stage5_lane_follow_map_status": _safe_text(last_stage5.get("lane_follow_map_status")),
        "any_reference_line_nonzero": any_reference_line_nonzero,
        "route_segment_visible_rows": len(route_segment_visible_rows),
        "route_segment_visible_refline_zero_rows": len(route_segment_visible_refline_zero_rows),
        "route_segment_visible_refline_zero_ratio": _pct(
            len(route_segment_visible_refline_zero_rows), len(route_segment_visible_rows)
        ),
        "first_planning_multi_preview_dt_sec": _dt(planning_start_ts, first_planning_multi),
        "first_planning_road22_dt_sec": _dt(planning_start_ts, first_planning_road22),
        "first_planning_lane_dt_sec": _dt(planning_start_ts, first_planning_lane),
        "first_planning_road11_dt_sec": _dt(planning_start_ts, first_planning_road11),
        "first_planning_road8_dt_sec": _dt(planning_start_ts, first_planning_road8),
        "first_planning_multi_preview_road_count": safe_int((first_planning_multi or {}).get("routing_road_count")),
        "first_planning_multi_preview_unique_lane_signature": _safe_text(
            (first_planning_multi or {}).get("routing_unique_lane_signature")
        ),
        "first_planning_multi_after_first_route_segment_sec": _delta_rows(first_planning_multi, first_stage5_route_segment),
        "first_planning_multi_after_first_current_lane_sec": _delta_rows(first_planning_multi, first_stage5_current_lane),
        "first_planning_road22_after_first_current_lane_sec": _delta_rows(first_planning_road22, first_stage5_current_lane),
        "first_post_transition_current_lane_after_long_phase_transition_sec": _delta_rows(
            first_post_transition_stage5_current_lane, long_phase_transition_event
        ),
        "first_post_transition_target_lane_after_long_phase_transition_sec": _delta_rows(
            first_post_transition_stage5_target_lane, long_phase_transition_event
        ),
        "first_post_transition_planning_lane_after_long_phase_transition_sec": _delta_rows(
            first_post_transition_planning_lane, long_phase_transition_event
        ),
        "first_post_transition_planning_target_lane_after_long_phase_transition_sec": _delta_rows(
            first_post_transition_planning_target_lane, long_phase_transition_event
        ),
        "first_post_transition_road22_preview_after_long_phase_transition_sec": _delta_rows(
            first_post_transition_planning_road22, long_phase_transition_event
        ),
        "first_post_transition_road22_preview_routing_road_count": safe_int(
            (first_post_transition_planning_road22 or {}).get("routing_road_count")
        ),
        "first_post_transition_road22_preview_lane_id_first": _safe_text(
            (first_post_transition_planning_road22 or {}).get("lane_id_first")
        ),
        "first_post_transition_road22_preview_lane_id_first_road_id": _safe_text(
            (first_post_transition_planning_road22 or {}).get("lane_id_first_road_id")
        ),
        "first_post_transition_road22_preview_target_lane_id_first": _safe_text(
            (first_post_transition_planning_road22 or {}).get("target_lane_id_first")
        ),
        "first_post_transition_road22_preview_target_lane_id_first_road_id": _safe_text(
            (first_post_transition_planning_road22 or {}).get("target_lane_id_first_road_id")
        ),
        "first_post_transition_planning_before_road22_delta_sec": _delta_rows(
            first_post_transition_planning_row_before_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_planning_before_road22_lane_id_first": _safe_text(
            (first_post_transition_planning_row_before_road22 or {}).get("lane_id_first")
        ),
        "first_post_transition_planning_before_road22_lane_id_first_road_id": _safe_text(
            (first_post_transition_planning_row_before_road22 or {}).get("lane_id_first_road_id")
        ),
        "first_post_transition_planning_before_road22_target_lane_id_first": _safe_text(
            (first_post_transition_planning_row_before_road22 or {}).get("target_lane_id_first")
        ),
        "first_post_transition_planning_before_road22_target_lane_id_first_road_id": _safe_text(
            (first_post_transition_planning_row_before_road22 or {}).get("target_lane_id_first_road_id")
        ),
        "first_post_transition_planning_before_road22_routing_road_count": safe_int(
            (first_post_transition_planning_row_before_road22 or {}).get("routing_road_count")
        ),
        "first_post_transition_stage5_row_at_road22_delta_sec": _delta_rows(
            first_post_transition_stage5_row_at_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_stage5_row_at_road22_current_lane_id": _safe_text(
            (first_post_transition_stage5_row_at_road22 or {}).get("current_lane_id")
        ),
        "first_post_transition_stage5_row_at_road22_current_lane_road_id": _safe_text(
            (first_post_transition_stage5_row_at_road22 or {}).get("current_lane_road_id")
        ),
        "first_post_transition_stage5_row_at_road22_target_lane_id": _safe_text(
            (first_post_transition_stage5_row_at_road22 or {}).get("target_lane_id")
        ),
        "first_post_transition_stage5_row_at_road22_target_lane_road_id": _safe_text(
            (first_post_transition_stage5_row_at_road22 or {}).get("target_lane_road_id")
        ),
        "first_post_transition_stage5_row_at_road22_routing_road_count": safe_int(
            (first_post_transition_stage5_row_at_road22 or {}).get("routing_road_count")
        ),
        "first_post_transition_stage5_row_at_road22_route_segment_count": safe_int(
            (first_post_transition_stage5_row_at_road22 or {}).get("route_segment_count")
        ),
        "first_post_transition_stage5_row_at_road22_reference_line_count": safe_int(
            (first_post_transition_stage5_row_at_road22 or {}).get("reference_line_count")
        ),
        "first_post_transition_stage5_row_at_road22_lane_follow_map_status": _safe_text(
            (first_post_transition_stage5_row_at_road22 or {}).get("lane_follow_map_status")
        ),
        "first_post_transition_apollo_route_row_at_road22_delta_sec": _delta_rows(
            first_post_transition_apollo_route_row_at_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_apollo_route_row_at_road22_lane_id_first_road_id": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("lane_id_first_road_id")
        ),
        "first_post_transition_apollo_route_row_at_road22_target_lane_id_first_road_id": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("target_lane_id_first_road_id")
        ),
        "first_post_transition_apollo_route_row_at_road22_current_lane_road_id": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("current_lane_road_id")
        ),
        "first_post_transition_apollo_route_row_at_road22_target_lane_road_id": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("target_lane_road_id")
        ),
        "first_post_transition_apollo_route_row_at_road22_reference_line_count": safe_int(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("reference_line_count")
        ),
        "first_post_transition_apollo_route_row_at_road22_route_segment_count": safe_int(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("route_segment_count")
        ),
        "first_post_transition_apollo_route_row_at_road22_reference_line_provider_status": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("reference_line_provider_status")
        ),
        "first_post_transition_apollo_route_row_at_road22_create_route_segments_status": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("create_route_segments_status")
        ),
        "first_post_transition_apollo_route_row_at_road22_lane_follow_map_status": _safe_text(
            (first_post_transition_apollo_route_row_at_road22 or {}).get("lane_follow_map_status")
        ),
        "first_post_transition_apollo_reference_row_at_road22_delta_sec": _delta_rows(
            first_post_transition_apollo_reference_row_at_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_apollo_reference_row_at_road22_lane_id_first_road_id": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("lane_id_first_road_id")
        ),
        "first_post_transition_apollo_reference_row_at_road22_target_lane_id_first_road_id": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("target_lane_id_first_road_id")
        ),
        "first_post_transition_apollo_reference_row_at_road22_current_lane_road_id": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("current_lane_road_id")
        ),
        "first_post_transition_apollo_reference_row_at_road22_target_lane_road_id": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("target_lane_road_id")
        ),
        "first_post_transition_apollo_reference_row_at_road22_reference_line_count": safe_int(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("reference_line_count")
        ),
        "first_post_transition_apollo_reference_row_at_road22_route_segment_count": safe_int(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("route_segment_count")
        ),
        "first_post_transition_apollo_reference_row_at_road22_reference_line_provider_status": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("reference_line_provider_status")
        ),
        "first_post_transition_apollo_reference_row_at_road22_create_route_segments_status": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("create_route_segments_status")
        ),
        "first_post_transition_apollo_reference_row_at_road22_lane_follow_map_status": _safe_text(
            (first_post_transition_apollo_reference_row_at_road22 or {}).get("lane_follow_map_status")
        ),
        "first_post_transition_lane_follow_row_at_road22_delta_sec": _delta_rows(
            first_post_transition_lane_follow_row_at_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_lane_follow_row_at_road22_lane_id_first_road_id": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("lane_id_first_road_id")
        ),
        "first_post_transition_lane_follow_row_at_road22_target_lane_id_first_road_id": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("target_lane_id_first_road_id")
        ),
        "first_post_transition_lane_follow_row_at_road22_current_lane_road_id": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("current_lane_road_id")
        ),
        "first_post_transition_lane_follow_row_at_road22_target_lane_road_id": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("target_lane_road_id")
        ),
        "first_post_transition_lane_follow_row_at_road22_reference_line_count": safe_int(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("reference_line_count")
        ),
        "first_post_transition_lane_follow_row_at_road22_route_segment_count": safe_int(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("route_segment_count")
        ),
        "first_post_transition_lane_follow_row_at_road22_reference_line_provider_status": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("reference_line_provider_status")
        ),
        "first_post_transition_lane_follow_row_at_road22_create_route_segments_status": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("create_route_segments_status")
        ),
        "first_post_transition_lane_follow_row_at_road22_lane_follow_map_status": _safe_text(
            (first_post_transition_lane_follow_row_at_road22 or {}).get("lane_follow_map_status")
        ),
        "road22_boundary_surface_sync_span_ms": road22_boundary_surface_sync_span_ms,
        "road22_boundary_surface_alignment_family": road22_boundary_surface_alignment_family,
        "road22_boundary_surface_status_family": road22_boundary_surface_status_family,
        "first_post_transition_planning_lane_after_road22_after_long_phase_transition_sec": _delta_rows(
            first_post_transition_planning_lane_after_road22, long_phase_transition_event
        ),
        "first_post_transition_planning_lane_after_road22_delta_sec": _delta_rows(
            first_post_transition_planning_lane_after_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_planning_lane_after_road22_id": _safe_text(
            (first_post_transition_planning_lane_after_road22 or {}).get("lane_id_first")
        ),
        "first_post_transition_planning_lane_after_road22_road_id": _safe_text(
            (first_post_transition_planning_lane_after_road22 or {}).get("lane_id_first_road_id")
        ),
        "first_post_transition_planning_target_lane_after_road22_id": _safe_text(
            (first_post_transition_planning_lane_after_road22 or {}).get("target_lane_id_first")
        ),
        "first_post_transition_planning_target_lane_after_road22_road_id": _safe_text(
            (first_post_transition_planning_lane_after_road22 or {}).get("target_lane_id_first_road_id")
        ),
        "first_post_transition_stage5_lane_after_road22_delta_sec": _delta_rows(
            first_post_transition_stage5_lane_after_road22, first_post_transition_planning_road22
        ),
        "first_post_transition_stage5_lane_after_road22_current_lane_id": _safe_text(
            (first_post_transition_stage5_lane_after_road22 or {}).get("current_lane_id")
        ),
        "first_post_transition_stage5_lane_after_road22_current_lane_road_id": _safe_text(
            (first_post_transition_stage5_lane_after_road22 or {}).get("current_lane_road_id")
        ),
        "first_post_transition_stage5_lane_after_road22_target_lane_id": _safe_text(
            (first_post_transition_stage5_lane_after_road22 or {}).get("target_lane_id")
        ),
        "first_post_transition_stage5_lane_after_road22_target_lane_road_id": _safe_text(
            (first_post_transition_stage5_lane_after_road22 or {}).get("target_lane_road_id")
        ),
        "post_transition_stage5_row_count": len(post_transition_stage5_rows),
        "post_transition_planning_row_count": len(post_transition_planning_rows),
        "post_transition_reference_line_nonzero_rows": len(post_transition_reference_line_nonzero_rows),
        "post_transition_any_non8_current_lane": post_transition_any_non8_current_lane,
        "post_transition_any_non8_target_lane": post_transition_any_non8_target_lane,
        "post_transition_any_non8_planning_lane": post_transition_any_non8_planning_lane,
        "post_transition_any_non8_planning_target_lane": post_transition_any_non8_planning_target_lane,
        "post_transition_any_current_lane_junction": post_transition_any_current_lane_junction,
        "post_transition_any_target_lane_junction": post_transition_any_target_lane_junction,
        "post_transition_any_planning_lane_junction": post_transition_any_planning_lane_junction,
        "post_transition_any_planning_target_lane_junction": post_transition_any_planning_target_lane_junction,
        "post_long_phase_upgrade_family": post_long_phase_upgrade_family,
        "post_long_phase_lane_semantic_order_family": post_long_phase_lane_semantic_order_family,
        "road22_boundary_semantic_alignment_family": road22_boundary_semantic_alignment_family,
        "first_apollo_materialization_stage5_sync_delta_sec": apollo_stage5_sync_delta,
        "first_apollo_materialization_reference_line_sync_delta_sec": apollo_reference_sync_delta,
        "first_apollo_materialization_lane_follow_sync_delta_sec": apollo_lane_follow_sync_delta,
        "first_apollo_materialization_planning_sync_delta_sec": apollo_planning_sync_delta,
        "first_apollo_materialization_sync": first_apollo_materialization_sync,
        "long_phase_trigger_goal_distance_m": safe_float((long_phase_transition_decision or {}).get("current_goal", {}).get("goal_distance_m")),
        "long_phase_trigger_route_segment_count": safe_int(
            (long_phase_transition_decision or {}).get("trigger_condition_snapshot", {}).get("route_segment_count")
        ),
        "long_phase_trigger_reference_line_count": safe_int(
            (long_phase_transition_decision or {}).get("trigger_condition_snapshot", {}).get("reference_line_count")
        ),
        "long_phase_trigger_route_debug_ready": (long_phase_transition_decision or {}).get("trigger_condition_snapshot", {}).get("route_debug_ready_for_long_phase"),
        "onset_family": onset_family,
        "last_planning_dt_sec": _dt(planning_start_ts, last_planning),
        "first_planning_lane_id_first": _safe_text((first_planning_lane or {}).get("lane_id_first")),
        "first_planning_lane_road_id": _safe_text((first_planning_lane or {}).get("lane_id_first_road_id")),
        "last_planning_lane_id_first": _safe_text(last_planning.get("lane_id_first")),
        "last_planning_lane_road_id": _safe_text(last_planning.get("lane_id_first_road_id")),
        "last_planning_target_lane_id_first": _safe_text(last_planning.get("target_lane_id_first")),
        "last_planning_target_lane_road_id": _safe_text(last_planning.get("target_lane_id_first_road_id")),
        "last_planning_routing_road_count": safe_int(last_planning.get("routing_road_count")),
        "last_planning_routing_segment_count": safe_int(last_planning.get("routing_segment_count")),
        "last_planning_reference_line_count": safe_int(last_planning.get("reference_line_count")),
    }


def _csv_rows(snapshots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(snapshot) for snapshot in snapshots]


def _render_report(snapshots: Sequence[Dict[str, Any]], *, generated_at_local: str) -> str:
    lines: List[str] = [
        "# Town01 Junction Same-Session Collapse Compare",
        "",
        f"- generated_at_local: `{generated_at_local}`",
        "",
        "## Scope",
        "",
    ]
    for snapshot in snapshots:
        lines.append(f"- `{snapshot['label']}`: `{snapshot['route_id']}`")
        lines.append(f"  - run_dir: `{snapshot['run_dir']}`")

    lines.extend(
        [
            "",
            "## Collapse Table",
            "",
            "| sample | route | completion | distance_m | fallback | startup_payload_family | earliest_split_boundary | startup_contract_family | goal_vs_window_delta_m | first_apollo_window_dt | first_route_segment_dt | first_current_lane_dt | after_startup_route_dt | before_long_phase_dt | apollo_preview_roads | route_seg_preview_roads | current_lane_preview_roads | first_22_after_long_phase_sec | planning_before_road22 | road22_lane_semantics | stage5_at_road22 | lane_after_road22_sec | stage5_after_road22_sec | road22_boundary_family | road22_surface_family | road22_surface_status | road22_surface_sync_ms | road22_semantic_order_family | post_long_phase_upgrade_family | planning_multi_after_current_lane_sec | apollo_stage5_sync | route_rows_all_ref0 | onset_family |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            "| "
            + " | ".join(
                [
                    snapshot["label"],
                    str(snapshot.get("route_id") or ""),
                    f"{safe_float(snapshot.get('route_completion_ratio')):.6f}" if safe_float(snapshot.get("route_completion_ratio")) is not None else "",
                    f"{safe_float(snapshot.get('route_distance_achieved_m')):.3f}" if safe_float(snapshot.get("route_distance_achieved_m")) is not None else "",
                    str(snapshot.get("path_fallback_count") or ""),
                    str(snapshot.get("startup_payload_family") or ""),
                    str(snapshot.get("earliest_split_boundary") or ""),
                    str(snapshot.get("startup_first_apollo_window_contract_family") or ""),
                    f"{safe_float(snapshot.get('startup_goal_vs_first_apollo_window_length_delta_m')):.4f}" if safe_float(snapshot.get("startup_goal_vs_first_apollo_window_length_delta_m")) is not None else "",
                    f"{safe_float(snapshot.get('first_apollo_route_window_dt_sec')):.3f}" if safe_float(snapshot.get("first_apollo_route_window_dt_sec")) is not None else "",
                    f"{safe_float(snapshot.get('first_stage5_route_segment_dt_sec')):.3f}" if safe_float(snapshot.get("first_stage5_route_segment_dt_sec")) is not None else "",
                    f"{safe_float(snapshot.get('first_stage5_current_lane_dt_sec')):.3f}" if safe_float(snapshot.get("first_stage5_current_lane_dt_sec")) is not None else "",
                    f"{safe_float(snapshot.get('first_route_segment_after_startup_route_sec')):.3f}" if safe_float(snapshot.get("first_route_segment_after_startup_route_sec")) is not None else "",
                    f"{safe_float(snapshot.get('first_route_segment_before_long_phase_transition_sec')):.3f}" if safe_float(snapshot.get("first_route_segment_before_long_phase_transition_sec")) is not None else "",
                    str(snapshot.get("first_apollo_route_window_routing_road_count") or ""),
                    str(snapshot.get("first_stage5_route_segment_routing_road_count") or ""),
                    str(snapshot.get("first_stage5_current_lane_routing_road_count") or ""),
                    f"{safe_float(snapshot.get('first_post_transition_road22_preview_after_long_phase_transition_sec')):.3f}" if safe_float(snapshot.get("first_post_transition_road22_preview_after_long_phase_transition_sec")) is not None else "",
                    (
                        f"{snapshot.get('first_post_transition_planning_before_road22_lane_id_first_road_id') or 'none'}"
                        f"/{snapshot.get('first_post_transition_planning_before_road22_target_lane_id_first_road_id') or 'none'}"
                    ),
                    (
                        f"{snapshot.get('first_post_transition_road22_preview_lane_id_first_road_id') or 'none'}"
                        f"/{snapshot.get('first_post_transition_road22_preview_target_lane_id_first_road_id') or 'none'}"
                    ),
                    (
                        f"{snapshot.get('first_post_transition_stage5_row_at_road22_current_lane_road_id') or 'none'}"
                        f"/{snapshot.get('first_post_transition_stage5_row_at_road22_target_lane_road_id') or 'none'}"
                    ),
                    f"{safe_float(snapshot.get('first_post_transition_planning_lane_after_road22_delta_sec')):.3f}" if safe_float(snapshot.get("first_post_transition_planning_lane_after_road22_delta_sec")) is not None else "",
                    f"{safe_float(snapshot.get('first_post_transition_stage5_lane_after_road22_delta_sec')):.3f}" if safe_float(snapshot.get("first_post_transition_stage5_lane_after_road22_delta_sec")) is not None else "",
                    str(snapshot.get("road22_boundary_semantic_alignment_family") or ""),
                    str(snapshot.get("road22_boundary_surface_alignment_family") or ""),
                    str(snapshot.get("road22_boundary_surface_status_family") or ""),
                    f"{safe_float(snapshot.get('road22_boundary_surface_sync_span_ms')):.3f}" if safe_float(snapshot.get("road22_boundary_surface_sync_span_ms")) is not None else "",
                    str(snapshot.get("post_long_phase_lane_semantic_order_family") or ""),
                    str(snapshot.get("post_long_phase_upgrade_family") or ""),
                    f"{safe_float(snapshot.get('first_planning_multi_after_first_current_lane_sec')):.3f}" if safe_float(snapshot.get("first_planning_multi_after_first_current_lane_sec")) is not None else "",
                    str(snapshot.get("first_apollo_materialization_sync") or ""),
                    f"{snapshot.get('route_segment_visible_refline_zero_rows')}/{snapshot.get('route_segment_visible_rows')}",
                    str(snapshot.get("onset_family") or ""),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Key Findings",
            "",
            "- The same-session triad now has a narrower shared boundary than a generic `road 8 tail` label:",
            "  every compared route reaches its first stage5 row with `route_segment_count>0` while `reference_line_count=0`.",
            "- The startup reroute / goal-validity payload itself is still qualitatively shared across the triad:",
            "  `goal_mode=ego_seed_ahead`, `goal_source=startup_short_ahead`, `invalid_goal=false`,",
            "  and the attached startup route-debug snapshot is still `route_segment_count=0`, `reference_line_count=0`, `routing_lane_window_signature=none`.",
            "- That missing-reference-line condition is not intermittent in this batch:",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            f"  - `{snapshot['label']}`: route-segment-visible rows with `reference_line_count=0` = "
            f"`{snapshot['route_segment_visible_refline_zero_rows']}/{snapshot['route_segment_visible_rows']}`"
        )
    lines.extend(
        [
            "- The semantic anchor and the second-family controls now diverge at onset ordering, not only at the tail:",
            "  `181066` begins Apollo route materialization with a multi-road preview already present (`11_1_-1 | 8_1_1`) and then surfaces current-lane semantics on road `11`,",
            "  while `219062` and `219063` begin with a single-road `8_1_1` preview and only later expand to a 22-road preview after current-lane semantics are already fixed on road `8`.",
            "- That first split is not stage5-only:",
            "  the first visible startup-route window appears synchronously across `apollo_route_segment_debug`, `apollo_reference_line_debug`, `stage5_apollo_lane_follow_map_debug`, `planning_topic_debug`, and stage5 route-segment debug in every compared run.",
            "- The first Apollo startup window now also checks out against the shared `startup_short_ahead` contract rather than exposing a hidden request-contract split:",
            "  in every compared run the first Apollo route-window total length matches the startup goal distance of about `30m` within `0.006m`.",
            "  The concrete difference is which geometry that shared `30m` startup contract spans:",
            "  `181066` already crosses a road boundary within that window (`11 -> 8`), while `219062/219063` stay entirely on road `8` for the same `30m` contract.",
            "- So the earliest concrete split is not yet visible inside the startup reroute payload itself:",
            "  it first becomes observable when Apollo materializes the first visible startup-route window immediately after startup routing.",
            "- That means the current blocker should not be phrased as `startup request contract split`:",
            "  the request contract is shared, and the onset split is consistent with route geometry inside that shared startup-short-ahead window.",
            "  The real remaining blocker stays one layer later: why the second-family never upgrades from that legitimate road-`8` startup window into a direct-runtime junction transition after the long-phase scenario goal takes over and the later 22-road preview arrives.",
            "- That split is already established during the startup-route phase rather than being created by the later long-phase transition:",
            "  all three runs first expose route segments roughly `0.2s-0.5s` after `startup_initial_route`,",
            "  and they do so about `12.9s-13.4s` before `long_phase_transition` arrives.",
            "- The remaining second-family blocker is now also more specific after the long-phase boundary:",
            "  `219062/219063` both deepen planning preview to `22` roads almost immediately after `long_phase_transition`,",
            "  but the ordering between deep preview and planning first-lane semantics is itself different from the anchor route:",
            "  `181066` carries resolved road-`11` planning semantics on the first `22-road` row itself,",
            "  `219062` first shows a road-`8` planning lane on a 1-road row, then the first `22-road` row blanks those semantics for about `0.098s` before road-`8` semantics resume,",
            "  and `219063` materializes the first `22-road` row about `0.062s` before any planning first-lane semantics appear at all.",
            "  Both second-family routes still never produce any non-road-`8` or junction-marked current / target / planning lane row afterward.",
            "- That long-phase ordering fault is not planning-only anymore:",
            "  at the exact first `22-road` row, the nearest stage5 row is the same-timestamp boundary row in all three runs.",
            "  `181066` already has stage5 current-lane road `11` on that boundary row,",
            "  while `219062/219063` both have `current_lane_id=None`, `target_lane_id=None`, `reference_line_count=0`, and `route_segment_count=22` on the same boundary row.",
            "  Their planning and stage5 semantics then rehydrate together only about `0.098s / 0.062s` later, and still only as road `8`.",
            "- The exact first deep-preview boundary is now localized as a five-surface family, not only a planning/stage5 pair:",
            "  for `219062/219063`, the same-timestamp boundary row is already aligned across `planning_topic_debug`, `stage5_apollo_reference_line_debug`, `apollo_route_segment_debug`, `apollo_reference_line_debug`, and `stage5_apollo_lane_follow_map_debug`.",
            "  On all five surfaces, the second-family boundary row keeps lane semantics blank while `route_segment_count=22`, `reference_line_count=0`, `create_route_segments_status=ready`, `reference_line_provider_status=failed`, `lane_follow_map_status=route_segments_present_reference_line_missing`, and `planning_empty_reason_guess=reference_line_missing`.",
            "  `181066` is also synchronized across the same five surfaces at its first deep-preview boundary row, but there the row already keeps resolved road-`11` planning/current/target semantics while still carrying the refline-zero debug-missing contract.",
            "- The current same-session blocker is therefore sharper than either of the earlier weekend reads:",
            "  it is not merely `fresh startup noise`, and it is not merely `final tail road-8 stasis`.",
            "  It is `Apollo's first route materialization begins without any reference line during startup-route phase, and the second-family starts as a single-road road-8 onset before later preview expansion; once long-phase deep preview arrives, the exact first deep-preview boundary row is already synchronized across all five Apollo/debug surfaces as a blank-semantics failed-reference-line family, and then only recovers as road-8 local semantics`.",
            "",
            "## Per-Run Read",
            "",
        ]
    )
    for snapshot in snapshots:
        lines.extend(
            [
                f"### `{snapshot['label']}` / `{snapshot['route_id']}`",
                "",
                f"- route_health_label: `{snapshot.get('route_health_label')}`",
                f"- completion / distance / fallback: "
                f"`{snapshot.get('route_completion_ratio')}` / `{snapshot.get('route_distance_achieved_m')}` / `{snapshot.get('path_fallback_count')}`",
                f"- first Apollo route materialization: "
                f"`dt={snapshot.get('first_apollo_route_window_dt_sec')}`, "
                f"`after_startup_route={snapshot.get('first_apollo_route_window_after_startup_route_sec')}`, "
                f"`before_long_phase_transition={snapshot.get('first_apollo_route_window_before_long_phase_transition_sec')}`, "
                f"`route_segment_count={snapshot.get('first_apollo_route_window_route_segment_count')}`, "
                f"`route_segment_total_length={snapshot.get('first_apollo_route_window_total_length')}`, "
                f"`routing_road_count={snapshot.get('first_apollo_route_window_routing_road_count')}`, "
                f"`routing_signature={snapshot.get('first_apollo_route_window_routing_unique_lane_signature')}`, "
                f"`routing_window={snapshot.get('first_apollo_route_window_routing_lane_window_signature')}`, "
                f"`reference_line_count={snapshot.get('first_apollo_route_window_reference_line_count')}`, "
                f"`lane_follow_status={snapshot.get('first_apollo_route_window_lane_follow_map_status')}`, "
                f"`create_route_segments_status={snapshot.get('first_apollo_route_window_create_route_segments_status')}`, "
                f"`reference_line_provider_status={snapshot.get('first_apollo_route_window_reference_line_provider_status')}`, "
                f"`sync_with_stage5={snapshot.get('first_apollo_materialization_sync')}`",
                f"- first stage5 route-segment visibility: "
                f"`dt={snapshot.get('first_stage5_route_segment_dt_sec')}`, "
                f"`route_segment_count={snapshot.get('first_stage5_route_segment_count')}`, "
                f"`route_segment_total_length={snapshot.get('first_stage5_route_segment_total_length')}`, "
                f"`routing_road_count={snapshot.get('first_stage5_route_segment_routing_road_count')}`, "
                f"`routing_signature={snapshot.get('first_stage5_route_segment_routing_unique_lane_signature')}`, "
                f"`routing_window={snapshot.get('first_stage5_route_segment_routing_lane_window_signature')}`, "
                f"`status={snapshot.get('first_stage5_lane_follow_map_status')}`",
                f"- first stage5 current-lane visibility: "
                f"`dt={snapshot.get('first_stage5_current_lane_dt_sec')}`, "
                f"`lane={snapshot.get('first_stage5_current_lane_id')}`, "
                f"`road={snapshot.get('first_stage5_current_lane_road_id')}`, "
                f"`routing_road_count={snapshot.get('first_stage5_current_lane_routing_road_count')}`, "
                f"`routing_signature={snapshot.get('first_stage5_current_lane_routing_unique_lane_signature')}`, "
                f"`routing_window={snapshot.get('first_stage5_current_lane_routing_lane_window_signature')}`",
                f"- startup-route payload before any visible route window: "
                f"`goal_mode={snapshot.get('startup_goal_mode')}`, "
                f"`goal_source={snapshot.get('startup_goal_source')}`, "
                f"`goal_distance_m={snapshot.get('startup_goal_distance_m')}`, "
                f"`start_signed_e_y_m={snapshot.get('startup_start_projection_signed_e_y_m')}`, "
                f"`start_heading_diff_abs_deg={snapshot.get('startup_start_projection_heading_diff_to_vehicle_deg_abs')}`, "
                f"`projection_source={snapshot.get('startup_goal_projection_source')}`, "
                f"`projection_signed_e_y_m={snapshot.get('startup_goal_projection_signed_e_y_m')}`, "
                f"`invalid_goal={snapshot.get('startup_goal_validity_invalid_goal')}`, "
                f"`route_debug_signature={snapshot.get('startup_recent_route_debug_routing_lane_window_signature')}`, "
                f"`route_debug_route_segment_count={snapshot.get('startup_recent_route_debug_route_segment_count')}`, "
                f"`route_debug_reference_line_count={snapshot.get('startup_recent_route_debug_reference_line_count')}`, "
                f"`trigger_route_debug_ready={snapshot.get('startup_trigger_route_debug_ready')}`, "
                f"`payload_family={snapshot.get('startup_payload_family')}`",
                f"- startup short-ahead contract check: "
                f"`first_window_total_length={snapshot.get('first_apollo_route_window_total_length')}`, "
                f"`goal_vs_window_delta_m={snapshot.get('startup_goal_vs_first_apollo_window_length_delta_m')}`, "
                f"`length_match={snapshot.get('startup_first_apollo_window_matches_goal_distance')}`, "
                f"`contract_family={snapshot.get('startup_first_apollo_window_contract_family')}`",
                f"- post-long-phase semantic upgrade read: "
                f"`first_22_after_long_phase_transition={snapshot.get('first_post_transition_road22_preview_after_long_phase_transition_sec')}`, "
                f"`road22_lane_id_first_road={snapshot.get('first_post_transition_road22_preview_lane_id_first_road_id') or 'none'}`, "
                f"`road22_target_lane_id_first_road={snapshot.get('first_post_transition_road22_preview_target_lane_id_first_road_id') or 'none'}`, "
                f"`lane_after_road22_dt={snapshot.get('first_post_transition_planning_lane_after_road22_delta_sec')}`, "
                f"`lane_after_road22_road={snapshot.get('first_post_transition_planning_lane_after_road22_road_id') or 'none'}`, "
                f"`target_after_road22_road={snapshot.get('first_post_transition_planning_target_lane_after_road22_road_id') or 'none'}`, "
                f"`order_family={snapshot.get('post_long_phase_lane_semantic_order_family')}`, "
                f"`post_transition_refline_nonzero_rows={snapshot.get('post_transition_reference_line_nonzero_rows')}`, "
                f"`any_non8_current_lane={snapshot.get('post_transition_any_non8_current_lane')}`, "
                f"`any_non8_target_lane={snapshot.get('post_transition_any_non8_target_lane')}`, "
                f"`any_non8_planning_lane={snapshot.get('post_transition_any_non8_planning_lane')}`, "
                f"`any_non8_planning_target_lane={snapshot.get('post_transition_any_non8_planning_target_lane')}`, "
                f"`any_current_lane_junction={snapshot.get('post_transition_any_current_lane_junction')}`, "
                f"`any_target_lane_junction={snapshot.get('post_transition_any_target_lane_junction')}`, "
                f"`any_planning_lane_junction={snapshot.get('post_transition_any_planning_lane_junction')}`, "
                f"`any_planning_target_lane_junction={snapshot.get('post_transition_any_planning_target_lane_junction')}`, "
                f"`upgrade_family={snapshot.get('post_long_phase_upgrade_family')}`",
                f"- road22 boundary alignment read: "
                f"`planning_before_road22="
                f"{snapshot.get('first_post_transition_planning_before_road22_lane_id_first_road_id') or 'none'}"
                f"/{snapshot.get('first_post_transition_planning_before_road22_target_lane_id_first_road_id') or 'none'}`, "
                f"`stage5_at_road22_current={snapshot.get('first_post_transition_stage5_row_at_road22_current_lane_road_id') or 'none'}`, "
                f"`stage5_at_road22_target={snapshot.get('first_post_transition_stage5_row_at_road22_target_lane_road_id') or 'none'}`, "
                f"`stage5_at_road22_route_segment_count={snapshot.get('first_post_transition_stage5_row_at_road22_route_segment_count')}`, "
                f"`stage5_at_road22_reference_line_count={snapshot.get('first_post_transition_stage5_row_at_road22_reference_line_count')}`, "
                f"`planning_after_road22_dt={snapshot.get('first_post_transition_planning_lane_after_road22_delta_sec')}`, "
                f"`planning_after_road22_road={snapshot.get('first_post_transition_planning_lane_after_road22_road_id') or 'none'}`, "
                f"`stage5_after_road22_dt={snapshot.get('first_post_transition_stage5_lane_after_road22_delta_sec')}`, "
                f"`stage5_after_road22_road={snapshot.get('first_post_transition_stage5_lane_after_road22_current_lane_road_id') or snapshot.get('first_post_transition_stage5_lane_after_road22_target_lane_road_id') or 'none'}`, "
                f"`boundary_family={snapshot.get('road22_boundary_semantic_alignment_family')}`",
                f"- deep-preview boundary surface read: "
                f"`surface_sync_span_ms={snapshot.get('road22_boundary_surface_sync_span_ms')}`, "
                f"`surface_family={snapshot.get('road22_boundary_surface_alignment_family')}`, "
                f"`surface_status_family={snapshot.get('road22_boundary_surface_status_family')}`, "
                f"`planning_road={snapshot.get('first_post_transition_road22_preview_lane_id_first_road_id') or snapshot.get('first_post_transition_road22_preview_target_lane_id_first_road_id') or 'none'}`, "
                f"`stage5_road={snapshot.get('first_post_transition_stage5_row_at_road22_current_lane_road_id') or snapshot.get('first_post_transition_stage5_row_at_road22_target_lane_road_id') or 'none'}`, "
                f"`apollo_route_road={snapshot.get('first_post_transition_apollo_route_row_at_road22_lane_id_first_road_id') or snapshot.get('first_post_transition_apollo_route_row_at_road22_current_lane_road_id') or snapshot.get('first_post_transition_apollo_route_row_at_road22_target_lane_road_id') or 'none'}`, "
                f"`apollo_ref_road={snapshot.get('first_post_transition_apollo_reference_row_at_road22_lane_id_first_road_id') or snapshot.get('first_post_transition_apollo_reference_row_at_road22_current_lane_road_id') or snapshot.get('first_post_transition_apollo_reference_row_at_road22_target_lane_road_id') or 'none'}`, "
                f"`lane_follow_road={snapshot.get('first_post_transition_lane_follow_row_at_road22_lane_id_first_road_id') or snapshot.get('first_post_transition_lane_follow_row_at_road22_current_lane_road_id') or snapshot.get('first_post_transition_lane_follow_row_at_road22_target_lane_road_id') or 'none'}`, "
                f"`apollo_route_status={snapshot.get('first_post_transition_apollo_route_row_at_road22_reference_line_provider_status') or 'none'}/"
                f"{snapshot.get('first_post_transition_apollo_route_row_at_road22_lane_follow_map_status') or 'none'}`, "
                f"`apollo_ref_status={snapshot.get('first_post_transition_apollo_reference_row_at_road22_reference_line_provider_status') or 'none'}/"
                f"{snapshot.get('first_post_transition_apollo_reference_row_at_road22_lane_follow_map_status') or 'none'}`, "
                f"`lane_follow_status={snapshot.get('first_post_transition_lane_follow_row_at_road22_reference_line_provider_status') or 'none'}/"
                f"{snapshot.get('first_post_transition_lane_follow_row_at_road22_lane_follow_map_status') or 'none'}`",
                f"- startup-route vs long-phase boundary: "
                f"`startup_route_dt={snapshot.get('startup_route_dt_sec')}`, "
                f"`long_phase_transition_dt={snapshot.get('long_phase_transition_dt_sec')}`, "
                f"`first_route_segment_after_startup_route={snapshot.get('first_route_segment_after_startup_route_sec')}`, "
                f"`first_current_lane_after_startup_route={snapshot.get('first_current_lane_after_startup_route_sec')}`, "
                f"`first_route_segment_before_long_phase_transition={snapshot.get('first_route_segment_before_long_phase_transition_sec')}`, "
                f"`first_current_lane_before_long_phase_transition={snapshot.get('first_current_lane_before_long_phase_transition_sec')}`",
                f"- first stage5 road-11 / road-8 timestamps: "
                f"`road11={snapshot.get('first_stage5_road11_dt_sec')}`, "
                f"`road8={snapshot.get('first_stage5_road8_dt_sec')}`",
                f"- route-segment-visible rows: "
                f"`{snapshot.get('route_segment_visible_rows')}` total, "
                f"`{snapshot.get('route_segment_visible_refline_zero_rows')}` with `reference_line_count=0`",
                f"- last stage5 row: "
                f"`lane={snapshot.get('last_stage5_current_lane_id')}`, "
                f"`road={snapshot.get('last_stage5_current_lane_road_id')}`, "
                f"`is_junction={snapshot.get('last_stage5_current_lane_is_junction')}`, "
                f"`reference_line_count={snapshot.get('last_stage5_reference_line_count')}`, "
                f"`route_segment_count={snapshot.get('last_stage5_route_segment_count')}`, "
                f"`status={snapshot.get('last_stage5_lane_follow_map_status')}`",
                f"- planning-side preview: "
                f"`first_multi_preview_dt={snapshot.get('first_planning_multi_preview_dt_sec')}`, "
                f"`first_road22_dt={snapshot.get('first_planning_road22_dt_sec')}`, "
                f"`multi_preview_road_count={snapshot.get('first_planning_multi_preview_road_count')}`, "
                f"`multi_after_first_route_segment={snapshot.get('first_planning_multi_after_first_route_segment_sec')}`, "
                f"`multi_after_first_current_lane={snapshot.get('first_planning_multi_after_first_current_lane_sec')}`, "
                f"`first_lane_dt={snapshot.get('first_planning_lane_dt_sec')}`, "
                f"`first_road11_dt={snapshot.get('first_planning_road11_dt_sec')}`, "
                f"`first_road8_dt={snapshot.get('first_planning_road8_dt_sec')}`, "
                f"`last_routing_road_count={snapshot.get('last_planning_routing_road_count')}`, "
                f"`onset_family={snapshot.get('onset_family')}`",
                f"- long-phase trigger snapshot: "
                f"`goal_distance_m={snapshot.get('long_phase_trigger_goal_distance_m')}`, "
                f"`route_segment_count={snapshot.get('long_phase_trigger_route_segment_count')}`, "
                f"`reference_line_count={snapshot.get('long_phase_trigger_reference_line_count')}`, "
                f"`route_debug_ready={snapshot.get('long_phase_trigger_route_debug_ready')}`",
                "",
            ]
        )

    lines.extend(
        [
            "## Next Step",
            "",
            "- Prioritize the exact first post-long-phase deep-preview boundary row over another generic startup rerun or another tail-only compare.",
            "- For `181066`, compare what keeps all five surfaces synchronized on resolved road-`11` semantics at the first deep-preview row even though `reference_line_count` is still `0`.",
            "- For `219062`, compare why the first deep-preview row is already synchronized across all five surfaces as a blank-semantics failed-reference-line family before both planning and stage5 rehydrate about `0.098s` later, still only as road `8`.",
            "- For `219063`, compare why the first deep-preview row is also synchronized as a blank-semantics failed-reference-line family, why it appears before any planning or stage5 lane semantics exist, and why the first recovered semantics then still land only on road `8`.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze same-session Town01 junction collapse boundaries across multiple run dirs.")
    parser.add_argument(
        "--case",
        action="append",
        required=True,
        help="Comparison case formatted as LABEL=RUN_DIR. Can be repeated.",
    )
    parser.add_argument("--report", required=True, help="Markdown report output path.")
    parser.add_argument("--summary-csv", required=True, help="CSV summary output path.")
    args = parser.parse_args()

    snapshots: List[Dict[str, Any]] = []
    for item in args.case:
        if "=" not in item:
            raise SystemExit(f"invalid --case, expected LABEL=RUN_DIR: {item}")
        label, run_dir_text = item.split("=", 1)
        snapshots.append(_build_snapshot(label.strip(), Path(run_dir_text).expanduser().resolve()))

    generated_at_local = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    report_path = Path(args.report).expanduser().resolve()
    csv_path = Path(args.summary_csv).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    report_path.write_text(_render_report(snapshots, generated_at_local=generated_at_local), encoding="utf-8")

    csv_rows = _csv_rows(snapshots)
    fieldnames = list(csv_rows[0].keys()) if csv_rows else []
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

    print(
        "[town01-junction-same-session-collapse] "
        f"cases={len(snapshots)} "
        f"report={report_path} "
        f"summary_csv={csv_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
