from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
import xml.etree.ElementTree as ET

BAGUANG_LANE_EVENT_CONTRACT_SCHEMA_VERSION = "baguang_lane_event_contract.v1"
DEFAULT_BAGUANG_XODR = Path(
    "/home/ubuntu/CARLA_0.9.16/CarlaUE4/Content/Carla/Maps/"
    "straight_road_for_baguang/OpenDrive/straight_road_for_baguang.xodr"
)
DEFAULT_TARGET_LANE_ID = -2
DEFAULT_EGO_HALF_WIDTH_M = 1.0
DEFAULT_CTE_WARN_M = 0.05
DEFAULT_HEADING_WARN_RAD = 0.02
DEFAULT_EARLY_EVENT_DISTANCE_M = 5.0


@dataclass(frozen=True)
class LaneMark:
    s_offset: float | None
    mark_type: str
    color: str
    width_m: float | None
    lane_change: str


@dataclass(frozen=True)
class LaneContract:
    road_id: str
    lane_id: int
    lane_type: str
    travel_dir: str | None
    width_m: float | None
    road_marks: tuple[LaneMark, ...]


def analyze_baguang_lane_event_contract(
    *,
    xodr_path: str | Path = DEFAULT_BAGUANG_XODR,
    run_dirs: Iterable[str | Path] = (),
    map_name: str = "straight_road_for_baguang",
    target_lane_id: int = DEFAULT_TARGET_LANE_ID,
    ego_half_width_m: float = DEFAULT_EGO_HALF_WIDTH_M,
    cte_warn_m: float = DEFAULT_CTE_WARN_M,
    heading_warn_rad: float = DEFAULT_HEADING_WARN_RAD,
    early_event_distance_m: float = DEFAULT_EARLY_EVENT_DISTANCE_M,
) -> dict[str, Any]:
    lanes = parse_xodr_lane_contracts(xodr_path)
    target_lane = _find_lane(lanes, int(target_lane_id))
    xodr_report = _xodr_report(
        xodr_path=Path(xodr_path).expanduser(),
        lanes=lanes,
        target_lane=target_lane,
        ego_half_width_m=float(ego_half_width_m),
    )
    run_reports = [
        analyze_lane_event_run_dir(
            run_dir,
            target_lane=target_lane,
            ego_half_width_m=float(ego_half_width_m),
            cte_warn_m=float(cte_warn_m),
            heading_warn_rad=float(heading_warn_rad),
            early_event_distance_m=float(early_event_distance_m),
        )
        for run_dir in run_dirs
    ]
    status = _overall_status(xodr_report, run_reports)
    quarantine = any(report.get("quarantine_recommended") for report in run_reports)
    quarantine_reason = next(
        (
            str(report.get("reason"))
            for report in run_reports
            if report.get("quarantine_recommended") and report.get("reason")
        ),
        "lane_invasion_trigger_inconsistent_with_centerline_evidence",
    )
    report = {
        "schema_version": BAGUANG_LANE_EVENT_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "map_name": str(map_name),
        "xodr": xodr_report,
        "run_reports": run_reports,
        "quarantine_recommended": quarantine,
        "claim_boundary": {
            "lane_invasion_event_can_be_used_as_hard_gate": not quarantine,
            "reason": quarantine_reason if quarantine else "no_inconsistent_lane_invasion_trigger_observed",
        },
        "thresholds": {
            "ego_half_width_m": float(ego_half_width_m),
            "cte_warn_m": float(cte_warn_m),
            "heading_warn_rad": float(heading_warn_rad),
            "early_event_distance_m": float(early_event_distance_m),
        },
    }
    report["representative_run_context"] = _representative_run_context(run_reports)
    return report


def parse_xodr_lane_contracts(xodr_path: str | Path) -> list[LaneContract]:
    path = Path(xodr_path).expanduser()
    root = ET.parse(path).getroot()
    lanes: list[LaneContract] = []
    for road in root.findall("road"):
        road_id = str(road.attrib.get("id") or "")
        for lane in road.findall(".//laneSection/*/lane"):
            lane_id = int(lane.attrib["id"])
            width = _first_float_attr(lane.findall("width"), "a")
            vector_lane = lane.find("./userData/vectorLane")
            travel_dir = vector_lane.attrib.get("travelDir") if vector_lane is not None else None
            marks = tuple(
                LaneMark(
                    s_offset=_float_or_none(mark.attrib.get("sOffset")),
                    mark_type=str(mark.attrib.get("type") or "unknown"),
                    color=str(mark.attrib.get("color") or "unknown"),
                    width_m=_float_or_none(mark.attrib.get("width")),
                    lane_change=str(mark.attrib.get("laneChange") or "unknown"),
                )
                for mark in lane.findall("roadMark")
            )
            lanes.append(
                LaneContract(
                    road_id=road_id,
                    lane_id=lane_id,
                    lane_type=str(lane.attrib.get("type") or "unknown"),
                    travel_dir=travel_dir,
                    width_m=width,
                    road_marks=marks,
                )
            )
    return lanes


def analyze_lane_event_run_dir(
    run_dir: str | Path,
    *,
    target_lane: LaneContract | None,
    ego_half_width_m: float = DEFAULT_EGO_HALF_WIDTH_M,
    cte_warn_m: float = DEFAULT_CTE_WARN_M,
    heading_warn_rad: float = DEFAULT_HEADING_WARN_RAD,
    early_event_distance_m: float = DEFAULT_EARLY_EVENT_DISTANCE_M,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    summary = _read_json(root / "summary.json")
    rows = _read_csv_rows(root / "timeseries.csv")
    events = _read_lane_events(root, rows)
    offset_sweep = _offset_sweep_report(root)
    lane_events = [event for event in events if str(event.get("event_type")) == "lane_invasion"]
    trigger = _first_lane_trigger(lane_events, rows)
    max_abs_cte = _max_abs_aliases(rows, ("cross_track_error", "cross_track_error_m"))
    max_abs_heading = _max_abs_aliases(rows, ("heading_error", "heading_error_rad"))
    final_lane_count = _last_number(rows, "lane_invasion_count")
    final_displacement = _displacement_x(rows)
    target_width = target_lane.width_m if target_lane is not None else None
    footprint = _ego_half_width_report(root, rows, float(ego_half_width_m))
    actual_half_width = float(footprint.get("ego_half_width_m") or ego_half_width_m)
    base_crossing_offset = _estimated_crossing_offset(
        lane_width_m=target_width,
        ego_half_width_m=actual_half_width,
        target_lane=target_lane,
    )
    trigger_cte = _number(trigger.get("cross_track_error")) if trigger else None
    trigger_heading = _number(trigger.get("heading_error")) if trigger else None
    trigger_displacement = _number(trigger.get("distance_from_start_x_m")) if trigger else None
    initial_heading = _first_number_aliases(rows, ("heading_error", "heading_error_rad"))
    initial_lateral_extent = _footprint_lateral_extent_m(
        ego_half_width_m=actual_half_width,
        ego_half_length_m=_number(footprint.get("ego_half_length_m")),
        heading_error_rad=initial_heading,
    )
    trigger_lateral_extent = _footprint_lateral_extent_m(
        ego_half_width_m=actual_half_width,
        ego_half_length_m=_number(footprint.get("ego_half_length_m")),
        heading_error_rad=trigger_heading,
    )
    initial_crossing_offset = _estimated_crossing_offset(
        lane_width_m=target_width,
        ego_half_width_m=initial_lateral_extent,
        target_lane=target_lane,
    )
    trigger_crossing_offset = _estimated_crossing_offset(
        lane_width_m=target_width,
        ego_half_width_m=trigger_lateral_extent,
        target_lane=target_lane,
    )
    marking_match = _crossed_marking_match_report(trigger, target_lane)
    departure_diagnostics = _departure_diagnostics(
        rows,
        trigger=trigger,
        estimated_crossing_offset_m=trigger_crossing_offset,
        early_event_distance_m=float(early_event_distance_m),
        control_apply_rows=_read_jsonl_rows(root / "artifacts" / "control_apply_trace.jsonl"),
    )
    vehicle_response_context = _lane_event_vehicle_response_context(root)
    path_control_context = _lane_event_path_control_context(root)
    route_lateral_sign_policy = _lane_event_route_lateral_sign_policy(root)
    projection_lateral_context = _lane_event_projection_lateral_context(
        root,
        route_lateral_sign_policy=route_lateral_sign_policy,
    )
    lane_event_attribution = _lane_event_attribution(
        departure_diagnostics,
        vehicle_response_context=vehicle_response_context,
        path_control_context=path_control_context,
        route_lateral_sign_policy=route_lateral_sign_policy,
    )
    initial_cte = _first_number_aliases(rows, ("cross_track_error", "cross_track_error_m"))
    initial_footprint = _footprint_crossing_check(initial_cte, initial_crossing_offset)
    trigger_footprint = _footprint_crossing_check(trigger_cte, trigger_crossing_offset)
    inconsistent_trigger = bool(
        trigger
        and trigger_cte is not None
        and abs(trigger_cte) <= float(cte_warn_m)
        and (trigger_heading is None or abs(trigger_heading) <= float(heading_warn_rad))
        and (trigger_displacement is None or abs(trigger_displacement) <= float(early_event_distance_m))
    )
    geometrically_implausible = bool(
        trigger_cte is not None
        and trigger_crossing_offset is not None
        and trigger_footprint.get("footprint_intersects_marking") is False
    )
    quarantine = bool(lane_events and geometrically_implausible)
    if not lane_events:
        status = "pass"
        reason = "no_lane_invasion_event_observed"
    elif quarantine:
        status = "warn"
        reason = (
            "lane_invasion_trigger_inconsistent_with_centerline_evidence"
            if inconsistent_trigger
            else "lane_invasion_trigger_before_static_footprint_crossing"
        )
    elif inconsistent_trigger:
        status = "warn"
        reason = "early_low_cte_lane_invasion_without_static_crossing_check"
    else:
        status = "fail"
        reason = "possible_real_lane_departure_or_unclassified_lane_event"
    return {
        "run_dir": str(root),
        "status": status,
        "reason": reason,
        "quarantine_recommended": quarantine,
        "lane_invasion_event_can_be_used_as_hard_gate": not quarantine,
        "summary": {
            "success": summary.get("success"),
            "fail_reason": summary.get("fail_reason"),
            "exit_reason": summary.get("exit_reason"),
            "frames": summary.get("frames"),
            "lane_invasion_count": summary.get("lane_invasion_count"),
            "collision_count": summary.get("collision_count"),
            "max_speed_mps": summary.get("max_speed_mps"),
        },
        "lane_event_count": len(lane_events),
        "first_lane_invasion": trigger,
        "timeseries": {
            "rows": len(rows),
            "max_abs_cross_track_error_m": max_abs_cte,
            "max_abs_heading_error_rad": max_abs_heading,
            "final_lane_invasion_count": final_lane_count,
            "final_distance_span_x_m": final_displacement,
        },
        "static_crossing_check": {
            "target_lane_id": target_lane.lane_id if target_lane else None,
            "target_lane_width_m": target_width,
            "ego_half_width_m": actual_half_width,
            "ego_half_length_m": footprint.get("ego_half_length_m"),
            "ego_half_width_source": footprint.get("source"),
            "estimated_center_offset_to_cross_mark_m": base_crossing_offset,
            "initial_center_offset_to_footprint_crossing_threshold_m": initial_crossing_offset,
            "trigger_center_offset_to_footprint_crossing_threshold_m": trigger_crossing_offset,
            "initial_footprint_lateral_extent_m": initial_lateral_extent,
            "trigger_footprint_lateral_extent_m": trigger_lateral_extent,
            "initial_heading_error_used_rad": initial_heading,
            "trigger_heading_error_used_rad": trigger_heading,
            "initial_abs_cross_track_error_m": abs(initial_cte) if initial_cte is not None else None,
            "initial_clearance_to_marking_m": initial_footprint.get("clearance_to_marking_m"),
            "initial_footprint_intersects_marking": initial_footprint.get("footprint_intersects_marking"),
            "trigger_abs_cross_track_error_m": abs(trigger_cte) if trigger_cte is not None else None,
            "trigger_clearance_to_marking_m": trigger_footprint.get("clearance_to_marking_m"),
            "trigger_footprint_intersects_marking": trigger_footprint.get("footprint_intersects_marking"),
            "trigger_geometrically_implausible": geometrically_implausible,
            "trigger_marking_types_match_target_lane": marking_match.get("types_match_target_lane"),
            "trigger_marking_type_reason": marking_match.get("reason"),
        },
        "departure_diagnostics": departure_diagnostics,
        "vehicle_response_context": vehicle_response_context,
        "path_control_context": path_control_context,
        "route_lateral_sign_policy": route_lateral_sign_policy,
        "projection_lateral_context": projection_lateral_context,
        "lane_event_attribution": lane_event_attribution,
        "crossed_marking_check": marking_match,
        "vehicle_footprint": footprint,
        "offset_sweep": offset_sweep,
        "missing_inputs": _missing_inputs(root),
        "event_sources": sorted({str(event.get("source") or "events.jsonl") for event in lane_events}),
    }


def write_baguang_lane_event_contract_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_lane_event_contract_report.json"
    md_path = out / "baguang_lane_event_contract_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _xodr_report(
    *,
    xodr_path: Path,
    lanes: list[LaneContract],
    target_lane: LaneContract | None,
    ego_half_width_m: float,
) -> dict[str, Any]:
    driving = [lane for lane in lanes if lane.lane_type == "driving"]
    return {
        "path": str(xodr_path),
        "exists": xodr_path.exists(),
        "lane_count": len(lanes),
        "driving_lane_ids": [lane.lane_id for lane in driving],
        "target_lane": _lane_to_dict(target_lane) if target_lane else None,
        "target_lane_center_crossing_margin_m": _estimated_crossing_offset(
            lane_width_m=target_lane.width_m if target_lane else None,
            ego_half_width_m=ego_half_width_m,
            target_lane=target_lane,
        ),
        "warnings": _xodr_warnings(target_lane),
    }


def _xodr_warnings(target_lane: LaneContract | None) -> list[str]:
    warnings: list[str] = []
    if target_lane is None:
        return ["target_lane_missing"]
    if target_lane.lane_type != "driving":
        warnings.append("target_lane_not_driving")
    if target_lane.width_m is None:
        warnings.append("target_lane_width_missing")
    elif target_lane.width_m < 3.0:
        warnings.append("target_lane_width_unusually_narrow")
    mark_types = {mark.mark_type for mark in target_lane.road_marks}
    if "solid" in mark_types:
        warnings.append("target_lane_has_solid_outer_roadmark")
    return warnings


def _first_lane_trigger(events: list[dict[str, Any]], rows: list[dict[str, str]]) -> dict[str, Any] | None:
    if not events:
        return None
    event = events[0]
    event_frame = _number(event.get("frame"))
    event_step = _number(event.get("step"))
    row: dict[str, str] | None = None
    if event_frame is not None:
        row = next((item for item in rows if _number(item.get("frame")) == event_frame), None)
        if row is None:
            row = next((item for item in rows if _number(item.get("frame_id")) == event_frame), None)
    if row is None:
        row = next((item for item in rows if (_number(item.get("lane_invasion_count")) or 0.0) > 0.0), None)
    first_x = _first_number(rows, "ego_x")
    row_x = _number(row.get("ego_x")) if row else None
    result: dict[str, Any] = {
        "event_frame": event_frame,
        "event_step": event_step,
        "event_time_s": _number(event.get("t"), event.get("timestamp")),
        "event_source": event.get("source"),
        "crossed_lane_marking_count": _number(event.get("crossed_lane_marking_count")),
        "crossed_lane_marking_types": _normalize_marking_types(event.get("crossed_lane_marking_types")),
        "row_found": row is not None,
    }
    if row is not None:
        result.update(
            {
                "row_frame": _number(row.get("frame")),
                "row_frame_id": _number(row.get("frame_id")),
                "sim_time_s": _number(row.get("sim_time")),
                "ego_x": row_x,
                "ego_y": _number(row.get("ego_y")),
                "ego_speed_mps": _number(row.get("ego_speed"), row.get("ego_speed_mps"), row.get("v_mps")),
                "cross_track_error": _number(row.get("cross_track_error"), row.get("cross_track_error_m")),
                "heading_error": _number(row.get("heading_error"), row.get("heading_error_rad")),
                "lane_invasion_count": _number(row.get("lane_invasion_count")),
                "collision_count": _number(row.get("collision_count")),
                "distance_from_start_x_m": (
                    abs(row_x - first_x) if row_x is not None and first_x is not None else None
                ),
            }
        )
    return result


def _estimated_crossing_offset(
    *,
    lane_width_m: float | None,
    ego_half_width_m: float,
    target_lane: LaneContract | None,
) -> float | None:
    if lane_width_m is None:
        return None
    mark_widths = [
        mark.width_m for mark in (target_lane.road_marks if target_lane is not None else ()) if mark.width_m
    ]
    max_mark_half_width = (max(mark_widths) / 2.0) if mark_widths else 0.0
    return max(0.0, (lane_width_m / 2.0) - float(ego_half_width_m) - max_mark_half_width)


def _footprint_lateral_extent_m(
    *,
    ego_half_width_m: float,
    ego_half_length_m: float | None,
    heading_error_rad: float | None,
) -> float:
    if ego_half_length_m is None or ego_half_length_m <= 0.0 or heading_error_rad is None:
        return float(ego_half_width_m)
    yaw = abs(float(heading_error_rad))
    return abs(math.cos(yaw)) * float(ego_half_width_m) + abs(math.sin(yaw)) * float(ego_half_length_m)


def _footprint_crossing_check(
    cross_track_error_m: float | None,
    estimated_center_offset_to_cross_mark_m: float | None,
) -> dict[str, Any]:
    if cross_track_error_m is None or estimated_center_offset_to_cross_mark_m is None:
        return {"footprint_intersects_marking": None, "clearance_to_marking_m": None}
    clearance = float(estimated_center_offset_to_cross_mark_m) - abs(float(cross_track_error_m))
    return {
        "footprint_intersects_marking": clearance <= 0.0,
        "clearance_to_marking_m": clearance,
    }


def _crossed_marking_match_report(trigger: Mapping[str, Any] | None, target_lane: LaneContract | None) -> dict[str, Any]:
    if not trigger:
        return {
            "available": False,
            "reason": "no_lane_invasion_trigger",
            "types_match_target_lane": None,
        }
    crossed = _normalize_marking_types(trigger.get("crossed_lane_marking_types"))
    target_types = _target_lane_marking_types(target_lane)
    if not crossed:
        return {
            "available": False,
            "reason": "crossed_marking_types_missing",
            "crossed_lane_marking_types": [],
            "target_lane_roadmark_types": target_types,
            "types_match_target_lane": None,
        }
    crossed_set = set(crossed)
    target_set = set(target_types)
    matched = bool(crossed_set & target_set)
    return {
        "available": True,
        "reason": "crossed_marking_types_match_target_lane" if matched else "crossed_marking_types_do_not_match_target_lane",
        "crossed_lane_marking_types": crossed,
        "target_lane_roadmark_types": target_types,
        "types_match_target_lane": matched,
    }


def _offset_sweep_report(root: Path) -> dict[str, Any]:
    rows = _read_csv_rows(root / "offset_summary.csv")
    if not rows:
        return {"available": False, "reason": "offset_summary_missing"}
    parsed: list[dict[str, Any]] = []
    for row in rows:
        offset = _number(row.get("offset_m"))
        count = _number(row.get("lane_invasion_count"))
        spawned = _bool_or_none(row.get("spawned"))
        parsed.append(
            {
                "offset_m": offset,
                "spawned": spawned,
                "lane_invasion_count": count,
                "static_lane_invasion_count": _number(row.get("static_lane_invasion_count")),
                "first_event_displacement_m": _number(row.get("first_event_displacement_m")),
                "first_event_cte_m": _number(row.get("first_event_cte_m")),
                "first_event_heading_error_rad": _number(row.get("first_event_heading_error_rad")),
                "first_event_marking_types": _parse_list_like(row.get("first_event_marking_types")),
            }
        )
    event_offsets = [
        row["offset_m"]
        for row in parsed
        if row.get("offset_m") is not None and (row.get("lane_invasion_count") or 0.0) > 0.0
    ]
    no_event_offsets = [
        row["offset_m"]
        for row in parsed
        if row.get("offset_m") is not None and (row.get("lane_invasion_count") or 0.0) <= 0.0
    ]
    positive_offsets_with_events = [offset for offset in event_offsets if offset is not None and offset > 0.5]
    zero_offset_event = any(offset is not None and abs(offset) <= 0.5 for offset in event_offsets)
    road_start_only = bool(zero_offset_event and not positive_offsets_with_events and no_event_offsets)
    min_clear_offset = min((offset for offset in no_event_offsets if offset is not None and offset > 0.5), default=None)
    return {
        "available": True,
        "reason": "road_start_only_lane_event" if road_start_only else "offset_sweep_observed",
        "road_start_only_trigger": road_start_only,
        "offsets_tested_m": [row["offset_m"] for row in parsed],
        "event_offsets_m": event_offsets,
        "no_event_offsets_m": no_event_offsets,
        "min_clear_offset_without_event_m": min_clear_offset,
        "rows": parsed,
    }


def _departure_diagnostics(
    rows: list[dict[str, str]],
    *,
    trigger: Mapping[str, Any] | None,
    estimated_crossing_offset_m: float | None,
    early_event_distance_m: float,
    control_apply_rows: list[dict[str, Any]] | None = None,
    window_rows: int = 20,
) -> dict[str, Any]:
    """Summarize the short window before a lane-invasion trigger.

    The contract status still decides pass/warn/fail. This block only makes a
    downstream failure easier to attribute without reclassifying it.
    """

    if not rows:
        return {"available": False, "reason": "timeseries_missing"}
    if not trigger:
        return {"available": False, "reason": "lane_invasion_trigger_missing"}
    trigger_index = _trigger_row_index(rows, trigger)
    if trigger_index is None:
        return {"available": False, "reason": "trigger_row_not_found"}
    start_index = max(0, trigger_index - int(window_rows) + 1)
    window = rows[start_index : trigger_index + 1]
    if not window:
        return {"available": False, "reason": "diagnostic_window_empty"}

    cte = _series_aliases(window, ("cross_track_error", "cross_track_error_m"))
    heading = _series_aliases(window, ("heading_error", "heading_error_rad"))
    sim_time = _series_aliases(window, ("sim_time", "t", "sim_time_s"))
    speed = _series_aliases(window, ("ego_speed", "ego_speed_mps", "v_mps"))
    control_window = _control_apply_rows_for_window(control_apply_rows or [], sim_time)
    if control_window:
        control_source = "artifacts/control_apply_trace.jsonl"
        applied_steer = _nested_series(control_window, (("carla_applied", "steer"),))
        raw_steer = _nested_series(control_window, (("apollo_raw", "steer"),))
        mapped_steer = _nested_series(
            control_window,
            (("bridge_mapped", "mapped_carla_steer_cmd"), ("bridge_mapped", "steer")),
        )
        applied_throttle = _nested_series(control_window, (("carla_applied", "throttle"),))
        applied_brake = _nested_series(control_window, (("carla_applied", "brake"),))
        raw_throttle = _nested_series(control_window, (("apollo_raw", "throttle"),))
        raw_brake = _nested_series(control_window, (("apollo_raw", "brake"),))
    else:
        control_source = "timeseries"
        applied_steer = _series_aliases(window, ("applied_steer", "carla_steer_applied"))
        raw_steer = _series_aliases(window, ("apollo_steer_raw", "cmd_steer"))
        mapped_steer = _series_aliases(window, ("bridge_steer_mapped", "clamped_steer"))
        applied_throttle = _series_aliases(window, ("applied_throttle", "throttle_applied"))
        applied_brake = _series_aliases(window, ("applied_brake", "brake_applied"))
        raw_throttle = _series_aliases(window, ("throttle_raw", "cmd_throttle"))
        raw_brake = _series_aliases(window, ("brake_raw", "cmd_brake"))

    cte_start = cte[0] if cte else None
    cte_end = cte[-1] if cte else _number(trigger.get("cross_track_error"))
    heading_start = heading[0] if heading else None
    heading_end = heading[-1] if heading else _number(trigger.get("heading_error"))
    abs_cte_growth_m = (
        abs(cte_end) - abs(cte_start)
        if cte_start is not None and cte_end is not None
        else None
    )
    abs_heading_growth_rad = (
        abs(heading_end) - abs(heading_start)
        if heading_start is not None and heading_end is not None
        else None
    )
    distance_from_start_m = _number(trigger.get("distance_from_start_x_m"))
    clearance_to_marking_m = (
        float(estimated_crossing_offset_m) - abs(float(cte_end))
        if estimated_crossing_offset_m is not None and cte_end is not None
        else None
    )
    cte_abs_increasing_ratio = _nondecreasing_abs_ratio(cte)
    heading_same_sign_as_cte = _same_sign(cte_end, heading_end)
    raw_steer_same_sign_as_cte = _same_sign(cte_end, _last_value(raw_steer))
    mapped_steer_same_sign_as_cte = _same_sign(cte_end, _last_value(mapped_steer))
    applied_steer_same_sign_as_cte = _same_sign(cte_end, _last_value(applied_steer))
    raw_mapped_applied_steer_available = bool(raw_steer and mapped_steer and applied_steer)
    raw_to_mapped_steer_error = _pairwise_abs_diff_summary(raw_steer, mapped_steer)
    raw_to_mapped_steer_gain = _pairwise_ratio_summary(mapped_steer, raw_steer)
    mapped_to_applied_steer_error = _pairwise_abs_diff_summary(mapped_steer, applied_steer)
    applied_nonzero_mapped_zero = _nonzero_while_reference_zero(
        applied_steer,
        mapped_steer,
        value_epsilon=0.01,
        reference_epsilon=0.005,
    )

    interpretation: list[str] = []
    if distance_from_start_m is not None and distance_from_start_m > early_event_distance_m:
        interpretation.append("lane_event_after_road_start_window")
    if abs_cte_growth_m is not None and abs_cte_growth_m >= 0.15:
        interpretation.append("cross_track_error_grew_before_event")
    if cte_abs_increasing_ratio is not None and cte_abs_increasing_ratio >= 0.75:
        interpretation.append("cross_track_error_mostly_increased_before_event")
    if abs_heading_growth_rad is not None and abs_heading_growth_rad > 0.02:
        interpretation.append("heading_error_grew_before_event")
    if heading_same_sign_as_cte is True:
        interpretation.append("heading_error_same_sign_as_cross_track_error")
    if clearance_to_marking_m is not None and clearance_to_marking_m <= 0.1:
        interpretation.append("near_lane_marking_crossing_threshold")
    if raw_mapped_applied_steer_available:
        interpretation.append("raw_mapped_applied_steer_trace_available")
    elif applied_steer:
        interpretation.append("applied_steer_trace_available")
    if control_source == "artifacts/control_apply_trace.jsonl":
        interpretation.append("control_apply_trace_used_for_control_diagnostics")
    else:
        interpretation.append("timeseries_control_fields_used_for_control_diagnostics")
    if (
        mapped_to_applied_steer_error.get("max_abs_error") is not None
        and mapped_to_applied_steer_error["max_abs_error"] > 0.02
    ):
        interpretation.append("mapped_to_applied_steer_mismatch_before_event")
    elif (
        mapped_to_applied_steer_error.get("max_abs_error") is not None
        and mapped_to_applied_steer_error["max_abs_error"] <= 0.005
        and raw_mapped_applied_steer_available
    ):
        interpretation.append("mapped_to_applied_steer_consistent_before_event")
    if applied_nonzero_mapped_zero.get("ratio", 0.0) >= 0.25:
        interpretation.append("applied_steer_nonzero_while_mapped_zero_before_event")
    if _gain_looks_like_legacy_steer_scale(raw_to_mapped_steer_gain):
        interpretation.append("raw_to_mapped_steer_gain_legacy_scale_like")
    if raw_steer_same_sign_as_cte is True:
        interpretation.append("raw_steer_same_sign_as_cross_track_error")

    if (
        distance_from_start_m is not None
        and distance_from_start_m > early_event_distance_m
        and (
            (abs_cte_growth_m is not None and abs_cte_growth_m >= 0.15)
            or (
                cte_abs_increasing_ratio is not None
                and cte_abs_increasing_ratio >= 0.75
                and cte_end is not None
                and abs(cte_end) >= 0.5
            )
        )
    ):
        classification = "downstream_progressive_lane_departure"
    elif distance_from_start_m is not None and distance_from_start_m <= early_event_distance_m:
        classification = "near_start_lane_event"
    else:
        classification = "unclassified_lane_event"

    return {
        "available": True,
        "classification": classification,
        "window_start_row": start_index,
        "trigger_row": trigger_index,
        "sample_count": len(window),
        "window_duration_s": _duration(sim_time),
        "distance_from_start_m": distance_from_start_m,
        "estimated_center_offset_to_cross_mark_m": estimated_crossing_offset_m,
        "clearance_to_marking_m": clearance_to_marking_m,
        "cross_track_error": {
            "start_m": cte_start,
            "end_m": cte_end,
            "delta_m": (cte_end - cte_start) if cte_start is not None and cte_end is not None else None,
            "abs_growth_m": abs_cte_growth_m,
            "abs_increasing_ratio": cte_abs_increasing_ratio,
            "max_abs_m": max((abs(value) for value in cte), default=None),
        },
        "heading_error": {
            "start_rad": heading_start,
            "end_rad": heading_end,
            "delta_rad": (
                heading_end - heading_start
                if heading_start is not None and heading_end is not None
                else None
            ),
            "abs_growth_rad": abs_heading_growth_rad,
            "same_sign_as_cross_track_error": heading_same_sign_as_cte,
        },
        "speed": {
            "start_mps": _first_value(speed),
            "end_mps": _last_value(speed),
            "max_mps": max(speed) if speed else None,
        },
        "control": {
            "source": control_source,
            "control_apply_trace_rows_used": len(control_window),
            "raw_mapped_applied_steer_available": raw_mapped_applied_steer_available,
            "apollo_steer_raw_end": _last_value(raw_steer),
            "bridge_steer_mapped_end": _last_value(mapped_steer),
            "carla_steer_applied_end": _last_value(applied_steer),
            "apollo_steer_raw_summary": _series_summary(raw_steer),
            "bridge_steer_mapped_summary": _series_summary(mapped_steer),
            "carla_steer_applied_summary": _series_summary(applied_steer),
            "raw_to_mapped_steer_abs_error": raw_to_mapped_steer_error,
            "raw_to_mapped_steer_gain": raw_to_mapped_steer_gain,
            "mapped_to_applied_steer_abs_error": mapped_to_applied_steer_error,
            "applied_nonzero_while_mapped_zero": applied_nonzero_mapped_zero,
            "apollo_steer_raw_same_sign_as_cross_track_error": raw_steer_same_sign_as_cte,
            "bridge_steer_mapped_same_sign_as_cross_track_error": mapped_steer_same_sign_as_cte,
            "applied_steer_same_sign_as_cross_track_error": applied_steer_same_sign_as_cte,
            "raw_throttle_end": _last_value(raw_throttle),
            "raw_brake_end": _last_value(raw_brake),
            "applied_throttle_end": _last_value(applied_throttle),
            "applied_brake_end": _last_value(applied_brake),
        },
        "interpretation": interpretation,
    }


def _lane_event_vehicle_response_context(root: Path) -> dict[str, Any]:
    report = _read_json(root / "analysis" / "control_health" / "control_health_report.json")
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    context = (
        metrics.get("lane_event_response_context")
        if isinstance(metrics.get("lane_event_response_context"), Mapping)
        else {}
    )
    if not context:
        return {
            "available": False,
            "reason": "control_health_lane_event_response_context_missing",
            "source": "analysis/control_health/control_health_report.json",
        }
    return {
        "available": True,
        "source": "analysis/control_health/control_health_report.json",
        "control_health_status": report.get("status"),
        "classification": context.get("classification"),
        "cross_track_error_abs_growth_m": context.get("cross_track_error_abs_growth_m"),
        "heading_error_abs_growth_rad": context.get("heading_error_abs_growth_rad"),
        "applied_steer_mean": context.get("applied_steer_mean"),
        "applied_steer_end": context.get("applied_steer_end"),
        "yaw_rate_mean_rad_s": context.get("yaw_rate_mean_rad_s"),
        "yaw_rate_end_rad_s": context.get("yaw_rate_end_rad_s"),
        "vehicle_response_rows_available": context.get("vehicle_response_rows_available"),
        "vehicle_response_sample_count": context.get("vehicle_response_sample_count"),
        "applied_steer_yaw_rate_same_sign": context.get("applied_steer_yaw_rate_same_sign"),
        "cte_growth_yaw_rate_same_sign": context.get("cte_growth_yaw_rate_same_sign"),
        "heading_growth_yaw_rate_same_sign": context.get("heading_growth_yaw_rate_same_sign"),
        "cte_growth_applied_steer_same_sign": context.get("cte_growth_applied_steer_same_sign"),
    }


def _lane_event_path_control_context(root: Path) -> dict[str, Any]:
    report = _read_json(
        root / "analysis" / "apollo_reference_line_contract" / "apollo_reference_line_contract_report.json"
    )
    target_vs_path = (
        report.get("control_target_point_vs_planning_path_candidate_sample")
        if isinstance(report.get("control_target_point_vs_planning_path_candidate_sample"), Mapping)
        else {}
    )
    path_hdmap = (
        report.get("planning_debug_path_candidate_hdmap_projection_alignment")
        if isinstance(report.get("planning_debug_path_candidate_hdmap_projection_alignment"), Mapping)
        else {}
    )
    path_vs_traj = (
        report.get("planning_debug_path_candidate_vs_trajectory_sample")
        if isinstance(report.get("planning_debug_path_candidate_vs_trajectory_sample"), Mapping)
        else {}
    )
    if not target_vs_path and not path_hdmap and not path_vs_traj:
        return {
            "available": False,
            "reason": "reference_line_path_candidate_context_missing",
            "source": "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        }
    return {
        "available": True,
        "source": "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        "reference_line_status": report.get("status"),
        "reference_line_claim_grade_allowed": _first_non_none(
            target_vs_path.get("reference_line_claim_grade_allowed"),
            path_hdmap.get("reference_line_claim_grade_allowed"),
            path_vs_traj.get("reference_line_claim_grade_allowed"),
        ),
        "control_target_vs_path_candidate_classification": target_vs_path.get("classification"),
        "target_inside_path_lateral_envelope": target_vs_path.get(
            "target_inside_path_lateral_envelope"
        ),
        "target_point_lane_l_abs_p95_m": target_vs_path.get("target_point_lane_l_abs_p95_m"),
        "target_point_to_path_candidate_line_abs_p95_m": target_vs_path.get(
            "target_point_to_path_candidate_line_abs_p95_m"
        ),
        "path_candidate_lane_l_min_m": _first_non_none(
            target_vs_path.get("path_candidate_lane_l_min_m"),
            path_hdmap.get("path_candidate_lane_l_min_m"),
        ),
        "path_candidate_lane_l_max_m": _first_non_none(
            target_vs_path.get("path_candidate_lane_l_max_m"),
            path_hdmap.get("path_candidate_lane_l_max_m"),
        ),
        "path_candidate_lane_l_abs_p95_m": _first_non_none(
            target_vs_path.get("path_candidate_lane_l_abs_p95_m"),
            path_hdmap.get("path_candidate_lane_l_abs_p95_m"),
        ),
        "path_candidate_hdmap_classification": path_hdmap.get("classification"),
        "routing_lane_window_compatible": path_hdmap.get("routing_lane_window_compatible"),
        "path_candidate_vs_trajectory_classification": path_vs_traj.get("classification"),
        "path_candidate_to_planning_sample_line_abs_p95_m": path_vs_traj.get(
            "path_candidate_to_planning_sample_line_abs_p95_m"
        ),
        "claim_boundary": (
            "Path candidate and Control target context is diagnostic only. It cannot replace "
            "claim-grade Planning reference-line evidence or make a lane-invasion run pass."
        ),
    }


def _lane_event_route_lateral_sign_policy(root: Path) -> dict[str, Any]:
    phase1 = _read_json(root / "analysis" / "phase1_status" / "phase1_status.json")
    policy = (
        phase1.get("route_lateral_field_policy")
        if isinstance(phase1.get("route_lateral_field_policy"), Mapping)
        else {}
    )
    if policy:
        return {
            "available": True,
            "source": "analysis/phase1_status/phase1_status.json",
            "status": policy.get("status"),
            "policy": policy.get("policy"),
            "source_field": policy.get("source_field"),
            "classification": policy.get("classification"),
            "sign_sensitive_gate_allowed": policy.get("sign_sensitive_gate_allowed"),
            "absolute_magnitude_gate_allowed": policy.get("absolute_magnitude_gate_allowed"),
            "recommended_gate_policy": policy.get("recommended_gate_policy"),
            "recommended_action": policy.get("recommended_action"),
            "projection_route_sample_sign_contract_classification": policy.get(
                "projection_route_sample_sign_contract_classification"
            ),
            "projection_route_sample_timeseries_opposite_sign_ratio": policy.get(
                "projection_route_sample_timeseries_opposite_sign_ratio"
            ),
            "projection_route_sample_simple_lat_same_sign_ratio": policy.get(
                "projection_route_sample_simple_lat_same_sign_ratio"
            ),
            "claim_boundary": (
                "Route-lateral sign policy governs whether cross_track_error-like fields can be "
                "used for sign-sensitive behavior conclusions. If sign-sensitive gates are blocked, "
                "same-sign steer/CTE diagnostics remain attribution hints only."
            ),
        }
    lateral = _read_json(
        root / "analysis" / "apollo_lateral_semantics" / "apollo_lateral_semantics_report.json"
    )
    alignment = (
        lateral.get("lateral_sign_alignment")
        if isinstance(lateral.get("lateral_sign_alignment"), Mapping)
        else {}
    )
    semantics = (
        alignment.get("route_lateral_field_semantics")
        if isinstance(alignment.get("route_lateral_field_semantics"), Mapping)
        else {}
    )
    if semantics:
        return {
            "available": True,
            "source": "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
            "status": semantics.get("status"),
            "policy": (
                "exclude_from_sign_sensitive_behavior_gates"
                if semantics.get("sign_sensitive_gate_allowed") is False
                else None
            ),
            "source_field": semantics.get("source_field"),
            "classification": semantics.get("classification"),
            "sign_sensitive_gate_allowed": semantics.get("sign_sensitive_gate_allowed"),
            "absolute_magnitude_gate_allowed": semantics.get("absolute_magnitude_gate_allowed"),
            "recommended_gate_policy": semantics.get("recommended_gate_policy"),
            "recommended_action": semantics.get("recommended_field_action"),
            "projection_route_sample_sign_contract_classification": semantics.get(
                "projection_route_sample_sign_contract_classification"
            ),
            "projection_route_sample_timeseries_opposite_sign_ratio": semantics.get(
                "projection_route_sample_timeseries_opposite_sign_ratio"
            ),
            "projection_route_sample_simple_lat_same_sign_ratio": semantics.get(
                "projection_route_sample_simple_lat_same_sign_ratio"
            ),
            "claim_boundary": (
                "Route-lateral sign policy governs whether cross_track_error-like fields can be "
                "used for sign-sensitive behavior conclusions. If sign-sensitive gates are blocked, "
                "same-sign steer/CTE diagnostics remain attribution hints only."
            ),
        }
    return {
        "available": False,
        "reason": "route_lateral_sign_policy_missing",
        "source": "analysis/phase1_status/phase1_status.json",
    }


def _lane_event_projection_lateral_context(
    root: Path,
    *,
    route_lateral_sign_policy: Mapping[str, Any],
) -> dict[str, Any]:
    lateral = _read_json(
        root / "analysis" / "apollo_lateral_semantics" / "apollo_lateral_semantics_report.json"
    )
    alignment = (
        lateral.get("lateral_sign_alignment")
        if isinstance(lateral.get("lateral_sign_alignment"), Mapping)
        else {}
    )
    if alignment:
        frame = (
            alignment.get("lateral_frame_convention_diagnostic")
            if isinstance(alignment.get("lateral_frame_convention_diagnostic"), Mapping)
            else {}
        )
        official = (
            alignment.get("official_hdmap_projection_alignment")
            if isinstance(alignment.get("official_hdmap_projection_alignment"), Mapping)
            else {}
        )
        provenance = (
            alignment.get("route_lateral_provenance")
            if isinstance(alignment.get("route_lateral_provenance"), Mapping)
            else {}
        )
        projection_contract = (
            provenance.get("projection_route_sample_sign_contract")
            if isinstance(provenance.get("projection_route_sample_sign_contract"), Mapping)
            else {}
        )
        timeseries_vs_projection = (
            projection_contract.get("timeseries_lateral_vs_projection_route_sample_signed_lateral")
            if isinstance(
                projection_contract.get("timeseries_lateral_vs_projection_route_sample_signed_lateral"),
                Mapping,
            )
            else {}
        )
        simple_lat_vs_projection = (
            projection_contract.get("simple_lat_vs_projection_route_sample_signed_lateral")
            if isinstance(
                projection_contract.get("simple_lat_vs_projection_route_sample_signed_lateral"),
                Mapping,
            )
            else {}
        )
        semantics = (
            alignment.get("route_lateral_field_semantics")
            if isinstance(alignment.get("route_lateral_field_semantics"), Mapping)
            else {}
        )
        magnitude = (
            alignment.get("route_simple_lat_magnitude_alignment")
            if isinstance(alignment.get("route_simple_lat_magnitude_alignment"), Mapping)
            else {}
        )
        station = (
            alignment.get("route_station_frame_alignment")
            if isinstance(alignment.get("route_station_frame_alignment"), Mapping)
            else {}
        )
        return {
            "available": True,
            "source": "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
            "classification": _first_non_none(
                frame.get("classification"),
                semantics.get("classification"),
                projection_contract.get("classification"),
            ),
            "route_lateral_source_field": _first_non_none(
                semantics.get("source_field"),
                projection_contract.get("route_lateral_source_field"),
                route_lateral_sign_policy.get("source_field"),
            ),
            "sign_sensitive_gate_allowed": _first_non_none(
                semantics.get("sign_sensitive_gate_allowed"),
                route_lateral_sign_policy.get("sign_sensitive_gate_allowed"),
            ),
            "recommended_gate_policy": _first_non_none(
                semantics.get("recommended_gate_policy"),
                route_lateral_sign_policy.get("recommended_gate_policy"),
            ),
            "recommended_action": _first_non_none(
                semantics.get("recommended_field_action"),
                route_lateral_sign_policy.get("recommended_action"),
            ),
            "official_hdmap_projection_available": bool(official.get("available")),
            "official_hdmap_projection_matched_sample_count": official.get("matched_sample_count"),
            "projection_route_sample_contract_available": bool(projection_contract.get("available")),
            "projection_route_sample_contract_classification": projection_contract.get("classification"),
            "projection_route_sample_matched_sample_count": projection_contract.get("matched_sample_count"),
            "projection_route_sample_count": projection_contract.get("route_sample_count"),
            "timeseries_lateral_vs_projection_route_sample_opposite_sign_ratio": (
                timeseries_vs_projection.get("opposite_sign_ratio")
            ),
            "timeseries_lateral_vs_projection_route_sample_sample_count": (
                timeseries_vs_projection.get("sample_count")
            ),
            "simple_lat_vs_projection_route_sample_same_sign_ratio": (
                simple_lat_vs_projection.get("same_sign_ratio")
            ),
            "simple_lat_vs_projection_route_sample_sample_count": (
                simple_lat_vs_projection.get("sample_count")
            ),
            "route_simple_lat_magnitude_agreement_candidate": magnitude.get(
                "magnitude_agreement_candidate"
            ),
            "route_simple_lat_opposite_sign_ratio": magnitude.get("opposite_sign_ratio"),
            "route_simple_lat_abs_magnitude_delta_p95_m": magnitude.get("abs_magnitude_delta_p95_m"),
            "route_station_frame_classification": station.get("classification"),
            "claim_boundary": (
                "Projection/simple_lat lateral context is diagnostic evidence for analysis-field "
                "semantics. It does not change runtime control, does not prove behavior success, "
                "and does not replace claim-grade Planning reference-line evidence."
            ),
        }
    if route_lateral_sign_policy.get("available"):
        return {
            "available": True,
            "source": str(route_lateral_sign_policy.get("source") or "route_lateral_sign_policy"),
            "classification": route_lateral_sign_policy.get("classification"),
            "route_lateral_source_field": route_lateral_sign_policy.get("source_field"),
            "sign_sensitive_gate_allowed": route_lateral_sign_policy.get(
                "sign_sensitive_gate_allowed"
            ),
            "recommended_gate_policy": route_lateral_sign_policy.get("recommended_gate_policy"),
            "recommended_action": route_lateral_sign_policy.get("recommended_action"),
            "projection_route_sample_contract_classification": route_lateral_sign_policy.get(
                "projection_route_sample_sign_contract_classification"
            ),
            "timeseries_lateral_vs_projection_route_sample_opposite_sign_ratio": (
                route_lateral_sign_policy.get("projection_route_sample_timeseries_opposite_sign_ratio")
            ),
            "simple_lat_vs_projection_route_sample_same_sign_ratio": (
                route_lateral_sign_policy.get("projection_route_sample_simple_lat_same_sign_ratio")
            ),
            "claim_boundary": (
                "Projection/simple_lat lateral context fell back to the route-lateral sign policy. "
                "It remains diagnostic-only and cannot prove behavior success."
            ),
        }
    return {
        "available": False,
        "reason": "projection_lateral_context_missing",
        "source": "analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json",
    }


def _lane_event_attribution(
    departure: Mapping[str, Any],
    *,
    vehicle_response_context: Mapping[str, Any],
    path_control_context: Mapping[str, Any],
    route_lateral_sign_policy: Mapping[str, Any],
) -> dict[str, Any]:
    if not departure.get("available"):
        return {
            "available": False,
            "classification": "insufficient_data",
            "reason": "departure_diagnostics_missing",
            "claim_boundary": "No behavior claim can be made from missing lane-event diagnostics.",
        }
    classification = "lane_event_attribution_available"
    reasons: list[str] = []
    if departure.get("classification") == "downstream_progressive_lane_departure":
        classification = "downstream_progressive_lane_departure"
        reasons.append("departure_diagnostics_progressive")
    vehicle_tracks = (
        vehicle_response_context.get("classification")
        == "applied_steer_yaw_response_tracks_progressive_lateral_departure"
    )
    if vehicle_tracks:
        reasons.append("vehicle_response_tracks_applied_steer_yaw_and_lateral_departure")
    target_between = (
        path_control_context.get("control_target_vs_path_candidate_classification")
        == "control_target_between_planning_path_candidate_lateral_bounds"
        or path_control_context.get("target_inside_path_lateral_envelope") is True
    )
    if target_between:
        reasons.append("control_target_between_path_candidate_lateral_bounds")
    sign_sensitive_allowed = route_lateral_sign_policy.get("sign_sensitive_gate_allowed")
    if sign_sensitive_allowed is False:
        reasons.append("route_lateral_sign_sensitive_gate_blocked")
    if (
        departure.get("classification") == "downstream_progressive_lane_departure"
        and vehicle_tracks
        and target_between
    ):
        classification = (
            "progressive_lane_departure_with_vehicle_response_and_control_target_between_path_candidate_bounds"
        )
    elif departure.get("classification") == "downstream_progressive_lane_departure" and vehicle_tracks:
        classification = "progressive_lane_departure_with_vehicle_response"
    elif departure.get("classification") == "downstream_progressive_lane_departure" and target_between:
        classification = "progressive_lane_departure_with_control_target_between_path_candidate_bounds"
    return {
        "available": True,
        "classification": classification,
        "reasons": reasons,
        "vehicle_response_context_available": bool(vehicle_response_context.get("available")),
        "path_control_context_available": bool(path_control_context.get("available")),
        "route_lateral_sign_policy_available": bool(route_lateral_sign_policy.get("available")),
        "route_lateral_sign_sensitive_gate_allowed": sign_sensitive_allowed,
        "route_lateral_field_policy": route_lateral_sign_policy.get("policy"),
        "route_lateral_source_field": route_lateral_sign_policy.get("source_field"),
        "route_lateral_field_classification": route_lateral_sign_policy.get("classification"),
        "route_lateral_recommended_action": route_lateral_sign_policy.get("recommended_action"),
        "reference_line_claim_grade_allowed": path_control_context.get(
            "reference_line_claim_grade_allowed"
        ),
        "claim_boundary": (
            "This is lane-event attribution evidence only. It does not alter the lane-event "
            "hard gate, does not prove Apollo natural driving, and must not be used to bypass "
            "reference-line or no-assist claim gates."
        ),
    }


def _trigger_row_index(rows: list[dict[str, str]], trigger: Mapping[str, Any]) -> int | None:
    row_frame = _number(trigger.get("row_frame"), trigger.get("event_frame"))
    row_frame_id = _number(trigger.get("row_frame_id"), trigger.get("event_frame"))
    for index, row in enumerate(rows):
        if row_frame is not None and _number(row.get("frame")) == row_frame:
            return index
        if row_frame_id is not None and _number(row.get("frame_id")) == row_frame_id:
            return index
    for index, row in enumerate(rows):
        if (_number(row.get("lane_invasion_count")) or 0.0) > 0.0:
            return index
    return None


def _series_aliases(rows: list[dict[str, str]], fields: tuple[str, ...]) -> list[float]:
    values: list[float] = []
    for row in rows:
        for field in fields:
            value = _number(row.get(field))
            if value is not None:
                values.append(value)
                break
    return values


def _control_apply_rows_for_window(
    rows: list[dict[str, Any]],
    sim_time_values: list[float],
) -> list[dict[str, Any]]:
    if not rows or len(sim_time_values) < 2:
        return []
    start = min(sim_time_values)
    end = max(sim_time_values)
    matched: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _number(row.get("sim_time"), row.get("timestamp"))
        if timestamp is None:
            continue
        if start <= timestamp <= end:
            matched.append(row)
    return matched


def _nested_series(
    rows: list[dict[str, Any]],
    paths: tuple[tuple[str, ...], ...],
) -> list[float]:
    values: list[float] = []
    for row in rows:
        for path in paths:
            current: Any = row
            for key in path:
                if not isinstance(current, Mapping):
                    current = None
                    break
                current = current.get(key)
            value = _number(current)
            if value is not None:
                values.append(value)
                break
    return values


def _series_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "sample_count": 0,
            "first": None,
            "last": None,
            "min": None,
            "max": None,
            "mean": None,
            "max_abs": None,
        }
    return {
        "sample_count": len(values),
        "first": values[0],
        "last": values[-1],
        "min": min(values),
        "max": max(values),
        "mean": sum(values) / len(values),
        "max_abs": max(abs(value) for value in values),
    }


def _pairwise_abs_diff_summary(left: list[float], right: list[float]) -> dict[str, Any]:
    sample_count = min(len(left), len(right))
    if sample_count <= 0:
        return {
            "sample_count": 0,
            "max_abs_error": None,
            "mean_abs_error": None,
            "last_abs_error": None,
        }
    diffs = [abs(right[index] - left[index]) for index in range(sample_count)]
    return {
        "sample_count": sample_count,
        "max_abs_error": max(diffs),
        "mean_abs_error": sum(diffs) / sample_count,
        "last_abs_error": diffs[-1],
    }


def _pairwise_ratio_summary(
    numerator: list[float],
    denominator: list[float],
    *,
    denominator_epsilon: float = 1e-6,
) -> dict[str, Any]:
    sample_count = min(len(numerator), len(denominator))
    ratios = [
        numerator[index] / denominator[index]
        for index in range(sample_count)
        if abs(denominator[index]) > float(denominator_epsilon)
    ]
    if not ratios:
        return {
            "sample_count": 0,
            "mean": None,
            "min": None,
            "max": None,
            "last": None,
            "max_abs_deviation_from_mean": None,
        }
    mean = sum(ratios) / len(ratios)
    return {
        "sample_count": len(ratios),
        "mean": mean,
        "min": min(ratios),
        "max": max(ratios),
        "last": ratios[-1],
        "max_abs_deviation_from_mean": max(abs(value - mean) for value in ratios),
    }


def _gain_looks_like_legacy_steer_scale(gain: Mapping[str, Any]) -> bool:
    mean = _number(gain.get("mean"))
    spread = _number(gain.get("max_abs_deviation_from_mean"))
    samples = int(_number(gain.get("sample_count")) or 0)
    return bool(
        samples >= 2
        and mean is not None
        and spread is not None
        and abs(mean - 0.25) <= 0.04
        and spread <= 0.04
    )


def _nonzero_while_reference_zero(
    values: list[float],
    reference: list[float],
    *,
    value_epsilon: float,
    reference_epsilon: float,
) -> dict[str, Any]:
    sample_count = min(len(values), len(reference))
    if sample_count <= 0:
        return {"sample_count": 0, "count": 0, "ratio": 0.0}
    count = sum(
        1
        for index in range(sample_count)
        if abs(values[index]) > float(value_epsilon)
        and abs(reference[index]) <= float(reference_epsilon)
    )
    return {
        "sample_count": sample_count,
        "count": count,
        "ratio": count / sample_count,
        "value_epsilon": float(value_epsilon),
        "reference_epsilon": float(reference_epsilon),
    }


def _nondecreasing_abs_ratio(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    comparisons = 0
    nondecreasing = 0
    previous = abs(values[0])
    for value in values[1:]:
        current = abs(value)
        comparisons += 1
        if current >= previous:
            nondecreasing += 1
        previous = current
    return nondecreasing / comparisons if comparisons else None


def _same_sign(left: float | None, right: float | None) -> bool | None:
    if left is None or right is None:
        return None
    if abs(left) < 1e-9 or abs(right) < 1e-9:
        return None
    return (left > 0.0) == (right > 0.0)


def _duration(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return values[-1] - values[0]


def _first_value(values: list[float]) -> float | None:
    return values[0] if values else None


def _last_value(values: list[float]) -> float | None:
    return values[-1] if values else None


def _parse_list_like(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text.replace("'", '"'))
    except Exception:
        parsed = None
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [text]


def _bool_or_none(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _target_lane_marking_types(target_lane: LaneContract | None) -> list[str]:
    if target_lane is None:
        return []
    types: list[str] = []
    for mark in target_lane.road_marks:
        for item in _normalize_marking_types(mark.mark_type):
            types.append(item)
    return sorted(dict.fromkeys(types))


def _normalize_marking_types(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    raw_items = value if isinstance(value, list) else [value]
    normalized: list[str] = []
    for raw in raw_items:
        text = str(raw or "").strip()
        if not text:
            continue
        # CARLA/Python may stringify enum values as "LaneMarkingType.Broken".
        text = text.split(".")[-1]
        for part in text.replace("_", " ").split():
            if part:
                normalized.append(part.lower())
    return sorted(dict.fromkeys(normalized))


def _ego_half_width_report(root: Path, rows: list[dict[str, str]], default_half_width_m: float) -> dict[str, Any]:
    bbox_extent_y = _first_number(rows, "bbox_extent_y_m")
    bbox_extent_x = _first_number(rows, "bbox_extent_x_m")
    if bbox_extent_y is not None and bbox_extent_y > 0.0:
        payload = {
            "ego_half_width_m": bbox_extent_y,
            "source": "timeseries.bbox_extent_y_m",
            "confidence": "runtime_bbox",
        }
        if bbox_extent_x is not None and bbox_extent_x > 0.0:
            payload["ego_half_length_m"] = bbox_extent_x
        return payload
    ego_width = _first_number(rows, "ego_width_m")
    ego_length = _first_number(rows, "ego_length_m")
    if ego_width is not None and ego_width > 0.0:
        payload = {
            "ego_half_width_m": ego_width / 2.0,
            "ego_width_m": ego_width,
            "source": "timeseries.ego_width_m",
            "confidence": "runtime_bbox",
        }
        if ego_length is not None and ego_length > 0.0:
            payload["ego_half_length_m"] = ego_length / 2.0
            payload["ego_length_m"] = ego_length
        return payload
    characteristics = _read_json(root / "artifacts" / "carla_vehicle_characteristics.json")
    left = _number(characteristics.get("left_edge_to_center"))
    right = _number(characteristics.get("right_edge_to_center"))
    front = _number(characteristics.get("front_edge_to_center"))
    back = _number(characteristics.get("back_edge_to_center"))
    if left is not None or right is not None:
        half_width = max(value for value in (left, right) if value is not None)
        payload = {
            "ego_half_width_m": half_width,
            "left_edge_to_center_m": left,
            "right_edge_to_center_m": right,
            "source": "artifacts/carla_vehicle_characteristics.json",
            "confidence": "runtime_vehicle_characteristics",
        }
        if front is not None or back is not None:
            payload["ego_half_length_m"] = max(value for value in (front, back) if value is not None)
            payload["front_edge_to_center_m"] = front
            payload["back_edge_to_center_m"] = back
        return payload
    width = _number(characteristics.get("width"))
    length = _number(characteristics.get("length"))
    if width is not None and width > 0.0:
        payload = {
            "ego_half_width_m": width / 2.0,
            "ego_width_m": width,
            "source": "artifacts/carla_vehicle_characteristics.json:width",
            "confidence": "runtime_vehicle_characteristics",
        }
        if length is not None and length > 0.0:
            payload["ego_half_length_m"] = length / 2.0
            payload["ego_length_m"] = length
        return payload
    return {
        "ego_half_width_m": float(default_half_width_m),
        "source": "default",
        "confidence": "assumed",
    }


def _overall_status(xodr_report: Mapping[str, Any], run_reports: list[Mapping[str, Any]]) -> str:
    if xodr_report.get("target_lane") is None:
        return "fail"
    if any(report.get("quarantine_recommended") for report in run_reports):
        return "warn"
    if any(report.get("status") == "fail" for report in run_reports):
        return "fail"
    return "pass"


def _representative_run_context(run_reports: list[Mapping[str, Any]]) -> dict[str, Any]:
    if not run_reports:
        return {
            "available": False,
            "reason": "no_run_reports",
            "claim_boundary": "No lane-event attribution can be summarized without run reports.",
        }
    selected = next(
        (
            report
            for report in run_reports
            if report.get("status") == "fail" or (report.get("lane_event_count") or 0) > 0
        ),
        run_reports[0],
    )
    attribution = (
        selected.get("lane_event_attribution")
        if isinstance(selected.get("lane_event_attribution"), Mapping)
        else {}
    )
    sign_policy = (
        selected.get("route_lateral_sign_policy")
        if isinstance(selected.get("route_lateral_sign_policy"), Mapping)
        else {}
    )
    projection_context = (
        selected.get("projection_lateral_context")
        if isinstance(selected.get("projection_lateral_context"), Mapping)
        else {}
    )
    path_control = (
        selected.get("path_control_context")
        if isinstance(selected.get("path_control_context"), Mapping)
        else {}
    )
    vehicle_response = (
        selected.get("vehicle_response_context")
        if isinstance(selected.get("vehicle_response_context"), Mapping)
        else {}
    )
    departure = (
        selected.get("departure_diagnostics")
        if isinstance(selected.get("departure_diagnostics"), Mapping)
        else {}
    )
    first_lane_invasion = (
        selected.get("first_lane_invasion")
        if isinstance(selected.get("first_lane_invasion"), Mapping)
        else {}
    )
    static_crossing = (
        selected.get("static_crossing_check")
        if isinstance(selected.get("static_crossing_check"), Mapping)
        else {}
    )
    return {
        "available": True,
        "run_dir": selected.get("run_dir"),
        "status": selected.get("status"),
        "reason": selected.get("reason"),
        "lane_event_count": selected.get("lane_event_count"),
        "lane_invasion_event_can_be_used_as_hard_gate": selected.get(
            "lane_invasion_event_can_be_used_as_hard_gate"
        ),
        "departure_classification": departure.get("classification"),
        "departure_interpretation": departure.get("interpretation"),
        "first_lane_invasion_event_time_s": first_lane_invasion.get("event_time_s"),
        "first_lane_invasion_row_frame": first_lane_invasion.get("row_frame"),
        "first_lane_invasion_distance_from_start_x_m": first_lane_invasion.get(
            "distance_from_start_x_m"
        ),
        "first_lane_invasion_cross_track_error_m": first_lane_invasion.get(
            "cross_track_error"
        ),
        "first_lane_invasion_heading_error_rad": first_lane_invasion.get(
            "heading_error"
        ),
        "static_crossing_trigger_clearance_to_marking_m": static_crossing.get(
            "trigger_clearance_to_marking_m"
        ),
        "static_crossing_trigger_footprint_intersects_marking": static_crossing.get(
            "trigger_footprint_intersects_marking"
        ),
        "static_crossing_trigger_geometrically_implausible": static_crossing.get(
            "trigger_geometrically_implausible"
        ),
        "lane_event_attribution_classification": attribution.get("classification"),
        "lane_event_attribution_reasons": attribution.get("reasons"),
        "vehicle_response_classification": vehicle_response.get("classification"),
        "path_control_classification": path_control.get(
            "control_target_vs_path_candidate_classification"
        ),
        "reference_line_claim_grade_allowed": path_control.get("reference_line_claim_grade_allowed"),
        "route_lateral_sign_policy": sign_policy.get("policy"),
        "route_lateral_sign_sensitive_gate_allowed": sign_policy.get(
            "sign_sensitive_gate_allowed"
        ),
        "route_lateral_source_field": sign_policy.get("source_field"),
        "route_lateral_field_classification": sign_policy.get("classification"),
        "route_lateral_recommended_action": sign_policy.get("recommended_action"),
        "projection_lateral_context_classification": projection_context.get("classification"),
        "projection_lateral_context_source": projection_context.get("source"),
        "projection_route_sample_contract_classification": projection_context.get(
            "projection_route_sample_contract_classification"
        ),
        "timeseries_lateral_vs_projection_route_sample_opposite_sign_ratio": (
            projection_context.get(
                "timeseries_lateral_vs_projection_route_sample_opposite_sign_ratio"
            )
        ),
        "simple_lat_vs_projection_route_sample_same_sign_ratio": projection_context.get(
            "simple_lat_vs_projection_route_sample_same_sign_ratio"
        ),
        "route_simple_lat_magnitude_agreement_candidate": projection_context.get(
            "route_simple_lat_magnitude_agreement_candidate"
        ),
        "route_station_frame_classification": projection_context.get(
            "route_station_frame_classification"
        ),
        "claim_boundary": (
            "Representative context is an audit-friendly summary of one run report. It is "
            "diagnostic-only and does not change lane-event status, reference-line claim-grade, "
            "or no-assist natural-driving gates."
        ),
    }


def _summary_markdown(report: Mapping[str, Any]) -> str:
    xodr = report.get("xodr") if isinstance(report.get("xodr"), Mapping) else {}
    representative = (
        report.get("representative_run_context")
        if isinstance(report.get("representative_run_context"), Mapping)
        else {}
    )
    lines = [
        "# Baguang Lane Event Contract",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- quarantine_recommended: `{report.get('quarantine_recommended')}`",
        f"- representative_reason: `{representative.get('reason')}`",
        f"- representative_departure_classification: `{representative.get('departure_classification')}`",
        f"- representative_first_lane_invasion_distance_from_start_x_m: `{representative.get('first_lane_invasion_distance_from_start_x_m')}`",
        f"- representative_first_lane_invasion_cross_track_error_m: `{representative.get('first_lane_invasion_cross_track_error_m')}`",
        f"- representative_static_crossing_trigger_clearance_to_marking_m: `{representative.get('static_crossing_trigger_clearance_to_marking_m')}`",
        f"- representative_static_crossing_trigger_footprint_intersects_marking: `{representative.get('static_crossing_trigger_footprint_intersects_marking')}`",
        f"- representative_lane_event_attribution: `{representative.get('lane_event_attribution_classification')}`",
        f"- representative_route_lateral_sign_policy: `{representative.get('route_lateral_sign_policy')}`",
        f"- representative_route_lateral_sign_sensitive_gate_allowed: `{representative.get('route_lateral_sign_sensitive_gate_allowed')}`",
        f"- representative_projection_lateral_context: `{representative.get('projection_lateral_context_classification')}`",
        f"- xodr: `{xodr.get('path')}`",
        f"- driving_lane_ids: `{xodr.get('driving_lane_ids')}`",
        f"- target_lane_center_crossing_margin_m: `{xodr.get('target_lane_center_crossing_margin_m')}`",
        "",
        "## Run Results",
    ]
    for item in report.get("run_reports") or []:
        if not isinstance(item, Mapping):
            continue
        trigger = item.get("first_lane_invasion") if isinstance(item.get("first_lane_invasion"), Mapping) else {}
        marking = item.get("crossed_marking_check") if isinstance(item.get("crossed_marking_check"), Mapping) else {}
        offset_sweep = item.get("offset_sweep") if isinstance(item.get("offset_sweep"), Mapping) else {}
        departure = item.get("departure_diagnostics") if isinstance(item.get("departure_diagnostics"), Mapping) else {}
        attribution = item.get("lane_event_attribution") if isinstance(item.get("lane_event_attribution"), Mapping) else {}
        vehicle_response = item.get("vehicle_response_context") if isinstance(item.get("vehicle_response_context"), Mapping) else {}
        path_control = item.get("path_control_context") if isinstance(item.get("path_control_context"), Mapping) else {}
        sign_policy = item.get("route_lateral_sign_policy") if isinstance(item.get("route_lateral_sign_policy"), Mapping) else {}
        projection_context = (
            item.get("projection_lateral_context")
            if isinstance(item.get("projection_lateral_context"), Mapping)
            else {}
        )
        control = departure.get("control") if isinstance(departure.get("control"), Mapping) else {}
        mapped_to_applied = (
            control.get("mapped_to_applied_steer_abs_error")
            if isinstance(control.get("mapped_to_applied_steer_abs_error"), Mapping)
            else {}
        )
        raw_to_mapped_gain = (
            control.get("raw_to_mapped_steer_gain")
            if isinstance(control.get("raw_to_mapped_steer_gain"), Mapping)
            else {}
        )
        applied_nonzero = (
            control.get("applied_nonzero_while_mapped_zero")
            if isinstance(control.get("applied_nonzero_while_mapped_zero"), Mapping)
            else {}
        )
        lines.extend(
            [
                "",
                f"- run_dir: `{item.get('run_dir')}`",
                f"- status: `{item.get('status')}`",
                f"- reason: `{item.get('reason')}`",
                f"- quarantine_recommended: `{item.get('quarantine_recommended')}`",
                f"- trigger_cross_track_error_m: `{trigger.get('cross_track_error')}`",
                f"- trigger_heading_error_rad: `{trigger.get('heading_error')}`",
                f"- trigger_distance_from_start_x_m: `{trigger.get('distance_from_start_x_m')}`",
                f"- crossed_lane_marking_types: `{marking.get('crossed_lane_marking_types')}`",
                f"- target_lane_roadmark_types: `{marking.get('target_lane_roadmark_types')}`",
                f"- crossed_marking_types_match_target_lane: `{marking.get('types_match_target_lane')}`",
                f"- ego_half_width_m: `{(item.get('vehicle_footprint') or {}).get('ego_half_width_m')}`",
                f"- trigger_clearance_to_marking_m: `{(item.get('static_crossing_check') or {}).get('trigger_clearance_to_marking_m')}`",
                f"- trigger_footprint_intersects_marking: `{(item.get('static_crossing_check') or {}).get('trigger_footprint_intersects_marking')}`",
                f"- departure_classification: `{departure.get('classification')}`",
                f"- lane_event_attribution: `{attribution.get('classification')}`",
                f"- lane_event_attribution_reasons: `{attribution.get('reasons')}`",
                f"- vehicle_response_classification: `{vehicle_response.get('classification')}`",
                f"- path_control_classification: `{path_control.get('control_target_vs_path_candidate_classification')}`",
                f"- path_control_reference_line_claim_grade_allowed: `{path_control.get('reference_line_claim_grade_allowed')}`",
                f"- route_lateral_sign_policy: `{sign_policy.get('policy')}`",
                f"- route_lateral_sign_sensitive_gate_allowed: `{sign_policy.get('sign_sensitive_gate_allowed')}`",
                f"- route_lateral_recommended_action: `{sign_policy.get('recommended_action')}`",
                f"- projection_lateral_context: `{projection_context.get('classification')}`",
                f"- projection_route_sample_contract: `{projection_context.get('projection_route_sample_contract_classification')}`",
                f"- timeseries_lateral_vs_projection_route_sample_opposite_sign_ratio: `{projection_context.get('timeseries_lateral_vs_projection_route_sample_opposite_sign_ratio')}`",
                f"- simple_lat_vs_projection_route_sample_same_sign_ratio: `{projection_context.get('simple_lat_vs_projection_route_sample_same_sign_ratio')}`",
                f"- departure_interpretation: `{departure.get('interpretation')}`",
                f"- departure_control_source: `{control.get('source')}`",
                f"- control_apply_trace_rows_used: `{control.get('control_apply_trace_rows_used')}`",
                f"- raw_to_mapped_steer_gain_mean: `{raw_to_mapped_gain.get('mean')}`",
                f"- apollo_steer_raw_same_sign_as_cross_track_error: `{control.get('apollo_steer_raw_same_sign_as_cross_track_error')}`",
                f"- mapped_to_applied_steer_max_abs_error: `{mapped_to_applied.get('max_abs_error')}`",
                f"- applied_nonzero_while_mapped_zero_ratio: `{applied_nonzero.get('ratio')}`",
                f"- offset_sweep_road_start_only_trigger: `{offset_sweep.get('road_start_only_trigger')}`",
                f"- offset_sweep_min_clear_offset_without_event_m: `{offset_sweep.get('min_clear_offset_without_event_m')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "A low cross-track-error lane-invasion trigger is treated as a CARLA/OpenDRIVE lane-event contract issue, not as AD-stack lane departure evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def _lane_to_dict(lane: LaneContract | None) -> dict[str, Any] | None:
    if lane is None:
        return None
    return {
        "road_id": lane.road_id,
        "lane_id": lane.lane_id,
        "lane_type": lane.lane_type,
        "travel_dir": lane.travel_dir,
        "width_m": lane.width_m,
        "road_marks": [
            {
                "s_offset": mark.s_offset,
                "type": mark.mark_type,
                "color": mark.color,
                "width_m": mark.width_m,
                "lane_change": mark.lane_change,
            }
            for mark in lane.road_marks
        ],
    }


def _find_lane(lanes: list[LaneContract], lane_id: int) -> LaneContract | None:
    return next((lane for lane in lanes if lane.lane_id == lane_id), None)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_events(path: Path) -> list[dict[str, Any]]:
    return _read_jsonl_rows(path)


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _read_lane_events(root: Path, rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in (root / "events.jsonl", root / "artifacts" / "safety_event_trace.jsonl"):
        for event in _read_events(path):
            payload = dict(event)
            payload.setdefault("source", str(path.relative_to(root)))
            events.append(payload)
    if not any(str(event.get("event_type")) == "lane_invasion" for event in events):
        first = next((row for row in rows if (_number(row.get("lane_invasion_count")) or 0.0) > 0.0), None)
        if first is not None:
            events.append(
                {
                    "event_type": "lane_invasion",
                    "frame": first.get("frame") or first.get("frame_id"),
                    "timestamp": first.get("sim_time") or first.get("t"),
                    "source": "timeseries.lane_invasion_count",
                }
            )
    return events


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _missing_inputs(root: Path) -> list[str]:
    missing: list[str] = []
    if not (root / "summary.json").exists():
        missing.append("summary.json")
    if not (root / "timeseries.csv").exists():
        missing.append("timeseries.csv")
    if not (root / "events.jsonl").exists() and not (root / "artifacts" / "safety_event_trace.jsonl").exists():
        missing.append("events.jsonl_or_artifacts/safety_event_trace.jsonl")
    return missing


def _first_float_attr(elements: list[ET.Element], attr: str) -> float | None:
    for elem in elements:
        value = _float_or_none(elem.attrib.get(attr))
        if value is not None:
            return value
    return None


def _float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _number(*values: object) -> float | None:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _max_abs(rows: list[dict[str, str]], field: str) -> float | None:
    values = [abs(value) for row in rows if (value := _number(row.get(field))) is not None]
    return max(values) if values else None


def _max_abs_aliases(rows: list[dict[str, str]], fields: tuple[str, ...]) -> float | None:
    values = [
        abs(value)
        for row in rows
        for field in fields
        if (value := _number(row.get(field))) is not None
    ]
    return max(values) if values else None


def _first_number(rows: list[dict[str, str]], field: str) -> float | None:
    for row in rows:
        value = _number(row.get(field))
        if value is not None:
            return value
    return None


def _first_number_aliases(rows: list[dict[str, str]], fields: tuple[str, ...]) -> float | None:
    for row in rows:
        for field in fields:
            value = _number(row.get(field))
            if value is not None:
                return value
    return None


def _last_number(rows: list[dict[str, str]], field: str) -> float | None:
    for row in reversed(rows):
        value = _number(row.get(field))
        if value is not None:
            return value
    return None


def _displacement_x(rows: list[dict[str, str]]) -> float | None:
    first = _first_number(rows, "ego_x")
    last = _last_number(rows, "ego_x")
    if first is None or last is None:
        return None
    return abs(last - first)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Baguang CARLA lane-invasion contract evidence.")
    parser.add_argument("--xodr", default=str(DEFAULT_BAGUANG_XODR), help="Baguang OpenDRIVE .xodr path.")
    parser.add_argument("--run-dir", action="append", default=[], help="Run directory to analyze. Repeatable.")
    parser.add_argument("--out", required=True, help="Output directory.")
    parser.add_argument("--target-lane-id", type=int, default=DEFAULT_TARGET_LANE_ID)
    parser.add_argument("--ego-half-width-m", type=float, default=DEFAULT_EGO_HALF_WIDTH_M)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_baguang_lane_event_contract(
        xodr_path=args.xodr,
        run_dirs=args.run_dir,
        target_lane_id=args.target_lane_id,
        ego_half_width_m=args.ego_half_width_m,
    )
    outputs = write_baguang_lane_event_contract_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "outputs": outputs}, indent=2, sort_keys=True))
    return 1 if report.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
