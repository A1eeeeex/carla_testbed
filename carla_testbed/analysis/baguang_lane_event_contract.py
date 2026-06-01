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
    return {
        "schema_version": BAGUANG_LANE_EVENT_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "map_name": "straight_road_for_baguang",
        "xodr": xodr_report,
        "run_reports": run_reports,
        "quarantine_recommended": quarantine,
        "claim_boundary": {
            "lane_invasion_event_can_be_used_as_hard_gate": not quarantine,
            "reason": (
                "lane_invasion_trigger_inconsistent_with_centerline_evidence"
                if quarantine
                else "no_inconsistent_lane_invasion_trigger_observed"
            ),
        },
        "thresholds": {
            "ego_half_width_m": float(ego_half_width_m),
            "cte_warn_m": float(cte_warn_m),
            "heading_warn_rad": float(heading_warn_rad),
            "early_event_distance_m": float(early_event_distance_m),
        },
    }


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
    events = _read_events(root / "events.jsonl")
    rows = _read_csv_rows(root / "timeseries.csv")
    lane_events = [event for event in events if str(event.get("event_type")) == "lane_invasion"]
    trigger = _first_lane_trigger(lane_events, rows)
    max_abs_cte = _max_abs(rows, "cross_track_error")
    max_abs_heading = _max_abs(rows, "heading_error")
    final_lane_count = _last_number(rows, "lane_invasion_count")
    final_displacement = _displacement_x(rows)
    target_width = target_lane.width_m if target_lane is not None else None
    estimated_crossing_offset = _estimated_crossing_offset(
        lane_width_m=target_width,
        ego_half_width_m=ego_half_width_m,
        target_lane=target_lane,
    )
    trigger_cte = _number(trigger.get("cross_track_error")) if trigger else None
    trigger_heading = _number(trigger.get("heading_error")) if trigger else None
    trigger_displacement = _number(trigger.get("distance_from_start_x_m")) if trigger else None
    inconsistent_trigger = bool(
        trigger
        and trigger_cte is not None
        and abs(trigger_cte) <= float(cte_warn_m)
        and (trigger_heading is None or abs(trigger_heading) <= float(heading_warn_rad))
        and (trigger_displacement is None or abs(trigger_displacement) <= float(early_event_distance_m))
    )
    geometrically_implausible = bool(
        trigger_cte is not None
        and estimated_crossing_offset is not None
        and abs(trigger_cte) < estimated_crossing_offset
    )
    quarantine = bool(lane_events and inconsistent_trigger and geometrically_implausible)
    if not lane_events:
        status = "pass"
        reason = "no_lane_invasion_event_observed"
    elif quarantine:
        status = "warn"
        reason = "lane_invasion_trigger_inconsistent_with_centerline_evidence"
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
            "ego_half_width_m": float(ego_half_width_m),
            "estimated_center_offset_to_cross_mark_m": estimated_crossing_offset,
            "trigger_abs_cross_track_error_m": abs(trigger_cte) if trigger_cte is not None else None,
            "trigger_geometrically_implausible": geometrically_implausible,
        },
        "missing_inputs": _missing_inputs(root),
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
        "event_time_s": _number(event.get("t")),
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
                "ego_speed_mps": _number(row.get("ego_speed"), row.get("v_mps")),
                "cross_track_error": _number(row.get("cross_track_error")),
                "heading_error": _number(row.get("heading_error")),
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


def _overall_status(xodr_report: Mapping[str, Any], run_reports: list[Mapping[str, Any]]) -> str:
    if xodr_report.get("target_lane") is None:
        return "fail"
    if any(report.get("quarantine_recommended") for report in run_reports):
        return "warn"
    if any(report.get("status") == "fail" for report in run_reports):
        return "fail"
    return "pass"


def _summary_markdown(report: Mapping[str, Any]) -> str:
    xodr = report.get("xodr") if isinstance(report.get("xodr"), Mapping) else {}
    lines = [
        "# Baguang Lane Event Contract",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- quarantine_recommended: `{report.get('quarantine_recommended')}`",
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
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _missing_inputs(root: Path) -> list[str]:
    missing: list[str] = []
    for name in ("summary.json", "events.jsonl", "timeseries.csv"):
        if not (root / name).exists():
            missing.append(name)
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


def _first_number(rows: list[dict[str, str]], field: str) -> float | None:
    for row in rows:
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
