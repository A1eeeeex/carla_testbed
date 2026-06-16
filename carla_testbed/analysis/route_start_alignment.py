from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.record.route_curve_context import route_definition_from_metadata
from carla_testbed.routes.geometry import heading_error, project_onto_route
from carla_testbed.routes.io import load_route_json
from carla_testbed.routes.schema import RouteDefinition, RoutePoint

ROUTE_START_ALIGNMENT_SCHEMA_VERSION = "route_start_alignment_report.v1"
DEFAULT_THRESHOLDS = {
    "max_spawn_lateral_offset_warn_m": 0.30,
    "max_initial_cross_track_error_warn_m": 0.30,
    "expected_rear_axle_back_offset_m": 1.4235,
    "rear_axle_offset_tolerance_m": 0.05,
    "start_gate_m": 5.0,
}


def analyze_route_start_alignment_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})

    manifest_path = _find_first(root, ["manifest.json"])
    summary_path = _find_first(root, ["summary.json"])
    route_path = _find_first(root, ["route.json", "artifacts/route.json"])
    scenario_metadata_path = _find_first(root, ["artifacts/scenario_metadata.json", "scenario_metadata.json"])
    timeseries_path = _find_first(root, ["timeseries.csv", "timeseries.jsonl"])
    failure_timeline_path = _find_first(
        root,
        [
            "analysis/failure_timeline/failure_timeline_report.json",
            "failure_timeline_report.json",
        ],
    )

    manifest = _read_json(manifest_path)
    summary = _read_json(summary_path)
    scenario_metadata = _read_json(scenario_metadata_path)
    route = _load_route(route_path, scenario_metadata)
    rows = _read_rows(timeseries_path)
    failure_timeline = _read_json(failure_timeline_path)

    missing_inputs: list[str] = []
    if route is None:
        missing_inputs.append("route")
    if not rows:
        missing_inputs.append("timeseries")
    if not failure_timeline_path:
        missing_inputs.append("failure_timeline")

    static_spawn = _static_spawn_alignment(route)
    initial = _initial_alignment(rows, active_thresholds)
    failure = _failure_anchor_alignment(failure_timeline)
    status, reason, warnings, hypotheses = _verdict(
        static_spawn=static_spawn,
        initial=initial,
        failure=failure,
        missing_inputs=missing_inputs,
        thresholds=active_thresholds,
    )
    recommendation = _alignment_recommendation(
        static_spawn=static_spawn,
        initial=initial,
        failure=failure,
        status=status,
        reason=reason,
        hypotheses=hypotheses,
    )

    return {
        "schema_version": ROUTE_START_ALIGNMENT_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=root.name),
        "route_id": _first_text(summary, "route_id", manifest, "route_id", scenario_metadata, "route_id"),
        "scenario_class": _first_text(summary, "scenario_class", manifest, "scenario_class"),
        "status": status,
        "reason": reason,
        "static_spawn_alignment": static_spawn,
        "initial_ego_alignment": initial,
        "failure_anchor_alignment": failure,
        "recommendation": recommendation,
        "hypotheses": hypotheses,
        "thresholds": active_thresholds,
        "missing_inputs": sorted(set(missing_inputs)),
        "missing_fields": _missing_fields(static_spawn=static_spawn, initial=initial, failure=failure),
        "warnings": sorted(set(warnings)),
        "source": {
            "run_dir": str(root),
            "manifest_path": str(manifest_path) if manifest_path else None,
            "summary_path": str(summary_path) if summary_path else None,
            "route_path": str(route_path) if route_path else None,
            "scenario_metadata_path": str(scenario_metadata_path) if scenario_metadata_path else None,
            "timeseries_path": str(timeseries_path) if timeseries_path else None,
            "failure_timeline_path": str(failure_timeline_path) if failure_timeline_path else None,
        },
        "interpretation_boundary": (
            "Route-start alignment is an offline diagnostic report. It can identify spawn, route-start, "
            "and rear-axle frame hypotheses, but it does not prove behavior success or justify changing "
            "steer_scale, physical mapping, or Apollo parameters."
        ),
    }


def write_route_start_alignment_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "route_start_alignment_report.json"
    md_path = output_dir / "route_start_alignment_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "route_start_alignment_report": str(json_path),
        "route_start_alignment_summary": str(md_path),
    }


def _load_route(path: Path | None, scenario_metadata: Mapping[str, Any]) -> RouteDefinition | None:
    if path is not None and path.exists():
        try:
            return load_route_json(path)
        except Exception:
            pass
    return route_definition_from_metadata(scenario_metadata)


def _static_spawn_alignment(route: RouteDefinition | None) -> dict[str, Any]:
    result = {
        "available": False,
        "spawn_to_route_start_distance_m": None,
        "spawn_longitudinal_offset_m": None,
        "spawn_lateral_offset_m": None,
        "spawn_heading_error_rad": None,
        "route_start": None,
        "spawn_pose": None,
    }
    if route is None or not route.points or not isinstance(route.spawn_pose, Mapping):
        return result
    start = route.points[0]
    spawn_x = _num(route.spawn_pose.get("x"))
    spawn_y = _num(route.spawn_pose.get("y"))
    spawn_heading = _heading_from_pose(route.spawn_pose)
    if spawn_x is None or spawn_y is None:
        return result
    start_heading = start.heading
    if start_heading is None and len(route.points) > 1:
        start_heading = math.atan2(float(route.points[1].y) - float(start.y), float(route.points[1].x) - float(start.x))
    dx = float(spawn_x) - float(start.x)
    dy = float(spawn_y) - float(start.y)
    longitudinal = None
    lateral = None
    if start_heading is not None:
        longitudinal = math.cos(float(start_heading)) * dx + math.sin(float(start_heading)) * dy
        lateral = -math.sin(float(start_heading)) * dx + math.cos(float(start_heading)) * dy
    result.update(
        {
            "available": True,
            "spawn_to_route_start_distance_m": math.hypot(dx, dy),
            "spawn_longitudinal_offset_m": longitudinal,
            "spawn_lateral_offset_m": lateral,
            "spawn_heading_error_rad": heading_error(float(spawn_heading), start_heading)
            if spawn_heading is not None
            else None,
            "route_start": _route_point_payload(start),
            "spawn_pose": dict(route.spawn_pose),
        }
    )
    return result


def _initial_alignment(rows: Sequence[Mapping[str, Any]], thresholds: Mapping[str, float]) -> dict[str, Any]:
    result = {
        "available": False,
        "anchor_event_type": None,
        "route_s": None,
        "cross_track_error_m": None,
        "heading_error_rad": None,
        "ego_speed_mps": None,
        "rear_axle_offset_compatible": None,
        "rear_axle_offset_error_m": None,
        "expected_rear_axle_back_offset_m": float(thresholds["expected_rear_axle_back_offset_m"]),
    }
    if not rows:
        return result
    first = rows[0]
    route_s = _num(first.get("route_s"))
    expected = float(thresholds["expected_rear_axle_back_offset_m"])
    offset_error = None if route_s is None else abs(float(route_s) + expected)
    result.update(
        {
            "available": True,
            "route_s": route_s,
            "cross_track_error_m": _num(first.get("cross_track_error"), first.get("lateral_error")),
            "heading_error_rad": _num(first.get("heading_error")),
            "ego_speed_mps": _num(first.get("ego_speed"), first.get("speed_mps")),
            "rear_axle_offset_compatible": None
            if offset_error is None
            else offset_error <= float(thresholds["rear_axle_offset_tolerance_m"]),
            "rear_axle_offset_error_m": offset_error,
        }
    )
    return result


def _failure_anchor_alignment(failure_timeline: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "available": False,
        "route_s": None,
        "cross_track_error_m": None,
        "heading_error_rad": None,
        "anchor_before_route_start": None,
        "anchor_near_route_start": None,
        "window_route_s_min": None,
        "window_route_s_max": None,
        "ordering_findings": [],
    }
    if not failure_timeline:
        return result
    start_gate = failure_timeline.get("route_start_gate")
    anchor = failure_timeline.get("anchor_event")
    context = anchor.get("row_context") if isinstance(anchor, Mapping) else None
    result.update(
        {
            "available": True,
            "anchor_event_type": str(anchor.get("event_type"))
            if isinstance(anchor, Mapping) and anchor.get("event_type") not in {None, ""}
            else None,
            "route_s": _num(start_gate.get("route_s_at_anchor")) if isinstance(start_gate, Mapping) else None,
            "cross_track_error_m": _num(context.get("cross_track_error")) if isinstance(context, Mapping) else None,
            "heading_error_rad": _num(context.get("heading_error")) if isinstance(context, Mapping) else None,
            "anchor_before_route_start": start_gate.get("anchor_before_route_start")
            if isinstance(start_gate, Mapping)
            else None,
            "anchor_near_route_start": start_gate.get("anchor_near_route_start")
            if isinstance(start_gate, Mapping)
            else None,
            "window_route_s_min": _num(start_gate.get("window_route_s_min")) if isinstance(start_gate, Mapping) else None,
            "window_route_s_max": _num(start_gate.get("window_route_s_max")) if isinstance(start_gate, Mapping) else None,
            "ordering_findings": [
                str(item) for item in (failure_timeline.get("ordering_findings") or []) if item not in {None, ""}
            ],
        }
    )
    return result


def _verdict(
    *,
    static_spawn: Mapping[str, Any],
    initial: Mapping[str, Any],
    failure: Mapping[str, Any],
    missing_inputs: Sequence[str],
    thresholds: Mapping[str, float],
) -> tuple[str, str | None, list[str], list[str]]:
    warnings: list[str] = []
    hypotheses: list[str] = []
    if failure.get("anchor_before_route_start") is True and _failure_anchor_actionable(failure):
        warnings.append("failure_anchor_before_route_start")
        hypotheses.append("spawn_or_route_start_alignment_candidate")
        if initial.get("rear_axle_offset_compatible") is True:
            hypotheses.append("rear_axle_localization_offset_compatible")
        return "warn", "failure_before_route_start", warnings, hypotheses
    if failure.get("anchor_near_route_start") is True and _failure_anchor_actionable(failure):
        warnings.append("failure_anchor_near_route_start")
        hypotheses.append("route_start_gate_candidate")
        return "warn", "failure_near_route_start", warnings, hypotheses

    lateral = _num(static_spawn.get("spawn_lateral_offset_m"))
    if lateral is not None and abs(lateral) > float(thresholds["max_spawn_lateral_offset_warn_m"]):
        warnings.append("spawn_lateral_offset_high")
        hypotheses.append("spawn_lateral_alignment_candidate")
    initial_cte = _num(initial.get("cross_track_error_m"))
    if initial_cte is not None and abs(initial_cte) > float(thresholds["max_initial_cross_track_error_warn_m"]):
        warnings.append("initial_cross_track_error_high")
        hypotheses.append("initial_lateral_alignment_candidate")
    if initial.get("rear_axle_offset_compatible") is True:
        hypotheses.append("rear_axle_localization_offset_compatible")

    if warnings:
        return "warn", warnings[0], warnings, hypotheses
    if "route" in missing_inputs and "timeseries" in missing_inputs:
        return "insufficient_data", "missing_route_and_timeseries", warnings, hypotheses
    if missing_inputs:
        return "warn", "partial_alignment_inputs", warnings + [f"missing_{item}" for item in missing_inputs], hypotheses
    return "pass", None, warnings, hypotheses


_NON_ACTIONABLE_ROUTE_START_ANCHOR_TYPES = {
    "first_high_steer",
    "matched_point_anomaly",
    "target_point_anomaly",
}


def _failure_anchor_actionable(failure: Mapping[str, Any]) -> bool:
    ordering_findings = [str(item) for item in (failure.get("ordering_findings") or []) if item not in {None, ""}]
    if ordering_findings:
        return True
    event_type = str(failure.get("anchor_event_type") or "")
    return event_type not in _NON_ACTIONABLE_ROUTE_START_ANCHOR_TYPES


def _alignment_recommendation(
    *,
    static_spawn: Mapping[str, Any],
    initial: Mapping[str, Any],
    failure: Mapping[str, Any],
    status: str,
    reason: str | None,
    hypotheses: Sequence[str],
) -> dict[str, Any]:
    lateral = _num(static_spawn.get("spawn_lateral_offset_m"))
    if lateral is None or "spawn_lateral_alignment_candidate" not in hypotheses and reason not in {
        "failure_before_route_start",
        "failure_near_route_start",
    }:
        return {
            "available": False,
            "action": "none",
            "reason": "no_spawn_lateral_alignment_probe_recommended",
        }
    initial_cte = _num(initial.get("cross_track_error_m"))
    initial_lateral_high = (
        initial_cte is not None
        and abs(float(initial_cte)) > float(DEFAULT_THRESHOLDS["max_initial_cross_track_error_warn_m"])
    )
    if (
        reason == "spawn_lateral_offset_high"
        and initial.get("rear_axle_offset_compatible") is True
        and (not initial_lateral_high or not _failure_anchor_actionable(failure))
    ):
        return {
            "available": False,
            "action": "none",
            "reason": "spawn_lateral_offset_already_compensated_by_initial_alignment",
            "rear_axle_offset_compatible": True,
            "failure_route_s": failure.get("route_s"),
            "notes": (
                "The route definition spawn is laterally offset from the route start, but the observed "
                "initial lateral alignment is already low and the longitudinal rear-axle frame is "
                "compatible. Do not repeat the same ego_offset_y_m probe from this report alone."
            ),
        }
    correction = -float(lateral)
    return {
        "available": True,
        "action": "probe_spawn_lateral_alignment",
        "status_context": status,
        "reason_context": reason,
        "recommended_config_field": "scenario.route_health.ego_offset_y_m",
        "recommended_ego_offset_y_delta_m": correction,
        "recommended_ego_offset_y_m_if_current_zero": correction,
        "probe_only": True,
        "needs_local_carla": True,
        "needs_local_apollo": True,
        "do_not_change": [
            "algo.apollo.control_mapping.steer_scale",
            "algo.apollo.control_mapping.actuator_mapping_mode",
            "algo.apollo.routing.target_speed_mps",
        ],
        "rear_axle_offset_compatible": initial.get("rear_axle_offset_compatible"),
        "failure_route_s": failure.get("route_s"),
        "notes": (
            "This recommends a single-run spawn lateral alignment probe using the existing "
            "ego_offset_y_m field. It is not a mainline parameter change and does not prove "
            "curve lateral semantics are healthy."
        ),
    }


def _missing_fields(
    *,
    static_spawn: Mapping[str, Any],
    initial: Mapping[str, Any],
    failure: Mapping[str, Any],
) -> list[str]:
    missing: list[str] = []
    if not static_spawn.get("available"):
        missing.append("static_spawn_alignment")
    for name in ("spawn_lateral_offset_m", "spawn_heading_error_rad"):
        if static_spawn.get("available") and static_spawn.get(name) is None:
            missing.append(f"static_spawn_alignment.{name}")
    if not initial.get("available"):
        missing.append("initial_ego_alignment")
    for name in ("route_s", "cross_track_error_m", "heading_error_rad"):
        if initial.get("available") and initial.get(name) is None:
            missing.append(f"initial_ego_alignment.{name}")
    if not failure.get("available"):
        missing.append("failure_anchor_alignment")
    return missing


def _route_point_payload(point: RoutePoint) -> dict[str, Any]:
    return {
        "index": point.index,
        "x": point.x,
        "y": point.y,
        "z": point.z,
        "s": point.s,
        "heading": point.heading,
        "curvature": point.curvature,
        "lane_id": point.lane_id,
    }


def _heading_from_pose(pose: Mapping[str, Any]) -> float | None:
    value = _num(pose.get("heading"), pose.get("yaw"))
    if value is not None:
        return value
    deg = _num(pose.get("heading_deg"), pose.get("yaw_deg"))
    return math.radians(deg) if deg is not None else None


def _markdown(report: Mapping[str, Any]) -> str:
    static = report.get("static_spawn_alignment") if isinstance(report.get("static_spawn_alignment"), Mapping) else {}
    initial = report.get("initial_ego_alignment") if isinstance(report.get("initial_ego_alignment"), Mapping) else {}
    failure = report.get("failure_anchor_alignment") if isinstance(report.get("failure_anchor_alignment"), Mapping) else {}
    recommendation = report.get("recommendation") if isinstance(report.get("recommendation"), Mapping) else {}
    lines = [
        "# Route Start Alignment",
        "",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        f"- run_id: `{report.get('run_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        "",
        "## Static Spawn",
        "",
        f"- lateral_offset_m: `{static.get('spawn_lateral_offset_m')}`",
        f"- longitudinal_offset_m: `{static.get('spawn_longitudinal_offset_m')}`",
        f"- heading_error_rad: `{static.get('spawn_heading_error_rad')}`",
        "",
        "## Initial Ego",
        "",
        f"- route_s: `{initial.get('route_s')}`",
        f"- cross_track_error_m: `{initial.get('cross_track_error_m')}`",
        f"- rear_axle_offset_compatible: `{initial.get('rear_axle_offset_compatible')}`",
        f"- rear_axle_offset_error_m: `{initial.get('rear_axle_offset_error_m')}`",
        "",
        "## Failure Anchor",
        "",
        f"- route_s: `{failure.get('route_s')}`",
        f"- cross_track_error_m: `{failure.get('cross_track_error_m')}`",
        f"- anchor_before_route_start: `{failure.get('anchor_before_route_start')}`",
        f"- anchor_near_route_start: `{failure.get('anchor_near_route_start')}`",
        "",
        "## Probe Recommendation",
        "",
        f"- available: `{recommendation.get('available')}`",
        f"- action: `{recommendation.get('action')}`",
        f"- config_field: `{recommendation.get('recommended_config_field')}`",
        f"- ego_offset_y_delta_m: `{recommendation.get('recommended_ego_offset_y_delta_m')}`",
        f"- probe_only: `{recommendation.get('probe_only')}`",
        "",
        "## Hypotheses",
        "",
    ]
    hypotheses = list(report.get("hypotheses") or [])
    lines.extend(f"- `{item}`" for item in hypotheses) if hypotheses else lines.append("- none")
    lines.extend(
        [
            "",
            "This report is diagnostic only. It does not prove behavior success or justify automatic "
            "control/calibration changes.",
            "",
        ]
    )
    return "\n".join(lines)


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
        return rows
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _first_text(*items: Any, default: str | None = None) -> str | None:
    index = 0
    while index + 1 < len(items):
        source = items[index]
        key = items[index + 1]
        if isinstance(source, Mapping):
            value = source.get(str(key))
            if value not in {None, ""}:
                return str(value)
        index += 2
    return default


def _num(*values: Any) -> float | None:
    for value in values:
        if value in {None, ""}:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number != number:
            continue
        return number
    return None
