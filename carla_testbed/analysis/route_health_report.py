from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from carla_testbed.analysis.route_health import (
    EVIDENCE_LEVEL_INSUFFICIENT,
    ROUTE_SOURCE_CONFIGURED_ROUTE_FILE,
    ROUTE_SOURCE_INLINE_ROUTE,
    ROUTE_SOURCE_MANIFEST_ROUTE,
    ROUTE_SOURCE_MANIFEST_ROUTE_TRACE,
    ROUTE_SOURCE_MISSING,
    ROUTE_SOURCE_RECONSTRUCTED_FROM_TIMESERIES,
    analyze_route_health,
)
from carla_testbed.routes.geometry import compute_cumulative_s, compute_curvature, compute_headings
from carla_testbed.routes.io import load_route_json
from carla_testbed.routes.schema import RouteDefinition, RoutePoint

ROUTE_HEALTH_REPORT_SCHEMA_VERSION = "route_health.v1"

ROUTE_HEALTH_CSV_FIELDS = [
    "route_id",
    "index",
    "s",
    "x",
    "y",
    "z",
    "heading",
    "curvature",
    "spacing_prev_m",
    "is_curve_segment",
    "curve_segment_id",
]

CURVE_SEGMENTS_CSV_FIELDS = [
    "route_id",
    "curve_segment_id",
    "start_index",
    "end_index",
    "start_s",
    "end_s",
    "length_m",
    "mean_abs_curvature",
    "max_abs_curvature",
    "direction",
    "warning",
]


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.9g}"
    return str(value)


def _segment_lookup(report: dict[str, Any]) -> dict[int, int]:
    lookup: dict[int, int] = {}
    for segment in report.get("route_geometry", {}).get("curve_segments", []) or []:
        segment_id = int(segment.get("curve_segment_id"))
        start = int(segment.get("start_index"))
        end = int(segment.get("end_index"))
        for index in range(start, end + 1):
            lookup[index] = segment_id
    return lookup


def write_route_health_json(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _write_header_only_csv(path: Path, fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields))
        writer.writeheader()


def write_route_health_csv(path: Path, route: RouteDefinition, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    segment_lookup = _segment_lookup(report)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROUTE_HEALTH_CSV_FIELDS)
        writer.writeheader()
        previous_s: float | None = None
        for point in route.points:
            spacing_prev = None if previous_s is None or point.s is None else float(point.s) - previous_s
            if point.s is not None:
                previous_s = float(point.s)
            segment_id = segment_lookup.get(point.index)
            writer.writerow(
                {
                    "route_id": route.route_id,
                    "index": point.index,
                    "s": _format_value(point.s),
                    "x": _format_value(point.x),
                    "y": _format_value(point.y),
                    "z": _format_value(point.z),
                    "heading": _format_value(point.heading),
                    "curvature": _format_value(point.curvature),
                    "spacing_prev_m": _format_value(spacing_prev),
                    "is_curve_segment": bool(segment_id is not None),
                    "curve_segment_id": "" if segment_id is None else segment_id,
                }
            )


def write_curve_segments_csv(path: Path, route: RouteDefinition, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CURVE_SEGMENTS_CSV_FIELDS)
        writer.writeheader()
        for segment in report.get("route_geometry", {}).get("curve_segments", []) or []:
            writer.writerow(
                {
                    "route_id": route.route_id,
                    **{field: _format_value(segment.get(field)) for field in CURVE_SEGMENTS_CSV_FIELDS if field != "route_id"},
                }
            )


def render_route_health_summary(report: dict[str, Any]) -> str:
    geometry = report.get("route_geometry", {})
    metrics = report.get("run_metrics", {})
    localization = report.get("localization_contract")
    if not isinstance(localization, Mapping):
        localization = {}
    spacing = geometry.get("spacing", {})
    heading = geometry.get("heading", {})
    curvature = geometry.get("curvature", {})
    spawn = geometry.get("spawn_alignment", {})
    direction = geometry.get("route_direction_consistency", {})
    verdict = report.get("verdict", {})
    lines = [
        "# Route Health Summary",
        "",
        f"- route_id: `{report.get('route_id')}`",
        f"- map_name: `{report.get('map_name')}`",
        f"- Route source: `{report.get('route_source')}`",
        f"- Evidence level: `{report.get('evidence_level')}`",
        f"- Hard gate eligible: `{report.get('hard_gate_eligible')}`",
        f"- Route evidence reason: `{report.get('route_evidence_reason')}`",
        f"- point_count: `{geometry.get('point_count')}`",
        f"- length_m: `{_format_value(geometry.get('length_m'))}`",
        f"- spacing mean/p95/max: `{_format_value(spacing.get('mean_m'))}` / `{_format_value(spacing.get('p95_m'))}` / `{_format_value(spacing.get('max_m'))}`",
        f"- max_heading_jump_rad: `{_format_value(heading.get('max_jump_rad'))}`",
        f"- curvature p95/max: `{_format_value(curvature.get('p95_abs'))}` / `{_format_value(curvature.get('max_abs'))}`",
        f"- curve segment count: `{geometry.get('curve_segments_count')}`",
        f"- spawn alignment distance_m: `{_format_value(spawn.get('distance_m'))}`",
        f"- spawn alignment heading_error_rad: `{_format_value(spawn.get('heading_error_rad'))}`",
        f"- spawn alignment direction_consistent: `{spawn.get('direction_consistent')}`",
        f"- route_direction_consistency: `{direction.get('status')}`",
        f"- route_direction_warnings: `{'; '.join(direction.get('warnings') or []) or 'none'}`",
        f"- ego_speed mean/p95/max: `{_format_value(metrics.get('ego_speed_mean_mps'))}` / `{_format_value(metrics.get('ego_speed_p95_mps'))}` / `{_format_value(metrics.get('ego_speed_max_mps'))}`",
        f"- stopped_ratio: `{_format_value(metrics.get('stopped_ratio'))}`",
        f"- ego_yaw_rate_abs_p95_rad_s: `{_format_value(metrics.get('ego_yaw_rate_abs_p95_rad_s'))}`",
        f"- throttle_applied_p95: `{_format_value(metrics.get('throttle_applied_p95'))}`",
        f"- brake_applied_p95: `{_format_value(metrics.get('brake_applied_p95'))}`",
        f"- brake_throttle_conflict_frames: `{_format_value(metrics.get('brake_throttle_conflict_frames'))}`",
        f"- localization_contract_status: `{localization.get('status') or 'not evaluated'}`",
        f"- localization warnings: `{'; '.join(localization.get('warnings') or []) or 'none'}`",
        f"- localization blocking_reasons: `{'; '.join(localization.get('blocking_reasons') or []) or 'none'}`",
        f"- missing_inputs: `{', '.join(report.get('missing_inputs') or []) or 'none'}`",
        f"- missing_fields: `{', '.join(report.get('missing_fields') or []) or 'none'}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- verdict_reason: `{verdict.get('reason')}`",
        "- plots: `skipped`",
        "",
    ]
    return "\n".join(lines)


def write_route_health_summary(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_route_health_summary(report), encoding="utf-8")


def write_route_health_report(out_dir: str | Path, route: RouteDefinition, report: dict[str, Any]) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "route_health_json": output / "route_health.json",
        "route_health_csv": output / "route_health.csv",
        "curve_segments_csv": output / "curve_segments.csv",
        "route_health_summary_md": output / "route_health_summary.md",
    }
    write_route_health_json(paths["route_health_json"], report)
    write_route_health_csv(paths["route_health_csv"], route, report)
    write_curve_segments_csv(paths["curve_segments_csv"], route, report)
    write_route_health_summary(paths["route_health_summary_md"], report)
    return {key: str(path) for key, path in paths.items()}


def write_route_health_report_files(
    out_dir: str | Path,
    report: dict[str, Any],
    *,
    route: RouteDefinition | None = None,
) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "route_health_json": output / "route_health.json",
        "route_health_csv": output / "route_health.csv",
        "curve_segments_csv": output / "curve_segments.csv",
        "route_health_summary_md": output / "route_health_summary.md",
    }
    write_route_health_json(paths["route_health_json"], report)
    if route is None:
        _write_header_only_csv(paths["route_health_csv"], ROUTE_HEALTH_CSV_FIELDS)
        _write_header_only_csv(paths["curve_segments_csv"], CURVE_SEGMENTS_CSV_FIELDS)
    else:
        write_route_health_csv(paths["route_health_csv"], route, report)
        write_curve_segments_csv(paths["curve_segments_csv"], route, report)
    write_route_health_summary(paths["route_health_summary_md"], report)
    return {key: str(path) for key, path in paths.items()}


def _read_json_object(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _read_yaml_object(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else None


def _nested_values(payload: Any, keys: set[str]) -> list[Any]:
    found: list[Any] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            key_text = str(key).lower()
            if key_text in keys:
                found.append(value)
            found.extend(_nested_values(value, keys))
    elif isinstance(payload, list):
        for value in payload:
            found.extend(_nested_values(value, keys))
    return found


def _resolve_candidate_path(value: Any, *, run_dir: Path) -> Path | None:
    if value is None or isinstance(value, Mapping):
        return None
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = run_dir / path
    return path if path.exists() else None


def _first_existing(paths: Sequence[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _inline_route_candidates(payload: Any) -> list[tuple[str, Mapping[str, Any]]]:
    candidates: list[tuple[str, Mapping[str, Any]]] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            key_text = str(key)
            if key_text in {"route_definition", "route_ref_resolved", "route"} and isinstance(value, Mapping):
                if isinstance(value.get("points"), list):
                    candidates.append((key_text, value))
            candidates.extend(_inline_route_candidates(value))
    elif isinstance(payload, list):
        for value in payload:
            candidates.extend(_inline_route_candidates(value))
    return candidates


def _route_point_from_inline(index: int, payload: Mapping[str, Any]) -> RoutePoint:
    tags = payload.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    return RoutePoint(
        index=int(payload.get("index", index)),
        x=float(payload["x"]),
        y=float(payload["y"]),
        z=float(payload.get("z", 0.0) or 0.0),
        s=_float_or_none(payload.get("s")),
        heading=_float_or_none(payload.get("heading")),
        curvature=_float_or_none(payload.get("curvature")),
        lane_id=str(payload.get("lane_id")) if payload.get("lane_id") is not None else None,
        tags=[str(item) for item in tags],
    )


def _fill_inline_geometry_defaults(points: list[RoutePoint]) -> None:
    cumulative_s = compute_cumulative_s(points)
    headings = compute_headings(points)
    curvatures = compute_curvature(points)
    for index, point in enumerate(points):
        if point.s is None and index < len(cumulative_s):
            point.s = cumulative_s[index]
        if point.heading is None and index < len(headings):
            point.heading = headings[index]
        if point.curvature is None and index < len(curvatures):
            point.curvature = curvatures[index]


def route_from_inline_payload(payload: Mapping[str, Any], *, inline_source_key: str) -> RouteDefinition | None:
    points_payload = payload.get("points")
    if not isinstance(points_payload, list) or not points_payload:
        return None
    try:
        points = [_route_point_from_inline(index, item) for index, item in enumerate(points_payload)]
    except Exception:
        return None
    _fill_inline_geometry_defaults(points)
    route_id = str(payload.get("route_id") or payload.get("id") or "").strip()
    map_name = str(payload.get("map") or payload.get("map_name") or "").strip()
    metadata = dict(payload.get("metadata") or {})
    metadata["inline_source_key"] = str(inline_source_key)
    if inline_source_key in {"route_definition", "route_ref_resolved"}:
        metadata["route_evidence_source"] = inline_source_key
    return RouteDefinition(
        route_id=route_id or "inline_route",
        map_name=map_name or "unknown_map",
        source=f"inline:{inline_source_key}",
        points=points,
        spawn_pose=payload.get("spawn_pose"),
        goal_pose=payload.get("goal_pose"),
        metadata=metadata,
    )


def _nested_get(payload: Mapping[str, Any] | None, path: Sequence[str]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _route_trace_candidates(
    manifest: Mapping[str, Any] | None,
    summary: Mapping[str, Any] | None,
) -> list[tuple[str, Any, Mapping[str, Any] | None]]:
    return [
        (
            "manifest.metadata.scenario_metadata.route_trace",
            _nested_get(manifest, ("metadata", "scenario_metadata", "route_trace")),
            manifest,
        ),
        (
            "manifest.scenario_metadata.route_trace",
            _nested_get(manifest, ("scenario_metadata", "route_trace")),
            manifest,
        ),
        (
            "summary.scenario_metadata.route_trace",
            _nested_get(summary, ("scenario_metadata", "route_trace")),
            summary,
        ),
    ]


def _route_trace_point(index: int, payload: Mapping[str, Any]) -> RoutePoint | None:
    location = payload.get("location")
    if not isinstance(location, Mapping):
        location = {}
    x = _float_or_none(payload.get("x"))
    if x is None:
        x = _float_or_none(location.get("x"))
    if x is None:
        x = _float_or_none(payload.get("carla_x"))
    y = _float_or_none(payload.get("y"))
    if y is None:
        y = _float_or_none(location.get("y"))
    if y is None:
        y = _float_or_none(payload.get("carla_y"))
    if x is None or y is None:
        return None
    z = _float_or_none(payload.get("z"))
    if z is None:
        z = _float_or_none(location.get("z"))
    if z is None:
        z = _float_or_none(payload.get("carla_z"))
    return RoutePoint(
        index=int(payload.get("index", index)),
        x=x,
        y=y,
        z=z or 0.0,
        s=_float_or_none(payload.get("s")),
        heading=_float_or_none(payload.get("heading")),
        curvature=_float_or_none(payload.get("curvature")),
        lane_id=str(payload.get("lane_id")) if payload.get("lane_id") else None,
        tags=["manifest_route_trace"],
    )


def route_from_manifest_route_trace(
    route_trace: Any,
    *,
    source_payload: Mapping[str, Any] | None,
    route_trace_source_key: str,
) -> RouteDefinition | None:
    if not isinstance(route_trace, list) or len(route_trace) < 2:
        return None
    points: list[RoutePoint] = []
    for index, item in enumerate(route_trace):
        if not isinstance(item, Mapping):
            continue
        point = _route_trace_point(index, item)
        if point is not None:
            points.append(point)
    if len(points) < 2:
        return None
    _fill_inline_geometry_defaults(points)
    source_payload = source_payload or {}
    scenario_metadata = source_payload.get("scenario_metadata")
    if not isinstance(scenario_metadata, Mapping):
        metadata_wrapper = source_payload.get("metadata")
        if isinstance(metadata_wrapper, Mapping) and isinstance(metadata_wrapper.get("scenario_metadata"), Mapping):
            scenario_metadata = metadata_wrapper.get("scenario_metadata")
    if not isinstance(scenario_metadata, Mapping):
        scenario_metadata = {}
    route_id = str(
        source_payload.get("route_id")
        or scenario_metadata.get("route_id")
        or source_payload.get("id")
        or "manifest_route_trace"
    )
    map_name = str(
        source_payload.get("map")
        or source_payload.get("map_name")
        or scenario_metadata.get("map")
        or scenario_metadata.get("map_name")
        or "unknown_map"
    )
    return RouteDefinition(
        route_id=route_id,
        map_name=map_name,
        source=f"manifest:{route_trace_source_key}",
        points=points,
        spawn_pose=scenario_metadata.get("spawn_pose") or source_payload.get("spawn_pose"),
        goal_pose=scenario_metadata.get("goal_pose") or source_payload.get("goal_pose"),
        metadata={
            "route_evidence_source": "manifest_route_trace",
            "route_trace_source_key": route_trace_source_key,
            "reference_line_verified": False,
            "apollo_reference_line_claim_grade": False,
        },
    )


def discover_route_health_run_inputs(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    manifest_path = _first_existing([root / "manifest.json"])
    summary_path = _first_existing([root / "summary.json"])
    config_path = _first_existing([root / "config.resolved.yaml", root / "effective_config.yaml", root / "effective.yaml"])
    timeseries_path = _first_existing(
        [
            root / "timeseries.csv",
            root / "timeseries.jsonl",
            root / "artifacts" / "timeseries.csv",
            root / "artifacts" / "timeseries.jsonl",
        ]
    )
    bridge_control_decode_path = _first_existing(
        [
            root / "artifacts" / "bridge_control_decode.jsonl",
            root / "bridge_control_decode.jsonl",
        ]
    )
    localization_contract_path = _first_existing(
        [
            root / "analysis" / "localization_contract" / "localization_contract_report.json",
            root / "localization_contract_report.json",
        ]
    )
    route_path = _first_existing(
        [
            root / "route.json",
            root / "artifacts" / "route.json",
            root / "config" / "route.json",
        ]
    )
    route_source = ROUTE_SOURCE_CONFIGURED_ROUTE_FILE if route_path is not None else ROUTE_SOURCE_MISSING
    inline_route_payload: Mapping[str, Any] | None = None
    inline_route_source_key: str | None = None
    route_trace_payload: Any = None
    route_trace_source_key: str | None = None
    route_trace_source_payload: Mapping[str, Any] | None = None
    manifest = _read_json_object(manifest_path)
    summary = _read_json_object(summary_path)
    config = _read_yaml_object(config_path)
    if route_path is None:
        for payload in (manifest, summary, config):
            for value in _nested_values(payload, {"route", "route_file", "route_json", "route_path"}):
                candidate = _resolve_candidate_path(value, run_dir=root)
                if candidate is not None and candidate.suffix.lower() == ".json":
                    route_path = candidate
                    route_source = ROUTE_SOURCE_MANIFEST_ROUTE
                    break
            if route_path is not None:
                break
    for payload in (manifest, config, summary):
        for key, value in _inline_route_candidates(payload):
            inline_route_payload = value
            inline_route_source_key = key
            if route_path is None:
                route_source = ROUTE_SOURCE_INLINE_ROUTE
            break
        if inline_route_payload is not None:
            break
    for key, trace, source_payload in _route_trace_candidates(manifest, summary):
        if isinstance(trace, list) and len(trace) >= 2:
            route_trace_payload = trace
            route_trace_source_key = key
            route_trace_source_payload = source_payload
            if route_path is None and inline_route_payload is None:
                route_source = ROUTE_SOURCE_MANIFEST_ROUTE_TRACE
            break
    return {
        "run_dir": root,
        "manifest_path": manifest_path,
        "summary_path": summary_path,
        "config_path": config_path,
        "timeseries_path": timeseries_path,
        "bridge_control_decode_path": bridge_control_decode_path,
        "localization_contract_path": localization_contract_path,
        "route_path": route_path,
        "route_source": route_source,
        "inline_route": inline_route_payload,
        "inline_route_source_key": inline_route_source_key,
        "route_trace": route_trace_payload,
        "route_trace_source_key": route_trace_source_key,
        "route_trace_source_payload": route_trace_source_payload,
    }


def load_timeseries_rows(path: str | Path) -> list[dict[str, Any]]:
    ts_path = Path(path).expanduser()
    suffix = ts_path.suffix.lower()
    if suffix == ".csv":
        with ts_path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    if suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        with ts_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    raise ValueError(f"JSONL row must be an object at {ts_path}:{line_number}")
                rows.append(payload)
        return rows
    if suffix == ".json":
        payload = json.loads(ts_path.read_text(encoding="utf-8"))
        if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
            return list(payload)
        raise ValueError(f"JSON timeseries must be a list of objects: {ts_path}")
    raise ValueError(f"unsupported timeseries format: {ts_path}")


def load_bridge_control_decode_rows(path: str | Path) -> list[dict[str, Any]]:
    """Return route-health compatible control rows from bridge decode artifacts.

    Some external-stack runs keep the per-frame harness timeseries intentionally
    sparse and write Apollo raw/mapped control evidence only to
    ``bridge_control_decode.jsonl``. Treat that file as a supplemental control
    evidence source instead of marking the run as missing raw/mapped control.
    """
    rows: list[dict[str, Any]] = []
    decode_path = Path(path).expanduser()
    with decode_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if not isinstance(payload, Mapping):
                raise ValueError(f"bridge decode row must be an object at {decode_path}:{line_number}")
            parsed = payload.get("parsed_control")
            output = payload.get("output_to_carla")
            if not isinstance(parsed, Mapping):
                parsed = {}
            if not isinstance(output, Mapping):
                output = {}
            output_source = output or payload
            rows.append(
                {
                    "sim_time": payload.get("ts_sec") or parsed.get("control_timestamp"),
                    "apollo_steer_raw": _first_number(
                        payload,
                        "raw_steer",
                        "steering_selected_normalized",
                        "steering_normalized_for_mapping",
                    )
                    if not parsed
                    else _first_number(
                        parsed,
                        "steer",
                        "raw_steer_value",
                        "steering_selected_normalized",
                        "steering_normalized_for_mapping",
                    ),
                    "bridge_steer_mapped": _first_number(
                        output_source,
                        "mapped_carla_steer_cmd",
                        "steer",
                        "steer_before_lateral_guards",
                        "commanded_steer",
                    ),
                    "throttle_raw": _first_number(payload, "raw_throttle")
                    if not parsed
                    else _first_number(parsed, "throttle"),
                    "throttle_mapped": _first_number(
                        output_source,
                        "mapped_throttle_cmd",
                        "throttle_before_boost",
                        "commanded_throttle",
                        "throttle",
                    ),
                    "brake_raw": _first_number(payload, "raw_brake")
                    if not parsed
                    else _first_number(parsed, "brake"),
                    "brake_mapped": _first_number(
                        output_source,
                        "mapped_brake_cmd",
                        "brake_after_deadzone",
                        "commanded_brake",
                        "brake",
                    ),
                    "lateral_guard_applied": bool(
                        output_source.get("sustained_lateral_guard_applied")
                        or output_source.get("low_speed_sustained_guard_applied")
                        or output_source.get("straight_lane_zero_steer_applied")
                    ),
                    "trajectory_contract_guard_applied": bool(
                        output_source.get("trajectory_contract_lateral_guard_applied")
                    ),
                    "control_source": "bridge_control_decode",
                }
            )
    return rows


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_number(payload: Mapping[str, Any], *names: str) -> float | None:
    for name in names:
        value = _float_or_none(payload.get(name))
        if value is not None:
            return value
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _first_int(payload: Mapping[str, Any], *names: str) -> int | None:
    for name in names:
        value = _int_or_none(payload.get(name))
        if value is not None:
            return value
    return None


def _remove_missing_field(report: dict[str, Any], field: str) -> None:
    report["missing_fields"] = [item for item in report.get("missing_fields", []) if item != field]


def _enrich_apollo_semantics_from_summary(report: dict[str, Any], summary: Mapping[str, Any]) -> None:
    if not summary:
        return
    apollo = report.setdefault("apollo_semantics", {})
    sources = apollo.setdefault("summary_sources", [])

    matched_seq = _first_int(
        summary,
        "first_matched_point_too_large_seq",
        "first_matched_point_anomaly_seq",
    )
    if matched_seq is not None:
        locations = apollo.setdefault("matched_point_anomaly_locations", [])
        if matched_seq not in locations:
            locations.append(matched_seq)
        apollo["first_matched_point_too_large"] = {
            "seq": matched_seq,
            "at": _first_number(summary, "first_matched_point_too_large_at", "semantic_window_anchor_at"),
        }
        sources.append("summary.first_matched_point_too_large_seq")
        _remove_missing_field(report, "matched_point")

    target_metric = _first_number(
        summary,
        "apollo_simple_lat_target_point_kappa_abs_p95",
        "apollo_simple_lat_target_point_kappa_abs_p99",
        "target_point_kappa_abs_p95_before_failure",
        "target_point_kappa_abs_p95_before_anchor",
    )
    if target_metric is not None:
        apollo["target_point_kappa_abs_p95"] = target_metric
        sources.append("summary.target_point_kappa")
        _remove_missing_field(report, "target_point")

    high_steer_seq = _first_int(summary, "first_high_steer_seq")
    if high_steer_seq is not None and apollo.get("first_high_steer") is None:
        apollo["first_high_steer"] = {
            "seq": high_steer_seq,
            "at": _first_number(summary, "first_high_steer_at"),
            "value": None,
        }
        sources.append("summary.first_high_steer_seq")
        _remove_missing_field(report, "apollo_raw_steer")

    if sources:
        apollo["summary_sources"] = sorted(set(str(item) for item in sources))


def _attach_localization_contract_summary(report: dict[str, Any], path: Path | None) -> None:
    if path is None:
        report["localization_contract"] = {
            "status": "not evaluated",
            "path": None,
            "warnings": [],
            "blocking_reasons": [],
        }
        return
    payload = _read_json_object(path) or {}
    verdict = payload.get("verdict")
    if not isinstance(verdict, Mapping):
        verdict = {}
    status = payload.get("status")
    if not isinstance(status, str | int | float | bool) or status in {None, ""}:
        status = verdict.get("status")
    if not isinstance(status, str | int | float | bool) or status in {None, ""}:
        status = "insufficient_data"
    report["localization_contract"] = {
        "status": str(status),
        "path": str(path),
        "warnings": [str(item) for item in (payload.get("warnings") or []) if item],
        "blocking_reasons": [str(item) for item in (verdict.get("blocking_reasons") or []) if item],
    }


def route_from_timeseries_rows(rows: Sequence[Mapping[str, Any]]) -> RouteDefinition | None:
    """Reconstruct a lightweight route from P0 route_curve columns.

    Online runs may not yet write a standalone route.json, but the P0
    recorder fields carry route_x/route_y/route_heading/route_curvature per
    frame. This fallback keeps route-health reporting useful while preserving
    a warning that the route asset was reconstructed from run evidence.
    """
    points_by_index: dict[int, RoutePoint] = {}
    next_index = 0
    route_id: str | None = None
    map_name: str | None = None
    for row in rows:
        x = _float_or_none(row.get("route_x"))
        y = _float_or_none(row.get("route_y"))
        if x is None or y is None:
            continue
        if route_id is None and row.get("route_id"):
            route_id = str(row.get("route_id"))
        if map_name is None and row.get("map_name"):
            map_name = str(row.get("map_name"))
        index = _int_or_none(row.get("nearest_route_index"))
        if index is None:
            index = _int_or_none(row.get("route_index"))
        if index is None:
            index = next_index
            next_index += 1
        if index in points_by_index:
            continue
        points_by_index[index] = RoutePoint(
            index=index,
            x=x,
            y=y,
            z=_float_or_none(row.get("route_z")) or 0.0,
            s=_float_or_none(row.get("route_s")),
            heading=_float_or_none(row.get("route_heading")),
            curvature=_float_or_none(row.get("route_curvature")) or _float_or_none(row.get("curvature_at_nearest")),
            lane_id=str(row.get("lane_id")) if row.get("lane_id") else None,
            tags=["reconstructed_from_timeseries"],
        )
    if not points_by_index:
        return None
    points = [points_by_index[key] for key in sorted(points_by_index)]
    return RouteDefinition(
        route_id=route_id or "unknown_route",
        map_name=map_name or "unknown_map",
        source="timeseries_route_curve_p0",
        points=points,
        metadata={"reconstructed_from": "timeseries_p0_route_curve_fields"},
    )


def _source_payload(inputs: Mapping[str, Any]) -> dict[str, str | None]:
    return {
        "run_dir": str(inputs.get("run_dir")) if inputs.get("run_dir") is not None else None,
        "manifest_path": str(inputs.get("manifest_path")) if inputs.get("manifest_path") is not None else None,
        "summary_path": str(inputs.get("summary_path")) if inputs.get("summary_path") is not None else None,
        "timeseries_path": str(inputs.get("timeseries_path")) if inputs.get("timeseries_path") is not None else None,
        "bridge_control_decode_path": (
            str(inputs.get("bridge_control_decode_path"))
            if inputs.get("bridge_control_decode_path") is not None
            else None
        ),
        "localization_contract_path": (
            str(inputs.get("localization_contract_path"))
            if inputs.get("localization_contract_path") is not None
            else None
        ),
        "route_path": str(inputs.get("route_path")) if inputs.get("route_path") is not None else None,
        "route_source": str(inputs.get("route_source") or ROUTE_SOURCE_MISSING),
        "inline_route_source_key": (
            str(inputs.get("inline_route_source_key")) if inputs.get("inline_route_source_key") is not None else None
        ),
        "route_trace_source_key": (
            str(inputs.get("route_trace_source_key")) if inputs.get("route_trace_source_key") is not None else None
        ),
    }


def build_insufficient_route_health_report(inputs: Mapping[str, Any]) -> dict[str, Any]:
    missing_inputs = ["route"]
    if inputs.get("timeseries_path") is None:
        missing_inputs.append("timeseries")
    return {
        "schema_version": ROUTE_HEALTH_REPORT_SCHEMA_VERSION,
        "route_id": None,
        "map_name": None,
        "route_source": ROUTE_SOURCE_MISSING,
        "evidence_level": EVIDENCE_LEVEL_INSUFFICIENT,
        "hard_gate_eligible": False,
        "route_evidence_reason": "route_missing_or_unrecognized_source",
        "source": _source_payload(inputs),
        "route_geometry": {
            "point_count": None,
            "length_m": None,
            "spacing": {"mean_m": None, "p95_m": None, "max_m": None},
            "heading": {"max_jump_rad": None, "jump_locations": []},
            "curvature": {"mean_abs": None, "p95_abs": None, "max_abs": None, "spike_locations": []},
            "curve_segments_count": None,
            "curve_segments": [],
            "spawn_alignment": {
                "distance_m": None,
                "heading_error_rad": None,
                "direction_consistent": None,
            },
            "route_direction_consistency": {"status": "unknown", "warnings": []},
        },
        "run_metrics": {
            "lateral_error_mean_m": None,
            "lateral_error_p95_m": None,
            "lateral_error_max_m": None,
            "heading_error_mean_rad": None,
            "heading_error_p95_rad": None,
            "heading_error_max_rad": None,
            "lateral_error_by_curvature_bucket": {},
            "heading_error_by_curvature_bucket": {},
        },
        "apollo_semantics": {
            "matched_point_anomaly_locations": [],
            "target_point_anomaly_locations": [],
            "first_high_steer": None,
        },
        "control_semantics": {
            "raw_mapped_applied_steer_available": False,
            "guard_apply_counts": {
                "lateral_guard": 0,
                "trajectory_contract_lateral_guard": 0,
                "low_speed_steer_guard": 0,
            },
        },
        "missing_fields": [],
        "missing_inputs": sorted(set(missing_inputs)),
        "warnings": ["route artifact not found"],
        "verdict": {"status": "insufficient_data", "reason": "route artifact not found"},
    }


def analyze_route_health_run_dir(
    run_dir: str | Path,
    *,
    out_dir: str | Path | None = None,
    curvature_abs_threshold: float = 0.03,
) -> dict[str, Any]:
    inputs = discover_route_health_run_inputs(run_dir)
    output_dir = Path(out_dir).expanduser() if out_dir is not None else Path(run_dir).expanduser() / "analysis" / "route_health"
    route_path = inputs.get("route_path")
    timeseries_path = inputs.get("timeseries_path")
    route_source = str(inputs.get("route_source") or ROUTE_SOURCE_MISSING)
    summary = _read_json_object(inputs.get("summary_path")) or {}
    route: RouteDefinition | None = None
    rows = load_timeseries_rows(timeseries_path) if timeseries_path is not None else None
    bridge_control_decode_path = inputs.get("bridge_control_decode_path")
    bridge_rows: list[dict[str, Any]] = []
    if bridge_control_decode_path is not None:
        bridge_rows = load_bridge_control_decode_rows(bridge_control_decode_path)
    analysis_rows = None if rows is None and not bridge_rows else [*(rows or []), *bridge_rows]
    route_load_error: str | None = None
    if route_path is not None:
        try:
            route = load_route_json(route_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            route_load_error = f"{type(exc).__name__}: {exc}"
            route = None
    if route is not None:
        report = analyze_route_health(
            route,
            analysis_rows,
            curvature_abs_threshold=curvature_abs_threshold,
            route_source=route_source,
        )
        report["source"] = _source_payload(inputs)
    elif inputs.get("inline_route") is not None:
        route = route_from_inline_payload(
            inputs["inline_route"],
            inline_source_key=str(inputs.get("inline_route_source_key") or "route"),
        )
        if route is not None:
            report = analyze_route_health(
                route,
                analysis_rows,
                curvature_abs_threshold=curvature_abs_threshold,
                route_source=ROUTE_SOURCE_INLINE_ROUTE,
            )
            report["source"] = _source_payload(inputs)
        else:
            report = build_insufficient_route_health_report(inputs)
    elif inputs.get("route_trace") is not None:
        route = route_from_manifest_route_trace(
            inputs.get("route_trace"),
            source_payload=inputs.get("route_trace_source_payload"),
            route_trace_source_key=str(inputs.get("route_trace_source_key") or "route_trace"),
        )
        if route is not None:
            report = analyze_route_health(
                route,
                analysis_rows,
                curvature_abs_threshold=curvature_abs_threshold,
                route_source=ROUTE_SOURCE_MANIFEST_ROUTE_TRACE,
            )
            report["source"] = _source_payload(inputs)
            report["reference_line_verified"] = False
            report["apollo_reference_line_claim_grade"] = False
        else:
            report = build_insufficient_route_health_report(inputs)
    elif rows is not None:
        route = route_from_timeseries_rows(rows)
        if route is not None:
            report = analyze_route_health(
                route,
                analysis_rows,
                curvature_abs_threshold=curvature_abs_threshold,
                route_source=ROUTE_SOURCE_RECONSTRUCTED_FROM_TIMESERIES,
            )
            report["source"] = _source_payload(inputs)
            report.setdefault("warnings", []).append("route reconstructed from timeseries P0 route_curve fields")
        else:
            report = build_insufficient_route_health_report(inputs)
    else:
        report = build_insufficient_route_health_report(inputs)
    if route_load_error:
        report.setdefault("warnings", []).append(f"route_artifact_invalid: {route_load_error}")
        report.setdefault("source", _source_payload(inputs))["route_load_error"] = route_load_error
    if bridge_rows and report.get("verdict", {}).get("status") != "insufficient_data":
        report.setdefault("warnings", []).append("control semantics enriched from bridge_control_decode.jsonl")
    _enrich_apollo_semantics_from_summary(report, summary)
    _attach_localization_contract_summary(report, inputs.get("localization_contract_path"))
    report["missing_fields"] = sorted(set(report.get("missing_fields") or []))
    outputs = write_route_health_report_files(output_dir, report, route=route)
    return {"inputs": _source_payload(inputs), "outputs": outputs, "report": report}


def route_health_inspect_summary(run_dir: str | Path) -> dict[str, Any] | None:
    path = Path(run_dir).expanduser() / "analysis" / "route_health" / "route_health.json"
    if not path.exists():
        return None
    report = _read_json_object(path)
    if report is None:
        return None
    metrics = report.get("run_metrics") or {}
    geometry = report.get("route_geometry") or {}
    verdict = report.get("verdict") or {}
    return {
        "status": verdict.get("status"),
        "route_id": report.get("route_id"),
        "lateral_error_max_m": metrics.get("lateral_error_max_m"),
        "heading_error_max_rad": metrics.get("heading_error_max_rad"),
        "curve_segments_count": geometry.get("curve_segments_count"),
        "missing_inputs_count": len(report.get("missing_inputs") or []),
        "missing_fields_count": len(report.get("missing_fields") or []),
        "path": str(path),
    }
