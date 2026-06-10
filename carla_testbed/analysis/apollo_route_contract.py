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

APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION = "apollo_route_contract.v1"

MAX_ABS_ROUTE_LENGTH_DIFF_M = 20.0
MAX_REL_ROUTE_LENGTH_DIFF = 0.15
MAX_GOAL_ERROR_M = 20.0
MAX_EXTRA_LANE_WINDOWS_FOR_CLAIM = 2


def analyze_apollo_route_contract_run_dir(
    run_dir: str | Path,
    *,
    frame_transform: str | Path | Mapping[str, Any] | ApolloFrameTransform | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    frame_transform_obj, frame_transform_source = _resolve_frame_transform(frame_transform)
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

    scenario = _scenario_route(manifest, summary)
    apollo = _apollo_route(planning_topic_debug_summary, routing_rows, route_rows)
    projection = _projection_route(hdmap_rows)
    route_phase = _route_phase(apollo, cyber_bridge_stats, scenario.get("route_length_m"))
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
    if length_delta is not None and length_tolerance is not None and abs(length_delta) > length_tolerance:
        blocking.append("apollo_routing_length_mismatch")

    scenario_lane_keys = set(scenario.get("route_lane_keys") or [])
    apollo_lane_keys = set(apollo.get("routing_lane_keys") or [])
    apollo_extra_lane_keys = sorted(apollo_lane_keys - scenario_lane_keys)
    missing_scenario_lane_keys = sorted(scenario_lane_keys - apollo_lane_keys)
    if not scenario_lane_keys:
        missing.append("scenario_route_lane_ids")
    if not apollo_lane_keys:
        missing.append("apollo_routing_lane_ids")
    if scenario_lane_keys and apollo_lane_keys and missing_scenario_lane_keys:
        blocking.append("apollo_routing_missing_scenario_lane")
    expected_lane_window_count = max(1, len(scenario_lane_keys) + MAX_EXTRA_LANE_WINDOWS_FOR_CLAIM)
    apollo_lane_window_count = apollo.get("routing_lane_window_count")
    if (
        apollo_lane_window_count is not None
        and scenario_lane_keys
        and apollo_lane_window_count > expected_lane_window_count
        and apollo_extra_lane_keys
    ):
        blocking.append("apollo_routing_lane_sequence_mismatch")

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

    if not routing_rows:
        warnings.append("routing_event_debug_missing")
    if not route_rows:
        warnings.append("planning_route_segment_debug_missing")
    if projection.get("available") is False:
        warnings.append("apollo_hdmap_projection_missing")
    if route_phase["routing_phase"] == "startup":
        blocking.append("claim_route_not_materialized")

    if blocking:
        status = "fail"
    elif missing:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"

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
        "scenario_start_lane": scenario.get("start_lane"),
        "scenario_goal_lane": scenario.get("goal_lane"),
        "scenario_route_lane_keys": sorted(scenario_lane_keys),
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
        "routing_phase": route_phase["routing_phase"],
        "routing_phase_reason": route_phase["routing_phase_reason"],
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
            f"- Comparison frame: `{report.get('comparison_frame')}`",
            f"- Transform source: `{report.get('transform_source')}`",
            f"- Route ID: `{report.get('route_id')}`",
            f"- Scenario route length m: `{report.get('scenario_route_length_m')}`",
            f"- Apollo routing total length m: `{report.get('apollo_routing_total_length_m')}`",
            f"- Routing length ratio: `{report.get('routing_length_ratio')}`",
            f"- Scenario start/goal lane: `{report.get('scenario_start_lane')}` / `{report.get('scenario_goal_lane')}`",
            f"- Apollo lane windows: `{report.get('apollo_routing_lane_window_count')}`",
            f"- Apollo lane signature: `{report.get('apollo_routing_lane_signature')}`",
            f"- Extra Apollo lane keys: `{', '.join(report.get('apollo_routing_extra_lane_keys') or []) or 'none'}`",
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
    )
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
    route_length = _num(
        scenario.get("route_length_m")
        or summary.get("route_length_m")
        or summary.get("expected_route_distance_m")
    )
    if route_length is None and len(route_trace) >= 2:
        first_s = _num(route_trace[0].get("s"))
        last_s = _num(route_trace[-1].get("s"))
        if first_s is not None and last_s is not None:
            route_length = abs(last_s - first_s)
    lane_keys = {_lane_key(lane) for lane in [*route_lanes, start_lane, goal_lane] if lane}
    return {
        "route_id": scenario.get("route_id") or summary.get("route_id") or manifest.get("route_id"),
        "route_length_m": route_length,
        "start_lane": start_lane,
        "goal_lane": goal_lane,
        "route_lane_keys": sorted(key for key in lane_keys if key),
        "start_xy": start_xy,
        "goal_xy": goal_xy,
    }


def _apollo_route(
    planning_summary: Mapping[str, Any],
    routing_rows: Sequence[Mapping[str, Any]],
    route_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    last_routing_row = _last_mapping(routing_rows)
    last_route_row = _last_route_row_with_signature(route_rows)
    lane_signature = _first_text(
        planning_summary,
        "last_routing_lane_window_signature",
        last_routing_row,
        "routing_lane_window_signature",
        last_route_row,
        "routing_lane_window_signature",
    )
    unique_signature = _first_text(
        planning_summary,
        "last_routing_unique_lane_signature",
        last_routing_row,
        "routing_unique_lane_signature",
        last_route_row,
        "routing_unique_lane_signature",
    )
    total_length = _first_num(
        planning_summary.get("last_routing_total_length"),
        last_routing_row.get("routing_total_length") if last_routing_row else None,
        last_routing_row.get("route_total_length_m") if last_routing_row else None,
        last_routing_row.get("goal_distance_m") if last_routing_row else None,
    )
    window_count = _first_int(
        planning_summary.get("last_routing_lane_window_count"),
        last_routing_row.get("routing_lane_window_count") if last_routing_row else None,
        last_route_row.get("routing_lane_window_count") if last_route_row else None,
    )
    lane_ids = _lane_ids_from_signature(lane_signature) or _lane_ids_from_signature(unique_signature)
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
