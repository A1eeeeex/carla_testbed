from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

import yaml

CONTRACT_SCHEMA_VERSION = "town01_apollo_contract.v1"
REPORT_SCHEMA_VERSION = "town01_apollo_contract_report.v1"
DEFAULT_ROUTE_THRESHOLDS = {
    "max_spawn_distance_m": 3.0,
    "max_spawn_heading_error_rad": 0.7853981633974483,
}
ROUTE_REQUIRED_FIELDS = {"route_id", "route_ref"}
SIGNAL_REQUIRED_FIELDS = {
    "logical_id",
    "apollo_signal_id",
    "stop_line_id",
    "lane_ids",
}


class Town01ApolloContractError(ValueError):
    pass


def load_town01_apollo_contract(path: str | Path) -> dict[str, Any]:
    contract_path = Path(path).expanduser()
    payload = yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise Town01ApolloContractError(f"Town01 Apollo contract must be a mapping: {contract_path}")
    payload["_source_path"] = str(contract_path)
    payload["_source_dir"] = str(contract_path.parent)
    return payload


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "fail": 2}.get(status, 2)


def _combine_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current


def _normalize_angle(angle_rad: float) -> float:
    while angle_rad > math.pi:
        angle_rad -= 2.0 * math.pi
    while angle_rad < -math.pi:
        angle_rad += 2.0 * math.pi
    return angle_rad


def _as_pose(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _xy(value: Mapping[str, Any]) -> tuple[float, float] | None:
    try:
        return float(value["x"]), float(value["y"])
    except (KeyError, TypeError, ValueError):
        return None


def _heading(value: Mapping[str, Any]) -> float | None:
    raw = value.get("heading", value.get("yaw"))
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _route_points(route: Mapping[str, Any], source_dir: Path) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    inline = route.get("route_points")
    if isinstance(inline, list):
        return [dict(point) for point in inline if isinstance(point, Mapping)], warnings

    route_ref = route.get("route_ref")
    if not isinstance(route_ref, str) or not route_ref:
        return [], ["route_ref_missing"]
    if route_ref.startswith("placeholder:"):
        return [], ["route_ref_placeholder"]
    if route_ref.startswith("inline:"):
        return [], ["route_ref_inline_without_points"]
    route_path = Path(route_ref)
    if not route_path.is_absolute():
        route_path = source_dir / route_path
    if not route_path.exists():
        return [], [f"route_ref_not_found:{route_ref}"]

    try:
        payload = json.loads(route_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return [], [f"route_ref_not_json:{route_ref}"]
    points = payload.get("points") if isinstance(payload, Mapping) else None
    if not isinstance(points, list):
        return [], [f"route_ref_points_missing:{route_ref}"]
    return [dict(point) for point in points if isinstance(point, Mapping)], warnings


def _nearest_route_point(points: list[Mapping[str, Any]], pose: Mapping[str, Any]) -> tuple[int, float] | None:
    pose_xy = _xy(pose)
    if pose_xy is None or not points:
        return None
    best_index = -1
    best_distance = float("inf")
    px, py = pose_xy
    for index, point in enumerate(points):
        point_xy = _xy(point)
        if point_xy is None:
            continue
        dx = px - point_xy[0]
        dy = py - point_xy[1]
        distance = math.hypot(dx, dy)
        if distance < best_distance:
            best_index = index
            best_distance = distance
    if best_index < 0:
        return None
    return best_index, best_distance


def _point_heading(points: list[Mapping[str, Any]], index: int) -> float | None:
    explicit = _heading(points[index])
    if explicit is not None:
        return explicit
    if len(points) < 2:
        return None
    prev_index = max(0, index - 1)
    next_index = min(len(points) - 1, index + 1)
    if prev_index == next_index:
        return None
    prev_xy = _xy(points[prev_index])
    next_xy = _xy(points[next_index])
    if prev_xy is None or next_xy is None:
        return None
    return math.atan2(next_xy[1] - prev_xy[1], next_xy[0] - prev_xy[0])


def _check_route(
    route: Mapping[str, Any],
    *,
    source_dir: Path,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    route_id = route.get("route_id")
    result: dict[str, Any] = {
        "route_id": route_id,
        "status": "pass",
        "issues": [],
        "warnings": [],
        "spawn_distance_m": None,
        "spawn_heading_error_rad": None,
        "nearest_route_index": None,
        "route_ref_exists": None,
    }

    missing = sorted(field for field in ROUTE_REQUIRED_FIELDS if not route.get(field))
    if missing:
        result["status"] = "fail"
        result["issues"].extend(f"missing_{field}" for field in missing)

    if "spawn_pose" not in route and "spawn_ref" not in route:
        result["status"] = "fail"
        result["issues"].append("missing_spawn_pose_or_spawn_ref")
    if "goal_pose" not in route and "goal_ref" not in route:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("missing_goal_pose_or_goal_ref")

    points, route_warnings = _route_points(route, source_dir)
    for warning in route_warnings:
        result["warnings"].append(warning)
        result["status"] = _combine_status(result["status"], "warn")
    route_ref = str(route.get("route_ref") or "")
    result["route_ref_exists"] = bool(points) or route_ref.startswith("inline:")

    spawn_pose = _as_pose(route.get("spawn_pose"))
    if spawn_pose is None:
        if route.get("spawn_ref"):
            result["status"] = _combine_status(result["status"], "warn")
            result["warnings"].append("spawn_ref_present_spawn_pose_unchecked")
        return result
    if not points:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("spawn_geometry_unchecked_no_route_points")
        return result

    nearest = _nearest_route_point(points, spawn_pose)
    if nearest is None:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("spawn_distance_unchecked_bad_geometry")
        return result

    nearest_index, distance = nearest
    result["nearest_route_index"] = nearest_index
    result["spawn_distance_m"] = distance
    if distance > float(thresholds["max_spawn_distance_m"]):
        result["status"] = "fail"
        result["issues"].append("spawn_too_far_from_route")

    spawn_heading = _heading(spawn_pose)
    route_heading = _point_heading(points, nearest_index)
    if spawn_heading is None or route_heading is None:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("spawn_heading_unchecked")
    else:
        heading_error = abs(_normalize_angle(spawn_heading - route_heading))
        result["spawn_heading_error_rad"] = heading_error
        if heading_error > float(thresholds["max_spawn_heading_error_rad"]):
            result["status"] = "fail"
            result["issues"].append("spawn_heading_misaligned")

    return result


def _check_signal(signal: Mapping[str, Any]) -> dict[str, Any]:
    logical_id = signal.get("logical_id")
    result: dict[str, Any] = {
        "logical_id": logical_id,
        "status": "pass",
        "issues": [],
        "warnings": [],
        "apollo_signal_id": signal.get("apollo_signal_id"),
        "stop_line_id": signal.get("stop_line_id"),
        "lane_ids": list(signal.get("lane_ids") or []) if isinstance(signal.get("lane_ids"), list) else [],
        "carla_actor_id": signal.get("carla_actor_id"),
        "carla_landmark_id": signal.get("carla_landmark_id"),
        "traffic_light_mappable": False,
    }

    missing = sorted(field for field in SIGNAL_REQUIRED_FIELDS if not signal.get(field))
    if missing:
        result["status"] = "fail"
        result["issues"].extend(f"missing_{field}" for field in missing)
    lane_ids = signal.get("lane_ids")
    if not isinstance(lane_ids, list) or not lane_ids:
        result["status"] = "fail"
        result["issues"].append("missing_lane_ids")

    carla_id = signal.get("carla_actor_id") or signal.get("carla_landmark_id")
    if carla_id and not str(carla_id).startswith("placeholder:"):
        result["traffic_light_mappable"] = True
    elif carla_id:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("carla_traffic_light_id_placeholder")
    else:
        result["status"] = "fail"
        result["issues"].append("missing_carla_actor_or_landmark_id")

    if str(signal.get("apollo_signal_id", "")).startswith("placeholder:"):
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("apollo_signal_id_placeholder")
    if str(signal.get("stop_line_id", "")).startswith("placeholder:"):
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("stop_line_id_placeholder")
    return result


def check_town01_apollo_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    source_dir = Path(str(contract.get("_source_dir") or "."))
    thresholds = dict(DEFAULT_ROUTE_THRESHOLDS)
    thresholds.update(contract.get("thresholds") or {})
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": "pass",
        "map_name": contract.get("map_name"),
        "apollo_map_root": contract.get("apollo_map_root"),
        "route_results": [],
        "signal_results": [],
        "warnings": [],
        "errors": [],
        "missing_fields": [],
    }

    if contract.get("schema_version") not in {None, CONTRACT_SCHEMA_VERSION}:
        report["status"] = "fail"
        report["errors"].append(f"schema_version must be {CONTRACT_SCHEMA_VERSION}")
    if contract.get("map_name") != "Town01":
        report["status"] = "fail"
        report["errors"].append("map_name must be Town01")
    if not contract.get("apollo_map_root"):
        report["status"] = _combine_status(report["status"], "warn")
        report["warnings"].append("apollo_map_root_missing")
        report["missing_fields"].append("apollo_map_root")
    else:
        map_root = str(contract["apollo_map_root"])
        if "${" in map_root:
            report["status"] = _combine_status(report["status"], "warn")
            report["warnings"].append("apollo_map_root_env_placeholder_unverified")
        elif not Path(map_root).expanduser().exists():
            report["status"] = _combine_status(report["status"], "warn")
            report["warnings"].append("apollo_map_root_not_found")

    routes = contract.get("routes")
    if not isinstance(routes, list) or not routes:
        report["status"] = "fail"
        report["errors"].append("routes must be a non-empty list")
        routes = []
    seen_route_ids: set[str] = set()
    for route in routes:
        if not isinstance(route, Mapping):
            report["status"] = "fail"
            report["errors"].append("route entry must be a mapping")
            continue
        route_id = str(route.get("route_id") or "")
        if route_id in seen_route_ids:
            report["status"] = "fail"
            report["errors"].append(f"duplicate route_id: {route_id}")
        seen_route_ids.add(route_id)
        route_result = _check_route(route, source_dir=source_dir, thresholds=thresholds)
        report["route_results"].append(route_result)
        report["status"] = _combine_status(report["status"], route_result["status"])

    signals = contract.get("signals")
    if not isinstance(signals, list):
        report["status"] = _combine_status(report["status"], "warn")
        report["warnings"].append("signals_missing_or_not_list")
        signals = []
    if not signals:
        report["status"] = _combine_status(report["status"], "warn")
        report["warnings"].append("signals_empty")
    for signal in signals:
        if not isinstance(signal, Mapping):
            report["status"] = "fail"
            report["errors"].append("signal entry must be a mapping")
            continue
        signal_result = _check_signal(signal)
        report["signal_results"].append(signal_result)
        report["status"] = _combine_status(report["status"], signal_result["status"])

    if not report["signal_results"]:
        report["warnings"].append("traffic_light_signal_contract_unverified")
    return report


def check_town01_apollo_contract_file(path: str | Path) -> dict[str, Any]:
    return check_town01_apollo_contract(load_town01_apollo_contract(path))


def write_town01_apollo_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "town01_apollo_contract_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"town01_apollo_contract_report": str(path)}
