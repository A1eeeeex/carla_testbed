from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

import yaml

CONTRACT_SCHEMA_VERSION = "town01_apollo_contract.v1"
REPORT_SCHEMA_VERSION = "town01_apollo_contract_report.v2"
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

CONTRACT_LEVELS = (
    "missing",
    "placeholder",
    "schema_only",
    "route_geometry_available",
    "hdmap_file_present",
    "hdmap_verified",
    "reference_line_verified",
    "signal_overlap_verified",
)
LEVEL_RANK = {level: index for index, level in enumerate(CONTRACT_LEVELS)}
VERIFICATION_ARTIFACT_KEYS = (
    "hdmap_validation_report",
    "reference_line_validation_report",
    "signal_overlap_report",
    "route_projection_report",
    "roadrunner_conversion_report",
)


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


def _is_placeholder(value: Any) -> bool:
    if value in (None, ""):
        return False
    return "placeholder" in str(value).lower() or str(value).strip().startswith("${")


def _placeholder_fields(payload: Any, prefix: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            current = f"{prefix}.{key}" if prefix else str(key)
            found.extend(_placeholder_fields(value, current))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            current = f"{prefix}[{index}]"
            found.extend(_placeholder_fields(value, current))
    elif _is_placeholder(payload):
        found.append(prefix)
    return found


def _min_level(*levels: str) -> str:
    valid = [level for level in levels if level in LEVEL_RANK]
    if not valid:
        return "missing"
    return min(valid, key=lambda item: LEVEL_RANK[item])


def _max_level(*levels: str) -> str:
    valid = [level for level in levels if level in LEVEL_RANK]
    if not valid:
        return "missing"
    return max(valid, key=lambda item: LEVEL_RANK[item])


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
        "placeholder_fields": _placeholder_fields(route),
        "spawn_distance_m": None,
        "spawn_heading_error_rad": None,
        "nearest_route_index": None,
        "route_ref_exists": None,
        "route_definition_hash": route.get("route_definition_hash"),
        "route_contract_level": "missing",
        "spawn_contract_level": "missing",
    }

    if result["placeholder_fields"]:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("route_contains_placeholder_fields")

    missing = sorted(field for field in ROUTE_REQUIRED_FIELDS if not route.get(field))
    if missing:
        result["status"] = "fail"
        result["issues"].extend(f"missing_{field}" for field in missing)

    if "spawn_pose" not in route and "spawn_ref" not in route:
        result["status"] = "fail"
        result["issues"].append("missing_spawn_pose_or_spawn_ref")
        result["spawn_contract_level"] = "missing"
    if "goal_pose" not in route and "goal_ref" not in route:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("missing_goal_pose_or_goal_ref")

    points, route_warnings = _route_points(route, source_dir)
    for warning in route_warnings:
        result["warnings"].append(warning)
        result["status"] = _combine_status(result["status"], "warn")
    route_ref = str(route.get("route_ref") or "")
    result["route_ref_exists"] = bool(points) or route_ref.startswith("inline:")
    if result["placeholder_fields"]:
        result["route_contract_level"] = "placeholder"
    elif points:
        result["route_contract_level"] = "route_geometry_available"
    elif missing:
        result["route_contract_level"] = "missing"
    else:
        result["route_contract_level"] = "schema_only"

    spawn_pose = _as_pose(route.get("spawn_pose"))
    if spawn_pose is None:
        if route.get("spawn_ref"):
            result["status"] = _combine_status(result["status"], "warn")
            result["warnings"].append("spawn_ref_present_spawn_pose_unchecked")
            result["spawn_contract_level"] = "placeholder" if _is_placeholder(route.get("spawn_ref")) else "schema_only"
        return result
    if not points:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("spawn_geometry_unchecked_no_route_points")
        result["spawn_contract_level"] = "placeholder" if result["placeholder_fields"] else "schema_only"
        return result

    nearest = _nearest_route_point(points, spawn_pose)
    if nearest is None:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("spawn_distance_unchecked_bad_geometry")
        result["spawn_contract_level"] = "placeholder" if result["placeholder_fields"] else "schema_only"
        return result

    nearest_index, distance = nearest
    result["nearest_route_index"] = nearest_index
    result["spawn_distance_m"] = distance
    result["spawn_contract_level"] = "placeholder" if result["placeholder_fields"] else "route_geometry_available"
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
        "placeholder_fields": _placeholder_fields(signal),
        "apollo_signal_id": signal.get("apollo_signal_id"),
        "stop_line_id": signal.get("stop_line_id"),
        "lane_ids": list(signal.get("lane_ids") or []) if isinstance(signal.get("lane_ids"), list) else [],
        "carla_actor_id": signal.get("carla_actor_id"),
        "carla_landmark_id": signal.get("carla_landmark_id"),
        "traffic_light_mappable": False,
        "signal_contract_level": "missing",
    }

    if result["placeholder_fields"]:
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("signal_contains_placeholder_fields")

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
    lane_ids = result["lane_ids"]
    if lane_ids and all(_is_placeholder(item) for item in lane_ids):
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("lane_ids_all_placeholder")
    if result["placeholder_fields"]:
        result["signal_contract_level"] = "placeholder"
    elif result["issues"]:
        result["signal_contract_level"] = "missing"
    else:
        result["signal_contract_level"] = "schema_only"
    return result


def _artifact_path(contract: Mapping[str, Any], key: str, source_dir: Path) -> Path | None:
    artifacts = contract.get("verification_artifacts")
    if not isinstance(artifacts, Mapping):
        artifacts = {}
    raw = artifacts.get(key) or contract.get(key)
    if raw in (None, ""):
        return None
    path = Path(str(raw)).expanduser()
    if not path.is_absolute():
        path = source_dir / path
    return path


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_status_pass(payload: Mapping[str, Any]) -> bool:
    status = str(payload.get("status") or payload.get("verdict") or "").lower()
    if status == "pass":
        return True
    verdict = payload.get("verdict")
    if isinstance(verdict, Mapping) and str(verdict.get("status") or "").lower() == "pass":
        return True
    return False


def _artifact_verified(payload: Mapping[str, Any], *keys: str) -> bool:
    if not payload or not _artifact_status_pass(payload):
        return False
    for key in keys:
        value = payload.get(key)
        if value is True or str(value).lower() in {"true", "verified", "pass"}:
            return True
    level = str(payload.get("contract_level") or "").strip()
    return level in {"hdmap_verified", "reference_line_verified", "signal_overlap_verified"}


def _verification_artifacts(contract: Mapping[str, Any], source_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    paths: dict[str, Any] = {}
    payloads: dict[str, Any] = {}
    for key in VERIFICATION_ARTIFACT_KEYS:
        path = _artifact_path(contract, key, source_dir)
        exists = bool(path and path.exists())
        paths[key] = {
            "path": None if path is None else str(path),
            "exists": exists,
        }
        payloads[key] = _read_json(path)
    return paths, payloads


def _map_source(contract: Mapping[str, Any], artifacts: Mapping[str, Any]) -> str:
    explicit = contract.get("map_source")
    if explicit not in (None, ""):
        return str(explicit)
    roadrunner = artifacts.get("roadrunner_conversion_report")
    if isinstance(roadrunner, Mapping) and roadrunner:
        return "roadrunner_conversion"
    root = str(contract.get("apollo_map_root") or "")
    if root:
        return "apollo_map_root"
    return "missing"


def _hdmap_contract_level(contract: Mapping[str, Any], artifact_payloads: Mapping[str, Any]) -> str:
    map_root = str(contract.get("apollo_map_root") or "")
    if map_root and _is_placeholder(map_root):
        return "placeholder"
    hdmap = artifact_payloads.get("hdmap_validation_report")
    if isinstance(hdmap, Mapping) and _artifact_verified(hdmap, "hdmap_verified"):
        return "hdmap_verified"
    if not map_root:
        return "missing"
    root = Path(map_root).expanduser()
    if root.exists() and any((root / name).exists() for name in ("base_map.bin", "base_map.xml", "sim_map.bin")):
        return "hdmap_file_present"
    return "schema_only"


def _reference_line_contract_level(artifact_payloads: Mapping[str, Any]) -> str:
    ref = artifact_payloads.get("reference_line_validation_report")
    if isinstance(ref, Mapping) and _artifact_verified(ref, "reference_line_verified"):
        return "reference_line_verified"
    return "missing"


def _signal_contract_level(signal_results: list[Mapping[str, Any]], artifact_payloads: Mapping[str, Any]) -> str:
    if not signal_results:
        return "missing"
    if any(item.get("signal_contract_level") == "placeholder" for item in signal_results):
        return "placeholder"
    signal = artifact_payloads.get("signal_overlap_report")
    if isinstance(signal, Mapping) and _artifact_verified(signal, "signal_overlap_verified"):
        return "signal_overlap_verified"
    if all(item.get("signal_contract_level") == "schema_only" for item in signal_results):
        return "schema_only"
    return "missing"


def _route_contract_level(route_results: list[Mapping[str, Any]]) -> str:
    if not route_results:
        return "missing"
    levels = [str(item.get("route_contract_level") or "missing") for item in route_results]
    if any(level == "missing" for level in levels):
        return "missing"
    if any(level == "placeholder" for level in levels):
        return "placeholder"
    if any(level == "route_geometry_available" for level in levels):
        return "route_geometry_available"
    return "schema_only"


def _spawn_contract_level(route_results: list[Mapping[str, Any]]) -> str:
    if not route_results:
        return "missing"
    levels = [str(item.get("spawn_contract_level") or "missing") for item in route_results]
    if any(level == "missing" for level in levels):
        return "missing"
    if any(level == "placeholder" for level in levels):
        return "placeholder"
    if any(level == "route_geometry_available" for level in levels):
        return "route_geometry_available"
    return "schema_only"


def _has_level(level: str, minimum: str) -> bool:
    return LEVEL_RANK.get(level, -1) >= LEVEL_RANK[minimum]


def _missing_evidence(
    *,
    route_level: str,
    spawn_level: str,
    hdmap_level: str,
    reference_level: str,
    signal_level: str,
) -> list[str]:
    missing: list[str] = []
    if not _has_level(route_level, "route_geometry_available"):
        missing.append("route_geometry_available")
    if not _has_level(spawn_level, "route_geometry_available"):
        missing.append("spawn_valid")
    if hdmap_level != "hdmap_verified":
        missing.append("hdmap_validation_report")
    if reference_level != "reference_line_verified":
        missing.append("reference_line_validation_report")
    if signal_level != "signal_overlap_verified":
        missing.append("signal_overlap_report")
    return missing


def _overall_contract_level(*levels: str) -> str:
    valid = [level for level in levels if level in LEVEL_RANK]
    if not valid:
        return "missing"
    if any(level == "placeholder" for level in valid):
        return "placeholder"
    if any(level == "missing" for level in valid):
        return "missing"
    return _min_level(*valid)


def check_town01_apollo_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    source_dir = Path(str(contract.get("_source_dir") or "."))
    thresholds = dict(DEFAULT_ROUTE_THRESHOLDS)
    thresholds.update(contract.get("thresholds") or {})
    verification_artifacts, artifact_payloads = _verification_artifacts(contract, source_dir)
    contract_payload = {key: value for key, value in contract.items() if not str(key).startswith("_")}
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": "pass",
        "map_name": contract.get("map_name"),
        "map_source": _map_source(contract, artifact_payloads),
        "apollo_map_root": contract.get("apollo_map_root"),
        "hdmap_contract_level": "missing",
        "route_contract_level": "missing",
        "signal_contract_level": "missing",
        "spawn_contract_level": "missing",
        "reference_line_contract_level": "missing",
        "overall_contract_level": "missing",
        "can_claim_lane_keep": False,
        "can_claim_junction": False,
        "can_claim_traffic_light": False,
        "missing_evidence": [],
        "placeholder_fields": _placeholder_fields(contract_payload),
        "verification_artifacts": verification_artifacts,
        "roadrunner_conversion_metadata": artifact_payloads.get("roadrunner_conversion_report") or None,
        "route_results": [],
        "signal_results": [],
        "warnings": [],
        "errors": [],
        "missing_fields": [],
    }

    if contract.get("schema_version") not in {None, CONTRACT_SCHEMA_VERSION}:
        report["status"] = "fail"
        report["errors"].append(f"schema_version must be {CONTRACT_SCHEMA_VERSION}")
    if not contract.get("map_name"):
        report["status"] = "fail"
        report["errors"].append("map_name_missing")
        report["missing_fields"].append("map_name")
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
    report["hdmap_contract_level"] = _hdmap_contract_level(contract, artifact_payloads)
    report["route_contract_level"] = _route_contract_level(report["route_results"])
    report["spawn_contract_level"] = _spawn_contract_level(report["route_results"])
    report["reference_line_contract_level"] = _reference_line_contract_level(artifact_payloads)
    report["signal_contract_level"] = _signal_contract_level(report["signal_results"], artifact_payloads)
    report["overall_contract_level"] = _overall_contract_level(
        report["route_contract_level"],
        report["spawn_contract_level"],
        report["hdmap_contract_level"],
        report["reference_line_contract_level"],
        report["signal_contract_level"],
    )

    route_ready = _has_level(str(report["route_contract_level"]), "route_geometry_available")
    spawn_ready = _has_level(str(report["spawn_contract_level"]), "route_geometry_available")
    report["can_claim_lane_keep"] = bool(route_ready and spawn_ready)
    report["can_claim_junction"] = bool(
        report["can_claim_lane_keep"] and report["reference_line_contract_level"] == "reference_line_verified"
    )
    report["can_claim_traffic_light"] = bool(
        report["can_claim_lane_keep"] and report["signal_contract_level"] == "signal_overlap_verified"
    )
    report["missing_evidence"] = _missing_evidence(
        route_level=str(report["route_contract_level"]),
        spawn_level=str(report["spawn_contract_level"]),
        hdmap_level=str(report["hdmap_contract_level"]),
        reference_level=str(report["reference_line_contract_level"]),
        signal_level=str(report["signal_contract_level"]),
    )
    return report


def check_town01_apollo_contract_file(path: str | Path) -> dict[str, Any]:
    return check_town01_apollo_contract(load_town01_apollo_contract(path))


def write_town01_apollo_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "town01_apollo_contract_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"town01_apollo_contract_report": str(path)}
