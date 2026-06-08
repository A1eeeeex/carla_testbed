from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

OBSTACLE_GT_CONTRACT_SCHEMA_VERSION = "obstacle_gt_contract.v1"

DYNAMIC_SCENARIO_CLASSES = {
    "follow_stop",
    "followstop",
    "dynamic_obstacle",
    "traffic_light_red_stop_with_lead",
}


def analyze_obstacle_gt_contract_file(
    path: str | Path,
    *,
    scenario_class: str | None = None,
    dynamic_obstacle_required: bool | None = None,
    pedestrian_required: bool | None = None,
    fixed_scene_actor_roles: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source_path = Path(path).expanduser()
    records = _read_records(source_path)
    return analyze_obstacle_gt_contract_records(
        records,
        scenario_class=scenario_class,
        dynamic_obstacle_required=dynamic_obstacle_required,
        pedestrian_required=pedestrian_required,
        source_path=source_path,
        fixed_scene_actor_roles=fixed_scene_actor_roles,
    )


def analyze_obstacle_gt_contract_run_dir(
    run_dir: str | Path,
    *,
    scenario_class: str | None = None,
    dynamic_obstacle_required: bool | None = None,
    pedestrian_required: bool | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    if pedestrian_required is None:
        pedestrian_required = _run_dir_declares_walkers(root)
    fixed_scene_actor_roles = _fixed_scene_actor_roles(root)
    source_path = _find_first(
        root,
        [
            "artifacts/obstacle_gt_contract.jsonl",
            "obstacle_gt_contract.jsonl",
            "analysis/obstacle_gt_contract/obstacle_gt_contract.jsonl",
            "artifacts/obstacle_gt_contract.json",
            "obstacle_gt_contract.json",
        ],
    )
    if source_path is None:
        return analyze_obstacle_gt_contract_records(
            [],
            scenario_class=scenario_class,
            dynamic_obstacle_required=dynamic_obstacle_required,
            pedestrian_required=pedestrian_required,
            source_path=None,
            fixed_scene_actor_roles=fixed_scene_actor_roles,
        )
    return analyze_obstacle_gt_contract_file(
        source_path,
        scenario_class=scenario_class,
        dynamic_obstacle_required=dynamic_obstacle_required,
        pedestrian_required=pedestrian_required,
        fixed_scene_actor_roles=fixed_scene_actor_roles,
    )


def analyze_obstacle_gt_contract_records(
    records: Sequence[Mapping[str, Any]],
    *,
    scenario_class: str | None = None,
    dynamic_obstacle_required: bool | None = None,
    pedestrian_required: bool | None = None,
    source_path: str | Path | None = None,
    fixed_scene_actor_roles: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    dynamic_required = (
        _scenario_requires_dynamic_obstacle(scenario_class)
        if dynamic_obstacle_required is None
        else bool(dynamic_obstacle_required)
    )
    pedestrian_required = (
        _scenario_requires_pedestrian_obstacle(scenario_class)
        if pedestrian_required is None
        else bool(pedestrian_required)
    )
    errors: list[str] = []
    warnings: list[str] = []
    missing_fields: list[str] = []
    object_results: list[dict[str, Any]] = []
    id_map: dict[str, str] = {}
    tracking_by_apollo_id: dict[str, float] = {}
    message_count = len(records)
    empty_message_count = 0
    pedestrian_results: list[dict[str, Any]] = []
    expected_fixed_scene_actor_ids = {
        str(actor_id)
        for actor_id in (fixed_scene_actor_roles or {}).values()
        if actor_id not in {None, ""}
    }

    if not records:
        missing_fields.append("obstacle_gt_contract.records")
        if pedestrian_required:
            missing_fields.append("pedestrian_obstacle_records")

    for index, record in enumerate(records):
        objects = _record_objects(record)
        if not objects and _looks_like_object(record):
            objects = [record]
        if not objects and _record_declares_empty_obstacle_message(record):
            empty_message_count += 1
            continue
        timestamp = _num(record.get("timestamp"))
        ego_actor_id = _optional_text(record.get("ego_actor_id"))
        for object_index, obstacle in enumerate(objects):
            result = _check_obstacle(
                obstacle,
                timestamp=timestamp,
                record_index=index,
                object_index=object_index,
                ego_actor_id=ego_actor_id,
                dynamic_required=dynamic_required,
                id_map=id_map,
                tracking_by_apollo_id=tracking_by_apollo_id,
                pedestrian_required=pedestrian_required,
            )
            object_results.append(result)
            if result.get("is_pedestrian"):
                pedestrian_results.append(result)
            errors.extend(result["errors"])
            warnings.extend(result["warnings"])
            missing_fields.extend(result["missing_fields"])

    if pedestrian_required and records and not pedestrian_results:
        errors.append("pedestrian_obstacle_section_missing")
    observed_carla_actor_ids = {
        str(result.get("carla_actor_id"))
        for result in object_results
        if result.get("carla_actor_id") not in {None, ""}
    }
    missing_fixed_scene_actor_ids = sorted(expected_fixed_scene_actor_ids - observed_carla_actor_ids)
    if missing_fixed_scene_actor_ids:
        errors.append("fixed_scene_actor_missing_from_obstacle_gt")

    if object_results:
        if errors:
            status = "fail"
        elif missing_fields:
            status = "insufficient_data"
        elif warnings:
            status = "warn"
        else:
            status = "pass"
    elif records and empty_message_count == message_count:
        if dynamic_required:
            status = "fail"
            errors.append("required_dynamic_obstacle_missing")
        elif pedestrian_required:
            status = "fail"
            errors.append("required_pedestrian_obstacle_missing")
        else:
            status = "pass_empty"
    elif missing_fields and not object_results:
        status = "insufficient_data"
    else:
        status = "insufficient_data"
    if errors:
        status = "fail"
    return {
        "schema_version": OBSTACLE_GT_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "scenario_class": scenario_class,
        "dynamic_obstacle_required": dynamic_required,
        "pedestrian_required": pedestrian_required,
        "message_count": message_count,
        "empty_message_count": empty_message_count,
        "empty_obstacle_messages_healthy": bool(
            records and empty_message_count == message_count and not dynamic_required
        ),
        "object_count": len(object_results),
        "object_results": object_results,
        "fixed_scene_actor_linkage": {
            "required": bool(expected_fixed_scene_actor_ids),
            "expected_actor_roles": dict(fixed_scene_actor_roles or {}),
            "observed_carla_actor_ids": sorted(observed_carla_actor_ids),
            "missing_actor_ids": missing_fixed_scene_actor_ids,
            "status": (
                "fail"
                if missing_fixed_scene_actor_ids
                else ("pass" if expected_fixed_scene_actor_ids else "not_applicable")
            ),
        },
        "pedestrians": {
            "required": pedestrian_required,
            "object_count": len(pedestrian_results),
            "carla_actor_ids": sorted(
                {
                    str(item.get("carla_actor_id"))
                    for item in pedestrian_results
                    if item.get("carla_actor_id") not in {None, ""}
                }
            ),
            "status": _pedestrian_status(
                pedestrian_required=pedestrian_required,
                pedestrian_results=pedestrian_results,
                errors=errors,
            ),
            "errors": sorted(
                {
                    error
                    for item in pedestrian_results
                    for error in item.get("errors", [])
                }
                | (
                    {"pedestrian_obstacle_section_missing"}
                    if "pedestrian_obstacle_section_missing" in errors
                    else set()
                )
            ),
            "warnings": sorted(
                {
                    warning
                    for item in pedestrian_results
                    for warning in item.get("warnings", [])
                }
            ),
        },
        "errors": sorted(set(errors)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing_fields)),
        "source": {
            "path": str(source_path) if source_path is not None else None,
            "expected_artifact": "artifacts/obstacle_gt_contract.jsonl",
        },
        "interpretation_boundary": (
            "This report validates CARLA GT obstacle contract evidence only. "
            "It does not prove Apollo prediction/planning behavior."
        ),
    }


def write_obstacle_gt_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "obstacle_gt_contract_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"obstacle_gt_contract_report": str(path)}


def _check_obstacle(
    obstacle: Mapping[str, Any],
    *,
    timestamp: float | None,
    record_index: int,
    object_index: int,
    ego_actor_id: str | None,
    dynamic_required: bool,
    id_map: dict[str, str],
    tracking_by_apollo_id: dict[str, float],
    pedestrian_required: bool,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    missing_fields: list[str] = []
    carla_actor_id = _first_text(obstacle, "carla_actor_id", obstacle, "actor_id")
    apollo_id = _first_text(obstacle, "apollo_perception_id", obstacle, "perception_id", obstacle, "id")
    is_pedestrian = _is_pedestrian_obstacle(obstacle)

    if not carla_actor_id:
        missing_fields.append("carla_actor_id")
    if not apollo_id:
        missing_fields.append("apollo_perception_id")
    if carla_actor_id and ego_actor_id and carla_actor_id == ego_actor_id:
        errors.append("ego_actor_included_as_obstacle")
    if _boolish(obstacle.get("is_ego")):
        errors.append("ego_actor_included_as_obstacle")

    if carla_actor_id and apollo_id:
        previous = id_map.setdefault(carla_actor_id, apollo_id)
        if previous != apollo_id:
            errors.append("unstable_carla_actor_to_apollo_perception_id")

    if _boolish(obstacle.get("frame_transform_checked")) is not True:
        missing_fields.append("frame_transform_checked")
    if _boolish(obstacle.get("position_frame_apollo_map")) is False:
        errors.append("position_not_in_apollo_map_frame")
    if _boolish(obstacle.get("theta_frame_checked")) is not True:
        missing_fields.append("theta_frame_checked")

    for field in ("length", "width", "height"):
        value = _num(_first_nested(obstacle, field, ("dimensions", field), ("size", field)))
        if value is None:
            missing_fields.append(field)
        elif value <= 0:
            errors.append(f"{field}_non_positive")

    velocity_source = str(obstacle.get("velocity_source") or "").strip()
    velocity = _velocity_tuple(obstacle)
    velocity_norm = math.sqrt(sum(component * component for component in velocity)) if velocity else None
    dynamic = _boolish(obstacle.get("dynamic")) or _boolish(obstacle.get("is_dynamic"))
    if velocity_source in {
        "",
        "missing",
        "zero_filled",
        "Detection3DArray_missing_velocity",
        "MarkerArray_missing_velocity",
    }:
        warnings.append("velocity_source_missing_or_zero_filled")
        if dynamic_required and dynamic:
            errors.append("dynamic_actor_velocity_zero_filled")
        if (
            pedestrian_required
            and is_pedestrian
            and dynamic
            and not _boolish(obstacle.get("actually_stationary"))
        ):
            errors.append("pedestrian_dynamic_velocity_zero_filled")
    elif velocity_norm is None:
        missing_fields.append("velocity")
    elif dynamic_required and dynamic and velocity_norm <= 1e-3 and not _boolish(obstacle.get("actually_stationary")):
        errors.append("dynamic_actor_velocity_zero_filled")

    tracking_time = _num(obstacle.get("tracking_time"))
    if tracking_time is None:
        missing_fields.append("tracking_time")
    elif apollo_id:
        previous_tracking = tracking_by_apollo_id.get(apollo_id)
        if previous_tracking is not None and tracking_time < previous_tracking:
            errors.append("tracking_time_non_monotonic")
        tracking_by_apollo_id[apollo_id] = tracking_time

    return {
        "record_index": record_index,
        "object_index": object_index,
        "carla_actor_id": carla_actor_id,
        "apollo_perception_id": apollo_id,
        "is_pedestrian": is_pedestrian,
        "timestamp": timestamp,
        "velocity_source": velocity_source or None,
        "velocity_norm_mps": velocity_norm,
        "errors": sorted(set(errors)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing_fields)),
        "status": "fail" if errors else ("insufficient_data" if missing_fields else ("warn" if warnings else "pass")),
    }


def _read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            records = payload.get("records") or payload.get("frames") or payload.get("objects")
            if isinstance(records, list):
                return [item for item in records if isinstance(item, dict)]
            return [payload]
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = root / relative
        if path.exists():
            return path
    return None


def _run_dir_declares_walkers(root: Path) -> bool:
    manifest_path = _find_first(root, ["artifacts/traffic_flow_manifest.json", "traffic_flow_manifest.json"])
    if manifest_path is None:
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(manifest, Mapping):
        return False
    try:
        return int(manifest.get("requested_walker_count", 0) or 0) > 0 or int(
            manifest.get("spawned_walker_count", 0) or 0
        ) > 0
    except (TypeError, ValueError):
        return False


def _fixed_scene_actor_roles(root: Path) -> dict[str, Any]:
    path = _find_first(root, ["artifacts/fixed_scene_runtime_state.json", "fixed_scene_runtime_state.json"])
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, Mapping):
        return {}
    roles = payload.get("actor_roles")
    return dict(roles) if isinstance(roles, Mapping) else {}


def _record_objects(record: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("objects", "obstacles", "perception_obstacles"):
        value = record.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _looks_like_object(record: Mapping[str, Any]) -> bool:
    return any(
        _optional_text(record.get(key))
        for key in ("carla_actor_id", "actor_id", "apollo_perception_id", "perception_id")
    )


def _record_declares_empty_obstacle_message(record: Mapping[str, Any]) -> bool:
    published_count = _num(record.get("published_obstacle_count"))
    if published_count is not None:
        return published_count <= 0
    payload_count = _num(record.get("payload_count"))
    if payload_count is not None:
        return payload_count <= 0
    if _boolish(record.get("empty_message")):
        return True
    return False


def _scenario_requires_dynamic_obstacle(scenario_class: str | None) -> bool:
    text = str(scenario_class or "").strip()
    return text in DYNAMIC_SCENARIO_CLASSES or "follow" in text or "obstacle" in text


def _scenario_requires_pedestrian_obstacle(scenario_class: str | None) -> bool:
    text = str(scenario_class or "").strip().lower()
    return "pedestrian" in text or "walker" in text


def _pedestrian_status(
    *,
    pedestrian_required: bool,
    pedestrian_results: Sequence[Mapping[str, Any]],
    errors: Sequence[str],
) -> str:
    if not pedestrian_required:
        return "not_applicable"
    if errors:
        return "fail"
    if pedestrian_results:
        return "pass"
    return "insufficient_data"


def _is_pedestrian_obstacle(obstacle: Mapping[str, Any]) -> bool:
    fields = [
        obstacle.get("object_type"),
        obstacle.get("type"),
        obstacle.get("classification"),
        obstacle.get("label"),
        obstacle.get("sub_type"),
        obstacle.get("blueprint_id"),
    ]
    return any("pedestrian" in str(value).lower() or "walker" in str(value).lower() for value in fields if value)


def _velocity_tuple(obstacle: Mapping[str, Any]) -> tuple[float, float, float] | None:
    velocity = obstacle.get("velocity")
    if isinstance(velocity, Mapping):
        values = (_num(velocity.get("x")), _num(velocity.get("y")), _num(velocity.get("z")))
    else:
        values = (_num(obstacle.get("vx")), _num(obstacle.get("vy")), _num(obstacle.get("vz")))
    if any(value is None for value in values):
        return None
    return (float(values[0]), float(values[1]), float(values[2]))


def _first_nested(obstacle: Mapping[str, Any], flat_key: str, *nested_keys: tuple[str, str]) -> Any:
    value = obstacle.get(flat_key)
    if value not in {None, ""}:
        return value
    for parent, child in nested_keys:
        payload = obstacle.get(parent)
        if isinstance(payload, Mapping) and payload.get(child) not in {None, ""}:
            return payload.get(child)
    return None


def _first_text(*args: Any) -> str | None:
    for mapping, key in zip(args[0::2], args[1::2]):
        if isinstance(mapping, Mapping):
            value = mapping.get(str(key))
            if value not in {None, ""}:
                return str(value)
    return None


def _optional_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "enabled"}
