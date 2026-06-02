from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

APOLLO_FRAME_TRANSFORM_SCHEMA_VERSION = "apollo_frame_transform.v1"
VEHICLE_REFERENCE_SCHEMA_VERSION = "vehicle_reference.v1"

CLAIM_GRADE_CONFIDENCE = {"verified"}
CONFIDENCE_VALUES = {"assumed", "measured", "verified"}


def load_apollo_frame_transform(path: str | Path) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def load_vehicle_reference(path: str | Path) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def validate_apollo_frame_transform(cfg: Mapping[str, Any]) -> dict[str, Any]:
    _require_value(cfg, "schema_version", expected=APOLLO_FRAME_TRANSFORM_SCHEMA_VERSION)
    _require_text(cfg, "map_name")
    _require_value(cfg, "source_frame", expected="carla_world")
    _require_value(cfg, "target_frame", expected="apollo_map")
    convention = _require_mapping(cfg, "convention")
    _require_value(convention, "carla", expected="UE_left_handed_X_forward_Y_right_Z_up", prefix="convention")
    _require_value(convention, "apollo", expected="ENU_East_North_Up", prefix="convention")
    transform = _require_mapping(cfg, "transform")
    for field in ("tx", "ty", "tz", "yaw_rad", "scale"):
        _require_number(transform, field, prefix="transform")
    _require_bool(transform, "y_flip", prefix="transform")
    if float(transform["scale"]) <= 0.0:
        raise ValueError("transform.scale must be > 0")
    _require_text(cfg, "source")
    confidence = _require_confidence(cfg)
    warnings = []
    can_claim_hard_gate = confidence in CLAIM_GRADE_CONFIDENCE
    if not can_claim_hard_gate:
        warnings.append(f"confidence={confidence} cannot claim hard_gate")
    return {
        "schema_version": APOLLO_FRAME_TRANSFORM_SCHEMA_VERSION,
        "status": "pass",
        "map_name": str(cfg["map_name"]),
        "can_claim_hard_gate": can_claim_hard_gate,
        "confidence": confidence,
        "warnings": warnings,
    }


def validate_vehicle_reference(cfg: Mapping[str, Any]) -> dict[str, Any]:
    _require_value(cfg, "schema_version", expected=VEHICLE_REFERENCE_SCHEMA_VERSION)
    _require_text(cfg, "vehicle_blueprint")
    _require_value(cfg, "apollo_reference_point", expected="rear_axle_center")
    _require_text(cfg, "carla_actor_origin_definition")
    origin_to_vrp = _require_mapping(cfg, "origin_to_vrp_carla")
    for field in ("x", "y", "z"):
        _require_number(origin_to_vrp, field, prefix="origin_to_vrp_carla")
    _require_text(cfg, "source")
    confidence = _require_confidence(cfg)
    warnings = []
    can_claim_hard_gate = confidence in CLAIM_GRADE_CONFIDENCE
    if not can_claim_hard_gate:
        warnings.append(f"confidence={confidence} cannot claim hard_gate")
    if str(cfg["carla_actor_origin_definition"]).strip().lower() == "unknown":
        warnings.append("carla_actor_origin_definition=unknown requires measurement before hard-gate claims")
        can_claim_hard_gate = False
    return {
        "schema_version": VEHICLE_REFERENCE_SCHEMA_VERSION,
        "status": "pass",
        "vehicle_blueprint": str(cfg["vehicle_blueprint"]),
        "apollo_reference_point": str(cfg["apollo_reference_point"]),
        "can_claim_hard_gate": can_claim_hard_gate,
        "confidence": confidence,
        "warnings": warnings,
    }


def _load_yaml_mapping(path: str | Path) -> dict[str, Any]:
    cfg_path = Path(path).expanduser()
    payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{cfg_path} must contain a YAML mapping")
    return payload


def _require_mapping(cfg: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = cfg.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} is required and must be a mapping")
    return value


def _require_value(
    cfg: Mapping[str, Any],
    field: str,
    *,
    expected: str,
    prefix: str | None = None,
) -> str:
    value = cfg.get(field)
    path = _field_path(field, prefix=prefix)
    if value in {None, ""}:
        raise ValueError(f"{path} is required")
    text = str(value)
    if text != expected:
        raise ValueError(f"{path} must be {expected!r}, got {text!r}")
    return text


def _require_text(cfg: Mapping[str, Any], field: str, *, prefix: str | None = None) -> str:
    value = cfg.get(field)
    path = _field_path(field, prefix=prefix)
    if value in {None, ""}:
        raise ValueError(f"{path} is required")
    return str(value)


def _require_number(cfg: Mapping[str, Any], field: str, *, prefix: str | None = None) -> float:
    value = cfg.get(field)
    path = _field_path(field, prefix=prefix)
    if value in {None, ""}:
        raise ValueError(f"{path} is required")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be numeric, got {value!r}") from exc


def _require_bool(cfg: Mapping[str, Any], field: str, *, prefix: str | None = None) -> bool:
    value = cfg.get(field)
    path = _field_path(field, prefix=prefix)
    if value is None:
        raise ValueError(f"{path} is required")
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be boolean, got {value!r}")
    return bool(value)


def _require_confidence(cfg: Mapping[str, Any]) -> str:
    confidence = _require_text(cfg, "confidence")
    if confidence not in CONFIDENCE_VALUES:
        raise ValueError(f"confidence must be one of {sorted(CONFIDENCE_VALUES)}, got {confidence!r}")
    return confidence


def _field_path(field: str, *, prefix: str | None = None) -> str:
    return f"{prefix}.{field}" if prefix else field
