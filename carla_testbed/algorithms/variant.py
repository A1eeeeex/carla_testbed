from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "algorithm_variant.v1"
VALID_VARIANT_TYPES = {"upstream", "replayed", "ported_carla_gt", "tuned_town01"}
VALID_BRIDGE_BACKENDS = {"ros2_gt", "carla_direct", "cyberrt_direct", "mock", "replay_only", None}
TOP_LEVEL_REQUIRED_FIELDS = {
    "schema_version",
    "algorithm",
    "variant_id",
    "variant_type",
    "apollo",
    "map",
    "vehicle",
    "bridge",
    "calibration",
    "allowed_changes",
    "forbidden_changes",
    "notes",
}
APOLLO_FIELDS = {
    "version",
    "git_commit",
    "docker_image",
    "docker_image_digest",
    "source_patch_path",
    "source_patch_sha256",
    "config_patch_path",
    "config_patch_sha256",
}
MAP_FIELDS = {
    "map_name",
    "base_map_path",
    "base_map_sha256",
    "routing_map_path",
    "routing_map_sha256",
}
VEHICLE_FIELDS = {
    "vehicle_config_path",
    "vehicle_config_sha256",
    "vehicle_blueprint",
}
BRIDGE_FIELDS = {
    "backend",
    "bridge_config_path",
    "bridge_config_sha256",
}
CALIBRATION_FIELDS = {
    "calibration_profile_id",
    "calibration_profile_path",
    "calibration_profile_sha256",
}
WARN_HASH_FIELDS = {
    "apollo.docker_image_digest",
    "apollo.source_patch_sha256",
    "apollo.config_patch_sha256",
    "map.base_map_sha256",
    "map.routing_map_sha256",
    "vehicle.vehicle_config_sha256",
    "bridge.bridge_config_sha256",
    "calibration.calibration_profile_sha256",
}
PORTED_REQUIRED_FORBIDDEN = {"planning_algorithm_code", "control_algorithm_code"}


class AlgorithmVariantError(ValueError):
    pass


@dataclass(frozen=True)
class AlgorithmVariantValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def status(self) -> str:
        if self.errors:
            return "fail"
        if self.warnings:
            return "warn"
        return "pass"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


def _section(manifest: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = manifest.get(key)
    return value if isinstance(value, Mapping) else {}


def _validate_section(
    manifest: Mapping[str, Any],
    section_name: str,
    required_fields: set[str],
    errors: list[str],
) -> None:
    section = manifest.get(section_name)
    if not isinstance(section, Mapping):
        errors.append(f"{section_name} must be a mapping")
        return
    missing = sorted(required_fields - set(section))
    if missing:
        errors.append(f"{section_name} missing fields: {', '.join(missing)}")


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _get_path(manifest: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = manifest
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _warn_missing_hashes(manifest: Mapping[str, Any], warnings: list[str]) -> None:
    for dotted_path in sorted(WARN_HASH_FIELDS):
        value = _get_path(manifest, dotted_path)
        path_value = _get_path(manifest, dotted_path.rsplit(".", 1)[0] + "_path")
        if dotted_path.endswith("_sha256") and _is_missing(value):
            if "patch" in dotted_path and _is_missing(path_value):
                continue
            warnings.append(f"missing hash: {dotted_path}")
        elif dotted_path.endswith("docker_image_digest") and _is_missing(value):
            warnings.append(f"missing hash: {dotted_path}")


def validate_algorithm_variant(manifest: Mapping[str, Any]) -> AlgorithmVariantValidation:
    errors: list[str] = []
    warnings: list[str] = []

    missing_top = sorted(TOP_LEVEL_REQUIRED_FIELDS - set(manifest))
    if missing_top:
        errors.append(f"missing top-level fields: {', '.join(missing_top)}")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if manifest.get("algorithm") != "apollo":
        errors.append("algorithm must be apollo")

    variant_id = manifest.get("variant_id")
    if not isinstance(variant_id, str) or not variant_id.strip():
        errors.append("variant_id is required")

    variant_type = manifest.get("variant_type")
    if variant_type not in VALID_VARIANT_TYPES:
        errors.append(f"variant_type must be one of {sorted(VALID_VARIANT_TYPES)}")

    _validate_section(manifest, "apollo", APOLLO_FIELDS, errors)
    _validate_section(manifest, "map", MAP_FIELDS, errors)
    _validate_section(manifest, "vehicle", VEHICLE_FIELDS, errors)
    _validate_section(manifest, "bridge", BRIDGE_FIELDS, errors)
    _validate_section(manifest, "calibration", CALIBRATION_FIELDS, errors)

    allowed_changes = manifest.get("allowed_changes")
    forbidden_changes = manifest.get("forbidden_changes")
    if not isinstance(allowed_changes, list):
        errors.append("allowed_changes must be a list")
        allowed_set: set[str] = set()
    else:
        allowed_set = {str(item) for item in allowed_changes}
    if not isinstance(forbidden_changes, list):
        errors.append("forbidden_changes must be a list")
        forbidden_set: set[str] = set()
    else:
        forbidden_set = {str(item) for item in forbidden_changes}

    apollo = _section(manifest, "apollo")
    bridge = _section(manifest, "bridge")
    backend = bridge.get("backend")
    if backend not in VALID_BRIDGE_BACKENDS:
        errors.append(f"bridge.backend must be one of {sorted(v for v in VALID_BRIDGE_BACKENDS if v is not None)} or null")

    if variant_type == "upstream":
        if not _is_missing(apollo.get("source_patch_path")) or not _is_missing(apollo.get("source_patch_sha256")):
            errors.append("upstream variant must not set source_patch_path/source_patch_sha256")
        if not _is_missing(apollo.get("config_patch_path")) or not _is_missing(apollo.get("config_patch_sha256")):
            errors.append("upstream variant must not set config_patch_path/config_patch_sha256")
        if backend not in {None, "replay_only"}:
            errors.append("upstream variant bridge.backend must be null or replay_only")

    if variant_type == "ported_carla_gt":
        missing_forbidden = sorted(PORTED_REQUIRED_FORBIDDEN - forbidden_set)
        if missing_forbidden:
            errors.append(
                "ported_carla_gt forbidden_changes must include: " + ", ".join(missing_forbidden)
            )
        disallowed = {"planning_algorithm_code", "control_algorithm_code"} & allowed_set
        if disallowed:
            errors.append("ported_carla_gt allowed_changes must not include: " + ", ".join(sorted(disallowed)))

    if variant_type == "tuned_town01":
        if _is_missing(manifest.get("tuning_reason")):
            errors.append("tuned_town01 variant requires tuning_reason")
        if _is_missing(apollo.get("config_patch_path")):
            errors.append("tuned_town01 variant requires apollo.config_patch_path")

    _warn_missing_hashes(manifest, warnings)
    return AlgorithmVariantValidation(tuple(errors), tuple(warnings))


def load_algorithm_variant(path: str | Path) -> dict[str, Any]:
    variant_path = Path(path).expanduser()
    payload = yaml.safe_load(variant_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise AlgorithmVariantError(f"algorithm variant manifest must be a mapping: {variant_path}")
    validation = validate_algorithm_variant(payload)
    if validation.errors:
        raise AlgorithmVariantError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(variant_path)
    return payload
