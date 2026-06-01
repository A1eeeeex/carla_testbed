from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "algorithm_inventory.v1"
REQUIRED_VARIANTS = {
    "apollo_upstream",
    "apollo_replayed",
    "apollo_ported_carla_gt",
    "apollo_tuned_town01",
}
REQUIRED_MODULES = {
    "routing",
    "localization",
    "perception",
    "prediction",
    "planning",
    "control",
    "cyberrt",
}
MODULE_REQUIRED_FIELDS = {
    "name",
    "category",
    "enabled_in_mvp",
    "replaced_by_gt_in_mvp",
    "required_inputs",
    "expected_outputs",
    "evidence_artifacts",
    "reproduction_level_required",
}


class AlgorithmInventoryError(ValueError):
    pass


@dataclass(frozen=True)
class AlgorithmInventoryValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": list(self.errors), "warnings": list(self.warnings)}


def _variant_name(variant: Any) -> str:
    if isinstance(variant, Mapping):
        return str(variant.get("name") or "")
    return str(variant or "")


def _variant_index(inventory: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    indexed: dict[str, Mapping[str, Any]] = {}
    for variant in inventory.get("supported_variants") or []:
        if isinstance(variant, Mapping):
            name = _variant_name(variant)
            if name:
                indexed[name] = variant
    return indexed


def _modules(inventory: Mapping[str, Any]) -> list[dict[str, Any]]:
    modules = inventory.get("modules")
    if not isinstance(modules, list):
        return []
    return [module for module in modules if isinstance(module, dict)]


def _validate_non_empty_list(
    module: Mapping[str, Any],
    field: str,
    errors: list[str],
) -> None:
    value = module.get(field)
    name = module.get("name", "<missing name>")
    if not isinstance(value, list) or not value:
        errors.append(f"{name}: {field} must be a non-empty list")


def validate_algorithm_inventory(inventory: Mapping[str, Any]) -> AlgorithmInventoryValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if inventory.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if inventory.get("algorithm") != "apollo":
        errors.append("algorithm must be apollo")

    supported_variants = inventory.get("supported_variants")
    if not isinstance(supported_variants, list) or not supported_variants:
        errors.append("supported_variants must be a non-empty list")
        variant_names: set[str] = set()
    else:
        variant_names = {_variant_name(variant) for variant in supported_variants}
        missing_variants = sorted(REQUIRED_VARIANTS - variant_names)
        if missing_variants:
            errors.append(f"supported_variants missing: {', '.join(missing_variants)}")

    variant_index = _variant_index(inventory)
    tuned = variant_index.get("apollo_tuned_town01")
    if tuned is None:
        warnings.append("apollo_tuned_town01 has no metadata mapping")
    elif tuned.get("upstream_compatible") is True or tuned.get("reproduction_level") == "upstream":
        errors.append("apollo_tuned_town01 must not be marked as upstream-compatible")

    modules = _modules(inventory)
    if not modules:
        errors.append("modules must be a non-empty list")
        return AlgorithmInventoryValidation(tuple(errors), tuple(warnings))

    seen_names: set[str] = set()
    for module in modules:
        name = str(module.get("name") or "")
        if not name:
            errors.append("module missing name")
        elif name in seen_names:
            errors.append(f"duplicate module name: {name}")
        seen_names.add(name)

        missing_fields = sorted(MODULE_REQUIRED_FIELDS - set(module))
        if missing_fields:
            errors.append(f"{name or '<missing name>'}: missing fields {', '.join(missing_fields)}")
        for field in ("required_inputs", "expected_outputs", "evidence_artifacts"):
            _validate_non_empty_list(module, field, errors)
        if not isinstance(module.get("enabled_in_mvp"), bool):
            errors.append(f"{name or '<missing name>'}: enabled_in_mvp must be bool")
        if not isinstance(module.get("replaced_by_gt_in_mvp"), bool):
            errors.append(f"{name or '<missing name>'}: replaced_by_gt_in_mvp must be bool")
        if not module.get("reproduction_level_required"):
            errors.append(f"{name or '<missing name>'}: reproduction_level_required is required")

    missing_modules = sorted(REQUIRED_MODULES - seen_names)
    if missing_modules:
        errors.append(f"modules missing: {', '.join(missing_modules)}")

    return AlgorithmInventoryValidation(tuple(errors), tuple(warnings))


def load_algorithm_inventory(path: str | Path) -> dict[str, Any]:
    inventory_path = Path(path).expanduser()
    payload = yaml.safe_load(inventory_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise AlgorithmInventoryError(f"algorithm inventory must be a mapping: {inventory_path}")
    validation = validate_algorithm_inventory(payload)
    if validation.errors:
        raise AlgorithmInventoryError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(inventory_path)
    return payload


def list_modules(inventory: Mapping[str, Any]) -> list[dict[str, Any]]:
    return list(_modules(inventory))


def list_enabled_mvp_modules(inventory: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [module for module in _modules(inventory) if module.get("enabled_in_mvp") is True]
