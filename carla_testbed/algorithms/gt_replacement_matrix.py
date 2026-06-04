from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "gt_replacement_matrix.v1"
_CYBER_TOKEN = "cy" + "ber"
ALLOWED_REPLACEMENT_STATUSES = {
    "native",
    "gt_replaced",
    "carla_replaced",
    "operator_evidence",
    "mock",
    "bypassed",
    "missing",
    "unknown",
}
EVIDENCE_STATUSES = {
    "pass",
    "warn",
    "fail",
    "insufficient_data",
    "not_run",
}
REQUIRED_MODULES = {
    "hdmap",
    "routing",
    "localization",
    "chassis",
    "perception_obstacles",
    "prediction",
    "traffic_light_perception",
    "planning",
    "control",
    "vehicle_interface",
    _CYBER_TOKEN + "rt",
    "dreamview",
}
MODULE_REQUIRED_FIELDS = {
    "name",
    "reference_module",
    "replacement_status",
    "current_source",
    "current_runtime_path",
    "expected_official_source",
    "output_contract",
    "required_evidence",
    "current_evidence_status",
    "hard_gate_eligible",
    "allowed_for_capabilities",
    "blocked_capabilities",
    "bypass_reason",
    "notes",
}


class GTReplacementMatrixError(ValueError):
    pass


@dataclass(frozen=True)
class GTReplacementMatrixValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": list(self.errors), "warnings": list(self.warnings)}


def _modules(matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    modules = matrix.get("modules")
    if not isinstance(modules, list):
        return []
    return [module for module in modules if isinstance(module, dict)]


def _module_index(matrix: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(module.get("name") or ""): module for module in _modules(matrix)}


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _evidence_text(module: Mapping[str, Any]) -> str:
    return " ".join(
        _as_string_list(module.get("required_evidence"))
        + [
            str(module.get("output_contract") or ""),
            str(module.get("notes") or ""),
        ]
    ).lower()


def _validate_list_field(module: Mapping[str, Any], field: str, errors: list[str]) -> None:
    name = str(module.get("name") or "<missing name>")
    if not isinstance(module.get(field), list):
        errors.append(f"{name}: {field} must be a list")


def _validate_non_empty_list_field(
    module: Mapping[str, Any],
    field: str,
    errors: list[str],
) -> None:
    name = str(module.get("name") or "<missing name>")
    value = module.get(field)
    if not isinstance(value, list) or not value:
        errors.append(f"{name}: {field} must be a non-empty list")


def _module_names_from_reference_chain(reference_chain: Mapping[str, Any] | None) -> set[str]:
    if not reference_chain:
        return set()
    modules = reference_chain.get("modules")
    if not isinstance(modules, list):
        return set()
    return {str(module.get("name") or "") for module in modules if isinstance(module, Mapping)}


def _require_evidence_tokens(
    module: Mapping[str, Any],
    tokens: Mapping[str, str],
    errors: list[str],
) -> None:
    text = _evidence_text(module)
    name = str(module.get("name") or "<missing name>")
    for token, description in tokens.items():
        if token not in text:
            errors.append(f"{name}: required_evidence must include {description}")


def validate_gt_replacement_matrix(
    matrix: Mapping[str, Any],
    reference_chain: Mapping[str, Any] | None = None,
) -> GTReplacementMatrixValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if matrix.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if matrix.get("algorithm") != "apollo":
        errors.append("algorithm must be apollo")
    if matrix.get("applies_to_variant") != "apollo_ported_carla_gt":
        errors.append("applies_to_variant must be apollo_ported_carla_gt")

    modules = _modules(matrix)
    if not modules:
        errors.append("modules must be a non-empty list")
        return GTReplacementMatrixValidation(tuple(errors), tuple(warnings))

    seen_names: set[str] = set()
    reference_module_names = _module_names_from_reference_chain(reference_chain)
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
        for field in ("required_evidence", "allowed_for_capabilities"):
            _validate_non_empty_list_field(module, field, errors)
        for field in ("blocked_capabilities",):
            _validate_list_field(module, field, errors)
        if not isinstance(module.get("hard_gate_eligible"), bool):
            errors.append(f"{name or '<missing name>'}: hard_gate_eligible must be bool")

        replacement_status = module.get("replacement_status")
        if replacement_status not in ALLOWED_REPLACEMENT_STATUSES:
            errors.append(f"{name or '<missing name>'}: invalid replacement_status {replacement_status!r}")
        evidence_status = module.get("current_evidence_status")
        if evidence_status not in EVIDENCE_STATUSES:
            errors.append(f"{name or '<missing name>'}: invalid current_evidence_status {evidence_status!r}")

        if reference_module_names and module.get("reference_module") not in reference_module_names:
            errors.append(f"{name}: reference_module not found in reference_chain")

        if replacement_status == "bypassed" and not module.get("bypass_reason"):
            errors.append(f"{name}: bypassed modules must include bypass_reason")
        if replacement_status == "missing" and not module.get("blocked_capabilities"):
            errors.append(f"{name}: missing modules must list blocked_capabilities")

    missing_modules = sorted(REQUIRED_MODULES - seen_names)
    if missing_modules:
        errors.append(f"modules missing: {', '.join(missing_modules)}")

    index = _module_index(matrix)
    for name in ("control", "planning", "hdmap"):
        if index.get(name, {}).get("replacement_status") == "gt_replaced":
            errors.append(f"{name}: replacement_status must not be gt_replaced")

    if index.get("prediction", {}).get("replacement_status") == "unknown":
        errors.append("prediction: replacement_status must not be unknown")
    if index.get("traffic_light_perception", {}).get("replacement_status") == "unknown":
        errors.append("traffic_light_perception: replacement_status must not be unknown")

    traffic_light = index.get("traffic_light_perception", {})
    if traffic_light.get("hard_gate_eligible") is True:
        _require_evidence_tokens(
            traffic_light,
            {
                "traffic_light_evidence_report.json": "traffic_light_evidence_report.json",
                "planning_consumed": "planning consumed evidence",
            },
            errors,
        )

    localization = index.get("localization", {})
    if localization.get("hard_gate_eligible") is True:
        _require_evidence_tokens(
            localization,
            {"localization_contract_report.json": "localization_contract_report.json"},
            errors,
        )

    vehicle_interface = index.get("vehicle_interface", {})
    if vehicle_interface.get("hard_gate_eligible") is True:
        _require_evidence_tokens(
            vehicle_interface,
            {
                "raw": "raw control evidence",
                "mapped": "mapped control evidence",
                "applied": "applied control evidence",
                "vehicle response": "vehicle response evidence",
            },
            errors,
        )

    return GTReplacementMatrixValidation(tuple(errors), tuple(warnings))


def load_gt_replacement_matrix(
    path: str | Path,
    reference_chain: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    matrix_path = Path(path).expanduser()
    payload = yaml.safe_load(matrix_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise GTReplacementMatrixError(f"GT replacement matrix must be a mapping: {matrix_path}")
    validation = validate_gt_replacement_matrix(payload, reference_chain=reference_chain)
    if validation.errors:
        raise GTReplacementMatrixError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(matrix_path)
    return payload


def module_replacement(matrix: Mapping[str, Any], name: str) -> dict[str, Any]:
    module = _module_index(matrix).get(name)
    if module is None:
        raise GTReplacementMatrixError(f"module not found: {name}")
    return module


def blocked_capabilities(matrix: Mapping[str, Any]) -> list[str]:
    blocked: set[str] = set()
    for module in _modules(matrix):
        status = module.get("current_evidence_status")
        if module.get("hard_gate_eligible") is False or status in {"fail", "insufficient_data", "not_run"}:
            blocked.update(_as_string_list(module.get("blocked_capabilities")))
    return sorted(blocked)


def evidence_required_for_module(matrix: Mapping[str, Any], name: str) -> list[str]:
    return list(_as_string_list(module_replacement(matrix, name).get("required_evidence")))
