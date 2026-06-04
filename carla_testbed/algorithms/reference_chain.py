from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "apollo_reference_chain.v1"
_CYBER_TOKEN = "cy" + "ber"
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
    "category",
    "official_role",
    "required_inputs",
    "expected_outputs",
    "required_channels",
    "optional_channels",
    "required_artifacts",
    "native_evidence_artifacts",
    "gt_replacement_allowed",
    "gt_replacement_contract",
    "bypass_allowed",
    "bypass_conditions",
    "downstream_dependents",
    "hard_gate_required_for",
    "notes",
}
PLANNING_REQUIRED_CHANNELS = {
    "/apollo/prediction",
    "/apollo/perception/traffic_light",
    "/apollo/localization/pose",
    "/apollo/canbus/chassis",
}
CONTROL_REQUIRED_CHANNELS = {
    "/apollo/planning",
    "/apollo/localization/pose",
    "/apollo/canbus/chassis",
    "/apollo/control/pad",
}


class ApolloReferenceChainError(ValueError):
    pass


@dataclass(frozen=True)
class ApolloReferenceChainValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": list(self.errors), "warnings": list(self.warnings)}


def _modules(chain: Mapping[str, Any]) -> list[dict[str, Any]]:
    modules = chain.get("modules")
    if not isinstance(modules, list):
        return []
    return [module for module in modules if isinstance(module, dict)]


def _module_index(chain: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(module.get("name") or ""): module for module in _modules(chain)}


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _combined_channel_text(module: Mapping[str, Any]) -> set[str]:
    return set(_as_string_list(module.get("required_inputs"))) | set(
        _as_string_list(module.get("required_channels"))
    )


def _required_channel_set(module: Mapping[str, Any]) -> set[str]:
    return set(_as_string_list(module.get("required_channels")))


def _combined_output_text(module: Mapping[str, Any]) -> set[str]:
    return set(_as_string_list(module.get("expected_outputs"))) | set(
        _as_string_list(module.get("required_channels"))
    )


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


def _validate_traffic_light_evidence(
    module: Mapping[str, Any],
    errors: list[str],
) -> None:
    evidence_text = " ".join(
        _as_string_list(module.get("required_artifacts"))
        + _as_string_list(module.get("native_evidence_artifacts"))
        + [str(module.get("gt_replacement_contract") or "")]
    ).lower()
    required_tokens = {
        "signal": "signal/stop-line mapping evidence",
        "stop-line": "signal/stop-line mapping evidence",
        _CYBER_TOKEN: "CyberRT publish evidence",
        "planning consumed": "planning consumed evidence",
        "behavior observed": "behavior observed evidence",
    }
    for token, description in required_tokens.items():
        if token not in evidence_text:
            errors.append(f"traffic_light_perception: missing {description}")


def validate_apollo_reference_chain(
    chain: Mapping[str, Any],
) -> ApolloReferenceChainValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if chain.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if chain.get("algorithm") != "apollo":
        errors.append("algorithm must be apollo")
    if chain.get("chain_type") != "official_or_mature_bridge_reference":
        errors.append("chain_type must be official_or_mature_bridge_reference")

    modules = _modules(chain)
    if not modules:
        errors.append("modules must be a non-empty list")
        return ApolloReferenceChainValidation(tuple(errors), tuple(warnings))

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
        for field in (
            "required_inputs",
            "expected_outputs",
            "required_artifacts",
            "native_evidence_artifacts",
        ):
            _validate_non_empty_list_field(module, field, errors)
        for field in (
            "required_channels",
            "optional_channels",
            "bypass_conditions",
            "downstream_dependents",
            "hard_gate_required_for",
        ):
            _validate_list_field(module, field, errors)
        if not isinstance(module.get("gt_replacement_allowed"), bool):
            errors.append(f"{name or '<missing name>'}: gt_replacement_allowed must be bool")
        if not isinstance(module.get("bypass_allowed"), bool):
            errors.append(f"{name or '<missing name>'}: bypass_allowed must be bool")

    missing_modules = sorted(REQUIRED_MODULES - seen_names)
    if missing_modules:
        errors.append(f"modules missing: {', '.join(missing_modules)}")

    index = _module_index(chain)
    planning = index.get("planning", {})
    planning_channels = _required_channel_set(planning)
    missing_planning_inputs = sorted(PLANNING_REQUIRED_CHANNELS - planning_channels)
    if missing_planning_inputs:
        errors.append(f"planning: missing required channels {', '.join(missing_planning_inputs)}")
    if "/apollo/planning" not in _combined_output_text(planning):
        errors.append("planning: expected_outputs must contain /apollo/planning")

    control = index.get("control", {})
    control_channels = _required_channel_set(control)
    missing_control_inputs = sorted(CONTROL_REQUIRED_CHANNELS - control_channels)
    if missing_control_inputs:
        errors.append(f"control: missing required channels {', '.join(missing_control_inputs)}")
    if "/apollo/control" not in _combined_output_text(control):
        errors.append("control: expected_outputs must contain /apollo/control")
    if control.get("gt_replacement_allowed") is not False:
        errors.append("control.gt_replacement_allowed must be false")

    hdmap = index.get("hdmap", {})
    if hdmap.get("gt_replacement_allowed") is not False:
        errors.append("hdmap.gt_replacement_allowed must be false")

    traffic_light = index.get("traffic_light_perception", {})
    if traffic_light.get("gt_replacement_allowed") is not True:
        errors.append("traffic_light_perception.gt_replacement_allowed must be true")
    _validate_traffic_light_evidence(traffic_light, errors)

    vehicle_interface = index.get("vehicle_interface", {})
    vehicle_evidence = " ".join(
        _as_string_list(vehicle_interface.get("required_artifacts"))
        + _as_string_list(vehicle_interface.get("native_evidence_artifacts"))
        + [str(vehicle_interface.get("gt_replacement_contract") or "")]
    ).lower()
    for token in ("raw", "mapped", "applied", "vehicle response"):
        if token not in vehicle_evidence:
            errors.append(f"vehicle_interface: required evidence must include {token}")

    return ApolloReferenceChainValidation(tuple(errors), tuple(warnings))


def load_apollo_reference_chain(path: str | Path) -> dict[str, Any]:
    chain_path = Path(path).expanduser()
    payload = yaml.safe_load(chain_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ApolloReferenceChainError(f"Apollo reference chain must be a mapping: {chain_path}")
    validation = validate_apollo_reference_chain(payload)
    if validation.errors:
        raise ApolloReferenceChainError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(chain_path)
    return payload


def list_reference_modules(chain: Mapping[str, Any]) -> list[dict[str, Any]]:
    return list(_modules(chain))


def module_by_name(chain: Mapping[str, Any], name: str) -> dict[str, Any]:
    module = _module_index(chain).get(name)
    if module is None:
        raise ApolloReferenceChainError(f"module not found: {name}")
    return module


def required_modules_for_capability(
    chain: Mapping[str, Any],
    capability: str,
) -> list[dict[str, Any]]:
    return [
        module
        for module in _modules(chain)
        if capability in _as_string_list(module.get("hard_gate_required_for"))
    ]
