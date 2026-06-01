from __future__ import annotations

from pathlib import Path

from carla_testbed.algorithms.inventory import (
    list_enabled_mvp_modules,
    list_modules,
    load_algorithm_inventory,
    validate_algorithm_inventory,
)

INVENTORY_PATH = Path("configs/algorithms/apollo_inventory.yaml")


def _variant_index(inventory: dict) -> dict[str, dict]:
    return {variant["name"]: variant for variant in inventory["supported_variants"]}


def test_apollo_inventory_yaml_loads() -> None:
    inventory = load_algorithm_inventory(INVENTORY_PATH)

    assert inventory["schema_version"] == "algorithm_inventory.v1"
    assert inventory["algorithm"] == "apollo"
    assert validate_algorithm_inventory(inventory).ok


def test_required_apollo_modules_present() -> None:
    inventory = load_algorithm_inventory(INVENTORY_PATH)
    module_names = {module["name"] for module in list_modules(inventory)}

    assert {
        "routing",
        "localization",
        "perception",
        "prediction",
        "planning",
        "control",
        "cyberrt",
    }.issubset(module_names)


def test_each_module_has_contract_and_evidence_fields() -> None:
    inventory = load_algorithm_inventory(INVENTORY_PATH)

    for module in list_modules(inventory):
        assert module["required_inputs"], module["name"]
        assert module["expected_outputs"], module["name"]
        assert module["evidence_artifacts"], module["name"]
        assert isinstance(module["enabled_in_mvp"], bool), module["name"]
        assert isinstance(module["replaced_by_gt_in_mvp"], bool), module["name"]
        assert module["reproduction_level_required"], module["name"]


def test_tuned_town01_is_not_upstream_variant() -> None:
    inventory = load_algorithm_inventory(INVENTORY_PATH)
    variants = _variant_index(inventory)

    assert "apollo_tuned_town01" in variants
    assert variants["apollo_tuned_town01"]["upstream_compatible"] is False
    assert variants["apollo_tuned_town01"]["reproduction_level"] != "upstream"


def test_enabled_mvp_modules_include_real_runtime_stack() -> None:
    inventory = load_algorithm_inventory(INVENTORY_PATH)
    enabled_names = {module["name"] for module in list_enabled_mvp_modules(inventory)}

    assert {"routing", "localization", "planning", "control", "cyberrt"}.issubset(enabled_names)
