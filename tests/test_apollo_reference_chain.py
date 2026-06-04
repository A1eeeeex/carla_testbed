from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from carla_testbed.algorithms.reference_chain import (
    ApolloReferenceChainError,
    list_reference_modules,
    load_apollo_reference_chain,
    module_by_name,
    required_modules_for_capability,
    validate_apollo_reference_chain,
)

CHAIN_PATH = Path("configs/reference/apollo_reference_chain.yaml")


def _load_raw_chain() -> dict:
    return load_apollo_reference_chain(CHAIN_PATH)


def test_apollo_reference_chain_yaml_loads() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)

    assert chain["schema_version"] == "apollo_reference_chain.v1"
    assert chain["algorithm"] == "apollo"
    assert validate_apollo_reference_chain(chain).ok


def test_required_modules_are_present() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)
    module_names = {module["name"] for module in list_reference_modules(chain)}

    assert {
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
        "cyberrt",
        "dreamview",
    }.issubset(module_names)


def test_planning_and_control_required_channels_are_declared() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)
    planning = module_by_name(chain, "planning")
    control = module_by_name(chain, "control")

    assert {
        "/apollo/prediction",
        "/apollo/perception/traffic_light",
        "/apollo/localization/pose",
        "/apollo/canbus/chassis",
    }.issubset(set(planning["required_channels"]))
    assert "/apollo/planning" in planning["expected_outputs"]

    assert {
        "/apollo/planning",
        "/apollo/localization/pose",
        "/apollo/canbus/chassis",
        "/apollo/control/pad",
    }.issubset(set(control["required_channels"]))
    assert "/apollo/control" in control["expected_outputs"]


def test_control_and_hdmap_do_not_allow_gt_replacement() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)

    assert module_by_name(chain, "control")["gt_replacement_allowed"] is False
    assert module_by_name(chain, "hdmap")["gt_replacement_allowed"] is False


def test_traffic_light_gt_replacement_requires_planning_consumed_evidence() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)
    traffic_light = module_by_name(chain, "traffic_light_perception")

    assert traffic_light["gt_replacement_allowed"] is True
    evidence_text = " ".join(
        traffic_light["required_artifacts"]
        + traffic_light["native_evidence_artifacts"]
        + [traffic_light["gt_replacement_contract"]]
    ).lower()
    assert "signal" in evidence_text
    assert "stop-line" in evidence_text
    assert "cyber" in evidence_text
    assert "planning consumed" in evidence_text
    assert "behavior observed" in evidence_text


def test_required_modules_for_traffic_light_capability() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)
    module_names = [module["name"] for module in required_modules_for_capability(chain, "traffic_light")]

    assert module_names == [
        "hdmap",
        "routing",
        "localization",
        "chassis",
        "traffic_light_perception",
        "planning",
        "control",
        "vehicle_interface",
    ]


def test_required_modules_for_closed_loop_include_runtime_stack() -> None:
    chain = load_apollo_reference_chain(CHAIN_PATH)
    module_names = {module["name"] for module in required_modules_for_capability(chain, "closed_loop")}

    assert {"planning", "control", "vehicle_interface", "cyberrt"}.issubset(module_names)


def test_validator_rejects_missing_planning_prediction_channel() -> None:
    chain = deepcopy(_load_raw_chain())
    planning = module_by_name(chain, "planning")
    planning["required_channels"] = [
        channel for channel in planning["required_channels"] if channel != "/apollo/prediction"
    ]

    validation = validate_apollo_reference_chain(chain)

    assert not validation.ok
    assert any("planning: missing required channels" in error for error in validation.errors)


def test_load_rejects_control_gt_replacement(tmp_path: Path) -> None:
    chain = deepcopy(_load_raw_chain())
    module_by_name(chain, "control")["gt_replacement_allowed"] = True
    path = tmp_path / "bad_reference_chain.yaml"

    import yaml

    path.write_text(yaml.safe_dump(chain), encoding="utf-8")

    with pytest.raises(ApolloReferenceChainError, match="control.gt_replacement_allowed"):
        load_apollo_reference_chain(path)
