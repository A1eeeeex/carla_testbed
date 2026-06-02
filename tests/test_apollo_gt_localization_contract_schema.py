from __future__ import annotations

from pathlib import Path

import pytest

from carla_testbed.adapters.apollo.localization_contract_schema import (
    load_apollo_frame_transform,
    load_vehicle_reference,
    validate_apollo_frame_transform,
    validate_vehicle_reference,
)

FRAME_TRANSFORM_EXAMPLE = Path("configs/town01/apollo_frame_transform.example.yaml")
VEHICLE_REFERENCE_EXAMPLE = Path("configs/vehicles/ego_vehicle_reference.example.yaml")
SCHEMA_MODULE = Path("carla_testbed/adapters/apollo/localization_contract_schema.py")


def test_example_frame_transform_loads_and_is_diagnostic_only() -> None:
    cfg = load_apollo_frame_transform(FRAME_TRANSFORM_EXAMPLE)
    result = validate_apollo_frame_transform(cfg)

    assert cfg["schema_version"] == "apollo_frame_transform.v1"
    assert cfg["transform"]["y_flip"] is True
    assert result["status"] == "pass"
    assert result["confidence"] == "assumed"
    assert result["can_claim_hard_gate"] is False
    assert "confidence=assumed cannot claim hard_gate" in result["warnings"]


def test_example_vehicle_reference_loads_and_is_diagnostic_only() -> None:
    cfg = load_vehicle_reference(VEHICLE_REFERENCE_EXAMPLE)
    result = validate_vehicle_reference(cfg)

    assert cfg["schema_version"] == "vehicle_reference.v1"
    assert cfg["apollo_reference_point"] == "rear_axle_center"
    assert cfg["origin_to_vrp_carla"]["x"] == -1.35
    assert result["status"] == "pass"
    assert result["confidence"] == "assumed"
    assert result["can_claim_hard_gate"] is False
    assert "confidence=assumed cannot claim hard_gate" in result["warnings"]


def test_missing_frame_transform_y_flip_has_clear_error() -> None:
    cfg = load_apollo_frame_transform(FRAME_TRANSFORM_EXAMPLE)
    cfg["transform"].pop("y_flip")

    with pytest.raises(ValueError, match="transform\\.y_flip is required"):
        validate_apollo_frame_transform(cfg)


def test_missing_frame_transform_yaw_has_clear_error() -> None:
    cfg = load_apollo_frame_transform(FRAME_TRANSFORM_EXAMPLE)
    cfg["transform"].pop("yaw_rad")

    with pytest.raises(ValueError, match="transform\\.yaw_rad is required"):
        validate_apollo_frame_transform(cfg)


def test_missing_vehicle_origin_to_vrp_has_clear_error() -> None:
    cfg = load_vehicle_reference(VEHICLE_REFERENCE_EXAMPLE)
    cfg.pop("origin_to_vrp_carla")

    with pytest.raises(ValueError, match="origin_to_vrp_carla is required"):
        validate_vehicle_reference(cfg)


def test_verified_confidence_can_claim_hard_gate_when_origin_is_known() -> None:
    frame_cfg = load_apollo_frame_transform(FRAME_TRANSFORM_EXAMPLE)
    frame_cfg["confidence"] = "verified"
    vehicle_cfg = load_vehicle_reference(VEHICLE_REFERENCE_EXAMPLE)
    vehicle_cfg["confidence"] = "verified"
    vehicle_cfg["carla_actor_origin_definition"] = "blueprint_origin_measured"

    assert validate_apollo_frame_transform(frame_cfg)["can_claim_hard_gate"] is True
    assert validate_vehicle_reference(vehicle_cfg)["can_claim_hard_gate"] is True


def test_schema_module_has_no_runtime_imports() -> None:
    text = SCHEMA_MODULE.read_text(encoding="utf-8").lower()

    assert "import carla" not in text
    assert "import cyber" not in text
    assert "pb2" not in text
